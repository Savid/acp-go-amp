package ampacp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-amp/internal/amp"
)

const (
	ForkSessionMethod = "_amp/session/fork"
	RawEventMethod    = "_amp/rawEvent"

	ampMetaKey       = "amp"
	configMode       = acp.SessionConfigId(optionModeKey)
	configTypeSelect = "select"

	jsonFieldError  = "error"
	jsonFieldField  = "field"
	jsonFieldMethod = "method"
	jsonFieldServer = "server"

	// Recurring _meta keys and native wire values shared across the mapping
	// surface. Centralized so the exact tokens sent to and read from amp cannot
	// drift between call sites.
	metaRawEventKey = "rawEvent"
	// metaParentToolUseIDKey is the _meta.amp key that carries delegated-agent
	// provenance. Amp delivers subagent, oracle, and Task activity as ordinary
	// stream-json frames whose parent_tool_use_id points at the spawning tool_use
	// block; every session/update derived from such a frame is stamped with this
	// key so hosts can attribute the activity to its parent tool call.
	metaParentToolUseIDKey = "parentToolUseId"
	optionModelKey         = "model"
	optionModeKey          = "mode"
	optionEnvKey           = "env"
	optionFieldHome        = "home"
	// optionFieldProviderAuthDirectHome names the unsupported exact-home consent
	// gate.
	optionFieldProviderAuthDirectHome = "providerAuthDirectHome"

	fieldValue    = "value"
	fieldPrompt   = "prompt"
	fieldCursor   = "cursor"
	fieldConfigID = "configId"
	// fieldType names the request discriminator. Neither
	// SetSessionConfigOptionRequest variant is itself a wire field, so `type` is
	// the only JSON path a wrong-variant rejection can name.
	fieldType = "type"

	keyType      = "type"
	keyDetail    = "detail"
	keyMessage   = "message"
	keyIndex     = "index"
	keyContent   = "content"
	keyData      = "data"
	keyMaxBytes  = "maxBytes"
	keySizeBytes = "sizeBytes"
	keyMIMEType  = "mimeType"
	keyMediaType = "media_type"
	keySource    = "source"
	keyURL       = "url"
	envHome      = "HOME"

	envXDGConfigHome = "XDG_CONFIG_HOME"
	envXDGCacheHome  = "XDG_CACHE_HOME"
	envXDGDataHome   = "XDG_DATA_HOME"
	envXDGStateHome  = "XDG_STATE_HOME"

	valUnsupported       = "unsupported"
	valNoTransport       = "no_transport"
	valText              = "text"
	valImage             = "image"
	valBase64            = "base64"
	valHTTP              = "http"
	valHTTPS             = "https"
	valUser              = "user"
	valRequired          = "required"
	valDuplicate         = "duplicate"
	valAmbiguous         = "ambiguous"
	valMismatch          = "mismatch"
	reasonUnserializable = "unserializable"

	modeLow    = "low"
	modeMedium = "medium"
	modeHigh   = "high"
)

var (
	errSessionClosed = errors.New("session closed")
	writeFile        = os.WriteFile
	readFile         = os.ReadFile
	mkdirAll         = os.MkdirAll
	mkdirTemp        = os.MkdirTemp
	removeSessionDir = os.RemoveAll
)

type ampManifest struct {
	Format    string `json:"format"`
	SessionID string `json:"sessionId"`
	// NativeSessionID is the server-side Amp thread id. It is empty until the
	// session's first prompt turn creates the thread and the wrapper adopts
	// the id from the stream-json init frame.
	NativeSessionID    string `json:"nativeSessionId,omitempty"`
	Cwd                string `json:"cwd"`
	Title              string `json:"title,omitempty"`
	Mode               string `json:"mode,omitempty"`
	UpdatedAtUnixMilli int64  `json:"updatedAtUnixMilli"`
	CreatedAtUnixMilli int64  `json:"createdAtUnixMilli"`
}

type agentSession struct {
	agent *Agent
	id    acp.SessionId
	// nativeID is the adopted server-side Amp thread id, empty until the
	// first prompt turn creates the thread. Guarded by mu.
	nativeID              string
	cwd                   string
	title                 string
	mode                  string
	createdUnix           int64
	updatedUnix           int64
	additionalDirectories []string
	mcpConfigJSON         string
	// env is the prompt child's complete environment, including the host's raw
	// PATH carrier. operationEnv is what every other child of this session
	// receives: the static agent base, the named operation values, and the
	// adapter-managed residence, with no session PATH.
	env                   map[string]string
	operationEnv          map[string]string
	rawEvents             bool
	rawEventMu            sync.Mutex
	rawEventSeq           atomic.Int64
	settingsDir           string
	settingsFile          string
	scratchRootRelease    func()
	closed                bool
	poisonCause           string
	nativeMissingCause    string
	scratchContainmentErr error
	unsyncedFrames        []SessionStoreEntry
	transcriptFrames      int
	// vacancyUnproven records that a completed prompt boundary failed to
	// enumerate an empty contained descendant set. A session that has never
	// prompted started no process at all, so it opens with nothing outstanding.
	vacancyUnproven bool
	// persistForbidden fences durable writes once the session's row is being
	// deleted, so nothing this session still holds is written back over the
	// tombstone.
	persistForbidden bool
	turn             chan struct{}
	cancelMu         sync.Mutex
	activePrompt     *promptTurnState
	persistMu        sync.Mutex
	mu               sync.Mutex
}

func newAgentSession(ctx context.Context, agent *Agent, id acp.SessionId, cwd string, meta parsedSessionMeta, mcpConfigJSON string, additionalDirs []string) (_ *agentSession, err error) {
	if validateErr := validateSessionID(string(id)); validateErr != nil {
		return nil, fmt.Errorf("invalid amp session id: %w", validateErr)
	}

	now := time.Now().UnixMilli()

	scratchRelease, err := reserveScratchRoot(ctx, agent.options.RuntimeResourceHooks, RuntimeResourceSession)
	if err != nil {
		return nil, err
	}

	var dir string

	keepScratch := false
	defer func() {
		if keepScratch {
			return
		}

		var removeErr error
		if dir != "" {
			removeErr = removeSessionDir(dir)
		}

		if removeErr == nil {
			scratchRelease()
		}

		err = errors.Join(err, removeErr)
	}()

	parent, err := ensureScratchParent(agent.options.ScratchDir)
	if err != nil {
		return nil, err
	}

	dir, err = mkdirTemp(parent, "acp-go-amp-session-*")
	if err != nil {
		return nil, fmt.Errorf("create amp settings dir: %w", err)
	}

	homeDir := filepath.Join(dir, "home")
	configDir := filepath.Join(dir, "xdg-config")
	cacheDir := filepath.Join(dir, "xdg-cache")
	dataDir := filepath.Join(dir, "xdg-data")

	stateDir := filepath.Join(dir, "xdg-state")
	for _, path := range []string{homeDir, configDir, cacheDir, dataDir, stateDir, filepath.Join(configDir, "amp")} {
		if err := mkdirAll(path, 0o700); err != nil {
			return nil, fmt.Errorf("create amp isolated home: %w", err)
		}
	}

	// The settings file the wrapper points amp at is the only place the native
	// secrets flag resolves from: it is read at global scope, which no managed
	// file intercepts. Asserting it false keeps the credential in the isolated
	// per-session file store, because the keystore item it would otherwise move
	// to is keyed by hostname alone and shared by every session on the machine.
	settingsFile := filepath.Join(configDir, "amp", "settings.json")
	if err := writeFile(settingsFile, amp.AuthSettingsDocument(), 0o600); err != nil {
		return nil, fmt.Errorf("write amp settings file: %w", err)
	}

	if err := writeSeedFiles(homeDir, agent.options.SeedFiles); err != nil {
		return nil, err
	}

	if err := handoffGeneratedNativeTree(dir, agent.options.ProcessIsolation); err != nil {
		return nil, err
	}

	mode := meta.options.Mode
	if mode == "" {
		mode = modeMedium
	}

	managed := managedSessionEnv(homeDir, configDir, cacheDir, dataDir, stateDir)

	// env is the complete session child environment, raw PATH carrier and all.
	// Only a prompt child receives it. Every other child this session starts
	// runs on operationEnv: the static agent base plus the named operation
	// values, so a directory on the session PATH reaches the prompt it was
	// meant for and reaches nothing else.
	env := composeEnv(agent.options.Env, meta.options.Env, managed)
	operationEnv := composeEnv(agent.options.Env, operationSessionEnv(meta.options.Env), managed)

	session := &agentSession{
		agent:                 agent,
		id:                    id,
		cwd:                   cwd,
		mode:                  mode,
		createdUnix:           now,
		updatedUnix:           now,
		additionalDirectories: append([]string(nil), additionalDirs...),
		mcpConfigJSON:         mcpConfigJSON,
		env:                   env,
		operationEnv:          operationEnv,
		rawEvents:             meta.rawEvent,
		settingsDir:           dir,
		settingsFile:          settingsFile,
		scratchRootRelease:    scratchRelease,
		turn:                  make(chan struct{}, 1),
	}
	keepScratch = true

	return session, nil
}

// client is the session's non-prompt client: thread export, thread delete, and
// account login run on the static operation environment.
func (s *agentSession) client() *amp.Client {
	return s.clientWithEnv(s.operationEnv, "", RuntimeResourceSession)
}

func (s *agentSession) clientWithEnv(env map[string]string, mcpConfigPath string, kind RuntimeResourceKind) *amp.Client {
	options := amp.Options{
		CLIPath:                    s.agent.options.ExecutablePath,
		Cwd:                        s.cwd,
		SettingsFile:               s.settingsFile,
		Env:                        env,
		ResolutionEnv:              composeEnv(s.agent.options.Env),
		ResolvedExecutable:         s.agent.retainedHarnessPath(),
		Mode:                       s.mode,
		MCPConfigPath:              mcpConfigPath,
		MaxLineBytes:               s.agent.options.runtime.maxJSONLineBytes,
		OnGoroutinePanic:           s.agent.onNativeGoroutinePanic,
		NewProcessSnapshotObserver: s.agent.newProcessSnapshotObserver,
		WritableRoot:               s.settingsDir,
	}
	s.agent.configureNativeClient(&options, kind)

	return amp.NewClient(s.agent.log, options)
}

func (s *agentSession) acquireTurn(ctx context.Context) (func(), error) {
	select {
	case s.turn <- struct{}{}:
		return func() { <-s.turn }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return nil, backpressureError("session_prompt")
	}
}

func (s *agentSession) Cancel(ctx context.Context) error {
	_ = ctx

	state := s.activePromptState()
	if state == nil {
		return nil
	}

	if state.isCancelled() {
		return nil
	}

	state.cancel()

	return s.interruptState(context.Background(), state)
}

func (s *agentSession) interrupt(ctx context.Context) error {
	state := s.activePromptState()
	if state == nil {
		return nil
	}

	return s.interruptState(ctx, state)
}

func (s *agentSession) interruptState(ctx context.Context, state *promptTurnState) error {
	_ = ctx

	if state == nil {
		return nil
	}

	turn := state.currentTurn()
	if turn == nil {
		return nil
	}

	timeout := s.agent.options.runtime.nativeCancelTimeout

	cancelCtx, cancel := context.WithTimeout(context.Background(), timeout+s.agent.options.runtime.nativeCloseTurnWait)
	defer cancel()

	return turn.Interrupt(cancelCtx, timeout)
}

func (s *agentSession) setActivePrompt(state *promptTurnState) {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()

	s.activePrompt = state
}

func (s *agentSession) activePromptState() *promptTurnState {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()

	return s.activePrompt
}

func (s *agentSession) clearActivePrompt(state *promptTurnState) {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()

	if s.activePrompt == state {
		s.activePrompt = nil
	}
}

func (s *agentSession) poison(cause string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.poisonCause = cause

	return acp.NewInternalError(map[string]any{jsonFieldError: cause})
}

func (s *agentSession) Close(ctx context.Context) error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()

	s.closeProviderAuth()

	state := s.activePromptState()
	if state != nil {
		state.cancel()
	}

	err := s.interruptState(context.Background(), state)

	boundaryErr := s.scratchContainmentError()
	if state != nil {
		waitCtx, cancelWait := context.WithTimeout(
			context.WithoutCancel(ctx),
			s.agent.options.runtime.nativeCancelTimeout+2*s.agent.options.runtime.nativeCloseTurnWait,
		)
		completionErr := state.awaitCompletion(waitCtx)

		cancelWait()

		s.recordScratchContainment(completionErr)
		err = errors.Join(err, completionErr)
		boundaryErr = errors.Join(boundaryErr, completionErr)
	}

	return finalizeSessionScratch(err, boundaryErr, s.settingsDir, s.scratchRootRelease)
}

func (s *agentSession) Delete(ctx context.Context) error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()

	s.closeProviderAuth()

	state := s.activePromptState()
	if state != nil {
		state.cancel()
	}

	interruptErr := s.interruptState(context.Background(), state)
	s.recordScratchContainment(interruptErr)

	boundaryErr := errors.Join(s.scratchContainmentError(), interruptErr)
	if state != nil {
		waitCtx, cancelWait := context.WithTimeout(
			context.WithoutCancel(ctx),
			s.agent.options.runtime.nativeCancelTimeout+2*s.agent.options.runtime.nativeCloseTurnWait,
		)
		completionErr := state.awaitCompletion(waitCtx)

		cancelWait()

		s.recordScratchContainment(completionErr)
		interruptErr = errors.Join(interruptErr, completionErr)
		boundaryErr = errors.Join(boundaryErr, completionErr)
	}

	if !amp.ProcessContainmentComplete(boundaryErr) {
		return finalizeSessionScratch(interruptErr, boundaryErr, s.settingsDir, s.scratchRootRelease)
	}

	// A session whose first prompt never ran has no server-side thread, so
	// there is nothing native to delete.
	nativeID := s.nativeSessionID()
	if nativeID == "" {
		return finalizeSessionScratch(interruptErr, boundaryErr, s.settingsDir, s.scratchRootRelease)
	}

	deleteErr := s.client().DeleteThread(ctx, nativeID)
	s.recordScratchContainment(deleteErr)

	return finalizeSessionScratch(errors.Join(interruptErr, deleteErr), errors.Join(boundaryErr, deleteErr), s.settingsDir, s.scratchRootRelease)
}

func finalizeSessionScratch(runtimeErr, boundaryErr error, settingsDir string, scratchRelease func()) error {
	if !amp.ProcessContainmentComplete(boundaryErr) {
		return errors.Join(runtimeErr, boundaryErr)
	}

	var removeErr error
	if settingsDir != "" {
		removeErr = removeSessionDir(settingsDir)
	}

	if removeErr == nil && scratchRelease != nil {
		scratchRelease()
	}

	return errors.Join(runtimeErr, removeErr)
}

func (s *agentSession) recordScratchContainment(err error) {
	if amp.ProcessContainmentComplete(err) {
		return
	}

	s.mu.Lock()
	s.scratchContainmentErr = errors.Join(s.scratchContainmentErr, err)
	s.mu.Unlock()
}

func (s *agentSession) scratchContainmentError() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.scratchContainmentErr
}

func (s *agentSession) verifyContinuable(ctx context.Context) error {
	if boundaryErr := s.scratchContainmentError(); boundaryErr != nil {
		return boundaryErr
	}

	// A session with no native thread yet is trivially continuable: its next
	// prompt creates the thread, so there is nothing server-side to probe.
	nativeID := s.nativeSessionID()
	if nativeID == "" {
		return nil
	}

	timeout := s.agent.options.runtime.nativeCommandTimeout

	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	_, exportErr := s.agent.options.runtime.exportThread(probeCtx, s.client(), nativeID)
	s.recordScratchContainment(exportErr)

	if exportErr != nil {
		if !amp.ProcessContainmentComplete(exportErr) {
			return nativeInternalError(exportErr)
		}

		if isNativeMissingError(exportErr) {
			s.mu.Lock()
			s.nativeMissingCause = exportErr.Error()
			s.mu.Unlock()

			return nil
		}

		return nativeInternalError(exportErr)
	}

	return nil
}

func (s *agentSession) ready() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.poisonCause != "" {
		return acp.NewInternalError(map[string]any{jsonFieldError: s.poisonCause})
	}

	if s.nativeMissingCause != "" {
		return acp.NewInternalError(map[string]any{jsonFieldError: "native_state_missing", keyDetail: s.nativeMissingCause})
	}

	if s.scratchContainmentErr != nil {
		return s.scratchContainmentErr
	}

	if s.closed {
		return errSessionClosed
	}

	return nil
}

func (s *agentSession) manifest() ampManifest {
	s.mu.Lock()
	defer s.mu.Unlock()

	return ampManifest{
		Format:             SessionStoreFormat,
		SessionID:          string(s.id),
		NativeSessionID:    s.nativeID,
		Cwd:                s.cwd,
		Title:              s.title,
		Mode:               s.mode,
		UpdatedAtUnixMilli: s.updatedUnix,
		CreatedAtUnixMilli: s.createdUnix,
	}
}

// nativeSessionID returns the adopted server-side Amp thread id, or "" while
// no thread exists yet.
func (s *agentSession) nativeSessionID() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.nativeID
}

// persistAfterTurn durably commits the manifest plus the full transcript in one
// Replace (X4: the whole load-append-Replace path is serialized per session).
// Per X2, any newly completed frames that fail to persist are retained in memory
// (mirror-unsynced) and re-committed on the next attempt so a store outage after
// a native turn success can never silently drop the turn.
func (s *agentSession) persistAfterTurn(ctx context.Context, transcript []SessionStoreEntry) error {
	s.persistMu.Lock()
	defer s.persistMu.Unlock()

	if s.persistFenced() {
		return nil
	}

	now := time.Now().UnixMilli()

	s.mu.Lock()
	s.updatedUnix = now
	pending := append(cloneEntries(s.unsyncedFrames), cloneEntries(transcript)...)
	s.mu.Unlock()

	if s.agent.store == nil {
		s.mu.Lock()
		s.transcriptFrames += len(pending)
		s.unsyncedFrames = nil
		s.mu.Unlock()

		return nil
	}

	loadCtx, cancelLoad := s.agent.sessionStoreLoadContext(ctx)
	fullTranscript, err := s.agent.store.Load(loadCtx, SessionKey{SessionID: string(s.id), Subpath: transcriptSubpath})

	cancelLoad()

	if err != nil {
		s.retainUnsynced(pending)

		return err
	}

	s.mu.Lock()
	persistedFrames := s.transcriptFrames
	s.mu.Unlock()

	if len(fullTranscript) != persistedFrames {
		s.retainUnsynced(pending)

		return acp.NewInternalError(map[string]any{
			jsonFieldError: "amp transcript frame count drift",
			"got":          len(fullTranscript),
			"want":         persistedFrames,
		})
	}

	if len(pending) > 0 {
		fullTranscript = append(cloneEntries(fullTranscript), pending...)
	}

	main, _ := json.Marshal(s.manifest())

	artifactReplacements, err := s.imageArtifactReplacements(ctx)
	if err != nil {
		s.retainUnsynced(pending)

		return acp.NewInternalError(map[string]any{jsonFieldError: err.Error()})
	}

	replacements := make([]SessionStoreReplacement, 0, 2+len(artifactReplacements))
	replacements = append(replacements,
		SessionStoreReplacement{
			Key:     SessionKey{SessionID: string(s.id), Subpath: SessionStoreMainSubpath},
			Entries: []SessionStoreEntry{main},
		},
		SessionStoreReplacement{
			Key:     SessionKey{SessionID: string(s.id), Subpath: transcriptSubpath},
			Entries: fullTranscript,
		},
	)
	replacements = append(replacements, artifactReplacements...)

	replaceCtx, cancelReplace := s.agent.sessionStoreWriteContext(ctx)
	defer cancelReplace()

	if err := s.agent.store.Replace(
		replaceCtx,
		SessionKey{SessionID: string(s.id), Subpath: SessionStoreMainSubpath},
		replacements,
	); err != nil {
		s.retainUnsynced(pending)

		return err
	}

	s.mu.Lock()
	s.unsyncedFrames = nil
	s.transcriptFrames = len(fullTranscript)
	s.mu.Unlock()

	return nil
}

func (s *agentSession) transcriptFrameCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.transcriptFrames
}

func (s *agentSession) setTranscriptFrameCount(count int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.transcriptFrames = count
}

// recordVacancy retains what the prompt's containment boundary proved, so the
// next incarnation's snapshot states a quiescence fact backed by evidence rather
// than by the configuration's advertisement alone.
func (s *agentSession) recordVacancy(vacant bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.vacancyUnproven = !vacant
}

// vacancyProven reports that nothing this session owns can still be live.
func (s *agentSession) vacancyProven() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return !s.vacancyUnproven
}

// fencePersistence stops this session's durable writes and waits out the one
// already in flight. A delete tombstones the row, and Replace clears the
// tombstone of every key it lists, so a commit that landed after the tombstone
// would durably resurrect a session the host was told is gone. Taking the same
// lock the commit holds is what makes the tombstone the last write to the row.
func (s *agentSession) fencePersistence() {
	s.persistMu.Lock()
	defer s.persistMu.Unlock()

	s.mu.Lock()
	s.persistForbidden = true
	s.mu.Unlock()
}

// resumePersistence lifts the fence. A delete whose tombstone never landed left
// the session where it was, and a session the host still owns commits its next
// turn.
func (s *agentSession) resumePersistence() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.persistForbidden = false
}

// persistFenced reports that this session's row is being deleted, so nothing it
// still holds may be written back.
func (s *agentSession) persistFenced() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.persistForbidden
}

// retainUnsynced marks the mirror as unsynced by keeping the exact frames that
// failed to persist so they can be retried verbatim.
func (s *agentSession) retainUnsynced(pending []SessionStoreEntry) {
	s.mu.Lock()
	s.unsyncedFrames = pending
	s.mu.Unlock()
}

// ensureMirrorSynced blocks a prompt with a loud error whenever the local mirror
// still holds frames from a previously completed turn that failed to persist. It
// retries the durable Replace of the exact frames on each call and only unblocks
// once that retry succeeds (X2).
func (s *agentSession) ensureMirrorSynced(ctx context.Context) error {
	s.mu.Lock()
	unsynced := len(s.unsyncedFrames) > 0
	s.mu.Unlock()

	if !unsynced {
		return nil
	}

	if err := s.persistAfterTurn(ctx, nil); err != nil {
		return acp.NewInternalError(map[string]any{jsonFieldError: "mirror_unsynced", keyDetail: err.Error()})
	}

	return nil
}
