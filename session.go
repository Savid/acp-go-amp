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

	valUnsupported              = "unsupported"
	valNoTransport              = "no_transport"
	valText                     = "text"
	valImage                    = "image"
	valBase64                   = "base64"
	valHTTP                     = "http"
	valHTTPS                    = "https"
	valUser                     = "user"
	valRequired                 = "required"
	valDuplicate                = "duplicate"
	valAmbiguous                = "ambiguous"
	valMismatch                 = "mismatch"
	reasonUnserializable        = "unserializable"
	deleteOwnershipChanged      = "delete ownership changed"
	wrapperOwnershipChanged     = "session wrapper ownership changed"
	persistenceOwnershipChanged = "persistence ownership changed"

	modeLow    = "low"
	modeMedium = "medium"
	modeHigh   = "high"
	modeUltra  = "ultra"
)

var (
	errSessionClosed     = errors.New("session closed")
	errPersistenceFenced = errors.New("session persistence fenced")
	errNativeDeleteOpen  = errors.New("native session deletion unresolved")
	writeFile            = os.WriteFile
	readFile             = os.ReadFile
	mkdirAll             = os.MkdirAll
	mkdirTemp            = os.MkdirTemp
	removeSessionDir     = os.RemoveAll
)

func closedCallbackRefusal() error {
	return errors.Join(
		errSessionClosed,
		acp.NewInvalidRequest(map[string]any{jsonFieldError: "session_closed"}),
	)
}

func persistenceFencedError() error {
	return errors.Join(
		errPersistenceFenced,
		acp.NewInvalidRequest(map[string]any{jsonFieldError: "session_closed"}),
	)
}

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
	mcpConfigFile         string
	nativeTreePrepared    bool
	nativeTreeOpaque      bool
	scratchRootRelease    func()
	closed                bool
	poisonCause           string
	nativeMissingCause    string
	scratchContainmentErr error
	unsyncedFrames        []SessionStoreEntry
	mirrorUnsynced        bool
	transcriptFrames      int
	persistenceCommit     *sessionPersistenceCommit
	// vacancyUnproven records that a completed prompt boundary failed to
	// enumerate an empty contained descendant set. A session that has never
	// prompted started no process at all, so it opens with nothing outstanding.
	vacancyUnproven    bool
	persistState       sessionPersistenceState
	turn               chan struct{}
	cancelMu           sync.Mutex
	activePrompt       *promptTurnState
	persistMu          sync.Mutex
	persistFlight      *sessionPersistenceFlight
	persistGeneration  uint64
	persistFenceGen    uint64
	teardownMu         sync.Mutex
	teardownFlight     *sessionTeardownFlight
	teardownGeneration uint64
	nativeDeleteDone   bool
	nativeDeleteErr    error
	deleteDone         bool
	scratchDone        bool
	closeBoundaryDone  bool
	closeBoundary      closeSettlement
	closeCommitDone    bool
	pendingTerminal    *promptTerminalDelivery
	promptSettlement   promptSettlement
	mu                 sync.Mutex
}

type sessionPersistenceFlight struct {
	generation uint64
	kind       sessionPersistenceKind
	done       chan struct{}
}

// sessionPersistenceCommit is the immutable identity of one Replace attempt.
// It is published before the callback is invoked and retained across every
// ambiguous return, including a delegate that commits and then panics. A retry
// replays these exact replacements instead of rebuilding from a stale local
// frame count.
type sessionPersistenceCommit struct {
	pending      []SessionStoreEntry
	successor    []SessionStoreEntry
	replacements []SessionStoreReplacement
	targetFrames int
}

type sessionPersistenceState uint8

const (
	sessionPersistenceOpen sessionPersistenceState = iota
	sessionPersistenceClosing
	sessionPersistenceDeleting
)

type sessionPersistenceKind uint8

const (
	sessionPersistenceOrdinary sessionPersistenceKind = iota + 1
	sessionPersistenceCloseRetry
)

type sessionPersistenceFence struct {
	previous   sessionPersistenceState
	installed  sessionPersistenceState
	generation uint64
	changed    bool
}

type sessionTeardownFlight struct {
	generation uint64
	done       chan struct{}
	panicErr   error
	waiters    int
}

func newAgentSession(ctx context.Context, agent *Agent, id acp.SessionId, cwd string, meta parsedSessionMeta, mcpConfigJSON string, additionalDirs []string) (_ *agentSession, err error) {
	if validateErr := validateSessionID(string(id)); validateErr != nil {
		return nil, fmt.Errorf("invalid amp session id: %w", validateErr)
	}

	now := time.Now().UnixMilli()

	session := &agentSession{
		agent:              agent,
		id:                 id,
		scratchRootRelease: func() {},
		turn:               make(chan struct{}, 1),
	}
	agent.retainCleanupOwner(id, session, agentCleanupConstructing)

	constructed := false
	defer func() {
		if constructed {
			return
		}

		cleanupErr := session.finalizeScratch(nil, nil)
		if cleanupErr == nil && !session.retainsOpaqueTree() {
			agent.clearCleanupOwner(id, session)
		}

		err = errors.Join(err, cleanupErr)
	}()

	parent, err := agent.ensureScratchParent()
	if err != nil {
		return nil, err
	}

	dir, err := mkdirTemp(parent, "acp-go-amp-session-*")
	if err != nil {
		return nil, fmt.Errorf("create amp settings dir: %w", err)
	}

	session.settingsDir = dir

	homeDir := filepath.Join(dir, "home")
	configDir := filepath.Join(dir, "xdg-config")
	cacheDir := filepath.Join(dir, "xdg-cache")
	dataDir := filepath.Join(dir, "xdg-data")

	stateDir := filepath.Join(dir, "xdg-state")
	for _, path := range []string{homeDir, configDir, cacheDir, dataDir, stateDir, filepath.Join(configDir, "amp")} {
		if mkdirErr := mkdirAll(path, 0o700); mkdirErr != nil {
			return nil, fmt.Errorf("create amp isolated home: %w", mkdirErr)
		}
	}

	// The settings file the wrapper points amp at is the only place the native
	// secrets flag resolves from: it is read at global scope, which no managed
	// file intercepts. Asserting it false keeps the credential in the isolated
	// per-session file store, because the keystore item it would otherwise move
	// to is keyed by hostname alone and shared by every session on the machine.
	settingsFile := filepath.Join(configDir, "amp", "settings.json")
	if writeErr := writeFile(settingsFile, amp.AuthSettingsDocument(), 0o600); writeErr != nil {
		return nil, fmt.Errorf("write amp settings file: %w", writeErr)
	}

	if seedErr := writeSeedFiles(homeDir, agent.options.SeedFiles); seedErr != nil {
		return nil, seedErr
	}

	mcpFile := filepath.Join(dir, "mcp.json")

	mcpDocument := mcpConfigJSON
	if mcpDocument == "" {
		mcpDocument = "{}\n"
	}

	if writeErr := writeFile(mcpFile, []byte(mcpDocument), 0o600); writeErr != nil {
		return nil, fmt.Errorf("write amp MCP config: %w", writeErr)
	}

	session.nativeTreeOpaque = agent.options.hostAuthoritySupplied
	if err := agent.prepareNativeTree(ctx, dir); err != nil {
		return nil, fmt.Errorf("prepare amp session residence: %w", err)
	}

	session.nativeTreePrepared = agent.options.hostAuthoritySupplied
	session.nativeTreeOpaque = false

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

	session.cwd = cwd
	session.mode = mode
	session.createdUnix = now
	session.updatedUnix = now

	session.additionalDirectories = append([]string(nil), additionalDirs...)
	session.mcpConfigJSON = mcpConfigJSON
	session.env = env
	session.operationEnv = operationEnv
	session.rawEvents = meta.rawEvent
	session.settingsFile = settingsFile
	session.mcpConfigFile = mcpFile
	agent.retainCleanupOwner(id, session, agentCleanupPrepared)

	constructed = true

	return session, nil
}

func (s *agentSession) retainsOpaqueTree() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.nativeTreeOpaque
}

// client is the session's non-prompt client: thread export, thread delete, and
// account login run on the static operation environment.
func (s *agentSession) client() *amp.Client {
	return s.clientWithEnv(s.operationEnv, "")
}

func (s *agentSession) clientWithEnv(env map[string]string, mcpConfigPath string) *amp.Client {
	options := amp.Options{
		CLIPath:            s.agent.options.ExecutablePath,
		Cwd:                s.cwd,
		SettingsFile:       s.settingsFile,
		Env:                env,
		ResolutionEnv:      composeEnv(s.agent.options.Env),
		ResolvedExecutable: s.agent.retainedHarnessPath(),
		Mode:               s.mode,
		MCPConfigPath:      mcpConfigPath,
		MaxLineBytes:       s.agent.options.runtime.maxJSONLineBytes,
		OnGoroutinePanic:   s.agent.onNativeGoroutinePanic,
		WritableRoot:       s.settingsDir,
	}
	s.agent.configureNativeClient(&options)

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
	if state == nil {
		return nil
	}

	turn := state.currentTurn()
	if turn == nil {
		return nil
	}

	timeout := s.agent.options.runtime.nativeCancelTimeout
	interruptCtx := context.WithoutCancel(ctx)
	interruptCtx = withCallbackProvenance(interruptCtx, s.agent, state)

	cancelCtx, cancel := context.WithTimeout(interruptCtx, timeout+s.agent.options.runtime.nativeCloseTurnWait)
	defer cancel()

	return turn.Interrupt(cancelCtx)
}

// admitPrompt publishes one prompt as the session's active turn. Admission takes
// the same lock a close or delete takes to fence one, and it re-reads the
// closure and delete fences under it, so the two orders are one linearization: a
// prompt is admitted before any teardown observes an empty slot, or it is
// refused and launches nothing.
func (s *agentSession) admitPrompt(state *promptTurnState) error {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()

	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	// A session fenced for delete keeps nothing it writes, so a turn admitted
	// here would stream frames its own commit is forbidden to take.
	fenced := s.persistenceForbidden()

	if closed || fenced {
		return errSessionClosed
	}

	s.activePrompt = state

	return nil
}

// fenceAdmission closes the session to new prompts and reports the one already
// admitted, both under the lock admission takes. Whatever it returns is the
// complete set of native work this teardown must wait out.
func (s *agentSession) fenceAdmission() *promptTurnState {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()

	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()

	return s.activePrompt
}

// rollbackDeleteAdmission reopens an active wrapper only after a delete failed
// before its tombstone landed. The agent's per-ID flight still owns the wrapper
// while this runs, so no close or second delete can race the rollback.
func (s *agentSession) rollbackDeleteAdmission() {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()

	s.mu.Lock()
	s.closed = false
	s.mu.Unlock()
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

// closeSettlement is what a close proved before anything is reclaimed: the error
// the teardown itself produced and the containment boundary the session ends on.
type closeSettlement struct {
	runtimeErr  error
	boundaryErr error
}

// settleClose runs every rung of a close that must complete while the session is
// still whole: the admission fence, the native cancel and interrupt, the wait for
// the prompt already admitted, and the containment re-read.
//
// It reclaims nothing. What a settled close still owes — the durable rung and the
// scratch removal — belongs to the caller, because only a caller that still holds
// the session's slot can leave it addressable when the rung fails.
func (s *agentSession) settleClose(ctx context.Context) closeSettlement {
	state := s.fenceAdmission()

	s.closeProviderAuth()

	if state != nil {
		state.cancel()
	}

	err := s.interruptState(context.Background(), state)
	completion := s.retainedPromptSettlement()

	if state != nil {
		waitCtx, cancelWait := context.WithTimeout(
			context.WithoutCancel(ctx),
			s.agent.options.runtime.nativeCancelTimeout+2*s.agent.options.runtime.nativeCloseTurnWait,
		)
		completion = state.awaitSettlement(waitCtx)

		cancelWait()
	}

	s.recordScratchContainment(completion.containmentErr)

	err = errors.Join(err, completion.containmentErr)
	if completion.deliveryErr != nil && !s.hasPendingTerminal() {
		err = errors.Join(err, completion.deliveryErr)
	}
	// The boundary is re-read after the wait, not before it: a prompt that lost
	// containment records it while this close is already waiting, and a close
	// judging the session on the reading it took first would remove scratch state
	// a surviving tree still runs against.
	boundaryErr := errors.Join(s.scratchContainmentError(), completion.containmentErr)

	return closeSettlement{runtimeErr: err, boundaryErr: boundaryErr}
}

func (s *agentSession) settleCloseRung(ctx context.Context) closeSettlement {
	s.mu.Lock()
	if s.closeBoundaryDone {
		settlement := s.closeBoundary
		s.mu.Unlock()

		return settlement
	}
	s.mu.Unlock()

	settlement := s.settleClose(ctx)
	if settlement.runtimeErr == nil && amp.ProcessContainmentComplete(settlement.boundaryErr) {
		s.mu.Lock()
		s.closeBoundaryDone = true
		s.closeBoundary = settlement
		s.mu.Unlock()
	}

	return settlement
}

// commitOnClose is the close ladder's durable rung. A settlement whose Replace
// failed keeps its frames mirror-unsynced rather than dropping them, and the
// close is the last request that can still write them, because the session it
// reclaims takes them with it. The retry runs on a context detached from the
// request's and bounded by the store's own read and write bounds, so a host that
// walked away from the close does not decide whether the frames land.
//
// A session fenced for delete commits nothing here: the retry is a no-op on it,
// because Replace clears the tombstone of every key it lists and a commit landing
// after the tombstone would durably resurrect the row.
func (s *agentSession) commitOnClose(ctx context.Context) error {
	commitCtx, cancelCommit := context.WithTimeout(
		context.WithoutCancel(ctx),
		s.agent.sessionStoreLoadTimeout()+sessionStoreWriteTimeout,
	)
	defer cancelCommit()

	return s.ensureMirrorSyncedForClose(commitCtx)
}

func (s *agentSession) commitCloseRung(ctx context.Context) error {
	s.mu.Lock()
	done := s.closeCommitDone
	s.mu.Unlock()

	if done {
		return nil
	}

	if err := s.commitOnClose(ctx); err != nil {
		return err
	}

	s.mu.Lock()
	s.closeCommitDone = true
	s.promptSettlement.commitErr = nil
	s.mu.Unlock()

	return nil
}

// Close settles the session and removes its scratch state. It carries no durable
// rung, because every caller of it is a path where nothing durable is owed: a
// session whose admission was refused never wrote a row, and a delete that found
// no stored row is fencing writes rather than making them.
func (s *agentSession) Close(ctx context.Context) (err error) {
	ctx, flight, err := s.beginTeardown(ctx)
	if err != nil {
		return err
	}
	defer s.finishTeardownOnReturn(flight)

	settlement := s.settleCloseRung(ctx)
	if fenceErr := s.fencePersistenceForClose(ctx); fenceErr != nil {
		return errors.Join(settlement.runtimeErr, fenceErr)
	}

	if terminalErr := s.deliverPendingTerminal(ctx); terminalErr != nil {
		return errors.Join(settlement.runtimeErr, terminalErr)
	}

	return s.finalizeScratch(settlement.runtimeErr, settlement.boundaryErr)
}

// closeAtShutdown is the ladder embedded shutdown runs. It carries the same
// fail-closed durable rung a wire `session/close` does — an Agent.Close that owes
// a commit a wire close would have made makes it, rather than dropping the
// session's retained frames with the wrapper.
//
// Scratch bookkeeping runs even when the commit fails. Agent.Close is the last
// owner: it reports any rung it cannot complete and then releases every local
// ownership path rather than depending on a later shutdown call.
func (s *agentSession) closeAtShutdown(ctx context.Context) (err error) {
	ctx, flight, err := s.beginTeardown(ctx)
	if err != nil {
		return err
	}
	defer s.finishTeardownOnReturn(flight)

	settlement := s.settleCloseRung(ctx)
	if fenceErr := s.fencePersistenceForClose(ctx); fenceErr != nil {
		return errors.Join(settlement.runtimeErr, fenceErr)
	}

	// Same order a prompt and a wire close settle in: the containment boundary is
	// proven before anything durable is written, so a shutdown that ends on an
	// unproven boundary commits nothing.
	var commitErr error
	if amp.ProcessContainmentComplete(settlement.boundaryErr) {
		commitErr = s.commitCloseRung(ctx)
	}

	var terminalErr error
	if commitErr == nil && amp.ProcessContainmentComplete(settlement.boundaryErr) {
		terminalErr = s.deliverPendingTerminal(ctx)
	}

	closeErr := errors.Join(
		s.finalizeScratch(settlement.runtimeErr, settlement.boundaryErr),
		commitErr,
		terminalErr,
	)

	return closeErr
}

func (s *agentSession) shutdownComplete() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.closeBoundaryDone && s.closeCommitDone && s.scratchDone && s.pendingTerminal == nil
}

func (s *agentSession) Delete(ctx context.Context) (err error) {
	ctx, flight, err := s.beginTeardown(ctx)
	if err != nil {
		return err
	}
	defer s.finishTeardownOnReturn(flight)

	s.mu.Lock()
	done := s.deleteDone
	s.mu.Unlock()

	if done {
		return nil
	}

	err = s.deleteOwned(ctx)
	if err == nil {
		s.mu.Lock()
		s.deleteDone = true
		s.mu.Unlock()
	}

	return err
}

func (s *agentSession) deleteOwned(ctx context.Context) error {
	state := s.fenceAdmission()

	s.closeProviderAuth()

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
		completion := state.awaitSettlement(waitCtx)

		cancelWait()

		s.recordScratchContainment(completion.containmentErr)
		interruptErr = errors.Join(interruptErr, completion.containmentErr, completion.deliveryErr)
		// Re-read after the wait for the same reason a close does: the boundary a
		// settling prompt lost is recorded while this delete is already waiting.
		boundaryErr = errors.Join(s.scratchContainmentError(), completion.containmentErr)
	}

	if !amp.ProcessContainmentComplete(boundaryErr) {
		return errors.Join(interruptErr, boundaryErr)
	}

	if !s.dischargeDeletedUnsyncedTerminal() {
		if terminalErr := s.deliverPendingTerminal(ctx); terminalErr != nil {
			return errors.Join(interruptErr, terminalErr)
		}
	}

	s.mu.Lock()
	nativeDeleteDone := s.nativeDeleteDone
	nativeDeleteErr := s.nativeDeleteErr
	s.mu.Unlock()

	if nativeDeleteErr != nil {
		return errors.Join(interruptErr, nativeDeleteErr)
	}

	if !nativeDeleteDone {
		// A session whose first prompt never ran has no server-side thread, so
		// there is nothing native to delete.
		nativeID := s.nativeSessionID()
		if nativeID != "" {
			deleteErr := s.client().DeleteThread(ctx, nativeID)
			s.recordScratchContainment(deleteErr)

			if deleteErr != nil {
				return errors.Join(interruptErr, deleteErr)
			}
		}

		s.mu.Lock()
		s.nativeDeleteDone = true
		s.mu.Unlock()
	}

	// A terminal-delivery or native-interrupt failure keeps the exact wrapper
	// internally owned even when the server-side thread delete succeeded. A later
	// retry re-evaluates the boundary and releases these resources once, without
	// issuing a second native delete.
	if interruptErr != nil {
		return interruptErr
	}

	return s.finalizeScratch(nil, boundaryErr)
}

// deleteAtShutdown gives a tombstoned wrapper its final cleanup attempt. Native
// deletion may be retried while the isolated operation state is intact. If that
// retry still fails, shutdown reports the permanent classified failure and
// releases the local state; no later shutdown can safely issue the command from
// a home it already released.
func (s *agentSession) deleteAtShutdown(ctx context.Context) error {
	deleteErr := s.Delete(ctx)
	if deleteErr == nil {
		return nil
	}

	_, flight, flightErr := s.beginTeardown(ctx)
	if flightErr != nil {
		return errors.Join(deleteErr, flightErr)
	}
	defer s.finishTeardownOnReturn(flight)

	s.mu.Lock()
	done := s.deleteDone
	s.mu.Unlock()

	if done {
		return deleteErr
	}

	boundaryErr := s.scratchContainmentError()
	if !amp.ProcessContainmentComplete(boundaryErr) {
		return errors.Join(deleteErr, boundaryErr)
	}

	s.mu.Lock()
	if !s.nativeDeleteDone && s.nativeID != "" && s.nativeDeleteErr == nil {
		s.nativeDeleteErr = errors.Join(errNativeDeleteOpen, deleteErr)
	}

	permanentErr := s.nativeDeleteErr
	s.mu.Unlock()

	cleanupErr := s.finalizeScratch(nil, boundaryErr)
	if cleanupErr == nil {
		s.mu.Lock()
		if s.nativeDeleteDone {
			s.deleteDone = true
		}
		s.mu.Unlock()
	}

	return errors.Join(deleteErr, permanentErr, cleanupErr)
}

func (s *agentSession) deleteComplete() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.deleteDone
}

// beginTeardown serializes exact-wrapper teardown without carrying teardownMu
// across native calls, store callbacks, filesystem hooks, or other host code.
func (s *agentSession) beginTeardown(ctx context.Context) (context.Context, *sessionTeardownFlight, error) {
	for {
		s.teardownMu.Lock()
		if s.teardownFlight == nil {
			if s.contextOwnsTeardownDependency(ctx, nil) {
				s.teardownMu.Unlock()

				return nil, nil, closedCallbackRefusal()
			}

			s.teardownGeneration++
			flight := &sessionTeardownFlight{generation: s.teardownGeneration, done: make(chan struct{})}
			s.teardownFlight = flight
			s.teardownMu.Unlock()

			flightCtx := withCallbackProvenance(ctx, s.agent, flight)

			return withCallbackSessionScope(flightCtx, s.agent, s.id), flight, nil
		}

		existing := s.teardownFlight
		wait := existing.done
		s.teardownMu.Unlock()

		if s.contextOwnsTeardownDependency(ctx, existing) {
			return nil, nil, closedCallbackRefusal()
		}

		s.teardownMu.Lock()
		existing.waiters++
		s.teardownMu.Unlock()

		select {
		case <-wait:
			if existing.panicErr != nil {
				return nil, nil, existing.panicErr
			}
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
	}
}

func (s *agentSession) contextOwnsTeardownDependency(ctx context.Context, flight *sessionTeardownFlight) bool {
	if flight != nil && contextOwnsCallbackGeneration(ctx, s.agent, flight) {
		return true
	}

	s.persistMu.Lock()
	persistence := s.persistFlight
	s.persistMu.Unlock()

	if persistence != nil && contextOwnsCallbackGeneration(ctx, s.agent, persistence) {
		return true
	}

	s.cancelMu.Lock()
	prompt := s.activePrompt
	s.cancelMu.Unlock()

	return prompt != nil && contextOwnsCallbackGeneration(ctx, s.agent, prompt)
}

func (s *agentSession) finishTeardown(flight *sessionTeardownFlight) {
	s.teardownMu.Lock()
	if s.teardownFlight == flight && s.teardownFlight.generation == flight.generation {
		s.teardownFlight = nil

		close(flight.done)
	}
	s.teardownMu.Unlock()
}

func (s *agentSession) finishTeardownWithPanic(flight *sessionTeardownFlight, panicErr error) {
	if flight == nil {
		return
	}

	s.teardownMu.Lock()
	if s.teardownFlight == flight && s.teardownFlight.generation == flight.generation {
		flight.panicErr = panicErr
		s.teardownFlight = nil

		close(flight.done)
	}
	s.teardownMu.Unlock()
}

func (s *agentSession) finishTeardownOnReturn(flight *sessionTeardownFlight) {
	if recovered := recover(); recovered != nil {
		s.finishTeardownWithPanic(flight, closedCallbackRefusal())

		panic(recovered)
	}

	s.finishTeardown(flight)
}

// finalizeScratch runs only for the current teardown flight. It is idempotent
// across retries while leaving a failed removal attached to the exact wrapper.
func (s *agentSession) finalizeScratch(runtimeErr, boundaryErr error) error {
	if !amp.ProcessContainmentComplete(boundaryErr) {
		return errors.Join(runtimeErr, boundaryErr)
	}

	s.mu.Lock()
	done := s.scratchDone
	s.mu.Unlock()

	if done {
		return runtimeErr
	}

	s.mu.Lock()
	prepared := s.nativeTreePrepared
	opaque := s.nativeTreeOpaque
	root := s.settingsDir
	s.mu.Unlock()

	if opaque {
		return errors.Join(runtimeErr, boundaryErr, ErrContainmentIncomplete)
	}

	if prepared {
		reclaimCtx, cancel := context.WithTimeout(context.WithoutCancel(context.Background()), defaultNativeCommandTimeout)
		reclaimErr := s.agent.reclaimNativeTree(reclaimCtx, root)

		cancel()

		if reclaimErr != nil {
			return errors.Join(runtimeErr, reclaimErr)
		}

		s.mu.Lock()
		s.nativeTreePrepared = false
		s.mu.Unlock()
	}

	var removeErr error
	if root != "" {
		removeErr = removeSessionDir(root)
	}

	if removeErr == nil {
		s.mu.Lock()
		s.scratchDone = true
		release := s.scratchRootRelease
		s.scratchRootRelease = nil
		s.mu.Unlock()

		if release != nil {
			release()
		}
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

	probeCtx = withExactCallbackGeneration(probeCtx, "native:export_thread")
	_, exportErr := invokeOwnedPair(func() (json.RawMessage, error) {
		return s.agent.options.runtime.exportThread(probeCtx, s.client(), nativeID)
	})
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

	if s.pendingTerminal != nil {
		return acp.NewInternalError(map[string]any{jsonFieldError: "terminal lifecycle delivery pending"})
	}

	if err := errors.Join(s.promptSettlement.containmentErr, s.promptSettlement.deliveryErr); err != nil {
		return err
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
	return s.persistAfterTurnKind(ctx, transcript, sessionPersistenceOrdinary)
}

func (s *agentSession) persistAfterTurnKind(ctx context.Context, transcript []SessionStoreEntry, kind sessionPersistenceKind) error {
	persistCtx, flight, err := s.beginPersistence(ctx, kind)
	if err != nil {
		if errors.Is(err, errPersistenceFenced) {
			s.mu.Lock()
			pending := append(cloneEntries(s.unsyncedFrames), cloneEntries(transcript)...)
			s.mu.Unlock()
			s.retainUnsynced(pending)
		}

		return err
	}
	defer s.finishPersistence(flight)

	return s.persistOwned(persistCtx, flight, transcript)
}

func (s *agentSession) persistOwned(ctx context.Context, flight *sessionPersistenceFlight, transcript []SessionStoreEntry) error {
	if !s.persistenceFlightCurrent(flight) {
		return acp.NewInternalError(map[string]any{jsonFieldError: persistenceOwnershipChanged})
	}

	s.mu.Lock()
	commit := s.persistenceCommit

	pending := append(cloneEntries(s.unsyncedFrames), cloneEntries(transcript)...)
	if commit != nil {
		if len(transcript) != 0 {
			commit.successor = append(commit.successor, cloneEntries(transcript)...)
		}

		pending = append(cloneEntries(commit.pending), cloneEntries(commit.successor)...)
	}
	s.mu.Unlock()

	committed := false
	defer func() {
		if !committed {
			s.retainUnsynced(pending)
		}
	}()

	if s.agent.store == nil {
		s.mu.Lock()
		s.transcriptFrames += len(pending)
		s.unsyncedFrames = nil
		s.mirrorUnsynced = false
		s.mu.Unlock()

		committed = true

		return nil
	}

	if commit != nil {
		err := s.replacePersistenceCommit(ctx, flight, commit)
		if err != nil {
			return err
		}

		if len(commit.successor) == 0 {
			committed = true

			return nil
		}

		// The exact earlier generation is now durable. The current settlement
		// owns a distinct successor generation containing only its new frames.
		pending = cloneEntries(commit.successor)
		commit = nil
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

	s.mu.Lock()
	s.updatedUnix = time.Now().UnixMilli()
	s.mu.Unlock()

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
	commit = &sessionPersistenceCommit{
		pending:      cloneEntries(pending),
		replacements: cloneSessionStoreReplacements(replacements),
		targetFrames: len(fullTranscript),
	}

	s.mu.Lock()
	s.persistenceCommit = commit
	s.mu.Unlock()

	err = s.replacePersistenceCommit(ctx, flight, commit)
	committed = err == nil

	return err
}

func (s *agentSession) replacePersistenceCommit(ctx context.Context, flight *sessionPersistenceFlight, commit *sessionPersistenceCommit) error {
	replaceCtx, cancelReplace := s.agent.sessionStoreWriteContext(ctx)
	defer cancelReplace()

	if beforeReplace := s.agent.options.runtime.beforePersistenceReplace; beforeReplace != nil {
		beforeReplace()
	}

	if err := s.agent.store.Replace(
		replaceCtx,
		SessionKey{SessionID: string(s.id), Subpath: SessionStoreMainSubpath},
		cloneSessionStoreReplacements(commit.replacements),
	); err != nil {
		return err
	}

	if !s.persistenceFlightCurrent(flight) {
		return acp.NewInternalError(map[string]any{jsonFieldError: persistenceOwnershipChanged})
	}

	s.mu.Lock()
	if s.persistenceCommit != commit {
		s.mu.Unlock()

		return acp.NewInternalError(map[string]any{jsonFieldError: persistenceOwnershipChanged})
	}

	s.persistenceCommit = nil
	s.unsyncedFrames = nil
	s.mirrorUnsynced = false
	s.transcriptFrames = commit.targetFrames
	s.mu.Unlock()

	return nil
}

func cloneSessionStoreReplacements(replacements []SessionStoreReplacement) []SessionStoreReplacement {
	cloned := make([]SessionStoreReplacement, len(replacements))
	for index, replacement := range replacements {
		cloned[index] = SessionStoreReplacement{Key: replacement.Key, Entries: cloneEntries(replacement.Entries)}
	}

	return cloned
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

func (s *agentSession) fencePersistenceForClose(ctx context.Context) error {
	_, err := s.installPersistenceFence(ctx, sessionPersistenceClosing)

	return err
}

func (s *agentSession) fencePersistenceForDelete(ctx context.Context) error {
	_, err := s.installPersistenceFence(ctx, sessionPersistenceDeleting)

	return err
}

func (s *agentSession) fencePersistenceForDeleteRollback(ctx context.Context) (sessionPersistenceFence, error) {
	return s.installPersistenceFence(ctx, sessionPersistenceDeleting)
}

func (s *agentSession) fencePersistence() {
	_ = s.fencePersistenceForDelete(context.Background())
}

// fencePersistence publishes an admission fence and joins the exact ordinary
// write generation that was already admitted. A callback carrying that same
// generation is refused before publication, so it can never wait on itself.
func (s *agentSession) installPersistenceFence(ctx context.Context, state sessionPersistenceState) (sessionPersistenceFence, error) {
	s.persistMu.Lock()

	flight := s.persistFlight
	if flight != nil && contextOwnsCallbackGeneration(ctx, s.agent, flight) {
		s.persistMu.Unlock()

		return sessionPersistenceFence{}, closedCallbackRefusal()
	}

	fence := sessionPersistenceFence{previous: s.persistState, installed: state}
	if state > s.persistState {
		s.persistState = state
		s.persistFenceGen++
		fence.generation = s.persistFenceGen
		fence.changed = true
	}
	s.persistMu.Unlock()

	if flight == nil {
		return fence, nil
	}

	select {
	case <-flight.done:
		return fence, nil
	case <-ctx.Done():
		return fence, ctx.Err()
	}
}

func (s *agentSession) rollbackPersistenceFence(fence sessionPersistenceFence) {
	if !fence.changed {
		return
	}

	s.persistMu.Lock()
	if s.persistState == fence.installed && s.persistFenceGen == fence.generation {
		s.persistState = fence.previous
		s.persistFenceGen++
	}
	s.persistMu.Unlock()
}

// resumePersistence lifts the fence. A delete whose tombstone never landed left
// the session where it was, and a session the host still owns commits its next
// turn.
func (s *agentSession) resumePersistence() {
	s.persistMu.Lock()
	defer s.persistMu.Unlock()

	if s.persistState == sessionPersistenceDeleting {
		s.persistState = sessionPersistenceOpen
		s.persistFenceGen++
	}
}

func (s *agentSession) persistenceForbidden() bool {
	s.persistMu.Lock()
	defer s.persistMu.Unlock()

	return s.persistState != sessionPersistenceOpen
}

// beginPersistence grants one exact write generation. The mutex protects only
// publication and validation; SessionStore callbacks run after it is released.
func (s *agentSession) beginPersistence(ctx context.Context, kind sessionPersistenceKind) (context.Context, *sessionPersistenceFlight, error) {
	for {
		s.persistMu.Lock()
		if s.persistState == sessionPersistenceDeleting ||
			(s.persistState == sessionPersistenceClosing && kind != sessionPersistenceCloseRetry) {
			s.persistMu.Unlock()

			return nil, nil, persistenceFencedError()
		}

		if s.persistFlight == nil {
			s.persistGeneration++
			flight := &sessionPersistenceFlight{generation: s.persistGeneration, kind: kind, done: make(chan struct{})}
			s.persistFlight = flight
			s.persistMu.Unlock()

			flightCtx := withCallbackProvenance(ctx, s.agent, flight)

			return withCallbackSessionScope(flightCtx, s.agent, s.id), flight, nil
		}

		existing := s.persistFlight
		wait := existing.done
		s.persistMu.Unlock()

		if contextOwnsCallbackGeneration(ctx, s.agent, existing) {
			return nil, nil, closedCallbackRefusal()
		}

		select {
		case <-wait:
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
	}
}

func (s *agentSession) finishPersistence(flight *sessionPersistenceFlight) {
	s.persistMu.Lock()
	if s.persistFlight == flight && s.persistFlight.generation == flight.generation {
		s.persistFlight = nil

		close(flight.done)
	}
	s.persistMu.Unlock()
}

func (s *agentSession) persistenceFlightCurrent(flight *sessionPersistenceFlight) bool {
	s.persistMu.Lock()
	defer s.persistMu.Unlock()

	return flight != nil && s.persistFlight == flight && s.persistFlight.generation == flight.generation
}

// retainUnsynced marks the mirror as unsynced by keeping the exact frames that
// failed to persist so they can be retried verbatim.
func (s *agentSession) retainUnsynced(pending []SessionStoreEntry) {
	s.mu.Lock()
	s.unsyncedFrames = pending
	s.mirrorUnsynced = true
	s.mu.Unlock()
}

// ensureMirrorSynced blocks a prompt with a loud error whenever the local mirror
// still holds frames from a previously completed turn that failed to persist. It
// retries the durable Replace of the exact frames on each call and only unblocks
// once that retry succeeds (X2).
func (s *agentSession) ensureMirrorSynced(ctx context.Context) error {
	return s.ensureMirrorSyncedKind(ctx, sessionPersistenceOrdinary)
}

func (s *agentSession) ensureMirrorSyncedForClose(ctx context.Context) error {
	return s.ensureMirrorSyncedKind(ctx, sessionPersistenceCloseRetry)
}

func (s *agentSession) ensureMirrorSyncedKind(ctx context.Context, kind sessionPersistenceKind) error {
	s.mu.Lock()
	unsynced := s.mirrorUnsynced
	s.mu.Unlock()

	if !unsynced {
		return nil
	}

	if err := s.persistAfterTurnKind(ctx, nil, kind); err != nil {
		return errors.Join(
			acp.NewInternalError(map[string]any{jsonFieldError: "mirror_unsynced", keyDetail: err.Error()}),
			err,
		)
	}

	return nil
}
