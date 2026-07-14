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
	configEffort     = acp.SessionConfigId(optionEffortKey)
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
	optionEffortKey        = "effort"
	optionEnvKey           = "env"
	optionFieldHome        = "home"

	fieldValue  = "value"
	fieldPrompt = "prompt"
	fieldCursor = "cursor"
	keyType     = "type"
	keyDetail   = "detail"
	keyMaxBytes = "maxBytes"
	keySource   = "source"
	envHome     = "HOME"

	valUnsupported       = "unsupported"
	valNoTransport       = "no_transport"
	valText              = "text"
	valUser              = "user"
	valRequired          = "required"
	valDuplicate         = "duplicate"
	reasonUnserializable = "unserializable"

	modeLow    = "low"
	modeMedium = "medium"
	modeHigh   = "high"

	effortNone    = "none"
	effortMinimal = "minimal"
	effortLow     = "low"
	effortMedium  = "medium"
	effortHigh    = "high"
	effortXHigh   = "xhigh"
	effortMax     = "max"
)

var (
	errSessionClosed = errors.New("session closed")
	writeFile        = os.WriteFile
	readFile         = os.ReadFile
	mkdirAll         = os.MkdirAll
	mkdirTemp        = os.MkdirTemp
)

type ampManifest struct {
	Format             string `json:"format"`
	ThreadID           string `json:"threadId"`
	Cwd                string `json:"cwd"`
	Title              string `json:"title,omitempty"`
	Mode               string `json:"mode,omitempty"`
	Effort             string `json:"effort,omitempty"`
	UpdatedAtUnixMilli int64  `json:"updatedAtUnixMilli"`
	CreatedAtUnixMilli int64  `json:"createdAtUnixMilli"`
}

type agentSession struct {
	agent                 *Agent
	id                    acp.SessionId
	cwd                   string
	title                 string
	mode                  string
	effort                string
	createdUnix           int64
	updatedUnix           int64
	additionalDirectories []string
	mcpConfigJSON         string
	env                   map[string]string
	rawEvents             bool
	rawEventSeq           atomic.Int64
	settingsDir           string
	settingsFile          string
	closed                bool
	poisonCause           string
	nativeMissingCause    string
	unsyncedFrames        []SessionStoreEntry
	turn                  chan struct{}
	cancelMu              sync.Mutex
	activePrompt          *promptTurnState
	persistMu             sync.Mutex
	mu                    sync.Mutex
}

func newAgentSession(agent *Agent, id acp.SessionId, cwd string, meta parsedSessionMeta, mcpConfigJSON string, additionalDirs []string) (*agentSession, error) {
	now := time.Now().UnixMilli()

	parent, err := ensureScratchParent(agent.options.ScratchDir)
	if err != nil {
		return nil, err
	}

	dir, err := mkdirTemp(parent, "acp-go-amp-session-*")
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
			_ = os.RemoveAll(dir)

			return nil, fmt.Errorf("create amp isolated home: %w", err)
		}
	}

	settingsFile := filepath.Join(configDir, "amp", "settings.json")
	if err := writeFile(settingsFile, []byte("{}\n"), 0o600); err != nil {
		_ = os.RemoveAll(dir)

		return nil, fmt.Errorf("write amp settings file: %w", err)
	}

	if err := writeSeedFiles(homeDir, agent.options.SeedFiles); err != nil {
		_ = os.RemoveAll(dir)

		return nil, err
	}

	mode := meta.options.Mode
	if mode == "" {
		mode = modeMedium
	}
	// Effort has no wrapper-imposed default: --effort is passed to amp only when
	// the host explicitly set it (per-request option or session config). When it
	// is unset the flag is omitted and amp chooses its own default, which is then
	// surfaced back via reconcileNativeConfig once the init frame reports it.
	effort := meta.options.Effort
	env := mergeEnv(agent.options.Env, meta.options.Env)
	env[envHome] = homeDir
	env["XDG_CONFIG_HOME"] = configDir
	env["XDG_CACHE_HOME"] = cacheDir
	env["XDG_DATA_HOME"] = dataDir
	env["XDG_STATE_HOME"] = stateDir

	return &agentSession{
		agent:                 agent,
		id:                    id,
		cwd:                   cwd,
		mode:                  mode,
		effort:                effort,
		createdUnix:           now,
		updatedUnix:           now,
		additionalDirectories: append([]string(nil), additionalDirs...),
		mcpConfigJSON:         mcpConfigJSON,
		env:                   env,
		rawEvents:             meta.rawEvent,
		settingsDir:           dir,
		settingsFile:          settingsFile,
		turn:                  make(chan struct{}, 1),
	}, nil
}

func (s *agentSession) client() *amp.Client {
	return s.clientWithEnv(s.env)
}

func (s *agentSession) clientWithEnv(env map[string]string) *amp.Client {
	return amp.NewClient(s.agent.log, amp.Options{
		CLIPath:          s.agent.options.ExecutablePath,
		Cwd:              s.cwd,
		SettingsFile:     s.settingsFile,
		Env:              env,
		ThreadID:         string(s.id),
		Mode:             s.mode,
		Effort:           s.effort,
		MCPConfigJSON:    s.mcpConfigJSON,
		MaxLineBytes:     s.agent.options.runtime.maxJSONLineBytes,
		OnGoroutinePanic: s.agent.onNativeGoroutinePanic,
	})
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
	_ = ctx

	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()

	state := s.activePromptState()
	if state != nil {
		state.cancel()
	}

	err := s.interruptState(context.Background(), state)
	if s.settingsDir != "" {
		err = errors.Join(err, os.RemoveAll(s.settingsDir))
	}

	return err
}

func (s *agentSession) Delete(ctx context.Context) error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	_ = s.interrupt(ctx)

	err := s.client().DeleteThread(ctx, string(s.id))
	if s.settingsDir != "" {
		err = errors.Join(err, os.RemoveAll(s.settingsDir))
	}

	return err
}

func (s *agentSession) verifyContinuable(ctx context.Context) error {
	timeout := s.agent.options.runtime.nativeCommandTimeout

	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if _, err := s.client().ExportThread(probeCtx, string(s.id)); err != nil {
		if isNativeMissingError(err) {
			s.mu.Lock()
			s.nativeMissingCause = err.Error()
			s.mu.Unlock()

			return nil
		}

		return acp.NewInternalError(map[string]any{jsonFieldError: err.Error()})
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
		ThreadID:           string(s.id),
		Cwd:                s.cwd,
		Title:              s.title,
		Mode:               s.mode,
		Effort:             s.effort,
		UpdatedAtUnixMilli: s.updatedUnix,
		CreatedAtUnixMilli: s.createdUnix,
	}
}

// persistAfterTurn durably commits the manifest plus the full transcript in one
// Replace (X4: the whole load-append-Replace path is serialized per session).
// Per X2, any newly completed frames that fail to persist are retained in memory
// (mirror-unsynced) and re-committed on the next attempt so a store outage after
// a native turn success can never silently drop the turn.
func (s *agentSession) persistAfterTurn(ctx context.Context, transcript []SessionStoreEntry) error {
	s.persistMu.Lock()
	defer s.persistMu.Unlock()

	now := time.Now().UnixMilli()

	s.mu.Lock()
	s.updatedUnix = now
	pending := append(cloneEntries(s.unsyncedFrames), cloneEntries(transcript)...)
	s.mu.Unlock()

	if s.agent.store == nil {
		return nil
	}

	loadCtx, cancelLoad := s.agent.sessionStoreLoadContext(ctx)
	fullTranscript, err := s.agent.store.Load(loadCtx, SessionKey{SessionID: string(s.id), Subpath: transcriptSubpath})

	cancelLoad()

	if err != nil {
		s.retainUnsynced(pending)

		return err
	}

	if len(pending) > 0 {
		fullTranscript = append(cloneEntries(fullTranscript), pending...)
	}

	main, _ := json.Marshal(s.manifest())

	replaceCtx, cancelReplace := s.agent.sessionStoreWriteContext(ctx)
	defer cancelReplace()

	if err := s.agent.store.Replace(replaceCtx, SessionKey{SessionID: string(s.id), Subpath: SessionStoreMainSubpath}, []SessionStoreReplacement{
		{Key: SessionKey{SessionID: string(s.id), Subpath: SessionStoreMainSubpath}, Entries: []SessionStoreEntry{main}},
		{Key: SessionKey{SessionID: string(s.id), Subpath: transcriptSubpath}, Entries: fullTranscript},
	}); err != nil {
		s.retainUnsynced(pending)

		return err
	}

	s.mu.Lock()
	s.unsyncedFrames = nil
	s.mu.Unlock()

	return nil
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
