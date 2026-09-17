package ampacp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-amp/internal/amp"
	"github.com/savid/acp-go-core/lifecycle"
	"github.com/savid/acp-go-core/process"
	"github.com/savid/acp-go-core/wire"
)

const nativeCommandTimeout = 30 * time.Second

type session struct {
	agent                 *Agent
	id                    acp.SessionId
	nativeID              string
	serviceURL            string
	usage                 map[string]map[string]any
	cwd                   string
	additionalDirectories []string
	gate                  chan struct{}
	mu                    sync.Mutex
	options               AmpOptions
	model                 string
	title                 string
	updatedAt             string
	turn                  *turn
	closing               bool
	closeDone             chan struct{}
	closeErr              error
	poison                string
	// rows are protected by the foreground gate; close takes it after joining the turn.
	rows      [][]byte
	rawEvents *wire.RawEvents
	lc        lifecycle.Publisher
}

type turn struct {
	cancelNative context.CancelFunc
	cancelled    bool
	settling     bool
	accepted     bool
	started      bool
	submission   lifecycle.Submission
	lifecycle.Cycle
	state    cycleState
	evidence amp.Outcome
}

func (s *session) admissionError() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.poison != "" {
		return wire.SessionPoisoned(vendor, s.poison)
	}

	if s.closing {
		return wire.UnknownSession()
	}

	return nil
}

func (s *session) acquireGate(limit string) (func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closing {
		return nil, wire.UnknownSession()
	}

	return wire.AcquireSessionGate(s.gate, limit)
}

func (s *session) cancel(_ context.Context) {
	s.mu.Lock()

	t := s.turn
	if t == nil || t.settling || t.cancelled {
		s.mu.Unlock()

		return
	}

	t.cancelled = true
	stop := t.cancelNative
	s.mu.Unlock()
	stop()
}

// close joins the foreground operation before publishing its final carrier.
func (s *session) close(ctx context.Context) error {
	s.mu.Lock()
	if s.closing {
		done := s.closeDone
		s.mu.Unlock()
		<-done

		return s.closeErr
	}

	s.closing = true
	s.closeDone = make(chan struct{})

	t := s.turn
	if t != nil && !t.settling {
		t.cancelled = true
		t.cancelNative()
	}
	s.mu.Unlock()
	// Setting closing prevents another admission while this waits for an earlier one.
	s.gate <- struct{}{}
	defer func() { <-s.gate }()

	commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), nativeCommandTimeout)
	defer cancel()

	err := s.commitMirror(commitCtx)
	s.fenceStream()
	s.mu.Lock()
	s.closeErr = err
	close(s.closeDone)
	s.mu.Unlock()

	return err
}

func (s *session) nativeRequest(ctx context.Context, args []string) (process.Request, error) {
	executable, err := s.agent.ensureExecutable(ctx)
	if err != nil {
		return process.Request{}, err
	}

	s.mu.Lock()
	options := s.options.clone()
	s.mu.Unlock()
	environment := s.agent.environment(options.Env, nil)
	environment.ExtraPathDirs = options.ExtraPathDirs

	env, err := environment.Build()
	if err != nil {
		return process.Request{}, err
	}

	if len(s.agent.options.SeedFiles) > 0 {
		root := ampConfigRoot(env, s.cwd)
		if err := process.WriteSeedFiles(root, s.agent.options.SeedFiles); err != nil {
			s.agent.log.ErrorContext(ctx, "Amp seed files rejected", slog.String("reason", err.Error()))

			if refusal := wire.SeedFileRefusal(err); refusal != nil {
				return process.Request{}, refusal
			}

			return process.Request{}, wire.InternalFailure(vendor, internalClassNativeStart)
		}
	}

	if options.Mode != "" {
		args = append([]string{"--mode", options.Mode}, args...)
	}

	return process.Request{Executable: executable, Args: args, Env: env, Dir: s.cwd}, nil
}

func ampConfigRoot(env []string, cwd string) string {
	if value, ok := process.Lookup(env, "AMP_SETTINGS_FILE"); ok && value != "" {
		if !filepath.IsAbs(value) {
			value = filepath.Join(cwd, value)
		}

		return filepath.Dir(value)
	}

	root, _ := process.Lookup(env, "XDG_CONFIG_HOME")
	if root == "" {
		home, _ := process.Lookup(env, "HOME")
		root = filepath.Join(home, ".config")
	}

	if !filepath.IsAbs(root) {
		root = filepath.Join(cwd, root)
	}

	return filepath.Join(root, "amp")
}

func (s *session) command(ctx context.Context, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, nativeCommandTimeout)
	defer cancel()

	request, err := s.nativeRequest(ctx, args)
	if err != nil {
		return nil, err
	}

	output, result, stderr, err := amp.Run(ctx, request, nil, nil)
	if err != nil {
		return nil, err
	}

	if result.ExitCode != 0 {
		return nil, fmt.Errorf("amp command exited with status %d: %s", result.ExitCode, stderr)
	}

	return output, nil
}

func (s *session) createNative(ctx context.Context, options ...string) error {
	output, err := s.command(ctx, append([]string{"threads", "new"}, options...)...)
	if err != nil {
		s.agent.log.ErrorContext(ctx, "Amp thread creation failed", slog.String("reason", err.Error()))

		return wire.InternalFailure(vendor, internalClassNativeStart)
	}

	id, err := amp.ParseThreadURL(output)
	if err != nil {
		return wire.InternalFailure(vendor, internalClassNativeStart)
	}

	serviceURL, err := amp.ServiceURL(output)
	if err != nil {
		return wire.InternalFailure(vendor, internalClassNativeStart)
	}

	s.nativeID = id

	s.serviceURL = serviceURL
	if s.id == "" {
		s.id = acp.SessionId(id)
	}

	rows, err := s.exportNative(ctx)
	if err != nil {
		s.agent.log.ErrorContext(ctx, "Amp initial export failed", slog.String("reason", err.Error()))

		return wire.InternalFailure(vendor, internalClassNativeStart)
	}

	s.rows = rows
	s.adoptSnapshot(rows)

	return nil
}

func (s *session) sessionInfo() acp.SessionInfo {
	s.mu.Lock()
	defer s.mu.Unlock()

	title := s.title
	if title == "" {
		title = string(s.id)
	}

	updated := s.updatedAt

	result := acp.SessionInfo{Meta: wire.NativeSessionMeta(vendor, s.nativeID), SessionId: s.id, Cwd: s.cwd, AdditionalDirectories: slices.Clone(s.additionalDirectories), Title: &title}
	if updated != "" {
		result.UpdatedAt = &updated
	}

	return result
}

func (s *session) emit(ctx context.Context, update acp.SessionUpdate) error {
	if conn := s.agent.connection(); conn != nil {
		return conn.SessionUpdate(ctx, acp.SessionNotification{SessionId: s.id, Update: update})
	}

	return nil
}

func (s *session) poisonSession(cause string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.poison = cause

	return wire.SessionPoisoned(vendor, cause)
}

func (s *session) lifecycleNegotiated() lifecycle.Negotiated { return s.agent.lifecycleNegotiated() }

var errIdentityDrift = errors.New("native session identity changed")

func (s *session) continueRequest(ctx context.Context) (process.Request, error) {
	return s.nativeRequest(ctx, []string{"threads", "continue", s.nativeID, "--execute", "--stream-json-thinking", "--stream-json-input", "--no-archive-after-execute"})
}
