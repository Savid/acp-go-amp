package ampacp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/coder/acp-go-sdk"
	nativeamp "github.com/savid/acp-go-amp/internal/amp"
	"github.com/stretchr/testify/require"
)

const populatedQuotaDisplay = "Signed in as private@example.test (private)\nSubscription Megawatt: 97% other usage and 100% orb usage remaining - resets upon renewal in 29 days\n**Individual credits:** $0 remaining - https://ampcode.com/settings\n# Run `amp usage --details` for more detailed information.\n"

type quotaTestAuthority struct {
	*recordingAuthority
	startQuota func(context.Context, NativeRequest) (NativeProcess, error)
}

func (a *quotaTestAuthority) StartNative(ctx context.Context, request NativeRequest) (NativeProcess, error) {
	if len(request.Arguments) > 0 && request.Arguments[len(request.Arguments)-1] == "usage" {
		return a.startQuota(ctx, request)
	}

	return a.recordingAuthority.StartNative(ctx, request)
}

type quotaTestWriteCloser struct{ bytes.Buffer }

func (*quotaTestWriteCloser) Close() error { return nil }

type quotaTestProcess struct {
	stdin          io.WriteCloser
	stdout, stderr io.ReadCloser
	wait           func(context.Context) (NativeResult, error)
	revoke         func(context.Context) error
	panicStdin     bool
}

func newQuotaTestProcess() *quotaTestProcess {
	return &quotaTestProcess{stdin: &quotaTestWriteCloser{}, stdout: io.NopCloser(strings.NewReader(populatedQuotaDisplay)), stderr: io.NopCloser(strings.NewReader("")), wait: func(context.Context) (NativeResult, error) { return NativeResult{}, nil }, revoke: func(context.Context) error { return nil }}
}

func (p *quotaTestProcess) Stdin() io.WriteCloser {
	if p.panicStdin {
		panic("private native callback")
	}

	return p.stdin
}
func (p *quotaTestProcess) Stdout() io.ReadCloser                          { return p.stdout }
func (p *quotaTestProcess) Stderr() io.ReadCloser                          { return p.stderr }
func (p *quotaTestProcess) Wait(ctx context.Context) (NativeResult, error) { return p.wait(ctx) }
func (p *quotaTestProcess) Revoke(ctx context.Context) error               { return p.revoke(ctx) }

func newQuotaBoundaryAgent(t *testing.T, start func(context.Context, NativeRequest) (NativeProcess, error)) (*Agent, *agentSession, *quotaTestAuthority) {
	t.Helper()
	authority := &quotaTestAuthority{recordingAuthority: newRecordingAuthority(), startQuota: start}
	authority.environment = map[string]string{"AMP_API_KEY": "host-key", "PATH": "host-path"}
	path := filepath.Join(t.TempDir(), "verified-amp")
	agent := NewAgent(WithHostAuthority(authority), WithExecutablePath(path), WithScratchDir(t.TempDir()), func(options *Options) {
		options.runtime.startupProbe = func(context.Context, *nativeamp.Client) (string, error) { return path, nil }
	})
	opened, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	session, err := agent.session(opened.SessionId)
	require.NoError(t, err)
	t.Cleanup(func() {
		// These fake handles own no external process. Restore the injected
		// sticky owner failures before ordinary fixture cleanup.
		agent.mu.Lock()
		agent.lifecycleContainmentErr = nil
		agent.configurationErr = nil
		agent.mu.Unlock()
		session.mu.Lock()
		session.scratchContainmentErr = nil
		session.poisonCause = ""
		session.mu.Unlock()
		require.NoError(t, agent.Close())
	})

	return agent, session, authority
}

func requestQuota(t *testing.T, ctx context.Context, agent *Agent, id acp.SessionId) (any, error) {
	t.Helper()
	params, err := json.Marshal(RateLimitsRequest{SessionID: id})
	require.NoError(t, err)

	return agent.HandleExtensionMethod(ctx, RateLimitsMethod, params)
}

func TestQuotaActualReaderThroughHandlerAndOptOut(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	var request NativeRequest
	agent, session, _ := newQuotaBoundaryAgent(t, func(_ context.Context, got NativeRequest) (NativeProcess, error) {
		calls.Add(1)
		request = got

		return newQuotaTestProcess(), nil
	})
	result, err := requestQuota(t, t.Context(), agent, session.id)
	require.NoError(t, err)
	response, ok := result.(RateLimitsResponse)
	require.True(t, ok)
	require.Equal(t, "available", response.Availability)
	require.Len(t, response.Pools, 2)
	require.Equal(t, "amp-subscription", response.Pools[0].ID)
	require.Equal(t, "Megawatt", response.Pools[0].PlanType)
	require.Equal(t, "other", response.Pools[0].Windows[0].ID)
	require.Equal(t, "orb", response.Pools[0].Windows[1].ID)
	require.Equal(t, 3.0, *response.Pools[0].Windows[0].UsedPercent)
	require.Equal(t, 0.0, *response.Pools[0].Windows[1].UsedPercent)
	require.Nil(t, response.Pools[0].Windows[0].DurationSeconds)
	require.Empty(t, response.Pools[0].Windows[0].ResetsAt)
	require.Empty(t, response.Pools[0].Windows[0].Status)
	require.Equal(t, "amp-individual-credits", response.Pools[1].ID)
	require.NotNil(t, response.Pools[1].Windows)
	require.Empty(t, response.Pools[1].Windows)
	balance := response.Pools[1].Balances[0]
	require.Equal(t, "credits", balance.ID)
	require.Equal(t, &RateLimitMoney{Amount: 0, Currency: "USD"}, balance.Remaining)
	require.Nil(t, balance.Used)
	require.Nil(t, balance.Limit)
	require.NotEmpty(t, balance.ObservedAt)
	require.Empty(t, balance.ResetsAt)
	require.Equal(t, session.cwd, request.WorkingDirectory)
	require.Contains(t, request.Arguments, session.settingsFile)
	require.Contains(t, request.Environment, "AMP_API_KEY=host-key")
	require.Contains(t, request.Environment, "HOME="+session.operationEnv["HOME"])
	encoded, err := json.Marshal(result)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "private")
	require.Empty(t, session.quotaReads)
	agent.options.DirectAPI = false
	result, err = requestQuota(t, t.Context(), agent, session.id)
	require.NoError(t, err)
	require.Equal(t, rateLimitsUnavailable("amp", "disabled"), result)
	require.Equal(t, int32(1), calls.Load())
	require.True(t, applyOptions(nil).DirectAPI)
	require.False(t, applyOptions([]Option{WithAmpDirectAPI(false)}).DirectAPI)
}

func TestQuotaOwnerErrorsBeforeAndDuringNativeRead(t *testing.T) {
	t.Parallel()
	for _, during := range []bool{false, true} {
		for _, source := range []string{"configuration", "authority", "session", "native", "panic"} {
			t.Run(source+map[bool]string{false: " before", true: " during"}[during], func(t *testing.T) {
				t.Parallel()
				ownerErr := errors.New("owner failure")
				var calls atomic.Int32
				agent, session, authority := newQuotaBoundaryAgent(t, nil)
				setFailure := func() {
					switch source {
					case "configuration":
						agent.configurationErr = ownerErr
					case "authority":
						agent.mu.Lock()
						agent.lifecycleContainmentErr = errors.Join(ownerErr, ErrHostAuthorityUnavailable)
						agent.mu.Unlock()
					case "session":
						session.mu.Lock()
						session.scratchContainmentErr = errors.Join(ownerErr, ErrContainmentIncomplete)
						session.mu.Unlock()
					}
				}
				authority.startQuota = func(context.Context, NativeRequest) (NativeProcess, error) {
					calls.Add(1)
					if source == "native" {
						return nil, errors.Join(ownerErr, ErrNativeTreeBusy)
					}
					process := newQuotaTestProcess()
					if source == "panic" {
						process.panicStdin = true
					} else {
						setFailure()
					}

					return process, nil
				}
				if !during {
					setFailure()
				}
				_, err := requestQuota(t, t.Context(), agent, session.id)
				var requestErr *acp.RequestError
				require.ErrorAs(t, err, &requestErr)
				if source == "panic" {
					require.ErrorIs(t, err, ErrContainmentIncomplete)
					require.ErrorIs(t, err, errAgentGoroutinePanic)
					require.NotContains(t, err.Error(), "private")
				} else {
					require.ErrorIs(t, err, ownerErr)
				}
				if during || source == "native" || source == "panic" {
					require.Equal(t, int32(1), calls.Load())
				} else {
					require.Zero(t, calls.Load())
				}
				require.Empty(t, session.quotaReads)
			})
		}
	}
}

func TestQuotaFencesInFlightTarget(t *testing.T) {
	t.Parallel()
	for _, change := range []string{"delete flight", "replacement flight", "closed", "poisoned", "binding", "cancelled"} {
		t.Run(change, func(t *testing.T) {
			t.Parallel()
			agent, session, authority := newQuotaBoundaryAgent(t, nil)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			authority.startQuota = func(context.Context, NativeRequest) (NativeProcess, error) {
				switch change {
				case "delete flight":
					agent.mu.Lock()
					agent.sessionFlights[session.id] = &agentSessionFlight{}
					agent.mu.Unlock()
				case "replacement flight":
					agent.mu.Lock()
					agent.sessionUses[session.id] = &agentSessionUse{replacing: true}
					agent.mu.Unlock()
				case "closed":
					session.mu.Lock()
					session.closed = true
					session.mu.Unlock()
				case "poisoned":
					session.mu.Lock()
					session.poisonCause = causeNativeIDDrift
					session.mu.Unlock()
				case "binding":
					session.mu.Lock()
					session.env["AMP_API_KEY"] = "replacement"
					session.mu.Unlock()
				case "cancelled":
					cancel()
				}

				return newQuotaTestProcess(), nil
			}
			result, err := requestQuota(t, ctx, agent, session.id)
			if change == "binding" {
				require.NoError(t, err)
				require.Equal(t, rateLimitsUnavailable("amp", "read_failed"), result)
			} else {
				require.Error(t, err)
			}
			if change == "cancelled" {
				require.ErrorIs(t, err, context.Canceled)
			}
			agent.mu.Lock()
			delete(agent.sessionFlights, session.id)
			delete(agent.sessionUses, session.id)
			agent.mu.Unlock()
		})
	}
}

func newHeldQuotaProcess() (*quotaTestProcess, <-chan struct{}, func()) {
	process := newQuotaTestProcess()
	revoked := make(chan struct{})
	release := make(chan struct{})
	var revokeOnce, releaseOnce sync.Once
	process.revoke = func(context.Context) error {
		revokeOnce.Do(func() { close(revoked) })

		return nil
	}
	process.wait = func(ctx context.Context) (NativeResult, error) {
		select {
		case <-revoked:
			<-release

			return NativeResult{Revoked: true}, nil
		case <-ctx.Done():
			return NativeResult{}, ctx.Err()
		}
	}

	return process, revoked, func() { releaseOnce.Do(func() { close(release) }) }
}

func TestQuotaCloseAndDeleteJoinExactNativeHandle(t *testing.T) {
	t.Parallel()
	for _, deleting := range []bool{false, true} {
		t.Run(map[bool]string{false: "close", true: "delete"}[deleting], func(t *testing.T) {
			t.Parallel()
			process, revoked, release := newHeldQuotaProcess()
			defer release()
			started := make(chan struct{})
			agent, session, authority := newQuotaBoundaryAgent(t, func(context.Context, NativeRequest) (NativeProcess, error) {
				close(started)

				return process, nil
			})
			done := make(chan error, 1)
			go func() { _, err := requestQuota(t, t.Context(), agent, session.id); done <- err }()
			<-started
			closed := make(chan error, 1)
			go func() {
				if deleting {
					_, err := agent.UnstableDeleteSession(t.Context(), DeleteSessionRequest(session.id))
					closed <- err
				} else {
					_, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.id})
					closed <- err
				}
			}()
			<-revoked
			authority.mu.Lock()
			reclaims := authority.reclaimCalls
			authority.mu.Unlock()
			require.Zero(t, reclaims)
			select {
			case err := <-closed:
				t.Fatalf("teardown passed unsettled quota: %v", err)
			default:
			}
			release()
			require.Error(t, <-done)
			require.NoError(t, <-closed)
			authority.mu.Lock()
			reclaims = authority.reclaimCalls
			authority.mu.Unlock()
			require.Equal(t, 1, reclaims)
		})
	}
}

func TestQuotaPromptPriorityFencesLifecycleAndLateReads(t *testing.T) {
	process, revoked, release := newHeldQuotaProcess()
	defer release()
	started := make(chan struct{})
	var calls atomic.Int32
	authority := &quotaTestAuthority{recordingAuthority: newRecordingAuthority(), startQuota: func(context.Context, NativeRequest) (NativeProcess, error) {
		calls.Add(1)
		close(started)

		return process, nil
	}}
	ledger := &settlementLedger{}
	agent, _, client, id := settlementAgent(t, ledger, WithHostAuthority(authority), WithEnv(map[string]string{"AMP_API_KEY": "fake"}))
	quiescenceStarted := make(chan struct{})
	quiescenceRelease := make(chan struct{})
	var releaseOnce sync.Once
	releaseQuiescence := func() { releaseOnce.Do(func() { close(quiescenceRelease) }) }
	defer releaseQuiescence()
	agent.options.runtime.beforeTerminalDelivery = func(notification acp.SessionNotification) {
		if isQuiescenceNotification(notification) {
			close(quiescenceStarted)
			<-quiescenceRelease
		}
	}
	type quotaReply struct {
		result any
		err    error
	}
	quotaDone := make(chan quotaReply, 1)
	go func() { result, err := requestQuota(t, t.Context(), agent, id); quotaDone <- quotaReply{result, err} }()
	<-started
	promptDone := make(chan error, 1)
	go func() {
		_, err := agent.Prompt(t.Context(), lifecyclePrompt(id, "hello", "quota-priority", "quota-nonce"))
		promptDone <- err
	}()
	<-revoked
	require.Empty(t, client.eventTypes(t), "opening lifecycle snapshot preceded quota settlement")
	require.Empty(t, ledger.snapshot(), "prompt snapshot or dispatch preceded quota settlement")
	select {
	case err := <-promptDone:
		t.Fatalf("prompt passed unsettled quota handle: %v", err)
	default:
	}
	release()
	quota := <-quotaDone
	require.NoError(t, quota.err)
	require.Equal(t, rateLimitsUnavailable("amp", "read_failed"), quota.result)
	<-quiescenceStarted
	require.NoError(t, <-promptDone, "foreground must not wait for final quiescence delivery")
	result, err := requestQuota(t, t.Context(), agent, id)
	require.NoError(t, err)
	require.Equal(t, rateLimitsUnavailable("amp", "read_failed"), result)
	require.Equal(t, int32(1), calls.Load(), "quota launched before the prompt's full terminal fence")
	releaseQuiescence()
	_, err = agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: id})
	require.NoError(t, err)
}

func TestQuotaWindowsAdminSelectorKeepsStaticPath(t *testing.T) {
	previous := runtimeGOOS
	runtimeGOOS = platformWindows
	t.Cleanup(func() { runtimeGOOS = previous })
	var request NativeRequest
	agent, session, _ := newQuotaBoundaryAgent(t, func(_ context.Context, got NativeRequest) (NativeProcess, error) {
		request = got

		return newQuotaTestProcess(), nil
	})
	session.mu.Lock()
	session.sessionEnv = map[string]string{"ProgramData": "session-admin", "PATH": "session-prompt-path", "AMP_API_KEY": "session-key"}
	session.env = composeEnv(session.env, session.sessionEnv)
	session.operationEnv = composeEnv(session.operationEnv, operationSessionEnv(session.sessionEnv))
	session.mu.Unlock()
	_, err := requestQuota(t, t.Context(), agent, session.id)
	require.NoError(t, err)
	require.Contains(t, request.Environment, "PROGRAMDATA=session-admin")
	require.Contains(t, request.Environment, "AMP_API_KEY=session-key")
	require.Contains(t, request.Environment, "PATH=host-path")
	require.NotContains(t, request.Environment, "PATH=session-prompt-path")
	require.Contains(t, request.Arguments, session.settingsFile)
}

func TestQuotaStickyFailureBlocksLaterAdmission(t *testing.T) {
	t.Parallel()
	agent, session, _ := newQuotaBoundaryAgent(t, func(context.Context, NativeRequest) (NativeProcess, error) {
		t.Fatal("known failed boundary launched quota")

		return nil, errors.New("unexpected native dispatch")
	})
	for _, global := range []bool{false, true} {
		ownerErr := errors.Join(errors.New("retained failure"), ErrContainmentIncomplete)
		if global {
			agent.mu.Lock()
			agent.lifecycleContainmentErr = ownerErr
			agent.mu.Unlock()
		} else {
			session.mu.Lock()
			session.scratchContainmentErr = ownerErr
			session.mu.Unlock()
		}
		lease, err := agent.admitQuotaRead(session.id, session, func() {})
		require.Nil(t, lease)
		require.ErrorIs(t, err, ownerErr)
		session.mu.Lock()
		session.scratchContainmentErr = nil
		session.mu.Unlock()
	}
}
