package ampacp

import (
	"context"
	"errors"
	"maps"
	"slices"

	"github.com/coder/acp-go-sdk"
	nativeamp "github.com/savid/acp-go-amp/internal/amp"
)

type quotaReadLease struct {
	cancel context.CancelFunc
	done   chan struct{}
}

var errQuotaPromptBusy = errors.New("quota read deferred to active prompt")

func (a *Agent) acquireRateLimits(ctx context.Context, id acp.SessionId, session *agentSession, cancel context.CancelFunc) (result nativeamp.ProviderQuota, returnErr error) {
	lease, err := a.admitQuotaRead(id, session, cancel)
	if errors.Is(err, errQuotaPromptBusy) {
		return nativeamp.ProviderQuota{Availability: "unavailable", Reason: "read_failed"}, nil
	}

	if err != nil {
		return nativeamp.ProviderQuota{}, err
	}

	defer func() {
		if recover() != nil {
			returnErr = nativeInternalError(classContainmentIncomplete, errors.Join(errAgentGoroutinePanic, ErrContainmentIncomplete))
			session.recordScratchContainment(returnErr)

			result = nativeamp.ProviderQuota{}
		}

		session.finishQuotaRead(lease)
	}()

	result, readErr := session.quotaClient().ReadProviderQuota(ctx)
	session.recordScratchContainment(readErr)

	if containmentIncomplete(readErr) || errors.Is(readErr, ErrHostAuthorityUnavailable) || errors.Is(readErr, ErrNativeTreeBusy) {
		return nativeamp.ProviderQuota{}, nativeInternalError(classContainmentIncomplete, readErr)
	}

	// Ordinary native command/source failures are values-free availability.
	// The caller rechecks cancellation and session ownership after settlement.
	if readErr != nil {
		return nativeamp.ProviderQuota{Availability: "unavailable", Reason: "read_failed"}, nil
	}

	return result, nil
}

func (s *agentSession) quotaClient() *nativeamp.Client {
	s.mu.Lock()

	environment := cloneStringMap(s.operationEnv)
	if runtimeGOOS == platformWindows {
		// Native Windows admin settings live below ProgramData. This account
		// selector is needed only by quota; PATH remains the static operation PATH.
		for _, key := range slices.Sorted(maps.Keys(s.sessionEnv)) {
			if canonicalEnvKey(key) == "PROGRAMDATA" {
				environment["PROGRAMDATA"] = s.sessionEnv[key]
			}
		}
	}
	s.mu.Unlock()

	return s.clientWithEnv(environment, "")
}

// admitQuotaRead shares the native admission fence with prompt and teardown.
// An admitted prompt blocks later polls through its full settlement lifetime.
func (a *Agent) admitQuotaRead(id acp.SessionId, session *agentSession, cancel context.CancelFunc) (*quotaReadLease, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if ownerErr := errors.Join(a.optionsError(), rateLimitsBoundaryError(a.lifecycleContainmentErr)); ownerErr != nil {
		return nil, ownerErr
	}

	_, deleted := a.deleted[id]

	use := a.sessionUses[id]
	if a.closed || a.sessions[id] != session || deleted || a.sessionFlights[id] != nil || use != nil && use.replacing {
		return nil, unknownSessionError()
	}

	session.cancelMu.Lock()
	defer session.cancelMu.Unlock()

	session.mu.Lock()
	defer session.mu.Unlock()

	if session.scratchContainmentErr != nil {
		return nil, rateLimitsBoundaryError(session.scratchContainmentErr)
	}

	if session.closed {
		return nil, unknownSessionError()
	}

	if session.poisonCause != "" {
		return nil, sessionPoisoned(session.poisonCause)
	}

	if session.activePrompt != nil {
		return nil, errQuotaPromptBusy
	}

	lease := &quotaReadLease{cancel: cancel, done: make(chan struct{})}

	if session.quotaReads == nil {
		session.quotaReads = map[*quotaReadLease]struct{}{}
	}

	session.quotaReads[lease] = struct{}{}

	return lease, nil
}

func (s *agentSession) finishQuotaRead(lease *quotaReadLease) {
	lease.cancel()
	s.mu.Lock()
	delete(s.quotaReads, lease)
	close(lease.done)
	s.mu.Unlock()
}

// settleQuotaReads ends exact admitted handles before any opening/terminal
// vacancy claim or residence reclamation. A failed settlement remains owned.
func (s *agentSession) settleQuotaReads(ctx context.Context) error {
	s.mu.Lock()

	leases := make([]*quotaReadLease, 0, len(s.quotaReads))
	for lease := range s.quotaReads {
		leases = append(leases, lease)
	}
	s.mu.Unlock()

	for _, lease := range leases {
		lease.cancel()
	}

	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), defaultNativeCancelTimeout+2*defaultNativeCloseTurnWait)
	defer cancel()

	for _, lease := range leases {
		select {
		case <-lease.done:
		case <-settleCtx.Done():
			err := errors.Join(ErrContainmentIncomplete, settleCtx.Err())
			s.recordScratchContainment(err)

			return err
		}
	}

	return s.scratchContainmentError()
}
