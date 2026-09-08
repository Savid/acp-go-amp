package ampacp

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"time"

	"github.com/coder/acp-go-sdk"
	nativeamp "github.com/savid/acp-go-amp/internal/amp"
)

const rateLimitsOtherWindowID = "other"

func (a *Agent) handleRateLimits(ctx context.Context, raw json.RawMessage) (RateLimitsResponse, error) {
	request, err := decodeRateLimitsRequest(raw)
	if err != nil {
		return RateLimitsResponse{}, err
	}

	session, environment, err := a.rateLimitsScope(ctx, request.SessionID, nil)
	if err != nil {
		return RateLimitsResponse{}, err
	}

	provider := request.ProviderID
	if provider == "" {
		provider = authProviderID
	}

	if provider != authProviderID {
		return rateLimitsUnsupported(provider), nil
	}

	if !a.options.DirectAPI {
		return rateLimitsUnavailable(provider, "disabled"), nil
	}

	if session == nil {
		return rateLimitsUnavailable(provider, "session_required"), nil
	}

	if !nativeamp.HasAPIKey(composeEnv(a.nativeEnvironmentBase(), environment)) {
		return rateLimitsUnavailable(provider, "not_authenticated"), nil
	}

	readCtx, cancel := context.WithTimeout(ctx, defaultNativeCommandTimeout)
	defer cancel()

	result, readErr := a.acquireRateLimits(readCtx, request.SessionID, session, cancel)

	_, currentEnvironment, err := a.rateLimitsScope(ctx, request.SessionID, session)
	if err != nil {
		return RateLimitsResponse{}, errors.Join(err, readErr)
	}

	if readErr != nil {
		return RateLimitsResponse{}, readErr
	}

	if !maps.Equal(environment, currentEnvironment) {
		return rateLimitsUnavailable(provider, "read_failed"), nil
	}

	return normalizeRateLimits(result), nil
}

// rateLimitsScope checks ownership without acquiring the prompt turn or making
// a new session. The session pointer fences replacement as well as deletion.
func (a *Agent) rateLimitsScope(ctx context.Context, id acp.SessionId, expected *agentSession) (*agentSession, map[string]string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	ownerErr := errors.Join(a.optionsError(), rateLimitsBoundaryError(a.lifecycleContainmentErr))
	if a.closed {
		ownerErr = errors.Join(ownerErr, acp.NewInvalidRequest(map[string]any{jsonFieldError: agentClosedMessage}))
	}

	if id == "" {
		return nil, cloneStringMap(a.options.Env), errors.Join(ownerErr, ctx.Err())
	}

	session := a.sessions[id]
	_, deleted := a.deleted[id]

	use := a.sessionUses[id]
	if session == nil || deleted || a.sessionFlights[id] != nil || use != nil && use.replacing || expected != nil && session != expected {
		return nil, nil, errors.Join(ownerErr, unknownSessionError(), ctx.Err())
	}

	session.mu.Lock()
	defer session.mu.Unlock()

	ownerErr = errors.Join(ownerErr, rateLimitsBoundaryError(session.scratchContainmentErr))
	if session.closed {
		ownerErr = errors.Join(ownerErr, unknownSessionError())
	}

	if session.poisonCause != "" {
		ownerErr = errors.Join(ownerErr, sessionPoisoned(session.poisonCause))
	}

	return session, cloneStringMap(session.env), errors.Join(ownerErr, ctx.Err())
}

func rateLimitsBoundaryError(err error) error {
	if err == nil {
		return nil
	}

	return nativeInternalError(classContainmentIncomplete, err)
}

func normalizeRateLimits(result nativeamp.ProviderQuota) RateLimitsResponse {
	if result.Availability != "available" {
		return RateLimitsResponse{
			ProviderID: authProviderID, Availability: result.Availability,
			Reason: result.Reason, Pools: []RateLimitPool{},
		}
	}

	response := RateLimitsResponse{ProviderID: authProviderID, Availability: "available", Pools: []RateLimitPool{}}
	observedAt := result.ObservedAt.Format(time.RFC3339Nano)

	if subscription := result.Subscription; subscription != nil {
		response.Pools = append(response.Pools, RateLimitPool{
			ID: "amp-subscription", Label: "Amp subscription", PlanType: subscription.Plan,
			Windows: []RateLimitWindow{
				{ID: rateLimitsOtherWindowID, UsedPercent: &subscription.OtherUsedPercent, ObservedAt: observedAt},
				{ID: "orb", UsedPercent: &subscription.OrbUsedPercent, ObservedAt: observedAt},
			},
		})
	}

	if result.IndividualRemainingUSD != nil {
		response.Pools = append(response.Pools, RateLimitPool{
			ID: "amp-individual-credits", Label: "Amp individual credits", Windows: []RateLimitWindow{},
			Balances: []RateLimitBalance{{
				ID: "credits", Remaining: &RateLimitMoney{Amount: *result.IndividualRemainingUSD, Currency: "USD"},
				ObservedAt: observedAt,
			}},
		})
	}

	return response
}
