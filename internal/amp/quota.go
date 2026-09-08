package amp

import (
	"context"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	quotaAvailable   = "available"
	quotaUnavailable = "unavailable"
	quotaReadFailed  = "read_failed"
	quotaBodyLimit   = 1024 * 1024
	quotaReadTimeout = 30 * time.Second
	quotaDetailsHint = "# Run `amp usage --details` for more detailed information."
)

var (
	quotaMoneyLine        = regexp.MustCompile(`^Individual credits: (-?\$(?:[0-9]+|[1-9][0-9]{0,2}(?:,[0-9]{3})+)(?:\.[0-9]{1,2})?) remaining - https://ampcode\.com/settings$`)
	quotaSubscriptionLine = regexp.MustCompile(`^Subscription ([A-Za-z][A-Za-z0-9 -]{0,63}): (-?[0-9]+(?:\.[0-9]+)?)% other usage and (-?[0-9]+(?:\.[0-9]+)?)% orb usage remaining - resets upon renewal in [0-9]+ days?$`)
)

// ProviderQuota contains only normalized observations, never account display
// text or the credential that authenticated the read.
type ProviderQuota struct {
	Availability           string
	Reason                 string
	ObservedAt             time.Time
	IndividualRemainingUSD *float64
	Subscription           *SubscriptionQuota
}

// SubscriptionQuota preserves the two separately reported native allowances.
type SubscriptionQuota struct {
	Plan             string
	OtherUsedPercent float64
	OrbUsedPercent   float64
}

// ReadProviderQuota runs the native read-only account command, whose settings
// resolver owns deployment routing and authentication. A delivered nonempty
// AMP_API_KEY bypasses stored credentials and OAuth refresh. No prompt is sent.
// Native boundary errors remain separate from source availability so the owner
// can retain its containment and authority identities.
func (c *Client) ReadProviderQuota(ctx context.Context) (ProviderQuota, error) {
	environment, err := c.buildEnvironment(c.options.Env, c.options.Cwd)
	if err != nil {
		return quotaFailure(), err
	}

	if !HasAPIKey(environmentMap(environment)) {
		return ProviderQuota{Availability: quotaUnavailable, Reason: "not_authenticated"}, nil
	}

	ctx, cancel := context.WithTimeout(ctx, quotaReadTimeout)
	defer cancel()

	bounded := *c

	var observedAt time.Time

	bounded.options.commandOutputLimit = quotaBodyLimit
	bounded.options.commandValuesFree = true

	bounded.options.commandObservedAt = &observedAt
	if bounded.options.ResolvedExecutable == "" {
		path, _, discoveryErr := bounded.discoverVersion(ctx)
		if discoveryErr != nil {
			return quotaFailure(), discoveryErr
		}

		bounded.options.ResolvedExecutable = path
	}

	output, readErr := bounded.output(ctx, "usage")
	if readErr != nil {
		return quotaFailure(), readErr
	}

	if ctx.Err() != nil {
		return quotaFailure(), ctx.Err()
	}

	return parseQuotaDisplay(string(output), observedAt), nil
}

func quotaFailure() ProviderQuota {
	return ProviderQuota{Availability: quotaUnavailable, Reason: quotaReadFailed}
}

func parseQuotaDisplay(display string, observedAt time.Time) ProviderQuota {
	result := ProviderQuota{Availability: quotaAvailable, ObservedAt: observedAt}
	identitySeen := false
	hintSeen := false

	for line := range strings.SplitSeq(display, "\n") {
		if line == "" {
			continue
		}

		if line == quotaDetailsHint && !hintSeen {
			hintSeen = true

			continue
		}

		if after, ok := strings.CutPrefix(line, "**Individual credits:** "); ok {
			line = "Individual credits: " + after
		}

		if strings.HasPrefix(line, "Signed in as ") && !identitySeen {
			identitySeen = true

			continue
		}

		if money := quotaMoneyLine.FindStringSubmatch(line); money != nil {
			amount, err := strconv.ParseFloat(strings.NewReplacer("$", "", ",", "").Replace(money[1]), 64)
			if err != nil || math.IsNaN(amount) || math.IsInf(amount, 0) || result.IndividualRemainingUSD != nil {
				return quotaFailure()
			}

			result.IndividualRemainingUSD = &amount

			continue
		}

		if subscription := quotaSubscriptionLine.FindStringSubmatch(line); subscription != nil {
			parsed, valid := parseSubscriptionQuota(subscription)
			if !valid || result.Subscription != nil {
				return quotaFailure()
			}

			result.Subscription = parsed

			continue
		}

		// A newly introduced allowance must not disappear behind a partially
		// successful result. Unknown source prose is never returned on the wire.
		return quotaFailure()
	}

	if result.Subscription == nil && result.IndividualRemainingUSD == nil {
		return ProviderQuota{Availability: quotaUnavailable, Reason: "not_observed"}
	}

	return result
}

func parseSubscriptionQuota(fields []string) (*SubscriptionQuota, bool) {
	other, otherErr := strconv.ParseFloat(fields[2], 64)

	orb, orbErr := strconv.ParseFloat(fields[3], 64)
	if otherErr != nil || orbErr != nil || other > 100 || orb > 100 || math.IsInf(other, 0) || math.IsInf(orb, 0) {
		return nil, false
	}

	plan := fields[1]
	if plan != strings.TrimSpace(plan) || strings.Contains(plan, "  ") {
		return nil, false
	}

	return &SubscriptionQuota{Plan: plan, OtherUsedPercent: 100 - other, OrbUsedPercent: 100 - orb}, true
}
