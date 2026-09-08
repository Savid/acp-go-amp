package amp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestQuotaDisplayNormalization(t *testing.T) {
	t.Parallel()
	observed := time.Now().UTC()
	result := parseQuotaDisplay("Signed in as private-account@example.test (private account)\nSubscription Megawatt: 97% other usage and 100% orb usage remaining - resets upon renewal in 29 days\nIndividual credits: $0 remaining - https://ampcode.com/settings", observed)
	require.Equal(t, quotaAvailable, result.Availability)
	require.Equal(t, observed, result.ObservedAt)
	require.Equal(t, 0.0, *result.IndividualRemainingUSD)
	require.Equal(t, &SubscriptionQuota{Plan: "Megawatt", OtherUsedPercent: 3, OrbUsedPercent: 0}, result.Subscription)
	encoded, err := json.Marshal(result)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "private-account")

	for _, test := range []struct {
		source string
		want   float64
	}{
		{"$1,234.56", 1234.56}, {"-$0.05", -0.05}, {"$0.00", 0},
	} {
		parsed := parseQuotaDisplay("Individual credits: "+test.source+" remaining - https://ampcode.com/settings", observed)
		require.Equal(t, quotaAvailable, parsed.Availability, test.source)
		require.Equal(t, test.want, *parsed.IndividualRemainingUSD)
	}

	overspent := parseQuotaDisplay("Subscription New Plan 2: -5.5% other usage and 0% orb usage remaining - resets upon renewal in 1 day", observed)
	require.Equal(t, quotaAvailable, overspent.Availability)
	require.Equal(t, 105.5, overspent.Subscription.OtherUsedPercent)
	require.Equal(t, 100.0, overspent.Subscription.OrbUsedPercent)
}

func TestQuotaDisplayRefusesAmbiguousOrUnobservedFormats(t *testing.T) {
	t.Parallel()
	valid := "Individual credits: $0 remaining - https://ampcode.com/settings"
	for _, display := range []string{
		"Individual credits: 15 credits remaining - https://ampcode.com/settings",
		"Individual credits: €15 remaining - https://ampcode.com/settings", "Individual credits: $NaN remaining - https://ampcode.com/settings",
		"Individual credits: $1,23.00 remaining - https://ampcode.com/settings", "Individual credits: $" + strings.Repeat("9", 400) + " remaining - https://ampcode.com/settings",
		valid + "\n" + valid, valid + "\nWorkspace credits: $10 remaining", valid + "\nUnknown allowance: 1% remaining",
		"Subscription Megawatt: 101% other usage and 100% orb usage remaining - resets upon renewal in 29 days",
		"Subscription Megawatt: 97% other usage and 100% orb usage remaining - resets tomorrow",
		"Subscription private@example.test: 97% other usage and 100% orb usage remaining - resets upon renewal in 29 days",
		"Subscription Plan  With Spaces: 97% other usage and 100% orb usage remaining - resets upon renewal in 29 days",
		"Subscription " + strings.Repeat("A", 65) + ": 97% other usage and 100% orb usage remaining - resets upon renewal in 29 days",
	} {
		require.Equal(t, quotaFailure(), parseQuotaDisplay(display, time.Now()), display)
	}
}

func TestQuotaNativeMarkdownAndUnobserved(t *testing.T) {
	t.Parallel()
	result := parseQuotaDisplay("Signed in as private@example.test (private)\n**Individual credits:** $0 remaining - https://ampcode.com/settings\n"+quotaDetailsHint+"\n", time.Now())
	require.Equal(t, quotaAvailable, result.Availability)
	require.Equal(t, 0.0, *result.IndividualRemainingUSD)
	for _, display := range []string{"", "Signed in as private@example.test\n", "\n" + quotaDetailsHint + "\n"} {
		require.Equal(t, ProviderQuota{Availability: quotaUnavailable, Reason: "not_observed"}, parseQuotaDisplay(display, time.Now()))
	}
}

func TestQuotaNativeUsesEffectiveContext(t *testing.T) {
	t.Parallel()
	calls := 0
	client := NewClient(nil, Options{
		ResolvedExecutable: absTestPath("verified", "amp"),
		Cwd:                absTestPath("project"), SettingsFile: absTestPath("session", "settings.json"),
		OrdinaryEnvironment: map[string]string{AuthAPIKeyEnv: "ordinary"},
		NativeEnvironment:   map[string]string{AuthAPIKeyEnv: "native", "PATH": "static-path"},
		Env:                 map[string]string{AuthAPIKeyEnv: "session", AuthDeploymentEnv: "https://custom-native.test", envHome: "session-home"},
		StartNative: func(ctx context.Context, request NativeRequest) (NativeProcess, error) {
			calls++
			require.Equal(t, absTestPath("verified", "amp"), request.Executable)
			require.Equal(t, absTestPath("project"), request.WorkingDirectory)
			require.Equal(t, []string{"--no-ide", "--no-color", "--no-notifications", "--settings-file", absTestPath("session", "settings.json"), "usage"}, request.Arguments)
			environment := environmentMap(request.Environment)
			require.Equal(t, "session", environment[AuthAPIKeyEnv])
			require.Equal(t, "https://custom-native.test", environment[AuthDeploymentEnv])
			require.Equal(t, "session-home", environment[envHome])
			require.Equal(t, "static-path", environment["PATH"])
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.LessOrEqual(t, time.Until(deadline), quotaReadTimeout)
			process := newCoverageNativeProcess()
			process.stdout = io.NopCloser(strings.NewReader("**Individual credits:** $0 remaining - https://ampcode.com/settings\n"))

			return process, nil
		},
	})
	result, err := client.ReadProviderQuota(t.Context())
	require.NoError(t, err)
	require.Equal(t, quotaAvailable, result.Availability)
	require.Equal(t, 1, calls)
}

func TestQuotaNativeNoCredentialDoesNotLaunch(t *testing.T) {
	t.Parallel()
	for _, key := range []string{"", "  "} {
		client := NewClient(nil, Options{NativeEnvironment: map[string]string{}, Env: map[string]string{AuthAPIKeyEnv: key}, StartNative: func(context.Context, NativeRequest) (NativeProcess, error) {
			t.Fatal("missing credential launched native")

			return nil, errors.New("unexpected native dispatch")
		}})
		result, err := client.ReadProviderQuota(t.Context())
		require.NoError(t, err)
		require.Equal(t, ProviderQuota{Availability: quotaUnavailable, Reason: "not_authenticated"}, result)
	}
}

func TestQuotaNativeFailuresAndOutputBounds(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, stdout, stderr string
		exitCode             int
		wantErr              bool
	}{
		{"unknown format", "New private allowance: 5 credits", "", 0, false},
		{"oversized stdout", strings.Repeat("x", quotaBodyLimit+1), "", 0, true},
		{"oversized stderr", "Individual credits: $0 remaining - https://ampcode.com/settings", strings.Repeat("x", quotaBodyLimit+1), 0, true},
		{"rejected account", "", "private credential and account details", 1, true},
	} {
		client := NewClient(nil, Options{ResolvedExecutable: absTestPath("verified", "amp"), NativeEnvironment: map[string]string{}, Env: map[string]string{AuthAPIKeyEnv: "private-key"}, StartNative: func(context.Context, NativeRequest) (NativeProcess, error) {
			process := newCoverageNativeProcess()
			process.stdout = io.NopCloser(strings.NewReader(test.stdout))
			process.stderr = io.NopCloser(strings.NewReader(test.stderr))
			process.wait = func(context.Context) (NativeResult, error) { return NativeResult{ExitCode: test.exitCode}, nil }

			return process, nil
		}})
		result, err := client.ReadProviderQuota(t.Context())
		require.Equal(t, quotaFailure(), result, test.name)
		if test.wantErr {
			require.Error(t, err)
			require.NotContains(t, err.Error(), "private")
		} else {
			require.NoError(t, err)
		}
	}
}

func TestQuotaNativeCancellationPreservesBoundary(t *testing.T) {
	t.Parallel()
	for _, uncertain := range []bool{false, true} {
		ctx, cancel := context.WithCancel(t.Context())
		var waits atomic.Int32
		ownerErr := errors.New("native authority wait failed")
		client := NewClient(nil, Options{ResolvedExecutable: absTestPath("verified", "amp"), NativeEnvironment: map[string]string{}, Env: map[string]string{AuthAPIKeyEnv: "key"}, StartNative: func(context.Context, NativeRequest) (NativeProcess, error) {
			process := newCoverageNativeProcess()
			process.wait = func(waitCtx context.Context) (NativeResult, error) {
				if waits.Add(1) == 1 {
					cancel()
					<-waitCtx.Done()

					return NativeResult{}, waitCtx.Err()
				}
				if uncertain {
					return NativeResult{}, ownerErr
				}

				return NativeResult{Revoked: true}, nil
			}

			return process, nil
		}})
		result, err := client.ReadProviderQuota(ctx)
		require.Equal(t, quotaFailure(), result)
		require.Error(t, err)
		require.Equal(t, int32(2), waits.Load())
		if uncertain {
			require.ErrorIs(t, err, ErrContainmentIncomplete)
			require.ErrorIs(t, err, ownerErr)
		}
	}
}

type quotaReceiptReader struct {
	reader io.Reader
	done   chan struct{}
}

func (r *quotaReceiptReader) Read(data []byte) (int, error) {
	n, err := r.reader.Read(data)
	if errors.Is(err, io.EOF) {
		close(r.done)
	}

	return n, err
}
func (*quotaReceiptReader) Close() error { return nil }

func TestQuotaNativeTimestampPrecedesDelayedWait(t *testing.T) {
	t.Parallel()
	readDone := make(chan struct{})
	waitEntered := make(chan time.Time, 1)
	releaseWait := make(chan struct{})
	client := NewClient(nil, Options{ResolvedExecutable: absTestPath("verified", "amp"), NativeEnvironment: map[string]string{}, Env: map[string]string{AuthAPIKeyEnv: "key"}, StartNative: func(context.Context, NativeRequest) (NativeProcess, error) {
		process := newCoverageNativeProcess()
		process.stdout = &quotaReceiptReader{reader: strings.NewReader("Individual credits: $0 remaining - https://ampcode.com/settings"), done: readDone}
		process.wait = func(context.Context) (NativeResult, error) {
			<-readDone
			waitEntered <- time.Now().UTC()
			<-releaseWait

			return NativeResult{}, nil
		}

		return process, nil
	}})
	type reply struct {
		result ProviderQuota
		err    error
	}
	completed := make(chan reply, 1)
	go func() { result, err := client.ReadProviderQuota(t.Context()); completed <- reply{result, err} }()
	beforeSettlement := <-waitEntered
	close(releaseWait)
	response := <-completed
	require.NoError(t, response.err)
	require.Equal(t, quotaAvailable, response.result.Availability)
	require.False(t, response.result.ObservedAt.IsZero())
	require.True(t, response.result.ObservedAt.Before(beforeSettlement))
}

type quotaPanicReader struct{}

func (quotaPanicReader) Read([]byte) (int, error) { panic("private reader callback") }
func (quotaPanicReader) Close() error             { return nil }

func TestQuotaNativeReaderPanicRetainsContainment(t *testing.T) {
	t.Parallel()
	client := NewClient(nil, Options{ResolvedExecutable: absTestPath("verified", "amp"), NativeEnvironment: map[string]string{}, Env: map[string]string{AuthAPIKeyEnv: "key"}, StartNative: func(context.Context, NativeRequest) (NativeProcess, error) {
		process := newCoverageNativeProcess()
		process.stdout = quotaPanicReader{}

		return process, nil
	}})
	result, err := client.ReadProviderQuota(t.Context())
	require.Equal(t, quotaFailure(), result)
	require.ErrorIs(t, err, ErrContainmentIncomplete)
	require.NotContains(t, err.Error(), "private")
}
