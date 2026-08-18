package ampacp

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-amp/internal/lifecycle"
	"github.com/stretchr/testify/require"
)

// gatedStore observes and holds durable commits. Replace announces itself on
// started and waits for release, so a test can hold a settlement open and drive
// a concurrent close or delete against the exact window the races live in.
type gatedStore struct {
	SessionStore

	mu       sync.Mutex
	started  chan struct{}
	release  chan struct{}
	gated    bool
	replaces int
	failWith error
	record   func(string)
}

func newGatedStore(record func(string)) *gatedStore {
	return &gatedStore{
		SessionStore: NewInMemorySessionStore(),
		started:      make(chan struct{}, 8),
		release:      make(chan struct{}),
		record:       record,
	}
}

// gate arms the hold. The early adoption commit lands before it, so a test gates
// only the settlement commit it means to hold.
func (s *gatedStore) gate() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.gated = true
}

func (s *gatedStore) fail(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.failWith = err
}

func (s *gatedStore) replaceCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.replaces
}

func (s *gatedStore) Replace(ctx context.Context, key SessionKey, replacements []SessionStoreReplacement) error {
	s.mu.Lock()
	gated, failWith := s.gated, s.failWith
	s.replaces++
	s.mu.Unlock()

	if s.record != nil {
		s.record("commit")
	}

	if gated {
		s.started <- struct{}{}
		<-s.release
	}

	if failWith != nil {
		return failWith
	}

	return s.SessionStore.Replace(ctx, key, replacements)
}

// settlementLedger records the order settlement steps complete in, so ordering
// is observed rather than asserted.
type settlementLedger struct {
	mu    sync.Mutex
	steps []string
}

func (l *settlementLedger) record(step string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.steps = append(l.steps, step)
}

func (l *settlementLedger) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return append([]string(nil), l.steps...)
}

// settlementAgent opens an agent whose store is gated and whose client records
// the terminal idle into the same ledger the commits write to.
func settlementAgent(t *testing.T, ledger *settlementLedger) (*Agent, *gatedStore, *orderingClient, acp.SessionId) {
	t.Helper()
	t.Setenv("AMP_API_KEY", "conformance-key")

	store := newGatedStore(ledger.record)
	client := &orderingClient{record: ledger.record}

	agent := NewAgent(testContainmentOptions([]Option{
		WithExecutablePath(lifecycleHarness(t)),
		WithScratchDir(testScratchDir(t)),
		WithSessionStore(store),
	})...)
	t.Cleanup(func() { require.NoError(t, agent.Close()) })

	agent.setConnection(client)

	_, err := agent.Initialize(t.Context(), acp.InitializeRequest{Meta: lifecycleOffer(1.0)})
	require.NoError(t, err)

	session, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.NoError(t, err)

	// The ledger starts at the prompt: session creation commits its own manifest
	// generation before any turn exists.
	ledger.mu.Lock()
	ledger.steps = nil
	ledger.mu.Unlock()

	return agent, store, client, session.SessionId
}

// TestSettlementSurvivesRequestCancellation pins that settlement is detached
// from the request context. The store fails a cancelled context immediately, so
// a settlement that reused the request's would lose the commit and the terminal
// boundary while telling the host the turn ended cleanly.
func TestSettlementSurvivesRequestCancellation(t *testing.T) {
	ledger := &settlementLedger{}
	agent, store, client, sessionID := settlementAgent(t, ledger)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	client.onAgentChunk = cancel

	resp, err := agent.Prompt(ctx, lifecyclePrompt(sessionID, "hang", "sub-1", "nonce-1"))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonCancelled, resp.StopReason)

	stored, err := store.Load(t.Context(), SessionKey{SessionID: string(sessionID), Subpath: transcriptSubpath})
	require.NoError(t, err)
	require.NotEmpty(t, stored, "a cancelled request still commits the frames it streamed")

	require.Equal(t, []string{"commit", "commit", "idle"}, ledger.snapshot())

	state := reduceEmittedStream(t, &client.lifecycleClient, negotiatedAnswer()).State()
	require.Len(t, state.Turns, 1)
	require.True(t, state.Turns[0].Terminal)
	require.Equal(t, lifecycle.OutcomeCancelled, state.Turns[0].Outcome)
}

// TestSettlementFailureIsNotACancelledSuccess pins that the cancelled-success
// answer is owed only to a prompt that settled. A commit the store refused is
// the prompt failing, and the host reads that failure rather than a clean cancel
// over durable state the store never took.
func TestSettlementFailureIsNotACancelledSuccess(t *testing.T) {
	ledger := &settlementLedger{}
	agent, store, client, sessionID := settlementAgent(t, ledger)

	outage := errors.New("session store outage")

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	client.onAgentChunk = func() {
		store.fail(outage)
		cancel()
	}

	_, err := agent.Prompt(ctx, lifecyclePrompt(sessionID, "hang", "sub-2", "nonce-2"))
	require.ErrorIs(t, err, outage)
	require.NotContains(t, ledger.snapshot(), "idle",
		"no terminal boundary claims a foreground prefix the store does not hold")

	// The marker is a marker: it neither renames the failure nor hides it from a
	// caller matching on the cause.
	marked := unsettled(outage)
	require.EqualError(t, marked, outage.Error())
	require.ErrorIs(t, marked, outage)
}

// TestDeleteFencesALaterSettlementCommit pins the other half of the delete
// serialization: a settlement that reaches its commit after the tombstone landed
// writes nothing at all, so the frames it held die with the session rather than
// recreating it.
func TestDeleteFencesALaterSettlementCommit(t *testing.T) {
	ledger := &settlementLedger{}
	agent, store, client, sessionID := settlementAgent(t, ledger)

	streamed := make(chan struct{})
	client.onAgentChunk = func() { close(streamed) }

	prompt := make(chan struct{})

	go func() {
		defer close(prompt)

		// A delete interrupts the live native process, and the interrupt's own
		// noise is not what this test is about.
		_, _ = agent.Prompt(t.Context(), lifecyclePrompt(sessionID, "hang", "sub-5", "nonce-5"))
	}()

	<-streamed

	before := store.replaceCount()
	_, _ = agent.UnstableDeleteSession(t.Context(), DeleteSessionRequest(sessionID))
	<-prompt

	require.Equal(t, before, store.replaceCount(), "a fenced session writes nothing back over its tombstone")

	main, err := store.Load(t.Context(), SessionKey{SessionID: string(sessionID), Subpath: SessionStoreMainSubpath})
	require.NoError(t, err)
	require.Empty(t, main)
}

// TestCloseWaitsForFullSettlement pins the completion latch: close waits on a
// prompt that is wholly settled, not on one whose native process merely exited.
// A close that returned at the native terminal would fence a stream the prompt
// is still writing to and answer before the frames the host was shown are
// durable.
func TestCloseWaitsForFullSettlement(t *testing.T) {
	ledger := &settlementLedger{}
	agent, store, client, sessionID := settlementAgent(t, ledger)

	streamed := make(chan struct{})
	client.onAgentChunk = func() { close(streamed) }

	prompt := make(chan struct{})

	go func() {
		defer close(prompt)

		_, err := agent.Prompt(t.Context(), lifecyclePrompt(sessionID, "hang", "sub-3", "nonce-3"))
		require.NoError(t, err)
	}()

	<-streamed
	store.gate()

	closed := make(chan struct{})

	go func() {
		defer close(closed)

		_, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: sessionID})
		require.NoError(t, err)
	}()

	<-store.started

	select {
	case <-closed:
		t.Fatal("close returned while the settlement commit was still held")
	case <-time.After(50 * time.Millisecond):
	}

	close(store.release)
	<-closed
	<-prompt

	ledger.record("closed")
	steps := ledger.snapshot()
	require.Equal(t, []string{"commit", "commit", "idle", "closed"}, steps)
}

// TestDeleteIsNeverResurrectedByALateCommit pins that a delete's tombstone is
// the last write to its row. Replace clears the tombstone of every key it lists,
// so a settlement commit still in flight when the delete arrives would durably
// recreate a session the host was told is gone. The commit is held open across
// the whole delete, which is the exact window the race lives in.
func TestDeleteIsNeverResurrectedByALateCommit(t *testing.T) {
	ledger := &settlementLedger{}
	agent, store, _, sessionID := settlementAgent(t, ledger)

	store.gate()

	prompt := make(chan struct{})

	go func() {
		defer close(prompt)

		_, err := agent.Prompt(t.Context(), lifecyclePrompt(sessionID, "hello", "sub-4", "nonce-4"))
		require.NoError(t, err)
	}()

	// The early adoption commit is the first through the gate; the settlement
	// commit is the second, and it is the one the delete must serialize behind.
	<-store.started
	store.release <- struct{}{}
	<-store.started

	deleted := make(chan error, 1)
	go func() {
		_, err := agent.UnstableDeleteSession(t.Context(), DeleteSessionRequest(sessionID))
		deleted <- err
	}()

	select {
	case err := <-deleted:
		t.Fatalf("delete tombstoned the row while a commit was in flight: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	store.release <- struct{}{}

	require.NoError(t, <-deleted)
	<-prompt

	after := store.replaceCount()

	main, err := store.Load(t.Context(), SessionKey{SessionID: string(sessionID), Subpath: SessionStoreMainSubpath})
	require.NoError(t, err)
	require.Empty(t, main, "the tombstone is the last word on a deleted session")

	transcript, err := store.Load(t.Context(), SessionKey{SessionID: string(sessionID), Subpath: transcriptSubpath})
	require.NoError(t, err)
	require.Empty(t, transcript)

	require.Equal(t, after, store.replaceCount(), "no write recreates the row after delete succeeds")
}
