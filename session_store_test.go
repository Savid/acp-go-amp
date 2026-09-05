//nolint:gocyclo,govet // Store contract edge tests intentionally keep setup in one scenario.
package ampacp

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	ampnative "github.com/savid/acp-go-amp/internal/amp"
	"github.com/stretchr/testify/require"
)

func TestInMemoryStoreReplaceAppendDelete(t *testing.T) {
	ctx := context.Background()
	store := NewInMemorySessionStore()
	main := SessionKey{SessionID: "T-1", Subpath: SessionStoreMainSubpath}
	transcript := SessionKey{SessionID: "T-1", Subpath: transcriptSubpath}
	manifest, _ := json.Marshal(ampManifest{Format: SessionStoreFormat, SessionID: "T-1", NativeSessionID: "T-1", Cwd: "/tmp", Env: map[string]string{}, UpdatedAtUnixMilli: 2})
	if err := store.Replace(ctx, main, []SessionStoreReplacement{{Key: main, Entries: []SessionStoreEntry{manifest}}}); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, transcript, []SessionStoreEntry{json.RawMessage(`{"type":"result"}`)}); err != nil {
		t.Fatal(err)
	}
	entries, err := store.Load(ctx, transcript)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("entries=%d", len(entries))
	}
	summaries, err := store.ListSessions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(summaries) != 1 || summaries[0].SessionID != "T-1" {
		t.Fatalf("summaries=%+v", summaries)
	}
	if deleteErr := store.Delete(ctx, main); deleteErr != nil {
		t.Fatal(deleteErr)
	}
	entries, err = store.Load(ctx, transcript)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatal("delete did not cascade")
	}
}

// TestInMemoryStoreRefusesForeignAndDuplicateReplacementKeys pins the two
// generation rules a Replace enforces before it writes anything.
//
// Every Replace is one session's: a replacement key naming another session is
// refused, because one session's generation may never rewrite or tombstone
// another's rows. And one generation states each key exactly once: a key stated
// twice would make slice position decide what the session holds, so it is
// refused by the duplicated key's own name rather than resolved by last-write-
// wins. Both errors name the offending {sessionId, subpath} in full — a subpath
// alone does not identify a row.
//
// The refusal is the whole effect. Validation completes before the store takes
// its lock, so nothing is written, nothing is deleted, nothing is tombstoned,
// and the generation the store already holds stands untouched.
func TestInMemoryStoreRefusesForeignAndDuplicateReplacementKeys(t *testing.T) {
	ctx := context.Background()
	store := NewInMemorySessionStore()
	main := SessionKey{SessionID: "T-1", Subpath: SessionStoreMainSubpath}
	transcript := SessionKey{SessionID: "T-1", Subpath: transcriptSubpath}
	manifest, _ := json.Marshal(ampManifest{Format: SessionStoreFormat, SessionID: "T-1", NativeSessionID: "T-1", Cwd: "/tmp", Env: map[string]string{}, UpdatedAtUnixMilli: 2})
	committed := json.RawMessage(`{"type":"committed"}`)

	if err := store.Replace(ctx, main, []SessionStoreReplacement{
		{Key: main, Entries: []SessionStoreEntry{manifest}},
		{Key: transcript, Entries: []SessionStoreEntry{committed}},
	}); err != nil {
		t.Fatalf("first generation: %v", err)
	}

	foreignMain := SessionKey{SessionID: "T-2", Subpath: SessionStoreMainSubpath}
	foreignTranscript := SessionKey{SessionID: "T-2", Subpath: transcriptSubpath}

	for _, testCase := range []struct {
		name         string
		replacements []SessionStoreReplacement
		wantMessage  string
	}{
		{
			name: "foreign session subpath",
			replacements: []SessionStoreReplacement{
				{Key: main, Entries: []SessionStoreEntry{manifest}},
				{Key: foreignTranscript, Entries: []SessionStoreEntry{json.RawMessage(`{"type":"stolen"}`)}},
			},
			wantMessage: `replacement key {sessionId: "T-2", subpath: "transcript"} does not belong to session "T-1"`,
		},
		{
			name: "foreign session main",
			replacements: []SessionStoreReplacement{
				{Key: foreignMain, Entries: []SessionStoreEntry{manifest}},
			},
			wantMessage: `replacement key {sessionId: "T-2", subpath: ""} does not belong to session "T-1"`,
		},
		{
			name: "duplicate subpath",
			replacements: []SessionStoreReplacement{
				{Key: main, Entries: []SessionStoreEntry{manifest}},
				{Key: transcript, Entries: []SessionStoreEntry{json.RawMessage(`{"type":"first"}`)}},
				{Key: transcript, Entries: []SessionStoreEntry{json.RawMessage(`{"type":"last"}`)}},
			},
			wantMessage: `duplicate replacement key {sessionId: "T-1", subpath: "transcript"}`,
		},
		{
			name: "duplicate main",
			replacements: []SessionStoreReplacement{
				{Key: main, Entries: []SessionStoreEntry{manifest}},
				{Key: main, Entries: []SessionStoreEntry{manifest}},
			},
			wantMessage: `duplicate replacement key {sessionId: "T-1", subpath: ""}`,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			err := store.Replace(ctx, main, testCase.replacements)
			if err == nil || err.Error() != testCase.wantMessage {
				t.Fatalf("replace error = %v, want %q", err, testCase.wantMessage)
			}
		})
	}

	// Nothing a refused generation named was written: this session's own rows are
	// exactly what the last accepted generation left, and the foreign session the
	// refused keys named was never created.
	entries, err := store.Load(ctx, transcript)
	if err != nil {
		t.Fatalf("load transcript: %v", err)
	}

	if len(entries) != 1 || string(entries[0]) != string(committed) {
		t.Fatalf("a refused generation changed the transcript: %s", entries)
	}

	mainEntries, err := store.Load(ctx, main)
	if err != nil {
		t.Fatalf("load main: %v", err)
	}

	if len(mainEntries) != 1 || string(mainEntries[0]) != string(manifest) {
		t.Fatalf("a refused generation changed the manifest: %s", mainEntries)
	}

	for _, key := range []SessionKey{foreignMain, foreignTranscript} {
		foreign, loadErr := store.Load(ctx, key)
		if loadErr != nil {
			t.Fatalf("load %v: %v", key, loadErr)
		}

		if len(foreign) != 0 {
			t.Fatalf("a refused generation wrote to another session at %v: %s", key, foreign)
		}
	}

	sessions, err := store.ListSessions(ctx)
	if err != nil {
		t.Fatalf("list sessions: %v", err)
	}

	if len(sessions) != 1 || sessions[0].SessionID != "T-1" {
		t.Fatalf("a refused generation changed the session list: %#v", sessions)
	}
}

func TestInMemoryStoreContractEdges(t *testing.T) {
	ctx := context.Background()
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()
	store := NewInMemorySessionStore()
	main1 := SessionKey{SessionID: "T-1", Subpath: SessionStoreMainSubpath}
	main2 := SessionKey{SessionID: "T-2", Subpath: SessionStoreMainSubpath}
	transcript := SessionKey{SessionID: "T-1", Subpath: transcriptSubpath}
	side := SessionKey{SessionID: "T-1", Subpath: "side"}

	if err := store.Append(cancelCtx, transcript, []SessionStoreEntry{json.RawMessage(`{}`)}); err == nil {
		t.Fatal("append canceled context succeeded")
	}
	if entries, err := store.Load(cancelCtx, transcript); err == nil || entries != nil {
		t.Fatalf("load canceled = %v, %v", entries, err)
	}
	if err := store.Replace(cancelCtx, main1, nil); err == nil {
		t.Fatal("replace canceled context succeeded")
	}
	if err := store.Delete(cancelCtx, main1); err == nil {
		t.Fatal("delete canceled context succeeded")
	}
	if _, err := store.ListSessions(cancelCtx); err == nil {
		t.Fatal("list sessions canceled context succeeded")
	}
	if _, err := store.ListSubkeys(cancelCtx, main1); err == nil {
		t.Fatal("list subkeys canceled context succeeded")
	}
	if err := store.Append(ctx, transcript, nil); err != nil {
		t.Fatalf("empty append: %v", err)
	}
	if err := store.Replace(ctx, SessionKey{SessionID: "T-1", Subpath: "bad"}, nil); err == nil {
		t.Fatal("replace non-main succeeded")
	}
	if err := store.Replace(ctx, main1, []SessionStoreReplacement{{Key: main2}}); err == nil {
		t.Fatal("replace mismatched session succeeded")
	}
	if err := store.Replace(ctx, main1, []SessionStoreReplacement{{Key: transcript}}); err == nil {
		t.Fatal("replace without main succeeded")
	}

	manifest1, _ := json.Marshal(ampManifest{Format: SessionStoreFormat, SessionID: "T-1", NativeSessionID: "T-1", Cwd: "/tmp/one", Title: "one", Env: map[string]string{}, UpdatedAtUnixMilli: 10})
	manifest2, _ := json.Marshal(ampManifest{Format: SessionStoreFormat, SessionID: "T-2", NativeSessionID: "T-2", Cwd: "/tmp/two", Title: "two", Env: map[string]string{}, UpdatedAtUnixMilli: 10})
	if err := store.Replace(ctx, main1, []SessionStoreReplacement{
		{Key: main1, Entries: []SessionStoreEntry{manifest1}},
		{Key: transcript, Entries: []SessionStoreEntry{json.RawMessage(`{"type":"assistant"}`)}},
		{Key: side, Entries: []SessionStoreEntry{json.RawMessage(`{"side":true}`)}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Replace(ctx, main2, []SessionStoreReplacement{{Key: main2, Entries: []SessionStoreEntry{manifest2}}}); err != nil {
		t.Fatal(err)
	}
	if err := store.Replace(ctx, SessionKey{SessionID: "bad-json"}, []SessionStoreReplacement{{Key: SessionKey{SessionID: "bad-json"}, Entries: []SessionStoreEntry{json.RawMessage(`{`)}}}); err != nil {
		t.Fatal(err)
	}
	if err := store.Replace(ctx, SessionKey{SessionID: "bad-format"}, []SessionStoreReplacement{{Key: SessionKey{SessionID: "bad-format"}, Entries: []SessionStoreEntry{json.RawMessage(`{"format":"wrong","threadId":"bad-format"}`)}}}); err != nil {
		t.Fatal(err)
	}
	summaries, err := store.ListSessions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 0, len(summaries))
	for _, summary := range summaries {
		ids = append(ids, summary.SessionID)
	}
	slices.Sort(ids)
	// Committed main keys always list; rows that are not valid amp manifests
	// still yield key-based summaries without cwd/title enrichment.
	if !slices.Equal(ids, []string{"T-1", "T-2", "bad-format", "bad-json"}) {
		t.Fatalf("summaries = %#v", summaries)
	}
	for _, summary := range summaries {
		if summary.UpdatedAtUnixMilli <= 0 {
			t.Fatalf("summary %q missing tracked updatedAt: %#v", summary.SessionID, summary)
		}
		if summary.Meta != nil {
			t.Fatalf("summary meta = %#v", summary.Meta)
		}
		switch summary.SessionID {
		case "T-1":
			if summary.Cwd != "/tmp/one" || summary.Title != "one" {
				t.Fatalf("manifest summary = %#v", summary)
			}
		case "bad-json", "bad-format":
			if summary.Cwd != "" || summary.Title != "" {
				t.Fatalf("key-based summary enriched: %#v", summary)
			}
		}
	}
	subkeys, err := store.ListSubkeys(ctx, main1)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(subkeys, []string{"side", transcriptSubpath}) {
		t.Fatalf("subkeys = %#v", subkeys)
	}
	if err := store.Delete(ctx, side); err != nil {
		t.Fatal(err)
	}
	subkeys, _ = store.ListSubkeys(ctx, main1)
	if !slices.Equal(subkeys, []string{transcriptSubpath}) {
		t.Fatalf("subkeys after side delete = %#v", subkeys)
	}
	if err := store.Delete(ctx, main1); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, transcript, []SessionStoreEntry{json.RawMessage(`{"ignored":true}`)}); err != nil {
		t.Fatal(err)
	}
	entries, err := store.Load(ctx, transcript)
	if err != nil || len(entries) != 0 {
		t.Fatalf("tombstoned append visible: entries=%d err=%v", len(entries), err)
	}
	if cloneRaw(nil) != nil {
		t.Fatal("nil raw clone changed")
	}
	if manifest, ok := manifestFromStoreEntry(json.RawMessage(`{"format":"wrong"}`)); ok || manifest.SessionID != "" {
		t.Fatalf("bad manifest = %#v ok=%v", manifest, ok)
	}
	if _, ok := manifestFromStoreEntry(json.RawMessage(`{`)); ok {
		t.Fatal("malformed manifest accepted")
	}
	if _, ok := manifestFromStoreEntry(json.RawMessage(`{"format":"amp-thread-mirror-v1","sessionId":"T-1","nativeSessionId":"T-1","cwd":1,"env":{}}`)); ok {
		t.Fatal("manifest with a malformed typed field accepted")
	}
	if _, ok := manifestFromStoreEntry(json.RawMessage(`{"format":"amp-thread-mirror-v1","sessionId":"T-1","nativeSessionId":"T-1"}`)); ok {
		t.Fatal("manifest without current env state accepted")
	}
	overlongManifest, _ := json.Marshal(ampManifest{
		Format: SessionStoreFormat, SessionID: "s-1",
		NativeSessionID: "T-" + strings.Repeat("x", ampnative.MaxThreadIDBytes),
		Env:             map[string]string{},
	})
	if _, ok := manifestFromStoreEntry(overlongManifest); ok {
		t.Fatal("overlong native thread id manifest accepted")
	}
	missingSession, _ := json.Marshal(ampManifest{Format: SessionStoreFormat, NativeSessionID: "T-1", Env: map[string]string{}})
	if _, ok := manifestFromStoreEntry(missingSession); ok {
		t.Fatal("manifest without session id accepted")
	}
}

func TestInMemoryStoreEmptySessionIDSemantics(t *testing.T) {
	ctx := context.Background()
	store := NewInMemorySessionStore()
	main := SessionKey{SessionID: "T-1", Subpath: SessionStoreMainSubpath}
	manifest, _ := json.Marshal(ampManifest{Format: SessionStoreFormat, SessionID: "T-1", NativeSessionID: "T-1", Cwd: "/tmp", Env: map[string]string{}, UpdatedAtUnixMilli: 2})
	if err := store.Replace(ctx, main, []SessionStoreReplacement{{Key: main, Entries: []SessionStoreEntry{manifest}}}); err != nil {
		t.Fatal(err)
	}

	// Append and Replace require a session id.
	if err := store.Append(ctx, SessionKey{}, []SessionStoreEntry{json.RawMessage(`{}`)}); err == nil || !strings.Contains(err.Error(), "session id is required") {
		t.Fatalf("empty-id append = %v", err)
	}
	if err := store.Replace(ctx, SessionKey{}, []SessionStoreReplacement{{Key: SessionKey{}, Entries: nil}}); err == nil || !strings.Contains(err.Error(), "session id is required") {
		t.Fatalf("empty-id replace = %v", err)
	}

	// Delete of an empty-id key is a pure no-op: no error, no tombstone.
	if err := store.Delete(ctx, SessionKey{}); err != nil {
		t.Fatalf("empty-id delete = %v", err)
	}
	summaries, err := store.ListSessions(ctx)
	if err != nil || len(summaries) != 1 {
		t.Fatalf("summaries after empty-id delete = %#v err=%v", summaries, err)
	}
}

func TestInMemoryStoreZeroEntryMainListsAndAppendBumpsOrder(t *testing.T) {
	ctx := context.Background()
	store := NewInMemorySessionStore()
	mainEmpty := SessionKey{SessionID: "T-empty", Subpath: SessionStoreMainSubpath}
	mainOther := SessionKey{SessionID: "T-other", Subpath: SessionStoreMainSubpath}

	// A Replace-listed main key with zero entries stays visible in ListSessions.
	if err := store.Replace(ctx, mainEmpty, []SessionStoreReplacement{{Key: mainEmpty, Entries: nil}}); err != nil {
		t.Fatal(err)
	}
	summaries, err := store.ListSessions(ctx)
	if err != nil || len(summaries) != 1 || summaries[0].SessionID != "T-empty" {
		t.Fatalf("zero-entry main not listed: %#v err=%v", summaries, err)
	}

	// Append bumps the tracked updatedAt so ordering between sessions changes.
	if err := store.Replace(ctx, mainOther, []SessionStoreReplacement{{Key: mainOther, Entries: nil}}); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.updatedAt[mainEmpty] = 10
	store.updatedAt[mainOther] = 20
	store.mu.Unlock()

	summaries, _ = store.ListSessions(ctx)
	if len(summaries) != 2 || summaries[0].SessionID != "T-other" {
		t.Fatalf("pre-append order = %#v", summaries)
	}

	before := time.Now().UnixMilli()
	if err := store.Append(ctx, mainEmpty, []SessionStoreEntry{json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	summaries, _ = store.ListSessions(ctx)
	if len(summaries) != 2 || summaries[0].SessionID != "T-empty" || summaries[0].UpdatedAtUnixMilli < before {
		t.Fatalf("append did not bump order: %#v", summaries)
	}
}

func TestInMemoryStoreNilReceiverGuards(t *testing.T) {
	ctx := context.Background()

	var nilStore *InMemorySessionStore
	if err := nilStore.Replace(ctx, SessionKey{SessionID: "T-1"}, nil); err == nil {
		t.Fatal("nil-receiver Replace did not error")
	}
	if err := nilStore.Delete(ctx, SessionKey{SessionID: "T-1"}); err == nil {
		t.Fatal("nil-receiver Delete did not error")
	}
	if _, err := nilStore.ListSessions(ctx); err == nil {
		t.Fatal("nil-receiver ListSessions did not error")
	}
	if _, err := nilStore.ListSubkeys(ctx, SessionKey{SessionID: "T-1"}); err == nil {
		t.Fatal("nil-receiver ListSubkeys did not error")
	}
}

func TestInMemoryStoreReplaceEmptyEntryKeySurvives(t *testing.T) {
	ctx := context.Background()
	store := NewInMemorySessionStore()
	main := SessionKey{SessionID: "T-1", Subpath: SessionStoreMainSubpath}
	transcript := SessionKey{SessionID: "T-1", Subpath: transcriptSubpath}
	manifest, _ := json.Marshal(ampManifest{Format: SessionStoreFormat, SessionID: "T-1", NativeSessionID: "T-1", Cwd: "/tmp", Env: map[string]string{}, UpdatedAtUnixMilli: 3})

	// A Replace that lists a subkey with an empty Entries slice must keep that
	// key live (present in Load and ListSubkeys), not tombstone it.
	if err := store.Replace(ctx, main, []SessionStoreReplacement{
		{Key: main, Entries: []SessionStoreEntry{manifest}},
		{Key: transcript, Entries: nil},
	}); err != nil {
		t.Fatal(err)
	}

	entries, err := store.Load(ctx, transcript)
	if err != nil {
		t.Fatalf("load empty-entry key: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("empty-entry key entries = %d, want 0", len(entries))
	}

	subkeys, err := store.ListSubkeys(ctx, main)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(subkeys, transcriptSubpath) {
		t.Fatalf("empty-entry listed key was dropped: subkeys=%#v", subkeys)
	}

	// Appending to the surviving key must succeed (it is not tombstoned).
	if err := store.Append(ctx, transcript, []SessionStoreEntry{json.RawMessage(`{"type":"result"}`)}); err != nil {
		t.Fatalf("append to surviving key: %v", err)
	}
	entries, err = store.Load(ctx, transcript)
	if err != nil || len(entries) != 1 {
		t.Fatalf("append after survive: entries=%d err=%v", len(entries), err)
	}
}

func TestInMemoryStoreZeroValueSelfHeals(t *testing.T) {
	ctx := context.Background()
	main := SessionKey{SessionID: "T-1", Subpath: SessionStoreMainSubpath}
	transcript := SessionKey{SessionID: "T-1", Subpath: transcriptSubpath}
	manifest, _ := json.Marshal(ampManifest{Format: SessionStoreFormat, SessionID: "T-1", NativeSessionID: "T-1", Cwd: "/tmp", Env: map[string]string{}, UpdatedAtUnixMilli: 4})

	// A zero-value store has nil maps; write paths must self-heal, not panic.
	store := &InMemorySessionStore{}
	if err := store.Replace(ctx, main, []SessionStoreReplacement{
		{Key: main, Entries: []SessionStoreEntry{manifest}},
	}); err != nil {
		t.Fatalf("zero-value replace: %v", err)
	}
	if err := store.Append(ctx, transcript, []SessionStoreEntry{json.RawMessage(`{"type":"result"}`)}); err != nil {
		t.Fatalf("zero-value append: %v", err)
	}
	if err := store.Delete(ctx, transcript); err != nil {
		t.Fatalf("zero-value delete: %v", err)
	}

	// A nil receiver returns an error rather than panicking.
	var nilStore *InMemorySessionStore
	if err := nilStore.Append(ctx, transcript, []SessionStoreEntry{json.RawMessage(`{}`)}); err == nil {
		t.Fatal("nil-receiver Append did not error")
	}
	if _, err := nilStore.Load(ctx, transcript); err == nil {
		t.Fatal("nil-receiver Load did not error")
	}
}

// TestInMemoryStoreTombstoneFinality pins the finality the store owes on its
// own: neither an append nor a replacement addressed to a tombstoned session
// writes anything or clears anything, and both answer success because the
// deleted state is already the caller's answer. An adapter-level deletion
// marker is not a substitute — a write that is already in flight when the
// tombstone lands reaches the store regardless of what the adapter believes.
func TestInMemoryStoreTombstoneFinality(t *testing.T) {
	ctx := context.Background()
	store := NewInMemorySessionStore()
	main := SessionKey{SessionID: "T-1", Subpath: SessionStoreMainSubpath}
	transcript := SessionKey{SessionID: "T-1", Subpath: transcriptSubpath}
	manifest, _ := json.Marshal(ampManifest{Format: SessionStoreFormat, SessionID: "T-1", NativeSessionID: "T-1", Cwd: "/tmp", Env: map[string]string{}, UpdatedAtUnixMilli: 5})

	if err := store.Replace(ctx, main, []SessionStoreReplacement{
		{Key: main, Entries: []SessionStoreEntry{manifest}},
		{Key: transcript, Entries: []SessionStoreEntry{json.RawMessage(`{"type":"result"}`)}},
	}); err != nil {
		t.Fatalf("seed replace: %v", err)
	}

	if err := store.Delete(ctx, main); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Append over the tombstone: writes nothing, clears nothing, succeeds.
	if err := store.Append(ctx, main, []SessionStoreEntry{manifest}); err != nil {
		t.Fatalf("append over a tombstone: %v", err)
	}
	if err := store.Append(ctx, transcript, []SessionStoreEntry{json.RawMessage(`{"type":"late"}`)}); err != nil {
		t.Fatalf("append over a cascaded tombstone: %v", err)
	}

	// Replace over the tombstone: the same three answers. This is the write a
	// commit racing a delete makes, and the one that durably resurrects a
	// deleted session when the store does not refuse it itself.
	if err := store.Replace(ctx, main, []SessionStoreReplacement{
		{Key: main, Entries: []SessionStoreEntry{manifest}},
		{Key: transcript, Entries: []SessionStoreEntry{json.RawMessage(`{"type":"late"}`)}},
	}); err != nil {
		t.Fatalf("replace over a tombstone: %v", err)
	}

	for _, key := range []SessionKey{main, transcript} {
		entries, err := store.Load(ctx, key)
		if err != nil {
			t.Fatalf("load %q after refused writes: %v", key.Subpath, err)
		}
		if len(entries) != 0 {
			t.Fatalf("tombstoned key %q holds %d entries after refused writes", key.Subpath, len(entries))
		}
	}

	summaries, err := store.ListSessions(ctx)
	if err != nil {
		t.Fatalf("list sessions: %v", err)
	}
	if len(summaries) != 0 {
		t.Fatalf("a refused write relisted a deleted session: %+v", summaries)
	}

	subkeys, err := store.ListSubkeys(ctx, main)
	if err != nil {
		t.Fatalf("list subkeys: %v", err)
	}
	if len(subkeys) != 0 {
		t.Fatalf("a refused write relisted a deleted subpath: %#v", subkeys)
	}

	// A different session is untouched by the first one's tombstone.
	other := SessionKey{SessionID: "T-2", Subpath: SessionStoreMainSubpath}
	otherManifest, _ := json.Marshal(ampManifest{Format: SessionStoreFormat, SessionID: "T-2", NativeSessionID: "T-2", Cwd: "/tmp", Env: map[string]string{}, UpdatedAtUnixMilli: 6})
	if err := store.Replace(ctx, other, []SessionStoreReplacement{{Key: other, Entries: []SessionStoreEntry{otherManifest}}}); err != nil {
		t.Fatalf("replace an untombstoned session: %v", err)
	}
	entries, err := store.Load(ctx, other)
	if err != nil || len(entries) != 1 {
		t.Fatalf("untombstoned replace: entries=%d err=%v", len(entries), err)
	}
}

type residualHookStore struct {
	*InMemorySessionStore
	afterTranscriptLoad func()
	afterReplace        func()
	failReplace         bool
}

func (s *residualHookStore) Load(ctx context.Context, key SessionKey) ([]SessionStoreEntry, error) {
	entries, err := s.InMemorySessionStore.Load(ctx, key)
	if err == nil && key.Subpath == transcriptSubpath && s.afterTranscriptLoad != nil {
		after := s.afterTranscriptLoad
		s.afterTranscriptLoad = nil
		after()
	}

	return entries, err
}

func (s *residualHookStore) Replace(ctx context.Context, main SessionKey, replacements []SessionStoreReplacement) error {
	if s.failReplace {
		return errors.New("replace refused")
	}
	if err := s.InMemorySessionStore.Replace(ctx, main, replacements); err != nil {
		return err
	}
	if s.afterReplace != nil {
		after := s.afterReplace
		s.afterReplace = nil
		after()
	}

	return nil
}

func TestColdCarrierPersistenceResidualCallSites(t *testing.T) {
	for _, phase := range []string{"persist", "validate"} {
		t.Run(phase, func(t *testing.T) {
			path, _ := fakeAgentAmpPath(t, "")
			store := &residualHookStore{InMemorySessionStore: NewInMemorySessionStore()}
			agent := newTestAgent(
				WithExecutablePath(path),
				WithScratchDir(testScratchDir(t)),
				WithSessionStore(store),
			)
			cwd := t.TempDir()
			created, err := agent.NewSession(t.Context(), NewSessionRequest(cwd,
				WithSessionAmpOptions(NewAmpOptions(WithAmpEnv(map[string]string{"AMP_API_KEY": "old"}))),
			))
			require.NoError(t, err)
			_, err = agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: created.SessionId})
			require.NoError(t, err)

			switch phase {
			case "persist":
				store.failReplace = true
			case "validate":
				store.afterReplace = func() {
					agent.mu.Lock()
					agent.sessionUses[created.SessionId] = &agentSessionUse{}
					agent.mu.Unlock()
				}
			}
			_, err = agent.ResumeSession(t.Context(), ResumeSessionRequest(created.SessionId, cwd,
				WithSessionAmpOptions(NewAmpOptions(WithAmpEnv(map[string]string{"AMP_API_KEY": "new"}))),
			))
			require.Error(t, err)
		})
	}
}

func TestManifestStrictDecodeAcceptsTheCurrentTypedShape(t *testing.T) {
	entry := json.RawMessage(`{"format":"amp-thread-mirror-v1","sessionId":"T-1","nativeSessionId":"T-1","cwd":"/cwd","env":{},"updatedAtUnixMilli":1,"createdAtUnixMilli":1}`)
	manifest, ok := manifestFromStoreEntry(entry)
	require.True(t, ok)
	require.Equal(t, "T-1", manifest.SessionID)
}
