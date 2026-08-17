package ampacp

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
)

type imageStoreTestDouble struct {
	*InMemorySessionStore
	appendErr       error
	loadErr         error
	deleteErr       error
	listSessionsErr error
	listSubkeysErr  error
	subkeys         []string
	overrideSubkeys bool
}

func (s *imageStoreTestDouble) Append(
	ctx context.Context,
	key SessionKey,
	entries []SessionStoreEntry,
) error {
	if s.appendErr != nil {
		return s.appendErr
	}

	return s.InMemorySessionStore.Append(ctx, key, entries)
}

func (s *imageStoreTestDouble) Load(ctx context.Context, key SessionKey) ([]SessionStoreEntry, error) {
	if s.loadErr != nil {
		return nil, s.loadErr
	}

	return s.InMemorySessionStore.Load(ctx, key)
}

func (s *imageStoreTestDouble) Delete(ctx context.Context, key SessionKey) error {
	if s.deleteErr != nil {
		return s.deleteErr
	}

	return s.InMemorySessionStore.Delete(ctx, key)
}

func (s *imageStoreTestDouble) ListSessions(ctx context.Context) ([]SessionSummary, error) {
	if s.listSessionsErr != nil {
		return nil, s.listSessionsErr
	}

	return s.InMemorySessionStore.ListSessions(ctx)
}

func (s *imageStoreTestDouble) ListSubkeys(ctx context.Context, key SessionKey) ([]string, error) {
	if s.listSubkeysErr != nil {
		return nil, s.listSubkeysErr
	}
	if s.overrideSubkeys {
		return append([]string(nil), s.subkeys...), nil
	}

	return s.InMemorySessionStore.ListSubkeys(ctx, key)
}

func TestImageArtifactPersistenceAndLoadEdges(t *testing.T) {
	ctx := context.Background()
	link := testLinkArtifact("tool:TU:0", "https://cdn.example/image.png")
	key := SessionKey{SessionID: "T-artifact", Subpath: imageArtifactSubpath(link.Identity, link.Fingerprint)}

	loadFailure := &imageStoreTestDouble{
		InMemorySessionStore: NewInMemorySessionStore(),
		loadErr:              errors.New("load failed"),
	}
	if _, err := testArtifactSession(loadFailure, key.SessionID).persistImageArtifact(ctx, link); err == nil {
		t.Fatal("artifact load failure ignored")
	}

	appendFailure := &imageStoreTestDouble{
		InMemorySessionStore: NewInMemorySessionStore(),
		appendErr:            errors.New("append failed"),
	}
	if _, err := testArtifactSession(appendFailure, key.SessionID).persistImageArtifact(ctx, link); err == nil {
		t.Fatal("artifact append failure ignored")
	}

	base := NewInMemorySessionStore()
	stored := link
	stored.Version = imageArtifactVersion
	stored.CreatedAt = time.Now().UnixMilli()
	entry, _ := json.Marshal(stored)
	if err := base.Append(ctx, key, []SessionStoreEntry{entry}); err != nil {
		t.Fatal(err)
	}

	session := testArtifactSession(base, key.SessionID)
	if ref, err := session.persistImageArtifact(ctx, link); err != nil || ref != key.Subpath {
		t.Fatalf("idempotent artifact persist = (%q, %v)", ref, err)
	}

	conflict := link
	conflict.MimeType = imageMIMEJPEG
	if _, err := session.persistImageArtifact(ctx, conflict); err == nil {
		t.Fatal("conflicting artifact accepted")
	}

	equivalent := link
	equivalent.Version = imageArtifactVersion
	if !sameStoredImageArtifact(stored, equivalent) {
		t.Fatal("equivalent artifact records differ")
	}

	if _, err := session.loadImageArtifact(ctx, "transcript"); err == nil {
		t.Fatal("non-artifact reference accepted")
	}
	if _, err := testArtifactSession(loadFailure, key.SessionID).loadImageArtifact(ctx, key.Subpath); err == nil {
		t.Fatal("artifact read failure ignored")
	}
	if _, err := testArtifactSession(NewInMemorySessionStore(), key.SessionID).
		loadImageArtifact(ctx, key.Subpath); err == nil {
		t.Fatal("missing artifact accepted")
	}

	corrupt := NewInMemorySessionStore()
	if err := corrupt.Append(ctx, key, []SessionStoreEntry{json.RawMessage(`{`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := testArtifactSession(corrupt, key.SessionID).loadImageArtifact(ctx, key.Subpath); err == nil {
		t.Fatal("corrupt artifact accepted")
	}

	wrongKey := SessionKey{SessionID: key.SessionID, Subpath: imageArtifactPrefix + "wrong.json"}
	if err := base.Append(ctx, wrongKey, []SessionStoreEntry{entry}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.loadImageArtifact(ctx, wrongKey.Subpath); err == nil {
		t.Fatal("artifact under mismatched key accepted")
	}
}

func TestExpiredImageArtifactDeleteFailure(t *testing.T) {
	originalNow := imageArtifactNow
	t.Cleanup(func() { imageArtifactNow = originalNow })

	now := time.Unix(1_800_000_000, 0)
	imageArtifactNow = func() time.Time { return now }

	artifact := testLinkArtifact("tool:expired:0", "https://cdn.example/expired.png")
	artifact.Version = imageArtifactVersion
	artifact.CreatedAt = now.Add(-imageArtifactTTL - time.Second).UnixMilli()
	key := SessionKey{SessionID: "T-expired-delete", Subpath: imageArtifactSubpath(artifact.Identity, artifact.Fingerprint)}
	entry, _ := json.Marshal(artifact)
	base := NewInMemorySessionStore()
	if err := base.Append(context.Background(), key, []SessionStoreEntry{entry}); err != nil {
		t.Fatal(err)
	}

	store := &imageStoreTestDouble{
		InMemorySessionStore: base,
		deleteErr:            errors.New("delete failed"),
	}
	if _, err := testArtifactSession(store, key.SessionID).
		loadImageArtifact(context.Background(), key.Subpath); err == nil {
		t.Fatal("expired artifact delete failure ignored")
	}

	successStore := NewInMemorySessionStore()
	if err := successStore.Append(context.Background(), key, []SessionStoreEntry{entry}); err != nil {
		t.Fatal(err)
	}
	if _, err := testArtifactSession(successStore, key.SessionID).
		loadImageArtifact(context.Background(), key.Subpath); err == nil {
		t.Fatal("expired artifact accepted")
	}
}

func TestDecodeStoredImageArtifactValidation(t *testing.T) {
	validEmbedded := storedImageArtifact{
		Version:     imageArtifactVersion,
		Kind:        imageArtifactKindEmbedded,
		Identity:    "tool:embedded:0",
		Fingerprint: "checksum",
		MimeType:    imageMIMEPNG,
		Data:        "data",
		CreatedAt:   1,
	}
	validLink := testLinkArtifact("tool:link:0", "https://cdn.example/link.png")
	validLink.Version = imageArtifactVersion
	validLink.CreatedAt = 1

	records := []storedImageArtifact{
		{},
		{
			Version:     imageArtifactVersion,
			Kind:        imageArtifactKindEmbedded,
			Identity:    "tool:embedded:0",
			Fingerprint: "checksum",
			CreatedAt:   1,
		},
		{
			Version:     imageArtifactVersion,
			Kind:        imageArtifactKindLink,
			Identity:    "tool:link:0",
			Fingerprint: "wrong",
			URI:         "file:///tmp/image.png",
			CreatedAt:   1,
		},
		{
			Version:     imageArtifactVersion,
			Kind:        "unknown",
			Identity:    "tool:unknown:0",
			Fingerprint: "checksum",
			CreatedAt:   1,
		},
	}

	if _, err := decodeStoredImageArtifact(json.RawMessage(`{`)); err == nil {
		t.Fatal("malformed artifact record accepted")
	}
	for _, record := range records {
		entry, _ := json.Marshal(record)
		if _, err := decodeStoredImageArtifact(entry); err == nil {
			t.Fatalf("invalid artifact record accepted: %#v", record)
		}
	}

	for _, record := range []storedImageArtifact{validEmbedded, validLink} {
		entry, _ := json.Marshal(record)
		if _, err := decodeStoredImageArtifact(entry); err != nil {
			t.Fatalf("valid artifact rejected: %v", err)
		}
	}
}

func TestImageArtifactReplacementAndSweepEdges(t *testing.T) {
	ctx := context.Background()
	listFailure := &imageStoreTestDouble{
		InMemorySessionStore: NewInMemorySessionStore(),
		listSubkeysErr:       errors.New("list subkeys failed"),
	}
	if _, err := testArtifactSession(listFailure, "T-list-failure").imageArtifactReplacements(ctx); err == nil {
		t.Fatal("artifact replacement list failure ignored")
	}
	persistSession := testArtifactSession(listFailure, "T-persist-list-failure")
	persistSession.nativeID = "T-native-persist-list-failure"
	if err := persistSession.persistAfterTurn(ctx, nil); err == nil {
		t.Fatal("artifact replacement failure did not fail transcript commit")
	}

	loadFailure := &imageStoreTestDouble{
		InMemorySessionStore: NewInMemorySessionStore(),
		loadErr:              errors.New("load failed"),
		subkeys:              []string{imageArtifactPrefix + "one.json"},
		overrideSubkeys:      true,
	}
	if _, err := testArtifactSession(loadFailure, "T-load-failure").imageArtifactReplacements(ctx); err == nil {
		t.Fatal("artifact replacement load failure ignored")
	}

	empty := &imageStoreTestDouble{
		InMemorySessionStore: NewInMemorySessionStore(),
		subkeys:              []string{"other", imageArtifactPrefix + "empty.json"},
		overrideSubkeys:      true,
	}
	replacements, err := testArtifactSession(empty, "T-empty").imageArtifactReplacements(ctx)
	if err != nil || len(replacements) != 0 {
		t.Fatalf("empty artifact replacements = (%#v, %v)", replacements, err)
	}

	listSessionsFailure := &imageStoreTestDouble{
		InMemorySessionStore: NewInMemorySessionStore(),
		listSessionsErr:      errors.New("list sessions failed"),
	}
	agent := newTestAgent(WithSessionStore(listSessionsFailure))
	if err := agent.sweepExpiredImageArtifacts(ctx); err == nil {
		t.Fatal("image sweep session-list failure ignored")
	}
	if _, err := agent.Initialize(ctx, acp.InitializeRequest{}); err != nil {
		t.Fatalf("best-effort image sweep failed initialize: %v", err)
	}

	mainStore := NewInMemorySessionStore()
	seedImageSweepSession(t, mainStore, "T-sweep")
	current := testLinkArtifact("tool:current:0", "https://cdn.example/current.png")
	current.Version = imageArtifactVersion
	current.CreatedAt = time.Now().UnixMilli()
	currentKey := SessionKey{
		SessionID: "T-sweep",
		Subpath:   imageArtifactSubpath(current.Identity, current.Fingerprint),
	}
	currentEntry, _ := json.Marshal(current)
	if err := mainStore.Append(ctx, currentKey, []SessionStoreEntry{currentEntry}); err != nil {
		t.Fatal(err)
	}
	if err := newTestAgent(WithSessionStore(mainStore)).sweepExpiredImageArtifacts(ctx); err != nil {
		t.Fatalf("current image sweep: %v", err)
	}

	subkeyFailure := &imageStoreTestDouble{
		InMemorySessionStore: mainStore,
		listSubkeysErr:       errors.New("list subkeys failed"),
	}
	if err := newTestAgent(WithSessionStore(subkeyFailure)).sweepExpiredImageArtifacts(ctx); err == nil {
		t.Fatal("image sweep subkey-list failure ignored")
	}

	sweepLoadFailure := &imageStoreTestDouble{
		InMemorySessionStore: mainStore,
		loadErr:              errors.New("load failed"),
		subkeys:              []string{"other", imageArtifactPrefix + "one.json"},
		overrideSubkeys:      true,
	}
	if err := newTestAgent(WithSessionStore(sweepLoadFailure)).sweepExpiredImageArtifacts(ctx); err == nil {
		t.Fatal("image sweep load failure ignored")
	}

	emptySweep := &imageStoreTestDouble{
		InMemorySessionStore: mainStore,
		subkeys:              []string{imageArtifactPrefix + "empty.json"},
		overrideSubkeys:      true,
	}
	if err := newTestAgent(WithSessionStore(emptySweep)).sweepExpiredImageArtifacts(ctx); err != nil {
		t.Fatalf("empty image sweep: %v", err)
	}

	deleteBase := NewInMemorySessionStore()
	seedImageSweepSession(t, deleteBase, "T-delete-failure")
	corruptKey := SessionKey{SessionID: "T-delete-failure", Subpath: imageArtifactPrefix + "corrupt.json"}
	if err := deleteBase.Append(ctx, corruptKey, []SessionStoreEntry{json.RawMessage(`{`)}); err != nil {
		t.Fatal(err)
	}
	deleteFailure := &imageStoreTestDouble{
		InMemorySessionStore: deleteBase,
		deleteErr:            errors.New("delete failed"),
	}
	if err := newTestAgent(WithSessionStore(deleteFailure)).sweepExpiredImageArtifacts(ctx); err == nil {
		t.Fatal("image sweep delete failure ignored")
	}
}

func testLinkArtifact(identity, uri string) storedImageArtifact {
	return storedImageArtifact{
		Kind:        imageArtifactKindLink,
		Identity:    identity,
		Fingerprint: fingerprintImageOutput([]byte(uri)),
		MimeType:    imageMIMEPNG,
		URI:         uri,
	}
}

func testArtifactSession(store SessionStore, sessionID string) *agentSession {
	return &agentSession{
		agent: newTestAgent(WithSessionStore(store)),
		id:    acp.SessionId(sessionID),
	}
}

func seedImageSweepSession(t *testing.T, store *InMemorySessionStore, sessionID string) {
	t.Helper()

	main := SessionKey{SessionID: sessionID, Subpath: SessionStoreMainSubpath}
	if err := store.Replace(context.Background(), main, []SessionStoreReplacement{{
		Key:     main,
		Entries: []SessionStoreEntry{json.RawMessage(`{"format":"amp-thread-mirror-v1"}`)},
	}, {
		Key:     SessionKey{SessionID: sessionID, Subpath: "other"},
		Entries: []SessionStoreEntry{json.RawMessage(`{}`)},
	}}); err != nil {
		t.Fatal(err)
	}
}
