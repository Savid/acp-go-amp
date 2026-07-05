package ampacp

import (
	"context"
	"encoding/json"
	"testing"
)

func TestInMemoryStoreReplaceAppendDelete(t *testing.T) {
	ctx := context.Background()
	store := NewInMemorySessionStore()
	main := SessionKey{SessionID: "T-1", Subpath: SessionStoreMainSubpath}
	transcript := SessionKey{SessionID: "T-1", Subpath: transcriptSubpath}
	manifest, _ := json.Marshal(ampManifest{Format: SessionStoreFormat, ThreadID: "T-1", Cwd: "/tmp", UpdatedAtUnixMilli: 2})
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
