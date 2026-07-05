//nolint:gocyclo,govet // Store contract edge tests intentionally keep setup in one scenario.
package ampacp

import (
	"context"
	"encoding/json"
	"slices"
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

	manifest1, _ := json.Marshal(ampManifest{Format: SessionStoreFormat, ThreadID: "T-1", Cwd: "/tmp/one", Title: "one", UpdatedAtUnixMilli: 10, Meta: map[string]any{"x": "y"}})
	manifest2, _ := json.Marshal(ampManifest{Format: SessionStoreFormat, ThreadID: "T-2", Cwd: "/tmp/two", Title: "two", UpdatedAtUnixMilli: 10})
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
	if len(summaries) != 2 || summaries[0].SessionID != "T-1" || summaries[1].SessionID != "T-2" {
		t.Fatalf("sorted summaries = %#v", summaries)
	}
	summaries[0].Meta["x"] = "changed"
	summaries, _ = store.ListSessions(ctx)
	if summaries[0].Meta["x"] != "y" {
		t.Fatalf("summary meta was not cloned: %#v", summaries[0].Meta)
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
	if summary, ok := summaryFromStoreEntry(json.RawMessage(`{"format":"wrong"}`)); ok || summary.SessionID != "" {
		t.Fatalf("bad summary = %#v ok=%v", summary, ok)
	}
}
