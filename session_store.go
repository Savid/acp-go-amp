//nolint:wsl_v5,nlreturn // in-memory store favors compact table-like branches.
package ampacp

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"sync"
)

const (
	SessionStoreMainSubpath = ""
	SessionStoreFormat      = "amp-thread-mirror-v1"
	transcriptSubpath       = "transcript"
)

type SessionStoreEntry = json.RawMessage

type SessionKey struct {
	SessionID string
	Subpath   string
}

type SessionSummary struct {
	SessionID          string
	UpdatedAtUnixMilli int64
	Cwd                string
	Title              string
	Meta               map[string]any
}

type SessionStoreReplacement struct {
	Key     SessionKey
	Entries []SessionStoreEntry
}

type SessionStore interface {
	Append(ctx context.Context, key SessionKey, entries []SessionStoreEntry) error
	Load(ctx context.Context, key SessionKey) ([]SessionStoreEntry, error)
	Replace(ctx context.Context, main SessionKey, replacements []SessionStoreReplacement) error
	Delete(ctx context.Context, key SessionKey) error
	ListSessions(ctx context.Context) ([]SessionSummary, error)
	ListSubkeys(ctx context.Context, key SessionKey) ([]string, error)
}

type InMemorySessionStore struct {
	mu      sync.RWMutex
	entries map[SessionKey][]SessionStoreEntry
	deleted map[SessionKey]struct{}
}

func NewInMemorySessionStore() *InMemorySessionStore {
	return &InMemorySessionStore{
		entries: make(map[SessionKey][]SessionStoreEntry),
		deleted: make(map[SessionKey]struct{}),
	}
}

func (s *InMemorySessionStore) Append(ctx context.Context, key SessionKey, entries []SessionStoreEntry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, tombstoned := s.deleted[key]; tombstoned {
		return nil
	}

	for _, entry := range entries {
		s.entries[key] = append(s.entries[key], cloneRaw(entry))
	}
	return nil
}

func (s *InMemorySessionStore) Load(ctx context.Context, key SessionKey) ([]SessionStoreEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, tombstoned := s.deleted[key]; tombstoned {
		return nil, nil
	}
	return cloneEntries(s.entries[key]), nil
}

func (s *InMemorySessionStore) Replace(ctx context.Context, main SessionKey, replacements []SessionStoreReplacement) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if main.Subpath != SessionStoreMainSubpath {
		return errors.New("main subpath must be empty")
	}

	mainCount := 0
	next := make(map[SessionKey][]SessionStoreEntry, len(replacements))
	for _, replacement := range replacements {
		if replacement.Key.SessionID != main.SessionID {
			return errors.New("replacement session id mismatch")
		}
		if replacement.Key == main {
			mainCount++
		}
		next[replacement.Key] = cloneEntries(replacement.Entries)
	}
	if mainCount != 1 {
		return errors.New("replacement must include main exactly once")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for key := range s.entries {
		if key.SessionID == main.SessionID {
			delete(s.entries, key)
			s.deleted[key] = struct{}{}
		}
	}
	for key, entries := range next {
		s.entries[key] = entries
		delete(s.deleted, key)
	}
	return nil
}

func (s *InMemorySessionStore) Delete(ctx context.Context, key SessionKey) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if key.Subpath == SessionStoreMainSubpath {
		for existing := range s.entries {
			if existing.SessionID == key.SessionID {
				delete(s.entries, existing)
				s.deleted[existing] = struct{}{}
			}
		}
		s.deleted[key] = struct{}{}
		return nil
	}

	delete(s.entries, key)
	s.deleted[key] = struct{}{}
	return nil
}

func (s *InMemorySessionStore) ListSessions(ctx context.Context) ([]SessionSummary, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	summaries := make([]SessionSummary, 0)
	for key, entries := range s.entries {
		if key.Subpath != SessionStoreMainSubpath {
			continue
		}
		if _, tombstoned := s.deleted[key]; tombstoned || len(entries) == 0 {
			continue
		}
		summary, ok := summaryFromStoreEntry(entries[len(entries)-1])
		if !ok {
			continue
		}
		summaries = append(summaries, summary)
	}

	sort.SliceStable(summaries, func(i, j int) bool {
		if summaries[i].UpdatedAtUnixMilli == summaries[j].UpdatedAtUnixMilli {
			return summaries[i].SessionID < summaries[j].SessionID
		}
		return summaries[i].UpdatedAtUnixMilli > summaries[j].UpdatedAtUnixMilli
	})

	return summaries, nil
}

func (s *InMemorySessionStore) ListSubkeys(ctx context.Context, key SessionKey) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	subkeys := make([]string, 0)
	seen := map[string]struct{}{}
	for existing := range s.entries {
		if existing.SessionID != key.SessionID || existing.Subpath == SessionStoreMainSubpath {
			continue
		}
		if _, tombstoned := s.deleted[existing]; tombstoned {
			continue
		}
		if _, ok := seen[existing.Subpath]; ok {
			continue
		}
		seen[existing.Subpath] = struct{}{}
		subkeys = append(subkeys, existing.Subpath)
	}
	sort.Strings(subkeys)
	return subkeys, nil
}

func cloneRaw(in json.RawMessage) json.RawMessage {
	if in == nil {
		return nil
	}
	return append(json.RawMessage(nil), in...)
}

func cloneEntries(entries []SessionStoreEntry) []SessionStoreEntry {
	if len(entries) == 0 {
		return nil
	}
	out := make([]SessionStoreEntry, 0, len(entries))
	for _, entry := range entries {
		out = append(out, cloneRaw(entry))
	}
	return out
}

func summaryFromStoreEntry(entry json.RawMessage) (SessionSummary, bool) {
	var manifest ampManifest
	if err := json.Unmarshal(entry, &manifest); err != nil {
		return SessionSummary{}, false
	}
	if manifest.ThreadID == "" || manifest.Format != SessionStoreFormat {
		return SessionSummary{}, false
	}
	return SessionSummary{
		SessionID:          manifest.ThreadID,
		UpdatedAtUnixMilli: manifest.UpdatedAtUnixMilli,
		Cwd:                manifest.Cwd,
		Title:              manifest.Title,
		Meta:               cloneAnyMap(manifest.Meta),
	}, true
}
