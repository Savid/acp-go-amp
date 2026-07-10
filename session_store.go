package ampacp

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"time"
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
	mu        sync.RWMutex
	entries   map[SessionKey][]SessionStoreEntry
	updatedAt map[SessionKey]int64
	deleted   map[SessionKey]struct{}
}

var _ SessionStore = (*InMemorySessionStore)(nil)

func (s *InMemorySessionStore) ensure() {
	if s.entries == nil {
		s.entries = make(map[SessionKey][]SessionStoreEntry)
	}

	if s.updatedAt == nil {
		s.updatedAt = make(map[SessionKey]int64)
	}

	if s.deleted == nil {
		s.deleted = make(map[SessionKey]struct{})
	}
}

func NewInMemorySessionStore() *InMemorySessionStore {
	return &InMemorySessionStore{
		entries:   make(map[SessionKey][]SessionStoreEntry),
		updatedAt: make(map[SessionKey]int64),
		deleted:   make(map[SessionKey]struct{}),
	}
}

func (s *InMemorySessionStore) Append(ctx context.Context, key SessionKey, entries []SessionStoreEntry) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if s == nil {
		return errors.New("nil InMemorySessionStore")
	}

	if len(entries) == 0 {
		return nil
	}

	if key.SessionID == "" {
		return errors.New("session id is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.ensure()

	if s.isTombstonedLocked(key) {
		return nil
	}

	for _, entry := range entries {
		s.entries[key] = append(s.entries[key], cloneRaw(entry))
	}

	s.updatedAt[key] = time.Now().UnixMilli()

	return nil
}

func (s *InMemorySessionStore) Load(ctx context.Context, key SessionKey) ([]SessionStoreEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if s == nil {
		return nil, errors.New("nil InMemorySessionStore")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.isTombstonedLocked(key) {
		return nil, nil
	}

	return cloneEntries(s.entries[key]), nil
}

// isTombstonedLocked reports whether a key is hidden by a tombstone. A tombstone
// on the main key cascades to every subpath of that session (including subpaths
// created after the main delete), and is cleared only by a valid Replace
// generation that re-publishes the main key.
func (s *InMemorySessionStore) isTombstonedLocked(key SessionKey) bool {
	if _, ok := s.deleted[key]; ok {
		return true
	}

	if key.Subpath != SessionStoreMainSubpath {
		_, ok := s.deleted[SessionKey{SessionID: key.SessionID, Subpath: SessionStoreMainSubpath}]

		return ok
	}

	return false
}

func (s *InMemorySessionStore) Replace(ctx context.Context, main SessionKey, replacements []SessionStoreReplacement) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if s == nil {
		return errors.New("nil InMemorySessionStore")
	}

	if main.SessionID == "" {
		return errors.New("session id is required")
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

	now := time.Now().UnixMilli()

	s.mu.Lock()
	defer s.mu.Unlock()

	s.ensure()

	for key := range s.entries {
		if key.SessionID == main.SessionID {
			delete(s.entries, key)
			delete(s.updatedAt, key)
			s.deleted[key] = struct{}{}
		}
	}

	for key, entries := range next {
		s.entries[key] = entries
		s.updatedAt[key] = now
		delete(s.deleted, key)
	}

	return nil
}

func (s *InMemorySessionStore) Delete(ctx context.Context, key SessionKey) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if s == nil {
		return errors.New("nil InMemorySessionStore")
	}

	// Deleting a key with no session id is a pure no-op: nothing can be
	// addressed by it and no tombstone is written.
	if key.SessionID == "" {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.ensure()

	if key.Subpath == SessionStoreMainSubpath {
		for existing := range s.entries {
			if existing.SessionID == key.SessionID {
				delete(s.entries, existing)
				delete(s.updatedAt, existing)
				s.deleted[existing] = struct{}{}
			}
		}

		s.deleted[key] = struct{}{}

		return nil
	}

	delete(s.entries, key)
	delete(s.updatedAt, key)
	s.deleted[key] = struct{}{}

	return nil
}

func (s *InMemorySessionStore) ListSessions(ctx context.Context) ([]SessionSummary, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if s == nil {
		return nil, errors.New("nil InMemorySessionStore")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	summaries := make([]SessionSummary, 0)

	for key, entries := range s.entries {
		if key.Subpath != SessionStoreMainSubpath || s.isTombstonedLocked(key) {
			continue
		}

		summary := SessionSummary{
			SessionID:          key.SessionID,
			UpdatedAtUnixMilli: s.updatedAt[key],
		}
		// A committed key always lists, even with zero entries or a last row
		// that is not a valid amp manifest; a valid manifest only enriches the
		// summary with its recorded cwd and title.
		if len(entries) > 0 {
			if manifest, ok := manifestFromStoreEntry(entries[len(entries)-1]); ok {
				summary.Cwd = manifest.Cwd
				summary.Title = manifest.Title
			}
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

	if s == nil {
		return nil, errors.New("nil InMemorySessionStore")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	subkeys := make([]string, 0)

	for existing := range s.entries {
		if existing.SessionID != key.SessionID || existing.Subpath == SessionStoreMainSubpath {
			continue
		}

		if s.isTombstonedLocked(existing) {
			continue
		}

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

// manifestFromStoreEntry parses a main-key row as an amp manifest, reporting
// whether it is a valid manifest for this store format.
func manifestFromStoreEntry(entry json.RawMessage) (ampManifest, bool) {
	var manifest ampManifest
	if err := json.Unmarshal(entry, &manifest); err != nil {
		return ampManifest{}, false
	}

	if manifest.ThreadID == "" || manifest.Format != SessionStoreFormat {
		return ampManifest{}, false
	}

	return manifest, true
}
