package ampacp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/url"
	"path/filepath"
	"reflect"
	"slices"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-amp/internal/amp"
	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/sessionlog"
	"github.com/savid/acp-go-core/wire"
)

type sessionRecord struct {
	SessionID             string                    `json:"sessionId"`
	NativeSessionID       string                    `json:"nativeSessionId"`
	ServiceURL            string                    `json:"serviceUrl"`
	Usage                 map[string]map[string]any `json:"usage,omitempty"`
	Cwd                   string                    `json:"cwd"`
	AdditionalDirectories []string                  `json:"additionalDirectories,omitempty"`
	Env                   map[string]string         `json:"env,omitempty"`
	ExtraPathDirs         []string                  `json:"extraPathDirs,omitempty"`
	Mode                  string                    `json:"mode,omitempty"`
	UpdatedAtUnixMilli    int64                     `json:"updatedAtUnixMilli"`
}

func (s *session) record() sessionRecord {
	s.mu.Lock()
	defer s.mu.Unlock()

	return sessionRecord{SessionID: string(s.id), NativeSessionID: s.nativeID, ServiceURL: s.serviceURL, Usage: cloneUsage(s.usage), Cwd: s.cwd, AdditionalDirectories: slices.Clone(s.additionalDirectories), Env: maps.Clone(s.options.Env), ExtraPathDirs: slices.Clone(s.options.ExtraPathDirs), Mode: s.options.Mode, UpdatedAtUnixMilli: time.Now().UnixMilli()}
}

func (r sessionRecord) validate(id string) error {
	service, err := url.Parse(r.ServiceURL)
	if err != nil || service.Host == "" || service.User != nil || service.Path != "/" || (service.Scheme != "http" && service.Scheme != "https") || service.RawQuery != "" || service.Fragment != "" {
		return errors.New("invalid native service URL")
	}

	if err := amp.ValidateThreadID(r.NativeSessionID); err != nil {
		return err
	}

	if r.NativeSessionID == "" || r.SessionID != id || !filepath.IsAbs(r.Cwd) || r.UpdatedAtUnixMilli <= 0 {
		return errors.New("invalid session record")
	}

	for _, dir := range r.AdditionalDirectories {
		if !filepath.IsAbs(dir) {
			return errors.New("invalid additional directory")
		}
	}

	return ValidateAmpSessionMeta(inheritCarrier(sessionMeta{}, r).Meta())
}

func (s *session) commitMirror(ctx context.Context) error {
	ctx, finish := s.agent.observe.StartSessionStore(ctx, "replace")
	err := sessionlog.Commit(ctx, s.agent.store, string(s.id), s.rows, s.record())
	finish(err)

	if err == nil {
		s.mu.Lock()
		s.persisted = true
		s.mu.Unlock()
	}

	return err
}

type storedSession struct {
	rows   [][]byte
	record sessionRecord
	found  bool
}

func (a *Agent) loadStored(ctx context.Context, id acp.SessionId) (storedSession, error) {
	ctx, cancel := context.WithTimeout(ctx, acpcore.SessionStoreTimeout)
	defer cancel()

	ctx, finish := a.observe.StartSessionStore(ctx, "load")

	var record sessionRecord

	rows, found, err := sessionlog.Load(ctx, a.store, string(id), &record)
	if err == nil && found {
		err = record.validate(string(id))
		if err == nil {
			if len(rows) != 1 {
				err = errors.New("expected one native export")
			} else {
				_, err = decodeSnapshot(rows[0], record.NativeSessionID)
			}
		}
	}

	finish(err)

	if err != nil {
		return storedSession{}, a.restoreRefused(ctx, id, err)
	}

	return storedSession{rows: rows, record: record, found: found}, nil
}

type nativeSnapshot struct {
	ID       string          `json:"id"`
	Title    string          `json:"title"`
	Messages []nativeMessage `json:"messages"`
}

type nativeMessage struct {
	ProtocolID json.RawMessage  `json:"protocolMessageID,omitempty"` //nolint:tagliatelle // Native exports use this field spelling.
	ID         int64            `json:"messageId"`
	Role       string           `json:"role"`
	Content    []map[string]any `json:"content"`
	State      map[string]any   `json:"state"`
	AgentMode  string           `json:"agentMode"`
	Usage      map[string]any   `json:"usage"`
}

func decodeSnapshot(data []byte, id string) (nativeSnapshot, error) {
	var snapshot nativeSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return snapshot, err
	}

	if snapshot.ID != id || snapshot.Messages == nil {
		return snapshot, errors.New("native snapshot identity or messages missing")
	}

	seen := make(map[int64]struct{}, len(snapshot.Messages))
	for _, message := range snapshot.Messages {
		if message.ID <= 0 || message.Content == nil {
			return snapshot, errors.New("invalid native message")
		}

		if _, ok := seen[message.ID]; ok {
			return snapshot, errors.New("duplicate native message")
		}

		seen[message.ID] = struct{}{}
		if !slices.Contains([]string{roleUser, roleAssistant, roleInfo}, message.Role) {
			return snapshot, errors.New("invalid native message role")
		}

		for _, part := range message.Content {
			if textValue(part[fieldType]) == "" {
				return snapshot, errors.New("native message part has no type")
			}

			switch textValue(part[fieldType]) {
			case fieldText:
				if _, ok := part[fieldText].(string); !ok {
					return snapshot, errors.New("invalid native text")
				}
			case contentToolUse:
				if textValue(part["id"]) == "" || textValue(part["name"]) == "" {
					return snapshot, errors.New("invalid native tool")
				}
			case contentToolResult:
				if textValue(part["toolUseID"]) == "" {
					return snapshot, errors.New("invalid native tool result")
				}
			}
		}
	}

	return snapshot, nil
}

func (s *session) exportNative(ctx context.Context) ([][]byte, error) {
	data, err := s.command(ctx, "threads", "export", s.nativeID)
	if err != nil {
		return nil, err
	}

	if _, err := decodeSnapshot(data, s.nativeID); err != nil {
		return nil, err
	}

	return [][]byte{data}, nil
}

// reconcileNative refuses disagreement with the mirror at a shared message.
func (s *session) reconcileNative(ctx context.Context, stored [][]byte, view amp.View) ([][]byte, error) {
	rows, err := s.verifiedExport(ctx, view)
	if err != nil {
		return nil, err
	}

	if len(stored) != 1 {
		return nil, errors.New("missing stored export")
	}

	want, err := decodeSnapshot(stored[0], s.nativeID)
	if err != nil {
		return nil, err
	}

	have, err := decodeSnapshot(rows[0], s.nativeID)
	if err != nil {
		return nil, err
	}

	if len(have.Messages) < len(want.Messages) {
		return nil, errors.New("remote thread is shorter than its mirror")
	}

	for i, wanted := range want.Messages {
		if !reflect.DeepEqual(wanted, have.Messages[i]) {
			return nil, fmt.Errorf("native message %d conflicts with the mirror", i)
		}
	}

	s.adoptSnapshot(rows)

	return rows, nil
}

// hydrate reconciles the native thread for a restore, answering with the
// restore verdict.
func (s *session) hydrate(ctx context.Context, stored storedSession) ([][]byte, error) {
	view, err := s.observeNative(ctx)
	if err != nil {
		rows, recoveryErr := s.recoverNative(ctx, stored)
		if recoveryErr != nil {
			return nil, s.agent.restoreRefused(ctx, s.id, recoveryErr)
		}

		return rows, nil
	}

	rows, err := s.reconcileNative(ctx, stored.rows, *view)
	if err != nil {
		return nil, s.agent.restoreRefused(ctx, s.id, err)
	}

	return rows, nil
}

// settleExport reconciles the native thread at the end of a turn, before the
// foreground commit that makes it durable.
func (s *session) settleExport(ctx context.Context, view *amp.View) ([][]byte, error) {
	if view == nil {
		var err error

		view, err = s.observeNative(ctx)
		if err != nil {
			return nil, err
		}
	}

	rows, err := s.reconcileNative(ctx, s.rows, *view)
	if err != nil {
		s.agent.log.ErrorContext(ctx, "amp turn export failed",
			slog.String("session_id", string(s.id)), slog.String("reason", err.Error()))

		return nil, err
	}

	return rows, nil
}

// observeNative reattaches without input, so a disconnected observer cannot
// mistake a still-running remote turn for a completed one.
func (s *session) observeNative(ctx context.Context) (*amp.View, error) {
	request, err := s.continueRequest(ctx)
	if err != nil {
		return nil, err
	}

	outcome, _, _, err := amp.RunAttached(ctx, request, s.agent.options.ScratchDir, s.nativeID, nil, nil, func([]byte) error { return nil })
	if err != nil {
		return nil, err
	}

	if outcome.View == nil {
		return nil, errors.New("native observer exited without thread state")
	}

	return outcome.View, nil
}

func (s *session) verifiedExport(ctx context.Context, view amp.View) ([][]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, nativeCommandTimeout)
	defer cancel()

	delay := 2 * time.Second

	for {
		rows, err := s.exportNative(ctx)
		if err != nil {
			return nil, err
		}

		complete, err := amp.VerifyExport(rows[0], view)
		if err != nil {
			return nil, err
		}

		if complete {
			return rows, nil
		}

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()

			return nil, errors.New("native export did not reach the observed thread state")
		case <-timer.C:
		}

		if delay < 8*time.Second {
			delay *= 2
		}
	}
}

func (s *session) adoptSnapshot(rows [][]byte) {
	if len(rows) != 1 {
		return
	}

	snapshot, err := decodeSnapshot(rows[0], s.nativeID)
	if err != nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.title = storedTitle(s.nativeID, rows)
	s.updatedAt = time.Now().UTC().Format(time.RFC3339)

	for _, message := range snapshot.Messages {
		if message.Usage != nil {
			if s.usage == nil {
				s.usage = make(map[string]map[string]any)
			}

			s.usage[string(message.ProtocolID)] = maps.Clone(message.Usage)
		}

		usage := s.usage[string(message.ProtocolID)]
		if model := textValue(usage["model"]); model != "" {
			s.model = model
		}
	}
}

func storedTitle(id string, rows [][]byte) string {
	if len(rows) != 1 {
		return id
	}

	snapshot, err := decodeSnapshot(rows[0], id)
	if err != nil {
		return id
	}

	if title := wire.NormalizeTitle(snapshot.Title); title != "" {
		return title
	}

	for _, message := range snapshot.Messages {
		if message.Role == roleUser {
			for _, part := range message.Content {
				if textValue(part[fieldType]) == fieldText {
					if text := wire.NormalizeTitle(textValue(part[fieldText])); text != "" {
						return text
					}
				}
			}
		}
	}

	return id
}

func (a *Agent) restoreRefused(ctx context.Context, id acp.SessionId, err error) error {
	a.log.ErrorContext(ctx, "amp session restore failed", slog.String("session_id", string(id)), slog.String("reason", err.Error()))

	return wire.RestoreFailed(vendor)
}

func (s *session) replay(ctx context.Context, rows [][]byte) error {
	if len(rows) != 1 {
		return errors.New("missing native export")
	}

	snapshot, err := decodeSnapshot(rows[0], s.nativeID)
	if err != nil {
		return err
	}

	state := cycleState{}
	for _, message := range snapshot.Messages {
		if err := s.projectContent(ctx, &state, message.Role, message.Content, "", true); err != nil {
			return err
		}

		usage := message.Usage
		if usage == nil {
			usage = s.usage[string(message.ProtocolID)]
		}

		if message.Role == roleAssistant && usage != nil {
			state.addUsage(&amp.Usage{
				InputTokens:              nativeTokens(usage, "inputTokens"),
				OutputTokens:             nativeTokens(usage, "outputTokens"),
				CacheReadInputTokens:     nativeTokens(usage, "cacheReadInputTokens"),
				CacheCreationInputTokens: nativeTokens(usage, "cacheCreationInputTokens"),
				MaxTokens:                nativeTokens(usage, "maxInputTokens"),
			}, true)
		}
	}

	return s.emitUsage(ctx, &state)
}

func nativeTokens(usage map[string]any, key string) int {
	value, _ := usage[key].(float64)

	return max(0, int(value))
}

func cloneUsage(usage map[string]map[string]any) map[string]map[string]any {
	if usage == nil {
		return nil
	}

	result := make(map[string]map[string]any, len(usage))
	for id, value := range usage {
		result[id] = maps.Clone(value)
	}

	return result
}
