package ampacp

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/savid/acp-go-amp/internal/amp"
)

func (s *session) recoverNative(ctx context.Context, stored storedSession) ([][]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*nativeCommandTimeout)
	defer cancel()

	request, err := s.nativeRequest(ctx, nil)
	if err != nil {
		return nil, err
	}

	remote, err := amp.NewRemote(request, s.serviceURL)
	if err != nil {
		return nil, err
	}
	defer remote.Close()

	missing, err := remote.Missing(ctx, s.nativeID)
	if err != nil {
		return nil, err
	}

	if !missing {
		return nil, errors.New("native thread exists but could not be observed")
	}

	sourceID := s.nativeID

	mode := s.options.Mode
	if mode == "" {
		mode = modeMedium
	}

	if _, _, err = amp.PrepareImport(stored.rows[0], sourceID, sourceID, mode); err != nil {
		return nil, err
	}

	if createErr := s.createNative(ctx, "--visibility", "private"); createErr != nil {
		return nil, createErr
	}

	rows, err := s.importRecovered(ctx, remote, stored, sourceID, mode)
	if err != nil {
		s.discardUnbound(ctx, stored.record)

		return nil, err
	}

	return rows, nil
}

// importRecovered fills the fresh private destination with the backup and
// verifies the result before the caller may commit the new binding.
func (s *session) importRecovered(ctx context.Context, remote *amp.Remote, stored storedSession, sourceID, mode string) ([][]byte, error) {
	if s.serviceURL != stored.record.ServiceURL {
		return nil, errors.New("native service changed during recovery")
	}

	payload, count, err := amp.PrepareImport(stored.rows[0], sourceID, s.nativeID, mode)
	if err != nil {
		return nil, err
	}

	if importErr := remote.Import(ctx, s.nativeID, payload, count); importErr != nil {
		return nil, importErr
	}

	view, err := s.observeImported(ctx, count)
	if err != nil {
		return nil, err
	}

	rows, err := s.verifiedExport(ctx, *view)
	if err != nil {
		return nil, err
	}

	if err := amp.VerifyImported(stored.rows[0], sourceID, rows[0], s.nativeID); err != nil {
		return nil, err
	}

	s.adoptSnapshot(rows)

	return rows, nil
}

// discardUnbound deletes the private destination that recovery created and
// never bound, then restores the committed binding. Only that unbound copy is
// ever deleted; the committed native history stays untouched.
func (s *session) discardUnbound(ctx context.Context, record sessionRecord) {
	unbound := s.nativeID
	if unbound == record.NativeSessionID {
		return
	}

	if _, err := s.command(context.WithoutCancel(ctx), "threads", "delete", unbound); err != nil {
		s.agent.log.WarnContext(ctx, "Amp recovery destination was not deleted",
			slog.String("native_id", unbound), slog.String("reason", err.Error()))
	}

	s.nativeID = record.NativeSessionID
	s.serviceURL = record.ServiceURL
}

// The first attachment can report an empty view while initializing imported history.
func (s *session) observeImported(ctx context.Context, count int) (*amp.View, error) {
	delay := 2 * time.Second

	for {
		view, err := s.observeNative(ctx)
		if err != nil {
			return nil, err
		}

		if len(view.Messages) == count {
			return view, nil
		}

		if len(view.Messages) > count {
			return nil, errors.New("native import gained unexpected messages")
		}

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()

			return nil, ctx.Err()
		case <-timer.C:
		}

		delay = min(delay*2, 8*time.Second)
	}
}
