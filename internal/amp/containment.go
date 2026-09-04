package amp

import (
	"context"
	"errors"
)

var ErrContainmentIncomplete = errors.New("native containment incomplete")

func ProcessContainmentComplete(err error) bool {
	return !errors.Is(err, ErrContainmentIncomplete)
}

func markDetachedWaitIncomplete(err error) error {
	if err == nil || errors.Is(err, ErrContainmentIncomplete) {
		return err
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return errors.Join(err, ErrContainmentIncomplete)
	}

	return err
}
