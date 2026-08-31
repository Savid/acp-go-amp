package amp

import "errors"

var ErrContainmentIncomplete = errors.New("native containment incomplete")

func ProcessContainmentComplete(err error) bool {
	return !errors.Is(err, ErrContainmentIncomplete)
}
