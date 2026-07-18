//go:build !darwin

package amp

import (
	"errors"
	"io"
)

func NewDarwinGenerationRecord(string, string, string) (*DarwinGeneration, error) {
	return nil, errors.New("Darwin best-effort containment is unavailable on this platform")
}

func DiagnoseDarwinContainment(string, io.Writer) error {
	return errors.New("containment diagnose is available only on darwin")
}

func CleanupDarwinContainment(string, string, bool, io.Writer) error {
	return errors.New("containment cleanup is available only on darwin")
}
