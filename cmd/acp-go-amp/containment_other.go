//go:build !darwin

package main

import (
	"errors"
	"io"
)

func diagnoseContainment(string, io.Writer) error {
	return errors.New("containment diagnose is available only on darwin")
}

func cleanupContainment(string, string, bool, io.Writer) error {
	return errors.New("containment cleanup is available only on darwin")
}
