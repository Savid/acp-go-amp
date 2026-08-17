//go:build !darwin

package main

import (
	"errors"
	"io"
)

var diagnoseContainment = func(string, io.Writer) error {
	return errors.New("containment diagnose is available only on darwin")
}

var cleanupContainment = func(string, string, bool, io.Writer) error {
	return errors.New("containment cleanup is available only on darwin")
}
