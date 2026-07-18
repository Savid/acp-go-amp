package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"strings"
)

const (
	containmentDiagnoseCommand = "diagnose"
	containmentCleanupCommand  = "cleanup"
)

func runContainment(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		_, _ = fmt.Fprintln(stderr, "acp-go-amp: containment requires diagnose or cleanup")

		return 2
	}

	var err error

	switch args[0] {
	case containmentDiagnoseCommand:
		flags := flag.NewFlagSet("acp-go-amp containment diagnose", flag.ContinueOnError)
		flags.SetOutput(stderr)

		scratchDir := flags.String("scratch-dir", "", "scratch parent containing the Amp containment registry")
		if parseErr := flags.Parse(args[1:]); parseErr != nil {
			return 2
		}

		if flags.NArg() != 0 {
			_, _ = fmt.Fprintln(stderr, "acp-go-amp: containment diagnose accepts no positional arguments")

			return 2
		}

		if !validContainmentScratchDir(*scratchDir) {
			_, _ = fmt.Fprintln(stderr, "acp-go-amp: containment diagnose requires -scratch-dir")

			return 2
		}

		err = diagnoseContainment(*scratchDir, stdout)
	case containmentCleanupCommand:
		flags := flag.NewFlagSet("acp-go-amp containment cleanup", flag.ContinueOnError)
		flags.SetOutput(stderr)

		scratchDir := flags.String("scratch-dir", "", "scratch parent containing the Amp containment registry")
		runtimeID := flags.String("runtime-id", "", "operator-selected 128-bit containment runtime id")

		force := flags.Bool("force", false, "accept PID-reuse TOCTOU and collateral-signalling risk, then signal individually revalidated marker-correlated processes")
		if parseErr := flags.Parse(args[1:]); parseErr != nil {
			return 2
		}

		if flags.NArg() != 0 {
			_, _ = fmt.Fprintln(stderr, "acp-go-amp: containment cleanup accepts no positional arguments")

			return 2
		}

		if !validContainmentScratchDir(*scratchDir) || !validContainmentRuntimeID(*runtimeID) || !*force {
			_, _ = fmt.Fprintln(stderr, "acp-go-amp: containment cleanup requires -scratch-dir, a 128-bit lowercase hexadecimal -runtime-id, and -force")

			return 2
		}

		err = cleanupContainment(*scratchDir, *runtimeID, *force, stdout)
	default:
		_, _ = fmt.Fprintf(stderr, "acp-go-amp: unknown containment command %q\n", args[0])

		return 2
	}

	if err != nil {
		_, _ = fmt.Fprintf(stderr, "acp-go-amp: %v\n", err)

		return 1
	}

	return 0
}

func validContainmentScratchDir(scratchDir string) bool {
	return strings.TrimSpace(scratchDir) != "" && !strings.ContainsRune(scratchDir, '\x00')
}

func validContainmentRuntimeID(runtimeID string) bool {
	if len(runtimeID) != 32 || runtimeID != strings.ToLower(runtimeID) {
		return false
	}

	_, err := hex.DecodeString(runtimeID)

	return err == nil
}
