package amp

import (
	"context"
	"path/filepath"
	"slices"
	"testing"
)

func TestExecuteLaunchesThreadlessTurnWithNoArchiveFlag(t *testing.T) {
	path, state := fakeAmpPath(t, "stream")
	client := newTestClient(t, nil, Options{CLIPath: path, Cwd: t.TempDir(), MaxLineBytes: 1024, Mode: "low"})

	turn, err := client.Execute(context.Background(), map[string]any{"type": "user", "text": "hello"})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	sawInit := false
	for msg := range turn.Messages() {
		if sys, ok := msg.(*SystemMessage); ok && sys.SessionID == "T-fake-thread" {
			sawInit = true
		}
	}
	_ = turn.Close()

	if !sawInit {
		t.Fatal("execute stream missing init frame with minted thread id")
	}

	records := readHelperJSON[[]string](t, filepath.Join(state, "args.jsonl"))
	launch := records[len(records)-1]
	if slices.Contains(launch, ampArgThreads) {
		t.Fatalf("execute launch used a threads subcommand: %#v", launch)
	}
	for _, want := range []string{ampArgNoArchiveAfterExecute, ampArgStreamJSON, ampArgStreamJSONInput, ampArgExecute, "-m", "low"} {
		if !slices.Contains(launch, want) {
			t.Fatalf("execute launch missing %q: %#v", want, launch)
		}
	}
}
