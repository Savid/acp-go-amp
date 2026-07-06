package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"

	ampacp "github.com/savid/acp-go-amp"
)

var serve = ampacp.Serve
var exit = os.Exit
var shutdownOpenTelemetry = shutdownTelemetry
var agentVersion = func() string { return version }

func main() {
	if code := run(context.Background(), os.Args[1:], os.Stdin, os.Stdout, os.Stderr); code != 0 {
		exit(code)
	}
}

func run(ctx context.Context, args []string, stdin io.Reader, stdout io.Writer, stderr io.Writer) int {
	var (
		path        string
		home        string
		model       string
		debug       bool
		showVersion bool
		seedFiles   = seedFileFlag{}
	)

	flags := flag.NewFlagSet("acp-go-amp", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&path, "path", "", "native amp executable path")
	flags.StringVar(&home, "home", "", "parent directory for isolated Amp settings files")
	flags.StringVar(&model, "model", "", "default model; unsupported by Amp and rejected at session start")
	flags.BoolVar(&debug, "debug", false, "enable debug logging")
	flags.BoolVar(&showVersion, "version", false, "print adapter version and exit")
	flags.Var(&seedFiles, "seed-file", "seed file as <relpath>=<hostpath>, written into each session's isolated native root; repeatable")

	if err := flags.Parse(args); err != nil {
		return 2
	}

	seeds, err := seedFiles.contents()
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "acp-go-amp: %v\n", err)

		return 2
	}

	agentVer := agentVersion()
	if showVersion {
		_, _ = fmt.Fprintln(stdout, agentVer)

		return 0
	}

	level := slog.LevelInfo
	if debug {
		level = slog.LevelDebug
	}

	logger := slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: level}))

	telemetry, err := configureTelemetry(ctx, logger, agentVer)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "acp-go-amp: configure OpenTelemetry: %v\n", err)

		return 1
	}

	logger = telemetry.logger

	signals := forwardedSignals()
	receivedSignals := make(chan os.Signal, 1)

	// NotifyContext cancels serving on a signal; this channel preserves the
	// actual signal value so the process can return the conventional exit code.
	signal.Notify(receivedSignals, signals...)
	defer signal.Stop(receivedSignals)

	ctx, stop := signal.NotifyContext(ctx, signals...)
	defer stop()

	serveOptions := make([]ampacp.Option, 0, 6+len(telemetry.options))

	serveOptions = append(serveOptions,
		ampacp.WithExecutablePath(path),
		ampacp.WithHome(home),
		ampacp.WithDefaultModel(model),
		ampacp.WithLogger(logger),
		ampacp.WithAgentVersion(agentVer),
	)
	if len(seeds) > 0 {
		serveOptions = append(serveOptions, ampacp.WithSeedFiles(seeds))
	}

	serveOptions = append(serveOptions, telemetry.options...)

	serveErr := serve(ctx, stdin, stdout, serveOptions...)
	shutdownErr := shutdownOpenTelemetry(context.Background(), telemetry.shutdown)

	if serveErr != nil && ctx.Err() == nil {
		_, _ = fmt.Fprintf(stderr, "acp-go-amp: %v\n", serveErr)

		return 1
	}

	if shutdownErr != nil {
		_, _ = fmt.Fprintf(stderr, "acp-go-amp: shutdown OpenTelemetry: %v\n", shutdownErr)

		return 1
	}

	if sig := pendingSignal(receivedSignals); sig != nil {
		return signalCode(sig)
	}

	return 0
}

// seedFileFlag collects repeatable -seed-file <relpath>=<hostpath> values. Each
// relpath maps to the contents read from its host file, which the agent writes
// into every session's isolated native root via WithSeedFiles.
type seedFileFlag struct {
	relPaths  []string
	hostPaths map[string]string
}

func (f *seedFileFlag) String() string { return strings.Join(f.relPaths, ",") }

func (f *seedFileFlag) Set(value string) error {
	relPath, hostPath, ok := strings.Cut(value, "=")
	if !ok || relPath == "" || hostPath == "" {
		return fmt.Errorf("invalid -seed-file %q: want <relpath>=<hostpath>", value)
	}

	if f.hostPaths == nil {
		f.hostPaths = map[string]string{}
	}

	if _, exists := f.hostPaths[relPath]; !exists {
		f.relPaths = append(f.relPaths, relPath)
	}

	f.hostPaths[relPath] = hostPath

	return nil
}

func (f *seedFileFlag) contents() (map[string]string, error) {
	seeds := make(map[string]string, len(f.relPaths))

	for _, relPath := range f.relPaths {
		data, err := os.ReadFile(f.hostPaths[relPath])
		if err != nil {
			return nil, fmt.Errorf("read -seed-file %q: %w", relPath, err)
		}

		seeds[relPath] = string(data)
	}

	return seeds, nil
}

func pendingSignal(signals <-chan os.Signal) os.Signal {
	select {
	case sig := <-signals:
		return sig
	default:
		return nil
	}
}
