package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"

	ampacp "github.com/savid/acp-go-amp"
	"github.com/savid/acp-go-core/process"
)

func main() {
	if code := run(context.Background(), os.Args[1:], os.Stdin, os.Stdout, os.Stderr); code != 0 {
		os.Exit(code)
	}
}

func run(ctx context.Context, args []string, stdin io.Reader, stdout io.Writer, stderr io.Writer) int {
	flags := flag.NewFlagSet("acp-go-amp", flag.ContinueOnError)
	flags.SetOutput(stderr)

	executablePath := flags.String("path", "", "amp executable; a bare name is searched on PATH")
	home := flags.String("home", "", "unsupported for Amp; leave empty")
	scratchDir := flags.String("scratch-dir", "", "parent directory for ephemeral adapter state; empty means the system temp directory")
	model := flags.String("model", "", "unsupported for Amp; use per-session mode")
	seedFiles := &process.SeedFileFlag{}
	flags.Var(seedFiles, "seed-file", "file seeded into Amp's config root as <relpath>=<hostpath>; repeatable")
	debug := flags.Bool("debug", false, "write debug logs to stderr")
	printVersion := flags.Bool("version", false, "print adapter version and exit")

	if err := flags.Parse(args); err != nil {
		return 2
	}

	if *printVersion {
		_, _ = fmt.Fprintln(stdout, version())

		return 0
	}

	level := slog.LevelWarn
	if *debug {
		level = slog.LevelDebug
	}

	logger := slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: level}))

	telemetry, telemetryOptions, err := configureTelemetry(ctx, logger, version())
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "acp-go-amp: configure OpenTelemetry: %v\n", err)

		return 1
	}

	logger = telemetry.Logger

	ctx, stop := signal.NotifyContext(ctx, forwardedSignals()...)
	defer stop()

	options := []ampacp.Option{
		ampacp.WithAgentVersion(version()),
		ampacp.WithExecutablePath(*executablePath),
		ampacp.WithHome(*home),
		ampacp.WithScratchDir(*scratchDir),
		ampacp.WithDefaultModel(*model),
		ampacp.WithLogger(logger),
	}
	if len(seedFiles.Files) > 0 {
		options = append(options, ampacp.WithSeedFiles(seedFiles.Files))
	}

	options = append(options, telemetryOptions...)

	serveErr := ampacp.Serve(ctx, stdin, stdout, options...)
	shutdownErr := telemetry.Shutdown(context.Background())

	if serveErr != nil && ctx.Err() == nil {
		_, _ = fmt.Fprintf(stderr, "acp-go-amp: %v\n", serveErr)

		return 1
	}

	if shutdownErr != nil {
		_, _ = fmt.Fprintf(stderr, "acp-go-amp: shutdown OpenTelemetry: %v\n", shutdownErr)

		return 1
	}

	return 0
}
