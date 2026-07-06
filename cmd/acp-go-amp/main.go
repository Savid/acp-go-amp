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
	)

	flags := flag.NewFlagSet("acp-go-amp", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&path, "path", "", "native amp executable path")
	flags.StringVar(&home, "home", "", "parent directory for isolated Amp settings files")
	flags.StringVar(&model, "model", "", "default model; unsupported by Amp and rejected at session start")
	flags.BoolVar(&debug, "debug", false, "enable debug logging")
	flags.BoolVar(&showVersion, "version", false, "print adapter version and exit")

	if err := flags.Parse(args); err != nil {
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

	serveOptions := make([]ampacp.Option, 0, 5+len(telemetry.options))

	serveOptions = append(serveOptions,
		ampacp.WithExecutablePath(path),
		ampacp.WithHome(home),
		ampacp.WithDefaultModel(model),
		ampacp.WithLogger(logger),
		ampacp.WithAgentVersion(agentVer),
	)
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

func pendingSignal(signals <-chan os.Signal) os.Signal {
	select {
	case sig := <-signals:
		return sig
	default:
		return nil
	}
}
