//nolint:wsl_v5,nlreturn // command entrypoint is intentionally linear.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	ampacp "github.com/savid/acp-go-amp"
)

func main() {
	if err := run(); err != nil {
		slog.New(slog.NewTextHandler(os.Stderr, nil)).Error("serve failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run() error {
	var (
		path        string
		home        string
		model       string
		debug       bool
		showVersion bool
	)
	flag.StringVar(&path, "path", "", "native amp executable path")
	flag.StringVar(&home, "home", "", "parent directory for isolated Amp settings files")
	flag.StringVar(&model, "model", "", "default model; unsupported by Amp and rejected at session start")
	flag.BoolVar(&debug, "debug", false, "enable debug logging")
	flag.BoolVar(&showVersion, "version", false, "print adapter version and exit")
	flag.Parse()

	if showVersion {
		fmt.Fprintln(os.Stdout, version)
		return nil
	}

	level := slog.LevelInfo
	if debug {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	err := ampacp.Serve(ctx, os.Stdin, os.Stdout,
		ampacp.WithExecutablePath(path),
		ampacp.WithHome(home),
		ampacp.WithDefaultModel(model),
		ampacp.WithLogger(logger),
		ampacp.WithAgentVersion(version),
	)
	if err != nil && ctx.Err() == nil {
		return err
	}

	return nil
}
