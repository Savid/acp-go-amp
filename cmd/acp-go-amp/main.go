//nolint:wsl_v5,nlreturn // command entrypoint is intentionally linear.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	ampacp "github.com/savid/acp-go-amp"
)

var (
	exit     = os.Exit
	serveACP = ampacp.Serve
)

func main() {
	exit(runCLI(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

func runCLI(args []string, input io.Reader, output io.Writer, errOutput io.Writer) int {
	if err := run(args, input, output, errOutput); err != nil {
		slog.New(slog.NewTextHandler(errOutput, nil)).Error("serve failed", slog.String("error", err.Error()))
		return 1
	}
	return 0
}

func run(args []string, input io.Reader, output io.Writer, errOutput io.Writer) error {
	var (
		path        string
		home        string
		model       string
		debug       bool
		showVersion bool
	)
	flags := flag.NewFlagSet("acp-go-amp", flag.ContinueOnError)
	flags.SetOutput(errOutput)
	flags.StringVar(&path, "path", "", "native amp executable path")
	flags.StringVar(&home, "home", "", "parent directory for isolated Amp settings files")
	flags.StringVar(&model, "model", "", "default model; unsupported by Amp and rejected at session start")
	flags.BoolVar(&debug, "debug", false, "enable debug logging")
	flags.BoolVar(&showVersion, "version", false, "print adapter version and exit")
	if err := flags.Parse(args); err != nil {
		return err
	}

	if showVersion {
		fmt.Fprintln(output, version)
		return nil
	}

	level := slog.LevelInfo
	if debug {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(errOutput, &slog.HandlerOptions{Level: level}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	err := serveACP(ctx, input, output,
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
