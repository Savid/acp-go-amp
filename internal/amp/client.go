// Package amp implements the native Amp command and stream-json boundary.
package amp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/savid/acp-go-core/process"
)

const (
	InternalEnvPrefix        = "ACP_GO_AMP_INTERNAL_"
	MaxInputImageBytes int64 = 5138022
	MaxImageDimension  int64 = 8000
	MaxNativeBytes           = 64 << 20
)

var threadIDPattern = regexp.MustCompile(`^T-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// ValidateThreadID accepts exactly a native thread identity.
func ValidateThreadID(id string) error {
	if !threadIDPattern.MatchString(id) {
		return errors.New("invalid native thread id")
	}

	return nil
}

// ParseThreadURL extracts the durable identity returned by threads new.
func ParseThreadURL(data []byte) (string, error) {
	text := strings.TrimSpace(string(data))

	parsed, err := url.Parse(text)
	if err != nil || (parsed.Scheme != schemeHTTPS && parsed.Scheme != schemeHTTP) || parsed.Host == "" {
		return "", errors.New("invalid native thread URL")
	}

	id := path.Base(parsed.Path)

	return id, ValidateThreadID(id)
}

// Run executes one native command, owns every pipe, and joins all readers.
// consume receives complete JSON lines when non-nil; otherwise stdout is captured.
func Run(ctx context.Context, request process.Request, input []byte, consume func([]byte) error) ([]byte, process.Result, string, error) {
	return run(ctx, request, func(p *process.Process) error {
		var err error
		if len(input) > 0 {
			_, err = p.Stdin().Write(input)
		}

		return errors.Join(err, p.Stdin().Close())
	}, consume)
}

func run(ctx context.Context, request process.Request, write func(*process.Process) error, consume func([]byte) error) ([]byte, process.Result, string, error) {
	p, err := process.Start(ctx, request)
	if err != nil {
		return nil, process.Result{}, "", err
	}
	defer func() { _ = p.Close() }()

	readDone := make(chan struct{})
	stopDone := make(chan struct{})
	// The watcher also bounds inherited pipes held open after the root exits.
	go func() {
		defer close(stopDone)

		select {
		case <-ctx.Done():
			stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()

			if stopErr := p.Shutdown(stopCtx, time.Second); stopErr != nil {
				_ = p.Kill()
			}

			_ = p.Close()
		case <-p.Done():
			timer := time.NewTimer(2 * time.Second)
			defer timer.Stop()

			select {
			case <-readDone:
			case <-timer.C:
				_ = p.Close()
			}
		}
	}()

	writeDone := make(chan error, 1)

	go func() {
		writeErr := write(p)
		if writeErr != nil {
			_ = p.Kill()
		}

		writeDone <- writeErr
	}()

	var output []byte
	if consume == nil {
		output, err = io.ReadAll(io.LimitReader(p.Stdout(), MaxNativeBytes+1))
		if len(output) > MaxNativeBytes {
			err = errors.New("native output exceeds maximum size")
		}
	} else {
		scanner := bufio.NewScanner(p.Stdout())
		scanner.Buffer(make([]byte, 64*1024), MaxNativeBytes)

		// Only object lines are frames; anything else Amp prints on stdout is
		// noise the stream ignores.
		for scanner.Scan() {
			line := bytes.TrimSpace(scanner.Bytes())
			if len(line) == 0 || line[0] != '{' {
				continue
			}

			if err = consume(bytes.Clone(scanner.Bytes())); err != nil {
				break
			}
		}

		if err == nil {
			err = scanner.Err()
		}
	}

	if err != nil {
		_ = p.Kill()
	}

	result, waitErr := p.Wait(ctx)
	if waitErr != nil {
		_ = p.Kill()
		result, waitErr = p.Wait(context.WithoutCancel(ctx))
	}

	writeErr := <-writeDone
	stderr := p.StderrLastLine()

	close(readDone)
	<-stopDone

	if ctx.Err() != nil {
		return output, result, stderr, ctx.Err()
	}

	return output, result, stderr, errors.Join(err, waitErr, writeErr)
}

// Frame is one stream-json record. Unknown types remain inert.
type Frame struct {
	Type            string `json:"type"`
	Subtype         string `json:"subtype"`
	SessionID       string `json:"session_id"`         //nolint:tagliatelle // Native stream-json uses snake_case.
	AgentMode       string `json:"agent_mode"`         //nolint:tagliatelle // Native stream-json uses snake_case.
	ParentToolUseID string `json:"parent_tool_use_id"` //nolint:tagliatelle // Native stream-json uses snake_case.
	Message         struct {
		Content    []map[string]any `json:"content"`
		StopReason string           `json:"stop_reason"` //nolint:tagliatelle // Native stream-json uses snake_case.
		Usage      *Usage           `json:"usage"`
	} `json:"message"`
	Usage   *Usage `json:"usage"`
	IsError bool   `json:"is_error"` //nolint:tagliatelle // Native stream-json uses snake_case.
	Error   string `json:"error"`
	Result  string `json:"result"`
}

// Usage carries native token accounting from a stream-json frame.
type Usage struct {
	InputTokens              int    `json:"input_tokens"`                //nolint:tagliatelle // Native stream-json uses snake_case.
	OutputTokens             int    `json:"output_tokens"`               //nolint:tagliatelle // Native stream-json uses snake_case.
	CacheReadInputTokens     int    `json:"cache_read_input_tokens"`     //nolint:tagliatelle // Native stream-json uses snake_case.
	CacheCreationInputTokens int    `json:"cache_creation_input_tokens"` //nolint:tagliatelle // Native stream-json uses snake_case.
	MaxTokens                int    `json:"max_tokens"`                  //nolint:tagliatelle // Native stream-json uses snake_case.
	ServiceTier              string `json:"service_tier"`                //nolint:tagliatelle // Native stream-json uses snake_case.
}

// DecodeFrame validates the framing without rejecting new native event types.
func DecodeFrame(data []byte) (Frame, error) {
	var f Frame

	err := json.Unmarshal(data, &f)
	if err == nil && f.Type == "" {
		err = errors.New("native frame lacks type")
	}

	return f, err
}
