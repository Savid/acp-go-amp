package ampacp

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"slices"
	"strings"

	"github.com/coder/acp-go-sdk"
)

func (a *Agent) validateSessionStartOptions(options AmpOptions) error {
	// Amp has no native config/auth root, so any configured Home is rejected
	// fail-closed on every session-establishing path (new/load/resume) with the
	// uniform unsupported "home" field error. Ephemeral state lives under
	// WithScratchDir instead.
	if a.options.Home != "" {
		return unsupportedField(optionFieldHome)
	}

	if a.options.DefaultModel != "" {
		return unsupportedField(optionModelKey)
	}

	if options.Model != "" {
		return unsupportedField(optionModelKey)
	}

	if options.OutputSchema != nil {
		return unsupportedField(metaOutputSchemaKey)
	}

	if options.Mode != "" && !slices.Contains(validModes(), options.Mode) {
		return acp.NewInvalidParams(map[string]any{jsonFieldField: "_meta.amp.options.mode"})
	}

	if options.Effort != "" && !slices.Contains(validEfforts(), options.Effort) {
		return acp.NewInvalidParams(map[string]any{jsonFieldField: "_meta.amp.options.effort"})
	}

	return nil
}

// validateOptionalAbsolutePath rejects a present-but-relative filter path with
// the uniform invalid-params shape; an absent or empty filter is valid.
func validateOptionalAbsolutePath(field string, path *string) error {
	if path == nil || *path == "" {
		return nil
	}

	if !filepath.IsAbs(*path) {
		return acp.NewInvalidParams(map[string]any{jsonFieldField: field})
	}

	return nil
}

func validateSessionPaths(cwd string, additionalDirs []string) error {
	if cwd == "" || !filepath.IsAbs(cwd) {
		return acp.NewInvalidParams(map[string]any{jsonFieldField: "cwd"})
	}

	for i, dir := range additionalDirs {
		if dir == "" || !filepath.IsAbs(dir) {
			return acp.NewInvalidParams(map[string]any{jsonFieldField: fmt.Sprintf("additionalDirectories[%d]", i)})
		}
	}

	return nil
}

func mismatchField(field string) error {
	return acp.NewInvalidParams(map[string]any{jsonFieldError: "mismatch", jsonFieldField: field})
}

// reserveMCPName enforces the adapter's MCP-name contract for the declaration at
// index i: every accepted server carries a name that is not empty or
// whitespace-only and is unique within the request. The raw name is stored and
// forwarded verbatim; names are never fabricated, rewritten, or deduplicated.
func reserveMCPName(seen map[string]struct{}, name string, i int) error {
	field := fmt.Sprintf("mcpServers[%d].name", i)
	if strings.TrimSpace(name) == "" {
		return acp.NewInvalidParams(map[string]any{field: valRequired})
	}

	if _, dup := seen[name]; dup {
		return acp.NewInvalidParams(map[string]any{field: valDuplicate})
	}

	seen[name] = struct{}{}

	return nil
}

func mcpConfigJSON(servers []acp.McpServer) (string, error) {
	if len(servers) == 0 {
		return "", nil
	}

	payload := make(map[string]any, len(servers))

	seen := make(map[string]struct{}, len(servers))
	for i, server := range servers {
		switch {
		case server.Stdio != nil:
			if err := reserveMCPName(seen, server.Stdio.Name, i); err != nil {
				return "", err
			}

			spec := map[string]any{"command": server.Stdio.Command}
			if len(server.Stdio.Args) > 0 {
				spec["args"] = server.Stdio.Args
			}

			if len(server.Stdio.Env) > 0 {
				env := make(map[string]string, len(server.Stdio.Env))
				for _, item := range server.Stdio.Env {
					env[item.Name] = item.Value
				}

				spec["env"] = env
			}

			payload[server.Stdio.Name] = spec
		case server.Http != nil:
			if err := reserveMCPName(seen, server.Http.Name, i); err != nil {
				return "", err
			}

			spec := map[string]any{"url": server.Http.Url}
			if len(server.Http.Headers) > 0 {
				headers := make(map[string]string, len(server.Http.Headers))
				for _, item := range server.Http.Headers {
					headers[item.Name] = item.Value
				}

				spec["headers"] = headers
			}

			payload[server.Http.Name] = spec
		case server.Sse != nil:
			return "", acp.NewInvalidParams(map[string]any{
				jsonFieldError:  valUnsupported,
				jsonFieldField:  fmt.Sprintf("mcpServers[%d]", i),
				jsonFieldServer: server.Sse.Name,
			})
		case server.Acp != nil:
			return "", acp.NewInvalidParams(map[string]any{
				jsonFieldError:  valUnsupported,
				jsonFieldField:  fmt.Sprintf("mcpServers[%d]", i),
				jsonFieldServer: server.Acp.Name,
			})
		default:
			return "", acp.NewInvalidParams(map[string]any{
				jsonFieldError: valNoTransport,
				jsonFieldField: fmt.Sprintf("mcpServers[%d]", i),
			})
		}
	}

	data, _ := json.Marshal(payload)

	return string(data), nil
}

func validModes() []string {
	return []string{modeSmart, modeDeep, modeRush}
}

func validEfforts() []string {
	return []string{effortNone, effortMinimal, effortLow, effortMedium, effortHigh, effortXHigh, effortMax}
}
