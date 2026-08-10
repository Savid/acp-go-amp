package ampacp

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/coder/acp-go-sdk"
)

func (a *Agent) validateSessionStartOptions(options AmpOptions) error {
	if optionsErr := a.optionsError(); optionsErr != nil {
		return optionsErr
	}

	// Amp has no native config/auth root, so any configured Home is rejected
	// fail-closed on every session-establishing path (new/load/resume) with the
	// uniform unsupported "home" field error. Ephemeral state lives under
	// WithScratchDir instead.
	if a.options.Home != "" {
		return unsupportedField(optionFieldHome)
	}

	// Amp's disconnect releases the ledger slot a connection owns and performs
	// no native removal, so no leg here reads or clears an operator's canonical
	// native home and the exact-home consent gate has nothing to authorize.
	if a.options.ProviderAuthDirectHome != "" {
		return unsupportedField(optionFieldProviderAuthDirectHome)
	}

	if err := validateProcessIsolationOption(a.options.ProcessIsolation); err != nil {
		return err
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

	for key := range options.Env {
		if strings.HasPrefix(strings.ToUpper(key), privateEnvPrefix) {
			return acp.NewInvalidParams(map[string]any{jsonFieldField: "_meta.amp.options.env." + key})
		}
	}

	return nil
}

func validateProcessIsolationOption(isolation *ProcessIsolation) error {
	// Omission is the ordinary default: native work runs as the current
	// identity, so there is nothing to validate and nothing to refuse.
	if isolation == nil {
		return nil
	}

	if isolation.UID == 0 || isolation.GID == 0 {
		return errors.New("process isolation UID and GID must be nonzero")
	}

	if runtimeGOOS == platformLinux {
		if err := validateStandaloneIdentityOption(
			isolation.IdentityLock != nil, isolation.AuthorityDomain != nil,
			isolation.StandaloneOwnerID, isolation.StandaloneStateRoot,
			sharedProcessIdentity(isolation),
		); err != nil {
			return err
		}
	}

	if runtimeGOOS == platformWindows {
		return errors.New("process isolation is unsupported on windows")
	}

	return nil
}

// sharedIdentitySupervisorRemedy states what an operator can change when the
// supervisor was asked to launch the native process under the very identity it
// already runs as and the shape it was handed describes something else. There
// is no privilege boundary to cross in that deployment, so the two answers are
// to give the supervisor one, or to describe the launch as what it is.
const sharedIdentitySupervisorRemedy = "run the supervisor as root to isolate the agent identity, " +
	"or launch the agent under the identity the supervisor already holds"

func validateStandaloneIdentityOption(identityLock, authorityDomain bool, ownerID, stateRoot string, shared bool) error {
	if identityLock != authorityDomain {
		return errors.New("process identity lock and authority domain must be provided together")
	}

	if identityLock {
		if ownerID != "" || stateRoot != "" {
			return errors.New("borrowed process identity forbids standalone owner fields")
		}

		return nil
	}

	// A native identity that is already the one this process runs as cannot be
	// recorded as a standalone one: the durable record proves an identity no
	// live task holds, and the process asking for it is such a task. The
	// canonical shape is therefore no capabilities and no standalone fields.
	if shared {
		if ownerID != "" || stateRoot != "" {
			return errors.New("standalone owner fields describe an identity the supervisor already holds; " +
				sharedIdentitySupervisorRemedy)
		}

		return nil
	}

	if !validStandaloneOwnerID(ownerID) {
		return errors.New("standalone owner id must match [A-Za-z0-9][A-Za-z0-9._:@/-]{0,255}")
	}

	if !validStandaloneStateRootPath(stateRoot) {
		return errors.New("standalone state root must be a clean absolute path")
	}

	return nil
}

func validStandaloneStateRootPath(value string) bool {
	if value == "" || len(value) > 4096 || !utf8.ValidString(value) || !filepath.IsAbs(value) ||
		filepath.Clean(value) != value || value == "/" || strings.IndexByte(value, 0) >= 0 {
		return false
	}

	const authorityRoot = "/var/lib/acp-go/agent-identities"

	if value == authorityRoot || strings.HasPrefix(value, authorityRoot+string(filepath.Separator)) {
		return false
	}

	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}

	return true
}

func validStandaloneOwnerID(value string) bool {
	if value == "" || len(value) > 256 {
		return false
	}

	letterOrDigit := func(value byte) bool {
		return value >= 'A' && value <= 'Z' || value >= 'a' && value <= 'z' || value >= '0' && value <= '9'
	}

	if !letterOrDigit(value[0]) {
		return false
	}

	for _, character := range []byte(value[1:]) {
		if letterOrDigit(character) || strings.ContainsRune("._:@/-", rune(character)) {
			continue
		}

		return false
	}

	return true
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

			spec := map[string]any{keyURL: server.Http.Url}
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
	return []string{modeLow, modeMedium, modeHigh}
}
