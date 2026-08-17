package ampacp

import (
	"context"
	"encoding/json"

	"github.com/coder/acp-go-sdk"
)

const (
	ampOptionsKey       = "options"
	metaOutputSchemaKey = "outputSchema"
	metaEnabledKey      = "enabled"
)

// SessionRequestOption configures embedded-Go ACP session lifecycle requests.
type SessionRequestOption func(*sessionRequestConfig)

type sessionRequestConfig struct {
	mcpServers            []acp.McpServer
	additionalDirectories []string
	meta                  map[string]any
}

// NewSessionRequest constructs a session/new request with ACP-required empty
// slices initialized for embedded Go callers.
func NewSessionRequest(cwd string, opts ...SessionRequestOption) acp.NewSessionRequest {
	cfg := applySessionRequestOptions(opts)

	return acp.NewSessionRequest{
		Cwd:                   cwd,
		McpServers:            cfg.stableMCPServers(),
		AdditionalDirectories: cfg.additionalDirectoriesClone(),
		Meta:                  cloneAnyMap(cfg.meta),
	}
}

// LoadSessionRequest constructs a session/load request with ACP-required empty
// slices initialized for embedded Go callers.
func LoadSessionRequest(sessionID acp.SessionId, cwd string, opts ...SessionRequestOption) acp.LoadSessionRequest {
	cfg := applySessionRequestOptions(opts)

	return acp.LoadSessionRequest{
		SessionId:             sessionID,
		Cwd:                   cwd,
		McpServers:            cfg.stableMCPServers(),
		AdditionalDirectories: cfg.additionalDirectoriesClone(),
		Meta:                  cloneAnyMap(cfg.meta),
	}
}

// ResumeSessionRequest constructs a session/resume request.
func ResumeSessionRequest(sessionID acp.SessionId, cwd string, opts ...SessionRequestOption) acp.ResumeSessionRequest {
	cfg := applySessionRequestOptions(opts)

	return acp.ResumeSessionRequest{
		SessionId:             sessionID,
		Cwd:                   cwd,
		McpServers:            cfg.stableMCPServers(),
		AdditionalDirectories: cfg.additionalDirectoriesClone(),
		Meta:                  cloneAnyMap(cfg.meta),
	}
}

// ForkSessionRequest constructs params for the Amp fork extension method.
func ForkSessionRequest(sessionID acp.SessionId, cwd string, opts ...SessionRequestOption) acp.UnstableForkSessionRequest {
	cfg := applySessionRequestOptions(opts)

	return acp.UnstableForkSessionRequest{
		SessionId:             sessionID,
		Cwd:                   cwd,
		McpServers:            unstableMCPServersFromStable(cfg.stableMCPServers()),
		AdditionalDirectories: cfg.additionalDirectoriesClone(),
		Meta:                  cloneAnyMap(cfg.meta),
	}
}

// CallForkSession calls the Amp fork extension method and decodes the SDK payload shape.
func CallForkSession(
	ctx context.Context,
	conn *acp.ClientSideConnection,
	params acp.UnstableForkSessionRequest,
) (acp.UnstableForkSessionResponse, error) {
	raw, err := conn.CallExtension(ctx, ForkSessionMethod, params)
	if err != nil {
		return acp.UnstableForkSessionResponse{}, err
	}

	var resp acp.UnstableForkSessionResponse

	err = json.Unmarshal(raw, &resp)

	return resp, err
}

// DeleteSessionRequest constructs a session/delete request.
func DeleteSessionRequest(sessionID acp.SessionId) acp.UnstableDeleteSessionRequest {
	return acp.UnstableDeleteSessionRequest{SessionId: sessionID}
}

// WithSessionMCPServers sets MCP servers for a session lifecycle request.
func WithSessionMCPServers(servers ...acp.McpServer) SessionRequestOption {
	cloned := cloneMCPServers(servers)

	return func(cfg *sessionRequestConfig) {
		cfg.mcpServers = cloneMCPServers(cloned)
	}
}

// WithSessionAdditionalDirectories sets additional workspace directories for a
// session lifecycle request.
func WithSessionAdditionalDirectories(paths ...string) SessionRequestOption {
	cloned := append([]string(nil), paths...)

	return func(cfg *sessionRequestConfig) {
		cfg.additionalDirectories = append([]string(nil), cloned...)
	}
}

// WithSessionMeta sets metadata for a session lifecycle request.
func WithSessionMeta(meta map[string]any) SessionRequestOption {
	cloned := cloneAnyMap(meta)

	return func(cfg *sessionRequestConfig) {
		cfg.meta = cloned
	}
}

// WithSessionOutputSchema sets Amp structured-output schema for a session
// lifecycle request.
func WithSessionOutputSchema(schema map[string]any) SessionRequestOption {
	return func(cfg *sessionRequestConfig) {
		mergeAmpOptionsMeta(cfg, map[string]any{metaOutputSchemaKey: cloneAnyMap(schema)})
	}
}

// WithSessionRawEvents toggles raw Amp event emission for a session lifecycle request.
func WithSessionRawEvents(enabled bool) SessionRequestOption {
	return func(cfg *sessionRequestConfig) {
		ampMeta := ensureAmpMeta(cfg)
		ampMeta[metaRawEventKey] = map[string]any{metaEnabledKey: enabled}
	}
}

// WithSessionAmpOptions merges Amp-specific options into a session lifecycle
// request's _meta.amp.options object.
func WithSessionAmpOptions(options AmpOptions) SessionRequestOption {
	return func(cfg *sessionRequestConfig) {
		mergeAmpOptionsMeta(cfg, ampOptionsPayload(options))
	}
}

// StdioMCPServer constructs an ACP stdio MCP server declaration.
func StdioMCPServer(name string, command string, args []string, env map[string]string) acp.McpServer {
	envVars := make([]acp.EnvVariable, 0, len(env))
	for key, value := range env {
		envVars = append(envVars, acp.EnvVariable{Name: key, Value: value})
	}

	return acp.McpServer{
		Stdio: &acp.McpServerStdio{
			Name:    name,
			Command: command,
			Args:    append([]string(nil), args...),
			Env:     envVars,
		},
	}
}

// HTTPMCPServer constructs an ACP HTTP MCP server declaration.
func HTTPMCPServer(name string, url string, headers map[string]string) acp.McpServer {
	out := make([]acp.HttpHeader, 0, len(headers))
	for key, value := range headers {
		out = append(out, acp.HttpHeader{Name: key, Value: value})
	}

	return acp.McpServer{
		Http: &acp.McpServerHttpInline{
			Name:    name,
			Url:     url,
			Headers: out,
		},
	}
}

// PromptRequest constructs a session/prompt request with a non-nil prompt slice
// for embedded Go callers.
func PromptRequest(sessionID acp.SessionId, turnNonce string, blocks ...acp.ContentBlock) acp.PromptRequest {
	_ = turnNonce

	return acp.PromptRequest{
		SessionId: sessionID,
		Prompt:    append([]acp.ContentBlock{}, blocks...),
	}
}

// TextPromptRequest constructs a session/prompt request containing one text block.
func TextPromptRequest(sessionID acp.SessionId, turnNonce, text string) acp.PromptRequest {
	return PromptRequest(sessionID, turnNonce, acp.TextBlock(text))
}

// CancelRequest builds an Amp cancellation. Amp has no elicitation route capability.
func CancelRequest(sessionID acp.SessionId, turnNonce string) acp.CancelNotification {
	_ = turnNonce

	return acp.CancelNotification{SessionId: sessionID}
}

// SetConfigOptionRequest constructs a value-id session/set_config_option request.
func SetConfigOptionRequest(sessionID acp.SessionId, configID acp.SessionConfigId, value acp.SessionConfigValueId) acp.SetSessionConfigOptionRequest {
	return acp.SetSessionConfigOptionRequest{
		ValueId: &acp.SetSessionConfigOptionValueId{
			SessionId: sessionID,
			ConfigId:  configID,
			Value:     value,
		},
	}
}

// SetModelRequest constructs a model selector update request.
func SetModelRequest(sessionID acp.SessionId, model string) acp.SetSessionConfigOptionRequest {
	return SetConfigOptionRequest(sessionID, optionModelKey, acp.SessionConfigValueId(model))
}

// ListSessionsRequestOption configures embedded-Go session/list requests.
type ListSessionsRequestOption func(*acp.ListSessionsRequest)

// ListSessionsRequest constructs a session/list request.
func ListSessionsRequest(opts ...ListSessionsRequestOption) acp.ListSessionsRequest {
	req := acp.ListSessionsRequest{}

	for _, opt := range opts {
		if opt != nil {
			opt(&req)
		}
	}

	return req
}

// WithListSessionsCwd filters session/list by cwd.
func WithListSessionsCwd(cwd string) ListSessionsRequestOption {
	return func(req *acp.ListSessionsRequest) {
		req.Cwd = &cwd
	}
}

// WithListSessionsCursor sets the cursor for session/list pagination.
func WithListSessionsCursor(cursor string) ListSessionsRequestOption {
	return func(req *acp.ListSessionsRequest) {
		req.Cursor = &cursor
	}
}

// WithListSessionsMeta sets metadata on a session/list request.
func WithListSessionsMeta(meta map[string]any) ListSessionsRequestOption {
	cloned := cloneAnyMap(meta)

	return func(req *acp.ListSessionsRequest) {
		req.Meta = cloned
	}
}

func applySessionRequestOptions(opts []SessionRequestOption) sessionRequestConfig {
	cfg := sessionRequestConfig{mcpServers: []acp.McpServer{}}

	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	if cfg.meta == nil {
		cfg.meta = map[string]any{}
	}

	return cfg
}

func (cfg sessionRequestConfig) stableMCPServers() []acp.McpServer {
	if cfg.mcpServers == nil {
		return []acp.McpServer{}
	}

	return cloneMCPServers(cfg.mcpServers)
}

func (cfg sessionRequestConfig) additionalDirectoriesClone() []string {
	return append([]string(nil), cfg.additionalDirectories...)
}

func ensureAmpMeta(cfg *sessionRequestConfig) map[string]any {
	if cfg.meta == nil {
		cfg.meta = map[string]any{}
	}

	raw, _ := cfg.meta[ampMetaKey].(map[string]any)
	if raw == nil {
		raw = map[string]any{}
		cfg.meta[ampMetaKey] = raw
	}

	return raw
}

func mergeAmpOptionsMeta(cfg *sessionRequestConfig, values map[string]any) {
	ampMeta := ensureAmpMeta(cfg)

	options, _ := ampMeta[ampOptionsKey].(map[string]any)
	if options == nil {
		options = map[string]any{}
		ampMeta[ampOptionsKey] = options
	}

	for key, value := range values {
		options[key] = value
	}
}

func ampOptionsPayload(options AmpOptions) map[string]any {
	payload := map[string]any{}
	if options.Model != "" {
		payload[optionModelKey] = options.Model
	}

	if len(options.Env) > 0 {
		payload[optionEnvKey] = cloneStringMap(options.Env)
	}

	if options.OutputSchema != nil {
		payload[metaOutputSchemaKey] = cloneAnyMap(options.OutputSchema)
	}

	if options.Mode != "" {
		payload[optionModeKey] = options.Mode
	}

	return payload
}

func cloneMCPServers(servers []acp.McpServer) []acp.McpServer {
	if servers == nil {
		return nil
	}

	cloned := make([]acp.McpServer, len(servers))
	for index, server := range servers {
		cloned[index] = cloneMCPServer(server)
	}

	return cloned
}

func cloneMCPServer(server acp.McpServer) acp.McpServer {
	switch {
	case server.Http != nil:
		value := *server.Http
		value.Meta = cloneAnyMap(value.Meta)
		value.Headers = cloneHTTPHeaders(value.Headers)

		return acp.McpServer{Http: &value}
	case server.Sse != nil:
		value := *server.Sse
		value.Meta = cloneAnyMap(value.Meta)
		value.Headers = cloneHTTPHeaders(value.Headers)

		return acp.McpServer{Sse: &value}
	case server.Acp != nil:
		value := *server.Acp
		value.Meta = cloneAnyMap(value.Meta)

		return acp.McpServer{Acp: &value}
	case server.Stdio != nil:
		value := cloneMCPServerStdio(server.Stdio)

		return acp.McpServer{Stdio: value}
	default:
		return acp.McpServer{}
	}
}

func cloneMCPServerStdio(server *acp.McpServerStdio) *acp.McpServerStdio {
	if server == nil {
		return nil
	}

	value := *server
	value.Meta = cloneAnyMap(value.Meta)
	value.Args = append([]string(nil), value.Args...)
	value.Env = cloneEnvVariables(value.Env)

	return &value
}

func cloneHTTPHeaders(headers []acp.HttpHeader) []acp.HttpHeader {
	if headers == nil {
		return nil
	}

	cloned := make([]acp.HttpHeader, len(headers))
	for index, header := range headers {
		cloned[index] = header
		cloned[index].Meta = cloneAnyMap(header.Meta)
	}

	return cloned
}

func cloneEnvVariables(env []acp.EnvVariable) []acp.EnvVariable {
	if env == nil {
		return nil
	}

	cloned := make([]acp.EnvVariable, len(env))
	for index, variable := range env {
		cloned[index] = variable
		cloned[index].Meta = cloneAnyMap(variable.Meta)
	}

	return cloned
}

func unstableMCPServersFromStable(servers []acp.McpServer) []acp.UnstableMcpServer {
	out := make([]acp.UnstableMcpServer, 0, len(servers))
	for _, server := range servers {
		out = append(out, unstableMCPServerFromStable(server))
	}

	return out
}

func unstableMCPServerFromStable(server acp.McpServer) acp.UnstableMcpServer {
	switch {
	case server.Http != nil:
		value := acp.UnstableMcpServerHttp{
			Meta:    cloneAnyMap(server.Http.Meta),
			Headers: cloneHTTPHeaders(server.Http.Headers),
			Name:    server.Http.Name,
			Type:    server.Http.Type,
			Url:     server.Http.Url,
		}

		return acp.UnstableMcpServer{Http: &value}
	case server.Sse != nil:
		value := acp.UnstableMcpServerSse{
			Meta:    cloneAnyMap(server.Sse.Meta),
			Headers: cloneHTTPHeaders(server.Sse.Headers),
			Name:    server.Sse.Name,
			Type:    server.Sse.Type,
			Url:     server.Sse.Url,
		}

		return acp.UnstableMcpServer{Sse: &value}
	case server.Acp != nil:
		value := acp.UnstableMcpServerAcpInline{
			Meta: cloneAnyMap(server.Acp.Meta),
			Id:   acp.UnstableMcpServerAcpId(server.Acp.Id),
			Name: server.Acp.Name,
			Type: server.Acp.Type,
		}

		return acp.UnstableMcpServer{Acp: &value}
	case server.Stdio != nil:
		return acp.UnstableMcpServer{Stdio: cloneMCPServerStdio(server.Stdio)}
	default:
		return acp.UnstableMcpServer{}
	}
}
