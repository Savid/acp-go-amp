//nolint:goconst,wsl_v5,nlreturn // request builders intentionally stay small and literal.
package ampacp

import (
	"context"
	"encoding/json"

	"github.com/coder/acp-go-sdk"
)

type SessionRequestOption func(*sessionRequestConfig)

type sessionRequestConfig struct {
	mcpServers            []acp.McpServer
	additionalDirectories []string
	meta                  map[string]any
}

func NewSessionRequest(cwd string, opts ...SessionRequestOption) acp.NewSessionRequest {
	cfg := applySessionRequestOptions(opts)
	return acp.NewSessionRequest{
		Cwd:                   cwd,
		McpServers:            cfg.mcpServers,
		AdditionalDirectories: cfg.additionalDirectories,
		Meta:                  cfg.meta,
	}
}

func LoadSessionRequest(sessionID acp.SessionId, cwd string, opts ...SessionRequestOption) acp.LoadSessionRequest {
	cfg := applySessionRequestOptions(opts)
	return acp.LoadSessionRequest{
		SessionId:             sessionID,
		Cwd:                   cwd,
		McpServers:            cfg.mcpServers,
		AdditionalDirectories: cfg.additionalDirectories,
		Meta:                  cfg.meta,
	}
}

func ResumeSessionRequest(sessionID acp.SessionId, cwd string, opts ...SessionRequestOption) acp.ResumeSessionRequest {
	cfg := applySessionRequestOptions(opts)
	return acp.ResumeSessionRequest{
		SessionId:             sessionID,
		Cwd:                   cwd,
		McpServers:            cfg.mcpServers,
		AdditionalDirectories: cfg.additionalDirectories,
		Meta:                  cfg.meta,
	}
}

func ForkSessionRequest(sessionID acp.SessionId, cwd string, opts ...SessionRequestOption) acp.UnstableForkSessionRequest {
	cfg := applySessionRequestOptions(opts)
	return acp.UnstableForkSessionRequest{
		SessionId:             sessionID,
		Cwd:                   cwd,
		McpServers:            unstableMCPServers(cfg.mcpServers),
		AdditionalDirectories: cfg.additionalDirectories,
		Meta:                  cfg.meta,
	}
}

func CallForkSession(ctx context.Context, conn *acp.ClientSideConnection, params acp.UnstableForkSessionRequest) (acp.UnstableForkSessionResponse, error) {
	raw, err := conn.CallExtension(ctx, ForkSessionMethod, params)
	if err != nil {
		return acp.UnstableForkSessionResponse{}, err
	}
	var resp acp.UnstableForkSessionResponse
	err = json.Unmarshal(raw, &resp)
	return resp, err
}

func DeleteSessionRequest(sessionID acp.SessionId) acp.UnstableDeleteSessionRequest {
	return acp.UnstableDeleteSessionRequest{SessionId: sessionID}
}

func WithSessionMCPServers(servers ...acp.McpServer) SessionRequestOption {
	return func(cfg *sessionRequestConfig) { cfg.mcpServers = append([]acp.McpServer(nil), servers...) }
}

func WithSessionAdditionalDirectories(paths ...string) SessionRequestOption {
	return func(cfg *sessionRequestConfig) { cfg.additionalDirectories = append([]string(nil), paths...) }
}

func WithSessionMeta(meta map[string]any) SessionRequestOption {
	return func(cfg *sessionRequestConfig) { cfg.meta = cloneAnyMap(meta) }
}

func WithSessionOutputSchema(schema map[string]any) SessionRequestOption {
	return func(cfg *sessionRequestConfig) {
		mergeAmpOptionsMeta(cfg, map[string]any{"outputSchema": cloneAnyMap(schema)})
	}
}

func WithSessionRawEvents(enabled bool) SessionRequestOption {
	return func(cfg *sessionRequestConfig) {
		ampMeta := ensureAmpMeta(cfg)
		ampMeta["rawEvent"] = map[string]any{"enabled": enabled}
	}
}

func WithSessionAmpOptions(options AmpOptions) SessionRequestOption {
	return func(cfg *sessionRequestConfig) {
		mergeAmpOptionsMeta(cfg, ampOptionsPayload(options))
	}
}

func StdioMCPServer(name string, command string, args []string, env map[string]string) acp.McpServer {
	envVars := make([]acp.EnvVariable, 0, len(env))
	for key, value := range env {
		envVars = append(envVars, acp.EnvVariable{Name: key, Value: value})
	}
	return acp.McpServer{Stdio: &acp.McpServerStdio{
		Name:    name,
		Command: command,
		Args:    append([]string(nil), args...),
		Env:     envVars,
	}}
}

func HTTPMCPServer(name string, url string, headers map[string]string) acp.McpServer {
	out := make([]acp.HttpHeader, 0, len(headers))
	for key, value := range headers {
		out = append(out, acp.HttpHeader{Name: key, Value: value})
	}
	return acp.McpServer{Http: &acp.McpServerHttpInline{
		Type:    "http",
		Name:    name,
		Url:     url,
		Headers: out,
	}}
}

func PromptRequest(sessionID acp.SessionId, blocks ...acp.ContentBlock) acp.PromptRequest {
	return acp.PromptRequest{SessionId: sessionID, Prompt: blocks}
}

func TextPromptRequest(sessionID acp.SessionId, text string) acp.PromptRequest {
	return PromptRequest(sessionID, acp.TextBlock(text))
}

func SetConfigOptionRequest(sessionID acp.SessionId, configID acp.SessionConfigId, value acp.SessionConfigValueId) acp.SetSessionConfigOptionRequest {
	return acp.SetSessionConfigOptionRequest{ValueId: &acp.SetSessionConfigOptionValueId{
		SessionId: sessionID,
		ConfigId:  configID,
		Value:     value,
	}}
}

func SetModelRequest(sessionID acp.SessionId, model string) acp.SetSessionConfigOptionRequest {
	return SetConfigOptionRequest(sessionID, "model", acp.SessionConfigValueId(model))
}

type ListSessionsRequestOption func(*acp.ListSessionsRequest)

func ListSessionsRequest(opts ...ListSessionsRequestOption) acp.ListSessionsRequest {
	req := acp.ListSessionsRequest{}
	for _, opt := range opts {
		if opt != nil {
			opt(&req)
		}
	}
	return req
}

func WithListSessionsCwd(cwd string) ListSessionsRequestOption {
	return func(req *acp.ListSessionsRequest) { req.Cwd = &cwd }
}

func WithListSessionsCursor(cursor string) ListSessionsRequestOption {
	return func(req *acp.ListSessionsRequest) { req.Cursor = &cursor }
}

func WithListSessionsMeta(meta map[string]any) ListSessionsRequestOption {
	return func(req *acp.ListSessionsRequest) { req.Meta = cloneAnyMap(meta) }
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

func ensureAmpMeta(cfg *sessionRequestConfig) map[string]any {
	if cfg.meta == nil {
		cfg.meta = map[string]any{}
	}
	raw, _ := cfg.meta["amp"].(map[string]any)
	if raw == nil {
		raw = map[string]any{}
		cfg.meta["amp"] = raw
	}
	return raw
}

func mergeAmpOptionsMeta(cfg *sessionRequestConfig, values map[string]any) {
	ampMeta := ensureAmpMeta(cfg)
	options, _ := ampMeta["options"].(map[string]any)
	if options == nil {
		options = map[string]any{}
		ampMeta["options"] = options
	}
	for key, value := range values {
		options[key] = value
	}
}

func ampOptionsPayload(options AmpOptions) map[string]any {
	payload := map[string]any{}
	if options.Model != "" {
		payload["model"] = options.Model
	}
	if len(options.Env) > 0 {
		payload["env"] = cloneStringMap(options.Env)
	}
	if options.OutputSchema != nil {
		payload["outputSchema"] = cloneAnyMap(options.OutputSchema)
	}
	if options.Mode != "" {
		payload["mode"] = options.Mode
	}
	if options.Effort != "" {
		payload["effort"] = options.Effort
	}
	return payload
}

func unstableMCPServers(servers []acp.McpServer) []acp.UnstableMcpServer {
	out := make([]acp.UnstableMcpServer, 0, len(servers))
	for _, server := range servers {
		out = append(out, acp.UnstableMcpServer{
			Stdio: server.Stdio,
			Http:  unstableHTTPMCP(server.Http),
			Sse:   unstableSSEMCP(server.Sse),
			Acp:   unstableACPMCP(server.Acp),
		})
	}
	return out
}

func unstableHTTPMCP(server *acp.McpServerHttpInline) *acp.UnstableMcpServerHttp {
	if server == nil {
		return nil
	}
	return &acp.UnstableMcpServerHttp{Name: server.Name, Url: server.Url, Headers: server.Headers}
}

func unstableSSEMCP(server *acp.McpServerSseInline) *acp.UnstableMcpServerSse {
	if server == nil {
		return nil
	}
	return &acp.UnstableMcpServerSse{Name: server.Name, Url: server.Url, Headers: server.Headers}
}

func unstableACPMCP(server *acp.McpServerAcpInline) *acp.UnstableMcpServerAcpInline {
	if server == nil {
		return nil
	}
	return &acp.UnstableMcpServerAcpInline{Name: server.Name, Id: acp.UnstableMcpServerAcpId(server.Id)}
}
