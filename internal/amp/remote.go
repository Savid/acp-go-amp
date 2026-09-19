package amp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/savid/acp-go-core/process"
	"golang.org/x/net/http/httpproxy"
)

const (
	schemeHTTP  = "http"
	schemeHTTPS = "https"
	fieldThread = "thread"
)

// The public routing key selects Amp's actor namespace; the websocket token authorizes its thread.
const actorRoutingKey = "pk_9tm4qz3zrMerdZXTlBRLRsmJIzSQIPH24meKBqiL6vVpscTvc4w1YPiBgymXf9Az"

// Remote uses the native installation's service address and current credentials.
type Remote struct {
	base       *url.URL
	actor      *url.URL
	routingKey string
	request    process.Request
	client     *http.Client
}

// ServiceURL extracts the service origin from a native thread URL.
func ServiceURL(data []byte) (string, error) {
	if _, err := ParseThreadURL(data); err != nil {
		return "", err
	}

	parsed, err := url.Parse(strings.TrimSpace(string(data)))
	if err != nil || parsed.User != nil {
		return "", errors.New("invalid native service URL")
	}

	return parsed.Scheme + "://" + parsed.Host + "/", nil
}

// NewRemote follows native environment selection without storing or refreshing credentials.
func NewRemote(request process.Request, serviceURL string) (*Remote, error) {
	base, err := url.Parse(serviceURL)
	if err != nil || (base.Scheme != schemeHTTP && base.Scheme != schemeHTTPS) || base.Host == "" || base.User != nil || base.RawQuery != "" || base.Fragment != "" || base.Path != "/" {
		return nil, errors.New("invalid native service URL")
	}

	actor := base.ResolveReference(&url.URL{Path: "/actors"})
	key := actorRoutingKey

	if endpoint, ok := process.Lookup(request.Env, "RIVET_PUBLIC_ENDPOINT"); ok && endpoint != "" {
		actor, err = url.Parse(endpoint)
		if err != nil || actor.Host == "" || (actor.Scheme != schemeHTTP && actor.Scheme != schemeHTTPS) {
			return nil, errors.New("invalid native actor endpoint")
		}

		if actor.User != nil {
			key, _ = actor.User.Password()
			actor.User = nil
		}
	}

	lookup := func(name string) string {
		value, _ := process.Lookup(request.Env, name)

		return value
	}
	proxy := (&httpproxy.Config{HTTPProxy: lookup("HTTP_PROXY"), HTTPSProxy: lookup("HTTPS_PROXY"), NoProxy: lookup("NO_PROXY")}).ProxyFunc()

	transport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, errors.New("native HTTP transport is unavailable")
	}

	transport = transport.Clone()
	transport.Proxy = func(request *http.Request) (*url.URL, error) { return proxy(request.URL) }

	return &Remote{base: base, actor: actor, routingKey: key, request: request, client: &http.Client{Timeout: 30 * time.Second, Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

// Close releases idle connections owned by this restore attempt.
func (r *Remote) Close() { r.client.CloseIdleConnections() }

func (r *Remote) apiKey() (string, error) {
	if key, ok := process.Lookup(r.request.Env, "AMP_API_KEY"); ok && key != "" {
		return key, nil
	}

	root, _ := process.Lookup(r.request.Env, "XDG_DATA_HOME")
	if root == "" {
		home, _ := process.Lookup(r.request.Env, "HOME")
		if home == "" {
			return "", errors.New("native credential directory is missing")
		}

		root = filepath.Join(home, ".local", "share")
	}

	if !filepath.IsAbs(root) {
		root = filepath.Join(r.request.Dir, root)
	}

	data, err := os.ReadFile(filepath.Join(root, "amp", "secrets.json"))
	if err != nil {
		return "", errors.New("native credentials are unavailable")
	}

	var secrets map[string]string
	if json.Unmarshal(data, &secrets) != nil {
		return "", errors.New("invalid native credential store")
	}

	key := secrets["apiKey@"+r.base.String()]
	if key == "" {
		return "", errors.New("native service credential is missing")
	}

	return key, nil
}

func (r *Remote) send(ctx context.Context, target *url.URL, body any, headers http.Header, result any) (int, error) {
	encoded, err := json.Marshal(body)
	if err != nil {
		return 0, err
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target.String(), bytes.NewReader(encoded))
	if err != nil {
		return 0, errors.New("invalid native API request")
	}

	key, err := r.apiKey()
	if err != nil {
		return 0, err
	}

	request.Header = headers.Clone()
	if request.Header == nil {
		request.Header = make(http.Header)
	}

	request.Header.Set("Authorization", "Bearer "+key)
	request.Header.Set("Content-Type", "application/json")

	response, err := r.client.Do(request)
	if err != nil {
		return 0, errors.New("native API transport failed")
	}

	defer func() { _ = response.Body.Close() }()

	data, err := io.ReadAll(io.LimitReader(response.Body, MaxNativeBytes+1))
	if err != nil || len(data) > MaxNativeBytes {
		return response.StatusCode, errors.New("native API response could not be read")
	}

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return response.StatusCode, fmt.Errorf("native API returned HTTP %d", response.StatusCode)
	}

	if err := json.Unmarshal(data, result); err != nil {
		return response.StatusCode, errors.New("invalid native API response")
	}

	return response.StatusCode, nil
}

// Missing requires the service's explicit missing-thread verdict.
func (r *Remote) Missing(ctx context.Context, id string) (bool, error) {
	if err := ValidateThreadID(id); err != nil {
		return false, err
	}

	target := r.base.ResolveReference(&url.URL{Path: "/api/internal", RawQuery: "getThreadTail"})

	var result struct {
		OK    bool `json:"ok"`
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}

	_, err := r.send(ctx, target, map[string]any{"method": "getThreadTail", "params": map[string]any{fieldThread: id, "limit": 1}}, nil, &result)
	if err != nil {
		return false, err
	}

	if result.OK {
		return false, nil
	}

	if result.Error.Code == "thread-not-found" {
		return true, nil
	}

	return false, errors.New("native thread lookup refused")
}

// Import initializes an empty actor with saved native history, without starting a turn.
func (r *Remote) Import(ctx context.Context, id string, payload json.RawMessage, messageCount int) error {
	if err := ValidateThreadID(id); err != nil {
		return err
	}

	var credentials struct {
		ThreadID      string `json:"threadId"`
		WSToken       string `json:"wsToken"`
		ThreadVersion int    `json:"threadVersion"`
	}

	_, err := r.send(ctx, r.base.ResolveReference(&url.URL{Path: "/api/thread-actors"}), map[string]any{"threadId": id, "usesThreadActors": true}, nil, &credentials)
	if err != nil {
		return err
	}

	if credentials.ThreadID != id || credentials.WSToken == "" || credentials.ThreadVersion != 0 {
		return errors.New("native import destination is not empty")
	}

	target := *r.actor
	target.Path = strings.TrimRight(target.Path, "/") + "/gateway/threadActor/request/import"
	query := url.Values{"rvt-namespace": {"default"}, "rvt-method": {"get"}, "rvt-key": {id}, "rvt-skip-ready-wait": {"true"}}
	target.RawQuery = query.Encode()
	parameters, _ := json.Marshal(map[string]any{"wsToken": credentials.WSToken, "transport": "json-rpc"})
	headers := http.Header{"X-Rivet-Conn-Params": {string(parameters)}, "X-Rivet-Token": {r.routingKey}, "X-Rivet-Skip-Ready-Wait": {"1"}}

	var imported struct {
		OK        bool `json:"ok"`
		Messages  int  `json:"importedMessages"`
		Summaries int  `json:"convertedSummaryMessages"`
	}

	_, err = r.send(ctx, &target, payload, headers, &imported)
	if err != nil {
		return err
	}

	if !imported.OK || imported.Messages != messageCount || imported.Summaries != 0 {
		return errors.New("native import did not preserve the message list")
	}

	var marked struct {
		OK       bool   `json:"ok"`
		ThreadID string `json:"threadId"`
	}

	_, err = r.send(ctx, r.base.ResolveReference(&url.URL{Path: "/api/thread-actors/" + id}), map[string]any{"executorType": "local-client"}, nil, &marked)
	if err != nil {
		return err
	}

	if !marked.OK || marked.ThreadID != id {
		return errors.New("native import was not acknowledged")
	}

	return nil
}
