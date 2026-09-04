package ampacp

import (
	"context"
	"sync"

	"github.com/coder/acp-go-sdk"
)

type callbackSessionStore struct {
	store SessionStore
}

func (s callbackSessionStore) Append(ctx context.Context, key SessionKey, entries []SessionStoreEntry) error {
	leave := enterExternalCallback(ctx)
	defer leave()

	return s.store.Append(ctx, key, entries)
}

func (s callbackSessionStore) Load(ctx context.Context, key SessionKey) ([]SessionStoreEntry, error) {
	leave := enterExternalCallback(ctx)
	defer leave()

	return s.store.Load(ctx, key)
}

func (s callbackSessionStore) Replace(ctx context.Context, main SessionKey, replacements []SessionStoreReplacement) error {
	leave := enterExternalCallback(ctx)
	defer leave()

	return s.store.Replace(ctx, main, replacements)
}

func (s callbackSessionStore) Delete(ctx context.Context, key SessionKey) error {
	leave := enterExternalCallback(ctx)
	defer leave()

	return s.store.Delete(ctx, key)
}

func (s callbackSessionStore) ListSessions(ctx context.Context) ([]SessionSummary, error) {
	leave := enterExternalCallback(ctx)
	defer leave()

	return s.store.ListSessions(ctx)
}

func (s callbackSessionStore) ListSubkeys(ctx context.Context, key SessionKey) ([]string, error) {
	leave := enterExternalCallback(ctx)
	defer leave()

	return s.store.ListSubkeys(ctx, key)
}

// callbackProvenance is an in-process ownership token. It never crosses the
// wire or enters a diagnostic: its only purpose is to keep an external callback
// from synchronously waiting on the exact generation that invoked it.
type callbackProvenance struct {
	agent *Agent
	owner any
}

type callbackProvenanceKey struct{}

type callbackProvenanceChain struct {
	value callbackProvenance
	prior *callbackProvenanceChain
}

type callbackSessionScopeKey struct{}

func withCallbackSessionScope(ctx context.Context, agent *Agent, id acp.SessionId) context.Context {
	return context.WithValue(ctx, callbackSessionScopeKey{}, callbackSessionScope{agent: agent, id: id})
}

type callbackSessionScope struct {
	agent *Agent
	id    acp.SessionId
}

func withCallbackProvenance(ctx context.Context, agent *Agent, owner any) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}

	prior, _ := ctx.Value(callbackProvenanceKey{}).(*callbackProvenanceChain)

	return context.WithValue(ctx, callbackProvenanceKey{}, &callbackProvenanceChain{
		value: callbackProvenance{agent: agent, owner: owner},
		prior: prior,
	})
}

func contextOwnsCallbackGeneration(ctx context.Context, agent *Agent, owner any) bool {
	if ctx == nil {
		return false
	}

	chain, _ := ctx.Value(callbackProvenanceKey{}).(*callbackProvenanceChain)
	for ; chain != nil; chain = chain.prior {
		if chain.value.agent == agent && chain.value.owner == owner {
			return true
		}
	}

	return false
}

func contextOwnsAgentCallback(ctx context.Context, agent *Agent) bool {
	if ctx == nil {
		return false
	}

	chain, _ := ctx.Value(callbackProvenanceKey{}).(*callbackProvenanceChain)
	for ; chain != nil; chain = chain.prior {
		if chain.value.agent == agent {
			return true
		}
	}

	return false
}

func callbackAgent(ctx context.Context) *Agent {
	if ctx == nil {
		return nil
	}

	chain, _ := ctx.Value(callbackProvenanceKey{}).(*callbackProvenanceChain)
	for ; chain != nil; chain = chain.prior {
		if chain.value.agent != nil {
			return chain.value.agent
		}
	}

	return nil
}

func withExactCallbackGeneration(ctx context.Context, kind string) context.Context {
	agent := callbackAgent(ctx)
	if agent == nil {
		return ctx
	}

	owner := &agentCallbackGeneration{
		generation: agent.nextCallbackGeneration.Add(1),
		kind:       kind,
	}

	return withCallbackProvenance(ctx, agent, owner)
}

// agentCallbackAuthority is the explicit admission token held for the complete
// dynamic extent of one external callback. It is agent-owned rather than
// goroutine-owned: callback code may discard its context or hand work to another
// goroutine, but no reentrant agent call can be admitted until the token leaves.
type agentCallbackAuthority struct {
	agent      *Agent
	generation uint64
	sessionID  acp.SessionId
	scoped     bool
}

func enterExternalCallback(ctx context.Context) func() {
	agent := callbackAgent(ctx)
	if agent == nil {
		return func() {}
	}

	agent.callbackMu.Lock()
	agent.nextCallback++

	authority := &agentCallbackAuthority{agent: agent, generation: agent.nextCallback}
	if scope, ok := ctx.Value(callbackSessionScopeKey{}).(callbackSessionScope); ok && scope.agent == agent {
		authority.sessionID = scope.id
		authority.scoped = true
	}

	agent.callbacks[authority.generation] = authority
	agent.callbackMu.Unlock()

	var once sync.Once

	return func() {
		once.Do(func() {
			agent.callbackMu.Lock()
			if agent.callbacks[authority.generation] == authority {
				delete(agent.callbacks, authority.generation)
			}
			agent.callbackMu.Unlock()
		})
	}
}

func (a *Agent) hasActiveCallbackForSession(id acp.SessionId) bool {
	if a == nil {
		return false
	}

	a.callbackMu.Lock()
	defer a.callbackMu.Unlock()

	for _, authority := range a.callbacks {
		if authority.scoped && authority.sessionID == id {
			return true
		}
	}

	return false
}

func invokeExternalResult[T any](ctx context.Context, callback func() T) (result T) {
	leave := enterExternalCallback(ctx)
	defer leave()

	return callback()
}

func invokeOwned(callback func()) {
	callback()
}

func invokeOwnedPair[A, B any](callback func() (A, B)) (first A, second B) {
	return callback()
}

func (a *Agent) hasActiveCallbackAuthority() bool {
	if a == nil {
		return false
	}

	a.callbackMu.Lock()
	active := len(a.callbacks) != 0
	a.callbackMu.Unlock()

	return active
}
