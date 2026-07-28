package ampacp

import (
	"context"
	"sync"

	"github.com/coder/acp-go-sdk"
)

// sessionClosed reports whether the session has already had its flows swept.
// The mark and the sweep set are taken in one critical section, so an id that
// reads closed here is one whose cleanup has already been decided.
func (p *providerAuth) sessionClosed(sessionID acp.SessionId) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	_, closed := p.closedSessions[sessionID]

	return closed
}

// publishFlow makes the flow addressable and is the authoritative admission
// check: a session whose close already ran refuses publication, so no flow can
// escape the cleanup set that close took. The check belongs here rather than at
// leg entry because a leg that passed entry is only a request, while a
// published flow is a login child writing into a home the close is about to
// reclaim.
//
// Close deliberately does not wait for in-flight legs. Draining would block
// session/close for the length of an unbounded native call, and refusing
// publication holds the same invariant without it.
func (p *providerAuth) publishFlow(key authFlowKey, flow *authFlow) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, closed := p.closedSessions[flow.sessionID]; closed {
		return false
	}

	p.flows[key] = flow
	p.byID[flow.id] = flow
	p.retained[key] = flow

	return true
}

// admitKey gates every authorize for one (session, provider) pair against every
// other. It is held from before the replay check to after the mint has settled,
// which is what makes the sequence replay → retired → supersede → record →
// publish → mint atomic against a second authorize and so what makes the
// idempotency key mean anything at all.
func (p *providerAuth) admitKey(ctx context.Context, key authFlowKey) (func(), bool) {
	return authAcquireGate(ctx, &p.mu, p.admissions, key)
}

// admitSlot gates every rewrite of one provider's recorded binding against
// every other. Amp keeps the account under a single provider id, so that id is
// the identity of the slot a mutation rewrites. A disconnect holds it across
// its whole read → compare → bump → record sequence and a completion across its
// lineage check, its submit, and its confirmation, so the two can never
// interleave into a generation that goes backwards.
func (p *providerAuth) admitSlot(ctx context.Context, providerID string) (func(), bool) {
	return authAcquireGate(ctx, &p.mu, p.slots, providerID)
}

// authGate is one single-holder gate together with the number of legs that hold
// it or are waiting for it.
type authGate struct {
	ch      chan struct{}
	waiters int
}

// authAcquireGate takes the gate named by key, creating it on first use, and
// returns the release its holder defers. A caller that stopped waiting takes
// nothing, so it has nothing to release.
//
// The gate's lifetime is the refcount and nothing else: authDropGate is the only
// thing that may ever remove an entry, and it does so only when the last leg has
// left. Deleting a gate any other way — sweeping a closed session's entries, for
// instance — replaces a gate a leg still holds with a fresh one that the next
// leg walks straight through, so the serialization silently stops happening
// while every map operation still looks correct. The refcount also bounds both
// maps: the key gate is keyed per session and would otherwise accumulate one
// dead entry per session an agent outlives.
func authAcquireGate[K comparable](ctx context.Context, mu *sync.Mutex, gates map[K]*authGate, key K) (func(), bool) {
	mu.Lock()

	gate, ok := gates[key]
	if !ok {
		gate = &authGate{ch: make(chan struct{}, 1)}
		gates[key] = gate
	}

	gate.waiters++
	mu.Unlock()

	select {
	case gate.ch <- struct{}{}:
		return func() {
			<-gate.ch
			authDropGate(mu, gates, key, gate)
		}, true
	case <-ctx.Done():
		authDropGate(mu, gates, key, gate)

		return nil, false
	}
}

// authDropGate accounts for one leg leaving and removes the gate once it was the
// last. A gate nobody holds or wants orders nothing, so the next leg to ask for
// that key can be handed a new one.
func authDropGate[K comparable](mu *sync.Mutex, gates map[K]*authGate, key K, gate *authGate) {
	mu.Lock()
	defer mu.Unlock()

	gate.waiters--
	if gate.waiters == 0 {
		delete(gates, key)
	}
}

// reopenSession clears the mark a session id carries once that id is live
// again. A close mark is a statement about a session's lifetime, not about the
// id, and an id that names a rebuilt session names a lifetime whose flows no
// close has swept.
func (p *providerAuth) reopenSession(sessionID acp.SessionId) {
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.closedSessions, sessionID)
}

// claimFlow admits the one leg that may drive this flow's login child and holds
// the claim for the whole attempt. The terminal check and the claim are one
// critical section because a native call sits between them: two callbacks that
// both pass a check neither has set yet both write to and close one child's
// stdin, losing a valid paste or reporting a refusal of a login that in fact
// succeeded — with no data race for the detector to find, since every
// individual field access is itself locked.
func (p *providerAuth) claimFlow(flow *authFlow) error {
	if p.tryClaimFlow(flow) {
		return nil
	}

	return authFailed(authCauseFlowState, flow.providerID, flow.method.ID, flow.id)
}

// tryClaimFlow is the claim a leg takes only if it is free. The status poll uses
// it: a flow another leg is already driving is one this poll has nothing to add
// to, and waiting would put a consumer's cadence behind a native call.
func (p *providerAuth) tryClaimFlow(flow *authFlow) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if authTerminal(flow.state) || flow.claimed {
		return false
	}

	flow.claimed = true

	return true
}

func (p *providerAuth) releaseFlow(flow *authFlow) {
	p.mu.Lock()
	defer p.mu.Unlock()

	flow.claimed = false
}

// requestRetired reports whether the key names a request a later authorize
// already replaced. Only the newest record is answerable verbatim, so an older
// key is unanswerable — and minting in its place would destroy the live flow it
// never named, which is the one thing an idempotency key exists to prevent.
func (p *providerAuth) requestRetired(key authFlowKey, requestID string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	_, retired := p.retired[key][requestID]

	return retired
}

// retire records a request id the broker can no longer answer. The caller holds
// the mutex.
func (p *providerAuth) retire(key authFlowKey, requestID string) {
	keys, ok := p.retired[key]
	if !ok {
		keys = make(map[string]struct{})
		p.retired[key] = keys
	}

	keys[requestID] = struct{}{}
}

// lineageCurrent reports whether the durable record still names exactly the
// binding this flow was minted against. A completion asks before it writes
// anything, native or recorded: a disconnect that already bumped the generation
// released the slot, and confirming afterwards would leave the host reading
// `removed` while the credential this flow installed is resident, live, and
// invisible on every surface it has. A record that cannot be read is no proof
// the binding survived, so it is not treated as one.
func (p *providerAuth) lineageCurrent(flow *authFlow) bool {
	record, ok, err := p.ledger.read(flow.providerID, flow.connectionID)

	return err == nil && ok &&
		record.ConnectionID == flow.connectionID &&
		record.Revision == flow.revision &&
		record.BindingGeneration == flow.bindingGeneration
}
