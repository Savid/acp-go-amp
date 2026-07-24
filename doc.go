// Package ampacp exposes the Amp CLI as an Agent Client Protocol agent.
//
// Hosts embed the package with Serve over caller-owned JSON-RPC streams. The
// adapter mints its own session ids, lets the first prompt turn create the
// server-side Amp thread, continues threads with short-lived stream-json
// processes, validates embedded image prompts, maps image tool results to typed
// ACP content, mirrors frames and image artifacts into a SessionStore, and
// keeps Amp settings isolated per ACP session.
package ampacp
