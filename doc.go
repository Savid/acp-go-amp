// Package ampacp exposes the Amp CLI as an Agent Client Protocol agent.
//
// Hosts embed the package with Serve over caller-owned JSON-RPC streams. The
// adapter creates Amp threads, continues them with short-lived stream-json
// processes, mirrors stream frames into a SessionStore, and keeps Amp settings
// isolated per ACP session.
package ampacp
