// Package ampacp exposes Amp as an Agent Client Protocol agent.
//
// [Serve] translates ACP requests into native Amp commands. Each prompt owns
// one stream-json child process with the inherited environment and session
// overlays and a temporary lifecycle plugin. Native end receipts and quiet
// thread state gate export verification and durable terminal publication.
// ACP session identity stays stable while the native binding is stored beside history.
//
// [WithSessionStore] supplies durable native exports and configuration records.
// Load reconciles the remote thread and replays history; resume omits replay.
// Confirmed missing threads recover through native import into a new thread;
// verified history and the replacement binding commit together. Close preserves native state.
//
// Hosts supply telemetry through [WithTracerProvider] and [WithMeterProvider];
// the package does not configure global providers.
package ampacp
