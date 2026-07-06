# CLAUDE.md

This file provides Claude Code-specific project guidance.

@AGENTS.md

## Claude Code Notes

- Project-level permissions and hooks live in `.claude/settings.json`.
- Custom Claude slash commands live in `.claude/commands/`.
- When updating integration coverage, use the real local `amp` CLI for the
  agent process. Local helper processes are only for deterministic MCP server or
  MCP proxy endpoints.
