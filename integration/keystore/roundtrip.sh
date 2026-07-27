#!/bin/sh
# Stores a canary and reads it back through the Secret Service. A service that
# claimed its bus name without creating a collection fails here.
set -eu

FIXTURE_DIR=/run/acp-go-amp-keystore
CANARY=readiness-canary

# shellcheck source=/dev/null
. "$FIXTURE_DIR/env"
export DBUS_SESSION_BUS_ADDRESS

printf '%s' "$CANARY" | secret-tool store --label=readiness service acp-go-amp-readiness username readiness
test "$(secret-tool lookup service acp-go-amp-readiness username readiness)" = "$CANARY"
