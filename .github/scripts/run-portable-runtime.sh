#!/bin/sh
# Execute the portable ordinary-lifecycle suite on a host that selects it.
#
# internal/amp/process_unsupported.go is selected only on !unix hosts, so a
# Unix-side `GOOS=windows go test -c` proves only that the file parses. That is
# exactly how an interrupt operation Go never implements on Windows survived
# every gate: it compiled, and nothing ever ran it. This lane closes that gap by
# running the class on a real Windows host.
#
# The selector must discover tests and every discovered test must pass. A skip
# is a failure here for the same reason it is in the trusted-supervisor lane: a
# lane that silently runs nothing is indistinguishable from a lane that passes.
set -eu

selector='^TestPortable'
package=./internal/amp

selected=$(go list -f '{{range .GoFiles}}{{println .}}{{end}}' "$package" | grep -Ec '^process_unsupported\.go$' || true)
[ "$selected" -eq 1 ] || {
	echo "the portable runtime lane needs a !unix host that selects process_unsupported.go, not $(go env GOOS)" >&2
	exit 1
}

listing=$(mktemp)
log=$(mktemp)
rc=$(mktemp)
cleanup() { rm -f "$listing" "$log" "$rc"; }
trap cleanup EXIT HUP INT TERM

go test -list "$selector" "$package" >"$listing"

expected=$(grep -Ec '^TestPortable' "$listing" || true)
[ "$expected" -gt 0 ] || {
	echo 'portable runtime selector discovered no tests' >&2
	exit 1
}

{ go test -race -count=1 -json -timeout="${GO_TEST_TIMEOUT:-40m}" -run "$selector" "$package"; echo $? >"$rc"; } | tee "$log"

status=$(cat "$rc")
passed=$(grep -Ec '"Action":"pass","Package":"[^"]+","Test":"TestPortable[^/"]*"' "$log" || true)
skipped=$(grep -Ec '"Action":"skip","Package":"[^"]+","Test":"TestPortable[^"]*"' "$log" || true)

[ "$status" -eq 0 ] || exit "$status"
[ "$passed" -eq "$expected" ] || {
	echo "portable runtime pass count $passed, want $expected" >&2
	exit 1
}
[ "$skipped" -eq 0 ] || {
	echo 'the portable runtime lane skipped a case' >&2
	exit 1
}
