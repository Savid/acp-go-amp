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
#
# The adapter surface above the process backend carries its own portable class:
# the Windows environment key identity, executable search rules, and the
# environment block a child actually receives are real-host facts that a
# cross-compile cannot establish. The root package is therefore selected by the
# same name and held to the same discovery/pass/skip rules.
set -eu

selector='^TestPortable'
packages='./internal/amp .'

selected=$(go list -f '{{range .GoFiles}}{{println .}}{{end}}' ./internal/amp | grep -Ec '^process_unsupported\.go$' || true)
[ "$selected" -eq 1 ] || {
	echo "the portable runtime lane needs a !unix host that selects process_unsupported.go, not $(go env GOOS)" >&2
	exit 1
}

log=$(mktemp)
cleanup() { rm -f "$log"; }
trap cleanup EXIT HUP INT TERM

expected=0
for package in $packages; do
	discovered=$(go test -list "$selector" "$package" | grep -Ec '^TestPortable' || true)
	[ "$discovered" -gt 0 ] || {
		echo "portable runtime selector discovered no tests in $package" >&2
		exit 1
	}
	expected=$((expected + discovered))
done

if go test -race -count=1 -json -timeout="${GO_TEST_TIMEOUT:-40m}" -run "$selector" $packages >"$log"; then
	status=0
else
	status=$?
fi
cat "$log"
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
