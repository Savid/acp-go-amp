#!/bin/sh
# Execute the portable ordinary-lifecycle suite on a host that selects it.
#
# The selector must discover tests and every discovered test must pass. A skip
# is a failure because a lane that silently runs nothing is indistinguishable
# from a lane that passes.
#
# Windows environment key identity, executable search rules, and the environment
# block a child actually receives are real-host facts that a cross-compile
# cannot establish.
set -eu

selector='^TestPortable'
packages='.'

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
