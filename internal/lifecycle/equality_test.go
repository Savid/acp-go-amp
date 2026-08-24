package lifecycle

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestLifecycleValueEqualityComparesNumbersExactly pins the predicate the whole
// extension compares by: a number is the value it names, not the lexeme it was
// written in and not the double it would round to. The pairs beyond IEEE-754
// precision are the ones a float64 decode would call equal, and the astronomical
// exponents are the ones it could not decode at all.
func TestLifecycleValueEqualityComparesNumbersExactly(t *testing.T) {
	t.Parallel()

	for _, pair := range []struct{ left, right string }{
		{"1", "1.0"},
		{"1", "1e0"},
		{"1", "0.1E1"},
		{"100", "1e2"},
		{"1.50", "1.5"},
		{"0", "-0"},
		{"0", "0.000"},
		{"0", "-0.0e9"},
		{"-2.5", "-25e-1"},
		{"1e999999999", "10e999999998"},
		{"1234567890123456789", "1234567890123456789"},
	} {
		require.True(t, numbersEqual(pair.left, pair.right), "%s == %s", pair.left, pair.right)
		require.True(t, numbersEqual(pair.right, pair.left), "%s == %s", pair.right, pair.left)
	}

	for _, pair := range []struct{ left, right string }{
		{"1234567890123456788", "1234567890123456789"},
		{"1", "-1"},
		{"1", "10"},
		{"1e999999999", "1e999999998"},
		{"0", "1e-999999999"},
		{"1.5", "1.6"},
	} {
		require.False(t, numbersEqual(pair.left, pair.right), "%s != %s", pair.left, pair.right)
		require.False(t, numbersEqual(pair.right, pair.left), "%s != %s", pair.right, pair.left)
	}
}

// TestLifecycleValueEqualityIgnoresSpellingAndOrder pins that key order and
// insignificant whitespace are not differences, and that structure still is.
func TestLifecycleValueEqualityIgnoresSpellingAndOrder(t *testing.T) {
	t.Parallel()

	for _, pair := range []struct{ left, right string }{
		{`{"a":1,"b":[1,2]}`, `{ "b" : [ 1 , 2.0 ] , "a" : 1.0 }`},
		{`{"a":null}`, `{"a":null}`},
		{`["x",true]`, `["x", true]`},
	} {
		require.True(t, rawValuesEqual(json.RawMessage(pair.left), json.RawMessage(pair.right)))
	}

	for _, pair := range []struct{ left, right string }{
		{`{"a":1}`, `{"a":1,"b":2}`},
		{`{"a":1}`, `{"b":1}`},
		{`{"a":1}`, `[1]`},
		{`[1,2]`, `[1,2,3]`},
		{`[1,2]`, `[1,3]`},
		{`[1]`, `{"a":1}`},
		{`1`, `"1"`},
		{`{"a":1}`, `{"a":"1"}`},
	} {
		require.False(t, rawValuesEqual(json.RawMessage(pair.left), json.RawMessage(pair.right)))
		require.False(t, rawValuesEqual(json.RawMessage(pair.right), json.RawMessage(pair.left)))
	}
}

// TestRawValueEqualityHasNothingToCompareAgainstAnAbsentMember pins that a record
// that never held the member is not equal to one stating it: absence is not a
// value that anything matches.
func TestRawValueEqualityHasNothingToCompareAgainstAnAbsentMember(t *testing.T) {
	t.Parallel()

	require.False(t, rawValuesEqual(nil, json.RawMessage(`{"stage":"final"}`)))
	require.False(t, rawValuesEqual(json.RawMessage(``), json.RawMessage(`{}`)))
}

// TestDecodeValueRefusesAFrameThatIsNotOneValue pins that a payload is one JSON
// document. Content after the value is not a second value to compare; it is a
// frame that was never one.
func TestDecodeValueRefusesAFrameThatIsNotOneValue(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{`{"a":1} {"a":2}`, `{`, ``, `1 2`} {
		_, decodable := decodeValue([]byte(raw))
		require.False(t, decodable, raw)
	}

	value, decodable := decodeValue([]byte(` {"a": 1} `))
	require.True(t, decodable)
	require.Equal(t, map[string]any{"a": json.Number("1")}, value)
}
