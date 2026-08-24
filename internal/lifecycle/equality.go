package lifecycle

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"strings"
)

// decodeValue reads one JSON document as a comparable value. Numbers are retained
// as their lexemes rather than decoded to binary floating point: lifecycle value
// equality compares exact mathematical values, and a float64 decode loses the
// difference between two integers past its precision and cannot hold a literal
// whose expansion no machine will. Trailing content after the document is not a
// second value to compare but a frame that was never one value.
func decodeValue(raw []byte) (any, bool) {
	reader := json.NewDecoder(bytes.NewReader(raw))
	reader.UseNumber()

	var value any

	if err := reader.Decode(&value); err != nil {
		return nil, false
	}

	if _, err := reader.Token(); !errors.Is(err, io.EOF) {
		return nil, false
	}

	return value, true
}

// valueOf decodes one document the decoder already proved. Bytes reach a record
// only after they were read as a valid value, so an undecodable one here would be
// unequal to everything, which is what the comparison would report anyway.
func valueOf(raw json.RawMessage) any {
	value, _ := decodeValue(raw)

	return value
}

// rawValuesEqual compares an encoded member a record holds against one an event
// carries. A record that never held the member has no value equal to the one
// being stated.
func rawValuesEqual(recorded, carried json.RawMessage) bool {
	if len(recorded) == 0 {
		return false
	}

	return valuesEqual(valueOf(recorded), valueOf(carried))
}

// valuesEqual is lifecycle value equality: two decoded values are equal when they
// are deeply equal, key order and insignificant whitespace are not differences,
// and numbers compare as exact mathematical values. It is deliberately stricter
// than RFC 8785, which collapses numbers to doubles.
func valuesEqual(left, right any) bool {
	switch typed := left.(type) {
	case map[string]any:
		return objectsEqual(typed, right)
	case []any:
		return arraysEqual(typed, right)
	case json.Number:
		other, ok := right.(json.Number)

		return ok && numbersEqual(string(typed), string(other))
	default:
		return left == right
	}
}

func objectsEqual(left map[string]any, right any) bool {
	other, ok := right.(map[string]any)
	if !ok || len(left) != len(other) {
		return false
	}

	for key, value := range left {
		counterpart, present := other[key]
		if !present || !valuesEqual(value, counterpart) {
			return false
		}
	}

	return true
}

func arraysEqual(left []any, right any) bool {
	other, ok := right.([]any)
	if !ok || len(left) != len(other) {
		return false
	}

	for index := range left {
		if !valuesEqual(left[index], other[index]) {
			return false
		}
	}

	return true
}

// numberForm is a JSON number's normalized decimal form: a sign, a coefficient
// with leading and trailing zeros stripped, and the power of ten that coefficient
// is scaled by. Every zero has the same form, so -0 and 0 are one value.
type numberForm struct {
	negative    bool
	coefficient string
	exponent    *big.Int
}

// numbersEqual decides two number lexemes without ever materializing what they
// name. Normalizing reads the sign, the digits, and the exponent straight off the
// lexeme, so a twelve-byte literal with an astronomical exponent costs what its
// twelve bytes cost.
func numbersEqual(left, right string) bool {
	first, second := normalizeNumber(left), normalizeNumber(right)

	return first.negative == second.negative &&
		first.coefficient == second.coefficient &&
		first.exponent.Cmp(second.exponent) == 0
}

// normalizeNumber reads the form of one lexeme the JSON decoder already validated.
func normalizeNumber(lexeme string) numberForm {
	negative := strings.HasPrefix(lexeme, "-")
	if negative {
		lexeme = lexeme[1:]
	}

	mantissa, exponentText, scaled := cutExponent(lexeme)
	integer, fraction, _ := strings.Cut(mantissa, ".")
	exponent := new(big.Int)

	if scaled {
		exponent.SetString(exponentText, 10)
	}

	exponent.Sub(exponent, big.NewInt(int64(len(fraction))))

	digits := strings.TrimLeft(integer+fraction, "0")
	coefficient := strings.TrimRight(digits, "0")

	if coefficient == "" {
		return numberForm{exponent: new(big.Int)}
	}

	exponent.Add(exponent, big.NewInt(int64(len(digits)-len(coefficient))))

	return numberForm{negative: negative, coefficient: coefficient, exponent: exponent}
}

// cutExponent splits a lexeme at whichever exponent marker JSON permits.
func cutExponent(lexeme string) (mantissa, exponent string, scaled bool) {
	if mantissa, exponent, scaled = strings.Cut(lexeme, "e"); scaled {
		return mantissa, exponent, true
	}

	return strings.Cut(lexeme, "E")
}
