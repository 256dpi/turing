package turing

import (
	"fmt"

	"github.com/256dpi/turing/pkg/coding"
)

// Kind represents the kind of value stored at a key.
type Kind byte

const (
	// FullValue is a full value.
	FullValue Kind = 1

	// StackValue is a stack of merge values.
	StackValue Kind = 2
)

// Valid returns whether the kind is valid.
func (k Kind) Valid() bool {
	switch k {
	case FullValue, StackValue:
		return true
	default:
		return false
	}
}

// Value represents a decoded value
type Value struct {
	// The kind of the value.
	Kind Kind

	// The value.
	Value []byte
}

// Encode will encode the value.
func (v *Value) Encode(borrow bool) ([]byte, Ref, error) {
	// check kind
	if !v.Kind.Valid() {
		return nil, NoopRef, fmt.Errorf("turing: encode value: invalid kind: %c", v.Kind)
	}

	return coding.Encode(borrow, func(enc *coding.Encoder) error {
		// write version
		enc.Uint(1)

		// write kind
		enc.Uint(uint64(v.Kind))

		// write value
		enc.Tail(v.Value)

		return nil
	})
}

// Decode will decode the value.
func (v *Value) Decode(bytes []byte, clone bool) error {
	return coding.Decode(bytes, func(dec *coding.Decoder) error {
		// decode version
		var version uint64
		dec.Uint(&version)
		if version != 1 {
			return fmt.Errorf("turing: decode value: invalid version")
		}

		// read kind
		var kind uint64
		dec.Uint(&kind)
		v.Kind = Kind(kind)
		if !v.Kind.Valid() {
			return fmt.Errorf("turing: decode value: invalid kind: %d", v.Kind)
		}

		// decode full value
		dec.Tail(&v.Value, clone)

		return nil
	})
}

// ComputeValue will compute the final value from a stack value using the zero
// value from the first operand operator.
func ComputeValue(value Value, registry *registry) (Value, Ref, error) {
	// check kind
	if value.Kind != StackValue {
		return Value{}, nil, fmt.Errorf("turing: compute value: expected stack value, got: %d", value.Kind)
	}

	// decode stack
	var stack Stack
	err := stack.Decode(value.Value, false)
	if err != nil {
		return Value{}, nil, err
	}

	// get first operator
	operator, ok := registry.ops[stack.Operands[0].Name]
	if !ok {
		return Value{}, nil, fmt.Errorf("turing: compute value: missing operator: %s", stack.Operands[0].Name)
	}

	// prepare zero value
	zero := Value{
		Kind:  FullValue,
		Value: operator.Zero,
	}

	// prepare list
	list := [2]Value{zero, value}

	// merge values
	computer := newComputer(registry)
	value, ref, err := computer.eval(list[:])
	if err != nil {
		return Value{}, NoopRef, err
	}

	return value, ref, nil
}
