package turing

import (
	"fmt"

	"github.com/256dpi/turing/pkg/coding"
)

// Kind represents the kind of value stored at a key.
type Kind uint8

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
		enc.Uint8(1)

		// write kind
		enc.Uint8(uint8(v.Kind))

		// write value
		enc.Tail(v.Value)

		return nil
	})
}

// Decode will decode the value.
func (v *Value) Decode(bytes []byte, clone bool) error {
	return coding.Decode(bytes, func(dec *coding.Decoder) error {
		// decode version
		var version uint8
		dec.Uint8(&version)
		if version != 1 {
			return fmt.Errorf("turing: decode value: invalid version")
		}

		// read kind
		var kind uint8
		dec.Uint8(&kind)
		v.Kind = Kind(kind)
		if !v.Kind.Valid() {
			return fmt.Errorf("turing: decode value: invalid kind: %d", v.Kind)
		}

		// decode full value
		dec.Tail(&v.Value, clone)

		return nil
	})
}
