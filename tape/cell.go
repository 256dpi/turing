package tape

import (
	"fmt"

	"github.com/256dpi/turing/coding"
)

// CellType represents the type of cell.
type CellType uint8

const (
	// RawCell holds a user supplied value.
	RawCell CellType = 1

	// StackCell holds a list of operator operands.
	StackCell CellType = 2
)

// Valid returns whether the cell type is valid.
func (k CellType) Valid() bool {
	switch k {
	case RawCell, StackCell:
		return true
	default:
		return false
	}
}

// Cell represents a value stored at a key. It is either a raw cell, that holds
// a user supplied value or a stack cell that holds a list of operator operands.
type Cell struct {
	Type  CellType
	Value []byte
}

// Encode will encode the cell.
func (v *Cell) Encode(borrow bool) ([]byte, *coding.Ref, error) {
	// check type
	if !v.Type.Valid() {
		return nil, nil, fmt.Errorf("turing: encode cell: invalid type: %c", v.Type)
	}

	return coding.Encode(borrow, func(enc *coding.Encoder) error {
		// write version
		enc.Uint8(1)

		// write type
		enc.Uint8(uint8(v.Type))

		// write value
		enc.Tail(v.Value)

		return nil
	})
}

// Decode will decode the cell.
func (v *Cell) Decode(bytes []byte, clone bool) error {
	return coding.Decode(bytes, func(dec *coding.Decoder) error {
		// decode version
		var version uint8
		dec.Uint8(&version)
		if version != 1 {
			return fmt.Errorf("turing: decode cell: invalid version")
		}

		// decode type
		var typ uint8
		dec.Uint8(&typ)
		v.Type = CellType(typ)
		if !v.Type.Valid() {
			return fmt.Errorf("turing: decode cell: invalid type: %d", v.Type)
		}

		// decode value
		dec.Tail(&v.Value, clone)

		return nil
	})
}
