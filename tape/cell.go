package tape

import (
	"fmt"

	"github.com/256dpi/fpack"
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
func (v *Cell) Encode(borrow bool) ([]byte, fpack.Ref, error) {
	// check type
	if !v.Type.Valid() {
		return nil, fpack.Ref{}, fmt.Errorf("turing: encode cell: invalid type: %c", v.Type)
	}

	return fpack.Encode(borrow, func(enc *fpack.Encoder) error {
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
	return fpack.Decode(bytes, func(dec *fpack.Decoder) error {
		// check version
		if dec.Uint8() != 1 {
			return fmt.Errorf("turing: decode cell: invalid version")
		}

		// decode type
		typ := dec.Uint8()
		v.Type = CellType(typ)
		if !v.Type.Valid() {
			return fmt.Errorf("turing: decode cell: invalid type: %d", v.Type)
		}

		// decode value
		v.Value = dec.Tail(clone)

		return nil
	})
}
