package tape

import (
	"fmt"

	"github.com/256dpi/turing/coding"
)

// State represents the current state of a database.
type State struct {
	// The index of the last fully applied batch.
	Index uint64

	// The index of the batch currently being processed.
	Batch uint64

	// The sequence of the last fully applied instruction.
	Last uint16
}

// Encode will encode the state.
func (s *State) Encode(borrow bool) ([]byte, *coding.Ref, error) {
	return coding.Encode(borrow, func(enc *coding.Encoder) error {
		// encode version
		enc.Uint8(1)

		// encode body
		enc.Uint64(s.Index)
		enc.Uint64(s.Batch)
		enc.Uint16(s.Last)

		return nil
	})
}

// Decode will decode the state.
func (s *State) Decode(bytes []byte) error {
	return coding.Decode(bytes, func(dec *coding.Decoder) error {
		// decode version
		var version uint8
		dec.Uint8(&version)
		if version != 1 {
			return fmt.Errorf("turing: state decode: invalid version")
		}

		// decode body
		dec.Uint64(&s.Index)
		dec.Uint64(&s.Batch)
		dec.Uint16(&s.Last)

		return nil
	})
}
