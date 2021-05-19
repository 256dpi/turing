package tape

import (
	"fmt"

	"github.com/256dpi/fpack"
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
func (s *State) Encode(borrow bool) ([]byte, *fpack.Ref, error) {
	return fpack.Encode(borrow, func(enc *fpack.Encoder) error {
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
	return fpack.Decode(bytes, func(dec *fpack.Decoder) error {
		// check version
		if dec.Uint8() != 1 {
			return fmt.Errorf("turing: state decode: invalid version")
		}

		// decode body
		s.Index = dec.Uint64()
		s.Batch = dec.Uint64()
		s.Last = dec.Uint16()

		return nil
	})
}
