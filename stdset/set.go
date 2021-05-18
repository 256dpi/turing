package stdset

import (
	"fmt"

	"github.com/256dpi/fpack"
	"github.com/256dpi/turing"
)

// Set will set a value.
type Set struct {
	Key   []byte
	Value []byte
}

var setDesc = &turing.Description{
	Name: "turing/Set",
}

// Describe implements the turing.Instruction interface.
func (s *Set) Describe() *turing.Description {
	return setDesc
}

// Effect implements the turing.Instruction interface.
func (s *Set) Effect() int {
	return 1
}

// Execute implements the turing.Instruction interface.
func (s *Set) Execute(mem turing.Memory, _ turing.Cache) error {
	// set pair
	err := mem.Set(s.Key, s.Value)
	if err != nil {
		return err
	}

	return nil
}

// Encode implements the turing.Instruction interface.
func (s *Set) Encode() ([]byte, turing.Ref, error) {
	return fpack.Encode(true, func(enc *fpack.Encoder) error {
		// encode version
		enc.Uint8(1)

		// encode body
		enc.VarBytes(s.Key)
		enc.Tail(s.Value)

		return nil
	})
}

// Decode implements the turing.Instruction interface.
func (s *Set) Decode(bytes []byte) error {
	return fpack.Decode(bytes, func(dec *fpack.Decoder) error {
		// decode version
		var version uint8
		dec.Uint8(&version)
		if version != 1 {
			return fmt.Errorf("stdset: decode set: invalid version")
		}

		// decode body
		dec.VarBytes(&s.Key, true)
		dec.Tail(&s.Value, true)

		return nil
	})
}
