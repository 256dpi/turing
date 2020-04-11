package stdset

import (
	"fmt"

	"github.com/256dpi/turing"
	"github.com/256dpi/turing/pkg/coding"
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
func (s *Set) Execute(txn *turing.Transaction) error {
	// set pair
	err := txn.Set(s.Key, s.Value)
	if err != nil {
		return err
	}

	return nil
}

// Encode implements the turing.Instruction interface.
func (s *Set) Encode() ([]byte, turing.Ref, error) {
	return coding.Encode(true, func(enc *coding.Encoder) error {
		// encode version
		enc.VarUint(1)

		// encode body
		enc.Bytes(s.Key)
		enc.Tail(s.Value)

		return nil
	})
}

// Decode implements the turing.Instruction interface.
func (s *Set) Decode(bytes []byte) error {
	return coding.Decode(bytes, func(dec *coding.Decoder) error {
		// decode version
		var version uint64
		dec.VarUint(&version)
		if version != 1 {
			return fmt.Errorf("stdset: decode set: invalid version")
		}

		// decode body
		dec.Bytes(&s.Key, true)
		dec.Tail(&s.Value, true)

		return nil
	})
}
