package stdset

import (
	"fmt"

	"github.com/256dpi/turing"
	"github.com/256dpi/turing/pkg/coding"
)

// Get will get a value.
type Get struct {
	Key    []byte
	Value  []byte
	Exists bool
}

var getDesc = &turing.Description{
	Name: "turing/Get",
}

// Describe implements the turing.Instruction interface.
func (g *Get) Describe() *turing.Description {
	return getDesc
}

// Effect implements the turing.Instruction interface.
func (g *Get) Effect() int {
	return 0
}

// Execute implements the turing.Instruction interface.
func (g *Get) Execute(txn *turing.Transaction) error {
	// get value
	err := txn.Use(g.Key, func(value []byte) error {
		g.Value = turing.Clone(value)
		g.Exists = true
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

// Encode implements the turing.Instruction interface.
func (g *Get) Encode() ([]byte, turing.Ref, error) {
	return coding.Encode(true, func(enc *coding.Encoder) error {
		// encode version
		enc.Uint(1)

		// encode body
		enc.Bytes(g.Key)
		enc.Bool(g.Exists)
		enc.Tail(g.Value)

		return nil
	})
}

// Decode implements the turing.Instruction interface.
func (g *Get) Decode(bytes []byte) error {
	return coding.Decode(bytes, func(dec *coding.Decoder) error {
		// decode version
		var version uint64
		dec.Uint(&version)
		if version != 1 {
			return fmt.Errorf("stdset: decode get: invalid version")
		}

		// decode body
		dec.Bytes(&g.Key, true)
		dec.Bool(&g.Exists)
		dec.Tail(&g.Value, true)

		return nil
	})
}
