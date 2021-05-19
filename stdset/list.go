package stdset

import (
	"fmt"

	"github.com/256dpi/fpack"
	"github.com/256dpi/turing"
)

// List will list all keys.
type List struct {
	Prefix []byte
	Keys   [][]byte
}

var listDesc = &turing.Description{
	Name: "turing/List",
}

// Describe implements the turing.Instruction interface.
func (l *List) Describe() *turing.Description {
	return listDesc
}

// Effect implements the turing.Instruction interface.
func (l *List) Effect() int {
	return 0
}

// Execute implements the turing.Instruction interface.
func (l *List) Execute(mem turing.Memory, _ turing.Cache) error {
	// reset map
	l.Keys = make([][]byte, 0, 512)

	// create iterator
	iter := mem.Iterate(l.Prefix)
	defer iter.Close()

	// add all keys
	for iter.First(); iter.Valid(); iter.Next() {
		l.Keys = append(l.Keys, turing.Clone(iter.TempKey()))
	}

	return nil
}

// Encode implements the turing.Instruction interface.
func (l *List) Encode() ([]byte, turing.Ref, error) {
	return fpack.Encode(true, func(enc *fpack.Encoder) error {
		// encode version
		enc.Uint8(1)

		// encode prefix
		enc.VarBytes(l.Prefix)

		// encode length
		enc.VarUint(uint64(len(l.Keys)))

		// encode keys
		for _, key := range l.Keys {
			enc.VarBytes(key)
		}

		return nil
	})
}

// Decode implements the turing.Instruction interface.
func (l *List) Decode(bytes []byte) error {
	return fpack.Decode(bytes, func(dec *fpack.Decoder) error {
		// check version
		if dec.Uint8() != 1 {
			return fmt.Errorf("stdset: decode list: invalid version")
		}

		// decode prefix
		l.Prefix = dec.VarBytes(true)

		// decode length
		length := dec.VarUint()

		// decode keys
		l.Keys = make([][]byte, length)
		for i := 0; i < int(length); i++ {
			l.Keys[i] = dec.VarBytes(true)
		}

		return nil
	})
}
