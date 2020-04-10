package std

import (
	"fmt"

	"github.com/256dpi/turing"
	"github.com/256dpi/turing/pkg/coding"
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
func (l *List) Execute(txn *turing.Transaction) error {
	// reset Map
	l.Keys = make([][]byte, 0, 512)

	// create iterator
	iter := txn.Iterator(l.Prefix)
	defer iter.Close()

	// add all keys
	for iter.First(); iter.Valid(); iter.Next() {
		l.Keys = append(l.Keys, turing.Copy(iter.TempKey()))
	}

	return nil
}

// Encode implements the turing.Instruction interface.
func (l *List) Encode() ([]byte, turing.Ref, error) {
	return coding.Encode(true, func(enc *coding.Encoder) error {
		// encode version
		enc.Uint(1)

		// encode prefix
		enc.Bytes(l.Prefix)

		// encode length
		enc.Uint(uint64(len(l.Keys)))

		// encode keys
		for _, key := range l.Keys {
			enc.Bytes(key)
		}

		return nil
	})
}

// Decode implements the turing.Instruction interface.
func (l *List) Decode(bytes []byte) error {
	return coding.Decode(bytes, func(dec *coding.Decoder) error {
		// decode version
		var version uint64
		dec.Uint(&version)
		if version != 1 {
			return fmt.Errorf("std: decode list: invalid version")
		}

		// decode prefix
		dec.Bytes(&l.Prefix, true)

		// decode length
		var length uint64
		dec.Uint(&length)

		// decode keys
		l.Keys = make([][]byte, length)
		for i := 0; i < int(length); i++ {
			dec.Bytes(&l.Keys[i], true)
		}

		return nil
	})
}
