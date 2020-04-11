package stdset

import (
	"fmt"

	"github.com/256dpi/turing"
	"github.com/256dpi/turing/pkg/coding"
)

// Map will map all key value pairs.
type Map struct {
	Prefix []byte
	Pairs  map[string][]byte
}

var mapDesc = &turing.Description{
	Name: "turing/Map",
}

// Describe implements the turing.Instruction interface.
func (m *Map) Describe() *turing.Description {
	return mapDesc
}

// Effect implements the turing.Instruction interface.
func (m *Map) Effect() int {
	return 0
}

// Execute implements the turing.Instruction interface.
func (m *Map) Execute(txn *turing.Transaction) error {
	// create map
	m.Pairs = make(map[string][]byte)

	// create iterator
	iter := txn.Iterator(m.Prefix)
	defer iter.Close()

	// iterate through all pairs
	for iter.First(); iter.Valid(); iter.Next() {
		// get value
		value, ref, err := iter.Value()
		if err != nil {
			return err
		}

		// add pair
		m.Pairs[string(iter.TempKey())] = turing.Clone(value)

		// release value
		ref.Release()
	}

	return nil
}

// Encode implements the turing.Instruction interface.
func (m *Map) Encode() ([]byte, turing.Ref, error) {
	return coding.Encode(true, func(enc *coding.Encoder) error {
		// encode version
		enc.VarUint(1)

		// encode prefix
		enc.Bytes(m.Prefix)

		// encode length
		enc.VarUint(uint64(len(m.Pairs)))

		// encode pairs
		for key, value := range m.Pairs {
			enc.String(key)
			enc.Bytes(value)
		}

		return nil
	})
}

// Decode implements the turing.Instruction interface.
func (m *Map) Decode(bytes []byte) error {
	return coding.Decode(bytes, func(dec *coding.Decoder) error {
		// decode version
		var version uint64
		dec.VarUint(&version)
		if version != 1 {
			return fmt.Errorf("stdset: decode map: invalid version")
		}

		// decode prefix
		dec.Bytes(&m.Prefix, true)

		// decode length
		var length uint64
		dec.VarUint(&length)

		// decode pairs
		m.Pairs = map[string][]byte{}
		for i := 0; i < int(length); i++ {
			// decode key
			var key string
			dec.String(&key, true)

			// decode value
			var value []byte
			dec.Bytes(&value, true)

			// set pair
			m.Pairs[key] = value
		}

		return nil
	})
}
