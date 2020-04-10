package std

import (
	"github.com/vmihailenco/msgpack/v4"

	"github.com/256dpi/turing"
)

type Map struct {
	Prefix []byte            `msgpack:"p,omitempty"`
	Pairs  map[string][]byte `msgpack:"m,omitempty"`
}

var mapDesc = &turing.Description{
	Name: "turing/Map",
}

func (m *Map) Describe() *turing.Description {
	return mapDesc
}

func (m *Map) Effect() int {
	return 0
}

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
		m.Pairs[string(iter.TempKey())] = turing.Copy(value)

		// release value
		ref.Release()
	}

	return nil
}

func (m *Map) Encode() ([]byte, turing.Ref, error) {
	buf, err := msgpack.Marshal(m)
	return buf, turing.NoopRef, err
}

func (m *Map) Decode(bytes []byte) error {
	return msgpack.Unmarshal(bytes, m)
}
