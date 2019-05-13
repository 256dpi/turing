package basic

import (
	"encoding/json"

	"github.com/256dpi/turing"
)

type Map struct {
	Prefix []byte            `json:"prefix,omitempty"`
	Pairs  map[string][]byte `json:"pairs,omitempty"`
}

func (m *Map) Name() string {
	return "stdset/basic.Map"
}

func (m *Map) Build() turing.Instruction {
	return &Map{}
}

func (m *Map) Encode() ([]byte, error) {
	return json.Marshal(m)
}

func (m *Map) Decode(data []byte) error {
	return json.Unmarshal(data, m)
}

func (m *Map) Execute(txn *turing.Transaction) error {
	// create map
	m.Pairs = make(map[string][]byte)

	// create iterator
	iter := txn.Iterator(turing.IteratorConfig{
		Prefix:   m.Prefix,
		Prefetch: 100,
	})

	// ensure closing
	defer iter.Close()

	// iterate through all pairs
	for iter.Seek(nil); iter.Valid(); iter.Next() {
		// load value
		value, err := iter.Pair().CopyValue(nil)
		if err != nil {
			return err
		}

		// set key value
		m.Pairs[string(iter.Pair().Key())] = value
	}

	return nil
}

func (m *Map) Cardinality() int {
	return 0
}