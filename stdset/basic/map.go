package basic

import (
	"github.com/256dpi/turing"
)

type Map struct {
	Prefix []byte            `json:"prefix,omitempty"`
	Pairs  map[string][]byte `json:"pairs,omitempty"`
}

func (m *Map) Describe() turing.Description {
	return turing.Description{
		Name:     "stdset/basic.Map",
		ReadOnly: true,
	}
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
