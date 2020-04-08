package turing

import (
	"fmt"
	"sync"
)

type merger struct {
	registry *registry
	stack    [][]byte
	order    bool
}

var mergerPool = sync.Pool{
	New: func() interface{} {
		return &merger{
			stack: make([][]byte, 0, 100),
		}
	},
}

func newMerger(registry *registry, value []byte) *merger {
	// borrow merger
	merger := mergerPool.Get().(*merger)
	merger.registry = registry
	merger.stack = append(merger.stack, value)
	merger.order = true

	return merger
}

func (m *merger) MergeNewer(value []byte) error {
	// sort stack
	m.sortStack(true)

	// add value
	m.stack = append(m.stack, value)

	return nil
}

func (m *merger) MergeOlder(value []byte) error {
	// sort stack
	m.sortStack(false)

	// add value
	m.stack = append(m.stack, value)

	return nil
}

func (m *merger) Finish() ([]byte, error) {
	// return merger
	defer func() {
		m.registry = nil
		for i := range m.stack {
			m.stack[i] = nil
		}
		m.stack = m.stack[:0]
		mergerPool.Put(m)
	}()

	// sort stack
	m.sortStack(true)

	// decode values
	values := make([]Value, 0, len(m.stack))
	for _, op := range m.stack {
		value, err := DecodeValue(op)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}

	// merge values if first value is a full value, otherwise stack all values
	switch values[0].Kind {
	case FullValue:
		// merge values
		value, err := MergeValues(values, m.registry)
		if err != nil {
			return nil, err
		}

		// encode result
		bytes, err := EncodeValue(value)
		if err != nil {
			return nil, err
		}

		return bytes, nil
	case StackValue:
		// stack values
		value, err := StackValues(values)
		if err != nil {
			return nil, err
		}

		// encode value
		bytes, err := EncodeValue(value)
		if err != nil {
			return nil, err
		}

		return bytes, nil
	default:
		return nil, fmt.Errorf("turing: merger: unexpected kind: %c", values[0].Kind)
	}
}

func (m *merger) sortStack(fwd bool) {
	// check if already sorted
	if m.order == fwd {
		return
	}

	// reverse stack
	for i := 0; i < len(m.stack)/2; i++ {
		j := len(m.stack) - 1 - i
		m.stack[i], m.stack[j] = m.stack[j], m.stack[i]
	}

	// set order
	m.order = fwd
}
