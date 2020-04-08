package turing

import (
	"fmt"
)

type merger struct {
	registry *registry
	values   [][]byte
	order    bool
}

func newMerger(registry *registry, value []byte) *merger {
	return &merger{
		registry: registry,
		values:   [][]byte{value},
		order:    true,
	}
}

func (m *merger) MergeNewer(value []byte) error {
	// sort values
	m.sort(true)

	// add value
	m.values = append(m.values, value)

	return nil
}

func (m *merger) MergeOlder(value []byte) error {
	// sort values
	m.sort(false)

	// add value
	m.values = append(m.values, value)

	return nil
}

func (m *merger) Finish() ([]byte, error) {
	// sort values
	m.sort(true)

	// decode values
	values := make([]Value, 0, len(m.values))
	for _, op := range m.values {
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

func (m *merger) sort(fwd bool) {
	// check if already sorted
	if m.order == fwd {
		return
	}

	// reverse values
	for i := 0; i < len(m.values)/2; i++ {
		j := len(m.values) - 1 - i
		m.values[i], m.values[j] = m.values[j], m.values[i]
	}

	// set order
	m.order = fwd
}
