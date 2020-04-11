package turing

import (
	"io"
	"sync"

	"github.com/256dpi/turing/pkg/coding"
)

const mergerPreAllocationSize = 1000

type merger struct {
	registry *registry
	stack    [][]byte
	refs     []Ref
	values   []Value
	order    bool
	retained bool
	resRef   Ref
}

var mergerPool = sync.Pool{
	New: func() interface{} {
		return &merger{
			stack:  make([][]byte, 0, mergerPreAllocationSize),
			refs:   make([]Ref, 0, mergerPreAllocationSize),
			values: make([]Value, 0, mergerPreAllocationSize),
		}
	},
}

func newMerger(registry *registry, value []byte) *merger {
	// borrow merger
	merger := mergerPool.Get().(*merger)
	merger.registry = registry
	merger.order = true

	// add value
	value, ref := coding.Copy(value)
	merger.stack = append(merger.stack, value)
	merger.refs = append(merger.refs, ref)

	return merger
}

func (m *merger) MergeNewer(value []byte) error {
	// sort stack
	m.sortStack(true)

	// add value
	value, ref := coding.Copy(value)
	m.stack = append(m.stack, value)
	m.refs = append(m.refs, ref)

	return nil
}

func (m *merger) MergeOlder(value []byte) error {
	// sort stack
	m.sortStack(false)

	// add value
	value, ref := coding.Copy(value)
	m.stack = append(m.stack, value)
	m.refs = append(m.refs, ref)

	return nil
}

func (m *merger) Finish() ([]byte, io.Closer, error) {
	// recycle merger
	defer m.recycle()

	// sort stack
	m.sortStack(true)

	// decode values (no need to clone as only used temporary)
	for _, op := range m.stack {
		var value Value
		err := value.Decode(op, false)
		if err != nil {
			return nil, nil, err
		}
		m.values = append(m.values, value)
	}

	// merge values if first value is a full value, otherwise stack all values
	switch m.values[0].Kind {
	case FullValue:
		// merge values
		value, ref, err := MergeValues(m.values, m.registry)
		if err != nil {
			return nil, nil, err
		}

		// ensure release
		defer ref.Release()

		// encode result
		res, resRef, err := value.Encode(true)
		if err != nil {
			return nil, nil, err
		}

		// retain for closing
		m.retained = true
		m.resRef = resRef

		return res, m, nil
	case StackValue:
		// stack values
		value, err := StackValues(m.values)
		if err != nil {
			return nil, nil, err
		}

		// encode value
		res, resRef, err := value.Encode(true)
		if err != nil {
			return nil, nil, err
		}

		// retain for closing
		m.retained = true
		m.resRef = resRef

		return res, m, nil
	default:
		panic("unexpected condition")
	}
}

func (m *merger) Close() error {
	// release ref
	m.resRef.Release()

	// clear flag
	m.retained = false

	// return
	mergerPool.Put(m)

	return nil
}

func (m *merger) recycle() {
	// unset registry
	m.registry = nil

	// release refs
	for _, ref := range m.refs {
		ref.Release()
	}

	// reset slices
	m.stack = m.stack[:0]
	m.refs = m.refs[:0]
	m.values = m.values[:0]

	// return if not retained
	if !m.retained {
		mergerPool.Put(m)
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
