package turing

import (
	"io"
	"sync"

	"github.com/256dpi/turing/coding"
)

type merger struct {
	registry *registry
	operands [][]byte
	opRefs   []Ref
	values   []Value
	order    bool
	retained bool
	resRef   Ref
}

var mergerPool = sync.Pool{
	New: func() interface{} {
		return &merger{
			operands: make([][]byte, 0, 1000),
			opRefs:   make([]Ref, 0, 1000),
			values:   make([]Value, 0, 1000),
		}
	},
}

func newMerger(registry *registry, value []byte) *merger {
	// borrow merger
	merger := mergerPool.Get().(*merger)
	merger.registry = registry
	merger.order = true

	// add operand
	op, ref := coding.Clone(value)
	merger.operands = append(merger.operands, op)
	merger.opRefs = append(merger.opRefs, ref)

	return merger
}

func (m *merger) MergeNewer(value []byte) error {
	// sort stack
	m.sort(true)

	// add operand
	op, ref := coding.Clone(value)
	m.operands = append(m.operands, op)
	m.opRefs = append(m.opRefs, ref)

	return nil
}

func (m *merger) MergeOlder(value []byte) error {
	// sort stack
	m.sort(false)

	// add operand
	op, ref := coding.Clone(value)
	m.operands = append(m.operands, op)
	m.opRefs = append(m.opRefs, ref)

	return nil
}

func (m *merger) Finish() ([]byte, io.Closer, error) {
	// recycle merger
	defer m.recycle()

	// sort stack
	m.sort(true)

	// decode values (no need to clone as only used temporary)
	for _, op := range m.operands {
		var value Value
		err := value.Decode(op, false)
		if err != nil {
			return nil, nil, err
		}
		m.values = append(m.values, value)
	}

	// TODO: Merge with zero value if the base value is a stack.
	//  => Improve pebble to provide the info.

	// merge values if first value is a full value, otherwise stack all values
	switch m.values[0].Kind {
	case FullValue:
		// merge values
		computer := newComputer(m.registry)
		value, ref, err := computer.eval(m.values)
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
		computer := newComputer(m.registry)
		value, ref, err := computer.combine(m.values)
		if err != nil {
			return nil, nil, err
		}

		// ensure release
		defer ref.Release()

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
	for _, ref := range m.opRefs {
		ref.Release()
	}

	// reset slices
	m.operands = m.operands[:0]
	m.opRefs = m.opRefs[:0]
	m.values = m.values[:0]

	// return if not retained
	if !m.retained {
		mergerPool.Put(m)
	}
}

func (m *merger) sort(fwd bool) {
	// check if already sorted
	if m.order == fwd {
		return
	}

	// reverse stack
	for i := 0; i < len(m.operands)/2; i++ {
		j := len(m.operands) - 1 - i
		m.operands[i], m.operands[j] = m.operands[j], m.operands[i]
	}

	// set order
	m.order = fwd
}
