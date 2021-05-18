package turing

import (
	"io"
	"sync"

	"github.com/256dpi/fpack"
	"github.com/256dpi/turing/tape"
)

type merger struct {
	registry *registry
	values   [][]byte
	refs     []Ref
	cells    []tape.Cell
	order    bool
	retained bool
	result   Ref
}

var mergerPool = sync.Pool{
	New: func() interface{} {
		return &merger{
			values: make([][]byte, 0, 1000),
			refs:   make([]Ref, 0, 1000),
			cells:  make([]tape.Cell, 0, 1000),
		}
	},
}

func newMerger(registry *registry, value []byte) *merger {
	// borrow merger
	merger := mergerPool.Get().(*merger)
	merger.registry = registry
	merger.order = true

	// add value
	op, ref := fpack.Clone(value)
	merger.values = append(merger.values, op)
	merger.refs = append(merger.refs, ref)

	return merger
}

func (m *merger) MergeNewer(value []byte) error {
	// sort stack
	m.sort(true)

	// add value
	op, ref := fpack.Clone(value)
	m.values = append(m.values, op)
	m.refs = append(m.refs, ref)

	return nil
}

func (m *merger) MergeOlder(value []byte) error {
	// sort stack
	m.sort(false)

	// add value
	op, ref := fpack.Clone(value)
	m.values = append(m.values, op)
	m.refs = append(m.refs, ref)

	return nil
}

func (m *merger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	// recycle merger
	defer m.recycle()

	// sort values
	m.sort(true)

	// decode cells
	for _, value := range m.values {
		var cell tape.Cell
		err := cell.Decode(value, false)
		if err != nil {
			return nil, nil, err
		}
		m.cells = append(m.cells, cell)
	}

	// get computer computer
	computer := newComputer(m.registry)
	defer computer.recycle()

	// apply if first cell is a raw cell, otherwise combine if stack cell
	switch m.cells[0].Type {
	case tape.RawCell:
		// apply cells
		result, ref, err := computer.apply(m.cells)
		if err != nil {
			return nil, nil, err
		}

		// ensure release
		defer ref.Release()

		// encode result
		res, resRef, err := result.Encode(true)
		if err != nil {
			return nil, nil, err
		}

		// retain for closing
		m.retained = true
		m.result = resRef

		return res, m, nil
	case tape.StackCell:
		// combine cells
		result, ref, err := computer.combine(m.cells)
		if err != nil {
			return nil, nil, err
		}

		// ensure release
		defer ref.Release()

		// resolve if base is included
		if includesBase {
			newResult, newRef, err := computer.resolve(result)
			if err != nil {
				return nil, nil, err
			}
			defer newRef.Release()
			result = newResult
		}

		// encode result
		res, resRef, err := result.Encode(true)
		if err != nil {
			return nil, nil, err
		}

		// retain for closing
		m.retained = true
		m.result = resRef

		return res, m, nil
	default:
		panic("unexpected condition")
	}
}

func (m *merger) Close() error {
	// release result
	m.result.Release()

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

	// reset lists
	m.values = m.values[:0]
	m.refs = m.refs[:0]
	m.cells = m.cells[:0]

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

	// otherwise reverse values
	for i := 0; i < len(m.values)/2; i++ {
		j := len(m.values) - 1 - i
		m.values[i], m.values[j] = m.values[j], m.values[i]
	}

	// set order
	m.order = fwd
}
