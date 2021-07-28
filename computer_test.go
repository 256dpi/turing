package turing

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/256dpi/turing/tape"
)

func TestComputerCombine(t *testing.T) {
	cells := []tape.Cell{
		{
			Type: tape.StackCell,
			Value: mustEncodeStack(tape.Stack{
				Operands: []tape.Operand{
					{
						Name:  "op1",
						Value: []byte("foo"),
					},
					{
						Name:  "op1",
						Value: []byte("bar"),
					},
				},
			}),
		},
		{
			Type: tape.StackCell,
			Value: mustEncodeStack(tape.Stack{
				Operands: []tape.Operand{
					{
						Name:  "op2",
						Value: []byte("baz"),
					},
				},
			}),
		},
		{
			Type: tape.StackCell,
			Value: mustEncodeStack(tape.Stack{
				Operands: []tape.Operand{
					{
						Name:  "op2",
						Value: []byte("bar"),
					},
					{
						Name:  "op1",
						Value: []byte("baz"),
					},
				},
			}),
		},
	}

	registry := &registry{
		ops: map[string]*Operator{
			"op1": {
				Name: "op1",
			},
			"op2": {
				Name: "op2",
				Combine: func(ops [][]byte) ([]byte, Ref, error) {
					// concat operands
					var value []byte
					for _, op := range ops {
						value = append(value, op...)
					}

					return value, nil, nil
				},
			},
		},
	}

	computer := newComputer(registry)
	result, ref, err := computer.combine(cells)
	assert.NoError(t, err)
	assert.Equal(t, tape.Cell{
		Type: tape.StackCell,
		Value: mustEncodeStack(tape.Stack{
			Operands: []tape.Operand{
				{
					Name:  "op1",
					Value: []byte("foo"),
				},
				{
					Name:  "op1",
					Value: []byte("bar"),
				},
				{
					Name:  "op2",
					Value: []byte("bazbar"),
				},
				{
					Name:  "op1",
					Value: []byte("baz"),
				},
			},
		}),
	}, result)
	ref.Release()

	// alloc comes from operator
	computer = newComputer(registry)
	assert.Equal(t, 2.0, testing.AllocsPerRun(10, func() {
		_, ref, _ := computer.combine(cells)
		ref.Release()
	}))
}

func TestComputerApply(t *testing.T) {
	cells := []tape.Cell{
		{
			Type:  tape.RawCell,
			Value: []byte("a"),
		},
		{
			Type: tape.StackCell,
			Value: mustEncodeStack(tape.Stack{
				Operands: []tape.Operand{
					{
						Name:  "op",
						Value: []byte("b"),
					},
					{
						Name:  "op",
						Value: []byte("c"),
					},
				},
			}),
		},
		{
			Type: tape.StackCell,
			Value: mustEncodeStack(tape.Stack{
				Operands: []tape.Operand{
					{
						Name:  "op",
						Value: []byte("d"),
					},
				},
			}),
		},
		{
			Type: tape.StackCell,
			Value: mustEncodeStack(tape.Stack{
				Operands: []tape.Operand{
					{
						Name:  "op",
						Value: []byte("e"),
					},
					{
						Name:  "op",
						Value: []byte("f"),
					},
				},
			}),
		},
	}

	registry := &registry{
		ops: map[string]*Operator{
			"op": {
				Name: "op",
				Apply: func(value []byte, ops [][]byte) ([]byte, Ref, error) {
					// clone value
					value = Clone(value)

					// concat operands
					for _, op := range ops {
						value = append(value, op...)
					}

					return value, nil, nil
				},
			},
		},
	}

	computer := newComputer(registry)
	result, ref, err := computer.apply(cells)
	assert.NoError(t, err)
	assert.Equal(t, tape.Cell{
		Type:  tape.RawCell,
		Value: []byte("abcdef"),
	}, result)
	ref.Release()

	// allocs come from operator
	computer = newComputer(registry)
	assert.Equal(t, 2.0, testing.AllocsPerRun(10, func() {
		_, ref, _ := computer.apply(cells)
		ref.Release()
	}))
}

func TestComputerResolve(t *testing.T) {
	cell := tape.Cell{
		Type: tape.StackCell,
		Value: mustEncodeStack(tape.Stack{
			Operands: []tape.Operand{
				{
					Name:  "op",
					Value: []byte("foo"),
				},
				{
					Name:  "op",
					Value: []byte("bar"),
				},
			},
		}),
	}

	registry := &registry{
		ops: map[string]*Operator{
			"op": {
				Name: "op",
				Zero: []byte(""),
				Apply: func(value []byte, ops [][]byte) ([]byte, Ref, error) {
					return ops[len(ops)-1], nil, nil
				},
			},
		},
	}

	computer := newComputer(registry)
	result, ref, err := computer.resolve(cell)
	assert.NoError(t, err)
	assert.Equal(t, tape.Cell{
		Type:  tape.RawCell,
		Value: []byte("bar"),
	}, result)
	ref.Release()

	computer = newComputer(registry)
	assert.Equal(t, 0.0, testing.AllocsPerRun(10, func() {
		_, ref, _ := computer.resolve(cell)
		ref.Release()
	}))
}

func BenchmarkComputerCombine(b *testing.B) {
	cells := []tape.Cell{
		{
			Type: tape.StackCell,
			Value: mustEncodeStack(tape.Stack{
				Operands: []tape.Operand{
					{
						Name:  "foo",
						Value: []byte("foo"),
					},
					{
						Name:  "bar",
						Value: []byte("bar"),
					},
				},
			}),
		},
		{
			Type: tape.StackCell,
			Value: mustEncodeStack(tape.Stack{
				Operands: []tape.Operand{
					{
						Name:  "baz",
						Value: []byte("baz"),
					},
				},
			}),
		},
		{
			Type: tape.StackCell,
			Value: mustEncodeStack(tape.Stack{
				Operands: []tape.Operand{
					{
						Name:  "bar",
						Value: []byte("bar"),
					},
					{
						Name:  "baz",
						Value: []byte("baz"),
					},
				},
			}),
		},
	}

	registry := &registry{
		ops: map[string]*Operator{
			"foo": {
				Name: "foo",
				Zero: []byte(""),
			},
			"bar": {
				Name: "bar",
				Zero: []byte(""),
			},
			"baz": {
				Name: "baz",
				Zero: []byte(""),
			},
		},
	}

	computer := newComputer(registry)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, ref, err := computer.combine(cells)
		if err != nil {
			panic(err)
		}
		ref.Release()
	}
}

func BenchmarkComputerApply(b *testing.B) {
	cells := []tape.Cell{
		{
			Type:  tape.RawCell,
			Value: []byte("foo"),
		},
		{
			Type: tape.StackCell,
			Value: mustEncodeStack(tape.Stack{
				Operands: []tape.Operand{
					{
						Name:  "op",
						Value: []byte("foo"),
					},
					{
						Name:  "op",
						Value: []byte("bar"),
					},
				},
			}),
		},
		{
			Type: tape.StackCell,
			Value: mustEncodeStack(tape.Stack{
				Operands: []tape.Operand{
					{
						Name:  "op",
						Value: []byte("baz"),
					},
				},
			}),
		},
		{
			Type: tape.StackCell,
			Value: mustEncodeStack(tape.Stack{
				Operands: []tape.Operand{
					{
						Name:  "op",
						Value: []byte("bar"),
					},
					{
						Name:  "op",
						Value: []byte("baz"),
					},
				},
			}),
		},
	}

	registry := &registry{
		ops: map[string]*Operator{
			"op": {
				Name: "op",
				Apply: func(value []byte, ops [][]byte) ([]byte, Ref, error) {
					return value, nil, nil
				},
			},
		},
	}

	computer := newComputer(registry)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, ref, err := computer.apply(cells)
		if err != nil {
			panic(err)
		}
		ref.Release()
	}
}

func BenchmarkComputerResolve(b *testing.B) {
	cell := tape.Cell{
		Type: tape.StackCell,
		Value: mustEncodeStack(tape.Stack{
			Operands: []tape.Operand{
				{
					Name:  "op",
					Value: []byte("foo"),
				},
				{
					Name:  "op",
					Value: []byte("bar"),
				},
			},
		}),
	}

	registry := &registry{
		ops: map[string]*Operator{
			"op": {
				Name: "op",
				Zero: []byte(""),
				Apply: func(value []byte, ops [][]byte) ([]byte, Ref, error) {
					return value, nil, nil
				},
			},
		},
	}

	computer := newComputer(registry)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, ref, err := computer.resolve(cell)
		if err != nil {
			panic(err)
		}
		ref.Release()
	}
}

func mustEncodeStack(stack tape.Stack) []byte {
	res, _, err := stack.Encode(false)
	if err != nil {
		panic(err)
	}

	return res
}
