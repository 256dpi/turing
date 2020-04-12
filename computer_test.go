package turing

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestComputerStack(t *testing.T) {
	values := []Value{
		{
			Kind: StackValue,
			Value: mustEncodeStack(Stack{
				Operands: []Operand{
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
			Kind: StackValue,
			Value: mustEncodeStack(Stack{
				Operands: []Operand{
					{
						Name:  "baz",
						Value: []byte("baz"),
					},
				},
			}),
		},
		{
			Kind: StackValue,
			Value: mustEncodeStack(Stack{
				Operands: []Operand{
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

	computer := newComputer(nil)
	value, ref, err := computer.stack(values)
	assert.NoError(t, err)
	assert.Equal(t, Value{
		Kind: StackValue,
		Value: mustEncodeStack(Stack{
			Operands: []Operand{
				{
					Name:  "foo",
					Value: []byte("foo"),
				},
				{
					Name:  "bar",
					Value: []byte("bar"),
				},
				{
					Name:  "baz",
					Value: []byte("baz"),
				},
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
	}, value)
	ref.Release()

	assert.Equal(t, 1.0, testing.AllocsPerRun(10, func() {
		computer := newComputer(nil)
		_, ref, _ := computer.stack(values)
		ref.Release()
	}))
}

func TestComputerEval(t *testing.T) {
	values := []Value{
		{
			Kind:  FullValue,
			Value: []byte("a"),
		},
		{
			Kind: StackValue,
			Value: mustEncodeStack(Stack{
				Operands: []Operand{
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
			Kind: StackValue,
			Value: mustEncodeStack(Stack{
				Operands: []Operand{
					{
						Name:  "op",
						Value: []byte("d"),
					},
				},
			}),
		},
		{
			Kind: StackValue,
			Value: mustEncodeStack(Stack{
				Operands: []Operand{
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
					// concat operands
					for _, op := range ops {
						value = append(value, op...)
					}

					return value, NoopRef, nil
				},
			},
		},
	}

	computer := newComputer(registry)
	value, ref, err := computer.eval(values)
	assert.NoError(t, err)
	assert.Equal(t, Value{
		Kind:  FullValue,
		Value: []byte("abcdef"),
	}, value)
	ref.Release()

	assert.Equal(t, 1.0, testing.AllocsPerRun(10, func() {
		computer := newComputer(registry)
		_, ref, _ := computer.eval(values)
		ref.Release()
	}))
}

func BenchmarkComputerStack(b *testing.B) {
	values := []Value{
		{
			Kind: StackValue,
			Value: mustEncodeStack(Stack{
				Operands: []Operand{
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
			Kind: StackValue,
			Value: mustEncodeStack(Stack{
				Operands: []Operand{
					{
						Name:  "baz",
						Value: []byte("baz"),
					},
				},
			}),
		},
		{
			Kind: StackValue,
			Value: mustEncodeStack(Stack{
				Operands: []Operand{
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

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		computer := newComputer(nil)

		_, ref, err := computer.stack(values)
		if err != nil {
			panic(err)
		}

		ref.Release()
	}
}

func BenchmarkComputerEval(b *testing.B) {
	values := []Value{
		{
			Kind:  FullValue,
			Value: []byte("foo"),
		},
		{
			Kind: StackValue,
			Value: mustEncodeStack(Stack{
				Operands: []Operand{
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
			Kind: StackValue,
			Value: mustEncodeStack(Stack{
				Operands: []Operand{
					{
						Name:  "op",
						Value: []byte("baz"),
					},
				},
			}),
		},
		{
			Kind: StackValue,
			Value: mustEncodeStack(Stack{
				Operands: []Operand{
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
					return value, NoopRef, nil
				},
			},
		},
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		computer := newComputer(registry)

		_, ref, err := computer.eval(values)
		if err != nil {
			panic(err)
		}

		ref.Release()
	}
}

func mustEncodeStack(stack Stack) []byte {
	res, _, err := stack.Encode(false)
	if err != nil {
		panic(err)
	}

	return res
}
