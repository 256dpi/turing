package turing

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestComputer(t *testing.T) {
	values := []Value{
		{
			Kind:  FullValue,
			Value: []byte("a"),
		},
		{
			Kind: StackValue,
			Stack: []Operand{
				{
					Name:  "op",
					Value: []byte("b"),
				},
				{
					Name:  "op",
					Value: []byte("c"),
				},
			},
		},
		{
			Kind: StackValue,
			Stack: []Operand{
				{
					Name:  "op",
					Value: []byte("d"),
				},
			},
		},
		{
			Kind: StackValue,
			Stack: []Operand{
				{
					Name:  "op",
					Value: []byte("e"),
				},
				{
					Name:  "op",
					Value: []byte("f"),
				},
			},
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
}

func BenchmarkComputer(b *testing.B) {
	values := []Value{
		{
			Kind:  FullValue,
			Value: []byte("foo"),
		},
		{
			Kind: StackValue,
			Stack: []Operand{
				{
					Name:  "op",
					Value: []byte("foo"),
				},
				{
					Name:  "op",
					Value: []byte("bar"),
				},
			},
		},
		{
			Kind: StackValue,
			Stack: []Operand{
				{
					Name:  "op",
					Value: []byte("baz"),
				},
			},
		},
		{
			Kind: StackValue,
			Stack: []Operand{
				{
					Name:  "op",
					Value: []byte("bar"),
				},
				{
					Name:  "op",
					Value: []byte("baz"),
				},
			},
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
