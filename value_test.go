package turing

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFullValueCoding(t *testing.T) {
	in := Value{
		Kind:  FullValue,
		Value: []byte("foo"),
	}

	bytes, err := EncodeValue(in)
	assert.NoError(t, err)
	assert.NotEmpty(t, bytes)

	out, err := DecodeValue(bytes)
	assert.NoError(t, err)
	assert.Equal(t, in, out)
}

func TestStackValueCoding(t *testing.T) {
	in := Value{
		Kind: StackValue,
		Stack: []Operand{
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
		},
	}

	bytes, err := EncodeValue(in)
	assert.NoError(t, err)
	assert.NotEmpty(t, bytes)

	out, err := DecodeValue(bytes)
	assert.NoError(t, err)
	assert.Equal(t, in, out)
}

func TestStackValues(t *testing.T) {
	values := []Value{
		{
			Kind: StackValue,
			Stack: []Operand{
				{
					Name:  "foo",
					Value: []byte("foo"),
				},
				{
					Name:  "bar",
					Value: []byte("bar"),
				},
			},
		},
		{
			Kind: StackValue,
			Stack: []Operand{
				{
					Name:  "baz",
					Value: []byte("baz"),
				},
			},
		},
		{
			Kind: StackValue,
			Stack: []Operand{
				{
					Name:  "bar",
					Value: []byte("bar"),
				},
				{
					Name:  "baz",
					Value: []byte("baz"),
				},
			},
		},
	}

	value, err := StackValues(values)
	assert.NoError(t, err)
	assert.Equal(t, Value{
		Kind: StackValue,
		Stack: []Operand{
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
	}, value)
}

func TestMergeValues(t *testing.T) {
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
		operators: map[string]*Operator{
			"op": {
				Name: "op",
				Apply: func(value []byte, ops [][]byte) ([]byte, error) {
					// concat
					for _, op := range ops {
						value = append(value, op...)
					}

					return value, nil
				},
			},
		},
	}

	value, err := MergeValues(values, registry)
	assert.NoError(t, err)
	assert.Equal(t, Value{
		Kind:  FullValue,
		Value: []byte("abcdef"),
	}, value)
}

func BenchmarkEncodeFullValue(b *testing.B) {
	value := Value{
		Kind:  FullValue,
		Value: []byte("foo"),
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := EncodeValue(value)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkEncodeStackValue(b *testing.B) {
	value := Value{
		Kind: StackValue,
		Stack: []Operand{
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
		},
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := EncodeValue(value)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkDecodeFullValue(b *testing.B) {
	data := []byte("\x01\x01foo")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := DecodeValue(data)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkDecodeStackValue(b *testing.B) {
	data := []byte("\x01\x02\x03\x03foo\x03foo\x03bar\x03bar\x03baz\x03baz")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := DecodeValue(data)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkStackValues(b *testing.B) {
	values := []Value{
		{
			Kind: StackValue,
			Stack: []Operand{
				{
					Name:  "foo",
					Value: []byte("foo"),
				},
				{
					Name:  "bar",
					Value: []byte("bar"),
				},
			},
		},
		{
			Kind: StackValue,
			Stack: []Operand{
				{
					Name:  "baz",
					Value: []byte("baz"),
				},
			},
		},
		{
			Kind: StackValue,
			Stack: []Operand{
				{
					Name:  "bar",
					Value: []byte("bar"),
				},
				{
					Name:  "baz",
					Value: []byte("baz"),
				},
			},
		},
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := StackValues(values)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkMergeValues(b *testing.B) {
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
		operators: map[string]*Operator{
			"op": {
				Name: "op",
				Apply: func(value []byte, ops [][]byte) ([]byte, error) {
					return value, nil
				},
			},
		},
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := MergeValues(values, registry)
		if err != nil {
			panic(err)
		}
	}
}
