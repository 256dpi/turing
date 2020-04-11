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

	bytes, _, err := in.Encode(false)
	assert.NoError(t, err)
	assert.NotEmpty(t, bytes)

	var out Value
	err = out.Decode(bytes, false)
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

	bytes, _, err := in.Encode(false)
	assert.NoError(t, err)
	assert.NotEmpty(t, bytes)

	var out Value
	err = out.Decode(bytes, false)
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

func BenchmarkEncodeFullValue(b *testing.B) {
	value := Value{
		Kind:  FullValue,
		Value: []byte("foo"),
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, ref, err := value.Encode(true)
		if err != nil {
			panic(err)
		}

		ref.Release()
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
		_, ref, err := value.Encode(true)
		if err != nil {
			panic(err)
		}

		ref.Release()
	}
}

func BenchmarkDecodeFullValue(b *testing.B) {
	data := []byte("\x01\x01foo")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var value Value
		err := value.Decode(data, false)
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
		var value Value
		err := value.Decode(data, false)
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
