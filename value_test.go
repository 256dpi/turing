package turing

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFullValue(t *testing.T) {
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

func TestStackValue(t *testing.T) {
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
	data := []byte("\x01foo")

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
	data := []byte("\x02\x03\x03foo\x03foo\x03bar\x03bar\x03baz\x03baz")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := DecodeValue(data)
		if err != nil {
			panic(err)
		}
	}
}
