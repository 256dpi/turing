package turing

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStackCoding(t *testing.T) {
	in := Stack{
		Operands: []Operand{
			{
				Name:  "foo",
				Value: []byte("bar"),
			},
			{
				Name:  "baz",
				Value: []byte("quz"),
			},
		},
	}

	bytes, _, err := in.Encode(false)
	assert.NoError(t, err)
	assert.NotEmpty(t, bytes)

	var out Stack
	err = out.Decode(bytes, false)
	assert.NoError(t, err)
	assert.Equal(t, in, out)
}

func BenchmarkEncodeStack(b *testing.B) {
	stack := Stack{
		Operands: []Operand{
			{
				Name:  "foo",
				Value: []byte("bar"),
			},
			{
				Name:  "baz",
				Value: []byte("quz"),
			},
		},
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, ref, err := stack.Encode(true)
		if err != nil {
			panic(err)
		}

		ref.Release()
	}
}

func BenchmarkDecodeStack(b *testing.B) {
	data := []byte("\x01\x02\x03foo\x03bar\x03baz\x03quz")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var stack Stack
		err := stack.Decode(data, false)
		if err != nil {
			panic(err)
		}
	}
}
