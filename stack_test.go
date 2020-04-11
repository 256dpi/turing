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

	var names []string
	var values [][]byte
	err = WalkStack(bytes, func(name string, value []byte) bool {
		names = append(names, name)
		values = append(values, value)
		return true
	})

	assert.Equal(t, []string{"foo", "baz"}, names)
	assert.Equal(t, [][]byte{[]byte("bar"), []byte("quz")}, values)
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

func BenchmarkWalkStack(b *testing.B) {
	data := []byte("\x01\x02\x03foo\x03bar\x03baz\x03quz")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := WalkStack(data, func(name string, value []byte) bool {
			return true
		})
		if err != nil {
			panic(err)
		}
	}
}
