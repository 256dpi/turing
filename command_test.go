package turing

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCommandCoding(t *testing.T) {
	in := Command{
		Operations: []Operation{
			{
				Name: "foo",
				Data: []byte("bar"),
			},
			{
				Name: "baz",
				Data: []byte("quz"),
			},
		},
	}

	bytes, _, err := in.Encode(false)
	assert.NoError(t, err)
	assert.NotEmpty(t, bytes)

	var out Command
	err = out.Decode(bytes, false)
	assert.NoError(t, err)
	assert.Equal(t, in, out)

	var ops []Operation
	err = WalkCommand(bytes, func(i int, op Operation) error {
		ops = append(ops, op)
		return nil
	})
	assert.Equal(t, in.Operations, ops)

	assert.Equal(t, 0.0, testing.AllocsPerRun(10, func() {
		_, ref, _ := in.Encode(true)
		ref.Release()
	}))

	assert.Equal(t, 1.0, testing.AllocsPerRun(10, func() {
		_ = out.Decode(bytes, false)
	}))

	assert.Equal(t, 0.0, testing.AllocsPerRun(10, func() {
		_ = WalkStack(bytes, func(op Operand) error {
			return nil
		})
	}))
}

func BenchmarkEncodeCommand(b *testing.B) {
	cmd := Command{
		Operations: []Operation{
			{
				Name: "foo",
				Data: []byte("bar"),
			},
			{
				Name: "baz",
				Data: []byte("quz"),
			},
		},
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, ref, err := cmd.Encode(true)
		if err != nil {
			panic(err)
		}

		ref.Release()
	}
}

func BenchmarkDecodeCommand(b *testing.B) {
	data := []byte("\x01\x02\x03foo\x03bar\x03baz\x03quz")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var cmd Command
		err := cmd.Decode(data, false)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkWalkCommand(b *testing.B) {
	data := []byte("\x01\x02\x03foo\x03bar\x03baz\x03quz")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := WalkCommand(data, func(i int, op Operation) error {
			return nil
		})
		if err != nil {
			panic(err)
		}
	}
}
