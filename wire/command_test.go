package wire

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCommandCoding(t *testing.T) {
	in := Command{
		Operations: []Operation{
			{
				Name: "foo",
				Code: []byte("bar"),
			},
			{
				Name: "baz",
				Code: []byte("quz"),
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
	err = WalkCommand(bytes, func(i int, op Operation) (bool, error) {
		ops = append(ops, op)
		return true, nil
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
		_ = WalkCommand(bytes, func(i int, op Operation) (bool, error) {
			return true, nil
		})
	}))
}

func BenchmarkCommandEncode(b *testing.B) {
	cmd := Command{
		Operations: []Operation{
			{
				Name: "foo",
				Code: []byte("bar"),
			},
			{
				Name: "baz",
				Code: []byte("quz"),
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

func BenchmarkCommandDecode(b *testing.B) {
	bytes := []byte("\x01\x00\x02\x00\x03foo\x00\x00\x00\x03bar\x00\x03baz\x00\x00\x00\x03quz")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var cmd Command
		err := cmd.Decode(bytes, false)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkWalkCommand(b *testing.B) {
	bytes := []byte("\x01\x00\x02\x00\x03foo\x00\x00\x00\x03bar\x00\x03baz\x00\x00\x00\x03quz")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := WalkCommand(bytes, func(i int, op Operation) (bool, error) {
			return true, nil
		})
		if err != nil {
			panic(err)
		}
	}
}
