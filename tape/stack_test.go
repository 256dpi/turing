package tape

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

	var ops []Operand
	err = WalkStack(bytes, func(i int, op Operand) (bool, error) {
		ops = append(ops, op)
		return true, nil
	})
	assert.Equal(t, in.Operands, ops)

	assert.Equal(t, 0.0, testing.AllocsPerRun(10, func() {
		_, ref, _ := in.Encode(true)
		ref.Release()
	}))

	assert.Equal(t, 1.0, testing.AllocsPerRun(10, func() {
		_ = out.Decode(bytes, false)
	}))

	assert.Equal(t, 0.0, testing.AllocsPerRun(10, func() {
		_ = WalkStack(bytes, func(i int, op Operand) (bool, error) {
			return true, nil
		})
	}))
}

func BenchmarkStackEncode(b *testing.B) {
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

func BenchmarkStackDecode(b *testing.B) {
	data := []byte("\x01\x00\x02\x00\x03foo\x00\x00\x00\x03bar\x00\x03baz\x00\x00\x00\x03quz")

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
	data := []byte("\x01\x00\x02\x00\x03foo\x00\x00\x00\x03bar\x00\x03baz\x00\x00\x00\x03quz")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := WalkStack(data, func(i int, op Operand) (bool, error) {
			return true, nil
		})
		if err != nil {
			panic(err)
		}
	}
}
