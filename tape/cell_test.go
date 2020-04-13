package tape

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCellCoding(t *testing.T) {
	in := Cell{
		Type:  RawCell,
		Value: []byte("foo bar"),
	}

	bytes, _, err := in.Encode(false)
	assert.NoError(t, err)
	assert.NotEmpty(t, bytes)

	var out Cell
	err = out.Decode(bytes, false)
	assert.NoError(t, err)
	assert.Equal(t, in, out)

	assert.Equal(t, 0.0, testing.AllocsPerRun(10, func() {
		_, ref, _ := in.Encode(true)
		ref.Release()
	}))

	assert.Equal(t, 0.0, testing.AllocsPerRun(10, func() {
		_ = out.Decode(bytes, false)
	}))
}

func BenchmarkCellEncode(b *testing.B) {
	value := Cell{
		Type:  RawCell,
		Value: []byte("foo bar"),
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

func BenchmarkCellDecode(b *testing.B) {
	bytes := []byte("\x01\x01foo")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var cell Cell
		err := cell.Decode(bytes, false)
		if err != nil {
			panic(err)
		}
	}
}
