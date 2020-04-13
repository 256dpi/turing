package tape

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStateCoding(t *testing.T) {
	in := State{
		Index: 1,
		Batch: 2,
		Last:  3,
	}

	bytes, _, err := in.Encode(false)
	assert.NoError(t, err)
	assert.NotEmpty(t, bytes)

	var out State
	err = out.Decode(bytes)
	assert.NoError(t, err)
	assert.Equal(t, in, out)

	assert.Equal(t, 0.0, testing.AllocsPerRun(10, func() {
		_, ref, _ := in.Encode(true)
		ref.Release()
	}))

	assert.Equal(t, 0.0, testing.AllocsPerRun(10, func() {
		_ = out.Decode(bytes)
	}))
}

func BenchmarkStateEncode(b *testing.B) {
	state := State{
		Index: 1,
		Batch: 2,
		Last:  3,
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, ref, err := state.Encode(true)
		if err != nil {
			panic(err)
		}

		ref.Release()
	}
}

func BenchmarkStateDecode(b *testing.B) {
	bytes := []byte("\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x03")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var state State
		err := state.Decode(bytes)
		if err != nil {
			panic(err)
		}
	}
}
