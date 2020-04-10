package std

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/256dpi/turing"
)

func TestInc(t *testing.T) {
	machine := turing.Test(&Inc{}, &Get{})
	defer machine.Stop()

	err := machine.Execute(&Inc{
		Key:   []byte("foo"),
		Value: 1,
	})
	assert.NoError(t, err)

	err = machine.Execute(&Inc{
		Key:   []byte("foo"),
		Value: 2,
	})
	assert.NoError(t, err)

	var get = Get{
		Key: []byte("foo"),
	}

	err = machine.Execute(&get)
	assert.NoError(t, err)
	assert.Equal(t, []byte("3"), get.Value)

	err = machine.Execute(&Inc{
		Key:   []byte("foo"),
		Value: 3,
	})
	assert.NoError(t, err)

	err = machine.Execute(&get)
	assert.NoError(t, err)
	assert.Equal(t, []byte("6"), get.Value)
}

func BenchmarkInc(b *testing.B) {
	machine := turing.Test(&Inc{}, &Get{})
	defer machine.Stop()

	inc := &Inc{
		Key:   []byte("foo"),
		Value: 1,
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := machine.Execute(inc)
		if err != nil {
			panic(err)
		}
	}
}
