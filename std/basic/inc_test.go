package basic

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/256dpi/turing"
)

func TestInc(t *testing.T) {
	machine := turing.TestMachine(&Inc{}, &Get{})
	defer machine.Stop()

	err := machine.Execute(&Inc{
		Key:   []byte("foo"),
		Value: 1,
	}, false)
	assert.NoError(t, err)

	err = machine.Execute(&Inc{
		Key:   []byte("foo"),
		Value: 2,
	}, false)
	assert.NoError(t, err)

	var get = Get{
		Key: []byte("foo"),
	}

	err = machine.Execute(&get, false)
	assert.NoError(t, err)
	assert.Equal(t, []byte("3"), get.Value)

	err = machine.Execute(&Inc{
		Key:   []byte("foo"),
		Value: 3,
	}, false)
	assert.NoError(t, err)

	err = machine.Execute(&get, false)
	assert.NoError(t, err)
	assert.Equal(t, []byte("6"), get.Value)
}

func BenchmarkInc(b *testing.B) {
	machine := turing.TestMachine(&Inc{}, &Get{})
	defer machine.Stop()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := machine.Execute(&Inc{
			Key:   []byte("foo"),
			Value: 1,
		}, false)
		if err != nil {
			panic(err)
		}
	}
}
