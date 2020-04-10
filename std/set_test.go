package std

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/256dpi/turing"
)

func TestSet(t *testing.T) {
	machine := turing.Test(&Set{}, &Get{})
	defer machine.Stop()

	get := &Get{
		Key: []byte("foo"),
	}

	err := machine.Execute(get)
	assert.NoError(t, err)
	assert.False(t, get.Exists)
	assert.Nil(t, get.Value)

	err = machine.Execute(&Set{
		Key:   []byte("foo"),
		Value: []byte("bar"),
	})
	assert.NoError(t, err)

	err = machine.Execute(get)
	assert.NoError(t, err)
	assert.True(t, get.Exists)
	assert.Equal(t, []byte("bar"), get.Value)
}

func BenchmarkSet(b *testing.B) {
	machine := turing.Test(&Set{})
	defer machine.Stop()

	set := &Set{
		Key:   []byte("foo"),
		Value: []byte("bar"),
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := machine.Execute(set)
		if err != nil {
			panic(err)
		}
	}
}
