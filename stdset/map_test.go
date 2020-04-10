package stdset

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/256dpi/turing"
)

func TestMap(t *testing.T) {
	machine := turing.Test(&Map{}, &Set{})
	defer machine.Stop()

	mp := &Map{
		Prefix: []byte("b"),
	}

	err := machine.Execute(mp)
	assert.NoError(t, err)
	assert.Empty(t, mp.Pairs)

	err = machine.Execute(&Set{
		Key:   []byte("bar"),
		Value: []byte("foo"),
	})
	assert.NoError(t, err)

	err = machine.Execute(mp)
	assert.NoError(t, err)
	assert.Equal(t, map[string][]byte{
		"bar": []byte("foo"),
	}, mp.Pairs)
}

func BenchmarkMap(b *testing.B) {
	machine := turing.Test(&Map{}, &Set{})
	defer machine.Stop()

	err := machine.Execute(&Set{
		Key:   []byte("bar"),
		Value: []byte("foo"),
	})
	if err != nil {
		panic(err)
	}

	mp := &Map{
		Prefix: []byte("b"),
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := machine.Execute(mp)
		if err != nil {
			panic(err)
		}

		mp.Pairs = nil
	}
}
