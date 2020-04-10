package stdset

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/256dpi/turing"
)

func TestList(t *testing.T) {
	machine := turing.Test(&List{}, &Set{})
	defer machine.Stop()

	list := &List{
		Prefix: []byte("b"),
	}

	err := machine.Execute(list)
	assert.NoError(t, err)
	assert.Empty(t, list.Keys)

	err = machine.Execute(&Set{
		Key:   []byte("bar"),
		Value: []byte("foo"),
	})
	assert.NoError(t, err)

	err = machine.Execute(list)
	assert.NoError(t, err)
	assert.Equal(t, [][]byte{
		[]byte("bar"),
	}, list.Keys)
}

func BenchmarkList(b *testing.B) {
	machine := turing.Test(&List{}, &Set{})
	defer machine.Stop()

	err := machine.Execute(&Set{
		Key:   []byte("bar"),
		Value: []byte("foo"),
	})
	if err != nil {
		panic(err)
	}

	list := &List{
		Prefix: []byte("b"),
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := machine.Execute(list)
		if err != nil {
			panic(err)
		}

		list.Keys = nil
	}
}
