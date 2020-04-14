package stdset

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/256dpi/turing"
)

func TestDump(t *testing.T) {
	machine := turing.Test(&Dump{}, &Set{})
	defer machine.Stop()

	mp := &Dump{
		Prefix: []byte("b"),
	}

	err := machine.Execute(mp)
	assert.NoError(t, err)
	assert.Empty(t, mp.Map)

	err = machine.Execute(&Set{
		Key:   []byte("bar"),
		Value: []byte("foo"),
	})
	assert.NoError(t, err)

	err = machine.Execute(mp)
	assert.NoError(t, err)
	assert.Equal(t, map[string]string{
		"bar": "foo",
	}, mp.Map)
}

func BenchmarkDump(b *testing.B) {
	machine := turing.Test(&Dump{}, &Set{})
	defer machine.Stop()

	err := machine.Execute(&Set{
		Key:   []byte("bar"),
		Value: []byte("foo"),
	})
	if err != nil {
		panic(err)
	}

	dump := &Dump{
		Prefix: []byte("b"),
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := machine.Execute(dump)
		if err != nil {
			panic(err)
		}

		dump.Map = nil
	}
}
