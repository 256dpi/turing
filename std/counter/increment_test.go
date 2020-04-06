package counter

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/256dpi/turing"
	"github.com/256dpi/turing/std/basic"
)

var instructions = []turing.Instruction{
	&Increment{},
}

var operators = []*turing.Operator{
	Add,
}

func TestIncrement(t *testing.T) {
	machine := turing.TestMachine(instructions, operators)
	defer machine.Stop()

	err := machine.Execute(nil, &Increment{
		Key:   []byte("foo"),
		Value: 1,
	}, false)
	assert.NoError(t, err)

	err = machine.Execute(nil, &Increment{
		Key:   []byte("foo"),
		Value: 2,
	}, false)
	assert.NoError(t, err)

	var get = basic.Get{
		Key: []byte("foo"),
	}

	err = machine.Execute(nil, &get, false)
	assert.NoError(t, err)
	assert.Equal(t, []byte("3"), get.Value)

	err = machine.Execute(nil, &Increment{
		Key:   []byte("foo"),
		Value: 3,
	}, false)
	assert.NoError(t, err)

	err = machine.Execute(nil, &get, false)
	assert.NoError(t, err)
	assert.Equal(t, []byte("6"), get.Value)
}
