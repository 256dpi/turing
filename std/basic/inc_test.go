package basic

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/256dpi/turing"
)

var instructions = []turing.Instruction{
	&Inc{},
}

var operators = []*turing.Operator{
	Add,
}

func TestInc(t *testing.T) {
	machine := turing.TestMachine(instructions, operators)
	defer machine.Stop()

	err := machine.Execute(nil, &Inc{
		Key:   []byte("foo"),
		Value: 1,
	}, false)
	assert.NoError(t, err)

	err = machine.Execute(nil, &Inc{
		Key:   []byte("foo"),
		Value: 2,
	}, false)
	assert.NoError(t, err)

	var get = Get{
		Key: []byte("foo"),
	}

	err = machine.Execute(nil, &get, false)
	assert.NoError(t, err)
	assert.Equal(t, []byte("3"), get.Value)

	err = machine.Execute(nil, &Inc{
		Key:   []byte("foo"),
		Value: 3,
	}, false)
	assert.NoError(t, err)

	err = machine.Execute(nil, &get, false)
	assert.NoError(t, err)
	assert.Equal(t, []byte("6"), get.Value)
}
