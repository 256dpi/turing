package turing

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFullValue(t *testing.T) {
	in := Value{
		Kind:  FullValue,
		Value: []byte("-foo"),
	}

	bytes, err := EncodeValue(in)
	assert.NoError(t, err)
	assert.Equal(t, []byte("f-foo"), bytes)

	out, err := DecodeValue(bytes)
	assert.NoError(t, err)
	assert.Equal(t, in, out)
}

func TestStackValue(t *testing.T) {
	in := Value{
		Kind: StackValue,
		Stack: []Operand{
			{
				Name:  "foo",
				Value: []byte("bar"),
			},
			{
				Name:  "bar",
				Value: []byte("baz"),
			},
		},
	}

	bytes, err := EncodeValue(in)
	assert.NoError(t, err)
	assert.NotEmpty(t, bytes)

	out, err := DecodeValue(bytes)
	assert.NoError(t, err)
	assert.Equal(t, in, out)
}
