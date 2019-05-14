package turing

import (
	"encoding/json"
	"reflect"
)

type Description struct {
	// The unique name of the function.
	Name string

	// The amount of modifications this instruction will induce. A positive
	// number is interpreted as the maximum amount of set or deleted keys during
	// the execution. A zero value indicates that the instruction is ready only
	// and will not set or delete any keys. A negative number indicates that the
	// effect is unbounded.
	Effect int

	// The builder can be set to implement a custom builder. If not set, the
	// default reflect based build will be used.
	Builder func() Instruction

	// The encoder can be set to implement a custom encoding. If not set, the
	// standard JSON encoder will be used.
	Encoder func(Instruction) ([]byte, error)

	// The decoder can tbe set to implement a custom decoding. If not set, the
	// standard JSON decoder will be used.
	Decoder func([]byte, Instruction) error
}

type Instruction interface {
	// Describe should return a description of the instruction.
	Describe() Description

	// Execute should execute the instruction.
	Execute(*Transaction) error
}

func buildInstruction(i Instruction) Instruction {
	// use builder if available
	if i.Describe().Builder != nil {
		return i.Describe().Builder()
	}

	// otherwise use reflect
	return reflect.New(reflect.TypeOf(i).Elem()).Interface().(Instruction)
}

func encodeInstruction(i Instruction) ([]byte, error) {
	// use encoder if available
	if i.Describe().Encoder != nil {
		return i.Describe().Encoder(i)
	}

	// otherwise use json
	return json.Marshal(i)
}

func decodeInstruction(data []byte, i Instruction) error {
	// use decoder if available
	if i.Describe().Decoder != nil {
		return i.Describe().Decoder(data, i)
	}

	// otherwise use json
	return json.Unmarshal(data, i)
}
