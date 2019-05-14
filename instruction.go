package turing

import "reflect"

type Description struct {
	// The unique name of the function.
	Name string

	// If the instruction only reads data.
	ReadOnly bool

	// The cardinality of the function.
	Cardinality int

	// The build that creates new instructions of that type.
	Builder func() Instruction
}

type Instruction interface {
	// Describe should return a description of the instruction.
	Describe() Description

	// Encode should encode the instruction.
	Encode() ([]byte, error)

	// Decode should decode the instruction.
	Decode([]byte) error

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
