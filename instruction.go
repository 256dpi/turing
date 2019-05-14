package turing

type Description struct {
	// The unique name of the function.
	Name string

	// If the instruction only reads data.
	ReadOnly bool

	// The cardinality of the function.
	Cardinality int
}

type Instruction interface {
	// Describe should return a description of the instruction.
	Describe() Description

	// Build should return a new instruction.
	Build() Instruction

	// Encode should encode the instruction.
	Encode() ([]byte, error)

	// Decode should decode the instruction.
	Decode([]byte) error

	// Execute should execute the instruction.
	Execute(*Transaction) error
}
