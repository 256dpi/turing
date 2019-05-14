package turing

type Description struct {
	Name        string
	Cardinality int
	ReadOnly    bool
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
