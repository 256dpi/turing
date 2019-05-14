package turing

type Instruction interface {
	// Name should return the name of the instruction.
	Name() string

	// Build should return a new instruction.
	Build() Instruction

	// Encode should encode the instruction.
	Encode() ([]byte, error)

	// Decode should decode the instruction.
	Decode([]byte) error

	// Execute should execute the instruction.
	Execute(*Transaction) error

	// Cardinality should return the number of changes introduced by the
	// instruction.
	Cardinality() int

	// ReadOnly should return true if the instruction will not change data.
	ReadOnly() bool
}
