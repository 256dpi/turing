package turing

// TODO: Writable instructions?

// TODO: Batchable instructions?

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
}
