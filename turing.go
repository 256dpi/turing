// Package turing provides a framework to build domain specific databases.
package turing

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

// MaxEffect is maximum effect that can be reported by an instruction.
// Instructions with a bigger effect must report an unbounded effect.
const MaxEffect = 10000

// UnboundedEffect can be used as an effect if the instruction potentially
// modifies more keys than MaxEffect allows. These instructions may be applied
// using multiple transactions.
const UnboundedEffect = -1

// Ref manages the reference to buffer that can be released.
type Ref interface {
	Release()
}

// Operator describes a merge operator.
type Operator struct {
	// The name of the operator.
	Name string

	// The zero value used as the base value if there is no full value.
	Zero []byte

	// The function called to apply operands to a value.
	Apply func(value []byte, ops [][]byte) ([]byte, Ref, error)

	// An optional function called to combine operands.
	Combine func(ops [][]byte) ([]byte, Ref, error)

	counter prometheus.Counter
}

// Instruction is the interface that is implemented by instructions that are
// executed by the machine.
type Instruction interface {
	// Describe should return a description of the instruction. This method is
	// called often, therefore it should just return a pointer to a statically
	// allocated object and never build the object on request.
	Describe() *Description

	// Effect should return the amount of modifications this instruction will
	// make. A positive number is interpreted as the maximum amount of set,
	// unset merged and deleted keys during the execution. A zero value
	// indicates that the instruction is read only and will not set or delete
	// any keys. A negative number indicates that the effect is unbounded and
	// may modify many keys.
	Effect() int

	// Execute should execute the instruction using the provided transaction.
	Execute(*Transaction) error

	// Encode should encode the instruction.
	Encode() ([]byte, Ref, error)

	// Decode should decode the instruction.
	Decode([]byte) error
}

// Description is a description of an instruction.
type Description struct {
	// The unique name of the instruction. The notation "path/package/Type" is
	// recommended to ease discoverability.
	Name string

	// The builder can be set to provide a custom builder. If not set, the
	// default reflect based builder will be used.
	Builder func() Instruction

	// The recycler can be used in conjunction with the custom builder to
	// recycle built instructions.
	Recycler func(Instruction)

	// The operators used by this instruction. Deprecated operators must be
	// retained to ensure they can be used to compact older database levels.
	Operators []*Operator

	// NoResult may be set to true to indicate that the write instruction does
	// not carry a result. This potentially reduces some RPC traffic.
	NoResult bool

	observer prometheus.Observer
}

// Validate will validate the instruction description.
func (d Description) Validate() error {
	// check name
	if d.Name == "" {
		return fmt.Errorf("turing: missing instruction name")
	}

	return nil
}
