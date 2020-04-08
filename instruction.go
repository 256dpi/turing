package turing

import (
	"errors"
	"reflect"
)

// MaxEffect is maximum effect that can be reported by an instruction.
// Instructions with a bigger effect must report an unbounded effect.
const MaxEffect = 10000

// UnboundedEffect can be used as an effect if the instruction potentially
// modifies more keys than MaxEffect allows. These instructions may be applied
// using multiple transactions.
const UnboundedEffect = -1

// Operator describes a merge operator.
type Operator struct {
	// The name of the operator.
	Name string

	// The zero value used as the base value if there is no full value.
	Zero []byte

	// The function called to apply operands to a value.
	Apply func(value []byte, ops [][]byte) ([]byte, error)

	// An optional function called to combine operands.
	// Combine func(ops [][]byte) ([]byte, error)
}

// Instruction is the interface that is implemented by instructions that are
// executed by the machine.
type Instruction interface {
	// Describe should return a description of the instruction.
	Describe() Description

	// Execute should execute the instruction.
	Execute(*Transaction) error

	// Encode should encode the instruction.
	Encode() ([]byte, error)

	// Decode should decode the instruction.
	Decode([]byte) error
}

// Description is a description of an instruction.
type Description struct {
	// The unique name of the instruction. The notation "path/package/Type" is
	// recommended to ease discoverability.
	Name string

	// The amount of modifications this instruction will make. A positive
	// number is interpreted as the maximum amount of set, unset merged and
	// deleted keys during the execution. A zero value indicates that the
	// instruction is read only and will not set or delete any keys. A negative
	// number indicates that the effect is unbounded and may modify many keys.
	Effect int

	// The builder can be set to implement a custom builder. If not set, the
	// default reflect based builder will be used.
	Builder func() Instruction

	// The operators used by this instruction. Deprecated operators must be
	// retained to ensure they can be used to compact older database levels.
	Operators []*Operator
}

// Validate will validate the instruction description.
func (d Description) Validate() error {
	// check name
	if d.Name == "" {
		return errors.New("turing: missing instruction name")
	}

	// check effect
	if d.Effect > MaxEffect {
		return errors.New("turing: instruction effect too high")
	} else if d.Effect < 0 && d.Effect != UnboundedEffect {
		return errors.New("turing: invalid instruction effect")
	}

	return nil
}

func buildInstruction(i Instruction) Instruction {
	// use builder if available
	if i.Describe().Builder != nil {
		return i.Describe().Builder()
	}

	// otherwise use reflect
	return reflect.New(reflect.TypeOf(i).Elem()).Interface().(Instruction)
}
