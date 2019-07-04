package turing

import (
	"encoding/json"
	"errors"
	"reflect"
)

// MaxEffect is maximum effect that can be reported by an instruction.
// Instructions with a bigger effect must report an unbounded effect.
const MaxEffect = 1000

// UnboundedEffect can be used as an effect if the instruction potentially
// modifies more keys than MaxEffect allows.
const UnboundedEffect = -1

// Instruction is the interface that is implemented by instructions that are
// executed by the machine.
type Instruction interface {
	// Describe should return a description of the instruction.
	Describe() Description

	// Execute should execute the instruction.
	Execute(*Transaction) error
}

// Description is a description of an instruction.
type Description struct {
	// The unique name of the instruction. We recommend the following notation:
	// "path/package.Instruction" to ease discoverability.
	Name string

	// The amount of modifications this instruction will make. A positive
	// number is interpreted as the maximum amount of set and deleted keys during
	// the execution. A zero value indicates that the instruction is read only
	// and will not set or delete any keys. A negative number indicates that the
	// effect is unbounded and may modify many keys.
	Effect int

	// NonLinear may be set to true to allow the instruction to query data
	// without linearizability guarantees. This may be substantially faster
	// but return stale data.
	NonLinear bool

	// The builder can be set to implement a custom builder. If not set, the
	// default reflect based builder will be used.
	Builder func() Instruction

	// The encoder can be set to implement a custom encoding. If not set, the
	// default JSON encoder will be used.
	Encoder func(Instruction) ([]byte, error)

	// The decoder can tbe set to implement a custom decoding. If not set, the
	// default JSON decoder will be used.
	Decoder func([]byte, Instruction) error
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
