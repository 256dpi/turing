// Package turing provides a framework to build domain specific databases.
package turing

import (
	"errors"
	"fmt"
	"io"

	"github.com/prometheus/client_golang/prometheus"
)

// MaxEffect is maximum effect that can be reported by an instruction.
// Instructions with a bigger effect must report an unbounded effect.
const MaxEffect = 10000

// UnboundedEffect can be used as an effect if the instruction potentially
// modifies more keys than MaxEffect allows. These instructions may be applied
// using multiple transactions.
const UnboundedEffect = -1

// ErrReadOnly is returned by a transaction on write operations if the
// instruction has been flagged as read only.
var ErrReadOnly = errors.New("turing: read only")

// ErrMaxEffect is returned by a transaction if the effect limit has been
// reached. The instruction should return with this error to have the current
// changes persistent and be executed again to persist the remaining changes.
var ErrMaxEffect = errors.New("turing: max effect")

// Ref manages the reference to buffer that can be released.
type Ref interface {
	Release()
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

	// Execute should execute the instruction using the provided memory.
	Execute(mem Memory) error

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
	Recycler func(ins Instruction)

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

// Operator describes a merge operator.
type Operator struct {
	// The name of the operator.
	Name string

	// The zero value used as the base value if there is none.
	Zero []byte

	// The function called to apply operands to a value.
	Apply func(value []byte, ops [][]byte) ([]byte, Ref, error)

	// An optional function called to combine operands.
	Combine func(ops [][]byte) ([]byte, Ref, error)

	counter prometheus.Counter
}

// Memory is interface used by instructions to read and write to the database.
type Memory interface {
	// Iterate will construct and return a new iterator. The iterator must be
	// closed as soon as it is not used anymore.
	Iterate(prefix []byte) Iterator

	// Get will lookup the specified key. The returned slice must not be modified
	// by the caller. A closer is returned that must be closed once the value is
	// not used anymore. Consider using Use() if the value is only used temporarily.
	Get(key []byte) ([]byte, bool, io.Closer, error)

	// Use will lookup the specified key and yield it to the provided function if
	// it exists.
	Use(key []byte, fn func(value []byte) error) error

	// Set will set the specified key to the new value. This operation will count
	// as one towards the effect of the backing transaction.
	Set(key, value []byte) error

	// Unset will remove the specified key. This operation will count as one towards
	// the effect of the backing transaction.
	Unset(key []byte) error

	// Delete deletes all of the keys in the range [start, end] (inclusive on start,
	// exclusive on end). This operation will count as one towards the effect of the
	// backing transaction.
	Delete(start, end []byte) error

	// Merge merges existing values with the provided value using the specified
	// operator.
	Merge(key, value []byte, operator *Operator) error

	// Effect will return the current effect of the backing transaction.
	Effect() int
}

// Iterator is used to iterate over the memory.
type Iterator interface {
	// SeekGE will seek to the exact key or the next greater key.
	SeekGE(key []byte) bool

	// SeekLT will seek to the exact key or the next smaller key.
	SeekLT(key []byte) bool

	// First will seek to the first key in the range.
	First() bool

	// Last will seek to the last key in the range.
	Last() bool

	// Valid will return whether a valid key/value pair is present.
	Valid() bool

	// Next will move on to the next key.
	Next() bool

	// Prev will go back to the previous key.
	Prev() bool

	// Key will return a buffered key that can be used until released.
	Key() ([]byte, Ref)

	// TempKey will return the temporary key which is only valid until the next
	// iteration or until the iterator is closed.
	TempKey() []byte

	// Value will return a buffered value that can be used until released.
	Value() ([]byte, Ref, error)

	// Use will yield the temporary value to the provided function.
	Use(fn func(value []byte) error) error

	// Error will return the error.
	Error() error

	// Close will close the iterator.
	Close() error
}

// Observer is the interface implemented by observers that want to observe the
// stream of instructions processed by the machine.
type Observer interface {
	// Init is called when the source instruction stream has been (re)opened.
	// This happens when the machine starts and whenever the node fails and
	// restarts.
	Init()

	// Process is called repeatedly with every instruction processed by the
	// machine. The implementation must ensure that the function returns as fast
	// as possible as it blocks the execution of other instructions. If false is
	// returned, the observer will be unsubscribed.
	Process(ins Instruction) bool
}
