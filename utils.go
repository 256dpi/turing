package turing

import "errors"

// ErrBreak can be returned in walker callbacks to return early.
var ErrBreak = errors.New("break")

// Clone will make a copy of the provided slice.
func Clone(src []byte) []byte {
	// make copy
	dst := make([]byte, len(src))
	copy(dst, src)

	return dst
}

// PrefixRange will compute the lower and upper bound of a prefix range.
func PrefixRange(prefix []byte) ([]byte, []byte) {
	var limit []byte
	for i := len(prefix) - 1; i >= 0; i-- {
		c := prefix[i]
		if c < 0xff {
			limit = make([]byte, i+1)
			copy(limit, prefix)
			limit[i] = c + 1
			break
		}
	}

	return prefix, limit
}

// Test will start and return a machine for testing purposes.
func Test(ins ...Instruction) *Machine {
	// create machine
	machine, err := Start(Config{
		Instructions: ins,
		Standalone:   true,
	})
	if err != nil {
		panic(err)
	}

	return machine
}
