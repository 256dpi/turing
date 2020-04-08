package turing

// Copy will make a copy of the provided slice.
func Copy(dst []byte, src []byte) []byte {
	// allocate if missing
	if dst == nil {
		dst = make([]byte, len(src))
	}

	// copy bytes
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

// TestMachine will start and return a machine for testing purposes.
func TestMachine(instructions ...Instruction) *Machine {
	// create machine
	machine, err := Start(Config{
		Instructions: instructions,
		Standalone:   true,
	})
	if err != nil {
		panic(err)
	}

	return machine
}
