package turing

// Copy will make a copy of the provided slice.
func Copy(dst []byte, src []byte) []byte {
	if dst == nil {
		dst = make([]byte, len(src))
	}

	copy(dst, src)

	return dst
}
