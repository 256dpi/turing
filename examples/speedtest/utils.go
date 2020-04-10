package main

import (
	"encoding/binary"
)

func encodeInt(num int64) []byte {
	// borrow slice
	buf := make([]byte, 10)

	// encode int
	n := binary.PutVarint(buf, num)
	if n <= 0 {
		panic("encode error")
	}

	// resize
	buf = buf[:n]

	return buf
}

func decodeInt(buf []byte) int64 {
	// decode int
	num, n := binary.Varint(buf)
	if n <= 0 {
		panic("decode error")
	}

	return num
}
