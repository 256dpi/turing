package main

import (
	"encoding/binary"
)

func encodeInt(num int64) []byte {
	// encode int (allocating a 10 byte slice is faster than using a pool)
	buf := make([]byte, 10)
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
