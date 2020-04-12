package main

import (
	"encoding/binary"
)

func encodeInt(num uint64) []byte {
	// encode int (allocating a 8 byte slice is faster than using a pool)
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, num)

	return buf
}

func decodeInt(buf []byte) uint64 {
	return binary.BigEndian.Uint64(buf)
}
