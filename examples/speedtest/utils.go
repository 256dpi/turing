package main

import (
	"encoding/binary"
)

func encodeNum(num uint64) []byte {
	buf := make([]byte, 8) // allocating a 8 byte slice is faster than using a pool
	binary.BigEndian.PutUint64(buf, num)
	return buf
}

func decodeNum(buf []byte) uint64 {
	return binary.BigEndian.Uint64(buf)
}
