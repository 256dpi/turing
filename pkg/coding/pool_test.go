package coding

import (
	"strconv"
	"testing"
)

var classes = []int{8, 16, 32, 64, 128, 256, 512, 1024, 2048}

func BenchmarkPool(b *testing.B) {
	for _, class := range classes {
		b.Run(strconv.Itoa(class), func(b *testing.B) {
			list := make([][]byte, b.N)

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				buf, ref := Borrow(class)
				list[i] = buf
				ref.Release()
			}
		})
	}
}

func BenchmarkMake(b *testing.B) {
	for _, class := range classes {
		b.Run(strconv.Itoa(class), func(b *testing.B) {
			list := make([][]byte, b.N)

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				list[i] = make([]byte, class)
			}
		})
	}
}
