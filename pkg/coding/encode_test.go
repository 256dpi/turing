package coding

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncode(t *testing.T) {
	res := Encode(func(enc *Encoder) {
		enc.Bool(true)
		enc.Int(7)
		enc.String("foo")
		enc.Bytes([]byte("bar"))
		enc.Uint(512)
		enc.Tail([]byte("baz"))
	})
	assert.Equal(t, []byte("\x01\x0e\x03foo\x03bar\x80\x04baz"), res)
}

func BenchmarkEncode(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		Encode(func(enc *Encoder) {
			enc.Bool(true)
			enc.Int(7)
			enc.String("foo")
			enc.Bytes([]byte("bar"))
			enc.Uint(512)
			enc.Tail([]byte("baz"))
		})
	}
}
