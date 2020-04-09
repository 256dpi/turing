package coding

import (
	"sync"
)

const size = 1 << 13 // 8KB

var pool = sync.Pool{
	New: func() interface{} {
		return &Ref{}
	},
}

// Ref is reference to a borrowed slice.
type Ref struct {
	array [size]byte
	done  bool
}

// Release will release the slice.
func (r *Ref) Release() {
	// return if unavailable
	if r == nil || r.done {
		return
	}

	// return
	pool.Put(r)
	r.done = true
}

// Borrow will return a slice that has at least the specified length. If the
// requested length is unavailable a slice will be allocated. To recycle the
// slice, it must be released by calling Release() on the returned ref value.
// Always release any returned value, event if the slice grows it is possible
// to return the originally requested slice.
func Borrow(len int) ([]byte, *Ref) {
	// allocate if too long
	if len > size {
		return make([]byte, len), nil
	}

	// otherwise get from pool
	ref := pool.Get().(*Ref)
	ref.done = false

	return ref.array[0:size], ref
}
