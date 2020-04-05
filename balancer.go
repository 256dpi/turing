package turing

// TODO: The balancer should be improved to ensure a good balance between read
// and writes.

type balancer struct {
	read  chan struct{}
	write chan struct{}
}

func newBalancer(read, write int) *balancer {
	// prepare
	b := &balancer{
		read:  make(chan struct{}, read),
		write: make(chan struct{}, write),
	}

	// fill read tokens
	for i := 0; i < cap(b.read); i++ {
		b.read <- struct{}{}
	}

	// fill write tokens
	for i := 0; i < cap(b.write); i++ {
		b.write <- struct{}{}
	}

	return b
}

func (b *balancer) get(write bool) {
	// get token
	if write {
		<-b.write
	} else {
		<-b.read
	}
}

func (b *balancer) put(write bool) {
	// put token
	if write {
		select {
		case b.write <- struct{}{}:
		default:
		}
	} else {
		select {
		case b.read <- struct{}{}:
		default:
		}
	}
}
