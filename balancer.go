package turing

// TODO: The balancer should be improved to ensure a good balance between read
//  and writes.

type balancer struct {
	read  chan struct{}
	write chan struct{}
}

func newBalancer(reads, writes int) *balancer {
	// prepare read bucket
	var read chan struct{}
	if reads > 0 {
		read = make(chan struct{}, reads)
		for i := 0; i < reads; i++ {
			read <- struct{}{}
		}
	}

	// prepare write bucket
	var write chan struct{}
	if writes > 0 {
		write = make(chan struct{}, writes)
		for i := 0; i < writes; i++ {
			write <- struct{}{}
		}
	}

	return &balancer{
		read:  read,
		write: write,
	}
}

func (b *balancer) get(write bool) {
	// get token
	if write {
		if b.write != nil {
			<-b.write
		}
	} else {
		if b.read != nil {
			<-b.read
		}
	}
}

func (b *balancer) put(write bool) {
	// put token
	if write {
		if b.write != nil {
			select {
			case b.write <- struct{}{}:
			default:
			}
		}
	} else {
		if b.read != nil {
			select {
			case b.read <- struct{}{}:
			default:
			}
		}
	}
}
