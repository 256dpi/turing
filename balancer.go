package turing

import "github.com/256dpi/turing/pkg/semaphore"

// TODO: The balancer should be improved to ensure a good balance between read
//  and write.

type balancer struct {
	read  *semaphore.Semaphore
	write *semaphore.Semaphore
}

func newBalancer(reads, writes int) *balancer {
	// prepare read
	var read *semaphore.Semaphore
	if reads > 0 {
		read = semaphore.New(reads)
	}

	// prepare write
	var write *semaphore.Semaphore
	if writes > 0 {
		write = semaphore.New(writes)
	}

	return &balancer{
		read:  read,
		write: write,
	}
}

func (b *balancer) get(write bool) {
	// acquire token
	if write {
		if b.write != nil {
			b.write.Acquire(nil, 0)
		}
	} else {
		if b.read != nil {
			b.read.Acquire(nil, 0)
		}
	}
}

func (b *balancer) put(write bool) {
	// release token
	if write {
		if b.write != nil {
			b.write.Release()
		}
	} else {
		if b.read != nil {
			b.read.Release()
		}
	}
}
