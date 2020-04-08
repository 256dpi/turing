package turing

import (
	"fmt"
	"sync"
)

type item struct {
	ins Instruction
	ack func(error)
}

type bundler struct {
	opts   bundlerOptions
	queue  chan item
	mutex  sync.RWMutex
	group  sync.WaitGroup
	closed bool
}

type bundlerOptions struct {
	queueSize   int
	batchSize   int
	concurrency int
	handler     func([]Instruction) error
}

func newBundler(opts bundlerOptions) *bundler {
	// prepare bundler
	c := &bundler{
		opts:  opts,
		queue: make(chan item, opts.queueSize),
	}

	// run processors
	c.group.Add(opts.concurrency)
	for i := 0; i < opts.concurrency; i++ {
		go c.processor()
	}

	return c
}

func (b *bundler) process(ins Instruction) error {
	// acquire mutex
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	// check if done
	if b.closed {
		return fmt.Errorf("turing: bundler closed")
	}

	// prepare result channel
	result := make(chan error, 1)

	// queue instruction
	b.queue <- item{
		ins: ins,
		ack: func(err error) {
			result <- err
		},
	}

	return <-result
}

func (b *bundler) processor() {
	// ensure done
	defer b.group.Done()

	// prepare list
	list := make([]Instruction, 0, b.opts.batchSize)
	acks := make([]func(error), 0, b.opts.batchSize)

	for {
		// await next instruction
		item, ok := <-b.queue
		if !ok {
			return
		}

		// add to list
		list = append(list, item.ins)
		acks = append(acks, item.ack)

		// add buffered instructions if list has room
		for len(b.queue) > 0 && len(list) < b.opts.batchSize {
			item, ok := <-b.queue
			if ok {
				list = append(list, item.ins)
				acks = append(acks, item.ack)
			}
		}

		// call handler and forward result
		err := b.opts.handler(list)
		for _, ack := range acks {
			ack(err)
		}

		// reset lists
		list = list[:0]
		acks = acks[:0]
	}
}

func (b *bundler) close() {
	// acquire mutex
	b.mutex.Lock()
	defer b.mutex.Unlock()

	// close queue
	close(b.queue)

	// await returns
	b.group.Wait()

	// set flag
	b.closed = true
}
