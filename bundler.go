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
	batch   int
	queue   chan item
	handler func([]Instruction) error
	mutex   sync.RWMutex
	group   sync.WaitGroup
	closed  bool
}

func newBundler(queue, batch, concurrency int, handler func([]Instruction) error) *bundler {
	// prepare bundler
	c := &bundler{
		batch:   batch,
		queue:   make(chan item, queue),
		handler: handler,
	}

	// run processors
	c.group.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go c.processor()
	}

	return c
}

func (b *bundler) process(instruction Instruction) error {
	// acquire mutex
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	// check if done
	if b.closed {
		return fmt.Errorf("closed")
	}

	// prepare result channel
	result := make(chan error, 1)

	// queue instruction
	b.queue <- item{
		ins: instruction,
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
	list := make([]Instruction, 0, b.batch)
	acks := make([]func(error), 0, b.batch)

	for {
		// await next instruction
		item, ok := <-b.queue
		if !ok {
			return
		}

		// add to list
		list = append(list, item.ins)
		acks = append(acks, item.ack)

		// add buffered instructions
		for len(b.queue) > 0 && cap(list) > 0 {
			item, ok := <-b.queue
			if ok {
				list = append(list, item.ins)
				acks = append(acks, item.ack)
			}
		}

		// call handler and forward result
		err := b.handler(list)
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
