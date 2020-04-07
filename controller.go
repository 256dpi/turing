package turing

import (
	"fmt"
	"sync"
)

type item struct {
	ins Instruction
	ack func(error)
}

type controller struct {
	queue    chan item
	database *database
	mutex    sync.RWMutex
	done     chan struct{}
}

func newController(database *database) *controller {
	// TODO: Allow configuring queue size?

	// prepare controller
	c := &controller{
		queue:    make(chan item, 1000),
		database: database,
		done:     make(chan struct{}),
	}

	// run processor
	go c.processor()

	return c
}

func (c *controller) update(instruction Instruction) error {
	// acquire mutex
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	// check if done
	if c.done == nil {
		return fmt.Errorf("closed")
	}

	// prepare result channel
	result := make(chan error, 1)

	// queue instruction
	c.queue <- item{
		ins: instruction,
		ack: func(err error) {
			result <- err
		},
	}

	return <-result
}

func (c *controller) processor() {
	// TODO: Allow configuring list sizes?

	// prepare list
	list := make([]Instruction, 0, 200)
	acks := make([]func(error), 0, 200)

	for {
		// await next instruction
		item, ok := <-c.queue
		if !ok {
			close(c.done)
			return
		}

		// add to list
		list = append(list, item.ins)
		acks = append(acks, item.ack)

		// add buffered instructions
		for len(c.queue) > 0 && cap(list) > 0 {
			item, ok := <-c.queue
			if ok {
				list = append(list, item.ins)
				acks = append(acks, item.ack)
			}
		}

		// perform update
		err := c.database.update(list, nil)

		// forward results
		for _, ack := range acks {
			ack(err)
		}

		// reset lists
		list = list[:0]
		acks = acks[:0]
	}
}

func (c *controller) close() {
	// acquire mutex
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// close queue
	close(c.queue)

	// wait until done
	<-c.done
	c.done = nil
}
