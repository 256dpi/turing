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
	updates  chan item
	lookups  chan item
	database *database
	mutex    sync.RWMutex
	wg       sync.WaitGroup
	closed   bool
}

func newController(config Config, database *database) *controller {
	// TODO: Allow configuring queue size?

	// prepare controller
	c := &controller{
		updates:  make(chan item, 1000),
		lookups:  make(chan item, 1000),
		database: database,
	}

	// run update processor
	c.wg.Add(1)
	go c.processor(c.updates, true)

	// run lookup processors
	c.wg.Add(config.ConcurrentReaders)
	for i := 0; i < config.ConcurrentReaders; i++ {
		go c.processor(c.lookups, false)
	}

	return c
}

func (c *controller) update(instruction Instruction) error {
	return c.queue(instruction, c.updates)
}

func (c *controller) lookup(instruction Instruction) error {
	return c.queue(instruction, c.lookups)
}

func (c *controller) queue(instruction Instruction, queue chan item) error {
	// acquire mutex
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	// check if done
	if c.closed {
		return fmt.Errorf("closed")
	}

	// prepare result channel
	result := make(chan error, 1)

	// queue instruction
	queue <- item{
		ins: instruction,
		ack: func(err error) {
			result <- err
		},
	}

	return <-result
}

func (c *controller) processor(queue chan item, update bool) {
	// TODO: Allow configuring list sizes?

	// prepare list
	list := make([]Instruction, 0, 200)
	acks := make([]func(error), 0, 200)

	for {
		// await next instruction
		item, ok := <-queue
		if !ok {
			c.wg.Done()
			return
		}

		// add to list
		list = append(list, item.ins)
		acks = append(acks, item.ack)

		// add buffered instructions
		for len(queue) > 0 && cap(list) > 0 {
			item, ok := <-queue
			if ok {
				list = append(list, item.ins)
				acks = append(acks, item.ack)
			}
		}

		// perform update or lookup
		var err error
		if update {
			err = c.database.update(list, nil)
		} else {
			err = c.database.lookup(list)
		}

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

	// close queues
	close(c.updates)
	close(c.lookups)

	// wait until done
	c.wg.Wait()

	// set flag
	c.closed = true
}
