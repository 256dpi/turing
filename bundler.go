package turing

import (
	"fmt"
	"sync"
	"time"
)

type bundlerOptions struct {
	queueSize   int
	batchSize   int
	concurrency int
	handler     func([]Instruction) error
}

type bundlerItem struct {
	ins Instruction
	err chan error
}

type bundler struct {
	opts   bundlerOptions
	queue  chan bundlerItem
	mutex  sync.RWMutex
	group  sync.WaitGroup
	closed bool
}

func newBundler(opts bundlerOptions) *bundler {
	// prepare bundler
	c := &bundler{
		opts:  opts,
		queue: make(chan bundlerItem, opts.queueSize),
	}

	// run processors
	c.group.Add(opts.concurrency)
	for i := 0; i < opts.concurrency; i++ {
		go c.processor()
	}

	return c
}

var bundlerChanPool = sync.Pool{
	New: func() interface{} {
		return make(chan error, 1)
	},
}

func (b *bundler) process(ins Instruction) error {
	// acquire mutex
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	// check if done
	if b.closed {
		return fmt.Errorf("turing: bundler closed")
	}

	// get result channel
	ch := bundlerChanPool.Get().(chan error)
	defer bundlerChanPool.Put(ch)

	// queue instruction
	b.queue <- bundlerItem{
		ins: ins,
		err: ch,
	}

	return <-ch
}

func (b *bundler) processor() {
	// ensure done
	defer b.group.Done()

	// prepare list
	list := make([]Instruction, 0, b.opts.batchSize)
	chs := make([]chan error, 0, b.opts.batchSize)

	for {
		// wait 0.1ms if no full batch is available yet
		if len(b.queue) < b.opts.batchSize {
			time.Sleep(time.Millisecond / 10)
		}

		// await next item
		item, ok := <-b.queue
		if !ok {
			return
		}

		// add to list
		list = append(list, item.ins)
		chs = append(chs, item.err)

		// add buffered instructions while list has room
		for len(b.queue) > 0 && len(list) < cap(list) {
			item, ok := <-b.queue
			if ok {
				list = append(list, item.ins)
				chs = append(chs, item.err)
			}
		}

		// call handler and forward result
		err := b.opts.handler(list)
		for _, ch := range chs {
			ch <- err
		}

		// reset lists
		list = list[:0]
		chs = chs[:0]
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
