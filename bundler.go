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
	ch  chan error
	fn  func(error)
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

func (b *bundler) process(ins Instruction, fn func(error)) error {
	// acquire mutex
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	// check if done
	if b.closed {
		return fmt.Errorf("turing: bundler closed")
	}

	// handle async
	if fn != nil {
		// queue instruction
		b.queue <- bundlerItem{
			ins: ins,
			fn:  fn,
		}

		return nil
	}

	// get result channel
	ch := bundlerChanPool.Get().(chan error)
	defer bundlerChanPool.Put(ch)

	// queue instruction
	b.queue <- bundlerItem{
		ins: ins,
		ch:  ch,
	}

	return <-ch
}

func (b *bundler) processor() {
	// ensure done
	defer b.group.Done()

	// prepare list
	list := make([]Instruction, 0, b.opts.batchSize)
	chs := make([]chan error, 0, b.opts.batchSize)
	fns := make([]func(error), 0, b.opts.batchSize)

	for {
		// wait up to 5ms until a full batch is available
		for i := 0; i < 5 && len(b.queue) < b.opts.batchSize; i++ {
			time.Sleep(time.Millisecond)
		}

		// await next item
		item, ok := <-b.queue
		if !ok {
			return
		}

		// add to list
		list = append(list, item.ins)
		if item.ch != nil {
			chs = append(chs, item.ch)
		}
		if item.fn != nil {
			fns = append(fns, item.fn)
		}

		// add buffered instructions while list has room
		for len(b.queue) > 0 && len(list) < cap(list) {
			item, ok := <-b.queue
			if ok {
				list = append(list, item.ins)
				if item.ch != nil {
					chs = append(chs, item.ch)
				}
				if item.fn != nil {
					fns = append(fns, item.fn)
				}
			}
		}

		// call handler
		err := b.opts.handler(list)

		// forward on channels
		for _, ch := range chs {
			ch <- err
		}

		// forward with callbacks
		for _, fn := range fns {
			fn(err)
		}

		// reset lists
		list = list[:0]
		chs = chs[:0]
		fns = fns[:0]
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
