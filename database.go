package turing

import (
	"errors"
	"io"
	"strconv"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/lni/dragonboat/v3/logger"

	"github.com/256dpi/turing/pkg/semaphore"
)

// ErrDatabaseClosed is returned if the database has been closed.
var ErrDatabaseClosed = errors.New("turing: database closed")

var indexKey = []byte("$index")

var transactionPool = sync.Pool{
	New: func() interface{} {
		return &Transaction{}
	},
}

func obtainTransaction() *Transaction {
	return transactionPool.Get().(*Transaction)
}

func recycleTransaction(txn *Transaction) {
	txn.registry = nil
	txn.reader = nil
	txn.writer = nil
	txn.closers = 0
	txn.effect = 0
	transactionPool.Put(txn)
}

type database struct {
	registry *registry
	manager  *manager
	pebble   *pebble.DB
	options  *pebble.WriteOptions
	write    sync.Mutex
	read     sync.RWMutex
	readers  *semaphore.Semaphore
	closed   bool
}

func openDatabase(config Config, registry *registry, manager *manager) (*database, uint64, error) {
	// get fs
	fs := config.dbFS()

	// ensure directory
	err := fs.MkdirAll(config.dbDir(), 0700)
	if err != nil {
		return nil, 0, err
	}

	// prepare logger
	lgr := &extendedLogger{ILogger: logger.GetLogger("pebble")}

	// create cache
	cache := pebble.NewCache(64 << 20) // 64MB

	// prepare merger
	merger := &pebble.Merger{
		Name: "turing", // DO NOT CHANGE!
		Merge: func(key, value []byte) (pebble.ValueMerger, error) {
			return newMerger(registry, value), nil
		},
	}

	// open db
	pdb, err := pebble.Open(config.dbDir(), &pebble.Options{
		FS:                          fs,
		Cache:                       cache,
		Merger:                      merger,
		MemTableSize:                16 << 20, // 16MB
		MemTableStopWritesThreshold: 4,
		MinFlushRate:                4 << 20, // 4MB
		L0CompactionThreshold:       2,
		L0StopWritesThreshold:       16,
		LBaseMaxBytes:               16 << 20, // 16MB
		Levels: []pebble.LevelOptions{{
			BlockSize: 32 << 10, // 32KB
		}},
		Logger:        lgr,
		EventListener: pebble.MakeLoggingEventListener(lgr),
	})
	if err != nil {
		return nil, 0, err
	}

	// unref cache
	cache.Unref()

	// get last committed index
	value, closer, err := pdb.Get(indexKey)
	if err != nil && err != pebble.ErrNotFound {
		return nil, 0, err
	}

	// parse index if available
	var index uint64
	if value != nil {
		// ensure close
		defer closer.Close()

		// parse value
		index, err = strconv.ParseUint(string(value), 10, 64)
		if err != nil {
			return nil, 0, err
		}

		// close value
		err = closer.Close()
		if err != nil {
			return nil, 0, err
		}
	}

	// prepare options
	options := &pebble.WriteOptions{
		Sync: config.Standalone,
	}

	// create database
	db := &database{
		registry: registry,
		manager:  manager,
		pebble:   pdb,
		options:  options,
		readers:  semaphore.New(config.ConcurrentReaders),
	}

	// init manager
	manager.init()

	return db, index, nil
}

func (d *database) update(list []Instruction, indexes []uint64) error {
	// acquire write mutex
	d.write.Lock()
	defer d.write.Unlock()

	// check if closed
	if d.closed {
		return ErrDatabaseClosed
	}

	// observe
	timer := observe(operationMetrics, "database.update")
	defer timer.ObserveDuration()

	// create initial batch
	batch := d.pebble.NewIndexedBatch()

	// create initial transaction
	txn := obtainTransaction()
	txn.registry = d.registry
	txn.reader = batch
	txn.writer = batch

	// ensure recycle
	defer recycleTransaction(txn)

	// execute all instructions
	for i, instruction := range list {
		// get description
		desc := instruction.Describe()

		// begin observation
		timer := observe(instructionMetrics, desc.Name)

		// check if new transaction is needed for bounded transaction
		if desc.Effect > 0 && txn.effect+desc.Effect >= MaxEffect {
			// commit current batch
			err := batch.Commit(d.options)
			if err != nil {
				return err
			}

			// create new batch
			batch = d.pebble.NewIndexedBatch()

			// reset transaction
			txn.reader = batch
			txn.writer = batch
			txn.effect = 0
		}

		for {
			// execute transaction
			exhausted, err := txn.execute(instruction)
			if err != nil {
				return err
			}

			// commit batch if exhausted and start over
			if exhausted {
				// commit current batch
				err := batch.Commit(d.options)
				if err != nil {
					return err
				}

				// create new batch
				batch = d.pebble.NewIndexedBatch()

				// reset transaction
				txn.reader = batch
				txn.writer = batch
				txn.effect = 0

				continue
			}

			// set index if available
			if indexes != nil {
				err = batch.Set(indexKey, []byte(strconv.FormatUint(indexes[i], 10)), nil)
				if err != nil {
					return err
				}
			}

			break
		}

		// finish observation
		timer.ObserveDuration()
	}

	// commit final batch
	err := batch.Commit(d.options)
	if err != nil {
		return err
	}

	// yield to manager
	for _, instruction := range list {
		d.manager.process(instruction)
	}

	return nil
}

func (d *database) lookup(list []Instruction) error {
	// get reader token
	d.readers.Acquire(nil, 0)
	defer d.readers.Release()

	// acquire read mutex
	d.read.RLock()
	defer d.read.RUnlock()

	// check if closed
	if d.closed {
		return ErrDatabaseClosed
	}

	// observe
	timer1 := observe(operationMetrics, "database.lookup")
	defer timer1.ObserveDuration()

	// get snapshot
	snapshot := d.pebble.NewSnapshot()
	defer snapshot.Close()

	// prepare transaction
	txn := obtainTransaction()
	txn.registry = d.registry
	txn.reader = snapshot

	// ensure recycle
	defer recycleTransaction(txn)

	// execute instruction
	for _, instruction := range list {
		// begin observation
		timer := observe(instructionMetrics, instruction.Describe().Name)

		// execute transaction
		_, err := txn.execute(instruction)
		if err != nil {
			return err
		}

		// finish observation
		timer.ObserveDuration()
	}

	return nil
}

func (d *database) sync() error {
	// TODO: Should we do something?

	return nil
}

func (d *database) backup(sink io.Writer) error {
	// acquire read mutex
	d.read.RLock()
	defer d.read.RUnlock()

	// check if closed
	if d.closed {
		return ErrDatabaseClosed
	}

	// observe
	timer := observe(operationMetrics, "database.backup")
	defer timer.ObserveDuration()

	// TODO: Implement.

	return nil
}

func (d *database) restore(source io.Reader) error {
	// acquire write mutex
	d.write.Lock()
	defer d.write.Unlock()

	// check if closed
	if d.closed {
		return ErrDatabaseClosed
	}

	// observe
	timer := observe(operationMetrics, "database.restore")
	defer timer.ObserveDuration()

	// TODO: Implement.

	return nil
}

func (d *database) close() error {
	// acquire read mutex
	d.read.Lock()
	defer d.read.Unlock()

	// acquire write mutex
	d.write.Lock()
	defer d.write.Unlock()

	// check if closed
	if d.closed {
		return ErrDatabaseClosed
	}

	// close database
	err := d.pebble.Close()
	if err != nil {
		return err
	}

	// set flag
	d.closed = true

	return nil
}
