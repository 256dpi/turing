package turing

import (
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/lni/dragonboat/v3/logger"

	"github.com/256dpi/turing/pkg/coding"
	"github.com/256dpi/turing/pkg/semaphore"
)

// ErrDatabaseClosed is returned if the database has been closed.
var ErrDatabaseClosed = errors.New("turing: database closed")

var stateKey = []byte("$state")

type state struct {
	Index uint64
	Batch uint64
	Last  uint16
}

func (s *state) Encode() ([]byte, Ref, error) {
	return coding.Encode(true, func(enc *coding.Encoder) error {
		// encode version
		enc.Uint8(1)

		// encode body
		enc.Uint64(s.Index)
		enc.Uint64(s.Batch)
		enc.Uint16(s.Last)

		return nil
	})
}

func (s *state) Decode(data []byte) error {
	return coding.Decode(data, func(dec *coding.Decoder) error {
		// decode version
		var version uint8
		dec.Uint8(&version)
		if version != 1 {
			return fmt.Errorf("turing: state decode: invalid version")
		}

		// decode body
		dec.Uint64(&s.Index)
		dec.Uint64(&s.Batch)
		dec.Uint16(&s.Last)

		return nil
	})
}

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
	txn.current = nil
	txn.reader = nil
	txn.writer = nil
	txn.closers = 0
	txn.iterators = 0
	txn.effect = 0
	transactionPool.Put(txn)
}

type database struct {
	state    state
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
	fs := config.DatabaseFS()

	// ensure directory
	err := fs.MkdirAll(config.DatabaseDir(), 0700)
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

	// TODO: Allow database tuning.

	// open db
	pdb, err := pebble.Open(config.DatabaseDir(), &pebble.Options{
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

	// get stored state
	value, closer, err := pdb.Get(stateKey)
	if err != nil && err != pebble.ErrNotFound {
		return nil, 0, err
	}

	// parse state if available
	var state state
	if len(value) > 0 {
		// ensure close
		defer closer.Close()

		// parse state
		err = state.Decode(value)
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
		state:    state,
		registry: registry,
		manager:  manager,
		pebble:   pdb,
		options:  options,
		readers:  semaphore.New(config.ConcurrentReaders),
	}

	// init manager
	manager.init()

	return db, state.Index, nil
}

var databaseUpdate = systemMetrics.WithLabelValues("database.update")

func (d *database) update(list []Instruction, index uint64) error {
	// acquire write mutex
	d.write.Lock()
	defer d.write.Unlock()

	// check if closed
	if d.closed {
		return ErrDatabaseClosed
	}

	// check index
	if index != 0 && d.state.Index >= index {
		return fmt.Errorf("turing: database update: already applied index: %d", index)
	}

	// observe
	timer := observe(databaseUpdate)
	defer timer.finish()

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
	for i, ins := range list {
		// skip instruction if already applied
		if index != 0 && d.state.Batch == index && d.state.Last >= uint16(i) {
			continue
		}

		// begin observation
		timer := observe(ins.Describe().observer)

		// check if new transaction is needed for bounded transaction
		effect := ins.Effect()
		if effect > 0 && txn.effect+effect >= MaxEffect {
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
			effectMaxed, err := txn.Execute(ins)
			if err != nil {
				return err
			}

			// commit batch if effect is maxed and start over
			if effectMaxed {
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

			// update state
			d.state.Batch = index
			d.state.Last = uint16(i)

			// encode state
			encodedState, ref, err := d.state.Encode()
			if err != nil {
				return err
			}

			// set state
			err = batch.Set(stateKey, encodedState, nil)
			if err != nil {
				ref.Release()
				return err
			}

			// release
			ref.Release()

			break
		}

		// finish observation
		timer.finish()
	}

	// update state
	d.state.Index = index
	d.state.Batch = 0
	d.state.Last = 0

	// encode state
	encodedState, ref, err := d.state.Encode()
	if err != nil {
		return err
	}

	// ensure release
	defer ref.Release()

	// set state
	err = batch.Set(stateKey, encodedState, nil)
	if err != nil {
		return err
	}

	// commit final batch
	err = batch.Commit(d.options)
	if err != nil {
		return err
	}

	// yield to manager
	for _, instruction := range list {
		d.manager.process(instruction)
	}

	return nil
}

var databaseLookup = systemMetrics.WithLabelValues("database.lookup")

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
	timer1 := observe(databaseLookup)
	defer timer1.finish()

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
	for _, ins := range list {
		// begin observation
		timer := observe(ins.Describe().observer)

		// execute transaction
		_, err := txn.Execute(ins)
		if err != nil {
			return err
		}

		// finish observation
		timer.finish()
	}

	return nil
}

func (d *database) sync() error {
	// TODO: Should we do something?

	return nil
}

var databaseBackup = systemMetrics.WithLabelValues("database.backup")

func (d *database) backup(sink io.Writer) error {
	// acquire read mutex
	d.read.RLock()
	defer d.read.RUnlock()

	// check if closed
	if d.closed {
		return ErrDatabaseClosed
	}

	// observe
	timer := observe(databaseBackup)
	defer timer.finish()

	// TODO: Implement.

	return nil
}

var databaseRestore = systemMetrics.WithLabelValues("database.restore")

func (d *database) restore(source io.Reader) error {
	// acquire write mutex
	d.write.Lock()
	defer d.write.Unlock()

	// check if closed
	if d.closed {
		return ErrDatabaseClosed
	}

	// observe
	timer := observe(databaseRestore)
	defer timer.finish()

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
