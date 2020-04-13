package turing

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/lni/dragonboat/v3/logger"
	"github.com/lni/dragonboat/v3/statemachine"

	"github.com/256dpi/turing/tape"
)

// ErrDatabaseClosed is returned if the database has been closed.
var ErrDatabaseClosed = errors.New("turing: database closed")

var stateKey = []byte("$state")
var syncKey = []byte("$sync")

type database struct {
	state    tape.State
	registry *registry
	manager  *manager
	pebble   *pebble.DB
	options  *pebble.WriteOptions
	write    sync.Mutex
	read     sync.RWMutex
	readers  chan struct{}
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
	var state tape.State
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
		readers:  make(chan struct{}, config.ConcurrentReaders),
	}

	// fill tokens
	for i := 0; i < cap(db.readers); i++ {
		db.readers <- struct{}{}
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

	// verify state
	if index != 0 && d.state.Index >= index {
		return fmt.Errorf("turing: database update: already applied index: %d", index)
	}

	// observe
	timer := observe(databaseUpdate)
	defer timer.finish()

	// create initial batch
	batch := d.pebble.NewIndexedBatch()

	// prepare transaction
	txn := newTransaction()
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
	// acquire reader token
	<-d.readers
	defer func() {
		d.readers <- struct{}{}
	}()

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
	txn := newTransaction()
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
	// get current time
	now := []byte(time.Now().UTC().Format(time.RFC3339))

	// write sync key to force sync
	err := d.pebble.Set(syncKey, now, pebble.Sync)
	if err != nil {
		return err
	}

	return nil
}

func (d *database) snapshot() (*pebble.Snapshot, error) {
	// acquire read mutex
	d.read.RLock()
	defer d.read.RUnlock()

	// check if closed
	if d.closed {
		return nil, ErrDatabaseClosed
	}

	// make snapshot
	snapshot := d.pebble.NewSnapshot()

	return snapshot, nil
}

var databaseBackup = systemMetrics.WithLabelValues("database.backup")

func (d *database) backup(snapshot *pebble.Snapshot, sink io.Writer, stopped <-chan struct{}) error {
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

	// create iterator
	iter := snapshot.NewIter(&pebble.IterOptions{})
	defer iter.Close()

	// prepare buffer
	buf := make([]byte, 8)

	// iterate over all keys
	for iter.First(); iter.Valid(); iter.Next() {
		// write key length
		binary.BigEndian.PutUint64(buf, uint64(len(iter.Key())))
		_, err := sink.Write(buf)
		if err != nil {
			return err
		}

		// write key
		_, err = sink.Write(iter.Key())
		if err != nil {
			return err
		}

		// write value length
		binary.BigEndian.PutUint64(buf, uint64(len(iter.Value())))
		_, err = sink.Write(buf)
		if err != nil {
			return err
		}

		// write value
		_, err = sink.Write(iter.Value())
		if err != nil {
			return err
		}

		// check stopped
		select {
		case <-stopped:
			return statemachine.ErrSnapshotStopped
		default:
		}
	}

	// close iterator
	err := iter.Close()
	if err != nil {
		return err
	}

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

	// TODO: Delete all current data?

	// prepare buffers
	lenBuf := make([]byte, 8)
	keyBuf := make([]byte, 1<<14) // ~16KB
	valBuf := make([]byte, 1<<24) // ~16MB

	// read from source
	for {
		// read key length
		_, err := io.ReadFull(source, lenBuf)
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		// decode key length
		keyLen := int(binary.BigEndian.Uint64(lenBuf))

		// grow slice if too small
		if cap(keyBuf) < keyLen {
			keyBuf = make([]byte, keyLen)
		}

		// read key
		_, err = io.ReadFull(source, keyBuf[:keyLen])
		if err != nil {
			return err
		}

		// read value length
		_, err = io.ReadFull(source, lenBuf)
		if err != nil {
			return err
		}

		// decode value length
		valLen := int(binary.BigEndian.Uint64(lenBuf))

		// grow slice if too small
		if cap(valBuf) < valLen {
			valBuf = make([]byte, valLen)
		}

		// read key
		_, err = io.ReadFull(source, valBuf[:valLen])
		if err != nil {
			return err
		}

		// set key
		err = d.pebble.Set(keyBuf[:keyLen], valBuf[:valLen], pebble.NoSync)
		if err != nil {
			return err
		}
	}
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
