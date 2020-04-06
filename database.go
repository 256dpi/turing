package turing

import (
	"fmt"
	"io"
	"strconv"

	"github.com/cockroachdb/pebble"
	"github.com/lni/dragonboat/v3/logger"
)

var indexKey = []byte("$index")

type database struct {
	pebble  *pebble.DB
	manager *manager
}

func openDatabase(config Config, manager *manager) (*database, uint64, error) {
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

	// open db
	pdb, err := pebble.Open(config.dbDir(), &pebble.Options{
		FS:                          fs,
		Cache:                       cache,
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

	// prepare index
	var index uint64

	// get last committed index
	value, closer, err := pdb.Get(indexKey)
	if err != nil && err != pebble.ErrNotFound {
		return nil, 0, err
	}

	// parse index if available
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

	// create database
	db := &database{
		pebble:  pdb,
		manager: manager,
	}

	// init manager
	manager.init()

	return db, index, nil
}

func (d *database) update(list []Instruction, indexes []uint64) error {
	// observe
	timer := observe(operationMetrics, "database.update")
	defer timer.ObserveDuration()

	// count batch size
	getObserver(databaseMetrics, "batch_length").Observe(float64(len(list)))

	// create initial batch
	batch := d.pebble.NewIndexedBatch()

	// create initial transaction
	txn := obtainTxn()
	txn.reader = batch
	txn.writer = batch

	// ensure recycle
	defer recycleTxn(txn)

	// prepare counters
	transactionCount := 1

	// execute all instructions
	for i, instruction := range list {
		// begin observation
		timer := observe(instructionMetrics, instruction.Describe().Name)

		// get estimated effect of instruction
		estimatedEffect := instruction.Describe().Effect

		// check if new transaction is needed for bounded transaction
		if estimatedEffect > 0 && txn.effect+estimatedEffect >= MaxEffect {
			// commit current batch
			err := batch.Commit(pebble.NoSync)
			if err != nil {
				return err
			}

			// create new batch
			batch = d.pebble.NewIndexedBatch()

			// reset transaction
			txn.reader = batch
			txn.writer = batch
			txn.effect = 0
			txn.closers = 0

			// update counters
			transactionCount++
		}

		for {
			// execute transaction
			var maxed bool
			err := instruction.Execute(txn)
			if err == ErrMaxEffect {
				maxed = true
			} else if err != nil {
				return err
			}

			// check closers
			if txn.closers != 0 {
				return fmt.Errorf("unclosed values after instruction execution")
			}

			// commit batch if maxed out and start over
			if maxed {
				// commit current batch (without index)
				err := batch.Commit(pebble.NoSync)
				if err != nil {
					return err
				}

				// create new batch
				batch = d.pebble.NewIndexedBatch()

				// reset transaction
				txn.reader = batch
				txn.writer = batch
				txn.effect = 0
				txn.closers = 0

				// update counters
				transactionCount++

				continue
			}

			// set index
			err = batch.Set(indexKey, []byte(strconv.FormatUint(indexes[i], 10)), nil)
			if err != nil {
				return err
			}

			break
		}

		// finish observation
		timer.ObserveDuration()
	}

	// commit final batch
	err := batch.Commit(pebble.NoSync)
	if err != nil {
		return err
	}

	// yield to manager
	for _, instruction := range list {
		d.manager.process(instruction)
	}

	// count transaction count
	getObserver(databaseMetrics, "transaction_count").Observe(float64(transactionCount))

	return nil
}

func (d *database) lookup(instruction Instruction) error {
	// observe
	timer1 := observe(operationMetrics, "database.lookup")
	defer timer1.ObserveDuration()

	// observe
	timer2 := observe(instructionMetrics, instruction.Describe().Name)
	defer timer2.ObserveDuration()

	// prepare transaction
	txn := obtainTxn()
	txn.reader = d.pebble

	// ensure recycle
	defer recycleTxn(txn)

	// execute instruction
	err := instruction.Execute(txn)
	if err != nil {
		return err
	}

	// check closers
	if txn.closers != 0 {
		return fmt.Errorf("unclosed values after instruction execution")
	}

	return nil
}

func (d *database) sync() error {
	// observe
	timer := observe(operationMetrics, "database.sync")
	defer timer.ObserveDuration()

	// flush database
	_, err := d.pebble.AsyncFlush()
	if err != nil {
		return err
	}

	return nil
}

func (d *database) backup(sink io.Writer) error {
	// observe
	timer := observe(operationMetrics, "database.backup")
	defer timer.ObserveDuration()

	// TODO: Implement.

	return nil
}

func (d *database) restore(source io.Reader) error {
	// observe
	timer := observe(operationMetrics, "database.restore")
	defer timer.ObserveDuration()

	// TODO: Implement.

	return nil
}

func (d *database) close() error {
	// close database
	err := d.pebble.Close()
	if err != nil {
		return err
	}

	return nil
}
