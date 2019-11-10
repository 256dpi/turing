package turing

import (
	"io"
	"os"
	"strconv"

	"github.com/cockroachdb/pebble"
	"github.com/lni/dragonboat/v3/logger"
)

var indexKey = []byte("$index")

type database struct {
	pebble  *pebble.DB
	manager *manager
}

func openDatabase(dir string, manager *manager) (*database, uint64, error) {
	// observe
	defer observe(operationMetrics.WithLabelValues("database.open"))()

	// ensure directory
	err := os.MkdirAll(dir, 0700)
	if err != nil {
		return nil, 0, err
	}

	// prepare logger
	lgr := &extendedLogger{ILogger: logger.GetLogger("pebble")}

	// open db
	pdb, err := pebble.Open(dir, &pebble.Options{
		Cache:                       pebble.NewCache(64 << 20),
		MemTableSize:                16 << 20,
		MemTableStopWritesThreshold: 4,
		MinFlushRate:                4 << 20,
		L0CompactionThreshold:       2,
		L0StopWritesThreshold:       16,
		LBaseMaxBytes:               16 << 20,
		Levels: []pebble.LevelOptions{{
			BlockSize: 32 << 10, // 32KB
		}},
		Logger:        lgr,
		EventListener: pebble.MakeLoggingEventListener(lgr),
	})
	if err != nil {
		return nil, 0, err
	}

	// prepare index
	var index uint64

	// get last committed index
	value, err := pdb.Get(indexKey)
	if err != nil && err != pebble.ErrNotFound {
		return nil, 0, err
	}

	// parse index if available
	if value != nil {
		index, err = strconv.ParseUint(string(value), 10, 64)
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

func (d *database) update(list []Instruction, index uint64) error {
	// observe
	defer observe(operationMetrics.WithLabelValues("database.update"))()

	// count batch size
	databaseMetrics.WithLabelValues("batch_length").Observe(float64(len(list)))

	// create initial batch
	batch := d.pebble.NewIndexedBatch()

	// create initial transaction
	txn := &Transaction{
		reader: batch,
		writer: batch,
	}

	// prepare counters
	transactionCount := 1

	// execute all instructions
	for _, instruction := range list {
		// begin observation
		finish := observe(instructionMetrics.WithLabelValues(instruction.Describe().Name))

		// get estimated effect of instruction
		estimatedEffect := instruction.Describe().Effect

		// check if new transaction is needed for bounded transaction
		if estimatedEffect > 0 && txn.effect+estimatedEffect >= MaxEffect {
			// commit current batch
			err := batch.Commit(nil)
			if err != nil {
				return err
			}

			// create new batch
			batch = d.pebble.NewIndexedBatch()

			// create new transaction
			txn = &Transaction{
				reader: batch,
				writer: batch,
			}

			// update counters
			transactionCount++
		}

		// execute transaction
		for {
			err := instruction.Execute(txn)
			if err == ErrMaxEffect {
				// commit current batch
				err := batch.Commit(nil)
				if err != nil {
					return err
				}

				// create new batch
				batch = d.pebble.NewIndexedBatch()

				// create new transaction
				txn = &Transaction{
					reader: batch,
					writer: batch,
				}

				// update counters
				transactionCount++

				continue
			}
			if err != nil {
				return err
			}

			break
		}

		// finish observation
		finish()
	}

	// TODO: Every batch should set the correct index. Otherwise we may execute
	//  and instruction multiple times if the last batch (with the index) fails.

	// set index
	err := batch.Set(indexKey, []byte(strconv.FormatUint(index, 10)), nil)
	if err != nil {
		return err
	}

	// commit final batch
	err = batch.Commit(nil)
	if err != nil {
		return err
	}

	// yield to manager
	for _, instruction := range list {
		d.manager.process(instruction)
	}

	// count transaction count
	databaseMetrics.WithLabelValues("transaction_count").Observe(float64(transactionCount))

	return nil
}

func (d *database) lookup(instruction Instruction) error {
	// observe
	defer observe(operationMetrics.WithLabelValues("database.lookup"))()

	// observe
	defer observe(instructionMetrics.WithLabelValues(instruction.Describe().Name))()

	// execute instruction
	err := instruction.Execute(&Transaction{reader: d.pebble})
	if err != nil {
		return err
	}

	return nil
}

func (d *database) sync() error {
	// observe
	defer observe(operationMetrics.WithLabelValues("database.sync"))()

	// flush database
	_, err := d.pebble.AsyncFlush()
	if err != nil {
		return err
	}

	return nil
}

func (d *database) backup(sink io.Writer) error {
	// observe
	defer observe(operationMetrics.WithLabelValues("database.backup"))()

	// TODO: Implement.

	return nil
}

func (d *database) restore(source io.Reader) error {
	// observe
	defer observe(operationMetrics.WithLabelValues("database.restore"))()

	// TODO: Implement.

	return nil
}

func (d *database) close() error {
	// observe
	defer observe(operationMetrics.WithLabelValues("database.close"))()

	// close database
	err := d.pebble.Close()
	if err != nil {
		return err
	}

	return nil
}
