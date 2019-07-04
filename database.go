package turing

import (
	"io"
	"os"
	"strconv"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/lni/dragonboat/v3/logger"
)

var indexKey = []byte("$index")

type database struct {
	badger  *badger.DB
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

	// prepare options
	bo := badger.DefaultOptions(dir)
	bo.Logger = logger.GetLogger("badger")

	// open database
	bdb, err := badger.Open(bo)
	if err != nil {
		return nil, 0, err
	}

	// run gc routine
	go func() {
		for {
			// sleep some time
			time.Sleep(time.Second)

			// run gc
			err = bdb.RunValueLogGC(0.5)
			if err == badger.ErrRejected {
				return
			} else if err != nil && err != badger.ErrNoRewrite {
				panic(err)
			}
		}
	}()

	// prepare index
	var index uint64

	// get last committed index
	err = bdb.View(func(txn *badger.Txn) error {
		// get key
		item, err := txn.Get(indexKey)
		if err == badger.ErrKeyNotFound {
			return nil
		} else if err != nil {
			return err
		}

		// parse value
		err = item.Value(func(val []byte) error {
			index, err = strconv.ParseUint(string(val), 10, 64)
			return err
		})
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return nil, 0, err
	}

	// create database
	db := &database{
		badger:  bdb,
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

	// calculate max effect (90% of max batch count)
	maxEffect := int(float64(d.badger.MaxBatchCount()) * 0.9)

	// create initial transaction
	txn := d.badger.NewTransaction(true)
	transactionCount := 1
	accumulatedEffect := 0

	// execute all instructions
	for _, instruction := range list {
		// begin observation
		finish := observe(instructionMetrics.WithLabelValues(instruction.Describe().Name))

		// get estimated effect of instruction
		estimatedEffect := instruction.Describe().Effect

		// check if new transaction is needed for bounded transaction
		if estimatedEffect > 0 && accumulatedEffect+estimatedEffect >= maxEffect {
			// commit current transaction
			err := txn.Commit()
			if err != nil {
				return err
			}

			// create new transaction
			txn = d.badger.NewTransaction(true)
			transactionCount++
			accumulatedEffect = 0
		}

		// prepare wrapper
		wrapper := &Transaction{txn: txn}

		// execute transaction
		for {
			err := instruction.Execute(wrapper)
			if err == ErrMaxEffect {
				// persist changes
				err = txn.Commit()
				if err != nil {
					return err
				}

				// create new transaction
				txn = d.badger.NewTransaction(true)
				transactionCount++
				accumulatedEffect = 0

				// reset wrapper
				wrapper = &Transaction{txn: txn}

				continue
			}
			if err != nil {
				return err
			}

			break
		}

		// add effect from uncommitted transaction
		accumulatedEffect += wrapper.effect

		// finish observation
		finish()
	}

	// set index
	err := txn.Set(indexKey, []byte(strconv.FormatUint(index, 10)))
	if err != nil {
		return err
	}

	// commit final transaction
	err = txn.Commit()
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

	// execute instruction
	err := d.badger.View(func(txn *badger.Txn) error {
		// observe
		defer observe(instructionMetrics.WithLabelValues(instruction.Describe().Name))()

		return instruction.Execute(&Transaction{txn: txn})
	})
	if err != nil {
		return err
	}

	return nil
}

func (d *database) backup(sink io.Writer) error {
	// observe
	defer observe(operationMetrics.WithLabelValues("database.backup"))()

	// perform backup
	_, err := d.badger.Backup(sink, 0)
	if err != nil {
		return err
	}

	return nil
}

func (d *database) restore(source io.Reader) error {
	// observe
	defer observe(operationMetrics.WithLabelValues("database.restore"))()

	// TODO: Clear database beforehand?

	// load backup
	err := d.badger.Load(source, 256)
	if err != nil {
		return err
	}

	return nil
}

func (d *database) close() error {
	// observe
	defer observe(operationMetrics.WithLabelValues("database.close"))()

	// close database
	err := d.badger.Close()
	if err != nil {
		return err
	}

	return nil
}
