package turing

import (
	"io"
	"os"
	"strconv"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/lni/dragonboat/logger"
)

var indexKey = []byte("$index")

type database struct {
	bdb *badger.DB
}

func openDatabase(dir string) (*database, uint64, error) {
	// ensure directory
	err := os.MkdirAll(dir, 0700)
	if err != nil {
		return nil, 0, err
	}

	// prepare options
	bo := badger.DefaultOptions
	bo.Dir = dir
	bo.ValueDir = dir
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
		bdb: bdb,
	}

	return db, index, nil
}

func (d *database) update(list []Instruction, index uint64) error {
	// calculate max effect (90% of max batch count)
	maxEffect := int(float64(d.bdb.MaxBatchCount())*0.9)

	// prepare total effect
	totalEffect := 0

	// create initial transaction
	txn := d.bdb.NewTransaction(true)

	// execute all instructions
	for _, instruction := range list {
		// get estimated effect of instruction
		estimatedEffect := instruction.Describe().Effect

		// TODO: Run unbounded instructions in multiple runs.

		// check if new transaction is needed
		if estimatedEffect < 0 || totalEffect+estimatedEffect >= maxEffect {
			// commit current transaction
			err := txn.Commit()
			if err != nil {
				return err
			}

			// create new transaction
			txn = d.bdb.NewTransaction(true)

			// reset total effect
			totalEffect = 0
		}

		// prepare transaction
		transaction := &Transaction{txn: txn}

		// execute transaction
		err := instruction.Execute(transaction)
		if err != nil {
			return err
		}

		// add transaction effect
		totalEffect += transaction.effect
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

	return nil
}

func (d *database) lookup(instruction Instruction) error {
	// execute instruction
	err := d.bdb.View(func(txn *badger.Txn) error {
		return instruction.Execute(&Transaction{txn: txn})
	})
	if err != nil {
		return err
	}

	return nil
}

func (d *database) backup(sink io.Writer) error {
	// perform backup
	_, err := d.bdb.Backup(sink, 0)
	if err != nil {
		return err
	}

	return nil
}

func (d *database) restore(source io.Reader) error {
	// TODO: Clear database beforehand?

	// load backup
	err := d.bdb.Load(source)
	if err != nil {
		return err
	}

	return nil
}

func (d *database) close() error {
	// close database
	err := d.bdb.Close()
	if err != nil {
		return err
	}

	return nil
}
