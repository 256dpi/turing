package turing

type controller struct {
	database *database
	updates  *bundler
	lookups  *bundler
}

// TODO: Provide index to prevent double processing in standalone mode.

func createController(config Config, registry *registry, manager *manager) (*controller, error) {
	// open database
	database, _, err := openDatabase(config, registry, manager)
	if err != nil {
		return nil, err
	}

	return &controller{
		database: database,
		updates: newBundler(bundlerOptions{
			queueSize:   2 * config.UpdateBatchSize,
			batchSize:   config.UpdateBatchSize,
			concurrency: 1, // database anyway only allows one writer
			handler: func(list []Instruction) error {
				return database.update(list, 0)
			},
		}),
		lookups: newBundler(bundlerOptions{
			queueSize:   (config.ConcurrentReaders + 1) * config.LookupBatchSize,
			batchSize:   config.LookupBatchSize,
			concurrency: config.ConcurrentReaders,
			handler: func(list []Instruction) error {
				return database.lookup(list)
			},
		}),
	}, nil
}

func (c *controller) update(ins Instruction, fn func(error)) error {
	return c.updates.process(ins, fn)
}

func (c *controller) lookup(ins Instruction, fn func(error)) error {
	return c.lookups.process(ins, fn)
}

func (c *controller) close() error {
	// close bundlers
	c.updates.close()
	c.lookups.close()

	// close database
	err := c.database.close()
	if err != nil {
		return err
	}

	return nil
}
