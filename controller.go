package turing

type controller struct {
	database *database
	updates  *bundler
	lookups  *bundler
}

func createController(config Config, registry *registry, manager *manager) (*controller, error) {
	// open database
	database, _, err := openDatabase(config, registry, manager)
	if err != nil {
		return nil, err
	}

	return &controller{
		database: database,
		updates: newBundler(bundlerOptions{
			queueSize:   2 * config.BatchSize,
			batchSize:   config.BatchSize,
			concurrency: 1,
			handler: func(list []Instruction) error {
				return database.update(list, nil)
			},
		}),
		lookups: newBundler(bundlerOptions{
			queueSize:   (config.ConcurrentReaders + 1) * config.BatchSize,
			batchSize:   config.BatchSize,
			concurrency: config.ConcurrentReaders,
			handler: func(list []Instruction) error {
				return database.lookup(list)
			},
		}),
	}, nil
}

func (c *controller) update(ins Instruction) error {
	return c.updates.process(ins)
}

func (c *controller) lookup(ins Instruction) error {
	return c.lookups.process(ins)
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
