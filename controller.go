package turing

type controller struct {
	updates  *bundler
	lookups  *bundler
	database *database
}

func newController(config Config, database *database) *controller {
	return &controller{
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
	}
}

func (c *controller) update(ins Instruction) error {
	return c.updates.process(ins)
}

func (c *controller) lookup(ins Instruction) error {
	return c.lookups.process(ins)
}

func (c *controller) close() {
	c.updates.close()
	c.lookups.close()
}
