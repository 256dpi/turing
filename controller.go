package turing

type controller struct {
	updates  *bundler
	lookups  *bundler
	database *database
}

func newController(config Config, database *database) *controller {
	return &controller{
		updates: newBundler(1000, 200, 1, func(list []Instruction) error {
			return database.update(list, nil)
		}),
		lookups: newBundler(1000, 200, config.ConcurrentReaders, func(list []Instruction) error {
			return database.lookup(list)
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
