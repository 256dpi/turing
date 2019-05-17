package turing

// Observer is the interface implemented by observers that want to observe the
// stream of instructions processed by the machine.
type Observer interface {
	// Init is called when the source instruction stream has been (re)opened.
	// This happens when the machine starts and whenever the node fails and
	// restarts.
	Init()

	// Process is called repeatedly with every instruction processed by the machine.
	// The callee must ensure that the function returns as fast a possible as it
	// blocks the execution of other instructions. If false is returned, the
	// observer will be removed when returning.
	Process(Instruction) bool
}
