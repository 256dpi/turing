package std

import (
	"os"
	"testing"

	"github.com/256dpi/turing"
)

func TestMain(m *testing.M) {
	// disable logging
	turing.SetLogger(nil)

	// run tests
	os.Exit(m.Run())
}
