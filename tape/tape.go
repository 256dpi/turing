// Package tape provides the internal types used for storage.
package tape

import "errors"

// ErrBreak may be returned in walk callbacks to stop execution.
var ErrBreak = errors.New("break")
