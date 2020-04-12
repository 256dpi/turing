// Package tape provides the internal types used for storage and transportation.
package tape

import "errors"

// ErrBreak may be returned in walk callbacks to return early.
var ErrBreak = errors.New("break")
