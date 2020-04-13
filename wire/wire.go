// Package wire provides the internal types used for transportation.
package wire

import "errors"

// ErrBreak may be returned in walk callbacks to stop execution.
var ErrBreak = errors.New("break")
