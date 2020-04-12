package tape

import "errors"

// ErrBreak can be returned in walker callbacks to return early.
var ErrBreak = errors.New("break")
