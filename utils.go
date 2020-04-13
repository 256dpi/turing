package turing

type zeroRef struct{}

func (*zeroRef) Release() {}

var noopRef = &zeroRef{}

type closerFunc func() error

func (f closerFunc) Close() error {
	return f()
}

var noopCloser = closerFunc(func() error {
	return nil
})
