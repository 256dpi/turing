# turing

[![Test](https://github.com/256dpi/turing/actions/workflows/test.yml/badge.svg)](https://github.com/256dpi/turing/actions/workflows/test.yml)
[![GoDoc](https://godoc.org/github.com/256dpi/turing?status.svg)](http://godoc.org/github.com/256dpi/turing)
[![Release](https://img.shields.io/github/release/256dpi/turing.svg)](https://github.com/256dpi/turing/releases)

Turing is a framework for building domain specific databases on top of a
replicated key value store. Database commands are implemented as instructions
that are managed and executed by a cluster of turing nodes. The framework can
be used to build client/server style databases or embedded databases within
application/services. The goal is to provide a simple API and toolkit that can
be used from standalone in-memory databases up to clusters consisting of many
nodes. Under the hood, turing uses the [pebble](https://github.com/cockroachdb/pebble)
(alpha) for storing the data and [dragonboat](https://github.com/lni/dragonboat)
for reaching consensus among the nodes.

## Examples

- An example implementing a simple counter can be found
  [here](https://github.com/256dpi/turing/blob/master/examples/counter/main.go). 
- The [`stdset`](https://github.com/256dpi/turing/tree/master/stdset)
  package implements a set of basic instructions.

## License

The MIT License (MIT)

Copyright (c) 2020 Joël Gähwiler
