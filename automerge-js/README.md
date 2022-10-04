## Automerge

Automerge is a library of data structures for building collaborative
applications, this package is the javascript implementation.

Please see [automerge.org](http://automerge.org/) for documentation.

## Setup

This package is a wrapper around a core library which is written in rust and
compiled to WASM. In `node` this should be transparent to you, but in the
browser you will need a bundler to include the WASM blob as part of your module
hierarchy. There are examples of doing this with common bundlers in `./examples`.

## Meta

Copyright 2017–2021, the Automerge contributors. Released under the terms of the
MIT license (see `LICENSE`).
