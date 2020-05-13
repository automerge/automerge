# Automerge

[![docs](https://docs.rs/automerge/badge.svg)](https://docs.rs/automerge)
[![crates](https://crates.io/crates/automerge)](https://crates.io/crates/automerge)
[![Build Status](https://travis-ci.org/alexjg/automerge-rs.svg?branch=master)](https://travis-ci.org/alexjg/automerge-rs)

This is a rust implementation of
[automerge](https://github.com/automerge/automerge). Currently this repo
contains an implementation of the "backend" of the Automerge library, designed
to be used via FFI from many different platforms. Very soon there will also be
a frontend which will be designed for Rust application developers to use.

## Using automerge-backend-wasm with automerge

This backend is tracking the [performance branch of automerge](https://github.com/automerge/automerge/tree/performance)

To build the wasm backend

```
  $ cd automerge-backend-wasm
  $ yarn release
```

Once it is built set the new default backend in your js application like this.

```js
  const wasmBackend = require(path.resolve(WASM_BACKEND_PATH))
  Automerge.setDefaultBackend(wasmBackend)
```

## Backend? Frontend?

Automerge is a JSON CRDT, in this sense it is just a data structure with a set
of rules about how to merge two different versions of that data structure.
However, in practice one often needs two separate roles when writing
applications which use the CRDT: 

- A very low latency process, usually running on some kind of UI thread, which
  records changes made by the user and reflects them in the UI
- A less latency sensitive process which executes the complex logic of merging changes
  received from the UI and over the network and send diffs to the frontend to apply

More details can be found [here](https://github.com/automerge/automerge/blob/performance/BINARY_FORMAT.md).

Note that the performance branch of automerge is under active development and is changing quickly.

## Community

Development of automerge rust is currently beeing coordinated at our [slack channel](https://automerge.slack.com/archives/CTQARU3NZ).  Come say hi. =)


