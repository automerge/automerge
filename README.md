# Automerge

[![docs](https://docs.rs/automerge/badge.svg)](https://docs.rs/automerge)
[![crates](https://crates.io/crates/automerge)](https://crates.io/crates/automerge)
[![Build Status](https://travis-ci.org/alexjg/automerge-rs.svg?branch=master)](https://travis-ci.org/alexjg/automerge-rs)

This is a rust implementation of
[automerge](https://github.com/automerge/automerge). Currently this repo
contains an implementation of the "backend" of the Automerge library, designed
to be used via FFI from many different platforms. Very soon there will also be
a frontend which will be designed for Rust application developers to use.

## Backend? Frontend?

Automerge is a JSON CRDT, in this sense it is just a data structure with a set
of rules about how to merge two different versions of that data structure.
However, in practice one often needs two separate roles when writing
applications which use the CRDT: 

- A very low latency process, usually running on some kind of UI thread, which
  records changes made by the user and reflects them in the UI
- A slower process which executes the complex logic of merging changes received
  from the UI and over the network and send diffs to the frontend to apply

This is the "frontend" and "backend" we're talking about. The different
responsibilities of the two components are outlined in detail 
[here](https://github.com/automerge/automerge/blob/performance/BINARY_FORMAT.md).
Note that this is for an upcoming release of Automerge so things may change.

