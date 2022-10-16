# Automerge

<img src='./img/sign.svg' width='500' alt='Automerge logo' />

[![homepage](https://img.shields.io/badge/homepage-published-informational)](https://automerge.org/)
[![main docs](https://img.shields.io/badge/docs-main-informational)](https://automerge.org/automerge-rs/automerge/)
[![ci](https://github.com/automerge/automerge-rs/actions/workflows/ci.yaml/badge.svg)](https://github.com/automerge/automerge-rs/actions/workflows/ci.yaml)
[![docs](https://github.com/automerge/automerge-rs/actions/workflows/docs.yaml/badge.svg)](https://github.com/automerge/automerge-rs/actions/workflows/docs.yaml)

Automerge is a library which provides fast implementations of several different
CRDTs, a compact compression format for these CRDTs, and a sync protocol for
efficiently transmitting those changes over the network. The objective of the
project is to support [local-first](https://www.inkandswitch.com/local-first/) applications in the same way that relational
databases support server applications - by providing mechanisms for persistence
which allow application developers to avoid thinking about hard distributed
computing problems. Automerge aims to be PostgreSQL for your local-first app.

If you're looking for documentation on the JavaScript implementation take a look
at https://automerge.org/docs/hello/. There are other implementations in both
Rust and C, but they are earlier and don't have documentation yet. You can find
them in `crates/automerge` and `crates/automerge-c` if you are comfortable
reading the code and tests to figure out how to use them.

If you're familiar with CRDTs and interested in the design of Automerge in
particular take a look at https://automerge.org/docs/how-it-works/backend/

Finally, if you want to talk to us about this project please [join the
Slack](https://join.slack.com/t/automerge/shared_invite/zt-1ho1ieas2-DnWZcRR82BRu65vCD4t3Xw)

## Status

This project is formed of a core Rust implementation which is exposed via FFI in
javascript+WASM, C, and soon other languages. Alex
([@alexjg](https://github.com/alexjg/)]) is working full time on maintaining
automerge, other members of Ink and Switch are also contributing time and there
are several other maintainers. The focus is currently on shipping the new JS
package. We expect to be iterating the API and adding new features over the next
six months so there will likely be several major version bumps in all packages
in that time.

In general we try and respect semver.

### JavaScript 

An alpha release of the javascript package is currently available as
`@automerge/automerge@2.0.0-alpha.n` where `n` is an integer. We are gathering
feedback on the API and looking to release a `2.0.0` in the next few weeks. 

### Rust

The rust codebase is currently oriented around producing a performant backend
for the Javascript wrapper and as such the API for Rust code is low level and
not well documented. We will be returning to this over the next few months but
for now you will need to be comfortable reading the tests and asking questions
to figure out how to use it.


## Repository Organisation

* `./crates` - the crates which make up the rust implementation and also the
  Rust components of platform specific wrappers (e.g. `automerge-wasm` for the
  WASM API or `automerge-c` for the C FFI bindings)
* `./wrappers` - code for specific languages which wraps the FFI interface in a
  more idiomatic API (e.g. `wrappers/javascript`)
* `./scripts` - scripts which are useful to maintenance of the repository.
  This includes the scripts which are run in CI.
* `./img` - static assets for use in `.md` files

This repository contains the primary implementation of automerge - which is
written in rust in `./crates` - as well as wrappers which expose the Rust
implementation via FFI in other languages in `./wrappers`. Because this is

## Building

To build this codebase you will need:

- `rust`
- `wasm-bindgen-cli`
- `wasm-opt`
- `node`
- `yarn`
- `cmake`

The various subprojects (the rust code, the wrapper projects) have their own
build instructions, but to run the tests that will be run in CI you can run
`./scripts/ci/run`. 

## Contributing

Please try and split your changes up into relatively independent commits which
change one subsystem at a time and add good commit messages which describe what
the change is and why you're making it (err on the side of longer commit
messages). `git blame` should give future maintainers a good idea of why
something is the way it is.
