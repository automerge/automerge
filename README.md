# Automerge - NEXT

This is pretty much a ground up rewrite of automerge-rs. The objective of this
rewrite is to radically simplify the API. The end goal being to produce a library
which is easy to work with both in Rust and from FFI.

## How?

The current iteration of automerge-rs is complicated to work with because it
adopts the frontend/backend split architecture of the JS implementation. This
architecture was necessary due to basic operations on the automerge opset being
too slow to perform on the UI thread. Recently @orionz has been able to improve
the performance to the point where the split is no longer necessary. This means
we can adopt a much simpler mutable API.

The architecture is now built around the `OpTree`. This is a data structure
which supports efficiently inserting new operations and realising values of
existing operations. Most interactions with the `OpTree` are in the form of
implementations of `TreeQuery` - a trait which can be used to traverse the
optree and producing state of some kind. User facing operations are exposed on
an `Automerge` object, under the covers these operations typically instantiate
some `TreeQuery` and run it over the `OpTree`.

## Status

We have working code which passes all of the tests in the JS test suite. We're
now working on writing a bunch more tests and cleaning up the API.

## Development

### Running CI

The steps CI will run are all defined in `./scripts/ci`. Obviously CI will run
everything when you submit a PR, but if you want to run everything locally
before you push you can run `./scripts/ci/run` to run everything.

### Running the JS tests

You will need to have [node](https://nodejs.org/en/), [yarn](https://yarnpkg.com/getting-started/install), [rust](https://rustup.rs/) and [wasm-pack](https://rustwasm.github.io/wasm-pack/installer/) installed.

To build and test the rust library:

```shell
  $ cd automerge
  $ cargo test
```

To build and test the wasm library:

```shell
  ## setup
  $ cd automerge-wasm
  $ yarn

  ## building or testing
  $ yarn build
  $ yarn test

  ## without this the js library wont automatically use changes
  $ yarn link

  ## cutting a release or doing benchmarking
  $ yarn release
  $ yarn opt ## or set `wasm-opt = false` in Cargo.toml on supported platforms (not arm64 osx)
```

And finally to test the js library. This is where most of the tests reside.

```shell
  ## setup
  $ cd automerge-js
  $ yarn
  $ yarn link "automerge-wasm"

  ## testing
  $ yarn test
```

## Benchmarking

The `edit-trace` folder has the main code for running the edit trace benchmarking.
