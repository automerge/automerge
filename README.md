# Automerge RS

<img src='./img/sign.svg' width='500' alt='Automerge logo' />

[![homepage](https://img.shields.io/badge/homepage-published-informational)](https://automerge.org/)
[![main docs](https://img.shields.io/badge/docs-main-informational)](https://automerge.org/automerge-rs/automerge/)
[![ci](https://github.com/automerge/automerge-rs/actions/workflows/ci.yaml/badge.svg)](https://github.com/automerge/automerge-rs/actions/workflows/ci.yaml)
[![docs](https://github.com/automerge/automerge-rs/actions/workflows/docs.yaml/badge.svg)](https://github.com/automerge/automerge-rs/actions/workflows/docs.yaml)

This is a rust implementation of the [Automerge](https://github.com/automerge/automerge) file format and network protocol.

If you are looking for the origional `automerge-rs` project that can be used as a wasm backend to the javascript implementation, it can be found [here](https://github.com/automerge/automerge-rs/tree/automerge-1.0).

## Status

This project has 4 components:

1. [_automerge_](automerge) - a rust implementation of the library. This project is the most mature and being used in a handful of small applications.
2. [_automerge-wasm_](automerge-wasm) - a js/wasm interface to the underlying rust library. This api is generally mature and in use in a handful of projects as well.
3. [_automerge-js_](automerge-js) - this is a javascript library using the wasm interface to export the same public api of the primary automerge project. Currently this project passes all of automerge's tests but has not been used in any real project or packaged as an NPM. Alpha testers welcome.
4. [_automerge-c_](automerge-c) - this is a c library intended to be an ffi integration point for all other languages. It is currently a work in progress and not yet ready for any testing.

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

## Development

Please feel free to open issues and pull requests.

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
```

To test the js library. This is where most of the tests reside.

```shell
  ## setup
  $ cd automerge-js
  $ yarn
  $ yarn link "automerge-wasm"

  ## testing
  $ yarn test
```

And finally, to build and test the C bindings with CMake:

```shell
## setup
$ cd automerge-c
$ mkdir -p build
$ cd build
$ cmake -S .. -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=OFF
## building and testing
$ cmake --build . --target test_automerge
```

To add debugging symbols, replace `Release` with `Debug`.
To build a shared library instead of a static one, replace `OFF` with `ON`.

The C bindings can be built and tested on any platform for which CMake is
available but the steps for doing so vary across platforms and are too numerous
to list here.

## Fuzzing

Fuzz tests are contained in the [`fuzz`](fuzz) directory.
Fuzz tests can be run from the root directory using [`cargo-fuzz`](https://github.com/rust-fuzz/cargo-fuzz):

```sh
# list fuzz targets available
cargo fuzz list

# actually run a fuzz target, this runs continuously
cargo fuzz run save_load
```

## Benchmarking

The [`edit-trace`](edit-trace) folder has the main code for running the edit trace benchmarking.
