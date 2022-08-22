# Automerge RS

<img src='./img/sign.svg' width='500' alt='Automerge logo' />

[![homepage](https://img.shields.io/badge/homepage-published-informational)](https://automerge.org/)
[![main docs](https://img.shields.io/badge/docs-main-informational)](https://automerge.org/automerge-rs/automerge/)
[![ci](https://github.com/automerge/automerge-rs/actions/workflows/ci.yaml/badge.svg)](https://github.com/automerge/automerge-rs/actions/workflows/ci.yaml)
[![docs](https://github.com/automerge/automerge-rs/actions/workflows/docs.yaml/badge.svg)](https://github.com/automerge/automerge-rs/actions/workflows/docs.yaml)

This is a Rust library implementation of the [Automerge](https://github.com/automerge/automerge) file format and network protocol. Its focus is to support the creation of Automerge implementations in other languages, currently; WASM, JS and C. A `libautomerge` if you will.

The original [Automerge](https://github.com/automerge/automerge) project (written in JS from the ground up) is still very much maintained and recommended. Indeed it is because of the success of that project that the next stage of Automerge is being explored here. Hopefully Rust can offer a more performant and scalable Automerge, opening up even more use cases. 

## Status

The project has 5 components:

1. [_automerge_](automerge) - The main Rust implementation of the library.
2. [_automerge-wasm_](automerge-wasm) - A JS/WASM interface to the underlying Rust library. This API is generally mature and in use in a handful of projects.
3. [_automerge-js_](automerge-js) - This is a Javascript library using the WASM interface to export the same public API of the primary Automerge project. Currently this project passes all of Automerge's tests but has not been used in any real project or packaged as an NPM. Alpha testers welcome.
4. [_automerge-c_](automerge-c) - This is a C library intended to be an FFI integration point for all other languages. It is currently a work in progress and not yet ready for any testing.
5. [_automerge-cli_](automerge-cli) - An experimental CLI wrapper around the Rust library. Currently not functional.

## How?

The magic of the architecture is built around the `OpTree`. This is a data structure
which supports efficiently inserting new operations and realising values of
existing operations. Most interactions with the `OpTree` are in the form of
implementations of `TreeQuery` - a trait which can be used to traverse the
`OpTree` and producing state of some kind. User facing operations are exposed on
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

## Benchmarking

The [`edit-trace`](edit-trace) folder has the main code for running the edit trace benchmarking.

## The old Rust project
If you are looking for the origional `automerge-rs` project that can be used as a wasm backend to the javascript implementation, it can be found [here](https://github.com/automerge/automerge-rs/tree/automerge-1.0).
