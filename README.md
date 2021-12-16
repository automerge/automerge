
# Automerge Experiment

### Setup

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

  And finally to test the js library.  This is where most of the tests reside.

```shell
  ## setup
  $ cd automerge-js
  $ yarn
  $ yarn link "automerge-wasm"

  ## testing
  $ yarn --test
```

