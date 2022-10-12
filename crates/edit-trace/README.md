# Edit trace benchmarks

Try the different editing traces on different automerge implementations

## Automerge Experiement - pure rust

```sh
make rust
```

### Benchmarks

There are some criterion benchmarks in the `benches` folder which can be run with `cargo bench` or `cargo criterion`.
For flamegraphing, `cargo flamegraph --bench main -- --bench "save" # or "load" or "replay" or nothing` can be useful.

## Automerge Experiement - wasm api

```sh
make wasm
```

## Automerge Experiment - JS wrapper

```sh
make js
```

## Automerge 1.0 pure javascript - new fast backend

This assumes automerge has been checked out in a directory along side this repo

```sh
node automerge-1.0.js
```

## Automerge 1.0 with rust backend

This assumes automerge has been checked out in a directory along side this repo

```sh
node automerge-rs.js
```

## Baseline Test. Javascript Array with no CRDT info

```sh
make baseline
```
