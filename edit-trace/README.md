Try the different editing traces on different automerge implementations

### Automerge Experiement - pure rust

```code
  # cargo --release run
```

#### Benchmarks

There are some criterion benchmarks in the `benches` folder which can be run with `cargo bench` or `cargo criterion`.
For flamegraphing, `cargo flamegraph --bench main -- --bench "save" # or "load" or "replay" or nothing` can be useful.

### Automerge Experiement - wasm api

```code
  # node automerge-wasm.js
```

### Automerge Experiment - JS wrapper

```code
  # node automerge-js.js
```

### Automerge 1.0 pure javascript - new fast backend

This assume automerge has been checked out in a directory along side this repo

```code
  # node automerge-1.0.js
```

### Automerge 1.0 with rust backend

This assume automerge has been checked out in a directory along side this repo

```code
  # node automerge-rs.js
```

### Automerge Experiment - JS wrapper

```code
  # node automerge-js.js
```

### Baseline Test. Javascript Array with no CRDT info

```code
  # node baseline.js
```
