# Eg-walker paper traces

This directory contains the editing traces used by the `egwalker_paper` benchmarks. They come from the [Eg-walker paper datasets](https://github.com/josephg/egwalker-paper/tree/master/datasets).

The `*.json` files are the source traces. Each trace records a transaction graph and positional text edits. The corresponding `*.am` files are generated Automerge documents consumed by the benchmarks.

To regenerate every Automerge document from its JSON trace, run this from any directory:

```sh
./rust/benchmark-battery/egwalker-paper/generate.sh
```

The generator uses the Automerge version in the current checkout and overwrites the `*.am` files. Generation is slow, and output is not expected to be byte-for-byte identical across Automerge versions.
