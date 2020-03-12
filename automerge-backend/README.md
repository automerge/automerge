# Automerge Backend

This crate implements the "backend" of automerge. You feed it changes and it returns diffs.

## Benchmarking

`cargo bench` runs a benchmark which loads a 1000 list ops 10 times and returns the timings

`cargo flamegraph --bench load_list_ops` generates a flamegraph of the list ops benchmark
