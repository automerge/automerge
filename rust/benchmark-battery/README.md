# benchmark-battery

A battery of benchmarks for the Automerge Rust core.

This crate was originally a separate repository at [automerge-battery](https://github.com/orionz/automerge-battery). We have moved it in-tree to make it easier to maintain and to run in CI.

This crate is intended to be used as a standalone CLI tool rather than via `cargo bench`. This is because as well as defining benchmarks the crate also defines a stable JSON output format and a comparison tool, which can then be used to compare benchmark runs across branches and commits.

## Running benchmarks

List benchmarks from the repository root:

```sh
cargo run --release -p benchmark-battery -- list
```

Run the fast tier and write a stable JSON result file. This is the default tier and is intended to finish within a minute on CI machines:

```sh
cargo run --release -p benchmark-battery -- run --output before.json
```

Benchmarks are classified into `fast` and `slow` tiers using hardcoded benchmark semantics. The fast tier currently takes about 18 seconds on the reference machine, leaving headroom for slower CI machines. Use `--tier slow` for the remaining expensive benchmarks or `--tier all` for the complete suite. `list` accepts the same tier values and defaults to `all`.

Run a subset by substring. The filter is matched against both benchmark group and benchmark name:

```sh
cargo run --release -p benchmark-battery -- run \
  --filter load_save \
  --output before-load-save.json
```

Every output file records all benchmarks known to the binary. Benchmarks excluded by the tier or filter, or unavailable in that build, are recorded as `not_run`. This lets comparison distinguish an existing benchmark that was not measured from a benchmark that was added or removed.

Additional measurements can be added to the same file with `--append`. New measurements replace matching `not_run` entries or earlier measurements of the same benchmark, while unrelated measured results are preserved:

```sh
cargo run --release -p benchmark-battery -- run \
  --filter load_save --output before.json

cargo run --release -p benchmark-battery -- run \
  --filter sync --append --output before.json
```

The runs must have matching suite, schema, commit, Rust compiler, target, and profile metadata.

Compare before and after JSON files:

```sh
cargo run -p benchmark-battery -- compare \
  before.json after.json
```

Markdown and JSON reports are also available:

```sh
cargo run --release -p benchmark-battery -- compare \
  before.json after.json \
  --format markdown

cargo run --release -p benchmark-battery -- compare \
  before.json after.json \
  --format json
```

Series benchmark graphs can be written while comparing:

```sh
cargo run --release -p benchmark-battery -- compare \
  before.json after.json \
  --format markdown \
  --report-dir target/benchmark-battery-report \
  > target/benchmark-battery-report/report.md
```

The markdown output links to SVG files under the report directory.

## Sampled, series, and memory benchmarks

The benchmark suite has sampled benchmarks, series benchmarks, and optional memory benchmarks.

Sampled benchmarks measure an operation repeatedly and store the raw sample values. An example would be the time taken to load a particular document. Comparison reports the median percent change and uses a bootstrap confidence interval to decide whether a sampled change is significant. By default, sampled changes below `--min-change-percent` or whose bootstrap CI overlaps zero are hidden as noise; pass `--show-all` to include them.

Series benchmarks measure a stateful sequence of operations and store every point in the series, an example might be the time take to apply a sequence of edits to a document. To compare this kind of benchmark we plot the two series and compute the area under the curve for each. The percent change is reported as the total/(area under curve) percent change, and also the percent change in the last window of points. The last-window percent change is useful for detecting regressions that only appear after a long sequence of operations.

Memory benchmarks measure the peak heap allocation during an operation and the steady heap allocation while its result remains alive. They run once because heap allocation for these deterministic workloads is not statistically sampled. Reports treat peak and steady-state memory as separate figures, so a regression in one and an improvement in the other are reported independently.

## Stable JSON shape

Run output has this broad shape:

```json
{
  "schema": 2,
  "suite": "benchmark-battery",
  "commit": "...",
  "results": [
    {
      "kind": "sampled",
      "name": "load_save/load_typing",
      "unit": "ns/iter",
      "setup_duration_ns": 1000,
      "median": 123,
      "sample_values": [120, 123, 130]
    },
    {
      "kind": "series",
      "name": "sync/tiny_text",
      "unit": "ns/step",
      "setup_duration_ns": 2000,
      "points": [1000, 1100, 1200],
      "summary": {
        "total": 3300,
        "mean": 1100,
        "median": 1100,
        "last_window_median": 1200
      }
    },
    {
      "kind": "memory",
      "name": "memory/load/essay",
      "unit": "bytes",
      "setup_duration_ns": 3000,
      "peak_bytes": 123456,
      "steady_bytes": 100000
    },
    {
      "kind": "not_run",
      "name": "memory/load/pathological",
      "group": "memory",
      "unit": "bytes",
      "benchmark_kind": "memory"
    }
  ]
}
```

Compare output is also JSON-serializable. Each entry has a `comparison_kind` tag, with different fields for `sampled`, `series`, `memory`, `not_run`, `added`, and `removed` comparisons. Memory comparisons have a `metric` of `peak` or `steady_state` and are emitted as separate entries.

## Memory benchmarks

Memory measurement requires a tracking global allocator, so it is kept behind the `memory` feature to avoid adding allocator overhead to normal timing runs. Memory benchmarks are still registered as `not_run` without the feature. First run timing benchmarks normally, then add the memory group to the same output file from a feature-enabled build:

```sh
cargo run --release --manifest-path rust/Cargo.toml \
  -p benchmark-battery -- \
  run --output before.json

cargo run --release --manifest-path rust/Cargo.toml \
  -p benchmark-battery --features memory -- \
  run --tier slow --filter memory --append --output before.json
```

Repeat the two commands for `after.json` in the checkout being compared.

The in-tree allocator records requested heap bytes on the thread running the benchmark; it does not measure process RSS or allocations made on other threads. Setup work, including generating or reading the saved input document, is excluded from the measurement. `steady_bytes` is recorded while the loaded Automerge document is still alive.
