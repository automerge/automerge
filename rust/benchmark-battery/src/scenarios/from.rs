use super::{Benchmark, SampledBenchmark};
use benchmark_battery::{build_hydrate_map, from_hydrate, hydrate};
use std::hint::black_box;

// Number of records in the generated hydrated document.
//
// Each record expands to roughly 11 ops, so 10,000 records is on the order of
// ~110k ops, comparable to the other size-parameterised benchmarks in this
// suite.
const SIZES: [u64; 2] = [1_000, 10_000];

pub fn benchmarks() -> Vec<Benchmark> {
    SIZES
        .into_iter()
        .map(|n| {
            SampledBenchmark::batched(
                "from",
                name(n),
                move || build_hydrate_map(n),
                run_from_hydrate,
            )
            .into()
        })
        .collect()
}

fn run_from_hydrate(map: hydrate::Map) {
    black_box(from_hydrate(black_box(&map)));
}

fn name(n: u64) -> &'static str {
    Box::leak(format!("from/init_root_from_hydrate/{n}").into_boxed_str())
}
