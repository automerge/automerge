use super::SampledBenchmark;
use benchmark_battery::automerge::{Automerge, ReadDoc, ROOT};
use benchmark_battery::{hidden, masked, wide_map_doc};
use std::hint::black_box;

/// `range` runs under the always-on `masked` frontier and, as `_hidden`, with
/// half the keys invisible.
pub fn benchmarks() -> Vec<SampledBenchmark> {
    vec![
        SampledBenchmark::no_setup("range", "range/range/10000", || range(10_000, masked)),
        SampledBenchmark::no_setup("range", "range/range/10000_hidden", || {
            range(10_000, hidden)
        }),
        SampledBenchmark::no_setup("range", "range/range_at/10000", || range_at(10_000)),
        SampledBenchmark::no_setup("range", "range/range/100000", || range(100_000, masked)),
        SampledBenchmark::no_setup("range", "range/range/100000_hidden", || {
            range(100_000, hidden)
        }),
        SampledBenchmark::no_setup("range", "range/range_at/100000", || range_at(100_000)),
    ]
}

fn range(n: u64, regime: fn(Automerge) -> Automerge) -> Box<dyn FnMut()> {
    let doc = regime(wide_map_doc(n));
    Box::new(move || {
        black_box(&doc).values(ROOT).for_each(drop);
    })
}

fn range_at(n: u64) -> Box<dyn FnMut()> {
    let doc = masked(wide_map_doc(n));
    let heads = doc.get_heads();
    Box::new(move || {
        black_box(&doc).values_at(ROOT, &heads).for_each(drop);
    })
}
