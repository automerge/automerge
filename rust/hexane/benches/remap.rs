//! `Column::remap` benchmark — re-encode every value through a closure.
//!
//! This is the hot path behind automerge's `remap_actors`
//! (`op_set2/columns.rs`): on merge/load, actor-index columns are rewritten
//! through a mapping table.  `remap` streams the old column's runs into an
//! encoder and rebuilds the column via `EncoderApi::into_column`.
//!
//! Workloads:
//!   *_runs   — few distinct values in long runs (real actor-column shape)
//!   *_mixed  — alternating sequential/shuffled blocks (mixed runs+literals)
//!   *_lit    — all-unique values (worst case: every value is a literal)
//!   opt_*    — `Option<u64>` with nulls (nullable actor columns)
//!   small    — 1k items (per-call overhead, many-small-columns shape)
//!
//! The input column is cloned outside the timed section (`with_inputs`),
//! so the numbers measure remap itself.

use divan::counter::ItemsCount;
use divan::Bencher;
use hexane::Column;
use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::{RngExt, SeedableRng};
use std::time::Duration;

fn main() {
    divan::main();
}

const MAX: Duration = Duration::from_secs(4);
const N: usize = 1_000_000;

fn mixed_values() -> Vec<u64> {
    // Same block pattern as `column_u64_mutations`: alternating sequential
    // (delta-friendly literal) and shuffled blocks.
    const BLOCK: usize = 200;
    let mut rng = SmallRng::seed_from_u64(0xC0FFEE);
    let mut out = Vec::with_capacity(N);
    let mut base: u64 = 0;
    let mut idx = 0usize;
    while out.len() < N {
        let len = BLOCK.min(N - out.len());
        if idx % 2 == 0 {
            for i in 0..len {
                out.push(base + i as u64);
            }
        } else {
            let mut v: Vec<u64> = (base..base + len as u64).collect();
            v.shuffle(&mut rng);
            out.extend(v);
        }
        base += len as u64;
        idx += 1;
    }
    out
}

fn bench_u64(bencher: Bencher, values: Vec<u64>) {
    let items = values.len();
    let col = Column::<u64>::from_values(values);
    bencher
        .counter(ItemsCount::new(items))
        .with_inputs(|| col.clone())
        .bench_local_values(|mut c| {
            c.remap(|v| v + 1);
            divan::black_box(c);
        });
}

#[divan::bench(max_time = MAX)]
fn u64_runs_1m(bencher: Bencher) {
    // 4 distinct values in 5000-item runs.
    bench_u64(bencher, (0..N as u64).map(|i| (i / 5000) % 4).collect());
}

#[divan::bench(max_time = MAX)]
fn u64_mixed_1m(bencher: Bencher) {
    bench_u64(bencher, mixed_values());
}

#[divan::bench(max_time = MAX)]
fn u64_lit_1m(bencher: Bencher) {
    bench_u64(bencher, (0..N as u64).collect());
}

#[divan::bench(max_time = MAX)]
fn u64_small_1k(bencher: Bencher) {
    let mut rng = SmallRng::seed_from_u64(0xBEEF);
    bench_u64(
        bencher,
        (0..1_000).map(|_| rng.random_range(0..8u64)).collect(),
    );
}

#[divan::bench(max_time = MAX)]
fn opt_u64_actors_100k(bencher: Bencher) {
    // Nullable actor-column shape: mostly runs of a few small indices,
    // ~10% nulls.
    let mut rng = SmallRng::seed_from_u64(0xAC70);
    let mut vals: Vec<Option<u64>> = Vec::with_capacity(100_000);
    while vals.len() < 100_000 {
        let run = rng.random_range(1..200usize).min(100_000 - vals.len());
        let v = if rng.random_range(0..10u32) == 0 {
            None
        } else {
            Some(rng.random_range(0..6u64))
        };
        vals.extend(std::iter::repeat_n(v, run));
    }
    let col = Column::<Option<u64>>::from_values(vals);
    bencher
        .counter(ItemsCount::new(100_000usize))
        .with_inputs(|| col.clone())
        .bench_local_values(|mut c| {
            c.remap(|v| v.map(|a| a + 1));
            divan::black_box(c);
        });
}
