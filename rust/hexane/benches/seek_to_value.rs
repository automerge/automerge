//! `seek_to_value`: plain RLE column vs delta column, over two 1M-value
//! shapes:
//!
//! * `runs`    — 100 copies of each value: 1×100, 2×100, 3×100, …
//!   (RLE-friendly; the delta column sees runs of zeros)
//! * `strided` — blocks of 100 consecutive values with 100-wide gaps:
//!   1..=100, 201..=300, 401..=500, … (delta-friendly stride of 1;
//!   every second target of the right parity is a miss)

use divan::{black_box, Bencher};
use hexane::{Column, DeltaColumn};

fn main() {
    divan::main();
}

const N: usize = 1_000_000;
const SEEKS: usize = 1_000;

fn values(shape: &str) -> Vec<u64> {
    match shape {
        "runs" => (0..N).map(|i| (i / 100 + 1) as u64).collect(),
        "strided" => (0..N)
            .map(|i| ((i / 100) * 200 + (i % 100) + 1) as u64)
            .collect(),
        _ => unreachable!(),
    }
}

/// an even spread of targets across the value domain — half hits,
/// and for `strided` half land in the gaps
fn targets(vals: &[u64]) -> Vec<u64> {
    let max = *vals.last().unwrap();
    (0..SEEKS as u64)
        .map(|i| i * max / SEEKS as u64 + 1)
        .collect()
}

#[divan::bench(args = ["runs", "strided"])]
fn column(bencher: Bencher, shape: &str) {
    let vals = values(shape);
    let targets = targets(&vals);
    let col: Column<u64> = Column::from_values(vals);
    bencher.bench_local(|| {
        for t in &targets {
            let mut iter = col.iter();
            black_box(iter.seek_to_value(black_box(*t), ..));
        }
    });
}

#[divan::bench(args = ["runs", "strided"])]
fn delta(bencher: Bencher, shape: &str) {
    let vals = values(shape);
    let targets = targets(&vals);
    let col: DeltaColumn<u64> = DeltaColumn::from_values(vals);
    bencher.bench_local(|| {
        for t in &targets {
            let mut iter = col.iter();
            black_box(iter.seek_to_value(black_box(*t), ..));
        }
    });
}
