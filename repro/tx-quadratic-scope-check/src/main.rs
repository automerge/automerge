//! Measures how the cost of a single transaction grows with its own length.
//!
//! Deliberately written against the smallest possible slice of the public API — `transaction`,
//! `put_object`, `put`, `commit` — so that the same source compiles unchanged against automerge
//! 0.9, 0.10, and 0.11. That makes it usable for A/B comparison and for `git bisect`.
//!
//! Usage:
//!
//! ```sh
//! cargo run --release
//! cargo run --release -- 1000 4000 16000 64000
//! ```
//!
//! Each size is 4x the last, so linear growth reports a ratio near 4 and quadratic growth reports
//! a ratio near 16.

use std::{
    env,
    hint::black_box,
    time::{Duration, Instant},
};

use automerge::{transaction::Transactable, Automerge, ObjType, ROOT};

/// Operations recorded per entry: one `put_object` plus two `put`s.
const OPS_PER_ENTRY: usize = 3;

const DEFAULT_SIZES: [usize; 5] = [4_000, 8_000, 16_000, 32_000, 64_000];

/// Fitted exponent above which we call the growth superlinear. Linear cost is 1.0 and quadratic
/// is 2.0, so this sits clear of measurement noise on either side.
const SUPERLINEAR_EXPONENT: f64 = 1.3;

fn main() {
    let sizes = parse_sizes();

    // Warm up the allocator so the first measured size is not penalised.
    black_box(bulk_file_commit(sizes[0] / 8));

    println!("automerge single-transaction scaling");
    println!("one transaction, {OPS_PER_ENTRY} ops per entry\n");
    println!(
        "{:>10}  {:>12}  {:>12}  {:>10}  {:>8}  {:>9}",
        "entries", "ops", "elapsed", "us/op", "growth", "exponent"
    );
    println!("{}", "-".repeat(72));

    let mut previous: Option<(usize, Duration)> = None;
    let mut worst_exponent = 0.0_f64;

    for &n in &sizes {
        let elapsed = time_bulk_file_commit(n);
        let ops = n * OPS_PER_ENTRY;
        let micros_per_op = elapsed.as_secs_f64() * 1e6 / ops as f64;

        match previous {
            Some((previous_n, previous_elapsed)) => {
                let step = n as f64 / previous_n as f64;
                let growth = elapsed.as_secs_f64() / previous_elapsed.as_secs_f64();
                // Local slope on a log-log plot: growth == step^exponent.
                let exponent = growth.ln() / step.ln();
                worst_exponent = worst_exponent.max(exponent);

                println!(
                    "{n:>10}  {ops:>12}  {elapsed:>11.3?}  {micros_per_op:>10.2}  \
                     {growth:>7.2}x  {exponent:>9.2}"
                );
            }
            None => println!(
                "{n:>10}  {ops:>12}  {elapsed:>11.3?}  {micros_per_op:>10.2}  {:>8}  {:>9}",
                "-", "-"
            ),
        }

        previous = Some((n, elapsed));
    }

    println!();
    if previous.is_none() || sizes.len() < 2 {
        println!("Need at least two sizes to report a trend.");
        return;
    }

    println!("steepest measured exponent: {worst_exponent:.2} (linear 1.0, quadratic 2.0)");
    if worst_exponent < SUPERLINEAR_EXPONENT {
        println!("LINEAR: per-operation cost is flat in transaction length.");
    } else {
        println!("SUPERLINEAR: per-operation cost grows with transaction length.");
    }
}

fn parse_sizes() -> Vec<usize> {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.is_empty() {
        return DEFAULT_SIZES.to_vec();
    }
    args.iter()
        .map(|arg| {
            arg.parse()
                .unwrap_or_else(|_| panic!("not a valid entry count: {arg}"))
        })
        .collect()
}

fn time_bulk_file_commit(entries: usize) -> Duration {
    let start = Instant::now();
    let doc = bulk_file_commit(entries);
    let elapsed = start.elapsed();
    black_box(doc);
    elapsed
}

/// A `files` map holding many small map entries, all written in one transaction.
///
/// Each entry is created and then immediately written into. That ordering is the worst case for a
/// front-to-back search of the pending operation list, because the object just created sits at the
/// very end of it.
fn bulk_file_commit(entries: usize) -> Automerge {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    let files = tx.put_object(ROOT, "files", ObjType::Map).unwrap();

    for i in 0..entries {
        let entry = tx
            .put_object(&files, format!("file{i}"), ObjType::Map)
            .unwrap();
        tx.put(&entry, "hash", i as i64).unwrap();
        tx.put(&entry, "url", "automerge:placeholder").unwrap();
    }

    tx.commit();
    doc
}
