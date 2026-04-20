//! Profile target for `Column<u64>` splice performance.
//!
//! Run:
//!   cargo run -p hexane --release --example column_splice_profile
//!
//! Flamegraph (macOS):
//!   cargo flamegraph -p hexane --release --example column_splice_profile
//!
//! Flamegraph (Linux with perf):
//!   cargo flamegraph -p hexane --release --example column_splice_profile
//!   # or:
//!   perf record --call-graph dwarf target/release/examples/column_splice_profile
//!   perf script | inferno-collapse-perf | inferno-flamegraph > flame.svg
//!
//! The three `do_*` functions are `#[inline(never)]` so they show up
//! as distinct stack frames in the flamegraph.

use hexane::v1::Column;
use std::time::Instant;

const N: usize = 1_000_000;
const OPS: usize = 1_000;
const K: usize = 1_000;

/// Deterministic xorshift — same stream across runs.
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

/// Alternating 200-sorted / 200-shuffled blocks, globally monotonic.
/// Same distribution as the column_u64_mutations benchmark.
fn initial_values() -> Vec<u64> {
    const BLOCK: usize = 200;
    let mut rng = Rng::new(0xC0FFEE);
    let mut out = Vec::with_capacity(N);
    let mut base: u64 = 0;
    let mut block_idx = 0usize;
    while out.len() < N {
        let len = BLOCK.min(N - out.len());
        if block_idx % 2 == 0 {
            for i in 0..len {
                out.push(base + i as u64);
            }
        } else {
            let mut vals: Vec<u64> = (base..base + len as u64).collect();
            for k in (1..vals.len()).rev() {
                let j = (rng.next() as usize) % (k + 1);
                vals.swap(j, k);
            }
            out.extend(vals);
        }
        base += len as u64;
        block_idx += 1;
    }
    out
}

/// Bulk-build a column from `N` values.  Visible in the flamegraph.
#[inline(never)]
fn do_build(values: &[u64]) -> Column<u64> {
    Column::<u64>::from_values(values.to_vec())
}

/// Run `OPS` random-position insert splices, each inserting `K` values.
#[inline(never)]
fn do_insert(col: &mut Column<u64>, seed: u64) {
    let mut rng = Rng::new(seed);
    for _ in 0..OPS {
        let pos = (rng.next() as usize) % (col.len() + 1);
        let new: Vec<u64> = (0..K).map(|_| rng.next() % N as u64).collect();
        col.splice(pos, 0, new);
    }
}

/// Run `OPS` random-position replace splices, each replacing `K` values.
#[inline(never)]
fn do_replace(col: &mut Column<u64>, seed: u64) {
    let mut rng = Rng::new(seed);
    for _ in 0..OPS {
        let len = col.len();
        if len < K {
            break;
        }
        let pos = (rng.next() as usize) % (len - K + 1);
        let new: Vec<u64> = (0..K).map(|_| rng.next() % N as u64).collect();
        col.splice(pos, K, new);
    }
}

/// Run `OPS` random-position delete splices, each removing `K` values.
#[inline(never)]
fn do_remove(col: &mut Column<u64>, seed: u64) {
    let mut rng = Rng::new(seed);
    for _ in 0..OPS {
        let len = col.len();
        if len < K {
            break;
        }
        let pos = (rng.next() as usize) % (len - K + 1);
        col.splice(pos, K, std::iter::empty::<u64>());
    }
}

fn main() {
    let values = initial_values();

    eprintln!("building {} items...", values.len());
    let t = Instant::now();
    let mut col = do_build(&values);
    eprintln!("build: {:?} ({} slabs)", t.elapsed(), col.slab_count());

    for round in 0..3 {
        let seed = 0x1234u64 + round as u64;

        let before = col.len();
        let t = Instant::now();
        do_insert(&mut col, seed);
        eprintln!(
            "round {round} insert {OPS}×{K}: {:?}  ({} → {} items, {} slabs)",
            t.elapsed(),
            before,
            col.len(),
            col.slab_count(),
        );

        let before = col.len();
        let t = Instant::now();
        do_replace(&mut col, seed ^ 0x5A5A);
        eprintln!(
            "round {round} replace {OPS}×{K}: {:?}  ({} → {} items, {} slabs)",
            t.elapsed(),
            before,
            col.len(),
            col.slab_count(),
        );

        let before = col.len();
        let t = Instant::now();
        do_remove(&mut col, seed ^ 0xA5A5);
        eprintln!(
            "round {round} remove  {OPS}×{K}: {:?}  ({} → {} items, {} slabs)",
            t.elapsed(),
            before,
            col.len(),
            col.slab_count(),
        );
    }

    // Black-box the result so the whole thing isn't dead code.
    std::hint::black_box(col);
}
