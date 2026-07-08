//! Column<u64> mutation benchmark — insert / delete / replace at
//! k = 1 / 10 / 100 / 1000.
//!
//! Same data in both columns (SmallRng seeded at 0xC0FFEE).  Each op
//! batch picks a random position and a fresh batch of new values via
//! the same RNG stream.
//!
//! To compare pre-B-tree v1 vs post-B-tree v1: run on current HEAD,
//! then `git checkout <pre-btree-commit>` and re-run.

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

const N: usize = 1_000_000;
const OPS: usize = 1_000;

fn initial_values() -> Vec<u64> {
    const BLOCK: usize = 200;
    let mut rng = SmallRng::seed_from_u64(0xC0FFEE);
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
            vals.shuffle(&mut rng);
            out.extend(vals);
        }
        base += len as u64;
        block_idx += 1;
    }
    out
}

fn rand_value(rng: &mut SmallRng) -> u64 {
    rng.random_range(0..N as u64)
}

// ╔══════════════════════════════════════════════════════════════════════════╗
// ║ v1 macros (Column<u64>)                                                  ║
// ╚══════════════════════════════════════════════════════════════════════════╝

macro_rules! v1_insert {
    ($name:ident, $k:expr) => {
        #[divan::bench(max_time = Duration::from_secs(8))]
        fn $name(b: Bencher) {
            let v = initial_values();
            b.counter(ItemsCount::new((OPS * $k) as u64))
                .bench_local(|| {
                    let mut c = Column::<u64>::from_values(v.clone());
                    let mut rng = SmallRng::seed_from_u64(0x1234);
                    for _ in 0..OPS {
                        let pos = rng.random_range(0..=c.len());
                        let new: Vec<u64> = (0..$k).map(|_| rand_value(&mut rng)).collect();
                        c.splice(pos, 0, new);
                    }
                    divan::black_box(c)
                });
        }
    };
}

macro_rules! v1_delete {
    ($name:ident, $k:expr) => {
        #[divan::bench(max_time = Duration::from_secs(8))]
        fn $name(b: Bencher) {
            let v = initial_values();
            b.counter(ItemsCount::new((OPS * $k) as u64))
                .bench_local(|| {
                    let mut c = Column::<u64>::from_values(v.clone());
                    let mut rng = SmallRng::seed_from_u64(0x1234);
                    for _ in 0..OPS {
                        let len = c.len();
                        if len < $k {
                            break;
                        }
                        let pos = rng.random_range(0..=len - $k);
                        c.splice(pos, $k, std::iter::empty::<u64>());
                    }
                    divan::black_box(c)
                });
        }
    };
}

macro_rules! v1_replace {
    ($name:ident, $k:expr) => {
        #[divan::bench(max_time = Duration::from_secs(8))]
        fn $name(b: Bencher) {
            let v = initial_values();
            b.counter(ItemsCount::new((OPS * $k) as u64))
                .bench_local(|| {
                    let mut c = Column::<u64>::from_values(v.clone());
                    let mut rng = SmallRng::seed_from_u64(0x1234);
                    for _ in 0..OPS {
                        let len = c.len();
                        if len < $k {
                            break;
                        }
                        let pos = rng.random_range(0..=len - $k);
                        let new: Vec<u64> = (0..$k).map(|_| rand_value(&mut rng)).collect();
                        c.splice(pos, $k, new);
                    }
                    divan::black_box(c)
                });
        }
    };
}

// ╔══════════════════════════════════════════════════════════════════════════╗
// ║ v0 macros (ColumnData<UIntCursor>)                                       ║
// ╚══════════════════════════════════════════════════════════════════════════╝

// v1 benches — insert
v1_insert!(v1_insert_1, 1);
v1_insert!(v1_insert_10, 10);
v1_insert!(v1_insert_100, 100);
v1_insert!(v1_insert_1000, 1000);

// v1 benches — delete
v1_delete!(v1_delete_1, 1);
v1_delete!(v1_delete_10, 10);
v1_delete!(v1_delete_100, 100);
v1_delete!(v1_delete_1000, 1000);

// v1 benches — replace
v1_replace!(v1_replace_1, 1);
v1_replace!(v1_replace_10, 10);
v1_replace!(v1_replace_100, 100);
v1_replace!(v1_replace_1000, 1000);

// v0 benches — insert

// v0 benches — delete

// v0 benches — replace
