//! B-tree branching factor sweep.  Run via the shell script that
//! modifies `const B` in btree.rs between runs.
//!
//! Tests Column<u64> (LenWeight = usize, tiny) and
//! IndexedDeltaColumn<i64> (SlabAgg = 4×i64, large) on build + the
//! three splice sizes that stress the B-tree: insert/delete/replace
//! at k=100.

use divan::counter::ItemsCount;
use divan::Bencher;
use hexane::v1::{Column, IndexedDeltaColumn};
use std::time::Duration;

fn main() {
    divan::main();
}

const N: usize = 1_000_000;
const OPS: usize = 1_000;
const K: usize = 1000;

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

fn initial_values_u64() -> Vec<u64> {
    const BLOCK: usize = 200;
    let mut rng = Rng::new(0xC0FFEE);
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
            for k in (1..v.len()).rev() {
                let j = (rng.next() as usize) % (k + 1);
                v.swap(j, k);
            }
            out.extend(v);
        }
        base += len as u64;
        idx += 1;
    }
    out
}

fn initial_values_i64() -> Vec<i64> {
    initial_values_u64().into_iter().map(|v| v as i64).collect()
}

// ── Column<u64> (LenWeight = usize) ────────────────────────────────────────

#[divan::bench(max_time = Duration::from_secs(6))]
fn len_build(b: Bencher) {
    let v = initial_values_u64();
    b.bench_local(|| divan::black_box(Column::<u64>::from_values(v.clone())));
}

#[divan::bench(max_time = Duration::from_secs(6))]
fn len_insert(b: Bencher) {
    let v = initial_values_u64();
    b.counter(ItemsCount::new((OPS * K) as u64))
        .bench_local(|| {
            let mut c = Column::<u64>::from_values(v.clone());
            let mut rng = Rng::new(0x1234);
            for _ in 0..OPS {
                let pos = (rng.next() as usize) % (c.len() + 1);
                let new: Vec<u64> = (0..K).map(|_| rng.next() % N as u64).collect();
                c.splice(pos, 0, new);
            }
            divan::black_box(c)
        });
}

#[divan::bench(max_time = Duration::from_secs(6))]
fn len_delete(b: Bencher) {
    let v = initial_values_u64();
    b.counter(ItemsCount::new((OPS * K) as u64))
        .bench_local(|| {
            let mut c = Column::<u64>::from_values(v.clone());
            let mut rng = Rng::new(0x1234);
            for _ in 0..OPS {
                let len = c.len();
                if len < K {
                    break;
                }
                let pos = (rng.next() as usize) % (len - K + 1);
                c.splice(pos, K, std::iter::empty::<u64>());
            }
            divan::black_box(c)
        });
}

#[divan::bench(max_time = Duration::from_secs(6))]
fn len_replace(b: Bencher) {
    let v = initial_values_u64();
    b.counter(ItemsCount::new((OPS * K) as u64))
        .bench_local(|| {
            let mut c = Column::<u64>::from_values(v.clone());
            let mut rng = Rng::new(0x1234);
            for _ in 0..OPS {
                let len = c.len();
                if len < K {
                    break;
                }
                let pos = (rng.next() as usize) % (len - K + 1);
                let new: Vec<u64> = (0..K).map(|_| rng.next() % N as u64).collect();
                c.splice(pos, K, new);
            }
            divan::black_box(c)
        });
}

#[divan::bench(max_time = Duration::from_secs(6))]
fn len_get(b: Bencher) {
    let c = Column::<u64>::from_values(initial_values_u64());
    b.counter(ItemsCount::new(OPS as u64)).bench_local(|| {
        let mut rng = Rng::new(0xABCD);
        let mut acc: u64 = 0;
        for _ in 0..OPS {
            let p = (rng.next() as usize) % c.len();
            if let Some(v) = c.get(p) {
                acc = acc.wrapping_add(v);
            }
        }
        divan::black_box(acc)
    });
}

// ── IndexedDeltaColumn<i64> (SlabAgg = 4×i64) ─────────────────────────────

#[divan::bench(max_time = Duration::from_secs(6))]
fn agg_build(b: Bencher) {
    let v = initial_values_i64();
    b.bench_local(|| divan::black_box(IndexedDeltaColumn::<i64>::from_values(v.clone())));
}

#[divan::bench(max_time = Duration::from_secs(6))]
fn agg_insert(b: Bencher) {
    let v = initial_values_i64();
    b.counter(ItemsCount::new((OPS * K) as u64))
        .bench_local(|| {
            let mut c = IndexedDeltaColumn::<i64>::from_values(v.clone());
            let mut rng = Rng::new(0x1234);
            for _ in 0..OPS {
                let pos = (rng.next() as usize) % (c.len() + 1);
                let new: Vec<i64> = (0..K).map(|_| (rng.next() % N as u64) as i64).collect();
                c.splice(pos, 0, new);
            }
            divan::black_box(c)
        });
}

#[divan::bench(max_time = Duration::from_secs(6))]
fn agg_delete(b: Bencher) {
    let v = initial_values_i64();
    b.counter(ItemsCount::new((OPS * K) as u64))
        .bench_local(|| {
            let mut c = IndexedDeltaColumn::<i64>::from_values(v.clone());
            let mut rng = Rng::new(0x1234);
            for _ in 0..OPS {
                let len = c.len();
                if len < K {
                    break;
                }
                let pos = (rng.next() as usize) % (len - K + 1);
                c.splice(pos, K, std::iter::empty::<i64>());
            }
            divan::black_box(c)
        });
}

#[divan::bench(max_time = Duration::from_secs(6))]
fn agg_replace(b: Bencher) {
    let v = initial_values_i64();
    b.counter(ItemsCount::new((OPS * K) as u64))
        .bench_local(|| {
            let mut c = IndexedDeltaColumn::<i64>::from_values(v.clone());
            let mut rng = Rng::new(0x1234);
            for _ in 0..OPS {
                let len = c.len();
                if len < K {
                    break;
                }
                let pos = (rng.next() as usize) % (len - K + 1);
                let new: Vec<i64> = (0..K).map(|_| (rng.next() % N as u64) as i64).collect();
                c.splice(pos, K, new);
            }
            divan::black_box(c)
        });
}

#[divan::bench(max_time = Duration::from_secs(6))]
fn agg_get(b: Bencher) {
    let c = IndexedDeltaColumn::<i64>::from_values(initial_values_i64());
    b.counter(ItemsCount::new(OPS as u64)).bench_local(|| {
        let mut rng = Rng::new(0xABCD);
        let mut acc: i64 = 0;
        for _ in 0..OPS {
            let p = (rng.next() as usize) % c.len();
            if let Some(v) = c.get(p) {
                acc = acc.wrapping_add(v);
            }
        }
        divan::black_box(acc)
    });
}

#[divan::bench(max_time = Duration::from_secs(6))]
fn agg_find_by_value(b: Bencher) {
    let c = IndexedDeltaColumn::<i64>::from_values(initial_values_i64());
    b.counter(ItemsCount::new(OPS as u64)).bench_local(|| {
        let mut rng = Rng::new(0xABCD);
        let mut acc: usize = 0;
        for _ in 0..OPS {
            let target = (rng.next() % N as u64) as i64;
            if let Some(idx) = c.find_first(target) {
                acc = acc.wrapping_add(idx);
            }
        }
        divan::black_box(acc)
    });
}
