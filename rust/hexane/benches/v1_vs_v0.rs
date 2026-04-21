//! v1 (new B-tree-backed columns) vs v0 (cursor-based ColumnData) on
//! identical workloads.  Data shape: 1M i64 values in alternating 200-
//! sorted / 200-shuffled blocks (globally monotonic) — mimics an
//! `id_ctr`-style column that sees both bulk ordered inserts and
//! concurrent edits.
//!
//! Ops measured (same on both sides):
//!   * build              — bulk-construct from a Vec.
//!   * get                — 1000 random positional lookups.
//!   * insert 1/10/100    — 1000 splices of k new values.
//!   * delete 1/10/100    — 1000 splices removing k values.
//!   * replace 1/10/100   — 1000 splices replacing k values.
//!   * find_by_value      — IndexedDelta only: 1000 unique-target lookups.
//!   * find_by_range      — IndexedDelta only: 1000 narrow range scans.

use divan::counter::ItemsCount;
use divan::Bencher;
use hexane::v1::prefix::PrefixWeightFn;
use hexane::v1::{Column, DeltaColumn, DeltaValue};
use hexane::{ColumnData, DeltaCursor, IntCursor};
use std::time::Duration;

type PrefixDeltaColumn<T> = DeltaColumn<T, PrefixWeightFn<<T as DeltaValue>::Inner>>;

fn main() {
    divan::main();
}

const N: usize = 1_000_000;
const OPS: usize = 1_000;

/// Deterministic xorshift.
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

fn initial_values() -> Vec<i64> {
    const BLOCK: usize = 200;
    let mut rng = Rng::new(0xC0FFEE);
    let mut out = Vec::with_capacity(N);
    let mut base: i64 = 0;
    let mut block_idx = 0usize;
    while out.len() < N {
        let len = BLOCK.min(N - out.len());
        if block_idx % 2 == 0 {
            for i in 0..len {
                out.push(base + i as i64);
            }
        } else {
            let mut vals: Vec<i64> = (base..base + len as i64).collect();
            for k in (1..vals.len()).rev() {
                let j = (rng.next() as usize) % (k + 1);
                vals.swap(j, k);
            }
            out.extend(vals);
        }
        base += len as i64;
        block_idx += 1;
    }
    out
}

fn rand_value(rng: &mut Rng) -> i64 {
    (rng.next() % N as u64) as i64
}

// ╔══════════════════════════════════════════════════════════════════════════╗
// ║ build                                                                    ║
// ╚══════════════════════════════════════════════════════════════════════════╝

#[divan::bench(max_time = Duration::from_secs(8))]
fn v1_column_build(b: Bencher) {
    let v = initial_values();
    b.bench_local(|| divan::black_box(Column::<i64>::from_values(v.clone())));
}

#[divan::bench(max_time = Duration::from_secs(8))]
fn v0_column_build(b: Bencher) {
    let v = initial_values();
    b.bench_local(|| {
        let mut c = ColumnData::<IntCursor>::new();
        c.splice(0, 0, v.iter().copied());
        divan::black_box(c)
    });
}

#[divan::bench(max_time = Duration::from_secs(8))]
fn v1_delta_build(b: Bencher) {
    let v = initial_values();
    b.bench_local(|| divan::black_box(PrefixDeltaColumn::<i64>::from_values(v.clone())));
}

#[divan::bench(max_time = Duration::from_secs(8))]
fn v0_delta_build(b: Bencher) {
    let v = initial_values();
    b.bench_local(|| {
        let mut c = ColumnData::<DeltaCursor>::new();
        c.splice(0, 0, v.iter().copied());
        divan::black_box(c)
    });
}

#[divan::bench(max_time = Duration::from_secs(8))]
fn v1_indexed_build(b: Bencher) {
    let v = initial_values();
    b.bench_local(|| divan::black_box(DeltaColumn::<i64>::from_values(v.clone())));
}

// v0 IndexedDelta = same as v0 Delta (DeltaCursor already carries aggregates);
// reuse the v0_delta_build measurement.

// ╔══════════════════════════════════════════════════════════════════════════╗
// ║ get (random positional reads)                                            ║
// ╚══════════════════════════════════════════════════════════════════════════╝

#[divan::bench(max_time = Duration::from_secs(6))]
fn v1_column_get(b: Bencher) {
    let c = Column::<i64>::from_values(initial_values());
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
fn v0_column_get(b: Bencher) {
    let mut c = ColumnData::<IntCursor>::new();
    c.splice(0, 0, initial_values());
    b.counter(ItemsCount::new(OPS as u64)).bench_local(|| {
        let mut rng = Rng::new(0xABCD);
        let mut acc: i64 = 0;
        for _ in 0..OPS {
            let p = (rng.next() as usize) % c.len();
            if let Some(Some(v)) = c.get(p) {
                acc = acc.wrapping_add(*v);
            }
        }
        divan::black_box(acc)
    });
}

#[divan::bench(max_time = Duration::from_secs(6))]
fn v1_delta_get(b: Bencher) {
    let c = PrefixDeltaColumn::<i64>::from_values(initial_values());
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
fn v0_delta_get(b: Bencher) {
    let mut c = ColumnData::<DeltaCursor>::new();
    c.splice(0, 0, initial_values());
    b.counter(ItemsCount::new(OPS as u64)).bench_local(|| {
        let mut rng = Rng::new(0xABCD);
        let mut acc: i64 = 0;
        for _ in 0..OPS {
            let p = (rng.next() as usize) % c.len();
            if let Some(Some(v)) = c.get(p) {
                acc = acc.wrapping_add(*v);
            }
        }
        divan::black_box(acc)
    });
}

// ╔══════════════════════════════════════════════════════════════════════════╗
// ║ insert / delete / replace macros                                         ║
// ╚══════════════════════════════════════════════════════════════════════════╝

macro_rules! v1_insert {
    ($name:ident, $col_ty:ty, $build:expr, $k:expr) => {
        #[divan::bench(max_time = Duration::from_secs(8))]
        fn $name(b: Bencher) {
            let v = initial_values();
            b.counter(ItemsCount::new((OPS * $k) as u64))
                .bench_local(|| {
                    let mut c: $col_ty = $build(v.clone());
                    let mut rng = Rng::new(0x1234);
                    for _ in 0..OPS {
                        let pos = (rng.next() as usize) % (c.len() + 1);
                        let new: Vec<i64> = (0..$k).map(|_| rand_value(&mut rng)).collect();
                        c.splice(pos, 0, new);
                    }
                    divan::black_box(c)
                });
        }
    };
}

macro_rules! v0_insert {
    ($name:ident, $cursor:ty, $k:expr) => {
        #[divan::bench(max_time = Duration::from_secs(8))]
        fn $name(b: Bencher) {
            let v = initial_values();
            b.counter(ItemsCount::new((OPS * $k) as u64))
                .bench_local(|| {
                    let mut c = ColumnData::<$cursor>::new();
                    c.splice(0, 0, v.iter().copied());
                    let mut rng = Rng::new(0x1234);
                    for _ in 0..OPS {
                        let pos = (rng.next() as usize) % (c.len() + 1);
                        let new: Vec<i64> = (0..$k).map(|_| rand_value(&mut rng)).collect();
                        c.splice(pos, 0, new);
                    }
                    divan::black_box(c)
                });
        }
    };
}

macro_rules! v1_delete {
    ($name:ident, $col_ty:ty, $build:expr, $k:expr) => {
        #[divan::bench(max_time = Duration::from_secs(8))]
        fn $name(b: Bencher) {
            let v = initial_values();
            b.counter(ItemsCount::new((OPS * $k) as u64))
                .bench_local(|| {
                    let mut c: $col_ty = $build(v.clone());
                    let mut rng = Rng::new(0x1234);
                    for _ in 0..OPS {
                        let len = c.len();
                        if len < $k {
                            break;
                        }
                        let pos = (rng.next() as usize) % (len - $k + 1);
                        c.splice(pos, $k, std::iter::empty::<i64>());
                    }
                    divan::black_box(c)
                });
        }
    };
}

macro_rules! v0_delete {
    ($name:ident, $cursor:ty, $k:expr) => {
        #[divan::bench(max_time = Duration::from_secs(8))]
        fn $name(b: Bencher) {
            let v = initial_values();
            b.counter(ItemsCount::new((OPS * $k) as u64))
                .bench_local(|| {
                    let mut c = ColumnData::<$cursor>::new();
                    c.splice(0, 0, v.iter().copied());
                    let mut rng = Rng::new(0x1234);
                    for _ in 0..OPS {
                        let len = c.len();
                        if len < $k {
                            break;
                        }
                        let pos = (rng.next() as usize) % (len - $k + 1);
                        c.splice::<i64, _>(pos, $k, std::iter::empty());
                    }
                    divan::black_box(c)
                });
        }
    };
}

macro_rules! v1_replace {
    ($name:ident, $col_ty:ty, $build:expr, $k:expr) => {
        #[divan::bench(max_time = Duration::from_secs(8))]
        fn $name(b: Bencher) {
            let v = initial_values();
            b.counter(ItemsCount::new((OPS * $k) as u64))
                .bench_local(|| {
                    let mut c: $col_ty = $build(v.clone());
                    let mut rng = Rng::new(0x1234);
                    for _ in 0..OPS {
                        let len = c.len();
                        if len < $k {
                            break;
                        }
                        let pos = (rng.next() as usize) % (len - $k + 1);
                        let new: Vec<i64> = (0..$k).map(|_| rand_value(&mut rng)).collect();
                        c.splice(pos, $k, new);
                    }
                    divan::black_box(c)
                });
        }
    };
}

macro_rules! v0_replace {
    ($name:ident, $cursor:ty, $k:expr) => {
        #[divan::bench(max_time = Duration::from_secs(8))]
        fn $name(b: Bencher) {
            let v = initial_values();
            b.counter(ItemsCount::new((OPS * $k) as u64))
                .bench_local(|| {
                    let mut c = ColumnData::<$cursor>::new();
                    c.splice(0, 0, v.iter().copied());
                    let mut rng = Rng::new(0x1234);
                    for _ in 0..OPS {
                        let len = c.len();
                        if len < $k {
                            break;
                        }
                        let pos = (rng.next() as usize) % (len - $k + 1);
                        let new: Vec<i64> = (0..$k).map(|_| rand_value(&mut rng)).collect();
                        c.splice(pos, $k, new);
                    }
                    divan::black_box(c)
                });
        }
    };
}

// constructors so the macros can take them as values
fn mk_column(v: Vec<i64>) -> Column<i64> {
    Column::from_values(v)
}
fn mk_delta(v: Vec<i64>) -> PrefixDeltaColumn<i64> {
    PrefixDeltaColumn::from_values(v)
}
fn mk_indexed(v: Vec<i64>) -> DeltaColumn<i64> {
    DeltaColumn::from_values(v)
}

// insert
v1_insert!(v1_column_insert_1, Column<i64>, mk_column, 1);
v1_insert!(v1_column_insert_10, Column<i64>, mk_column, 10);
v1_insert!(v1_column_insert_100, Column<i64>, mk_column, 100);
v0_insert!(v0_column_insert_1, IntCursor, 1);
v0_insert!(v0_column_insert_10, IntCursor, 10);
v0_insert!(v0_column_insert_100, IntCursor, 100);

v1_insert!(v1_delta_insert_1, PrefixDeltaColumn<i64>, mk_delta, 1);
v1_insert!(v1_delta_insert_10, PrefixDeltaColumn<i64>, mk_delta, 10);
v1_insert!(v1_delta_insert_100, PrefixDeltaColumn<i64>, mk_delta, 100);
v0_insert!(v0_delta_insert_1, DeltaCursor, 1);
v0_insert!(v0_delta_insert_10, DeltaCursor, 10);
v0_insert!(v0_delta_insert_100, DeltaCursor, 100);

v1_insert!(v1_indexed_insert_1, DeltaColumn<i64>, mk_indexed, 1);
v1_insert!(v1_indexed_insert_10, DeltaColumn<i64>, mk_indexed, 10);
v1_insert!(v1_indexed_insert_100, DeltaColumn<i64>, mk_indexed, 100);

// delete
v1_delete!(v1_column_delete_1, Column<i64>, mk_column, 1);
v1_delete!(v1_column_delete_10, Column<i64>, mk_column, 10);
v1_delete!(v1_column_delete_100, Column<i64>, mk_column, 100);
v0_delete!(v0_column_delete_1, IntCursor, 1);
v0_delete!(v0_column_delete_10, IntCursor, 10);
v0_delete!(v0_column_delete_100, IntCursor, 100);

v1_delete!(v1_delta_delete_1, PrefixDeltaColumn<i64>, mk_delta, 1);
v1_delete!(v1_delta_delete_10, PrefixDeltaColumn<i64>, mk_delta, 10);
v1_delete!(v1_delta_delete_100, PrefixDeltaColumn<i64>, mk_delta, 100);
v0_delete!(v0_delta_delete_1, DeltaCursor, 1);
v0_delete!(v0_delta_delete_10, DeltaCursor, 10);
v0_delete!(v0_delta_delete_100, DeltaCursor, 100);

v1_delete!(v1_indexed_delete_1, DeltaColumn<i64>, mk_indexed, 1);
v1_delete!(v1_indexed_delete_10, DeltaColumn<i64>, mk_indexed, 10);
v1_delete!(v1_indexed_delete_100, DeltaColumn<i64>, mk_indexed, 100);

// replace
v1_replace!(v1_column_replace_1, Column<i64>, mk_column, 1);
v1_replace!(v1_column_replace_10, Column<i64>, mk_column, 10);
v1_replace!(v1_column_replace_100, Column<i64>, mk_column, 100);
v0_replace!(v0_column_replace_1, IntCursor, 1);
v0_replace!(v0_column_replace_10, IntCursor, 10);
v0_replace!(v0_column_replace_100, IntCursor, 100);

v1_replace!(v1_delta_replace_1, PrefixDeltaColumn<i64>, mk_delta, 1);
v1_replace!(v1_delta_replace_10, PrefixDeltaColumn<i64>, mk_delta, 10);
v1_replace!(v1_delta_replace_100, PrefixDeltaColumn<i64>, mk_delta, 100);
v0_replace!(v0_delta_replace_1, DeltaCursor, 1);
v0_replace!(v0_delta_replace_10, DeltaCursor, 10);
v0_replace!(v0_delta_replace_100, DeltaCursor, 100);

v1_replace!(v1_indexed_replace_1, DeltaColumn<i64>, mk_indexed, 1);
v1_replace!(v1_indexed_replace_10, DeltaColumn<i64>, mk_indexed, 10);
v1_replace!(v1_indexed_replace_100, DeltaColumn<i64>, mk_indexed, 100);

// ╔══════════════════════════════════════════════════════════════════════════╗
// ║ value queries — v0 DeltaCursor vs v1 DeltaColumn                  ║
// ╚══════════════════════════════════════════════════════════════════════════╝

#[divan::bench(max_time = Duration::from_secs(8))]
fn v1_indexed_find_by_value(b: Bencher) {
    let c = DeltaColumn::<i64>::from_values(initial_values());
    b.counter(ItemsCount::new(OPS as u64)).bench_local(|| {
        let mut rng = Rng::new(0xABCD);
        let mut acc: usize = 0;
        for _ in 0..OPS {
            let target = rand_value(&mut rng);
            if let Some(idx) = c.find_first(target) {
                acc = acc.wrapping_add(idx);
            }
        }
        divan::black_box(acc)
    });
}

#[divan::bench(max_time = Duration::from_secs(8))]
fn v0_indexed_find_by_value(b: Bencher) {
    let mut c = ColumnData::<DeltaCursor>::new();
    c.splice(0, 0, initial_values());
    b.counter(ItemsCount::new(OPS as u64)).bench_local(|| {
        let mut rng = Rng::new(0xABCD);
        let mut acc: usize = 0;
        for _ in 0..OPS {
            let target = rand_value(&mut rng);
            if let Some(idx) = c.find_by_value(target).next() {
                acc = acc.wrapping_add(idx);
            }
        }
        divan::black_box(acc)
    });
}

#[divan::bench(max_time = Duration::from_secs(8))]
fn v1_indexed_find_by_range(b: Bencher) {
    let c = DeltaColumn::<i64>::from_values(initial_values());
    let window = (N / 1000) as i64;
    b.counter(ItemsCount::new(OPS as u64)).bench_local(|| {
        let mut rng = Rng::new(0xABCD);
        let mut acc: usize = 0;
        for _ in 0..OPS {
            let lo = rand_value(&mut rng).max(0);
            let hi = lo + window;
            acc = acc.wrapping_add(c.find_by_range(lo..hi).count());
        }
        divan::black_box(acc)
    });
}

#[divan::bench(max_time = Duration::from_secs(8))]
fn v0_indexed_find_by_range(b: Bencher) {
    let mut c = ColumnData::<DeltaCursor>::new();
    c.splice(0, 0, initial_values());
    let window = N / 1000;
    b.counter(ItemsCount::new(OPS as u64)).bench_local(|| {
        let mut rng = Rng::new(0xABCD);
        let mut acc: usize = 0;
        for _ in 0..OPS {
            let lo = (rng.next() as usize) % N;
            let hi = lo + window;
            acc = acc.wrapping_add(c.find_by_range(lo..hi).count());
        }
        divan::black_box(acc)
    });
}
