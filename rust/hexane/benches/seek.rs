use divan::Bencher;
use hexane::*;
use std::time::Duration;

use rand::{rng, RngCore};

const N: u64 = 10_000;

fn main() {
    divan::main();
}

const IRANGE: i64 = 1000;
const URANGE: u64 = 1000;

fn rand_u64() -> u64 {
    rng().next_u64() % URANGE
}
fn rand_i64() -> i64 {
    rand_u64() as i64 - IRANGE / 2
}
fn rand_bool() -> bool {
    rand_u64() % 2 == 0
}
fn rand_usize() -> usize {
    rng().next_u64() as usize
}

#[inline(never)]
#[divan::bench(max_time = Duration::from_secs(3))]
fn seek_bool(bencher: Bencher) {
    let col: ColumnData<BooleanCursor> = (0..N).map(|_| rand_bool()).collect();
    bencher.bench_local(|| {
        let pos = rand_usize() % col.len();
        col.get(pos);
    });
}

#[inline(never)]
#[divan::bench(max_time = Duration::from_secs(3))]
fn seek_int(bencher: Bencher) {
    let col: ColumnData<IntCursor> = (0..N).map(|_| rand_i64()).collect();
    bencher.bench_local(|| {
        let pos = rand_usize() % col.len();
        col.get(pos);
    });
}

#[inline(never)]
#[divan::bench(max_time = Duration::from_secs(3))]
fn seek_unt(bencher: Bencher) {
    let col: ColumnData<UIntCursor> = (0..N).map(|_| rand_u64()).collect();
    bencher.bench_local(|| {
        let pos = rand_usize() % col.len();
        col.get(pos);
    });
}

#[inline(never)]
#[divan::bench(max_time = Duration::from_secs(3))]
fn seek_delta(bencher: Bencher) {
    let col: ColumnData<DeltaCursor> = (0..N).map(|_| rand_i64()).collect();
    bencher.bench_local(|| {
        let pos = rand_usize() % col.len();
        col.get(pos);
    });
}

#[inline(never)]
#[divan::bench(max_time = Duration::from_secs(3))]
fn seek_raw(bencher: Bencher) {
    let col: ColumnData<RawCursor> = (0..N).map(|_| vec![0, 1, 2, 3, 4]).collect();
    bencher.bench_local(|| {
        let pos = rand_usize() % (col.len() / 5) * 5;
        col.get(pos);
    });
}
