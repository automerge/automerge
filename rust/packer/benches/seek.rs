use criterion::{criterion_group, criterion_main, Criterion};
use packer::*;
use rand::distributions::{Alphanumeric, DistString};
use rand::prelude::*;

fn bench_seek<const B: usize>(c: &mut Criterion, rng: &mut ThreadRng, n: usize) {
    c.bench_function(&format!("seek_u64_norun_n:{}_B:{}", n, B), |b| {
        let col = gen_u64_col::<B>(rng, n, 100_000);
        b.iter(|| seek(rng, &col));
    });
    c.bench_function(&format!("seek_i64_norun_n:{}_B:{}", n, B), |b| {
        let col = gen_i64_col::<B>(rng, n, 100_000);
        b.iter(|| seek(rng, &col));
    });
    c.bench_function(&format!("seek_u32_norun_n:{}_B:{}", n, B), |b| {
        let col = gen_u32_col::<B>(rng, n, 100_000);
        b.iter(|| seek(rng, &col));
    });
    c.bench_function(&format!("seek_u64_run_n:{}_B:{}", n, B), |b| {
        let col = gen_u64_col::<B>(rng, n, 1);
        b.iter(|| seek(rng, &col))
    });
    for len in [8, 128] {
        c.bench_function(&format!("seek_str_n:{}_B:{}_len:{}", n, B, len), |b| {
            let col = gen_str_col::<B>(rng, n, len);
            b.iter(|| seek(rng, &col));
        });
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut rng = rand::thread_rng();
    let sizes = [100_000];
    //let sizes = [10, 100, 1_000, 10_000, 100_000];
    for n in sizes {
        /*
                bench_seek::<8>(c, &mut rng, n);
                bench_seek::<16>(c, &mut rng, n);
                bench_seek::<32>(c, &mut rng, n);
                bench_seek::<64>(c, &mut rng, n);
                bench_seek::<128>(c, &mut rng, n);
                bench_seek::<256>(c, &mut rng, n);
                bench_seek::<512>(c, &mut rng, n);
                bench_seek::<1024>(c, &mut rng, n);
                bench_seek::<2048>(c, &mut rng, n);
                bench_seek::<4096>(c, &mut rng, n);
        */
        bench_seek::<128>(c, &mut rng, n);
    }
}

fn gen_u64_col<const B: usize>(
    rng: &mut ThreadRng,
    n: usize,
    max: u64,
) -> ColumnData<RleCursor<B, u64>> {
    let mut col = ColumnData::new();
    let values: Vec<u64> = (0..n).map(|_| rng.gen::<u64>() % max).collect();
    col.splice(0, 0, values);
    col
}

fn gen_i64_col<const B: usize>(
    rng: &mut ThreadRng,
    n: usize,
    max: i64,
) -> ColumnData<RleCursor<B, i64>> {
    let mut col = ColumnData::new();
    let values: Vec<i64> = (0..n).map(|_| rng.gen::<i64>() % max).collect();
    col.splice(0, 0, values);
    col
}

fn gen_u32_col<const B: usize>(
    rng: &mut ThreadRng,
    n: usize,
    max: u32,
) -> ColumnData<RleCursor<B, u32>> {
    let mut col = ColumnData::new();
    let values: Vec<u32> = (0..n).map(|_| rng.gen::<u32>() % max).collect();
    col.splice(0, 0, values);
    col
}

fn gen_str_col<const B: usize>(
    rng: &mut ThreadRng,
    n: usize,
    strlen: usize,
) -> ColumnData<RleCursor<B, str>> {
    let mut col = ColumnData::new();
    let values: Vec<String> = (0..n)
        .map(|_| Alphanumeric.sample_string(rng, strlen))
        .collect();
    col.splice(0, 0, values);
    col
}

fn seek<const B: usize, P: Packable + ?Sized>(
    rng: &mut ThreadRng,
    col: &ColumnData<RleCursor<B, P>>,
) {
    let pos = rng.gen::<usize>() % col.len();
    col.get(pos);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
