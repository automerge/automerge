use criterion::{black_box, criterion_group, criterion_main, Criterion};
use packer::*;
use rand::prelude::*;

const MIN: u64 = 1;
const MAX: u64 = u32::MAX as u64;

fn criterion_benchmark(c: &mut Criterion) {
    let mut rng = rand::thread_rng();
    //for n in vec![10, 100, 1_000, 10_000] {
    for n in [10_000, 100_000] {
        c.bench_function(&format!("seek_{}_large", n), |b| {
            let mut col = gen_col(&mut rng, n, MAX);
            b.iter(|| seek(&mut rng, black_box(&mut col)))
        });
        c.bench_function(&format!("seek_{}_small", n), |b| {
            let mut col = gen_col(&mut rng, n, MIN);
            b.iter(|| seek(&mut rng, black_box(&mut col)))
        });
    }
}

fn gen_col(rng: &mut ThreadRng, n: usize, max: u64) -> ColumnData<IntCursor> {
    let mut col: ColumnData<IntCursor> = ColumnData::new();
    let values: Vec<u64> = (0..n).map(|_| rng.gen::<u64>() % max).collect();
    col.splice(0, 0, values);
    col
}

fn seek(rng: &mut ThreadRng, col: &mut ColumnData<IntCursor>) {
    let pos = rng.gen::<usize>() % col.len();
    col.get(pos);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
