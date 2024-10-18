use criterion::{criterion_group, criterion_main, Criterion};
use packer::*;
use rand::prelude::*;

const MIN: u64 = 1;
const MAX: u64 = u32::MAX as u64;

fn criterion_benchmark(c: &mut Criterion) {
    let mut rng = rand::thread_rng();
    for n in [100_000] {
        c.bench_function(&format!("insert_u64_n:{}_large", n), |b| {
            let mut col = gen_col(&mut rng, n, MAX);
            b.iter(|| insert(&mut rng, &mut col, MAX))
        });
        c.bench_function(&format!("insert_u64_n:{}_small", n), |b| {
            let mut col = gen_col(&mut rng, n, MIN);
            b.iter(|| insert(&mut rng, &mut col, MIN))
        });
    }
}

fn gen_col(rng: &mut ThreadRng, n: usize, max: u64) -> ColumnData<IntCursor> {
    (0..n).map(|_| rng.gen::<u64>() % max).collect()
}

fn insert(rng: &mut ThreadRng, col: &mut ColumnData<IntCursor>, max: u64) {
    let val: u64 = rng.gen();
    let val = val % max;
    let pos: usize = rng.gen();
    let pos = pos % (col.len() + 1);
    col.splice(pos, 0, vec![val]);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
