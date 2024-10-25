use criterion::{criterion_group, criterion_main, Criterion};
use packer::*;
use rand::distributions::Standard;
use rand::prelude::*;
use std::ops::Rem;

fn criterion_benchmark(c: &mut Criterion) {
    let mut rng = rand::thread_rng();
    for n in [100_000] {
        c.bench_function(&format!("insert_u64_n:{}_large", n), |b| {
            let max = u32::MAX as u64;
            let mut col = gen_col(&mut rng, n, max).collect();
            b.iter(|| int_insert(&mut rng, &mut col, max))
        });

        c.bench_function(&format!("insert_delta_n:{}_large", n), |b| {
            let max = u32::MAX as i64;
            let mut col = gen_col(&mut rng, n, max).collect();
            b.iter(|| delta_insert(&mut rng, &mut col, max))
        });
    }
}

fn gen_col<'a, N>(rng: &'a mut ThreadRng, n: usize, max: N) -> impl Iterator<Item = N> + 'a
where
    Standard: Distribution<N>,
    N: Rem<Output = N> + Copy + 'static,
{
    (0..n).map(move |_| rng.gen::<N>() % max)
}

fn int_insert(rng: &mut ThreadRng, col: &mut ColumnData<IntCursor>, max: u64) {
    let val = rng.gen::<u64>() % max;
    let pos: usize = rng.gen();
    let pos = pos % (col.len() + 1);
    col.splice(pos, 0, vec![val]);
}

fn delta_insert(rng: &mut ThreadRng, col: &mut ColumnData<DeltaCursor>, max: i64) {
    let val = rng.gen::<i64>() % max;
    let pos: usize = rng.gen();
    let pos = pos % (col.len() + 1);
    col.splice(pos, 0, vec![val]);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
