use hexane::v1::Column;
use rand::{rng, Rng};
use std::time::Instant;

fn rand_bool() -> bool {
    rng().next_u64() % 2 == 0
}
fn rand_u64() -> u64 {
    rng().next_u64() % 1000
}
fn rand_usize(max: usize) -> usize {
    rng().next_u64() as usize % max
}

fn bench(name: &str, n: u64, mut f: impl FnMut()) {
    for _ in 0..1000 {
        f();
    }
    let start = Instant::now();
    for _ in 0..n {
        f();
    }
    let ns = start.elapsed().as_nanos() as f64 / n as f64;
    println!("{name}: {ns:.0} ns/iter");
}

fn main() {
    let bool_col = Column::<bool>::from_values((0..10_000).map(|_| rand_bool()).collect());
    let u64_col = Column::<u64>::from_values((0..10_000).map(|_| rand_u64()).collect());

    let mut c = bool_col.clone();
    bench("bool replace_1", 500_000, || {
        c.splice(rand_usize(c.len()), 1, [rand_bool()]);
    });

    let mut c = u64_col.clone();
    bench("u64  replace_1", 500_000, || {
        c.splice(rand_usize(c.len()), 1, [rand_u64()]);
    });

    let mut c = u64_col.clone();
    bench("u64  insert_1 ", 10_000, || {
        c.splice(rand_usize(c.len()), 0, [rand_u64()]);
    });

    let mut c = u64_col.clone();
    bench("u64  delete_1 ", 10_000, || {
        if c.len() > 1 {
            c.splice(rand_usize(c.len()), 1, std::iter::empty::<u64>());
        }
    });
}
