use divan::Bencher;
use hexane::v1;
use std::time::Duration;

use rand::{rng, Rng};

fn main() {
    divan::main();
}

const URANGE: u64 = 1000;

fn rand_u64() -> u64 {
    rng().next_u64() % URANGE
}
fn rand_bool() -> bool {
    rng().next_u64() % 2 == 0
}
fn rand_usize() -> usize {
    rng().next_u64() as usize
}
fn rand_vals(n: usize) -> Vec<u64> {
    (0..n).map(|_| rand_u64()).collect()
}
fn rand_bools(n: usize) -> Vec<bool> {
    (0..n).map(|_| rand_bool()).collect()
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn build_v1(n: usize, ms: usize) -> v1::Column<u64> {
    v1::Column::from_values_with_max_segments(rand_vals(n), ms)
}

fn build_v1_bool(n: usize, ms: usize) -> v1::Column<bool> {
    v1::Column::from_values_with_max_segments(rand_bools(n), ms)
}

// ── Bulk load u64 100k ───────────────────────────────────────────────────────

#[divan::bench_group(name = "bulk_load_u64_100k")]
mod bulk_load_u64 {
    use super::*;

    const N: usize = 100_000;

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn ms_4(b: Bencher) {
        let v = rand_vals(N);
        b.bench_local(|| v1::Column::<u64>::from_values_with_max_segments(v.clone(), 4));
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn ms_8(b: Bencher) {
        let v = rand_vals(N);
        b.bench_local(|| v1::Column::<u64>::from_values_with_max_segments(v.clone(), 8));
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn ms_16(b: Bencher) {
        let v = rand_vals(N);
        b.bench_local(|| v1::Column::<u64>::from_values_with_max_segments(v.clone(), 16));
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn ms_32(b: Bencher) {
        let v = rand_vals(N);
        b.bench_local(|| v1::Column::<u64>::from_values_with_max_segments(v.clone(), 32));
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn ms_64(b: Bencher) {
        let v = rand_vals(N);
        b.bench_local(|| v1::Column::<u64>::from_values_with_max_segments(v.clone(), 64));
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn ms_128(b: Bencher) {
        let v = rand_vals(N);
        b.bench_local(|| v1::Column::<u64>::from_values_with_max_segments(v.clone(), 128));
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn ms_256(b: Bencher) {
        let v = rand_vals(N);
        b.bench_local(|| v1::Column::<u64>::from_values_with_max_segments(v.clone(), 256));
    }
}

// ── Get u64 100k ─────────────────────────────────────────────────────────────

#[divan::bench_group(name = "get_u64_100k")]
mod get_u64 {
    use super::*;

    const N: usize = 100_000;

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn ms_4(b: Bencher) {
        let col = build_v1(N, 4);
        b.bench_local(|| col.get(rand_usize() % col.len()));
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn ms_8(b: Bencher) {
        let col = build_v1(N, 8);
        b.bench_local(|| col.get(rand_usize() % col.len()));
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn ms_16(b: Bencher) {
        let col = build_v1(N, 16);
        b.bench_local(|| col.get(rand_usize() % col.len()));
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn ms_32(b: Bencher) {
        let col = build_v1(N, 32);
        b.bench_local(|| col.get(rand_usize() % col.len()));
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn ms_64(b: Bencher) {
        let col = build_v1(N, 64);
        b.bench_local(|| col.get(rand_usize() % col.len()));
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn ms_128(b: Bencher) {
        let col = build_v1(N, 128);
        b.bench_local(|| col.get(rand_usize() % col.len()));
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn ms_256(b: Bencher) {
        let col = build_v1(N, 256);
        b.bench_local(|| col.get(rand_usize() % col.len()));
    }
}

// ── Insert-1 u64 10k ─────────────────────────────────────────────────────────

#[divan::bench_group(name = "insert1_u64_10k")]
mod insert1_u64 {
    use super::*;

    const N: usize = 10_000;

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn ms_4(b: Bencher) {
        let mut col = build_v1(N, 4);
        b.bench_local(|| {
            col.insert(rand_usize() % col.len(), rand_u64());
        });
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn ms_8(b: Bencher) {
        let mut col = build_v1(N, 8);
        b.bench_local(|| {
            col.insert(rand_usize() % col.len(), rand_u64());
        });
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn ms_16(b: Bencher) {
        let mut col = build_v1(N, 16);
        b.bench_local(|| {
            col.insert(rand_usize() % col.len(), rand_u64());
        });
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn ms_32(b: Bencher) {
        let mut col = build_v1(N, 32);
        b.bench_local(|| {
            col.insert(rand_usize() % col.len(), rand_u64());
        });
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn ms_64(b: Bencher) {
        let mut col = build_v1(N, 64);
        b.bench_local(|| {
            col.insert(rand_usize() % col.len(), rand_u64());
        });
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn ms_128(b: Bencher) {
        let mut col = build_v1(N, 128);
        b.bench_local(|| {
            col.insert(rand_usize() % col.len(), rand_u64());
        });
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn ms_256(b: Bencher) {
        let mut col = build_v1(N, 256);
        b.bench_local(|| {
            col.insert(rand_usize() % col.len(), rand_u64());
        });
    }
}

// ── Splice replace-5 u64 10k ─────────────────────────────────────────────────

#[divan::bench_group(name = "splice_replace5_u64_10k")]
mod splice_replace5 {
    use super::*;

    const N: usize = 10_000;

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn ms_4(b: Bencher) {
        let mut col = build_v1(N, 4);
        b.bench_local(|| {
            let len = col.len();
            if len < 6 {
                return;
            }
            col.splice(rand_usize() % (len - 5), 5, rand_vals(5));
        });
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn ms_8(b: Bencher) {
        let mut col = build_v1(N, 8);
        b.bench_local(|| {
            let len = col.len();
            if len < 6 {
                return;
            }
            col.splice(rand_usize() % (len - 5), 5, rand_vals(5));
        });
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn ms_16(b: Bencher) {
        let mut col = build_v1(N, 16);
        b.bench_local(|| {
            let len = col.len();
            if len < 6 {
                return;
            }
            col.splice(rand_usize() % (len - 5), 5, rand_vals(5));
        });
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn ms_32(b: Bencher) {
        let mut col = build_v1(N, 32);
        b.bench_local(|| {
            let len = col.len();
            if len < 6 {
                return;
            }
            col.splice(rand_usize() % (len - 5), 5, rand_vals(5));
        });
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn ms_64(b: Bencher) {
        let mut col = build_v1(N, 64);
        b.bench_local(|| {
            let len = col.len();
            if len < 6 {
                return;
            }
            col.splice(rand_usize() % (len - 5), 5, rand_vals(5));
        });
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn ms_128(b: Bencher) {
        let mut col = build_v1(N, 128);
        b.bench_local(|| {
            let len = col.len();
            if len < 6 {
                return;
            }
            col.splice(rand_usize() % (len - 5), 5, rand_vals(5));
        });
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn ms_256(b: Bencher) {
        let mut col = build_v1(N, 256);
        b.bench_local(|| {
            let len = col.len();
            if len < 6 {
                return;
            }
            col.splice(rand_usize() % (len - 5), 5, rand_vals(5));
        });
    }
}

// ── Get bool 100k ────────────────────────────────────────────────────────────

#[divan::bench_group(name = "get_bool_100k")]
mod get_bool {
    use super::*;

    const N: usize = 100_000;

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn ms_4(b: Bencher) {
        let col = build_v1_bool(N, 4);
        b.bench_local(|| col.get(rand_usize() % col.len()));
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn ms_8(b: Bencher) {
        let col = build_v1_bool(N, 8);
        b.bench_local(|| col.get(rand_usize() % col.len()));
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn ms_16(b: Bencher) {
        let col = build_v1_bool(N, 16);
        b.bench_local(|| col.get(rand_usize() % col.len()));
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn ms_32(b: Bencher) {
        let col = build_v1_bool(N, 32);
        b.bench_local(|| col.get(rand_usize() % col.len()));
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn ms_64(b: Bencher) {
        let col = build_v1_bool(N, 64);
        b.bench_local(|| col.get(rand_usize() % col.len()));
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn ms_128(b: Bencher) {
        let col = build_v1_bool(N, 128);
        b.bench_local(|| col.get(rand_usize() % col.len()));
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn ms_256(b: Bencher) {
        let col = build_v1_bool(N, 256);
        b.bench_local(|| col.get(rand_usize() % col.len()));
    }
}

// ── Insert-1 bool 10k ────────────────────────────────────────────────────────

#[divan::bench_group(name = "insert1_bool_10k")]
mod insert1_bool {
    use super::*;

    const N: usize = 10_000;

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn ms_4(b: Bencher) {
        let mut col = build_v1_bool(N, 4);
        b.bench_local(|| {
            col.insert(rand_usize() % col.len(), rand_bool());
        });
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn ms_8(b: Bencher) {
        let mut col = build_v1_bool(N, 8);
        b.bench_local(|| {
            col.insert(rand_usize() % col.len(), rand_bool());
        });
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn ms_16(b: Bencher) {
        let mut col = build_v1_bool(N, 16);
        b.bench_local(|| {
            col.insert(rand_usize() % col.len(), rand_bool());
        });
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn ms_32(b: Bencher) {
        let mut col = build_v1_bool(N, 32);
        b.bench_local(|| {
            col.insert(rand_usize() % col.len(), rand_bool());
        });
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn ms_64(b: Bencher) {
        let mut col = build_v1_bool(N, 64);
        b.bench_local(|| {
            col.insert(rand_usize() % col.len(), rand_bool());
        });
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn ms_128(b: Bencher) {
        let mut col = build_v1_bool(N, 128);
        b.bench_local(|| {
            col.insert(rand_usize() % col.len(), rand_bool());
        });
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn ms_256(b: Bencher) {
        let mut col = build_v1_bool(N, 256);
        b.bench_local(|| {
            col.insert(rand_usize() % col.len(), rand_bool());
        });
    }
}

// ── Save u64 100k ────────────────────────────────────────────────────────────

#[divan::bench_group(name = "save_u64_100k")]
mod save_u64 {
    use super::*;

    const N: usize = 100_000;

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3), sample_count = 20)]
    fn ms_4(b: Bencher) {
        let col = build_v1(N, 4);
        b.bench_local(|| col.save());
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3), sample_count = 20)]
    fn ms_8(b: Bencher) {
        let col = build_v1(N, 8);
        b.bench_local(|| col.save());
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3), sample_count = 20)]
    fn ms_16(b: Bencher) {
        let col = build_v1(N, 16);
        b.bench_local(|| col.save());
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3), sample_count = 20)]
    fn ms_32(b: Bencher) {
        let col = build_v1(N, 32);
        b.bench_local(|| col.save());
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3), sample_count = 20)]
    fn ms_64(b: Bencher) {
        let col = build_v1(N, 64);
        b.bench_local(|| col.save());
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3), sample_count = 20)]
    fn ms_128(b: Bencher) {
        let col = build_v1(N, 128);
        b.bench_local(|| col.save());
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3), sample_count = 20)]
    fn ms_256(b: Bencher) {
        let col = build_v1(N, 256);
        b.bench_local(|| col.save());
    }
}

// ── Slab count (informational) ───────────────────────────────────────────────

#[divan::bench_group(name = "slab_count_u64_100k")]
mod slab_count {
    use super::*;

    const N: usize = 100_000;

    // These "benches" just print the slab count so we can see the structural effect.
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_millis(100), sample_count = 1)]
    fn ms_4(b: Bencher) {
        let col = build_v1(N, 4);
        eprintln!("  ms=4   slabs={}", col.slab_count());
        b.bench_local(|| col.len());
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_millis(100), sample_count = 1)]
    fn ms_8(b: Bencher) {
        let col = build_v1(N, 8);
        eprintln!("  ms=8   slabs={}", col.slab_count());
        b.bench_local(|| col.len());
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_millis(100), sample_count = 1)]
    fn ms_16(b: Bencher) {
        let col = build_v1(N, 16);
        eprintln!("  ms=16  slabs={}", col.slab_count());
        b.bench_local(|| col.len());
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_millis(100), sample_count = 1)]
    fn ms_32(b: Bencher) {
        let col = build_v1(N, 32);
        eprintln!("  ms=32  slabs={}", col.slab_count());
        b.bench_local(|| col.len());
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_millis(100), sample_count = 1)]
    fn ms_64(b: Bencher) {
        let col = build_v1(N, 64);
        eprintln!("  ms=64  slabs={}", col.slab_count());
        b.bench_local(|| col.len());
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_millis(100), sample_count = 1)]
    fn ms_128(b: Bencher) {
        let col = build_v1(N, 128);
        eprintln!("  ms=128 slabs={}", col.slab_count());
        b.bench_local(|| col.len());
    }
    #[inline(never)]
    #[divan::bench(max_time = Duration::from_millis(100), sample_count = 1)]
    fn ms_256(b: Bencher) {
        let col = build_v1(N, 256);
        eprintln!("  ms=256 slabs={}", col.slab_count());
        b.bench_local(|| col.len());
    }
}
