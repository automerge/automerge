#![allow(clippy::len_zero)]

use divan::Bencher;
use hexane::v1::{DeltaColumn, PrefixColumn};
use hexane::*;
use std::time::Duration;

use rand::{rng, Rng, RngExt};

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

// ── Helpers: build columns ──────────────────────────────────────────────────

fn build_v0(n: usize) -> ColumnData<UIntCursor> {
    (0..n).map(|_| rand_u64()).collect()
}

fn build_v1(n: usize) -> v1::Column<u64> {
    v1::Column::from_values((0..n).map(|_| rand_u64()).collect())
}

fn build_v0_bool(n: usize) -> ColumnData<BooleanCursor> {
    (0..n).map(|_| rand_bool()).collect()
}

fn build_v1_bool(n: usize) -> v1::Column<bool> {
    v1::Column::from_values((0..n).map(|_| rand_bool()).collect())
}

fn rand_string(len: usize) -> String {
    let mut r = rng();
    (0..len)
        .map(|_| (b'a' + (r.random::<u8>() % 26)) as char)
        .collect()
}

fn rand_vals(n: usize) -> Vec<u64> {
    (0..n).map(|_| rand_u64()).collect()
}

fn rand_bools(n: usize) -> Vec<bool> {
    (0..n).map(|_| rand_bool()).collect()
}

// ── Get (random access) ─────────────────────────────────────────────────────

#[divan::bench_group(name = "get")]
mod get {
    use super::*;

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn v0_get_10k(bencher: Bencher) {
        let col = build_v0(10_000);
        bencher.bench_local(|| {
            let pos = rand_usize() % col.len();
            col.get(pos)
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn v1_get_10k(bencher: Bencher) {
        let col = build_v1(10_000);
        bencher.bench_local(|| {
            let pos = rand_usize() % col.len();
            col.get(pos)
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn v0_get_100k(bencher: Bencher) {
        let col = build_v0(100_000);
        bencher.bench_local(|| {
            let pos = rand_usize() % col.len();
            col.get(pos)
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn v1_get_100k(bencher: Bencher) {
        let col = build_v1(100_000);
        bencher.bench_local(|| {
            let pos = rand_usize() % col.len();
            col.get(pos)
        });
    }
}

// ── Small splice: insert only (no deletes) ──────────────────────────────────

#[divan::bench_group(name = "u64_insert")]
mod u64_insert {
    use super::*;

    #[divan::bench(args = [1, 10, 1000], max_time = Duration::from_secs(3))]
    fn v0(bencher: Bencher, n: usize) {
        let mut col = build_v0(10_000);
        bencher.bench_local(|| {
            let pos = rand_usize() % col.len();
            col.splice(pos, 0, rand_vals(n));
        });
    }

    #[divan::bench(args = [1, 10, 1000], max_time = Duration::from_secs(3))]
    fn v1(bencher: Bencher, n: usize) {
        let mut col = build_v1(10_000);
        bencher.bench_local(|| {
            let pos = rand_usize() % col.len();
            col.splice(pos, 0, rand_vals(n));
        });
    }
}

#[divan::bench_group(name = "u64_replace")]
mod u64_replace {
    use super::*;

    #[divan::bench(args = [1, 10, 1000], max_time = Duration::from_secs(3))]
    fn v0(bencher: Bencher, n: usize) {
        let mut col = build_v0(10_000);
        bencher.bench_local(|| {
            let len = col.len();
            if len <= n {
                return;
            }
            let pos = rand_usize() % (len - n);
            col.splice(pos, n, rand_vals(n));
        });
    }

    #[divan::bench(args = [1, 10, 1000], max_time = Duration::from_secs(3))]
    fn v1(bencher: Bencher, n: usize) {
        let mut col = build_v1(10_000);
        bencher.bench_local(|| {
            let len = col.len();
            if len <= n {
                return;
            }
            let pos = rand_usize() % (len - n);
            col.splice(pos, n, rand_vals(n));
        });
    }
}

#[divan::bench_group(name = "u64_delete")]
mod u64_delete {
    use super::*;

    #[divan::bench(args = [1, 10, 1000], max_time = Duration::from_secs(3))]
    fn v0(bencher: Bencher, n: usize) {
        let mut col = build_v0(10_000);
        bencher.bench_local(|| {
            let len = col.len();
            if len <= n {
                return;
            }
            let pos = rand_usize() % (len - n);
            col.splice(pos, n, std::iter::empty::<u64>());
        });
    }

    #[divan::bench(args = [1, 10, 1000], max_time = Duration::from_secs(3))]
    fn v1(bencher: Bencher, n: usize) {
        let mut col = build_v1(10_000);
        bencher.bench_local(|| {
            let len = col.len();
            if len <= n {
                return;
            }
            let pos = rand_usize() % (len - n);
            col.splice(pos, n, std::iter::empty::<u64>());
        });
    }
}

// ── String splice ───────────────────────────────────────────────────────────

fn rand_strings(n: usize, len: usize) -> Vec<String> {
    (0..n).map(|_| rand_string(len)).collect()
}

fn build_v0_str(n: usize, slen: usize) -> ColumnData<StrCursor> {
    (0..n).map(|_| rand_string(slen)).collect()
}

fn build_v1_str(n: usize, slen: usize) -> v1::Column<String> {
    v1::Column::from_values((0..n).map(|_| rand_string(slen)).collect())
}

#[divan::bench_group(name = "string_replace_8b")]
mod string_replace_8b {
    use super::*;

    #[divan::bench(args = [1, 10, 1000], max_time = Duration::from_secs(3))]
    fn v0(bencher: Bencher, n: usize) {
        let mut col = build_v0_str(10_000, 8);
        bencher.bench_local(|| {
            let len = col.len();
            if len <= n {
                return;
            }
            let pos = rand_usize() % (len - n);
            col.splice(pos, n, rand_strings(n, 8));
        });
    }

    #[divan::bench(args = [1, 10, 1000], max_time = Duration::from_secs(3))]
    fn v1(bencher: Bencher, n: usize) {
        let mut col = build_v1_str(10_000, 8);
        bencher.bench_local(|| {
            let len = col.len();
            if len <= n {
                return;
            }
            let pos = rand_usize() % (len - n);
            col.splice(pos, n, rand_strings(n, 8));
        });
    }
}

#[divan::bench_group(name = "string_replace_10b")]
mod string_replace_10b {
    use super::*;

    #[divan::bench(args = [1, 10, 1000], max_time = Duration::from_secs(3))]
    fn v0(bencher: Bencher, n: usize) {
        let mut col = build_v0_str(10_000, 10);
        bencher.bench_local(|| {
            let len = col.len();
            if len <= n {
                return;
            }
            let pos = rand_usize() % (len - n);
            col.splice(pos, n, rand_strings(n, 10));
        });
    }

    #[divan::bench(args = [1, 10, 1000], max_time = Duration::from_secs(3))]
    fn v1(bencher: Bencher, n: usize) {
        let mut col = build_v1_str(10_000, 10);
        bencher.bench_local(|| {
            let len = col.len();
            if len <= n {
                return;
            }
            let pos = rand_usize() % (len - n);
            col.splice(pos, n, rand_strings(n, 10));
        });
    }
}

#[divan::bench_group(name = "string_replace_1kb")]
mod string_replace_1kb {
    use super::*;

    #[divan::bench(args = [1, 10, 1000], max_time = Duration::from_secs(3))]
    fn v0(bencher: Bencher, n: usize) {
        let mut col = build_v0_str(10_000, 1024);
        bencher.bench_local(|| {
            let len = col.len();
            if len <= n {
                return;
            }
            let pos = rand_usize() % (len - n);
            col.splice(pos, n, rand_strings(n, 1024));
        });
    }

    #[divan::bench(args = [1, 10, 1000], max_time = Duration::from_secs(3))]
    fn v1(bencher: Bencher, n: usize) {
        let mut col = build_v1_str(10_000, 1024);
        bencher.bench_local(|| {
            let len = col.len();
            if len <= n {
                return;
            }
            let pos = rand_usize() % (len - n);
            col.splice(pos, n, rand_strings(n, 1024));
        });
    }
}

// ── Large splice: insert 100k elements into a 1k column ─────────────────────

#[divan::bench_group(name = "splice_large_insert")]
mod splice_large_insert {
    use super::*;

    const LARGE: usize = 100_000;

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(10), sample_count = 10)]
    fn v0_insert_100k(bencher: Bencher) {
        let vals = rand_vals(LARGE);
        bencher.bench_local(|| {
            let mut col = build_v0(1_000);
            let pos = rand_usize() % col.len();
            col.splice(pos, 0, vals.clone());
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(10), sample_count = 10)]
    fn v1_insert_100k(bencher: Bencher) {
        let vals = rand_vals(LARGE);
        bencher.bench_local(|| {
            let mut col = build_v1(1_000);
            let pos = rand_usize() % col.len();
            col.splice(pos, 0, vals.clone());
        });
    }
}

// ── Large splice: delete only ───────────────────────────────────────────────

#[divan::bench_group(name = "splice_large_delete")]
mod splice_large_delete {
    use super::*;

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(10), sample_count = 10)]
    fn v0_delete_1k_of_10k(bencher: Bencher) {
        bencher.bench_local(|| {
            let mut col = build_v0(10_000);
            let pos = rand_usize() % (col.len() - 1_000);
            col.splice(pos, 1_000, std::iter::empty::<u64>());
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(10), sample_count = 10)]
    fn v1_delete_1k_of_10k(bencher: Bencher) {
        bencher.bench_local(|| {
            let mut col = build_v1(10_000);
            let pos = rand_usize() % (col.len() - 1_000);
            col.splice(pos, 1_000, std::iter::empty::<u64>());
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(10), sample_count = 10)]
    fn v0_delete_50k_of_100k(bencher: Bencher) {
        bencher.bench_local(|| {
            let mut col = build_v0(100_000);
            let pos = rand_usize() % (col.len() / 2);
            col.splice(pos, 50_000, std::iter::empty::<u64>());
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(10), sample_count = 10)]
    fn v1_delete_50k_of_100k(bencher: Bencher) {
        bencher.bench_local(|| {
            let mut col = build_v1(100_000);
            let pos = rand_usize() % (col.len() / 2);
            col.splice(pos, 50_000, std::iter::empty::<u64>());
        });
    }
}

// ── Large splice: replace ───────────────────────────────────────────────────

#[divan::bench_group(name = "splice_large_replace")]
mod splice_large_replace {
    use super::*;

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(10), sample_count = 10)]
    fn v0_replace_1k_of_10k(bencher: Bencher) {
        let vals = rand_vals(1_000);
        bencher.bench_local(|| {
            let mut col = build_v0(10_000);
            let pos = rand_usize() % (col.len() - 1_000);
            col.splice(pos, 1_000, vals.clone());
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(10), sample_count = 10)]
    fn v1_replace_1k_of_10k(bencher: Bencher) {
        let vals = rand_vals(1_000);
        bencher.bench_local(|| {
            let mut col = build_v1(10_000);
            let pos = rand_usize() % (col.len() - 1_000);
            col.splice(pos, 1_000, vals.clone());
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(10), sample_count = 10)]
    fn v0_replace_50k_of_100k(bencher: Bencher) {
        let vals = rand_vals(50_000);
        bencher.bench_local(|| {
            let mut col = build_v0(100_000);
            let pos = rand_usize() % (col.len() / 2);
            col.splice(pos, 50_000, vals.clone());
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(10), sample_count = 10)]
    fn v1_replace_50k_of_100k(bencher: Bencher) {
        let vals = rand_vals(50_000);
        bencher.bench_local(|| {
            let mut col = build_v1(100_000);
            let pos = rand_usize() % (col.len() / 2);
            col.splice(pos, 50_000, vals.clone());
        });
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Boolean benchmarks
// ═══════════════════════════════════════════════════════════════════════════

// ── Bool get (random access) ────────────────────────────────────────────────

#[divan::bench_group(name = "bool_get")]
mod bool_get {
    use super::*;

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn v0_get_10k(bencher: Bencher) {
        let col = build_v0_bool(10_000);
        bencher.bench_local(|| {
            let pos = rand_usize() % col.len();
            col.get(pos)
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn v1_get_10k(bencher: Bencher) {
        let col = build_v1_bool(10_000);
        bencher.bench_local(|| {
            let pos = rand_usize() % col.len();
            col.get(pos)
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn v0_get_100k(bencher: Bencher) {
        let col = build_v0_bool(100_000);
        bencher.bench_local(|| {
            let pos = rand_usize() % col.len();
            col.get(pos)
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn v1_get_100k(bencher: Bencher) {
        let col = build_v1_bool(100_000);
        bencher.bench_local(|| {
            let pos = rand_usize() % col.len();
            col.get(pos)
        });
    }
}

// ── Bool small splice: replace ──────────────────────────────────────────────

#[divan::bench_group(name = "bool_insert")]
mod bool_insert {
    use super::*;

    #[divan::bench(args = [1, 10, 1000], max_time = Duration::from_secs(3))]
    fn v0(bencher: Bencher, n: usize) {
        let mut col = build_v0_bool(10_000);
        bencher.bench_local(|| {
            let pos = rand_usize() % col.len();
            col.splice(pos, 0, rand_bools(n));
        });
    }

    #[divan::bench(args = [1, 10, 1000], max_time = Duration::from_secs(3))]
    fn v1(bencher: Bencher, n: usize) {
        let mut col = build_v1_bool(10_000);
        bencher.bench_local(|| {
            let pos = rand_usize() % col.len();
            col.splice(pos, 0, rand_bools(n));
        });
    }
}

#[divan::bench_group(name = "bool_replace")]
mod bool_replace {
    use super::*;

    #[divan::bench(args = [1, 10, 1000], max_time = Duration::from_secs(3))]
    fn v0(bencher: Bencher, n: usize) {
        let mut col = build_v0_bool(10_000);
        bencher.bench_local(|| {
            let len = col.len();
            if len <= n {
                return;
            }
            let pos = rand_usize() % (len - n);
            col.splice(pos, n, rand_bools(n));
        });
    }

    #[divan::bench(args = [1, 10, 1000], max_time = Duration::from_secs(3))]
    fn v1(bencher: Bencher, n: usize) {
        let mut col = build_v1_bool(10_000);
        bencher.bench_local(|| {
            let len = col.len();
            if len <= n {
                return;
            }
            let pos = rand_usize() % (len - n);
            col.splice(pos, n, rand_bools(n));
        });
    }
}

#[divan::bench_group(name = "bool_delete")]
mod bool_delete {
    use super::*;

    #[divan::bench(args = [1, 10, 1000], max_time = Duration::from_secs(3))]
    fn v0(bencher: Bencher, n: usize) {
        let mut col = build_v0_bool(10_000);
        bencher.bench_local(|| {
            let len = col.len();
            if len <= n {
                return;
            }
            let pos = rand_usize() % (len - n);
            col.splice(pos, n, std::iter::empty::<bool>());
        });
    }

    #[divan::bench(args = [1, 10, 1000], max_time = Duration::from_secs(3))]
    fn v1(bencher: Bencher, n: usize) {
        let mut col = build_v1_bool(10_000);
        bencher.bench_local(|| {
            let len = col.len();
            if len <= n {
                return;
            }
            let pos = rand_usize() % (len - n);
            col.splice(pos, n, std::iter::empty::<bool>());
        });
    }
}

// ── Bool large splice: insert 100k ──────────────────────────────────────────

#[divan::bench_group(name = "bool_splice_large_insert")]
mod bool_splice_large_insert {
    use super::*;

    const LARGE: usize = 100_000;

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(10), sample_count = 10)]
    fn v0_insert_100k(bencher: Bencher) {
        let vals = rand_bools(LARGE);
        bencher.bench_local(|| {
            let mut col = build_v0_bool(1_000);
            let pos = rand_usize() % col.len();
            col.splice(pos, 0, vals.clone());
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(10), sample_count = 10)]
    fn v1_insert_100k(bencher: Bencher) {
        let vals = rand_bools(LARGE);
        bencher.bench_local(|| {
            let mut col = build_v1_bool(1_000);
            let pos = rand_usize() % col.len();
            col.splice(pos, 0, vals.clone());
        });
    }
}

// ── Bool large splice: delete only ──────────────────────────────────────────

#[divan::bench_group(name = "bool_splice_large_delete")]
mod bool_splice_large_delete {
    use super::*;

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(10), sample_count = 10)]
    fn v0_delete_1k_of_10k(bencher: Bencher) {
        bencher.bench_local(|| {
            let mut col = build_v0_bool(10_000);
            let pos = rand_usize() % (col.len() - 1_000);
            col.splice(pos, 1_000, std::iter::empty::<bool>());
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(10), sample_count = 10)]
    fn v1_delete_1k_of_10k(bencher: Bencher) {
        bencher.bench_local(|| {
            let mut col = build_v1_bool(10_000);
            let pos = rand_usize() % (col.len() - 1_000);
            col.splice(pos, 1_000, std::iter::empty::<bool>());
        });
    }
}

// ── Bool large splice: replace ──────────────────────────────────────────────

#[divan::bench_group(name = "bool_splice_large_replace")]
mod bool_splice_large_replace {
    use super::*;

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(10), sample_count = 10)]
    fn v0_replace_1k_of_10k(bencher: Bencher) {
        let vals = rand_bools(1_000);
        bencher.bench_local(|| {
            let mut col = build_v0_bool(10_000);
            let pos = rand_usize() % (col.len() - 1_000);
            col.splice(pos, 1_000, vals.clone());
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(10), sample_count = 10)]
    fn v1_replace_1k_of_10k(bencher: Bencher) {
        let vals = rand_bools(1_000);
        bencher.bench_local(|| {
            let mut col = build_v1_bool(10_000);
            let pos = rand_usize() % (col.len() - 1_000);
            col.splice(pos, 1_000, vals.clone());
        });
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Load benchmarks
// ═══════════════════════════════════════════════════════════════════════════

#[divan::bench_group(name = "load_u64")]
mod load_u64 {
    use super::*;

    const N: usize = 100_000;

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(10), sample_count = 20)]
    fn v0_load_100k(bencher: Bencher) {
        let col: ColumnData<UIntCursor> = (0..N).map(|_| rand_u64()).collect();
        let bytes = col.save();
        bencher.bench_local(|| ColumnData::<UIntCursor>::load(&bytes).unwrap());
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(10), sample_count = 20)]
    fn v1_load_100k(bencher: Bencher) {
        let col = v1::Column::<u64>::from_values((0..N).map(|_| rand_u64()).collect());
        let bytes = col.save();
        bencher.bench_local(|| v1::Column::<u64>::load(&bytes).unwrap());
    }
}

#[divan::bench_group(name = "load_string")]
mod load_string {
    use super::*;

    const N: usize = 100_000;
    const STR_LEN: usize = 20;

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(10), sample_count = 20)]
    fn v0_load_100k(bencher: Bencher) {
        let col: ColumnData<StrCursor> = (0..N).map(|_| rand_string(STR_LEN)).collect();
        let bytes = col.save();
        bencher.bench_local(|| ColumnData::<StrCursor>::load(&bytes).unwrap());
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(10), sample_count = 20)]
    fn v1_load_100k(bencher: Bencher) {
        let col = v1::Column::<String>::from_values((0..N).map(|_| rand_string(STR_LEN)).collect());
        let bytes = col.save();
        bencher.bench_local(|| v1::Column::<String>::load(&bytes).unwrap());
    }
}

#[divan::bench_group(name = "load_bool")]
mod load_bool {
    use super::*;

    const N: usize = 100_000;

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(10), sample_count = 20)]
    fn v0_load_100k(bencher: Bencher) {
        let col: ColumnData<BooleanCursor> = (0..N).map(|_| rand_bool()).collect();
        let bytes = col.save();
        bencher.bench_local(|| ColumnData::<BooleanCursor>::load(&bytes).unwrap());
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(10), sample_count = 20)]
    fn v1_load_100k(bencher: Bencher) {
        let col = v1::Column::<bool>::from_values((0..N).map(|_| rand_bool()).collect());
        let bytes = col.save();
        bencher.bench_local(|| v1::Column::<bool>::load(&bytes).unwrap());
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// PrefixColumn benchmarks
// ═══════════════════════════════════════════════════════════════════════════

fn build_prefix(n: usize) -> PrefixColumn<u64> {
    PrefixColumn::from_values((0..n).map(|_| rand_u64()).collect())
}

fn build_prefix_bool(n: usize) -> PrefixColumn<bool> {
    PrefixColumn::from_values((0..n).map(|_| rand_bool()).collect())
}

// ── Bulk load: v0 vs v1 vs prefix ────────────────────────────────────────────

#[divan::bench_group(name = "bulk_load_u64")]
mod bulk_load_u64 {
    use super::*;

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v0_from_iter_10k(bencher: Bencher) {
        let vals = rand_vals(10_000);
        bencher.bench_local(|| {
            let _: ColumnData<UIntCursor> = vals.iter().copied().collect();
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v1_from_values_10k(bencher: Bencher) {
        let vals = rand_vals(10_000);
        bencher.bench_local(|| {
            v1::Column::<u64>::from_values(vals.clone());
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn prefix_from_values_10k(bencher: Bencher) {
        let vals = rand_vals(10_000);
        bencher.bench_local(|| {
            PrefixColumn::<u64>::from_values(vals.clone());
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v0_from_iter_100k(bencher: Bencher) {
        let vals = rand_vals(100_000);
        bencher.bench_local(|| {
            let _: ColumnData<UIntCursor> = vals.iter().copied().collect();
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v1_from_values_100k(bencher: Bencher) {
        let vals = rand_vals(100_000);
        bencher.bench_local(|| {
            v1::Column::<u64>::from_values(vals.clone());
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn prefix_from_values_100k(bencher: Bencher) {
        let vals = rand_vals(100_000);
        bencher.bench_local(|| {
            PrefixColumn::<u64>::from_values(vals.clone());
        });
    }
}

#[divan::bench_group(name = "bulk_load_bool")]
mod bulk_load_bool {
    use super::*;

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v0_from_iter_100k(bencher: Bencher) {
        let vals = rand_bools(100_000);
        bencher.bench_local(|| {
            let _: ColumnData<BooleanCursor> = vals.iter().copied().collect();
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v1_from_values_100k(bencher: Bencher) {
        let vals = rand_bools(100_000);
        bencher.bench_local(|| {
            v1::Column::<bool>::from_values(vals.clone());
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn prefix_from_values_100k(bencher: Bencher) {
        let vals = rand_bools(100_000);
        bencher.bench_local(|| {
            PrefixColumn::<bool>::from_values(vals.clone());
        });
    }
}

// ── PrefixColumn queries: get vs get_prefix vs get_index_for_prefix ──────────

#[divan::bench_group(name = "prefix_queries_u64")]
mod prefix_queries_u64 {
    use super::*;

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn get_10k(bencher: Bencher) {
        let col = build_prefix(10_000);
        bencher.bench_local(|| {
            let pos = rand_usize() % col.len();
            col.get(pos)
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn get_prefix_10k(bencher: Bencher) {
        let col = build_prefix(10_000);
        bencher.bench_local(|| {
            let pos = rand_usize() % (col.len() + 1);
            col.get_prefix(pos)
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn get_index_for_prefix_10k(bencher: Bencher) {
        let col = build_prefix(10_000);
        let total = col.get_prefix(col.len());
        bencher.bench_local(|| {
            let target = if total > 0 {
                rng().next_u64() as u128 % total
            } else {
                0
            };
            col.get_index_for_prefix(target)
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn get_100k(bencher: Bencher) {
        let col = build_prefix(100_000);
        bencher.bench_local(|| {
            let pos = rand_usize() % col.len();
            col.get(pos)
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn get_prefix_100k(bencher: Bencher) {
        let col = build_prefix(100_000);
        bencher.bench_local(|| {
            let pos = rand_usize() % (col.len() + 1);
            col.get_prefix(pos)
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn get_index_for_prefix_100k(bencher: Bencher) {
        let col = build_prefix(100_000);
        let total = col.get_prefix(col.len());
        bencher.bench_local(|| {
            let target = if total > 0 {
                rng().next_u64() as u128 % total
            } else {
                0
            };
            col.get_index_for_prefix(target)
        });
    }
}

#[divan::bench_group(name = "prefix_queries_bool")]
mod prefix_queries_bool {
    use super::*;

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn get_10k(bencher: Bencher) {
        let col = build_prefix_bool(10_000);
        bencher.bench_local(|| {
            let pos = rand_usize() % col.len();
            col.get(pos)
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn get_prefix_10k(bencher: Bencher) {
        let col = build_prefix_bool(10_000);
        bencher.bench_local(|| {
            let pos = rand_usize() % (col.len() + 1);
            col.get_prefix(pos)
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn get_index_for_prefix_10k(bencher: Bencher) {
        let col = build_prefix_bool(10_000);
        let total = col.get_prefix(col.len());
        bencher.bench_local(|| {
            let target = if total > 0 {
                rng().random_range(0..total)
            } else {
                0
            };
            col.get_index_for_prefix(target)
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn get_100k(bencher: Bencher) {
        let col = build_prefix_bool(100_000);
        bencher.bench_local(|| {
            let pos = rand_usize() % col.len();
            col.get(pos)
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn get_prefix_100k(bencher: Bencher) {
        let col = build_prefix_bool(100_000);
        bencher.bench_local(|| {
            let pos = rand_usize() % (col.len() + 1);
            col.get_prefix(pos)
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn get_index_for_prefix_100k(bencher: Bencher) {
        let col = build_prefix_bool(100_000);
        let total = col.get_prefix(col.len());
        bencher.bench_local(|| {
            let target = if total > 0 {
                rng().random_range(0..total)
            } else {
                0
            };
            col.get_index_for_prefix(target)
        });
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Delta column benchmarks: v0 DeltaCursor vs v1 Column<i64> vs v1 DeltaColumn<i64>
// ═══════════════════════════════════════════════════════════════════════════

fn rand_i64_vals(n: usize) -> Vec<i64> {
    (0..n).map(|_| rng().random_range(0..1000i64)).collect()
}

fn rand_monotonic_i64(n: usize) -> Vec<i64> {
    let mut v = Vec::with_capacity(n);
    let mut acc: i64 = 0;
    for _ in 0..n {
        acc += rng().random_range(0..10i64);
        v.push(acc);
    }
    v
}

fn build_v0_delta(n: usize) -> ColumnData<DeltaCursor> {
    let vals = rand_i64_vals(n);
    let mut col: ColumnData<DeltaCursor> = ColumnData::new();
    col.splice(0, 0, vals);
    col
}

fn build_v1_i64(n: usize) -> v1::Column<i64> {
    v1::Column::from_values(rand_i64_vals(n).into_iter().collect())
}

fn build_v1_delta(n: usize) -> DeltaColumn<i64> {
    DeltaColumn::from_values(rand_i64_vals(n))
}

fn build_v0_delta_monotonic(n: usize) -> ColumnData<DeltaCursor> {
    let vals = rand_monotonic_i64(n);
    let mut col: ColumnData<DeltaCursor> = ColumnData::new();
    col.splice(0, 0, vals);
    col
}

fn build_v1_i64_monotonic(n: usize) -> v1::Column<i64> {
    v1::Column::from_values(rand_monotonic_i64(n).into_iter().collect())
}

fn build_v1_delta_monotonic(n: usize) -> DeltaColumn<i64> {
    DeltaColumn::from_values(rand_monotonic_i64(n))
}

// ── Delta get (random access) ────────────────────────────────────────────────

#[divan::bench_group(name = "delta_get")]
mod delta_get {
    use super::*;

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn v0_delta_get_10k(bencher: Bencher) {
        let col = build_v0_delta(10_000);
        bencher.bench_local(|| {
            let pos = rand_usize() % col.len();
            col.get(pos)
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn v1_plain_get_10k(bencher: Bencher) {
        let col = build_v1_i64(10_000);
        bencher.bench_local(|| {
            let pos = rand_usize() % col.len();
            col.get(pos)
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn v1_delta_get_10k(bencher: Bencher) {
        let col = build_v1_delta(10_000);
        bencher.bench_local(|| {
            let pos = rand_usize() % col.len();
            col.get(pos)
        });
    }
}

// ── Delta insert ─────────────────────────────────────────────────────────────

#[divan::bench_group(name = "delta_insert")]
mod delta_insert {
    use super::*;

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn v0_delta_insert_1(bencher: Bencher) {
        let mut col = build_v0_delta(10_000);
        bencher.bench_local(|| {
            let pos = rand_usize() % col.len();
            col.splice(pos, 0, [rng().random_range(0..1000i64)]);
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn v1_plain_insert_1(bencher: Bencher) {
        let mut col = build_v1_i64(10_000);
        bencher.bench_local(|| {
            let pos = rand_usize() % col.len();
            col.splice(pos, 0, [rng().random_range(0..1000i64)]);
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn v1_delta_insert_1(bencher: Bencher) {
        let mut col = build_v1_delta(10_000);
        bencher.bench_local(|| {
            let pos = rand_usize() % col.len();
            col.insert(pos, rng().random_range(0..1000i64));
        });
    }
}

// ── Sparse Option<i64> delta mutations at 100k (50% None vs 0% None) ────────
//
// Compares v0 `ColumnData<DeltaCursor>` against v1 `DeltaColumn<Option<i64>>`
// for single-op splice/insert/delete on columns of 100_000 entries.  Two
// fills: `sparse` = 50% `None`, `dense` = 0% `None`.

fn rand_opt_i64_vals(n: usize, none_frac: f64) -> Vec<Option<i64>> {
    let mut r = rng();
    (0..n)
        .map(|_| {
            if r.random::<f64>() < none_frac {
                None
            } else {
                Some(r.random_range(0..1000i64))
            }
        })
        .collect()
}

fn build_v0_opt_delta(n: usize, none_frac: f64) -> ColumnData<DeltaCursor> {
    let vals = rand_opt_i64_vals(n, none_frac);
    let mut col: ColumnData<DeltaCursor> = ColumnData::new();
    col.splice(0, 0, vals);
    col
}

fn build_v1_opt_delta(n: usize, none_frac: f64) -> DeltaColumn<Option<i64>> {
    DeltaColumn::<Option<i64>>::from_values(rand_opt_i64_vals(n, none_frac))
}

fn rand_opt_val(none_frac: f64) -> Option<i64> {
    let mut r = rng();
    if r.random::<f64>() < none_frac {
        None
    } else {
        Some(r.random_range(0..1000i64))
    }
}

#[divan::bench_group(name = "delta_opt_insert_1_100k")]
mod delta_opt_insert_1_100k {
    use super::*;

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn v0_sparse(bencher: Bencher) {
        let mut col = build_v0_opt_delta(100_000, 0.5);
        bencher.bench_local(|| {
            let pos = rand_usize() % col.len();
            col.splice(pos, 0, [rand_opt_val(0.5)]);
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn v1_sparse(bencher: Bencher) {
        let mut col = build_v1_opt_delta(100_000, 0.5);
        bencher.bench_local(|| {
            let pos = rand_usize() % col.len();
            col.insert(pos, rand_opt_val(0.5));
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn v0_dense(bencher: Bencher) {
        let mut col = build_v0_opt_delta(100_000, 0.0);
        bencher.bench_local(|| {
            let pos = rand_usize() % col.len();
            col.splice(pos, 0, [rand_opt_val(0.0)]);
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn v1_dense(bencher: Bencher) {
        let mut col = build_v1_opt_delta(100_000, 0.0);
        bencher.bench_local(|| {
            let pos = rand_usize() % col.len();
            col.insert(pos, rand_opt_val(0.0));
        });
    }
}

#[divan::bench_group(name = "delta_opt_replace_1_100k")]
mod delta_opt_replace_1_100k {
    use super::*;

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn v0_sparse(bencher: Bencher) {
        let mut col = build_v0_opt_delta(100_000, 0.5);
        bencher.bench_local(|| {
            let pos = rand_usize() % col.len();
            col.splice(pos, 1, [rand_opt_val(0.5)]);
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn v1_sparse(bencher: Bencher) {
        let mut col = build_v1_opt_delta(100_000, 0.5);
        bencher.bench_local(|| {
            let pos = rand_usize() % col.len();
            col.splice(pos, 1, [rand_opt_val(0.5)]);
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn v0_dense(bencher: Bencher) {
        let mut col = build_v0_opt_delta(100_000, 0.0);
        bencher.bench_local(|| {
            let pos = rand_usize() % col.len();
            col.splice(pos, 1, [rand_opt_val(0.0)]);
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn v1_dense(bencher: Bencher) {
        let mut col = build_v1_opt_delta(100_000, 0.0);
        bencher.bench_local(|| {
            let pos = rand_usize() % col.len();
            col.splice(pos, 1, [rand_opt_val(0.0)]);
        });
    }
}

#[divan::bench_group(name = "delta_opt_delete_1_100k")]
mod delta_opt_delete_1_100k {
    use super::*;

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn v0_sparse(bencher: Bencher) {
        let mut col = build_v0_opt_delta(100_000, 0.5);
        bencher.bench_local(|| {
            if col.len() == 0 {
                return;
            }
            let pos = rand_usize() % col.len();
            col.splice::<Option<i64>, _>(pos, 1, std::iter::empty::<Option<i64>>());
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn v1_sparse(bencher: Bencher) {
        let mut col = build_v1_opt_delta(100_000, 0.5);
        bencher.bench_local(|| {
            if col.len() == 0 {
                return;
            }
            let pos = rand_usize() % col.len();
            col.remove(pos);
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn v0_dense(bencher: Bencher) {
        let mut col = build_v0_opt_delta(100_000, 0.0);
        bencher.bench_local(|| {
            if col.len() == 0 {
                return;
            }
            let pos = rand_usize() % col.len();
            col.splice::<Option<i64>, _>(pos, 1, std::iter::empty::<Option<i64>>());
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn v1_dense(bencher: Bencher) {
        let mut col = build_v1_opt_delta(100_000, 0.0);
        bencher.bench_local(|| {
            if col.len() == 0 {
                return;
            }
            let pos = rand_usize() % col.len();
            col.remove(pos);
        });
    }
}

// ── Delta save ───────────────────────────────────────────────────────────────

#[divan::bench_group(name = "delta_save")]
mod delta_save {
    use super::*;

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v0_delta_save_10k(bencher: Bencher) {
        let col = build_v0_delta(10_000);
        bencher.bench_local(|| col.save());
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v1_plain_save_10k(bencher: Bencher) {
        let col = build_v1_i64(10_000);
        bencher.bench_local(|| col.save());
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v1_delta_save_10k(bencher: Bencher) {
        let col = build_v1_delta(10_000);
        bencher.bench_local(|| col.save());
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v0_delta_save_100k(bencher: Bencher) {
        let col = build_v0_delta(100_000);
        bencher.bench_local(|| col.save());
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v1_plain_save_100k(bencher: Bencher) {
        let col = build_v1_i64(100_000);
        bencher.bench_local(|| col.save());
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v1_delta_save_100k(bencher: Bencher) {
        let col = build_v1_delta(100_000);
        bencher.bench_local(|| col.save());
    }
}

// ── Delta save (monotonic data — shows compression advantage) ────────────────

#[divan::bench_group(name = "delta_save_monotonic")]
mod delta_save_monotonic {
    use super::*;

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v0_delta_save_10k(bencher: Bencher) {
        let col = build_v0_delta_monotonic(10_000);
        bencher.bench_local(|| col.save());
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v1_plain_save_10k(bencher: Bencher) {
        let col = build_v1_i64_monotonic(10_000);
        bencher.bench_local(|| col.save());
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v1_delta_save_10k(bencher: Bencher) {
        let col = build_v1_delta_monotonic(10_000);
        bencher.bench_local(|| col.save());
    }
}

// ── Delta load ───────────────────────────────────────────────────────────────

#[divan::bench_group(name = "delta_load")]
mod delta_load {
    use super::*;

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v0_delta_load_100k(bencher: Bencher) {
        let col = build_v0_delta(100_000);
        let bytes = col.save();
        bencher.bench_local(|| ColumnData::<DeltaCursor>::load(&bytes).unwrap());
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v1_plain_load_100k(bencher: Bencher) {
        let col = build_v1_i64(100_000);
        let bytes = col.save();
        bencher.bench_local(|| v1::Column::<i64>::load(&bytes).unwrap());
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v1_delta_load_100k(bencher: Bencher) {
        let col = build_v1_delta(100_000);
        let bytes = col.save();
        bencher.bench_local(|| DeltaColumn::<i64>::load(&bytes).unwrap());
    }
}

// ── Delta bulk construction ──────────────────────────────────────────────────

#[divan::bench_group(name = "delta_bulk_load")]
mod delta_bulk_load {
    use super::*;

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v0_delta_build_10k(bencher: Bencher) {
        let vals = rand_i64_vals(10_000);
        bencher.bench_local(|| {
            let mut col: ColumnData<DeltaCursor> = ColumnData::new();
            col.splice(0, 0, vals.clone());
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v1_plain_build_10k(bencher: Bencher) {
        let vals = rand_i64_vals(10_000);
        bencher.bench_local(|| {
            v1::Column::<i64>::from_values(vals.clone());
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v1_delta_build_10k(bencher: Bencher) {
        let vals = rand_i64_vals(10_000);
        bencher.bench_local(|| {
            DeltaColumn::<i64>::from_values(vals.clone());
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v0_delta_build_100k(bencher: Bencher) {
        let vals = rand_i64_vals(100_000);
        bencher.bench_local(|| {
            let mut col: ColumnData<DeltaCursor> = ColumnData::new();
            col.splice(0, 0, vals.clone());
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v1_plain_build_100k(bencher: Bencher) {
        let vals = rand_i64_vals(100_000);
        bencher.bench_local(|| {
            v1::Column::<i64>::from_values(vals.clone());
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v1_delta_build_100k(bencher: Bencher) {
        let vals = rand_i64_vals(100_000);
        bencher.bench_local(|| {
            DeltaColumn::<i64>::from_values(vals.clone());
        });
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Iterator benchmarks
// ═══════════════════════════════════════════════════════════════════════════

#[divan::bench_group(name = "iter_u64")]
mod iter_u64 {
    use super::*;

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v0_iter_10k(bencher: Bencher) {
        let col = build_v0(10_000);
        bencher.bench_local(|| {
            let sum: u64 = col.iter().map(|v| v.unwrap_or_default().into_owned()).sum();
            std::hint::black_box(sum)
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v1_iter_10k(bencher: Bencher) {
        let col = build_v1(10_000);
        bencher.bench_local(|| {
            let sum: u64 = col.iter().sum();
            std::hint::black_box(sum)
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v0_iter_100k(bencher: Bencher) {
        let col = build_v0(100_000);
        bencher.bench_local(|| {
            let sum: u64 = col.iter().map(|v| v.unwrap_or_default().into_owned()).sum();
            std::hint::black_box(sum)
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v1_iter_100k(bencher: Bencher) {
        let col = build_v1(100_000);
        bencher.bench_local(|| {
            let sum: u64 = col.iter().sum();
            std::hint::black_box(sum)
        });
    }
}

#[divan::bench_group(name = "iter_range_u64")]
mod iter_range_u64 {
    use super::*;

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v0_iter_range_10k(bencher: Bencher) {
        let col = build_v0(10_000);
        bencher.bench_local(|| {
            let sum: u64 = col
                .iter_range(2500..7500)
                .map(|v| v.unwrap_or_default().into_owned())
                .sum();
            std::hint::black_box(sum)
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v1_iter_range_10k(bencher: Bencher) {
        let col = build_v1(10_000);
        bencher.bench_local(|| {
            let sum: u64 = col.iter_range(2500..7500).sum();
            std::hint::black_box(sum)
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v0_iter_range_100k(bencher: Bencher) {
        let col = build_v0(100_000);
        bencher.bench_local(|| {
            let sum: u64 = col
                .iter_range(25000..75000)
                .map(|v| v.unwrap_or_default().into_owned())
                .sum();
            std::hint::black_box(sum)
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v1_iter_range_100k(bencher: Bencher) {
        let col = build_v1(100_000);
        bencher.bench_local(|| {
            let sum: u64 = col.iter_range(25000..75000).sum();
            std::hint::black_box(sum)
        });
    }
}

// ── nth() on alternating 10k-element runs (1M total) ────────────────────────

fn build_v0_alternating(run_len: usize, num_runs: usize) -> ColumnData<UIntCursor> {
    let vals: Vec<u64> = (0..num_runs)
        .flat_map(|r| vec![r as u64; run_len])
        .collect();
    vals.into_iter().collect()
}

fn build_v1_alternating(run_len: usize, num_runs: usize) -> v1::Column<u64> {
    let vals: Vec<u64> = (0..num_runs)
        .flat_map(|r| vec![r as u64; run_len])
        .collect();
    v1::Column::from_values(vals)
}

#[divan::bench_group(name = "nth_10k_runs")]
mod nth_10k_runs {
    use super::*;

    const RUN_LEN: usize = 10_000;
    const NUM_RUNS: usize = 100; // 1M total

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn v0_nth_10k(bencher: Bencher) {
        let col = build_v0_alternating(RUN_LEN, NUM_RUNS);
        bencher.bench_local(|| {
            let mut iter = col.iter_range(20_000..col.len());
            let _ = iter.next();
            iter.nth(10_000)
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn v1_nth_10k(bencher: Bencher) {
        let col = build_v1_alternating(RUN_LEN, NUM_RUNS);
        bencher.bench_local(|| {
            let mut iter = col.iter_range(20_000..col.len());
            let _ = iter.next();
            iter.nth(10_000)
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn v0_nth_100k(bencher: Bencher) {
        let col = build_v0_alternating(RUN_LEN, NUM_RUNS);
        bencher.bench_local(|| {
            let mut iter = col.iter_range(20_000..col.len());
            let _ = iter.next();
            iter.nth(100_000)
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn v1_nth_100k(bencher: Bencher) {
        let col = build_v1_alternating(RUN_LEN, NUM_RUNS);
        bencher.bench_local(|| {
            let mut iter = col.iter_range(20_000..col.len());
            let _ = iter.next();
            iter.nth(100_000)
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn v0_nth_500k(bencher: Bencher) {
        let col = build_v0_alternating(RUN_LEN, NUM_RUNS);
        bencher.bench_local(|| {
            let mut iter = col.iter_range(20_000..col.len());
            let _ = iter.next();
            iter.nth(500_000)
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn v1_nth_500k(bencher: Bencher) {
        let col = build_v1_alternating(RUN_LEN, NUM_RUNS);
        bencher.bench_local(|| {
            let mut iter = col.iter_range(20_000..col.len());
            let _ = iter.next();
            iter.nth(500_000)
        });
    }
}

#[divan::bench_group(name = "iter_bool")]
mod iter_bool {
    use super::*;

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v0_iter_100k(bencher: Bencher) {
        let col = build_v0_bool(100_000);
        bencher.bench_local(|| {
            let count = col.iter().filter(|v| v.as_deref() == Some(&true)).count();
            std::hint::black_box(count)
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v1_iter_100k(bencher: Bencher) {
        let col = build_v1_bool(100_000);
        bencher.bench_local(|| {
            let count = col.iter().filter(|v| *v).count();
            std::hint::black_box(count)
        });
    }
}

// ── Prefix iter: random values ──────────────────────────────────────────────

fn build_v0_prefix(n: usize) -> ColumnData<UIntCursor> {
    (0..n).map(|_| rand_u64()).collect()
}

fn build_v1_prefix(n: usize) -> v1::PrefixColumn<u64> {
    v1::PrefixColumn::from_values((0..n).map(|_| rand_u64()).collect())
}

fn build_v0_prefix_alternating(run_len: usize, num_runs: usize) -> ColumnData<UIntCursor> {
    let vals: Vec<u64> = (0..num_runs)
        .flat_map(|r| vec![(r as u64 + 1) * 10; run_len])
        .collect();
    vals.into_iter().collect()
}

fn build_v1_prefix_alternating(run_len: usize, num_runs: usize) -> v1::PrefixColumn<u64> {
    let vals: Vec<u64> = (0..num_runs)
        .flat_map(|r| vec![(r as u64 + 1) * 10; run_len])
        .collect();
    v1::PrefixColumn::from_values(vals)
}

#[divan::bench_group(name = "prefix_iter_random")]
mod prefix_iter_random {
    use super::*;

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v0_with_acc_10k(bencher: Bencher) {
        let col = build_v0_prefix(10_000);
        bencher.bench_local(|| {
            let sum: u64 = col
                .iter()
                .with_acc()
                .map(|g| g.acc.as_u64())
                .last()
                .unwrap_or(0);
            std::hint::black_box(sum)
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v1_prefix_iter_10k(bencher: Bencher) {
        let col = build_v1_prefix(10_000);
        bencher.bench_local(|| {
            let sum: u128 = col.iter().map(|(p, _)| p).last().unwrap_or(0);
            std::hint::black_box(sum)
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v0_with_acc_100k(bencher: Bencher) {
        let col = build_v0_prefix(100_000);
        bencher.bench_local(|| {
            let sum: u64 = col
                .iter()
                .with_acc()
                .map(|g| g.acc.as_u64())
                .last()
                .unwrap_or(0);
            std::hint::black_box(sum)
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v1_prefix_iter_100k(bencher: Bencher) {
        let col = build_v1_prefix(100_000);
        bencher.bench_local(|| {
            let sum: u128 = col.iter().map(|(p, _)| p).last().unwrap_or(0);
            std::hint::black_box(sum)
        });
    }
}

// ── Prefix iter: alternating runs ───────────────────────────────────────────

#[divan::bench_group(name = "prefix_iter_runs")]
mod prefix_iter_runs {
    use super::*;

    const RUN_LEN: usize = 10_000;
    const NUM_RUNS: usize = 100; // 1M total

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v0_with_acc_1m(bencher: Bencher) {
        let col = build_v0_prefix_alternating(RUN_LEN, NUM_RUNS);
        bencher.bench_local(|| {
            let sum: u64 = col
                .iter()
                .with_acc()
                .map(|g| g.acc.as_u64())
                .last()
                .unwrap_or(0);
            std::hint::black_box(sum)
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v1_prefix_iter_1m(bencher: Bencher) {
        let col = build_v1_prefix_alternating(RUN_LEN, NUM_RUNS);
        bencher.bench_local(|| {
            let sum: u128 = col.iter().map(|(p, _)| p).last().unwrap_or(0);
            std::hint::black_box(sum)
        });
    }
}

// ── Prefix nth: alternating runs ────────────────────────────────────────────

#[divan::bench_group(name = "prefix_nth_runs")]
mod prefix_nth_runs {
    use super::*;

    const RUN_LEN: usize = 10_000;
    const NUM_RUNS: usize = 100; // 1M total

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn v0_nth_10k(bencher: Bencher) {
        let col = build_v0_prefix_alternating(RUN_LEN, NUM_RUNS);
        bencher.bench_local(|| {
            let mut iter = col.iter_range(20_000..col.len()).with_acc();
            let _ = iter.next();
            iter.nth(10_000)
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn v1_nth_10k(bencher: Bencher) {
        let col = build_v1_prefix_alternating(RUN_LEN, NUM_RUNS);
        bencher.bench_local(|| {
            let mut iter = col.iter_range(20_000..col.len());
            let _ = iter.next();
            iter.nth(10_000)
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn v0_nth_100k(bencher: Bencher) {
        let col = build_v0_prefix_alternating(RUN_LEN, NUM_RUNS);
        bencher.bench_local(|| {
            let mut iter = col.iter_range(20_000..col.len()).with_acc();
            let _ = iter.next();
            iter.nth(100_000)
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn v1_nth_100k(bencher: Bencher) {
        let col = build_v1_prefix_alternating(RUN_LEN, NUM_RUNS);
        bencher.bench_local(|| {
            let mut iter = col.iter_range(20_000..col.len());
            let _ = iter.next();
            iter.nth(100_000)
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn v0_nth_500k(bencher: Bencher) {
        let col = build_v0_prefix_alternating(RUN_LEN, NUM_RUNS);
        bencher.bench_local(|| {
            let mut iter = col.iter_range(20_000..col.len()).with_acc();
            let _ = iter.next();
            iter.nth(500_000)
        });
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn v1_nth_500k(bencher: Bencher) {
        let col = build_v1_prefix_alternating(RUN_LEN, NUM_RUNS);
        bencher.bench_local(|| {
            let mut iter = col.iter_range(20_000..col.len());
            let _ = iter.next();
            iter.nth(500_000)
        });
    }
}

// ── Prefix nth: random values ───────────────────────────────────────────────

#[divan::bench_group(name = "prefix_nth_random")]
mod prefix_nth_random {
    use super::*;

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn v0_nth_10k(bencher: Bencher) {
        let col = build_v0_prefix(100_000);
        bencher.bench_local(|| col.iter_range(20_000..col.len()).with_acc().nth(10_000));
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn v1_nth_10k(bencher: Bencher) {
        let col = build_v1_prefix(100_000);
        bencher.bench_local(|| col.iter_range(20_000..col.len()).nth(10_000));
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn v0_nth_50k(bencher: Bencher) {
        let col = build_v0_prefix(100_000);
        bencher.bench_local(|| col.iter_range(20_000..col.len()).with_acc().nth(50_000));
    }

    #[inline(never)]
    #[divan::bench(max_time = Duration::from_secs(3))]
    fn v1_nth_50k(bencher: Bencher) {
        let col = build_v1_prefix(100_000);
        bencher.bench_local(|| col.iter_range(20_000..col.len()).nth(50_000));
    }
}

// ── Save benchmarks ─────────────────────────────────────────────────────────

#[divan::bench_group(name = "save_u64")]
mod save_u64 {
    use super::*;

    #[divan::bench(args = [1_000, 10_000, 100_000], max_time = Duration::from_secs(3))]
    fn v0(bencher: Bencher, n: usize) {
        let col = build_v0(n);
        bencher.bench_local(|| col.save());
    }

    #[divan::bench(args = [1_000, 10_000, 100_000], max_time = Duration::from_secs(3))]
    fn v1(bencher: Bencher, n: usize) {
        let col = build_v1(n);
        bencher.bench_local(|| col.save());
    }
}

#[divan::bench_group(name = "save_bool")]
mod save_bool {
    use super::*;

    #[divan::bench(args = [1_000, 10_000, 100_000], max_time = Duration::from_secs(3))]
    fn v0(bencher: Bencher, n: usize) {
        let col = build_v0_bool(n);
        bencher.bench_local(|| col.save());
    }

    #[divan::bench(args = [1_000, 10_000, 100_000], max_time = Duration::from_secs(3))]
    fn v1(bencher: Bencher, n: usize) {
        let col = build_v1_bool(n);
        bencher.bench_local(|| col.save());
    }
}

#[divan::bench_group(name = "save_string_8b")]
mod save_string_8b {
    use super::*;

    #[divan::bench(args = [1_000, 10_000, 100_000], max_time = Duration::from_secs(3))]
    fn v0(bencher: Bencher, n: usize) {
        let col = build_v0_str(n, 8);
        bencher.bench_local(|| col.save());
    }

    #[divan::bench(args = [1_000, 10_000, 100_000], max_time = Duration::from_secs(3))]
    fn v1(bencher: Bencher, n: usize) {
        let col = build_v1_str(n, 8);
        bencher.bench_local(|| col.save());
    }
}

#[divan::bench_group(name = "save_opt_u64")]
mod save_opt_u64 {
    use super::*;

    fn build_v1_opt(n: usize) -> v1::Column<Option<u64>> {
        let choices: [Option<u64>; 5] = [None, Some(1), Some(2), Some(3), Some(4)];
        v1::Column::from_values(
            (0..n)
                .map(|_| choices[rng().next_u64() as usize % 5])
                .collect(),
        )
    }

    #[divan::bench(args = [1_000, 10_000, 100_000], max_time = Duration::from_secs(3))]
    fn v1(bencher: Bencher, n: usize) {
        let col = build_v1_opt(n);
        bencher.bench_local(|| col.save());
    }
}

#[divan::bench_group(name = "iter_range_next")]
mod iter_range_next {
    use super::*;

    // 100k element column, walked in 1000 contiguous 100-element ranges.
    // Each iteration of the bench does 1000 `iter_range(r).next()` calls —
    // measuring the cost of "set up an iter at an arbitrary position and
    // pull a single item."  Same underlying data across all 5 column types
    // (monotonic i64 counters).

    const N: usize = 100_000;
    const STEP: usize = 100;

    fn seeded_monotonic_i64s(n: usize) -> Vec<i64> {
        let mut state = 0xCAFEBABE_u64;
        let mut acc = 0i64;
        (0..n)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                acc += (state % 10) as i64;
                acc
            })
            .collect()
    }

    fn ranges() -> impl Iterator<Item = std::ops::Range<usize>> + Clone {
        (0..N).step_by(STEP).map(|s| s..(s + STEP).min(N))
    }

    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v0_int(bencher: Bencher) {
        let mut c = ColumnData::<IntCursor>::new();
        c.splice(0, 0, seeded_monotonic_i64s(N));
        bencher.bench_local(|| {
            let mut acc = 0i64;
            for r in ranges() {
                if let Some(Some(v)) = c.iter_range(r).next() {
                    acc = acc.wrapping_add(v.into_owned());
                }
            }
            std::hint::black_box(acc)
        });
    }

    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v0_delta(bencher: Bencher) {
        let mut c = ColumnData::<DeltaCursor>::new();
        c.splice(0, 0, seeded_monotonic_i64s(N));
        bencher.bench_local(|| {
            let mut acc = 0i64;
            for r in ranges() {
                if let Some(Some(v)) = c.iter_range(r).next() {
                    acc = acc.wrapping_add(v.into_owned());
                }
            }
            std::hint::black_box(acc)
        });
    }

    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v1_column(bencher: Bencher) {
        let c = v1::Column::<i64>::from_values(seeded_monotonic_i64s(N));
        bencher.bench_local(|| {
            let mut acc = 0i64;
            for r in ranges() {
                if let Some(v) = c.iter_range(r).next() {
                    acc = acc.wrapping_add(v);
                }
            }
            std::hint::black_box(acc)
        });
    }

    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v1_prefix(bencher: Bencher) {
        let c = PrefixColumn::<i64>::from_values(seeded_monotonic_i64s(N));
        bencher.bench_local(|| {
            let mut acc = 0i64;
            for r in ranges() {
                if let Some((_prefix, v)) = c.iter_range(r).next() {
                    acc = acc.wrapping_add(v);
                }
            }
            std::hint::black_box(acc)
        });
    }

    #[divan::bench(max_time = Duration::from_secs(5), sample_count = 20)]
    fn v1_delta(bencher: Bencher) {
        let c = DeltaColumn::<i64>::from_values(seeded_monotonic_i64s(N));
        bencher.bench_local(|| {
            let mut acc = 0i64;
            for r in ranges() {
                if let Some(v) = c.iter_range(r).next() {
                    acc = acc.wrapping_add(v);
                }
            }
            std::hint::black_box(acc)
        });
    }
}
