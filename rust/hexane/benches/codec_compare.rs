//! Leb128 vs Bijou64 codec comparison.
//!
//! Runs the same workloads generically over both codecs (divan `types`
//! parameter) across value-magnitude distributions chosen to hit the
//! interesting wire-format bands:
//!
//!   * `small`   0..100        — 1 byte in both codecs
//!   * `band248` 128..248      — 1 byte bijou64, 2 bytes LEB128
//!   * `band16k` 504..16384    — 3 bytes bijou64, 2 bytes LEB128
//!   * `hash64`  full-width    — ≤9 bytes bijou64, ≤10 bytes LEB128
//!   * `mixed`   skewed-small  — realistic id/count-ish distribution
//!
//! Values are distinct per index (literal-run heavy), so value coding —
//! not run-length collapsing — dominates.  A size table prints before
//! the timings.
//!
//! Run: `cargo bench -p hexane --features bijou64 --bench codec_compare`

use divan::counter::ItemsCount;
use divan::{black_box, Bencher};
use hexane::{Bijou64, Codec, Column, DeltaColumn, Leb128};
use rand::rngs::SmallRng;
use rand::{RngExt, SeedableRng};

const N: usize = 100_000;

const DISTS: [&str; 5] = ["small", "band248", "band16k", "hash64", "mixed"];

fn values(dist: &str) -> Vec<u64> {
    let mut rng = SmallRng::seed_from_u64(0xC0FFEE);
    (0..N)
        .map(|_| match dist {
            "small" => rng.random_range(0u64..100),
            "band248" => rng.random_range(128u64..248),
            "band16k" => rng.random_range(504u64..16_384),
            "hash64" => rng.random_range(0u64..u64::MAX),
            "mixed" => {
                // 70% tiny, 20% mid, 10% wide — id/count-shaped
                match rng.random_range(0u32..10) {
                    0..=6 => rng.random_range(0u64..128),
                    7..=8 => rng.random_range(128u64..66_000),
                    _ => rng.random_range(0u64..u64::MAX),
                }
            }
            _ => unreachable!(),
        })
        .collect()
}

/// Deltas whose successive differences land in each band (for DeltaColumn,
/// which codes the signed diffs, not the values).
fn delta_values(dist: &str) -> Vec<u64> {
    let mut acc = 0u64;
    values(dist)
        .into_iter()
        .map(|v| {
            acc = acc.wrapping_add(v % 1_000_000);
            acc
        })
        .collect()
}

fn main() {
    // ── Size report ─────────────────────────────────────────────────────
    eprintln!("encoded bytes for {N} distinct u64 values (RLE literal runs):");
    eprintln!(
        "{:>10} {:>12} {:>12} {:>8}",
        "dist", "leb128", "bijou64", "ratio"
    );
    for dist in DISTS {
        let vals = values(dist);
        let leb = Column::<u64, Leb128>::from_values(vals.clone()).save();
        let bij = Column::<u64, Bijou64>::from_values(vals).save();
        eprintln!(
            "{:>10} {:>12} {:>12} {:>7.3}",
            dist,
            leb.len(),
            bij.len(),
            bij.len() as f64 / leb.len() as f64,
        );
    }
    eprintln!();
    eprintln!("delta column (signed diffs) for {N} values:");
    eprintln!(
        "{:>10} {:>12} {:>12} {:>8}",
        "dist", "leb128", "bijou64", "ratio"
    );
    for dist in DISTS {
        let vals = delta_values(dist);
        let leb: DeltaColumn<u64, Leb128> = vals.iter().copied().collect();
        let bij: DeltaColumn<u64, Bijou64> = vals.iter().copied().collect();
        let (l, b) = (leb.save().len(), bij.save().len());
        eprintln!(
            "{:>10} {:>12} {:>12} {:>7.3}",
            dist,
            l,
            b,
            b as f64 / l as f64
        );
    }
    eprintln!();

    divan::main();
}

// ── Raw codec primitives ────────────────────────────────────────────────────

#[divan::bench(types = [Leb128, Bijou64], args = DISTS)]
fn raw_encode<C: Codec>(bencher: Bencher, dist: &str) {
    let vals = values(dist);
    bencher.counter(ItemsCount::new(N)).bench(|| {
        let mut out = Vec::with_capacity(N * 10);
        for &v in &vals {
            out.extend(C::encode_unsigned(black_box(v)));
        }
        out
    });
}

#[divan::bench(types = [Leb128, Bijou64], args = DISTS)]
fn raw_decode<C: Codec>(bencher: Bencher, dist: &str) {
    let vals = values(dist);
    let mut bytes = Vec::new();
    for &v in &vals {
        bytes.extend(C::encode_unsigned(v));
    }
    bencher.counter(ItemsCount::new(N)).bench(|| {
        let mut pos = 0;
        let mut sum = 0u64;
        while pos < bytes.len() {
            let (n, v) = C::read_unsigned(black_box(&bytes[pos..])).unwrap();
            pos += n;
            sum = sum.wrapping_add(v);
        }
        sum
    });
}

/// Skip-over without materialising values — the `value_len` / `nth` path.
#[divan::bench(types = [Leb128, Bijou64], args = DISTS)]
fn raw_skip<C: Codec>(bencher: Bencher, dist: &str) {
    let vals = values(dist);
    let mut bytes = Vec::new();
    for &v in &vals {
        bytes.extend(C::encode_unsigned(v));
    }
    bencher.counter(ItemsCount::new(N)).bench(|| {
        let mut pos = 0;
        let mut count = 0usize;
        while pos < bytes.len() {
            pos += C::unsigned_len(black_box(&bytes[pos..])).unwrap();
            count += 1;
        }
        count
    });
}

// ── Column paths ────────────────────────────────────────────────────────────

#[divan::bench(types = [Leb128, Bijou64], args = DISTS)]
fn column_build<C: Codec>(bencher: Bencher, dist: &str) {
    let vals = values(dist);
    bencher
        .counter(ItemsCount::new(N))
        .bench(|| Column::<u64, C>::from_values(black_box(vals.clone())));
}

/// Untrusted bytes → Column: full decode + canonical-form validation.
#[divan::bench(types = [Leb128, Bijou64], args = DISTS)]
fn column_load<C: Codec>(bencher: Bencher, dist: &str) {
    let bytes = Column::<u64, C>::from_values(values(dist)).save();
    bencher
        .counter(ItemsCount::new(N))
        .bench(|| Column::<u64, C>::load(black_box(&bytes)).unwrap());
}

#[divan::bench(types = [Leb128, Bijou64], args = DISTS)]
fn column_iter_sum<C: Codec>(bencher: Bencher, dist: &str) {
    let col = Column::<u64, C>::from_values(values(dist));
    bencher
        .counter(ItemsCount::new(N))
        .bench(|| black_box(&col).iter().fold(0u64, u64::wrapping_add));
}

/// Random access — B-tree to the slab, then value_len skips within it.
#[divan::bench(types = [Leb128, Bijou64], args = DISTS)]
fn column_get_random<C: Codec>(bencher: Bencher, dist: &str) {
    const GETS: usize = 10_000;
    let col = Column::<u64, C>::from_values(values(dist));
    let mut rng = SmallRng::seed_from_u64(0xBEEF);
    let idxs: Vec<usize> = (0..GETS).map(|_| rng.random_range(0..N)).collect();
    bencher.counter(ItemsCount::new(GETS)).bench(|| {
        let mut sum = 0u64;
        for &i in &idxs {
            sum = sum.wrapping_add(black_box(&col).get(i).unwrap());
        }
        sum
    });
}

// ── Delta (signed) paths ────────────────────────────────────────────────────

#[divan::bench(types = [Leb128, Bijou64], args = DISTS)]
fn delta_build<C: Codec>(bencher: Bencher, dist: &str) {
    let vals = delta_values(dist);
    bencher.counter(ItemsCount::new(N)).bench(|| {
        black_box(&vals)
            .iter()
            .copied()
            .collect::<DeltaColumn<u64, C>>()
    });
}

#[divan::bench(types = [Leb128, Bijou64], args = DISTS)]
fn delta_iter_sum<C: Codec>(bencher: Bencher, dist: &str) {
    let col: DeltaColumn<u64, C> = delta_values(dist).into_iter().collect();
    bencher
        .counter(ItemsCount::new(N))
        .bench(|| black_box(&col).iter().fold(0u64, u64::wrapping_add));
}

/// Control experiment: the bijou skip loop with all abstraction removed —
/// raw pointer walk, no trait, no Option, no bounds checks.  If this
/// plateaus at the same rate as `raw_skip`, the cost is the data-dependent
/// load chain (pos depends on the byte at pos), not codegen.
#[divan::bench(args = DISTS)]
fn raw_skip_manual_bijou(bencher: Bencher, dist: &str) {
    let vals = values(dist);
    let mut bytes = Vec::new();
    for &v in &vals {
        bytes.extend(Bijou64::encode_unsigned(v));
    }
    bencher.counter(ItemsCount::new(N)).bench(|| {
        let data = black_box(&bytes[..]);
        let mut pos = 0usize;
        let mut count = 0usize;
        while pos < data.len() {
            let b = unsafe { *data.get_unchecked(pos) };
            pos += 1 + b.saturating_sub(0xF7) as usize;
            count += 1;
        }
        count
    });
}
