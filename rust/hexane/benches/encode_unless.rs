// v0 `encode_unless_empty` vs v1 `encode_to_unless` — head-to-head.
//
// This bench targets the hot path in `op_set2/change.rs`:
//   for change in changes {
//       Cursor::encode_unless_empty(out, change.field_iter)   // v0
//       Encoder::encode_to_unless(out, change.field_iter, S)  // v1
//   }
// The `load_typing` workload in automerge-battery hits ~14 of these calls per
// change, with iterators of length 1 (single-op changes). v1 is ~2× slower
// per call, suspected to be a per-encoder allocation + final byte copy.
//
// Each bench reports time per *outer iteration* — i.e. cost of one
// `encode_unless` call, amortized.
//
// Workloads:
//   per_call/N=1   — many small encodes, mimics typing-style histories
//   per_call/N=10  — small but multi-op changes
//   per_call/N=100 — medium-batch changes
//   one_shot/N=10k — one big encode (per-call overhead amortized away)
//
// Variants:
//   *_sentinel  — every value matches the elision sentinel (fast path)
//   *_value     — every value differs from the sentinel (must encode)

use divan::{black_box, Bencher};
use std::time::Duration;

use hexane::v1::{DeltaColumn, EncoderApi, PrefixColumn};
use hexane::*;

fn main() {
    divan::main();
}

const MAX: Duration = Duration::from_secs(2);

// Outer-call counts. Picked so per-call benches touch enough work to be
// stable but not so many we exceed MAX.
const OUTER_TINY: usize = 100_000; // per_call N=1
const OUTER_SMALL: usize = 20_000; // per_call N=10

// ─── u64 (UIntCursor / RleEncoder<u64>) ─────────────────────────────────────

#[divan::bench_group(name = "u64_per_call_N1_value", max_time = MAX)]
mod u64_per_call_n1_value {
    use super::*;
    // Every call: 1-element iter with a non-zero value (must encode).
    #[divan::bench]
    fn v0(bencher: Bencher) {
        let vals: Vec<u64> = (1..=OUTER_TINY as u64).collect();
        let mut out = Vec::with_capacity(OUTER_TINY * 4);
        bencher.bench_local(|| {
            out.clear();
            for &v in &vals {
                let _ = UIntCursor::encode_unless_empty(&mut out, [v]);
            }
            black_box(&out);
        });
    }
    #[divan::bench]
    fn v1(bencher: Bencher) {
        let vals: Vec<u64> = (1..=OUTER_TINY as u64).collect();
        let mut out = Vec::with_capacity(OUTER_TINY * 4);
        bencher.bench_local(|| {
            out.clear();
            for &v in &vals {
                let _ = v1::Encoder::<u64>::encode_to_unless(&mut out, [v], 0);
            }
            black_box(&out);
        });
    }
}

#[divan::bench_group(name = "u64_per_call_N10_value", max_time = MAX)]
mod u64_per_call_n10_value {
    use super::*;
    #[divan::bench]
    fn v0(bencher: Bencher) {
        let chunks: Vec<[u64; 10]> = (0..OUTER_SMALL)
            .map(|i| std::array::from_fn(|j| (i * 10 + j) as u64 + 1))
            .collect();
        let mut out = Vec::with_capacity(OUTER_SMALL * 16);
        bencher.bench_local(|| {
            out.clear();
            for c in &chunks {
                let _ = UIntCursor::encode_unless_empty(&mut out, c.iter().copied());
            }
            black_box(&out);
        });
    }
    #[divan::bench]
    fn v1(bencher: Bencher) {
        let chunks: Vec<[u64; 10]> = (0..OUTER_SMALL)
            .map(|i| std::array::from_fn(|j| (i * 10 + j) as u64 + 1))
            .collect();
        let mut out = Vec::with_capacity(OUTER_SMALL * 16);
        bencher.bench_local(|| {
            out.clear();
            for c in &chunks {
                let _ = v1::Encoder::<u64>::encode_to_unless(&mut out, c.iter().copied(), 0);
            }
            black_box(&out);
        });
    }
}

// ─── Option<u64> (ActorCursor-shaped — RleCursor over Option<P>) ────────────

#[divan::bench_group(name = "opt_u64_per_call_N1_sentinel", max_time = MAX)]
mod opt_u64_per_call_n1_sentinel {
    use super::*;
    // Every call: 1-element iter == None (sentinel match → fast path).
    #[divan::bench]
    fn v0(bencher: Bencher) {
        let mut out = Vec::with_capacity(OUTER_TINY * 4);
        bencher.bench_local(|| {
            out.clear();
            for _ in 0..OUTER_TINY {
                let v: Option<u64> = None;
                let _ = UIntCursor::encode_unless_empty(&mut out, [v]);
            }
            black_box(&out);
        });
    }
    #[divan::bench]
    fn v1(bencher: Bencher) {
        let mut out = Vec::with_capacity(OUTER_TINY * 4);
        bencher.bench_local(|| {
            out.clear();
            for _ in 0..OUTER_TINY {
                let v: Option<u64> = None;
                let _ = v1::Encoder::<Option<u64>>::encode_to_unless(&mut out, [v], None);
            }
            black_box(&out);
        });
    }
}

#[divan::bench_group(name = "opt_u64_per_call_N1_value", max_time = MAX)]
mod opt_u64_per_call_n1_value {
    use super::*;
    // Every call: 1-element iter with Some(v) ≠ sentinel.
    #[divan::bench]
    fn v0(bencher: Bencher) {
        let vals: Vec<Option<u64>> = (1..=OUTER_TINY as u64).map(Some).collect();
        let mut out = Vec::with_capacity(OUTER_TINY * 4);
        bencher.bench_local(|| {
            out.clear();
            for &v in &vals {
                let _ = UIntCursor::encode_unless_empty(&mut out, [v]);
            }
            black_box(&out);
        });
    }
    #[divan::bench]
    fn v1(bencher: Bencher) {
        let vals: Vec<Option<u64>> = (1..=OUTER_TINY as u64).map(Some).collect();
        let mut out = Vec::with_capacity(OUTER_TINY * 4);
        bencher.bench_local(|| {
            out.clear();
            for &v in &vals {
                let _ = v1::Encoder::<Option<u64>>::encode_to_unless(&mut out, [v], None);
            }
            black_box(&out);
        });
    }
}

// ─── bool (BooleanCursor / BoolEncoder) — typical "expand=false" sentinel ──

#[divan::bench_group(name = "bool_per_call_N1_sentinel", max_time = MAX)]
mod bool_per_call_n1_sentinel {
    use super::*;
    #[divan::bench]
    fn v0(bencher: Bencher) {
        let mut out = Vec::with_capacity(OUTER_TINY * 4);
        bencher.bench_local(|| {
            out.clear();
            for _ in 0..OUTER_TINY {
                let _ = BooleanCursor::encode_unless_empty(&mut out, [false]);
            }
            black_box(&out);
        });
    }
    #[divan::bench]
    fn v1(bencher: Bencher) {
        let mut out = Vec::with_capacity(OUTER_TINY * 4);
        bencher.bench_local(|| {
            out.clear();
            for _ in 0..OUTER_TINY {
                let _ = v1::Encoder::<bool>::encode_to_unless(&mut out, [false], false);
            }
            black_box(&out);
        });
    }
}

#[divan::bench_group(name = "bool_per_call_N1_value", max_time = MAX)]
mod bool_per_call_n1_value {
    use super::*;
    #[divan::bench]
    fn v0(bencher: Bencher) {
        let mut out = Vec::with_capacity(OUTER_TINY * 4);
        bencher.bench_local(|| {
            out.clear();
            for _ in 0..OUTER_TINY {
                let _ = BooleanCursor::encode_unless_empty(&mut out, [true]);
            }
            black_box(&out);
        });
    }
    #[divan::bench]
    fn v1(bencher: Bencher) {
        let mut out = Vec::with_capacity(OUTER_TINY * 4);
        bencher.bench_local(|| {
            out.clear();
            for _ in 0..OUTER_TINY {
                let _ = v1::Encoder::<bool>::encode_to_unless(&mut out, [true], false);
            }
            black_box(&out);
        });
    }
}

// ─── Delta<Option<i64>> (key_ctr / pred_ctr) ────────────────────────────────

#[divan::bench_group(name = "delta_opt_per_call_N1_sentinel", max_time = MAX)]
mod delta_opt_per_call_n1_sentinel {
    use super::*;
    #[divan::bench]
    fn v0(bencher: Bencher) {
        let mut out = Vec::with_capacity(OUTER_TINY * 4);
        bencher.bench_local(|| {
            out.clear();
            for _ in 0..OUTER_TINY {
                let v: Option<i64> = None;
                let _ = DeltaCursor::encode_unless_empty(&mut out, [v]);
            }
            black_box(&out);
        });
    }
    #[divan::bench]
    fn v1(bencher: Bencher) {
        let mut out = Vec::with_capacity(OUTER_TINY * 4);
        bencher.bench_local(|| {
            out.clear();
            for _ in 0..OUTER_TINY {
                let v: Option<i64> = None;
                let _ = v1::DeltaEncoder::<Option<i64>>::encode_to_unless(&mut out, [v], None);
            }
            black_box(&out);
        });
    }
}

#[divan::bench_group(name = "delta_opt_per_call_N1_value", max_time = MAX)]
mod delta_opt_per_call_n1_value {
    use super::*;
    #[divan::bench]
    fn v0(bencher: Bencher) {
        let vals: Vec<Option<i64>> = (1..=OUTER_TINY as i64).map(Some).collect();
        let mut out = Vec::with_capacity(OUTER_TINY * 8);
        bencher.bench_local(|| {
            out.clear();
            for &v in &vals {
                let _ = DeltaCursor::encode_unless_empty(&mut out, [v]);
            }
            black_box(&out);
        });
    }
    #[divan::bench]
    fn v1(bencher: Bencher) {
        let vals: Vec<Option<i64>> = (1..=OUTER_TINY as i64).map(Some).collect();
        let mut out = Vec::with_capacity(OUTER_TINY * 8);
        bencher.bench_local(|| {
            out.clear();
            for &v in &vals {
                let _ = v1::DeltaEncoder::<Option<i64>>::encode_to_unless(&mut out, [v], None);
            }
            black_box(&out);
        });
    }
}

// ─── Option<String> (key_str / mark_name) ───────────────────────────────────

#[divan::bench_group(name = "opt_str_per_call_N1_sentinel", max_time = MAX)]
mod opt_str_per_call_n1_sentinel {
    use super::*;
    #[divan::bench]
    fn v0(bencher: Bencher) {
        let mut out = Vec::with_capacity(OUTER_TINY * 4);
        bencher.bench_local(|| {
            out.clear();
            for _ in 0..OUTER_TINY {
                let v: Option<&str> = None;
                let _ = StrCursor::encode_unless_empty(&mut out, [v]);
            }
            black_box(&out);
        });
    }
    #[divan::bench]
    fn v1(bencher: Bencher) {
        let mut out = Vec::with_capacity(OUTER_TINY * 4);
        bencher.bench_local(|| {
            out.clear();
            for _ in 0..OUTER_TINY {
                let v: Option<&str> = None;
                let _ = v1::Encoder::<Option<String>>::encode_to_unless(&mut out, [v], None);
            }
            black_box(&out);
        });
    }
}

#[divan::bench_group(name = "opt_str_per_call_N1_value", max_time = MAX)]
mod opt_str_per_call_n1_value {
    use super::*;
    #[divan::bench]
    fn v0(bencher: Bencher) {
        let strs: Vec<String> = (0..OUTER_TINY).map(|i| format!("k{}", i % 1024)).collect();
        let mut out = Vec::with_capacity(OUTER_TINY * 8);
        bencher.bench_local(|| {
            out.clear();
            for s in &strs {
                let v: Option<&str> = Some(s.as_str());
                let _ = StrCursor::encode_unless_empty(&mut out, [v]);
            }
            black_box(&out);
        });
    }
    #[divan::bench]
    fn v1(bencher: Bencher) {
        let strs: Vec<String> = (0..OUTER_TINY).map(|i| format!("k{}", i % 1024)).collect();
        let mut out = Vec::with_capacity(OUTER_TINY * 8);
        bencher.bench_local(|| {
            out.clear();
            for s in &strs {
                let v: Option<&str> = Some(s.as_str());
                let _ = v1::Encoder::<Option<String>>::encode_to_unless(&mut out, [v], None);
            }
            black_box(&out);
        });
    }
}

// ─── u64 size sweep: chart the crossover ────────────────────────────────────
// 100k items total, partitioned into outer_calls of inner_size each.
// inner_size=1   → 100,000 calls × 1 item   (typing-style)
// inner_size=4   → 25,000  calls × 4 items
// inner_size=16  → 6,250   calls × 16 items
// inner_size=100 → 1,000   calls × 100 items
// inner_size=10000 → 10    calls × 10,000 items (one-shot-ish)

const SWEEP_TOTAL: usize = 100_000;

fn sweep_data(inner: usize) -> Vec<Vec<u64>> {
    let outer = SWEEP_TOTAL / inner;
    (0..outer)
        .map(|c| (0..inner).map(|i| (c * inner + i) as u64 + 1).collect())
        .collect()
}

#[divan::bench_group(name = "u64_sweep", max_time = MAX)]
mod u64_sweep {
    use super::*;
    const SIZES: &[usize] = &[1, 4, 16, 100, 1000, 10_000];

    #[divan::bench(args = SIZES)]
    fn v0(bencher: Bencher, inner: usize) {
        let chunks = sweep_data(inner);
        let mut out = Vec::with_capacity(SWEEP_TOTAL * 8);
        bencher.bench_local(|| {
            out.clear();
            for c in &chunks {
                let _ = UIntCursor::encode_unless_empty(&mut out, c.iter().copied());
            }
            black_box(&out);
        });
    }

    #[divan::bench(args = SIZES)]
    fn v1(bencher: Bencher, inner: usize) {
        let chunks = sweep_data(inner);
        let mut out = Vec::with_capacity(SWEEP_TOTAL * 8);
        bencher.bench_local(|| {
            out.clear();
            for c in &chunks {
                let _ = v1::Encoder::<u64>::encode_to_unless(&mut out, c.iter().copied(), 0);
            }
            black_box(&out);
        });
    }
}

// ─── Column build+save sweep ────────────────────────────────────────────────
// Same workload shape as `u64_sweep`, but each "chunk" is built as a column
// (DeltaColumn / PrefixColumn / Column) and saved.  Compared to the streaming
// encoder above, this measures the per-call overhead of building a column
// data structure (slab vec, index, etc.) — relevant if any code path uses
// these column types in the small-batch hot path.

fn sweep_i64(inner: usize) -> Vec<Vec<i64>> {
    let outer = SWEEP_TOTAL / inner;
    (0..outer)
        .map(|c| (0..inner).map(|i| (c * inner + i) as i64 + 1).collect())
        .collect()
}

#[divan::bench_group(name = "delta_col_sweep", max_time = MAX)]
mod delta_col_sweep {
    use super::*;
    const SIZES: &[usize] = &[1, 4, 16, 100, 1000, 10_000];

    #[divan::bench(args = SIZES)]
    fn v0(bencher: Bencher, inner: usize) {
        let chunks = sweep_i64(inner);
        let mut out = Vec::with_capacity(SWEEP_TOTAL * 8);
        bencher.bench_local(|| {
            out.clear();
            for c in &chunks {
                let _ = DeltaCursor::encode_unless_empty(&mut out, c.iter().copied());
            }
            black_box(&out);
        });
    }

    #[divan::bench(args = SIZES)]
    fn v1_encoder(bencher: Bencher, inner: usize) {
        let chunks = sweep_i64(inner);
        let mut out = Vec::with_capacity(SWEEP_TOTAL * 8);
        bencher.bench_local(|| {
            out.clear();
            for c in &chunks {
                let _ = v1::DeltaEncoder::<i64>::encode_to_unless(&mut out, c.iter().copied(), 0);
            }
            black_box(&out);
        });
    }

    /// Build via DeltaColumn::from_values + save_to.  This is what a caller
    /// who already has an in-memory column would do when serializing.
    #[divan::bench(args = SIZES)]
    fn v1_column(bencher: Bencher, inner: usize) {
        let chunks = sweep_i64(inner);
        let mut out = Vec::with_capacity(SWEEP_TOTAL * 8);
        bencher.bench_local(|| {
            out.clear();
            for c in &chunks {
                let col = DeltaColumn::<i64>::from_values(c.clone());
                let _ = col.save_to(&mut out);
            }
            black_box(&out);
        });
    }
}

#[divan::bench_group(name = "prefix_col_sweep", max_time = MAX)]
mod prefix_col_sweep {
    use super::*;
    const SIZES: &[usize] = &[1, 4, 16, 100, 1000, 10_000];

    /// PrefixColumn over u64 — v0 equivalent is RLE-encoded UInt (the
    /// PrefixColumn adds the BIT prefix index but the on-disk bytes are
    /// the same RLE encoding).
    #[divan::bench(args = SIZES)]
    fn v0(bencher: Bencher, inner: usize) {
        let chunks = sweep_data(inner);
        let mut out = Vec::with_capacity(SWEEP_TOTAL * 8);
        bencher.bench_local(|| {
            out.clear();
            for c in &chunks {
                let _ = UIntCursor::encode_unless_empty(&mut out, c.iter().copied());
            }
            black_box(&out);
        });
    }

    #[divan::bench(args = SIZES)]
    fn v1_encoder(bencher: Bencher, inner: usize) {
        let chunks = sweep_data(inner);
        let mut out = Vec::with_capacity(SWEEP_TOTAL * 8);
        bencher.bench_local(|| {
            out.clear();
            for c in &chunks {
                let _ = v1::Encoder::<u64>::encode_to_unless(&mut out, c.iter().copied(), 0);
            }
            black_box(&out);
        });
    }

    #[divan::bench(args = SIZES)]
    fn v1_column(bencher: Bencher, inner: usize) {
        let chunks = sweep_data(inner);
        let mut out = Vec::with_capacity(SWEEP_TOTAL * 8);
        bencher.bench_local(|| {
            out.clear();
            for c in &chunks {
                let col = PrefixColumn::<u64>::from_values(c.clone());
                let _ = col.save_to(&mut out);
            }
            black_box(&out);
        });
    }
}

// ─── Plain Column<u64> (no prefix index) sweep ──────────────────────────────
// Same shape as PrefixColumn but using `v1::Column<u64>` — which is what
// most callers use when they don't need prefix sums.

#[divan::bench_group(name = "u64_col_sweep", max_time = MAX)]
mod u64_col_sweep {
    use super::*;
    const SIZES: &[usize] = &[1, 4, 16, 100, 1000, 10_000];

    #[divan::bench(args = SIZES)]
    fn v1_column(bencher: Bencher, inner: usize) {
        let chunks = sweep_data(inner);
        let mut out = Vec::with_capacity(SWEEP_TOTAL * 8);
        bencher.bench_local(|| {
            out.clear();
            for c in &chunks {
                let col = v1::Column::<u64>::from_values(c.clone());
                let _ = col.save_to(&mut out);
            }
            black_box(&out);
        });
    }
}
