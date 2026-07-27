//! Model-based splice fuzzing with slab-boundary targeting and automatic
//! shrinking.
//!
//! # Why this exists
//!
//! A `Column<bool>` splice once duplicated the run at a slab boundary,
//! inserting more items than it was given. Every fuzzer in the crate
//! missed it, and the reasons are worth keeping written down — they are
//! what this harness is built around.
//!
//! 1. **Self-consistency is not correctness.** [`validate_column`] checks
//!    `total_len == Σ slab.len` and that each slab's metadata matches its
//!    own bytes. A slab holding a duplicated run satisfies all of that: it
//!    honestly contains the extra items and the totals add up. Only an
//!    independent model notices. So every step here compares against a
//!    `Vec`.
//!
//! 2. **A fresh column has a canonical layout.** `from_values` and `load`
//!    lay slabs out in one pass, and the bug needed a layout accumulated
//!    over many splices — replaying the offending splice against a
//!    reloaded column passed. So these fuzzers splice repeatedly into one
//!    long-lived column instead of rebuilding per op.
//!
//! 3. **Small edits never reach the interesting code.** The overflow path
//!    only runs when a single splice pushes a slab past its segment
//!    budget. Fuzzers that insert one to five values never enter it. So
//!    insert sizes here range up to several times `max_segments`.
//!
//! 4. **Boundaries are where encoders go wrong.** Positions are drawn
//!    deliberately at and adjacent to slab boundaries, not just uniformly,
//!    which is what makes the interesting cases common instead of rare.
//!
//! On failure the op sequence is shrunk and printed as a compact literal
//! that can be pasted into a regression test.

use crate::{Column, ColumnValueRef, DeltaColumn, PrefixColumn};
use std::fmt::Debug;

// ── deterministic rng ───────────────────────────────────────────────────────

pub(crate) struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        // avoid the xorshift fixed point at 0
        Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            0
        } else {
            (self.next() as usize) % n
        }
    }
    fn chance(&mut self, one_in: usize) -> bool {
        self.below(one_in) == 0
    }
}

// ── the target abstraction ──────────────────────────────────────────────────

/// A column type the harness can drive. Implementors expose enough to run
/// the model comparison plus, where the internals are reachable, slab
/// boundaries (for position targeting) and structural invariants.
pub(crate) trait FuzzTarget: Sized {
    type Val: Clone + PartialEq + Debug;

    fn name() -> &'static str;
    fn new(max_segments: usize) -> Self;
    fn len(&self) -> usize;
    fn splice(&mut self, pos: usize, del: usize, vals: &[Self::Val]);
    /// The column's contents, in a form directly comparable to the model.
    fn to_model(&self) -> Vec<Self::Val>;
    /// Contents after a `save` / `load` round trip.
    fn round_trip(&self) -> Vec<Self::Val>;
    /// Cumulative slab boundaries, for position targeting.
    fn boundaries(&self) -> Vec<usize>;
    /// Structural invariants (slab metadata vs its own bytes).
    /// `cap` says whether the `segments <= max_segments` bound is
    /// checkable — see [`FuzzTarget::SEGMENT_CAP_MIN`].
    fn check_internal(&self, cap: bool);

    /// Smallest `max_segments` at which the per-slab segment cap is
    /// actually satisfiable for this encoding. Bool slabs must start on a
    /// `false` run, so a leading `true` run already costs a zero-count pad
    /// plus the run itself; add a boundary partial and two is impossible.
    /// The content oracles still run at those budgets — only the cap
    /// assertion is skipped.
    const SEGMENT_CAP_MIN: usize = 0;
    /// Generate `n` values with the run structure this encoding cares about.
    fn gen(rng: &mut Rng, n: usize) -> Vec<Self::Val>;
}

fn column_boundaries<T, C, WF, Idx>(col: &Column<T, C, WF, Idx>) -> Vec<usize>
where
    T: ColumnValueRef,
    C: crate::Codec,
    WF: crate::column::WeightFn<T, C>,
    Idx: crate::index::ColumnIndex<WF::Weight>,
{
    let mut acc = 0;
    col.slabs
        .iter()
        .map(|s| {
            acc += s.len;
            acc
        })
        .collect()
}

/// The structural half of the oracle: totals agree and every slab's
/// metadata matches the bytes it actually holds.
fn check_column<T, C, WF, Idx>(col: &Column<T, C, WF, Idx>, cap: bool)
where
    T: ColumnValueRef,
    C: crate::Codec,
    WF: crate::column::WeightFn<T, C>,
    Idx: crate::index::ColumnIndex<WF::Weight>,
{
    use crate::encoding::ColumnEncoding;
    let sum: usize = col.slabs.iter().map(|s| s.len).sum();
    assert_eq!(
        col.total_len, sum,
        "total_len {} vs Σ slab.len {sum}",
        col.total_len
    );
    for (i, slab) in col.slabs.iter().enumerate() {
        assert!(
            !cap || slab.segments <= col.max_segments,
            "slab {i}: {} segments exceeds max {}",
            slab.segments,
            col.max_segments
        );
        let info = T::Encoding::<C>::validate_encoding(&slab.data)
            .unwrap_or_else(|e| panic!("slab {i}: invalid encoding: {e}"));
        assert_eq!(slab.len, info.len, "slab {i}: len");
        assert_eq!(slab.segments, info.segments, "slab {i}: segments");
    }
}

// ── the harness ─────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub(crate) struct Op<V> {
    pos: usize,
    del: usize,
    vals: Vec<V>,
}

/// How a step is shaped. Sizes are expressed relative to `max_segments`
/// so the segment budget is crossed regularly whatever it is set to.
fn gen_op<T: FuzzTarget>(
    rng: &mut Rng,
    len: usize,
    bounds: &[usize],
    max_seg: usize,
) -> Op<T::Val> {
    // Positions: uniform, but often snapped to a slab boundary or just
    // either side of one, which is where encoders split and rejoin runs.
    let pos = if bounds.is_empty() || rng.chance(3) {
        rng.below(len + 1)
    } else {
        let b = bounds[rng.below(bounds.len())];
        match rng.below(4) {
            0 => b.min(len),
            1 => b.saturating_sub(1),
            2 => (b + 1).min(len),
            _ => rng.below(len + 1),
        }
    };

    let remaining = len - pos;
    let del = match rng.below(6) {
        0 => 0,
        1 => remaining.min(1),
        2 => remaining.min(rng.below(max_seg * 2 + 2)),
        3 => remaining.min(rng.below(16)),
        // whole-slab and slab-spanning deletes
        4 => {
            let target = bounds.iter().copied().find(|b| *b > pos).unwrap_or(len);
            (target - pos).min(remaining)
        }
        _ => rng.below(remaining + 1),
    };

    // Insert sizes: frequently large enough to blow through the segment
    // budget inside a single splice, which is the overflow path.
    let n = match rng.below(6) {
        0 => 0,
        1 => 1,
        2 => rng.below(max_seg + 2),
        3 => max_seg * 2 + rng.below(max_seg * 2 + 1),
        4 => rng.below(200),
        _ => rng.below(max_seg * 4 + 4),
    };

    Op {
        pos,
        del,
        vals: T::gen(rng, n),
    }
}

/// Replay a sequence; `None` if it completes cleanly, otherwise a message.
/// Positions are clamped so a shrunk sequence stays replayable.
fn replay<T: FuzzTarget>(max_seg: usize, ops: &[Op<T::Val>], deep: bool) -> Option<String> {
    let cap = max_seg >= T::SEGMENT_CAP_MIN;
    let mut col = T::new(max_seg);
    let mut model: Vec<T::Val> = Vec::new();

    for (i, op) in ops.iter().enumerate() {
        let pos = op.pos.min(model.len());
        let del = op.del.min(model.len() - pos);

        let before = col.len();
        col.splice(pos, del, &op.vals);
        model.splice(pos..pos + del, op.vals.iter().cloned());

        let grew = col.len() as i64 - before as i64;
        let want = op.vals.len() as i64 - del as i64;
        if grew != want {
            return Some(format!(
                "op {i} splice(pos={pos}, del={del}, {} values): len grew by {grew}, expected {want}",
                op.vals.len()
            ));
        }
        let got = col.to_model();
        if got.len() != col.len() {
            return Some(format!(
                "op {i} splice(pos={pos}, del={del}, {} values): len() says {} but iteration yields {}",
                op.vals.len(),
                col.len(),
                got.len()
            ));
        }
        if got != model {
            let at = (0..got.len().min(model.len()))
                .find(|&k| got[k] != model[k])
                .map(|k| k.to_string())
                .unwrap_or_else(|| "end".into());
            return Some(format!(
                "op {i} splice(pos={pos}, del={del}, {} values): contents diverge at {at}",
                op.vals.len()
            ));
        }
        col.check_internal(cap);
        // the round trip is O(n); it is the slowest oracle, so it runs on
        // the last op always and occasionally in between
        if deep || i + 1 == ops.len() {
            let rt = col.round_trip();
            if rt != model {
                return Some(format!(
                    "op {i} splice(pos={pos}, del={del}, {} values): save/load round trip diverges",
                    op.vals.len()
                ));
            }
        }
    }
    None
}

/// Remove ops, then trim values, while the failure survives.
fn shrink<T: FuzzTarget>(max_seg: usize, mut ops: Vec<Op<T::Val>>) -> Vec<Op<T::Val>> {
    // drop whole ops
    let mut i = 0;
    while i < ops.len() {
        let mut cand = ops.clone();
        cand.remove(i);
        if replay::<T>(max_seg, &cand, false).is_some() {
            ops = cand;
        } else {
            i += 1;
        }
    }
    // trim trailing ops past the failing one
    if let Some(msg) = replay::<T>(max_seg, &ops, false) {
        if let Some(n) = msg
            .strip_prefix("op ")
            .and_then(|s| s.split_whitespace().next())
            .and_then(|s| s.parse::<usize>().ok())
        {
            ops.truncate(n + 1);
        }
    }
    // halve value lists
    for i in 0..ops.len() {
        loop {
            let n = ops[i].vals.len();
            if n == 0 {
                break;
            }
            let mut cand = ops.clone();
            cand[i].vals.truncate(n / 2);
            if replay::<T>(max_seg, &cand, false).is_some() {
                ops = cand;
            } else {
                break;
            }
        }
    }
    ops
}

fn report<T: FuzzTarget>(max_seg: usize, seed: u64, msg: &str, ops: Vec<Op<T::Val>>) -> ! {
    let small = shrink::<T>(max_seg, ops);
    let msg2 = replay::<T>(max_seg, &small, true).unwrap_or_else(|| msg.to_string());
    let mut out = String::new();
    out.push_str(&format!(
        "\n{} FAILED (seed {seed}, max_segments {max_seg}): {msg2}\n\
         shrunk to {} ops:\n",
        T::name(),
        small.len()
    ));
    for op in &small {
        out.push_str(&format!(
            "    ({}, {}, vec!{:?}),\n",
            op.pos, op.del, op.vals
        ));
    }
    panic!("{out}");
}

/// Run `seeds` sequences of `steps` splices for each segment budget.
pub(crate) fn run<T: FuzzTarget>(seeds: u64, steps: usize, budgets: &[usize]) {
    for &max_seg in budgets {
        for seed in 0..seeds {
            let cap = max_seg >= T::SEGMENT_CAP_MIN;
            let mut rng = Rng::new(seed ^ ((max_seg as u64) << 32));
            let mut col = T::new(max_seg);
            let mut model: Vec<T::Val> = Vec::new();
            let mut ops: Vec<Op<T::Val>> = Vec::new();

            for _ in 0..steps {
                let bounds = col.boundaries();
                let op = gen_op::<T>(&mut rng, model.len(), &bounds, max_seg);

                let before = col.len();
                col.splice(op.pos, op.del, &op.vals);
                model.splice(op.pos..op.pos + op.del, op.vals.iter().cloned());
                ops.push(op.clone());

                let grew = col.len() as i64 - before as i64;
                let want = op.vals.len() as i64 - op.del as i64;
                if grew != want {
                    report::<T>(max_seg, seed, "length arithmetic", ops);
                }
                let got = col.to_model();
                if got.len() != col.len() || got != model {
                    report::<T>(max_seg, seed, "contents", ops);
                }
                col.check_internal(cap);
            }

            if col.round_trip() != model {
                report::<T>(max_seg, seed, "save/load round trip", ops);
            }
        }
    }
}

// ── targets ─────────────────────────────────────────────────────────────────

macro_rules! plain_column {
    ($ty:ty, $name:literal, $gen:expr) => {
        plain_column!($ty, $name, $gen, 0);
    };
    ($ty:ty, $name:literal, $gen:expr, $cap_min:expr) => {
        impl FuzzTarget for Column<$ty> {
            type Val = $ty;
            const SEGMENT_CAP_MIN: usize = $cap_min;
            fn name() -> &'static str {
                $name
            }
            fn new(max_segments: usize) -> Self {
                Column::with_max_segments(max_segments)
            }
            fn len(&self) -> usize {
                Column::len(self)
            }
            fn splice(&mut self, pos: usize, del: usize, vals: &[$ty]) {
                Column::splice(self, pos, del, vals.iter().cloned())
            }
            fn to_model(&self) -> Vec<$ty> {
                self.iter().collect()
            }
            fn round_trip(&self) -> Vec<$ty> {
                Column::<$ty>::load(&self.save()).unwrap().iter().collect()
            }
            fn boundaries(&self) -> Vec<usize> {
                column_boundaries(self)
            }
            fn check_internal(&self, cap: bool) {
                check_column(self, cap)
            }
            fn gen(rng: &mut Rng, n: usize) -> Vec<$ty> {
                #[allow(clippy::redundant_closure_call)]
                ($gen)(rng, n)
            }
        }
    };
}

/// Values in runs: `distinct` controls how often the value changes, which
/// is what decides run length and therefore segment pressure.
fn runs_of<V: Clone>(rng: &mut Rng, n: usize, mut pick: impl FnMut(&mut Rng) -> V) -> Vec<V> {
    let mut out = Vec::with_capacity(n);
    while out.len() < n {
        let v = pick(rng);
        // occasional very long runs alongside rapid alternation
        let run = if rng.chance(8) {
            1 + rng.below(40)
        } else {
            1 + rng.below(4)
        };
        for _ in 0..run.min(n - out.len()) {
            out.push(v.clone());
        }
    }
    out
}

plain_column!(
    bool,
    "Column<bool>",
    |rng: &mut Rng, n| runs_of(rng, n, |r| r.chance(2)),
    3
);

plain_column!(u64, "Column<u64> (runs)", |rng: &mut Rng, n| runs_of(
    rng,
    n,
    |r| r.next() % 3
));

plain_column!(Option<u64>, "Column<Option<u64>>", |rng: &mut Rng, n| {
    runs_of(rng, n, |r| {
        if r.chance(3) {
            None
        } else {
            Some(r.next() % 4)
        }
    })
});

impl FuzzTarget for Column<Option<String>> {
    type Val = Option<String>;
    fn name() -> &'static str {
        "Column<Option<String>>"
    }
    fn new(max_segments: usize) -> Self {
        Column::with_max_segments(max_segments)
    }
    fn len(&self) -> usize {
        Column::len(self)
    }
    fn splice(&mut self, pos: usize, del: usize, vals: &[Self::Val]) {
        Column::splice(self, pos, del, vals.iter().cloned())
    }
    fn to_model(&self) -> Vec<Self::Val> {
        self.iter().map(|v| v.map(|s| s.to_string())).collect()
    }
    fn round_trip(&self) -> Vec<Self::Val> {
        Column::<Option<String>>::load(&self.save())
            .unwrap()
            .iter()
            .map(|v| v.map(|s| s.to_string()))
            .collect()
    }
    fn boundaries(&self) -> Vec<usize> {
        column_boundaries(self)
    }
    fn check_internal(&self, cap: bool) {
        check_column(self, cap)
    }
    fn gen(rng: &mut Rng, n: usize) -> Vec<Self::Val> {
        runs_of(rng, n, |r| match r.next() % 4 {
            0 => None,
            k => Some(format!("v{k}")),
        })
    }
}

impl FuzzTarget for DeltaColumn<i64> {
    type Val = i64;
    fn name() -> &'static str {
        "DeltaColumn<i64>"
    }
    fn new(max_segments: usize) -> Self {
        DeltaColumn::with_max_segments(max_segments)
    }
    fn len(&self) -> usize {
        DeltaColumn::len(self)
    }
    fn splice(&mut self, pos: usize, del: usize, vals: &[i64]) {
        DeltaColumn::splice(self, pos, del, vals.iter().cloned())
    }
    fn to_model(&self) -> Vec<i64> {
        self.iter().collect()
    }
    fn round_trip(&self) -> Vec<i64> {
        DeltaColumn::<i64>::load(&self.save())
            .unwrap()
            .iter()
            .collect()
    }
    fn boundaries(&self) -> Vec<usize> {
        column_boundaries(&self.col)
    }
    fn check_internal(&self, cap: bool) {
        check_column(&self.col, cap)
    }
    fn gen(rng: &mut Rng, n: usize) -> Vec<i64> {
        // both smooth deltas (well compressed) and jumps (literals)
        let mut out = Vec::with_capacity(n);
        let mut cur = 0i64;
        for _ in 0..n {
            if rng.chance(10) {
                cur = cur.wrapping_add((rng.next() % 100_000) as i64);
            } else {
                cur += (rng.next() % 5) as i64 - 2;
            }
            out.push(cur);
        }
        out
    }
}

impl FuzzTarget for PrefixColumn<bool> {
    type Val = bool;
    const SEGMENT_CAP_MIN: usize = 3;
    fn name() -> &'static str {
        "PrefixColumn<bool>"
    }
    fn new(max_segments: usize) -> Self {
        PrefixColumn::with_max_segments(max_segments)
    }
    fn len(&self) -> usize {
        PrefixColumn::len(self)
    }
    fn splice(&mut self, pos: usize, del: usize, vals: &[bool]) {
        PrefixColumn::splice(self, pos, del, vals.iter().cloned())
    }
    fn to_model(&self) -> Vec<bool> {
        self.values().iter().collect()
    }
    fn round_trip(&self) -> Vec<bool> {
        PrefixColumn::<bool>::load(&self.save())
            .unwrap()
            .values()
            .iter()
            .collect()
    }
    fn boundaries(&self) -> Vec<usize> {
        column_boundaries(&self.col)
    }
    fn check_internal(&self, cap: bool) {
        check_column(&self.col, cap)
    }
    fn gen(rng: &mut Rng, n: usize) -> Vec<bool> {
        runs_of(rng, n, |r| r.chance(2))
    }
}

// ── tests ───────────────────────────────────────────────────────────────────

// Budgets deliberately include the tiny ones: they make the overflow and
// slab-split paths common rather than rare, and 64 is what automerge runs.
const BUDGETS: &[usize] = &[2, 3, 4, 8, 16, 64];

macro_rules! fuzz_tests {
    ($($fast:ident, $slow:ident, $ty:ty;)*) => {
        $(
            #[test]
            fn $fast() {
                run::<$ty>(12, 40, BUDGETS);
            }

            #[test]
            #[ignore]
            fn $slow() {
                run::<$ty>(200, 120, BUDGETS);
            }
        )*
    };
}

fuzz_tests! {
    splice_fuzz_bool, splice_fuzz_bool_long, Column<bool>;
    splice_fuzz_u64, splice_fuzz_u64_long, Column<u64>;
    splice_fuzz_option_u64, splice_fuzz_option_u64_long, Column<Option<u64>>;
    splice_fuzz_option_string, splice_fuzz_option_string_long, Column<Option<String>>;
    splice_fuzz_delta_i64, splice_fuzz_delta_i64_long, DeltaColumn<i64>;
    splice_fuzz_prefix_bool, splice_fuzz_prefix_bool_long, PrefixColumn<bool>;
}
