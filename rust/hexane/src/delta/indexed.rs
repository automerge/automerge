//! [`IndexedDeltaWeightFn`]: the `WeightFn` that backs [`DeltaColumn`].
//! Tracks `SlabAgg` (`len + total + min_offset + max_offset`) per slab
//! so `find_by_value` / `find_by_range` can prune via min/max in O(log n).
//!
//! All delta-semantics — insert / remove / splice / null handling /
//! iter — live in [`DeltaColumn`].  This module adds:
//!   * [`IndexedDeltaWeightFn`] — the `WeightFn` impl that walks the
//!     RLE decoder and produces `SlabAgg`.
//!   * Value-query methods (`find_by_value` / `find_by_range` /
//!     `find_first`) on [`DeltaColumn`].

use crate::sealed::Sealed;
use crate::PackError;
use std::ops::Range;

use crate::btree::SlabAgg;
use crate::column::{Dx, Slab, TailOf, WeightFn};
use crate::cursor::Inserting;
use crate::delta::agg::{agg_run_any, slab_agg, DeltaOps};
use crate::delta::ddomain::DeltaWrite;
use crate::delta::dfind::{d_find_new, d_find_next, DFind};
use crate::delta::{DeltaColumn, DeltaInner, DeltaValue};
use crate::encoding::ColumnEncoding;
use crate::ColumnValueRef;
use crate::{Codec, Leb128};
use std::marker::PhantomData;

// ── Weight function ─────────────────────────────────────────────────────────

/// Per-slab weight: `SlabAgg` with `min_offset`/`max_offset` for
/// value-range pruning.
#[derive(Copy, Clone, Debug, Default)]
pub struct IndexedDeltaWeightFn;

impl Sealed for IndexedDeltaWeightFn {}

impl<I: DeltaInner, C: Codec> WeightFn<I, C> for IndexedDeltaWeightFn {
    type Weight = SlabAgg;
    type Model = crate::weights::DeltaModel<Dx<I, C>, DB<I, C>>;

    // verified (`agg::slab_agg`), given a well-formed slab
    fn compute(slab: &Slab<TailOf<I, C>>) -> SlabAgg {
        slab_agg::<Dx<I, C>, DB<I, C>>(slab)
    }

    // `compute` re-decodes the slab; let the streaming loader accumulate
    // weights as the runs go past instead.
    const ACCUMULATES: bool = true;

    /// Incremental twin of `compute` (`agg::slab_agg`): both fold each
    /// run with the same verified step (`agg_run`), so they agree by
    /// construction.
    ///
    /// Checked arithmetic: this runs on untrusted bytes during load, and
    /// a within-slab partial overflowing `i64` is provably invalid — the
    /// [`DeltaValue`](crate::delta::DeltaValue) domain contract bounds
    /// all realized values to a 2^63-wide window, so offsets between
    /// values in one slab always fit. Erroring here (instead of wrapping,
    /// which hostile bytes can steer back into plausible-looking ranges)
    /// is what lets [`DeltaColumn`] validate the finished aggregates per
    /// *slab* rather than per run.
    #[inline]
    fn accumulate_run(w: &mut SlabAgg, count: usize, value: I::Get<'_>) -> Result<(), PackError> {
        // verified: the aggregate stays exact, or a sum leaves `i64` (or
        // the count overflows)
        if agg_run_any(w, count, I::to_opt(value)) {
            Ok(())
        } else {
            Err(PackError::InvalidValue(
                "delta running sum overflows i64".into(),
            ))
        }
    }
}

/// A delta column's values as deltas, for the verified aggregate
/// (`agg::slab_agg`). Trusted: `DeltaInner::to_opt`.
#[doc(hidden)]
pub struct DB<I, C = Leb128>(PhantomData<fn() -> (I, C)>);

impl<'a, I: DeltaInner, C: Codec> DeltaOps<I::Get<'a>> for DB<I, C> {
    #[cfg(verus_keep_ghost)]
    fn dv(_v: I::Get<'a>) -> Option<i64> {
        unimplemented!()
    }

    #[inline(always)]
    fn to_opt(v: I::Get<'a>) -> Option<i64> {
        I::to_opt(v)
    }
}

/// The delta column's edit model.
type DModel<I, C> = <<I as crate::ColumnValueRef>::Encoding<C> as crate::edit::SlabEdit>::Model;

/// A delta column's values as written, for the verified splice
/// (`ColumnCore::splice_delta`). Trusted: `DeltaInner::from_opt` writes
/// the delta it is given (`to_opt` reads it back), and a value read holds
/// the delta its owned form does.
impl<'a, I: DeltaInner, C: Codec> DeltaWrite<'a, DModel<I, C>, Dx<I, C>> for DB<I, C> {
    type Ins = Inserting<<I as crate::ColumnValueRef>::Encoding<C>, I>;

    #[cfg(verus_keep_ghost)]
    fn dvo(_o: <DModel<I, C> as crate::cursor::Slabs>::V) -> Option<i64> {
        unimplemented!()
    }

    #[cfg(verus_keep_ghost)]
    fn axiom_own(_v: I::Get<'a>) {
        unimplemented!()
    }

    #[inline(always)]
    fn ins(d: Option<i64>) -> Self::Ins {
        Inserting::new(I::from_opt(d))
    }
}

// ── Value-query methods ────────────────────────────────────────────────────

impl<T: DeltaValue, C: Codec> DeltaColumn<T, C> {
    /// Iterator over all indices whose realized value equals `target`.
    ///
    /// A `target` outside the [`DeltaValue`] domain (e.g. unsigned
    /// `> i64::MAX`) can never be stored, so the iterator is simply
    /// empty — matching [`find_by_range`](Self::find_by_range)'s
    /// behaviour for unrepresentable bounds.
    pub fn find_by_value(&self, target: T) -> FindByRange<'_, T, C> {
        match target.try_to_i64() {
            // inclusive: `v..v + 1` would overflow at `i64::MAX`
            Some(v) => FindByRange::between(v, v, self),
            None => FindByRange::default(),
        }
    }

    pub fn find_first(&self, target: T) -> Option<usize> {
        self.find_by_value(target).next()
    }

    /// Iterator over indices whose realized value lies in the half-open
    /// range `[range.start, range.end)` — matching Rust's `Range<T>`
    /// and v0's `find_by_range` semantics.  Generic over any integer
    /// type `X` so callers can pass `u32`, `i64`, or the delta's native
    /// `T` interchangeably. Bounds needn't fit an `i64`:
    /// `5u64..u64::MAX` finds every value from 5 up.
    pub fn find_by_range<X>(&self, range: Range<X>) -> FindByRange<'_, T, C>
    where
        X: TryInto<i64> + PartialOrd + Default,
    {
        // a bound that doesn't fit an `i64` lies above every value if it
        // is positive, below every value if negative
        let start_above = range.start > X::default();
        let end_above = range.end > X::default();
        let lo = match range.start.try_into() {
            Ok(lo) => lo,
            Err(_) if start_above => return FindByRange::default(),
            Err(_) => i64::MIN,
        };
        let hi = match range.end.try_into() {
            // the last value in the range is `hi - 1`
            Ok(hi) if hi > lo => hi - 1,
            Ok(_) => return FindByRange::default(),
            Err(_) if end_above => i64::MAX,
            Err(_) => return FindByRange::default(),
        };
        FindByRange::between(lo, hi, self)
    }
}

/// Iterator returned by [`DeltaColumn::find_by_range`] and
/// [`DeltaColumn::find_by_value`].  Walks the B-tree's pruned slabs and,
/// for each candidate, scans its runs (verified: `delta::dfind`).
pub struct FindByRange<'a, T: DeltaValue, C: Codec = Leb128> {
    slabs: &'a [Slab<TailOf<T::Inner, C>>],
    find: Option<DFind<'a, InnerDecoder<'a, T, C>>>,
}

type InnerDecoder<'a, T, C> =
    <<<T as DeltaValue>::Inner as ColumnValueRef>::Encoding<C> as ColumnEncoding>::Decoder<'a>;

impl<T: DeltaValue, C: Codec> Default for FindByRange<'_, T, C> {
    fn default() -> Self {
        Self {
            slabs: &[],
            find: None,
        }
    }
}

impl<'a, T: DeltaValue, C: Codec> FindByRange<'a, T, C> {
    /// Build a `FindByRange` over `col` for the half-open range `[lo, hi)`.
    /// If `hi <= lo` the returned iterator is empty.
    pub fn new(lo: i64, hi: i64, col: &'a DeltaColumn<T, C>) -> Self {
        if hi > lo {
            Self::between(lo, hi - 1, col)
        } else {
            Self::default()
        }
    }

    /// The values in `[lo, hi]`, inclusive; `lo <= hi`.
    fn between(lo: i64, hi: i64, col: &'a DeltaColumn<T, C>) -> Self {
        // the readers' invariant, from the column's (verified,
        // `ColumnCore::delta_parts`)
        let (slabs, index) = col.col.core.delta_parts();
        Self {
            slabs,
            find: Some(d_find_new::<Dx<T::Inner, C>, DB<T::Inner, C>>(
                index, lo, hi, slabs,
            )),
        }
    }
}

impl<T: DeltaValue, C: Codec> Iterator for FindByRange<'_, T, C> {
    type Item = usize;
    // verified (`delta::dfind`): the next item whose value is in range,
    // given the index's slab aggregates
    fn next(&mut self) -> Option<usize> {
        let find = self.find.as_mut()?;
        d_find_next::<Dx<T::Inner, C>, DB<T::Inner, C>>(find, self.slabs)
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use crate::delta::*;

    #[test]
    fn empty() {
        let col = DeltaColumn::<u64>::new();
        assert_eq!(col.len(), 0);
        assert!(col.is_empty());
        assert_eq!(col.find_first(0), None);
        assert_eq!(
            col.find_by_value(42).collect::<Vec<_>>(),
            Vec::<usize>::new()
        );
    }

    #[test]
    fn unique_counters() {
        let vals: Vec<u64> = (1..=100).collect();
        let col = DeltaColumn::<u64>::from_values(vals.clone());
        for (i, &v) in vals.iter().enumerate() {
            assert_eq!(col.find_first(v), Some(i), "v={v}");
        }
        assert_eq!(col.find_first(200), None);
    }

    #[test]
    fn duplicates() {
        let vals = vec![1u64, 2, 2, 3, 2, 4];
        let col = DeltaColumn::<u64>::from_values(vals);
        let twos: Vec<_> = col.find_by_value(2).collect();
        assert_eq!(twos, vec![1, 2, 4]);
    }

    #[test]
    fn range_basic() {
        let vals: Vec<u64> = (0..100).map(|i| i * 10).collect();
        let col = DeltaColumn::<u64>::from_values(vals);
        let hits: Vec<_> = col.find_by_range(200..400).collect();
        assert_eq!(
            hits,
            vec![20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39]
        );
    }

    #[test]
    fn range_empty() {
        let vals: Vec<u64> = (0..10).collect();
        let col = DeltaColumn::<u64>::from_values(vals);
        assert_eq!(col.find_by_range(5..5).count(), 0);
        let mut reversed = std::ops::Range {
            start: 0u64,
            end: 0,
        };
        reversed.start = 7;
        reversed.end = 3;
        assert_eq!(col.find_by_range(reversed).count(), 0);
    }

    #[test]
    fn range_shortcut_brute_force_parity() {
        let vals: Vec<i64> = (0..5_000).collect();
        let col = DeltaColumn::<i64>::from_values(vals.clone());
        for &(lo, hi) in &[
            (0i64, 1),
            (0, 100),
            (100, 200),
            (4999, 5000),
            (0, 5000),
            (-10, 10),
            (4995, 10_000),
            (42, 42),
            (100, 99),
        ] {
            let got: Vec<usize> = if hi > lo {
                col.find_by_range(lo..hi).collect()
            } else {
                Vec::new()
            };
            let want: Vec<usize> = vals
                .iter()
                .enumerate()
                .filter(|(_, &v)| v >= lo && v < hi)
                .map(|(i, _)| i)
                .collect();
            assert_eq!(got, want, "range {lo}..{hi}");
        }
    }

    #[test]
    fn find_by_value_matches_find_by_range_half_open() {
        let mut seed = 0xA11u64;
        let vals: Vec<u64> = (0..500)
            .map(|_| {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                seed % 100
            })
            .collect();
        let col = DeltaColumn::<u64>::from_values(vals);
        for t in 0u64..=110 {
            let v: Vec<usize> = col.find_by_value(t).collect();
            let r: Vec<usize> = col.find_by_range(t..t + 1).collect();
            assert_eq!(v, r, "target {t}: value vs range half-open diverged");
        }
    }

    #[test]
    fn nullable_values() {
        let vals = vec![Some(10i64), None, Some(20), None, None, Some(15)];
        let col = DeltaColumn::<Option<i64>>::from_values(vals.clone());
        assert_eq!(col.find_first(Some(10)), Some(0));
        assert_eq!(col.find_first(Some(20)), Some(2));
        assert_eq!(col.find_first(Some(15)), Some(5));
        assert_eq!(col.find_first(Some(999)), None);
        let iter_vals: Vec<Option<i64>> = col.iter().collect();
        assert_eq!(iter_vals, vals);
    }

    fn reference_find(values: &[u64], target: u64) -> Vec<usize> {
        values
            .iter()
            .enumerate()
            .filter(|(_, &v)| v == target)
            .map(|(i, _)| i)
            .collect()
    }

    #[test]
    fn find_by_value_fuzz() {
        let mut seed = 1u64;
        let values: Vec<u64> = (0..500)
            .map(|_| {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                seed % 10_000
            })
            .collect();
        let col = DeltaColumn::<u64>::from_values(values.clone());
        for &v in values.iter().take(20) {
            let got: Vec<usize> = col.find_by_value(v).collect();
            let want = reference_find(&values, v);
            assert_eq!(got, want, "find_by_value({v})");
        }
    }
}
