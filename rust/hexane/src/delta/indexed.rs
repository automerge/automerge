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

use crate::btree::{FindByValueRange, SlabAgg};
use crate::column::{Slab, TailOf, WeightFn};
use crate::{Codec, Leb128};
use crate::delta::{DeltaColumn, DeltaInner, DeltaValue};
use crate::encoding::{ColumnEncoding, RunDecoder};
use crate::ColumnValueRef;

// Type aliases to keep the decoder / slab types readable in struct fields.
type OptI64Encoding<C> = <Option<i64> as ColumnValueRef>::Encoding<C>;
type OptI64Decoder<'a, C> = <OptI64Encoding<C> as ColumnEncoding>::Decoder<'a>;

// ── Weight function ─────────────────────────────────────────────────────────

/// Per-slab weight: `SlabAgg` with `min_offset`/`max_offset` for
/// value-range pruning.
#[derive(Copy, Clone, Debug, Default)]
pub struct IndexedDeltaWeightFn;

impl Sealed for IndexedDeltaWeightFn {}

impl<I: DeltaInner, C: Codec> WeightFn<I, C> for IndexedDeltaWeightFn {
    type Weight = SlabAgg;

    fn compute(slab: &Slab<TailOf<I, C>>) -> SlabAgg {
        compute_slab_agg::<I, C>(&slab.data)
    }

    // `compute` re-decodes the slab; let the streaming loader accumulate
    // weights as the runs go past instead.
    const ACCUMULATES: bool = true;

    /// Incremental twin of [`compute_slab_agg`] — same endpoint math, with
    /// `len == 0` standing in for the "no runs yet" min/max sentinels (an
    /// empty weight stays `SlabAgg::default()`, matching `compute`).
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
        let Some(v) = I::to_opt(value) else {
            // a null run holds the partial where it is
            if w.len == 0 {
                w.min_offset = 0;
                w.max_offset = 0;
            } else {
                w.min_offset = w.min_offset.min(w.total);
                w.max_offset = w.max_offset.max(w.total);
            }
            w.len += count;
            return Ok(());
        };
        let overflow = || PackError::InvalidValue("delta running sum overflows i64".into());
        let step = i64::try_from(count)
            .ok()
            .and_then(|c| v.checked_mul(c))
            .ok_or_else(overflow)?;
        let first = w.total.checked_add(v).ok_or_else(overflow)?;
        let last = w.total.checked_add(step).ok_or_else(overflow)?;
        let (lo, hi) = (first.min(last), first.max(last));
        if w.len == 0 {
            w.min_offset = lo;
            w.max_offset = hi;
        } else {
            w.min_offset = w.min_offset.min(lo);
            w.max_offset = w.max_offset.max(hi);
        }
        w.len += count;
        w.total = last;
        Ok(())
    }
}

/// Compute a `SlabAgg` by walking the encoding's run decoder.
///
/// For a run of `count` items with delta `v`, the realized-prefix
/// progression is monotonic: `partial + v`, `partial + 2v`, …,
/// `partial + count·v`.  Min/max are at the endpoints — no need to
/// visit each intermediate item.
fn compute_slab_agg<I: DeltaInner, C: Codec>(data: &[u8]) -> SlabAgg {
    let mut decoder = <I as ColumnValueRef>::Encoding::<C>::decoder(data);
    let mut partial = 0i64;
    let mut min_off = i64::MAX;
    let mut max_off = i64::MIN;
    let mut len = 0usize;

    while let Some(run) = decoder.next_run() {
        len += run.count;
        match I::to_opt(run.value) {
            None => {
                min_off = min_off.min(partial);
                max_off = max_off.max(partial);
            }
            Some(v) => {
                let first = partial + v;
                let last = partial + v * run.count as i64;
                min_off = min_off.min(first.min(last));
                max_off = max_off.max(first.max(last));
                partial += v * run.count as i64;
            }
        }
    }

    if len == 0 {
        SlabAgg::default()
    } else {
        SlabAgg {
            len,
            total: partial,
            min_offset: min_off,
            max_offset: max_off,
        }
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
            Some(v) => self.find_by_range(v..v + 1),
            None => self.find_by_range(0i64..0i64),
        }
    }

    pub fn find_first(&self, target: T) -> Option<usize> {
        self.find_by_value(target).next()
    }

    /// Iterator over indices whose realized value lies in the half-open
    /// range `[range.start, range.end)` — matching Rust's `Range<T>`
    /// and v0's `find_by_range` semantics.  Generic over any `X` that
    /// can be converted to `i64` so callers can pass `u32`, `i64`, or the
    /// delta's native `T` interchangeably.
    pub fn find_by_range<X>(&self, range: Range<X>) -> FindByRange<'_, T, C>
    where
        X: TryInto<i64>,
    {
        match (range.start.try_into(), range.end.try_into()) {
            (Ok(lo), Ok(hi)) => FindByRange::new(lo, hi, self),
            _ => FindByRange::default(),
        }
    }
}

/// Iterator returned by [`DeltaColumn::find_by_range`] and
/// [`DeltaColumn::find_by_value`].  Walks the B-tree's pruned slab
/// iterator and, for each candidate slab, runs a slab scan over its
/// RLE-encoded data.
pub struct FindByRange<'a, T: DeltaValue, C: Codec = Leb128> {
    lo: i64,
    hi: i64,
    slabs: &'a [Slab<TailOf<T::Inner, C>>],
    outer: Option<FindByValueRange<'a>>,
    current: Option<SlabScan<'a, C>>,
}

impl<T: DeltaValue, C: Codec> Default for FindByRange<'_, T, C> {
    fn default() -> Self {
        Self {
            lo: 0,
            hi: 0,
            slabs: &[],
            outer: None,
            current: None,
        }
    }
}

impl<'a, T: DeltaValue, C: Codec> FindByRange<'a, T, C> {
    /// Build a `FindByRange` over `col` for the half-open range `[lo, hi)`.
    /// If `hi <= lo` the returned iterator is empty.
    pub fn new(lo: i64, hi: i64, col: &'a DeltaColumn<T, C>) -> Self {
        if hi > lo {
            Self {
                lo,
                hi,
                slabs: &col.col.slabs,
                outer: Some(col.col.find_by_value_range(lo, hi - 1)),
                current: None,
            }
        } else {
            Self::default()
        }
    }
}

impl<T: DeltaValue, C: Codec> Iterator for FindByRange<'_, T, C> {
    type Item = usize;
    fn next(&mut self) -> Option<usize> {
        loop {
            if let Some(scan) = self.current.as_mut() {
                if let Some(i) = scan.next() {
                    return Some(i);
                }
                self.current = None;
            }
            let (slab_idx, items_before, prefix_before) = self.outer.as_mut()?.next()?;
            self.current = Some(SlabScan::new(
                &self.slabs[slab_idx].data,
                self.lo,
                self.hi,
                items_before,
                prefix_before,
            ));
        }
    }
}

/// Scan of a single slab's RLE-encoded data for items whose realized
/// value lies in `[lo, hi)`.  Uses the encoding's run decoder + O(1)
/// arithmetic per repeat run (no per-item iteration).
struct SlabScan<'a, C: Codec> {
    decoder: OptI64Decoder<'a, C>,
    lo: i64,
    hi: i64,
    items_before: usize,
    prefix_before: i64,
    partial: i64,
    item_idx: usize,
    /// Pending index range produced by the current matching run.  Drained
    /// first on each `next` call before advancing to the next run.
    pending: Range<usize>,
}

impl<'a, C: Codec> SlabScan<'a, C> {
    fn new(data: &'a [u8], lo: i64, hi: i64, items_before: usize, prefix_before: i64) -> Self {
        Self {
            decoder: OptI64Encoding::<C>::decoder(data),
            lo,
            hi,
            items_before,
            prefix_before,
            partial: 0,
            item_idx: 0,
            pending: 0..0,
        }
    }
}

impl<C: Codec> Iterator for SlabScan<'_, C> {
    type Item = usize;
    fn next(&mut self) -> Option<usize> {
        loop {
            if let Some(i) = self.pending.next() {
                return Some(i);
            }
            let run = self.decoder.next_run()?;
            let count = run.count;
            if let Some(v) = run.value {
                let a = self.prefix_before + self.partial; // f(k) = a + v*k for k in [1, count]
                if v == 0 {
                    if a >= self.lo && a < self.hi {
                        let start = self.items_before + self.item_idx;
                        self.pending = start..start + count;
                    }
                } else {
                    // Half-open constraint: lo ≤ a + v·k < hi.  Translate
                    // to bounds on k (handling v sign).  i64 throughout —
                    // realized values fit by construction.
                    let lo_diff = self.lo - a;
                    let hi_diff = self.hi - a;
                    let (k_min, k_max) = if v > 0 {
                        (div_ceil_i64(lo_diff, v), div_ceil_i64(hi_diff, v) - 1)
                    } else {
                        (div_floor_i64(hi_diff, v) + 1, div_floor_i64(lo_diff, v))
                    };
                    let k_start = k_min.max(1);
                    let k_end = k_max.min(count as i64);
                    if k_start <= k_end {
                        let start = self.items_before + self.item_idx + (k_start as usize - 1);
                        let len = (k_end - k_start + 1) as usize;
                        self.pending = start..start + len;
                    }
                }
                self.partial += v * count as i64;
            }
            self.item_idx += count;
        }
    }
}

/// Integer floor division for `i64` that handles mixed signs correctly
/// (unlike Rust's `/` which truncates toward zero).  Rust's `i64::div_floor`
/// and `i64::div_ceil` are both still nightly-only under `int_roundings`
/// (tracking issue #88581), so we keep our own here.
#[inline]
fn div_floor_i64(a: i64, b: i64) -> i64 {
    let q = a / b;
    let r = a % b;
    if r != 0 && (r ^ b) < 0 {
        q - 1
    } else {
        q
    }
}

#[inline]
fn div_ceil_i64(a: i64, b: i64) -> i64 {
    let q = a / b;
    let r = a % b;
    if r != 0 && (r ^ b) >= 0 {
        q + 1
    } else {
        q
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
