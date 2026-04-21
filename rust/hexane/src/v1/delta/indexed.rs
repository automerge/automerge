//! [`IndexedDeltaWeightFn`]: the default `WeightFn` for [`DeltaColumn`].
//! Tracks `SlabAgg` (`len + total + min_offset + max_offset`) per slab
//! so `find_by_value` / `find_by_range` can prune via min/max in O(log n).
//!
//! All delta-semantics — insert / remove / splice / null handling /
//! iter — live in [`DeltaColumn`].  This module adds just:
//!   * [`IndexedDeltaWeightFn`] — the `WeightFn` impl that walks the
//!     RLE decoder and produces `SlabAgg`.
//!   * A conditional `impl` block adding `find_by_value` /
//!     `find_by_range` / `find_first` when `WF = IndexedDeltaWeightFn`.

use std::marker::PhantomData;
use std::ops::Range;

use super::super::btree::SlabAgg;
use super::super::column::{Slab, TailOf, WeightFn};
use super::super::encoding::{ColumnEncoding, RunDecoder};
use super::super::{ColumnValueRef, RleValue};
use super::{DeltaColumn, DeltaValue};

// ── Weight function ─────────────────────────────────────────────────────────

/// Per-slab weight: `SlabAgg` with `min_offset`/`max_offset` for
/// value-range pruning.
pub struct IndexedDeltaWeightFn<T>(PhantomData<fn() -> T>);

impl<T> Clone for IndexedDeltaWeightFn<T> {
    fn clone(&self) -> Self {
        Self(PhantomData)
    }
}

impl<T: DeltaValue> WeightFn<T::Inner> for IndexedDeltaWeightFn<T>
where
    T::Inner: RleValue,
{
    type Weight = SlabAgg;

    fn compute(slab: &Slab<TailOf<T::Inner>>) -> SlabAgg {
        compute_slab_agg::<T>(&slab.data)
    }
}

/// Compute a `SlabAgg` by walking the encoding's run decoder.
///
/// For a run of `count` items with delta `v`, the realized-prefix
/// progression is monotonic: `partial + v`, `partial + 2v`, …,
/// `partial + count·v`.  Min/max are at the endpoints — no need to
/// visit each intermediate item.
fn compute_slab_agg<T: DeltaValue>(data: &[u8]) -> SlabAgg
where
    T::Inner: RleValue,
{
    let mut decoder = <T::Inner as ColumnValueRef>::Encoding::decoder(data);
    let mut partial = 0i64;
    let mut min_off = i64::MAX;
    let mut max_off = i64::MIN;
    let mut len = 0usize;

    while let Some(run) = decoder.next_run() {
        len += run.count;
        match T::get_inner(run.value) {
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

// ── Value-query methods (only when WF = IndexedDeltaWeightFn) ──────────────

impl<T: DeltaValue> DeltaColumn<T, IndexedDeltaWeightFn<T>>
where
    T::Inner: RleValue,
{
    /// Iterator over all indices whose realized value equals `target`.
    ///
    /// Thin wrapper over [`find_by_range`](Self::find_by_range) — the
    /// half-open range `[t, t+1)` is semantically "values equal to t".
    pub fn find_by_value(&self, target: T) -> Box<dyn Iterator<Item = usize> + '_> {
        let Some(lo) = target.to_i64() else {
            return Box::new(std::iter::empty());
        };
        self.find_by_range_i64(lo..lo.saturating_add(1))
    }

    /// Iterator over indices whose realized value lies in the half-open
    /// range `[range.start, range.end)` — matching Rust's `Range<T>`
    /// and v0's `find_by_range` semantics.
    pub fn find_by_range(&self, range: Range<T>) -> Box<dyn Iterator<Item = usize> + '_> {
        let (Some(lo), Some(hi)) = (range.start.to_i64(), range.end.to_i64()) else {
            return Box::new(std::iter::empty());
        };
        self.find_by_range_i64(lo..hi)
    }

    pub fn find_first(&self, target: T) -> Option<usize> {
        self.find_by_value(target).next()
    }

    fn find_by_range_i64(&self, range: Range<i64>) -> Box<dyn Iterator<Item = usize> + '_> {
        let Range { start: lo, end: hi } = range;
        if hi <= lo {
            return Box::new(std::iter::empty());
        }
        let hi_incl = hi - 1;
        Box::new(self.col.find_by_value_range(lo, hi_incl).flat_map(
            move |(slab_idx, items_before, prefix_before)| {
                let mut hits = Vec::new();
                scan_slab_range::<T>(
                    &self.col.slabs[slab_idx].data,
                    lo,
                    hi,
                    items_before,
                    prefix_before,
                    &mut hits,
                );
                hits.into_iter()
            },
        ))
    }
}

/// Scan a slab's RLE-encoded data for items whose realized value lies
/// in `[lo, hi)`.  Uses the encoding's run decoder + O(1) arithmetic
/// per repeat run (no per-item iteration).
fn scan_slab_range<T: DeltaValue>(
    data: &[u8],
    lo: i64,
    hi: i64,
    items_before: usize,
    prefix_before: i64,
    results: &mut Vec<usize>,
) where
    T::Inner: RleValue,
{
    let mut decoder = <T::Inner as ColumnValueRef>::Encoding::decoder(data);
    let mut partial = 0i64;
    let mut item_idx = 0usize;

    while let Some(run) = decoder.next_run() {
        let count = run.count;
        match T::get_inner(run.value) {
            None => {} // null run contributes nothing to realized values
            Some(v) => {
                let a = prefix_before + partial; // f(k) = a + v*k for k in [1, count]
                if v == 0 {
                    if a >= lo && a < hi {
                        for k in 0..count {
                            results.push(items_before + item_idx + k);
                        }
                    }
                } else {
                    // Half-open constraint on realized value: lo ≤ a + v·k < hi.
                    // Translate to bounds on k (handling v sign).  i64 throughout —
                    // realized values fit by construction.
                    let lo_diff = lo - a;
                    let hi_diff = hi - a;
                    let (k_min, k_max) = if v > 0 {
                        (div_ceil_i64(lo_diff, v), div_ceil_i64(hi_diff, v) - 1)
                    } else {
                        (div_floor_i64(hi_diff, v) + 1, div_floor_i64(lo_diff, v))
                    };
                    let k_start = k_min.max(1);
                    let k_end = k_max.min(count as i64);
                    if k_start <= k_end {
                        let start = items_before + item_idx + (k_start as usize - 1);
                        let len = (k_end - k_start + 1) as usize;
                        for off in 0..len {
                            results.push(start + off);
                        }
                    }
                }
                partial += v * count as i64;
            }
        }
        item_idx += count;
    }
}

/// Integer floor division for `i64` that handles mixed signs correctly
/// (unlike Rust's `/` which truncates toward zero).
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

/// Integer ceiling division for `i64` that handles mixed signs correctly.
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
    use super::super::super::prefix::PrefixWeightFn;
    use super::*;

    type PrefixDeltaColumn<T> = DeltaColumn<T, PrefixWeightFn<<T as DeltaValue>::Inner>>;

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
        // Half-open: realized in [200, 400) → indices 20..=39.
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
        // Reversed range → no hits.  Construct piecewise so clippy doesn't
        // lint the literal as an empty-range bug.
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
    fn mutations_match_delta_column() {
        let init: Vec<u64> = (1..=20).collect();
        let mut a = PrefixDeltaColumn::<u64>::from_values(init.clone());
        let mut b = DeltaColumn::<u64>::from_values(init);
        a.insert(5, 999);
        b.insert(5, 999);
        a.remove(10);
        b.remove(10);
        a.splice(3, 2, [100u64, 200]);
        b.splice(3, 2, [100u64, 200]);
        assert_eq!(a.save(), b.save());
        for i in 0..a.len() {
            assert_eq!(a.get(i), b.get(i));
        }
    }

    fn parity_u64(values: Vec<u64>) {
        let a = PrefixDeltaColumn::<u64>::from_values(values.clone());
        let b = DeltaColumn::<u64>::from_values(values.clone());
        assert_eq!(a.save(), b.save(), "save bytes mismatch");
        assert_eq!(a.len(), b.len());
        for i in 0..values.len() {
            assert_eq!(a.get(i), b.get(i), "get({i})");
        }
        for &v in values.iter().take(20) {
            let got: Vec<usize> = b.find_by_value(v).collect();
            let want = reference_find(&values, v);
            assert_eq!(got, want, "find_by_value({v})");
        }
    }

    #[test]
    fn parity_sequential() {
        parity_u64((1..=500).collect());
    }

    #[test]
    fn parity_scattered() {
        let mut seed = 1u64;
        let vals: Vec<u64> = (0..500)
            .map(|_| {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                seed % 10_000
            })
            .collect();
        parity_u64(vals);
    }

    #[test]
    fn fuzz_splices() {
        struct Rng(u64);
        impl Rng {
            fn new(s: u64) -> Self {
                Self(s.max(1))
            }
            fn next(&mut self) -> u64 {
                self.0 ^= self.0 << 13;
                self.0 ^= self.0 >> 7;
                self.0 ^= self.0 << 17;
                self.0
            }
        }
        let mut rng = Rng::new(0xBEEF);
        let init: Vec<u64> = (0..100).map(|i| i * 2 + 1).collect();
        let mut a = PrefixDeltaColumn::<u64>::from_values(init.clone());
        let mut b = DeltaColumn::<u64>::from_values(init);

        for _ in 0..500 {
            let op = rng.next() % 3;
            let len = a.len();
            match op {
                0 => {
                    let at = (rng.next() as usize) % (len + 1);
                    let v = rng.next() % 10_000;
                    a.insert(at, v);
                    b.insert(at, v);
                }
                1 if len > 0 => {
                    let at = (rng.next() as usize) % len;
                    a.remove(at);
                    b.remove(at);
                }
                _ if len > 0 => {
                    let at = (rng.next() as usize) % len;
                    let del = (rng.next() as usize) % (len - at).min(3) + 1;
                    let count = (rng.next() as usize) % 3 + 1;
                    let vals: Vec<u64> = (0..count).map(|_| rng.next() % 10_000).collect();
                    a.splice(at, del, vals.clone());
                    b.splice(at, del, vals);
                }
                _ => {}
            }
            assert_eq!(a.len(), b.len());
        }
        assert_eq!(a.save(), b.save());
        for i in 0..a.len() {
            assert_eq!(a.get(i), b.get(i));
        }
    }
}
