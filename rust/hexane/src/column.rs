use crate::attrib::{Accumulate, Attribution};
use crate::btree::SlabAggregate;
use crate::column_core::ColumnCore;
use crate::cursor::WeightOf;
use crate::sealed::Sealed;
use crate::splices::{Cuts, Gaps, Normalize};
use std::marker::PhantomData;
use std::ops::{AddAssign, Range, SubAssign};
#[allow(unused_imports)]
use vstd::prelude::*;

use crate::btree::{FindByValue, FindByValueRange, SlabAgg, SlabBTree};
use crate::edit::Edit;
use crate::encoding::{ColumnEncoding, RunDecoder};
use crate::index::ColumnIndex;
use crate::iter_core::{Cmp, ICore};
use crate::PackError;
use crate::{AsColumnRef, Codec, ColumnValueRef, Leb128, LoadOpts, MaybeFill, Run};

/// Type alias for the slab tail metadata of a column value type.
pub type TailOf<T, C = Leb128> = <<T as ColumnValueRef>::Encoding<C> as ColumnEncoding>::Tail;

/// Default maximum number of RLE/bool segments per slab.
///
/// Slabs are loaded at half capacity (`max / 2`) to leave room for inserts
/// without overflowing. A splice that exceeds `max` triggers an overflow split.
pub const DEFAULT_MAX_SEG: usize = 64;

// ── Slab ─────────────────────────────────────────────────────────────────────

verus! {

#[doc(hidden)]
#[derive(Clone, Debug, Default, PartialEq)]
#[verifier::allow(autoderive_clone_without_spec)]
pub struct Slab<Tail: Copy + Clone + std::fmt::Debug + Default = ()> {
    pub(crate) data: Vec<u8>,
    pub(crate) len: usize,
    pub(crate) segments: usize,
    pub(crate) tail: Tail,
}

} // verus!

impl<Tail: Copy + Clone + std::fmt::Debug + Default> Slab<Tail> {
    pub(crate) fn new(data: Vec<u8>, len: usize, segments: usize) -> Self {
        Self {
            data,
            len,
            segments,
            tail: Tail::default(),
        }
    }
}

// ── SlabWeight ───────────────────────────────────────────────────────────────

verus! {

/// A value stored in a Fenwick tree (BIT) node.
///
/// For plain `Column`, this is `usize` (just the slab item count).
/// For columns that track prefix sums, this is a compound value carrying
/// both the item count and the slab's contribution to the prefix sum.
///
/// The Fenwick tree ([`crate::fenwick`]) is verified against the item
/// counts alone, so these methods are specified only by what they do to
/// [`weight_len`](Self::weight_len).
#[doc(hidden)]
pub trait SlabWeight: Sealed + Clone + Default + std::fmt::Debug + AddAssign + SubAssign {
    /// [`len`](Self::len), for specifications.
    spec fn weight_len(&self) -> nat;

    /// The length (item count) component of this weight.
    fn len(&self) -> (n: usize)
        ensures n == self.weight_len();

    /// Whether this weight represents zero items.
    fn is_empty(&self) -> (b: bool)
        ensures b == (self.weight_len() == 0),
    {
        self.len() == 0
    }

    /// The weight of no items ([`Default`]).
    fn zero() -> (z: Self)
        ensures z.weight_len() == 0;

    /// A copy of this weight ([`Clone`]).
    fn copy(&self) -> (c: Self)
        ensures c.weight_len() == self.weight_len();

    /// `*self += other`.
    fn add_weight(&mut self, other: &Self)
        requires old(self).weight_len() + other.weight_len() <= usize::MAX,
        ensures final(self).weight_len() == old(self).weight_len() + other.weight_len();

    /// `*self -= other`.
    fn sub_weight(&mut self, other: &Self)
        requires other.weight_len() <= old(self).weight_len(),
        ensures final(self).weight_len() == old(self).weight_len() - other.weight_len();
}

impl Sealed for usize {}

impl SlabWeight for usize {
    open spec fn weight_len(&self) -> nat {
        *self as nat
    }

    #[inline]
    fn len(&self) -> usize {
        *self
    }

    #[inline]
    fn zero() -> usize {
        0
    }

    #[inline]
    fn copy(&self) -> usize {
        *self
    }

    #[inline]
    fn add_weight(&mut self, other: &usize) {
        *self += *other;
    }

    #[inline]
    fn sub_weight(&mut self, other: &usize) {
        *self -= *other;
    }
}

} // verus!

// ── WeightFn ─────────────────────────────────────────────────────────────────

/// Strategy for computing a [`SlabWeight`] from a slab's data.
///
/// This is a zero-sized type parameter on [`Column`] that controls what
/// the Fenwick tree stores.  The default [`LenWeight`] records only item
/// counts.  Prefix-aware columns use a compound weight that also tracks
/// prefix sums in the same BIT.
#[doc(hidden)]
pub trait WeightFn<T: ColumnValueRef, C: Codec = Leb128>: Sealed {
    /// The per-slab weight type.
    ///
    /// For the Fenwick-BIT-backed [`Column`] and [`BitIndex`](crate::index::BitIndex),
    /// this must additionally implement [`SlabWeight`] (invertible aggregation
    /// via `AddAssign`/`SubAssign`).  For the B-tree-backed path it only needs
    /// [`SlabAggregate`](SlabAggregate), which permits
    /// non-invertible aggregates like min/max.
    type Weight: Clone + Default + std::fmt::Debug + SlabAggregate;
    fn compute(slab: &Slab<TailOf<T, C>>) -> Self::Weight;

    /// `true` when `compute` has to re-decode the slab bytes (prefix sums,
    /// delta aggregates).  The streaming loader then builds each slab's
    /// weight incrementally via [`accumulate_run`](Self::accumulate_run)
    /// as runs stream past, instead of a second decode pass at the end.
    const ACCUMULATES: bool = false;

    /// Fold a run of `count` copies of `value` into an in-progress slab
    /// weight (starting from `Default`).  Must agree exactly with
    /// [`compute`](Self::compute); runs may arrive split into pieces.
    ///
    /// Only called during load, so it is fallible: weight functions whose
    /// arithmetic can overflow on hostile bytes (delta aggregates) return
    /// an error instead of wrapping or panicking.
    fn accumulate_run(
        weight: &mut Self::Weight,
        count: usize,
        value: T::Get<'_>,
    ) -> Result<(), PackError>;

    /// `true` when the weights of a column's slabs can overflow together
    /// where none of them does on its own (prefix sums), so that every
    /// load checks [`check_total`](Self::check_total), and every edit
    /// [`check_splice`](Self::check_splice), before the weights reach the
    /// index.
    const CHECKS_TOTAL: bool = false;

    /// Check that one column of `len` items can hold slabs of weights
    /// `ws`. The default accepts any.
    #[inline(always)]
    fn check_total(_ws: &[Self::Weight], _len: usize) -> Result<(), PackError> {
        Ok(())
    }

    /// Check that `index` can take `new` in place of the weights of slabs
    /// `old`, leaving a column of `len` items; panic if not. The default
    /// accepts any.
    #[inline(always)]
    fn check_splice<Idx: crate::index::IndexOps<Self::Weight>>(
        _index: &Idx,
        _old: Range<usize>,
        _new: &[Self::Weight],
        _len: usize,
    ) {
    }
}

/// Default weight strategy: BIT stores only slab lengths.
#[doc(hidden)]
#[derive(Clone)]
pub struct LenWeight;

impl Sealed for LenWeight {}

impl<T: ColumnValueRef, C: Codec> WeightFn<T, C> for LenWeight {
    type Weight = usize;
    #[inline]
    fn compute(slab: &Slab<TailOf<T, C>>) -> usize {
        slab.len
    }

    // `compute` is O(1) here, so the loader keeps its fast drain path
    // (`ACCUMULATES = false`); this impl exists for completeness.
    #[inline]
    fn accumulate_run(
        weight: &mut usize,
        count: usize,
        _value: T::Get<'_>,
    ) -> Result<(), PackError> {
        *weight += count;
        Ok(())
    }
}

// ── Splice helpers ───────────────────────────────────────────────────────────

/// Normalize any `RangeBounds` into a concrete `(start, end)` pair, with
/// `max` standing in for an unbounded end.  Moved verbatim from the v0
/// `columndata` module — callers (`iter_range`, `set_max`, `advance_to`)
/// do their own clamping.
pub(crate) fn normalize_range_max<R: std::ops::RangeBounds<usize>>(
    range: R,
    max: usize,
) -> (usize, usize) {
    use std::ops::Bound;
    let start = match range.start_bound() {
        Bound::Unbounded => usize::MIN,
        Bound::Included(n) => *n,
        // past `usize::MAX` there are no items: saturating is exact
        Bound::Excluded(n) => n.saturating_add(1),
    };

    let end = match range.end_bound() {
        Bound::Unbounded => max,
        Bound::Included(n) => n.saturating_add(1),
        Bound::Excluded(n) => *n,
    };
    if start > end {
        (start, start)
    } else {
        (start, end)
    }
}

/// Byte-level `Vec::splice` without the per-element iterator dance.
///
/// `Vec::splice(range, iter)` in stdlib walks the iterator and does a
/// `ptr::write` per element in its drop handler — no memcpy fast path for
/// `Copy` types with a known size.  This replacement uses three safe
/// stdlib calls that all lower to SIMD memcpy / memmove:
///
///   * [`Vec::resize`] (memset-for-`u8`) to grow,
///   * [`<[u8]>::copy_within`] to shift the suffix,
///   * [`<[u8]>::copy_from_slice`] to drop the new bytes in.
///
/// Compared to `Vec::splice` the inserted bytes go in as one memcpy instead
/// of a byte-at-a-time `ptr::write` loop.  Compared to a hand-rolled
/// `unsafe` version this leaves one wasted pass over the grown tail
/// (the memset-to-zero before we overwrite it), which is negligible:
/// `bytes.len()` is typically tiny next to the suffix shift.
///
/// Used by the RLE, bool, and raw splice paths.
#[inline]
pub(crate) fn splice_bytes(vec: &mut Vec<u8>, index: usize, del: usize, bytes: &[u8]) {
    let old_len = vec.len();
    debug_assert!(index + del <= old_len);
    let insert = bytes.len();
    let new_len = old_len - del + insert;

    use std::cmp::Ordering;
    match insert.cmp(&del) {
        Ordering::Greater => {
            vec.resize(new_len, 0);
            vec.copy_within(index + del..old_len, index + insert);
        }
        Ordering::Less => {
            vec.copy_within(index + del..old_len, index + insert);
            vec.truncate(new_len);
        }
        Ordering::Equal => {}
    }
    vec[index..index + insert].copy_from_slice(bytes);
}

// ── Iter ─────────────────────────────────────────────────────────────────────

/// Object-safe view of a `Column` used by [`Iter`] for index-assisted
/// slab lookups, independent of the column's `WF` / `Idx` parameters.
pub(crate) trait ColumnRef<T: ColumnValueRef> {
    fn len(&self) -> usize;
    fn find_slab(&self, index: usize) -> (usize, usize);
    fn slab_start(&self, index: usize) -> usize;
}

impl<T, C, WF, Idx> ColumnRef<T> for Column<T, C, WF, Idx>
where
    T: ColumnValueRef,
    C: Codec,
    WF: WeightFn<T, C>,
    Idx: ColumnIndex<WF::Weight>,
{
    fn len(&self) -> usize {
        self.core.len()
    }

    fn find_slab(&self, index: usize) -> (usize, usize) {
        // verified (`IndexOps::ix_find`); the iterator's seeks rely on it
        self.core.index().ix_find(index)
    }

    fn slab_start(&self, index: usize) -> usize {
        self.slab_start(index)
    }
}

/// Forward iterator over column items.
///
/// Created by [`Column::iter`] or [`Column::iter_range`].
///
/// `nth()` is O(log S + runs_skipped) — uses the column's index structure
/// to skip directly to the target slab.
pub struct Iter<'a, T: ColumnValueRef, C: Codec = Leb128> {
    pub(crate) slabs: &'a [Slab<TailOf<T, C>>],
    pub(crate) col: Option<&'a dyn ColumnRef<T>>,
    /// The position and decoder, verified ([`crate::iter_core`]).
    pub(crate) core: ICore<<T::Encoding<C> as ColumnEncoding>::Decoder<'a>>,
    pub(crate) counter: usize,
}

/// The decoders an iterator over a `Column<T, C>` opens.
pub(crate) type Dx<T, C> = <<T as ColumnValueRef>::Encoding<C> as ColumnEncoding>::ReadModel;

impl<T: ColumnValueRef, C: Codec> Default for Iter<'_, T, C> {
    fn default() -> Self {
        Self {
            slabs: &[],
            col: None,
            core: ICore {
                slab_idx: 0,
                decoder: T::Encoding::<C>::slab_decoder(&[], crate::sealed::Trusted(())),
                items_left: 0,
                slab_remaining: 0,
                pos: 0,
            },
            counter: 0,
        }
    }
}

impl<'a, T: ColumnValueRef, C: Codec> Iterator for Iter<'a, T, C> {
    type Item = T::Get<'a>;

    #[inline(always)]
    fn next(&mut self) -> Option<T::Get<'a>> {
        self.core.next::<Dx<T, C>>(self.slabs)
    }

    /// O(log S + runs_skipped) — uses the column's index for slab lookup.
    fn nth(&mut self, n: usize) -> Option<T::Get<'a>> {
        let col = self.col;
        self.core.nth::<Dx<T, C>, _, _>(
            self.slabs,
            n,
            &|t: usize| match col {
                Some(col) => col.find_slab(t),
                // a default iterator: no column, no slabs
                None => (0, t),
            },
            &|i: usize| col.map_or(0, |col| col.slab_start(i)),
        )
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        // the window (`set_max`) can run past the column's end
        let left = match self.col {
            Some(col) => self
                .core
                .items_left
                .min(col.len().saturating_sub(self.core.pos)),
            None => 0,
        };
        (left, Some(left))
    }

    fn fold<B, F>(mut self, init: B, mut f: F) -> B
    where
        F: FnMut(B, Self::Item) -> B,
    {
        let mut acc = init;
        while self.core.items_left > 0 {
            let max = self.core.items_left;
            match self.core.run_once::<Dx<T, C>>(self.slabs, max) {
                Some(run) => {
                    for _ in 0..run.count {
                        acc = f(acc, run.value);
                    }
                }
                None => break,
            }
        }
        acc
    }
}

impl<T: ColumnValueRef, C: Codec> ExactSizeIterator for Iter<'_, T, C> {}

impl<'a, T: ColumnValueRef, C: Codec> crate::encoding::RunSrc<'a, T> for Iter<'a, T, C> {
    fn try_next_run(&mut self) -> Result<Option<Run<T::Get<'a>>>, PackError> {
        Ok(self.next_run())
    }
}

impl<T: ColumnValueRef, C: Codec> std::fmt::Debug for Iter<'_, T, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Iter")
            .field("pos", &self.core.pos)
            .field("items_left", &self.core.items_left)
            .finish()
    }
}

impl<T: ColumnValueRef, C: Codec> Clone for Iter<'_, T, C> {
    fn clone(&self) -> Self {
        Self {
            slabs: self.slabs,
            col: self.col,
            core: ICore {
                slab_idx: self.core.slab_idx,
                decoder: self.core.decoder.clone(),
                items_left: self.core.items_left,
                slab_remaining: self.core.slab_remaining,
                pos: self.core.pos,
            },
            counter: self.counter,
        }
    }
}

impl<'a, T: ColumnValueRef, C: Codec> Iter<'a, T, C> {
    /// Set the iterator to yield items up to (but not past) `pos`.
    ///
    /// Can both shorten and extend the iterator window. If `pos` is
    /// beyond the column length, the iterator will cleanly return `None`
    /// when it reaches the end of the data.
    pub fn set_max(&mut self, pos: usize) {
        self.core
            .set_max::<Dx<T, C>>(vstd::prelude::Ghost::assume_new(), pos);
    }

    /// Returns the index of the current slab.
    #[inline]
    pub fn get_slab(&self) -> usize {
        self.core.slab_idx
    }

    /// Returns the index of the next item to be yielded.
    #[inline]
    pub fn pos(&self) -> usize {
        self.core.pos
    }

    #[inline]
    pub fn end_pos(&self) -> usize {
        self.core.pos + self.core.items_left
    }

    pub fn advance_to(&mut self, target: usize) {
        if target > self.core.pos {
            self.nth(target - self.core.pos - 1);
        }
    }

    /// Reposition the iterator window to `range`.
    ///
    /// After this call the iterator yields the items in `range` and then
    /// returns `None`. Equivalent to `set_max(range.end)` followed by
    /// `advance_to(range.start)`. It moves only forwards: if `range`
    /// starts before [`pos`](Self::pos), it yields the items from `pos()`
    /// to `range.end`.
    ///
    /// ```
    /// let col = hexane::Column::<u64>::from_values(vec![10, 20, 30, 40, 50]);
    /// let mut it = col.iter();
    /// it.shift(1..3);
    /// assert_eq!(it.collect::<Vec<_>>(), vec![20, 30]);
    /// let mut it = col.iter();
    /// it.nth(2);
    /// it.shift(1..4); // already at 3
    /// assert_eq!(it.collect::<Vec<_>>(), vec![40]);
    /// ```
    pub fn shift(&mut self, range: Range<usize>) {
        self.set_max(range.end);
        self.advance_to(range.start);
    }

    pub fn advance_by(&mut self, amount: usize) {
        self.advance_to(self.core.pos.saturating_add(amount))
    }

    /// Returns the next run of identical values, merging across slab boundaries.
    ///
    /// For repeat runs, returns the full count. For literal runs, returns
    /// count=1 per value. Null runs return the null value with the full count.
    pub fn next_run(&mut self) -> Option<crate::Run<T::Get<'a>>> {
        self.next_run_max(self.core.items_left)
    }

    /// Adapt into an iterator of [`Run`]s — the item shape
    /// [`Column::splice_runs`] accepts.
    pub fn runs(self) -> Runs<'a, T, C> {
        Runs(self)
    }

    pub(crate) fn next_run_max(&mut self, max: usize) -> Option<crate::Run<T::Get<'a>>> {
        self.core.next_run_max::<Dx<T, C>>(self.slabs, max)
    }

    /// Moves the iterator window to `range` and returns the item at `range.start`.
    ///
    /// # Panics
    ///
    /// Panics if `range.start < self.pos()`.
    pub fn shift_next(&mut self, range: Range<usize>) -> Option<T::Get<'a>> {
        assert!(range.start >= self.core.pos);
        self.set_max(range.end);
        self.nth(range.start - self.core.pos)
    }
}

// ── copy_ranges support ─────────────────────────────────────────────────────

/// Execution strategy for `copy_ranges`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CopyStrategy {
    Direct,
    Invert,
}

/// Safety factor favouring Direct in [`Column::copy_strategy`].  The
/// seam term is already a gross upper bound, so 1 is not aggressive;
/// see `copy_ranges_bench`.
pub(crate) const COPY_INVERT_FACTOR: usize = 1;

/// Clamp `ranges` to `len`, drop empties, merge touching neighbours,
/// and assert ascending/non-overlapping.
#[cfg(test)]
pub(crate) fn normalize_ranges(
    ranges: impl IntoIterator<Item = Range<usize>>,
    len: usize,
) -> Vec<Range<usize>> {
    let mut out: Vec<Range<usize>> = Vec::new();
    let mut pos = 0usize;
    for r in ranges {
        assert!(
            r.start >= pos,
            "ranges must be ascending and non-overlapping"
        );
        pos = pos.max(r.end);
        let start = r.start.min(len);
        let end = r.end.min(len);
        if end > start {
            match out.last_mut() {
                Some(last) if last.end == start => last.end = end,
                _ => out.push(start..end),
            }
        }
    }
    out
}

/// One point of a multi-point [`Column::copy_ranges`]: at destination
/// position `pos` (pre-splice coordinates), delete `delete` rows and
/// insert the source rows `range`.
///
/// The default is *all of the source, inserted at the front, deleting
/// nothing* — source ranges are clamped to the source's length, so
/// `0..usize::MAX` is the whole column. Copying a whole column onto the
/// end of another is then `Splice { pos: dst.len(), ..Default::default() }`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Splice {
    pub pos: usize,
    pub delete: usize,
    pub range: Range<usize>,
}

impl Default for Splice {
    fn default() -> Self {
        Self {
            pos: 0,
            delete: 0,
            range: 0..usize::MAX,
        }
    }
}

/// Normalize a multi-point splice list: clamp src ranges to `src_len`,
/// drop no-ops, and merge contiguous neighbours (next edit starting
/// where the previous one ended, with touching src ranges). Asserts
/// destination regions are ascending/non-overlapping and in bounds,
/// and src ranges globally ascending/non-overlapping.
pub(crate) fn normalize_splices(
    splices: impl IntoIterator<Item = Splice>,
    src_len: usize,
    dst_len: usize,
) -> Vec<Splice> {
    // verified (`splices::Normalize`): the list is normalized, and applied
    // gives what the splices give one by one
    let mut n = Normalize::new(src_len, dst_len);
    for sp in splices {
        n.push(sp);
    }
    n.finish()
}

// ── Runs ────────────────────────────────────────────────────────────────────

/// Iterator adapter yielding [`Run`]s instead of individual items;
/// created by [`Iter::runs`].
#[derive(Debug)]
pub struct Runs<'a, T: ColumnValueRef, C: Codec = Leb128>(pub(crate) Iter<'a, T, C>);

impl<'a, T: ColumnValueRef, C: Codec> Iterator for Runs<'a, T, C> {
    type Item = Run<T::Get<'a>>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next_run()
    }
}

// ── IterState (suspend / resume) ────────────────────────────────────────────

/// Serializable snapshot of an [`Iter`] position.
///
/// Created by [`Iter::suspend`] and restored by [`IterState::try_resume`].
pub struct IterState {
    counter: usize,
    num_slabs: usize,
    slab_idx: usize,
    pos: usize,
    items_left: usize,
    /// How many items from the start of the current slab have been consumed.
    slab_consumed: usize,
}

impl IterState {
    /// Restores the iterator position in any column.
    ///
    /// Returns [`PackError::InvalidResume`] if `column` was mutated since
    /// [`Iter::suspend`], or if the position doesn't fit it (a state taken
    /// from another column).
    pub fn try_resume<'a, T, C, WF, Idx>(
        &self,
        column: &'a Column<T, C, WF, Idx>,
    ) -> Result<Iter<'a, T, C>, PackError>
    where
        T: ColumnValueRef,
        C: Codec,
        WF: WeightFn<T, C>,
        Idx: ColumnIndex<WF::Weight>,
    {
        if self.counter != column.core.counter() {
            return Err(PackError::InvalidResume);
        }
        if self.num_slabs != column.core.slabs().len() {
            return Err(PackError::InvalidResume);
        }
        let slabs = column.core.slabs();
        // verified (`ColumnCore::resume`): `Some` exactly when the state is a
        // position of this column's slabs, the iterator then standing there
        let core = column
            .core
            .resume::<Dx<T, C>>(self.slab_idx, self.slab_consumed, self.pos, self.items_left)
            .ok_or(PackError::InvalidResume)?;
        Ok(Iter {
            slabs,
            col: Some(column),
            core,
            counter: self.counter,
        })
    }
}

impl<'a, T: ColumnValueRef, C: Codec> Iter<'a, T, C> {
    /// Captures the current iterator position so it can be restored later.
    pub fn suspend(&self) -> IterState {
        // verified (`ICore::suspended`): a state that resumes, over the
        // same slabs, to an iterator with the same values left
        // (`ICore::lemma_resumed`)
        let (slab_idx, slab_consumed, pos, items_left) =
            self.core.suspended::<Dx<T, C>>(self.slabs);
        IterState {
            counter: self.counter,
            num_slabs: self.slabs.len(),
            slab_idx,
            pos,
            items_left,
            slab_consumed,
        }
    }
}

impl<'a, T: ColumnValueRef, C: Codec> Iter<'a, T, C> {
    /// Scan forward for the next occurrence of `target`.
    ///
    /// Unlike [`Self::seek_to_value`] the values need not be sorted —
    /// this is a find, walked run-at-a-time (for RLE data that is one
    /// step per run, not per item). On a hit, returns `Some(pos)` with
    /// the value consumed: iteration continues with the item after it —
    /// so repeated calls walk successive occurrences, including
    /// consecutive items inside one run, with no caller-side run state.
    /// On a miss, returns `None` with the iterator at the end of its
    /// window.
    pub fn scan_to_value<'t>(&mut self, target: T::Get<'t>) -> Option<usize> {
        let end = self.end_pos();
        let mut probe = self.clone();
        let mut pos = probe.pos();
        while pos < end {
            let Some(run) = probe.next_run() else {
                break;
            };
            if T::eq(run.value, T::shorten(&target)) {
                // consume self through the hit (one indexed jump)
                self.nth(pos - self.pos())?;
                return Some(pos);
            }
            pos += run.count;
        }
        self.advance_to(end);
        None
    }

    /// Find the items equal to `target` within `range` of the window,
    /// whose values are sorted, and move the iterator to the first of
    /// them: returns their range, or, if there are none, the empty range
    /// where `target` would go, with the iterator there. The window keeps
    /// its end.
    ///
    /// Assumes values within `range` are sorted.  If they aren't, the
    /// result is unspecified — a wrong or empty range may be returned —
    /// but never a panic, memory unsafety, or column corruption.
    ///
    /// ```
    /// let col = hexane::Column::<u64>::from_values(vec![1, 2, 4, 4, 5]);
    /// let mut it = col.iter();
    /// assert_eq!(it.seek_to_value(4, ..), 2..4);
    /// assert_eq!(it.next(), Some(4));
    /// let mut it = col.iter();
    /// assert_eq!(it.seek_to_value(3, ..), 2..2);
    /// assert_eq!((it.pos(), it.next()), (2, Some(4)));
    /// ```
    pub fn seek_to_value<'t>(
        &mut self,
        target: T::Get<'t>,
        range: impl std::ops::RangeBounds<usize>,
    ) -> Range<usize>
    where
        for<'x> T::Get<'x>: Ord + crate::AsColumnRef<T>,
    {
        let (start, end) = normalize_range_max(range, self.end_pos());
        // the iterator cannot leave its window, so neither can the search
        let end = end.min(self.end_pos());

        if start > self.core.pos {
            self.advance_to(start);
        }

        let mut checkpoint = self.clone();
        checkpoint.set_max(end);
        let range = checkpoint.sorted_scan_to_value(target);

        self.advance_to(range.start);

        range
    }

    fn sorted_scan_to_value<'t>(mut self, target: T::Get<'t>) -> Range<usize>
    where
        for<'x> T::Get<'x>: Ord,
    {
        let col = self.col;
        // the column's values and the caller's target have unrelated
        // borrow lifetimes; shorten both to compare
        let probe = Cmp(|v: &T::Get<'a>| T::shorten(v).cmp(&T::shorten(&target)));
        let (start, end) = self.core.seek_value::<Dx<T, C>, _, _, _>(
            self.slabs,
            &probe,
            &|t: usize| col.unwrap().find_slab(t),
            &|i: usize| col.unwrap().slab_start(i),
        );
        start..end
    }
}

// ── Column (B-tree indexed, pluggable WF + Idx) ─────────────────────────────
//
// Default `Idx` is `SlabBTree<WF::Weight>`.  Swap to `BitIndex<WF::Weight>`
// for a Fenwick-BIT backing.  `splice_inner` inlines the body of
// `ColumnEncoding::splice`'s default — that trait method is parameterised
// over the old BIT-backed Column (now deleted); this is the equivalent for
// the B-tree path, mutating via the `ColumnIndex` trait.

/// A typed column of RLE- or bool-encoded values with O(log S) random
/// access and in-place splice.
///
/// **Treat the public type as `Column<T>`.**  The `WF` (per-slab weight
/// strategy) and `Idx` (index backend) parameters are internal plumbing
/// with the right defaults; the traits behind them are sealed and may
/// change shape in any release.  `PrefixColumn` and `DeltaColumn` are the
/// supported ways to get non-default weights.
pub struct Column<
    T: ColumnValueRef,
    C: Codec = Leb128,
    WF: WeightFn<T, C> = LenWeight,
    Idx = SlabBTree<<WF as WeightFn<T, C>>::Weight>,
> where
    Idx: ColumnIndex<WF::Weight>,
{
    /// The slabs, the per-slab index over them (B-tree by default,
    /// Fenwick BIT optional) and the counts: always a column (verified,
    /// [`crate::column_core`]).
    pub(crate) core: CoreOf<T, C, WF, Idx>,
    #[allow(clippy::type_complexity)]
    _phantom: PhantomData<fn() -> (T, C, WF)>,
}

/// A `Column<T, C, WF, Idx>`'s state, as the verified core sees it.
pub(crate) type CoreOf<T, C, WF, Idx> = ColumnCore<
    <<T as ColumnValueRef>::Encoding<C> as crate::edit::SlabEdit>::Model,
    <WF as WeightFn<T, C>>::Weight,
    Idx,
    WeightOf<T, C, WF>,
>;

impl<T, C, WF, Idx> Clone for Column<T, C, WF, Idx>
where
    T: ColumnValueRef,
    C: Codec,
    WF: WeightFn<T, C>,
    Idx: ColumnIndex<WF::Weight> + Clone,
{
    fn clone(&self) -> Self {
        Self {
            core: self.core.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<T, C, WF, Idx> std::fmt::Debug for Column<T, C, WF, Idx>
where
    T: ColumnValueRef,
    C: Codec,
    WF: WeightFn<T, C>,
    Idx: ColumnIndex<WF::Weight>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Column")
            .field("len", &self.core.len())
            .field("slabs", &self.core.slabs().len())
            .finish()
    }
}

impl<T, C, WF, Idx> Default for Column<T, C, WF, Idx>
where
    T: ColumnValueRef,
    C: Codec,
    WF: WeightFn<T, C>,
    Idx: ColumnIndex<WF::Weight>,
{
    fn default() -> Self {
        Self::new()
    }
}

/// Call `f` for each gap between `splices` (normalized) in a destination
/// of `dst_len` rows, with the destination's range to read and the
/// position in the retained column it belongs at.
///
/// Positions are the copied ranges' running length: the rows a gap brings
/// back land before the next copied range and do not move it.
pub(crate) fn for_each_gap(
    splices: &[Splice],
    dst_len: usize,
    mut f: impl FnMut(usize, Range<usize>),
) {
    // verified (`splices::Gaps`): the gaps, as splices inserting them into
    // the ranges retained, are normalized and give what `splices` give
    let mut gaps = Gaps::<()>::new(splices, dst_len, Ghost::assume_new(), Ghost::assume_new());
    while let Some((at, gap)) = gaps.next(splices) {
        f(at, gap);
    }
}

/// Call `f(at, n)` for each run of rows to remove, back to front, from a
/// column of `len` rows to keep only those in `splices`' ranges
/// (normalized).
pub(crate) fn for_each_cut(splices: &[Splice], len: usize, mut f: impl FnMut(usize, usize)) {
    // verified (`splices::Cuts`): the cuts, made in turn, leave exactly the
    // ranges' rows
    let mut cuts = Cuts::<()>::new(splices, len, Ghost::assume_new());
    while let Some((at, n)) = cuts.next(splices) {
        f(at, n);
    }
}

/// Splices copying `ranges`, for `retain_ranges`.
#[cfg(test)]
pub(crate) fn ranges_as_splices(ranges: &[Range<usize>]) -> Vec<Splice> {
    ranges
        .iter()
        .map(|r| Splice {
            pos: 0,
            delete: 0,
            range: r.clone(),
        })
        .collect()
}

impl<T, C, WF, Idx> Column<T, C, WF, Idx>
where
    T: ColumnValueRef,
    C: Codec,
    WF: WeightFn<T, C>,
    Idx: ColumnIndex<WF::Weight>,
{
    pub fn new() -> Self {
        Self::with_max_segments(DEFAULT_MAX_SEG)
    }

    pub fn with_max_segments(max_segments: usize) -> Self {
        // `load` splits slabs at `max_segments / 2` and `splice` asserts
        // `slab.segments <= max_segments`, so budgets below 2 produce
        // columns that panic on their next mutation.  Reject up front
        // (same policy as `RawColumn::with_max_segments`).
        Self {
            // verified (`ColumnCore::new`): no slabs, and panics on a
            // budget below 2
            core: ColumnCore::new(max_segments),
            _phantom: PhantomData,
        }
    }

    pub fn from_values(values: Vec<T>) -> Self {
        Self::from_values_with_max_segments(values, DEFAULT_MAX_SEG)
    }

    pub fn from_values_with_max_segments(values: Vec<T>, max_segments: usize) -> Self {
        let mut col = Self::with_max_segments(max_segments);
        col.splice_inner(0, 0, values.into_iter().map(|v| (v, 1)));
        col
    }

    pub fn load(data: &[u8]) -> Result<Self, PackError> {
        Self::load_iter(data, LoadOpts::new()).finalize()
    }

    /// Deserialize with options.  Supports:
    ///   * `with_length(n)` — validate the column has exactly `n` items.
    ///   * `with_fill(v)` — when data is empty and `length` is set, read
    ///     as `length` copies of `v` instead of an empty column.
    ///   * `with_max_segments(n)` — override the slab segment budget.
    pub fn load_with<'a, F>(data: &'a [u8], opts: LoadOpts<F>) -> Result<Self, PackError>
    where
        F: MaybeFill<T::Get<'a>>,
    {
        Self::load_iter(data, opts).finalize()
    }

    /// Streaming load: decode + validate the saved bytes run by run while
    /// the column's slabs build underneath (the loader's byte-copy
    /// mechanics), then [`finalize`](ColumnLoadIter::finalize) into the
    /// column.
    ///
    /// Pulling runs is optional — `load_iter(data, opts).finalize()` is a
    /// plain load — but consumers that need the data anyway (like an
    /// index builder) can walk the canonical runs during the same decode
    /// pass. A fill option on empty data emits one run and writes one
    /// slab.
    pub fn load_iter<'a, F>(data: &'a [u8], opts: LoadOpts<F>) -> ColumnLoadIter<'a, T, C, WF, Idx>
    where
        F: MaybeFill<T::Get<'a>>,
    {
        assert!(opts.max_segments >= 2, "max_segments must be at least 2");
        let fill = match (data.is_empty(), opts.length) {
            // a fill is one run, which holds at most `i64::MAX` items
            // (`Column::fill`); past that, the empty data loads as it is
            // and the length check rejects it
            (true, Some(len)) if len <= i64::MAX as usize => {
                opts.fill.fill_value().map(|value| (value, len))
            }
            _ => None,
        };
        ColumnLoadIter {
            iter: T::Encoding::<C>::load_iter(data, opts.max_segments),
            fill,
            fill_emitted: false,
            length: opts.length,
            max_segments: opts.max_segments,
            attr: Attribution::new::<WeightOf<T, C, WF>>(),
            failed: false,
            _phantom: PhantomData,
        }
    }

    /// Build a column directly from pre-encoded slabs, skipping the
    /// decode/validate pass that `load` performs.
    ///
    /// Callers must guarantee each slab is well-formed with correct
    /// `len` / `segments` / `tail` metadata and `segments <= max_segments`
    /// (debug builds verify).  Used by the streaming encoders'
    /// `into_column` fast path.
    pub(crate) fn from_slabs(slabs: Vec<Slab<TailOf<T, C>>>, max_segments: usize) -> Self {
        #[cfg(debug_assertions)]
        for s in &slabs {
            let info = T::Encoding::<C>::validate_encoding(&s.data)
                .unwrap_or_else(|e| panic!("from_slabs: invalid slab encoding: {e}"));
            debug_assert_eq!(s.len, info.len, "from_slabs: slab len mismatch");
            debug_assert_eq!(
                s.segments, info.segments,
                "from_slabs: slab segments mismatch"
            );
            debug_assert!(s.segments <= max_segments);
        }
        let weights: Vec<WF::Weight> = slabs.iter().map(WF::compute).collect();
        if WF::CHECKS_TOTAL {
            let len = slabs.iter().fold(0usize, |n, s| n.saturating_add(s.len));
            if let Err(e) = WF::check_total(&weights, len) {
                panic!("from_slabs: {e}");
            }
        }
        Self::of_slabs(slabs, weights, max_segments)
    }

    /// The column of `slabs`, weighed by `weights` (verified,
    /// `ColumnCore::from_slabs`, which computes the index). Panics if a
    /// weight does not count its slab's items, which would be a bug in a
    /// weight function, or if the items are more than an edit can count.
    pub(crate) fn of_slabs(
        slabs: Vec<Slab<TailOf<T, C>>>,
        weights: Vec<WF::Weight>,
        max_segments: usize,
    ) -> Self {
        match ColumnCore::from_slabs(slabs, weights, max_segments) {
            Some(core) => Self {
                core,
                _phantom: PhantomData,
            },
            None => {
                panic!("column weights do not match its slabs, or it has over usize::MAX - 3 items")
            }
        }
    }

    /// Create a column of `len` copies of `value`.
    pub fn fill(len: usize, value: T::Get<'_>) -> Self {
        Self::fill_with_max_segments(len, value, DEFAULT_MAX_SEG)
    }

    /// [`fill`](Self::fill), with the segment budget given.
    pub(crate) fn fill_with_max_segments(
        len: usize,
        value: T::Get<'_>,
        max_segments: usize,
    ) -> Self {
        // (the second bound only matters where `usize` is narrower)
        assert!(
            len <= i64::MAX as usize && len < usize::MAX - 1,
            "fill length {len} is not encodable"
        );
        if len == 0 {
            return Self::with_max_segments(max_segments);
        }
        let slabs = vec![T::Encoding::<C>::fill(len, value)];
        let weights = vec![WF::compute(&slabs[0])];
        Self::of_slabs(slabs, weights, max_segments)
    }

    pub fn len(&self) -> usize {
        self.core.len()
    }

    pub fn is_empty(&self) -> bool {
        self.core.len() == 0
    }

    pub fn slab_count(&self) -> usize {
        self.core.slabs().len()
    }

    pub fn get(&self, index: usize) -> Option<T::Get<'_>> {
        self.iter().nth(index)
    }

    /// Replace this column with a re-encoded version where every item has
    /// been transformed by `f`.
    ///
    /// Walks runs (not items) so a single `f` call covers a whole repeat
    /// run.  For nullable columns, `f` sees `None` for null entries.
    pub fn remap<F>(&mut self, f: F)
    where
        F: Fn(T) -> T,
    {
        let counter = self.core.counter();
        *self = T::Encoding::<C>::remap(self.iter(), self.core.max_segments(), f);
        // a write like any other: iterators suspended before it can't
        // resume (the rebuilt column's count starts again at 0)
        self.core.set_counter(counter.wrapping_add(1));
    }

    /// Check the *structural* invariants of the slab vector and index —
    /// the things [`save`](Self::save) cannot catch, because it
    /// re-encodes canonically and so hides slab layout entirely:
    ///
    /// * no empty slab (they carry no weight but are not transparent to
    ///   sequential readers)
    /// * no slab over the segment budget
    /// * no adjacent pair that the merge policy should have coalesced —
    ///   i.e. one of them under `max_segments / 4` and the two fitting
    ///   in one slab
    /// * each slab's cached `len` / `segments` / `tail` agrees with a
    ///   fresh parse of its own bytes
    /// * `total_len` is the sum of the slab lengths, and the index has
    ///   one entry per slab with the matching weight
    ///
    /// Debug/test helper: O(items).
    #[doc(hidden)]
    pub fn check_invariants(&self)
    where
        WF::Weight: PartialEq + std::fmt::Debug,
    {
        let max = self.core.max_segments();
        let min = (max / 4).max(1);
        if self.core.len() == 0 {
            // an empty column keeps one empty slab to land the next
            // splice in; more than that is a leak
            assert!(
                self.core.slabs().len() <= 1,
                "empty column holds {} slabs",
                self.core.slabs().len()
            );
            return;
        }
        for (i, slab) in self.core.slabs().iter().enumerate() {
            assert_ne!(
                slab.len,
                0,
                "slab {i} of {} is empty",
                self.core.slabs().len()
            );
            assert!(
                slab.segments <= max || slab.segments == 1,
                "slab {i} has {} segments, over the budget of {max} \
                 (only a single oversize value may exceed it)",
                slab.segments
            );
            let info = T::Encoding::<C>::validate_encoding(&slab.data)
                .unwrap_or_else(|e| panic!("slab {i} does not parse: {e}"));
            assert_eq!(info.len, slab.len, "slab {i} cached len");
            assert_eq!(info.segments, slab.segments, "slab {i} cached segments");
        }
        for i in 1..self.core.slabs().len() {
            let (a, b) = (
                self.core.slabs()[i - 1].segments,
                self.core.slabs()[i].segments,
            );
            assert!(
                !((a < min || b < min) && a + b <= max),
                "slabs {} and {i} ({a} and {b} segments) should have been merged \
                 (min {min}, max {max})",
                i - 1
            );
        }
        assert_eq!(
            self.core.len(),
            self.core.slabs().iter().map(|s| s.len).sum::<usize>(),
            "total_len"
        );
        assert_eq!(
            self.core.index().len(),
            self.core.slabs().len(),
            "index length"
        );
        let mut before = 0usize;
        for (i, slab) in self.core.slabs().iter().enumerate() {
            assert_eq!(
                self.core.index().items_before(i),
                before,
                "index items_before({i})"
            );
            before += slab.len;
        }
    }

    /// Validate that the canonical encoding is well-formed.
    pub fn validate_encoding(&self) -> Result<(), PackError> {
        let bytes = self.save();
        T::Encoding::<C>::validate_encoding(&bytes)?;
        Ok(())
    }

    pub fn save(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.save_to(&mut out);
        out
    }

    pub fn save_to(&self, out: &mut Vec<u8>) -> Range<usize> {
        let start = out.len();
        // verified with Verus: the canonical encoding of the slabs' values
        T::Encoding::<C>::save_slabs(self.core.slabs(), self.core.len(), out);
        start..out.len()
    }

    pub fn iter(&self) -> Iter<'_, T, C> {
        self.iter_range(0..self.core.len())
    }

    /// Iterator over items in `range`, clamped to the column's length.
    /// O(log S) seek via the index, O(1) per item after.
    #[inline]
    pub fn iter_range(&self, range: Range<usize>) -> Iter<'_, T, C> {
        let start = range.start.min(self.core.len());
        let end = range.end.min(self.core.len()).max(start);
        // Even an empty range must leave the iterator *positioned* at
        // `start` (decoder and slab state valid): windows are routinely
        // re-extended later via `set_max`/`shift`, and an unpositioned
        // iterator would yield nothing from `next` and violate
        // `advance_to_slab`'s forward invariant from `nth`. Only a
        // start at/past the end of the data — where there is nothing
        // to sit on — takes the unpositioned shortcut.
        //
        // verified (`ColumnCore::reader`): the iterator stands at `start`
        // and gives exactly the column's values in `start..end`, from the
        // column's type invariant
        Iter {
            slabs: self.core.slabs(),
            col: Some(self),
            core: self.core.reader::<Dx<T, C>>(start, end),
            counter: self.core.counter(),
        }
    }

    /// Collect all values into a Vec.
    pub fn to_vec(&self) -> Vec<T::Get<'_>> {
        self.iter().collect()
    }

    /// Returns `true` if the column is empty or every item equals `value`.
    pub fn is_only(&self, value: T::Get<'_>) -> bool {
        self.core.len() == 0
            || self.core.slabs().iter().all(|s| {
                if s.len == 0 {
                    return true;
                }
                let mut dec = T::Encoding::<C>::slab_decoder(&s.data, crate::sealed::Trusted(()));
                match dec.next_run() {
                    Some(run) => {
                        run.count == s.len && T::eq(run.value, value) && dec.next_run().is_none()
                    }
                    None => true,
                }
            })
    }

    /// Serialize, unless all values equal `value` — in which case
    /// return an empty range (and write nothing to `out`).
    pub fn save_to_unless(&self, out: &mut Vec<u8>, value: T::Get<'_>) -> Range<usize> {
        if self.is_only(value) {
            out.len()..out.len()
        } else {
            self.save_to(out)
        }
    }

    pub fn slab_lens(&self) -> Vec<usize> {
        self.core.slabs().iter().map(|s| s.len).collect()
    }

    pub fn slab_segments(&self) -> Vec<usize> {
        self.core.slabs().iter().map(|s| s.segments).collect()
    }

    pub fn dump_slabs(&self) {
        let mut offset = 0;
        for (i, s) in self.core.slabs().iter().enumerate() {
            let mut dec = T::Encoding::<C>::slab_decoder(&s.data, crate::sealed::Trusted(()));
            let first = dec.next_run();
            let last = T::Encoding::<C>::last_run(s);
            log!(
                "  slab[{i}]: offset={offset} len={} segments={} first={:?} last={:?}",
                s.len,
                s.segments,
                first,
                last
            );
            offset += s.len;
        }
    }

    /// Sum of slab lengths over `0..si`.  O(log S) via the index — used by
    /// `scope_to_value`'s slab binary search.
    fn slab_start(&self, si: usize) -> usize {
        // verified (`IndexOps::ix_before`); the value search relies on it
        self.core.index().ix_before(si)
    }

    /// Narrow a range to the contiguous run of items matching `value`.
    ///
    /// Assumes values within `range` are sorted.  If they aren't, the
    /// result is unspecified — a wrong or empty range may be returned —
    /// but never a panic, memory unsafety, or column corruption.
    ///
    /// Returns the sub-range of `range` where every item equals `value`,
    /// or an empty range at the appropriate insertion point if `value` is
    /// not present.
    pub fn scope_to_value<V: AsColumnRef<T>>(
        &self,
        value: V,
        range: impl std::ops::RangeBounds<usize>,
    ) -> Range<usize>
    where
        for<'x> T::Get<'x>: Ord,
    {
        let (start, end) = normalize_range_max(range, self.core.len());
        let target = value.as_column_ref();
        self.iter_range(start..end).sorted_scan_to_value(target)
    }

    // ── Mutations ───────────────────────────────────────────────────────

    pub fn insert(&mut self, index: usize, value: impl AsColumnRef<T>) {
        self.splice_inner(index, 0, [(value, 1)]);
    }

    pub fn remove(&mut self, index: usize) {
        if index < self.core.len() {
            self.splice_inner::<T, _>(index, 1, std::iter::empty());
        }
    }

    /// Remove `n` items starting at `index` — same arguments as
    /// [`splice`](Self::splice)`(index, n, [])`, without the typed
    /// empty-iterator dance.
    ///
    /// # Panics
    ///
    /// Panics if `index + n` exceeds the column length (matching
    /// `splice`'s contract).
    pub fn remove_n(&mut self, index: usize, n: usize) {
        if n > 0 {
            self.splice_inner::<T, _>(index, n, std::iter::empty());
        }
    }

    pub fn push(&mut self, value: impl AsColumnRef<T>) {
        let len = self.core.len();
        self.splice_inner(len, 0, [(value, 1)]);
    }

    pub fn clear(&mut self) {
        let len = self.core.len();
        if len > 0 {
            self.splice_inner::<T, _>(0, len, std::iter::empty());
        }
    }

    pub fn truncate(&mut self, len: usize) {
        if len < self.core.len() {
            self.splice_inner::<T, _>(len, self.core.len() - len, std::iter::empty());
        }
    }

    pub fn splice<V, I>(&mut self, index: usize, del: usize, values: I)
    where
        V: AsColumnRef<T>,
        I: IntoIterator<Item = V>,
    {
        self.splice_inner(index, del, values.into_iter().map(|v| (v, 1)));
    }

    /// Splice [`Run`]s in — the run-aware fast path for bulk uniform data.
    /// Accepts `Run<T::Get<'_>>` straight from a column iterator, or runs of
    /// any other insertable form.
    pub fn splice_runs<V, I>(&mut self, index: usize, del: usize, runs: I)
    where
        V: AsColumnRef<T>,
        I: IntoIterator<Item = Run<V>>,
    {
        self.splice_inner(index, del, runs.into_iter().map(|r| (r.value, r.count)));
    }

    /// Splice `ranges` of `src` into `self` at `index`, deleting `del`
    /// rows — equivalent to
    /// `splice_runs(index, del, src.iter().runs().ranges(ranges))`.
    /// Takes `src` by value so the implementation may invert: swap the
    /// columns, delete the gaps (reusing `src`'s slabs without
    /// decoding), and splice the old `self` back around them.  The
    /// choice is unobservable — both paths yield identical values and
    /// `save()` bytes.
    ///
    /// Ranges must be ascending and non-overlapping (touching is fine)
    /// and are clamped to `src`'s length.
    pub fn copy_ranges<I>(&mut self, src: Self, splices: I)
    where
        I: IntoIterator<Item = Splice>,
        for<'b> T::Get<'b>: AsColumnRef<T>,
    {
        let splices = normalize_splices(splices, src.len(), self.len());
        if splices.is_empty() {
            return;
        }
        let ranges: Vec<Range<usize>> = splices.iter().map(|sp| sp.range.clone()).collect();
        let strategy = self.copy_strategy(&src, &ranges, splices.len());
        self.copy_splices_with(src, &splices, strategy);
    }

    /// [`copy_ranges`](Self::copy_ranges) with the strategy pinned;
    /// `splices` must already be normalized.
    pub(crate) fn copy_splices_with(
        &mut self,
        mut src: Self,
        splices: &[Splice],
        strategy: CopyStrategy,
    ) where
        for<'b> T::Get<'b>: AsColumnRef<T>,
    {
        match strategy {
            CopyStrategy::Direct => {
                // Two forward passes that never restart. The source
                // ranges ascend and never overlap, so one iterator covers
                // them; the splice positions ascend in the original
                // coordinates a cursor seeks in, so one cursor covers
                // those, and the ones landing in a slab share its rebuild.
                // Verified (`copy::copy_direct`): the column becomes
                // `applied(dst, src, splices)`.
                self.copy_direct_from(&src, splices);
            }
            CopyStrategy::Invert => {
                // keep src's slabs: swap, drop the gaps (interior slabs
                // undecoded), then splice the old column's kept
                // segments in between the retained groups (deleted old
                // rows simply never come back). Verified
                // (`copy::copy_invert`): the same result.
                self.swap_contents(&mut src);
                self.copy_invert_from(&src, splices);
            }
        }
    }

    /// Decide direct vs inverted for [`copy_ranges`](Self::copy_ranges)
    /// from stored per-slab item/segment counts — O(slabs), no data
    /// decoded.
    pub(crate) fn copy_strategy(
        &self,
        src: &Self,
        ranges: &[Range<usize>],
        points: usize,
    ) -> CopyStrategy {
        // Inverting keeps src's slabs, built under src's segment
        // budget — only sound when the budgets match.
        if self.core.max_segments() != src.core.max_segments() || src.core.slabs().len() < 8 {
            return CopyStrategy::Direct;
        }

        let mut item_prefix = Vec::with_capacity(src.core.slabs().len() + 1);
        let mut seg_prefix = Vec::with_capacity(src.core.slabs().len() + 1);
        item_prefix.push(0usize);
        seg_prefix.push(0usize);
        for s in src.core.slabs() {
            item_prefix.push(item_prefix.last().unwrap() + s.len);
            seg_prefix.push(seg_prefix.last().unwrap() + s.segments);
        }
        let slab_of = |row: usize| item_prefix.partition_point(|&p| p <= row) - 1;

        // Segments the direct path would decode + re-encode.
        let mut retained_segs = 0usize;
        for r in ranges {
            // a splice that only deletes copies nothing
            if r.is_empty() {
                continue;
            }
            let sa = slab_of(r.start);
            let sb = slab_of(r.end - 1);
            if sa == sb {
                retained_segs += src.core.slabs()[sa].segments;
            } else {
                retained_segs += src.core.slabs()[sa].segments
                    + src.core.slabs()[sb].segments
                    + (seg_prefix[sb] - seg_prefix[sa + 1]);
            }
        }

        // Direct-path work also cuts a seam in self per insertion
        // point.
        let direct_cost = retained_segs + points * 2 * self.core.max_segments();

        // Inverted-path work: re-encode all of self, boundary segments
        // per gap seam and per insertion point, O(slabs) bookkeeping.
        let dest_segs: usize = self.core.slabs().iter().map(|s| s.segments).sum();
        let seam_segs = (ranges.len() + points) * 2 * self.core.max_segments();
        let invert_cost = dest_segs + seam_segs + src.core.slabs().len();

        if direct_cost > COPY_INVERT_FACTOR * invert_cost {
            CopyStrategy::Invert
        } else {
            CopyStrategy::Direct
        }
    }

    /// Single-point [`copy_ranges`](Self::copy_ranges) with a deletion
    /// count and the strategy pinned — kept for the strategy-agreement
    /// tests, which exercise `del`.
    #[cfg(test)]
    pub(crate) fn copy_ranges_with(
        &mut self,
        index: usize,
        del: usize,
        mut src: Self,
        ranges: Vec<Range<usize>>,
        strategy: CopyStrategy,
    ) where
        for<'b> T::Get<'b>: AsColumnRef<T>,
    {
        // `index + del` could overflow
        assert!(
            index <= self.len() && del <= self.len() - index,
            "splice range out of bounds"
        );
        match strategy {
            CopyStrategy::Direct => {
                use crate::shift::Shiftable;
                self.splice_runs(index, del, src.iter().runs().ranges(ranges));
            }
            CopyStrategy::Invert => {
                let old_len = self.len();
                self.swap_contents(&mut src);
                self.retain_ranges(&ranges_as_splices(&ranges));
                let retained = self.len();
                if index > 0 {
                    self.splice_runs(0, 0, src.iter_range(0..index).runs());
                }
                if index + del < old_len {
                    self.splice_runs(
                        index + retained,
                        0,
                        src.iter_range(index + del..old_len).runs(),
                    );
                }
            }
        }
    }

    /// Keep only the rows in `splices`' ranges (normalized), deleting the
    /// gaps back-to-front; interior slabs of a gap drop without decoding.
    #[cfg(test)]
    pub(crate) fn retain_ranges(&mut self, splices: &[Splice]) {
        for_each_cut(splices, self.len(), |at, n| self.remove_n(at, n));
    }

    /// Swap contents with `other`, keeping this column's segment budget
    /// and bumping the counter past both so suspended iterators of
    /// either column can't resume against the swapped data.
    pub(crate) fn swap_contents(&mut self, other: &mut Self) {
        let counter = self.core.counter().max(other.core.counter()) + 1;
        std::mem::swap(self, other);
        // verified (`ColumnCore::swap_budgets`): each keeps its values
        self.core.swap_budgets(&mut other.core);
        self.core.set_counter(counter);
    }

    /// Returns the affected slab range (post-merge), mirroring
    /// `Column::splice_inner`.
    ///
    /// Each iterator item is `(value, count)` — pass `count = 1` for a single
    /// value, or a larger count to insert a run of identical values in bulk.
    /// The one write path: every mutation goes through the edit cursor.
    pub(crate) fn splice_inner<V, I>(&mut self, index: usize, del: usize, values: I)
    where
        V: AsColumnRef<T>,
        I: IntoIterator<Item = (V, usize)>,
    {
        // `index + del` could overflow
        assert!(
            index <= self.core.len() && del <= self.core.len() - index,
            "splice range out of bounds"
        );
        // verified (`cursor::splice_feed`): the column's values become
        // those before `index`, the runs, then those from `index + del`
        self.splice_feed(index, del, values.into_iter());
    }
}

// ── Value-range queries (B-tree + SlabAgg weight only) ─────────────────────

impl<T, C, WF> Column<T, C, WF, SlabBTree<SlabAgg>>
where
    T: ColumnValueRef,
    C: Codec,
    WF: WeightFn<T, C, Weight = SlabAgg>,
{
    /// Iterator over `(slab_idx, items_before_slab)` for slabs whose
    /// prefix-sum range covers `target`.  Passes through to
    /// `SlabBTree::find_by_value` on the inner index.
    pub fn find_by_value(&self, target: i64) -> FindByValue<'_> {
        self.core.index().find_by_value(target)
    }

    /// Iterator over `(slab_idx, items_before_slab)` for slabs whose
    /// prefix-sum range overlaps `[lo, hi]`.
    pub fn find_by_value_range(&self, lo: i64, hi: i64) -> FindByValueRange<'_> {
        self.core.index().find_by_value_range(lo, hi)
    }
}

// ── ColumnLoadIter ──────────────────────────────────────────────────────────

/// Streaming column load — see [`Column::load_iter`].
///
/// Yields the input's runs while the slabs build underneath. The input
/// must be canonical — non-canonical structure (adjacent mergeable runs,
/// count-0 runs, count-1 repeats) is a load error — so consumers get the
/// "adjacent runs never carry equal values" contract by construction.
/// All methods return `Result` — the bytes are untrusted.
pub struct ColumnLoadIter<
    'a,
    T,
    C = Leb128,
    WF = LenWeight,
    Idx = SlabBTree<<WF as WeightFn<T, C>>::Weight>,
> where
    T: ColumnValueRef,
    C: Codec,
    WF: WeightFn<T, C>,
{
    iter: <T::Encoding<C> as ColumnEncoding>::LoadIter<'a>,
    /// engaged when the data was absent and the load options carried a
    /// fill: emit one run, write one slab
    fill: Option<(T::Get<'a>, usize)>,
    fill_emitted: bool,
    length: Option<usize>,
    max_segments: usize,
    /// per-slab weights accumulated as runs stream past — only when
    /// `WF::ACCUMULATES`, sparing `finalize` the `WF::compute` re-decode
    /// (verified: `attrib`)
    attr: Attribution<T::Get<'a>, WF::Weight>,
    /// an error ended the load: the runs after it don't make a column
    failed: bool,
    _phantom: PhantomData<(WF, Idx)>,
}

impl<'a, T, C, WF, Idx> ColumnLoadIter<'a, T, C, WF, Idx>
where
    T: ColumnValueRef,
    C: Codec,
    WF: WeightFn<T, C>,
    Idx: ColumnIndex<WF::Weight>,
{
    /// The next canonical run, or `None` at end of input. A fill emits
    /// its entire contents as one run. An error ends the load: later calls,
    /// and [`finalize`](Self::finalize), return [`PackError::BadFormat`].
    pub fn try_next_run(&mut self) -> Result<Option<Run<T::Get<'a>>>, PackError> {
        if self.failed {
            return Err(PackError::BadFormat);
        }
        let r = self.next_run_inner();
        self.failed = r.is_err();
        r
    }

    fn next_run_inner(&mut self) -> Result<Option<Run<T::Get<'a>>>, PackError> {
        use crate::encoding::LoadIterApi;
        if let Some((value, len)) = self.fill {
            if self.fill_emitted || len == 0 {
                return Ok(None);
            }
            self.fill_emitted = true;
            return Ok(Some(Run { count: len, value }));
        }
        let run = self.iter.try_next_run()?;
        if WF::ACCUMULATES {
            if let Some(run) = &run {
                self.attribute(run.count, run.value)?;
            }
        }
        Ok(run)
    }

    /// Attribute a pulled run's items to the current slab's weight.
    /// Every run is exactly one wire segment (canonical input is
    /// enforced by the loader), so a run never spans a slab boundary —
    /// if this run's segment completed a slab, the cut landed at the
    /// run's end and the weight closes with it.
    fn attribute(&mut self, count: usize, value: T::Get<'a>) -> Result<(), PackError> {
        use crate::encoding::LoadIterApi;
        // the loader cuts only at a run's end (verified), so this run
        // completed the slab being cut if any slab was cut since the last
        let cut = self.attr.weights.len() < self.iter.slabs_completed();
        self.attr
            .run::<WeightOf<T, C, WF>>(count, value, cut, Ghost::assume_new())
    }

    /// Drain and validate whatever was not pulled, apply the length check
    /// from the load options, and return the finished column.
    pub fn finalize(self) -> Result<Column<T, C, WF, Idx>, PackError> {
        self.finalize_with(|_| Ok(()))
    }

    /// Like [`finalize`](Self::finalize), but runs `check` over every
    /// slab's weight (in column order) *before* any weight enters the
    /// index — so callers can validate aggregates (e.g. delta domain
    /// bounds) before they reach the index's merge arithmetic.
    pub(crate) fn finalize_with(
        mut self,
        mut check: impl FnMut(&WF::Weight) -> Result<(), PackError>,
    ) -> Result<Column<T, C, WF, Idx>, PackError> {
        use crate::encoding::LoadIterApi;
        if self.failed {
            return Err(PackError::BadFormat);
        }
        if let Some((value, len)) = self.fill {
            // the fill's one slab is `len` copies of `value`: its weight
            // gets the checks a loaded slab's does
            if WF::ACCUMULATES && len > 0 {
                let mut w = WF::Weight::default();
                WF::accumulate_run(&mut w, len, value)?;
                check(&w)?;
            }
            if len == 0 {
                return Ok(Column::with_max_segments(self.max_segments));
            }
            // `fill` makes one slab of at most two segments, within any
            // budget `load_iter` accepts
            return Ok(Column::fill_with_max_segments(
                len,
                value,
                self.max_segments,
            ));
        }
        if WF::ACCUMULATES {
            // `iter.finalize()` below drains and validates whatever is
            // left, but its fast discard loop never materializes runs —
            // accumulating weights need every run to pass through
            // `attribute`, so eat the tail here first (the finalize
            // below then only flushes the last slab)
            while self.try_next_run()?.is_some() {}
        }
        let slabs = self.iter.finalize()?;
        if let Some(expected) = self.length {
            let len: usize = slabs.iter().map(|s| s.len).sum();
            if len != expected {
                return Err(PackError::InvalidLength(len, expected));
            }
        }
        let weights: Vec<WF::Weight> = if WF::ACCUMULATES {
            // the final slab is only cut by `finalize` itself; whatever is
            // not yet attributed to a closed slab belongs to it (verified:
            // it adds at most one)
            let last = self.attr.weights.len() < slabs.len();
            let weights = self
                .attr
                .finish::<WeightOf<T, C, WF>>(last, Ghost::assume_new());
            debug_assert_eq!(weights.len(), slabs.len());
            weights
        } else {
            slabs.iter().map(WF::compute).collect()
        };
        for w in &weights {
            check(w)?;
        }
        if WF::CHECKS_TOTAL {
            // saturating: a column too long to count, which `index_of`
            // rejects, is checked too
            let len = slabs.iter().fold(0usize, |n, s| n.saturating_add(s.len));
            WF::check_total(&weights, len)?;
        }
        // edits and `save` count a column's items with room for three more:
        // a longer column is an error here, not one they would panic on
        let total_len = slabs.iter().fold(0usize, |n, s| n.saturating_add(s.len));
        if total_len > usize::MAX - 3 {
            return Err(PackError::InvalidLength(total_len, usize::MAX - 3));
        }
        Ok(Column::of_slabs(slabs, weights, self.max_segments))
    }
}

impl<'a, T, C, WF, Idx> crate::encoding::RunSrc<'a, T> for ColumnLoadIter<'a, T, C, WF, Idx>
where
    T: ColumnValueRef,
    C: Codec,
    WF: WeightFn<T, C>,
    Idx: ColumnIndex<WF::Weight>,
{
    fn try_next_run(&mut self) -> Result<Option<Run<T::Get<'a>>>, PackError> {
        ColumnLoadIter::try_next_run(self)
    }
}

/// A column's weight function, as [`Accumulate`] for the verified
/// attribution of a load's runs. Trusted: that `accumulate_run` folds a
/// run in as `acc` says.
impl<'a, T: ColumnValueRef, C: Codec, WF: WeightFn<T, C>> Accumulate<T::Get<'a>, WF::Weight>
    for WeightOf<T, C, WF>
{
    #[cfg(verus_keep_ghost)]
    fn zero() -> WF::Weight {
        unimplemented!()
    }

    #[cfg(verus_keep_ghost)]
    fn acc(_w: WF::Weight, _n: nat, _v: T::Get<'a>) -> WF::Weight {
        unimplemented!()
    }

    #[inline(always)]
    fn new_zero() -> WF::Weight {
        WF::Weight::default()
    }

    #[inline(always)]
    fn accumulate_run(w: &mut WF::Weight, n: usize, v: T::Get<'a>) -> Result<(), PackError> {
        WF::accumulate_run(w, n, v)
    }
}

// ── Trait impls ─────────────────────────────────────────────────────────────

impl<T: ColumnValueRef> FromIterator<T> for Column<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self::from_values(iter.into_iter().collect())
    }
}

impl<V, T, C, WF, Idx> Extend<V> for Column<T, C, WF, Idx>
where
    V: AsColumnRef<T>,
    T: ColumnValueRef,
    C: Codec,
    WF: WeightFn<T, C>,
    Idx: ColumnIndex<WF::Weight>,
{
    fn extend<I: IntoIterator<Item = V>>(&mut self, iter: I) {
        let len = self.core.len();
        self.splice_inner(len, 0, iter.into_iter().map(|v| (v, 1)));
    }
}

impl<T, C, WF, Idx> Column<T, C, WF, Idx>
where
    T: ColumnValueRef,
    C: Codec,
    WF: WeightFn<T, C>,
    Idx: ColumnIndex<WF::Weight>,
{
    /// Open a forward-only read/write cursor at the start of the column.
    ///
    /// ```ignore
    /// col.edit()
    ///     .seek(10).delete(2).insert(value)
    ///     .seek(100).replace(|n| n + 1)
    ///     .finish();
    /// ```
    pub fn edit(&mut self) -> Edit<'_, T, C, WF, Idx> {
        Edit::new(self, 0)
    }

    /// [`edit`](Self::edit) starting at original position `at`.
    pub fn edit_at(&mut self, at: usize) -> Edit<'_, T, C, WF, Idx> {
        Edit::new(self, at)
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::Column;
    use crate::index::BitIndex;

    fn parity_column<T>(values: Vec<T>)
    where
        T: ColumnValueRef + Clone + PartialEq + std::fmt::Debug + AsColumnRef<T>,
        for<'a> T::Get<'a>: PartialEq + std::fmt::Debug,
    {
        let base: Column<T> = Column::from_values(values.clone());
        let v2: Column<T> = Column::from_values(values.clone());
        assert_eq!(v2.len(), base.len());
        assert_eq!(v2.slab_count(), base.slab_count());
        assert_eq!(v2.save(), base.save());
        for i in 0..values.len() {
            assert_eq!(v2.get(i), base.get(i), "get({i})");
        }
    }

    #[test]
    fn parity_empty() {
        let base: Column<u64> = Column::new();
        let v2: Column<u64> = Column::new();
        assert_eq!(v2.len(), base.len());
        assert_eq!(v2.save(), base.save());
    }

    #[test]
    fn parity_u64_sequential() {
        parity_column((0u64..50).collect());
    }

    #[test]
    fn parity_u64_duplicates() {
        parity_column(vec![42u64; 100]);
    }

    #[test]
    fn parity_u64_large() {
        parity_column((0u64..5_000).collect());
    }

    #[test]
    fn parity_splice_roundtrip() {
        let mut base: Column<u64> = Column::from_values((0u64..20).collect());
        let mut v2: Column<u64> = Column::from_values((0u64..20).collect());

        base.splice(5, 3, [100u64, 200, 300, 400]);
        v2.splice(5, 3, [100u64, 200, 300, 400]);
        assert_eq!(v2.len(), base.len());
        assert_eq!(v2.save(), base.save());
        for i in 0..base.len() {
            assert_eq!(v2.get(i), base.get(i));
        }

        base.insert(0, 999u64);
        v2.insert(0, 999u64);
        for i in 0..base.len() {
            assert_eq!(v2.get(i), base.get(i));
        }

        base.remove(10);
        v2.remove(10);
        for i in 0..base.len() {
            assert_eq!(v2.get(i), base.get(i));
        }
    }

    #[test]
    fn parity_load_save() {
        let col: Column<u64> = Column::from_values((0u64..100).collect());
        let bytes = col.save();
        let v2: Column<u64> = Column::load(&bytes).unwrap();
        assert_eq!(v2.len(), col.len());
        assert_eq!(v2.save(), bytes);
    }

    #[test]
    fn parity_with_bit_index() {
        // Column backed by BitIndex — same semantics, different guts.
        let base: Column<u64> = Column::from_values((0u64..100).collect());
        let v2: Column<u64, Leb128, LenWeight, BitIndex<usize>> =
            Column::from_values((0u64..100).collect());
        assert_eq!(v2.len(), base.len());
        assert_eq!(v2.save(), base.save());
        for i in 0..base.len() {
            assert_eq!(v2.get(i), base.get(i));
        }
    }

    #[test]
    fn fuzz_parity() {
        use rand::{RngExt, SeedableRng};

        let mut rng = rand::rngs::SmallRng::seed_from_u64(0xFEEDFACE);
        let init: Vec<u64> = (0..40).map(|_| rng.random_range(0..1000)).collect();
        let mut base: Column<u64> = Column::from_values(init.clone());
        let mut v2: Column<u64> = Column::from_values(init);

        for step in 0..400 {
            let op = rng.random_range(0..3);
            let len = base.len();
            match op {
                0 => {
                    let at = rng.random_range(0..=len);
                    let v = rng.random_range(0..1000);
                    base.insert(at, v);
                    v2.insert(at, v);
                }
                1 if len > 0 => {
                    let at = rng.random_range(0..len);
                    base.remove(at);
                    v2.remove(at);
                }
                _ if len > 0 => {
                    let at = rng.random_range(0..len);
                    let del = rng.random_range(1..=(len - at).min(4));
                    let count = rng.random_range(1..=4);
                    let new: Vec<u64> = (0..count).map(|_| rng.random_range(0..1000)).collect();
                    base.splice(at, del, new.clone());
                    v2.splice(at, del, new);
                }
                _ => {}
            }
            assert_eq!(v2.len(), base.len(), "len mismatch at step {step}");
            if !base.is_empty() {
                for probe in [0, base.len() / 2, base.len() - 1] {
                    assert_eq!(v2.get(probe), base.get(probe), "step={step} probe={probe}");
                }
            }
        }

        assert_eq!(v2.save(), base.save());
    }

    #[test]
    fn column_u32_rejects_u64_overflow() {
        let col = Column::<u64>::from_values(vec![u32::MAX as u64 + 1]);
        let bytes = col.save();
        let result = Column::<u32>::load(&bytes);
        assert!(
            result.is_err(),
            "Column<u32> should reject u64 value > u32::MAX"
        );
    }

    #[test]
    fn column_u32_accepts_max_u32() {
        let col = Column::<u64>::from_values(vec![u32::MAX as u64]);
        let bytes = col.save();
        let loaded = Column::<u32>::load(&bytes);
        assert!(loaded.is_ok(), "Column<u32> should accept u32::MAX");
        assert_eq!(loaded.unwrap().get(0), Some(u32::MAX));
    }
}

#[cfg(test)]
mod edit_tests {
    use super::*;
    /// The cursor and a loop of one-shot splices must agree exactly —
    /// values, saved bytes and length — for every edit script.
    fn check(input: Vec<u64>, script: Vec<(usize, usize, Vec<u64>)>, max_seg: usize) {
        let mut a = Column::<u64>::from_values_with_max_segments(input.clone(), max_seg);
        let mut b = a.clone();

        // via the cursor: one pass, forward only
        {
            let mut e = a.edit_at(0);
            for (pos, del, ins) in &script {
                e.seek(*pos)
                    .delete(*del)
                    .insert_runs(ins.iter().map(|v| Run {
                        count: 1,
                        value: *v,
                    }));
            }
            e.finish();
        }

        // via one-shot splices. The cursor seeks in *original*
        // coordinates, so the equivalent splice sequence is back to
        // front — each splice then sees the same positions the cursor
        // did, with no shift bookkeeping on either side.
        for (pos, del, ins) in script.iter().rev() {
            b.splice(*pos, *del, ins.iter().copied());
        }

        a.check_invariants();
        b.check_invariants();
        assert_eq!(
            a.iter().collect::<Vec<_>>(),
            b.iter().collect::<Vec<_>>(),
            "values for {script:?}"
        );
        assert_eq!(a.len(), b.len(), "len for {script:?}");
        assert_eq!(a.save(), b.save(), "bytes for {script:?}");
    }

    #[test]
    fn single_insert() {
        check(vec![1, 1, 2, 3], vec![(2, 0, vec![9])], 64);
    }

    #[test]
    fn two_edits_same_slab() {
        check(
            vec![1, 1, 1, 1, 1, 1, 1, 1],
            vec![(1, 0, vec![7]), (4, 0, vec![8, 8])],
            64,
        );
    }

    #[test]
    fn edits_with_deletes() {
        check(
            (0..40u64).map(|i| i / 3).collect(),
            vec![(3, 2, vec![9]), (10, 1, vec![]), (20, 0, vec![5, 5, 5])],
            64,
        );
    }

    #[test]
    fn peek_and_replace() {
        let mut col = Column::<u64>::from_values_with_max_segments(vec![1, 2, 2, 3, 9], 4);
        {
            let mut e = col.edit();
            assert_eq!(e.peek(), Some(1));
            e.seek(1);
            assert_eq!(e.peek(), Some(2));
            // read-modify-write, the succ-count / visibility shape
            e.replace(|v| v + 10);
            assert_eq!(e.pos(), 2, "replace consumes the item it rewrote");
            e.seek(4).replace(|v| v * 2);
            e.finish();
        }
        assert_eq!(col.iter().collect::<Vec<_>>(), vec![1, 12, 2, 3, 18]);
    }

    /// A replace that does not change the value must leave the slab
    /// clean — no rebuild at all.
    #[test]
    fn noop_replace_stays_clean() {
        let mut col = Column::<u64>::from_values_with_max_segments(vec![5, 5, 5, 7, 7], 4);
        let before = col.save();
        {
            let mut e = col.edit();
            e.seek(1).replace(|v| v); // same value
            e.seek(3).replace(|v| v);
            assert_eq!(e.rebuilds(), 0, "a no-op replace must not rebuild");
            e.finish();
            // and the cursor is still where a real replace would leave it
        }
        assert_eq!(col.save(), before, "bytes unchanged");
        col.check_invariants();

        // one real change rebuilds exactly one slab
        let mut e = col.edit();
        e.seek(1).replace(|v| v + 1);
        assert_eq!(e.rebuilds(), 0, "not yet — the rebuild closes at finish");
        e.finish();
        assert_eq!(e.rebuilds(), 1, "one slab touched");
        drop(e);
        assert_eq!(col.iter().collect::<Vec<_>>(), vec![5, 6, 5, 7, 7]);
        col.check_invariants();
    }

    #[test]
    fn peek_past_the_end_is_none() {
        let mut col = Column::<u64>::from_values(vec![1, 2]);
        let mut e = col.edit();
        e.seek(2);
        assert_eq!(e.peek(), None);
        e.finish();
    }

    #[test]
    fn across_slabs() {
        // small budget so the column has many slabs
        check(
            (0..500u64).collect(),
            vec![(5, 0, vec![9]), (100, 3, vec![7]), (400, 0, vec![1, 1])],
            8,
        );
    }

    #[test]
    fn overflowing_insert() {
        // insert enough distinct values in one place to outgrow the budget
        check(
            (0..100u64).collect(),
            vec![(50, 0, (1000..1100u64).collect())],
            8,
        );
    }

    #[test]
    fn delete_across_slabs() {
        check((0..300u64).collect(), vec![(10, 120, vec![7])], 8);
    }
}

#[cfg(test)]
mod edit_fuzz {
    use super::*;

    /// xorshift, so the cases are reproducible without a dev-dependency
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }
        fn below(&mut self, n: usize) -> usize {
            if n == 0 {
                0
            } else {
                (self.next() % n as u64) as usize
            }
        }
    }

    /// An edit script in original coordinates: ascending positions, each
    /// with a delete count and a run of inserts.
    fn script(rng: &mut Rng, len: usize, edits: usize) -> Vec<(usize, usize, usize)> {
        let mut out = Vec::new();
        let mut pos = 0usize;
        for _ in 0..edits {
            pos += rng.below(len / edits.max(1) + 2);
            if pos > len {
                break;
            }
            let del = rng.below(4).min(len - pos);
            let ins = rng.below(5);
            out.push((pos, del, ins));
            pos += del;
            if pos > len {
                break;
            }
        }
        out
    }

    /// The cursor and a loop of one-shot splices must agree exactly, for
    /// every value shape and every slab budget.
    fn run<T, F>(seed: u64, make: F, max_seg: usize)
    where
        T: ColumnValueRef + Clone + PartialEq + std::fmt::Debug,
        for<'x> T::Get<'x>: AsColumnRef<T> + PartialEq + std::fmt::Debug,
        F: Fn(&mut Rng) -> T,
    {
        let mut rng = Rng(seed.max(1));
        let len = 1 + rng.below(120);
        let vals: Vec<T> = (0..len).map(|_| make(&mut rng)).collect();
        let mut a = Column::<T>::from_values_with_max_segments(vals.clone(), max_seg);
        let mut b = a.clone();
        let edits = 1 + rng.below(6);
        let plan = script(&mut rng, len, edits);
        let inserts: Vec<Vec<T>> = plan
            .iter()
            .map(|(_, _, n)| (0..*n).map(|_| make(&mut rng)).collect())
            .collect();

        {
            let mut e = a.edit();
            for ((pos, del, _), ins) in plan.iter().zip(&inserts) {
                e.seek(*pos).delete(*del);
                for v in ins {
                    e.insert(v.clone());
                }
            }
            e.finish();
        }
        for ((pos, del, _), ins) in plan.iter().zip(&inserts).rev() {
            b.splice(*pos, *del, ins.iter().cloned());
        }

        a.check_invariants();
        b.check_invariants();
        let av: Vec<T> = a.iter().map(T::to_owned).collect();
        let bv: Vec<T> = b.iter().map(T::to_owned).collect();
        assert_eq!(av, bv, "values seed={seed} max_seg={max_seg} plan={plan:?}");
        assert_eq!(a.len(), b.len(), "len seed={seed}");
        assert_eq!(a.save(), b.save(), "bytes seed={seed} plan={plan:?}");
    }

    #[test]
    fn u64_columns() {
        for seed in 1..300u64 {
            for max_seg in [4usize, 8, 64] {
                // a mix of long runs and distinct values, so slabs hold
                // both repeat runs and literal groups
                run::<u64, _>(seed, |r| r.below(6) as u64, max_seg);
                run::<u64, _>(seed, |r| r.next() % 10_000, max_seg);
            }
        }
    }

    #[test]
    fn nullable_columns() {
        for seed in 1..200u64 {
            for max_seg in [4usize, 64] {
                run::<Option<u64>, _>(
                    seed,
                    |r| match r.below(3) {
                        0 => None,
                        _ => Some(r.below(5) as u64),
                    },
                    max_seg,
                );
            }
        }
    }

    /// The clone the owned cursor state pays only shows up here.
    #[test]
    fn string_columns() {
        for seed in 1..150u64 {
            for max_seg in [4usize, 64] {
                run::<Option<String>, _>(
                    seed,
                    |r| match r.below(4) {
                        0 => None,
                        _ => Some(format!("v{}", r.below(4))),
                    },
                    max_seg,
                );
            }
        }
    }
}

#[cfg(test)]
mod edit_small {
    use super::*;

    /// Exhaustive over small scripts — cheap, and it is what caught the
    /// end-of-column reposition bug (a delete, then a seek to the end,
    /// then an insert, landing through a stale slab offset).
    #[test]
    fn small_scripts_exhaustive() {
        for len in 2..10usize {
            for max_seg in [4usize] {
                let vals: Vec<u64> = (0..len as u64).collect();
                for p0 in 0..len {
                    for d0 in 0..=(len - p0).min(3) {
                        for p1 in (p0 + d0)..=len {
                            for i1 in 0..2usize {
                                let mut a = Column::<u64>::from_values_with_max_segments(
                                    vals.clone(),
                                    max_seg,
                                );
                                let mut b = a.clone();
                                {
                                    let mut e = a.edit();
                                    e.seek(p0).delete(d0).seek(p1);
                                    for k in 0..i1 {
                                        e.insert(100 + k as u64);
                                    }
                                    e.finish();
                                }
                                b.splice(p1, 0, (0..i1).map(|k| 100 + k as u64));
                                b.splice(p0, d0, std::iter::empty::<u64>());
                                b.check_invariants();
                                a.check_invariants();
                                let av: Vec<u64> = a.iter().collect();
                                let bv: Vec<u64> = b.iter().collect();
                                if av != bv {
                                    panic!(
                                        "len={len} p0={p0} d0={d0} p1={p1} ins={i1}\n cursor={av:?}\n splice={bv:?}"
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
