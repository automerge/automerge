use std::cmp::Ordering;
use std::marker::PhantomData;
use std::ops::{AddAssign, Range, SubAssign};

use super::btree::{FindByValue, FindByValueRange, SlabAgg, SlabBTree};
use super::encoding::{ColumnEncoding, RunDecoder};
use super::index::ColumnIndex;
use super::{AsColumnRef, ColumnValueRef, TypedLoadOpts};
use crate::PackError;

/// Type alias for the slab tail metadata of a column value type.
pub type TailOf<T> = <<T as ColumnValueRef>::Encoding as ColumnEncoding>::Tail;

/// Default maximum number of RLE/bool segments per slab.
///
/// Slabs are loaded at half capacity (`max / 2`) to leave room for inserts
/// without overflowing. A splice that exceeds `max` triggers an overflow split.
pub const DEFAULT_MAX_SEG: usize = 64;

// ── Slab ─────────────────────────────────────────────────────────────────────

#[doc(hidden)]
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Slab<Tail: Copy + Clone + std::fmt::Debug + Default = ()> {
    pub(crate) data: Vec<u8>,
    pub(crate) len: usize,
    pub(crate) segments: usize,
    pub(crate) tail: Tail,
}

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

/// A value stored in a Fenwick tree (BIT) node.
///
/// For plain `Column`, this is `usize` (just the slab item count).
/// For columns that track prefix sums, this is a compound value carrying
/// both the item count and the slab's contribution to the prefix sum.
#[doc(hidden)]
pub trait SlabWeight: Clone + Default + std::fmt::Debug + AddAssign + SubAssign {
    /// The length (item count) component of this weight.
    fn len(&self) -> usize;

    /// Whether this weight represents zero items.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl SlabWeight for usize {
    #[inline]
    fn len(&self) -> usize {
        *self
    }
}

// ── WeightFn ─────────────────────────────────────────────────────────────────

/// Strategy for computing a [`SlabWeight`] from a slab's data.
///
/// This is a zero-sized type parameter on [`Column`] that controls what
/// the Fenwick tree stores.  The default [`LenWeight`] records only item
/// counts.  Prefix-aware columns use a compound weight that also tracks
/// prefix sums in the same BIT.
#[doc(hidden)]
pub trait WeightFn<T: ColumnValueRef> {
    /// The per-slab weight type.
    ///
    /// For the Fenwick-BIT-backed [`Column`] and [`BitIndex`](super::index::BitIndex),
    /// this must additionally implement [`SlabWeight`] (invertible aggregation
    /// via `AddAssign`/`SubAssign`).  For the B-tree-backed path it only needs
    /// [`SlabAggregate`](super::btree::SlabAggregate), which permits
    /// non-invertible aggregates like min/max.
    type Weight: Clone + Default + std::fmt::Debug;
    fn compute(slab: &Slab<TailOf<T>>) -> Self::Weight;
}

/// Default weight strategy: BIT stores only slab lengths.
#[doc(hidden)]
#[derive(Clone)]
pub struct LenWeight;

impl<T: ColumnValueRef> WeightFn<T> for LenWeight {
    type Weight = usize;
    #[inline]
    fn compute(slab: &Slab<TailOf<T>>) -> usize {
        slab.len
    }
}

// ── Fenwick tree helpers ─────────────────────────────────────────────────────

/// Rebuild BIT from scratch. O(S).
/// The BIT is 1-indexed: `bit\[0\]` is unused, `bit\[1..=n\]` holds the tree.
///
/// Generic over the slab type and weight: callers pass a closure that extracts
/// the per-slab weight. `Column` passes `WF::compute`; other column types
/// (e.g. the raw-byte arena) pass their own slab-length extractor.
pub(crate) fn rebuild_bit<S, W: SlabWeight>(slabs: &[S], weight: impl Fn(&S) -> W) -> Vec<W> {
    let n = slabs.len();
    let mut bit = vec![W::default(); n + 1];
    for i in 0..n {
        bit[i + 1] = weight(&slabs[i]);
    }
    // Standard O(n) BIT construction: propagate to parent.
    for i in 1..=n {
        let parent = i + (i & i.wrapping_neg());
        if parent <= n {
            let child = bit[i].clone();
            bit[parent] += child;
        }
    }
    bit
}

/// Update the BIT at slab index `si` after a weight change. O(log S).
///
/// Subtracts the old weight and adds the new weight at every ancestor node.
#[inline]
pub(crate) fn bit_point_update<W: SlabWeight>(bit: &mut [W], si: usize, old: W, new: W) {
    let mut i = si + 1; // BIT is 1-indexed
    while i < bit.len() {
        bit[i] -= old.clone();
        bit[i] += new.clone();
        i += i & i.wrapping_neg();
    }
}

/// Walk the four slab boundaries around a recently-modified range and attempt
/// to merge undersized neighbours.  The decision of *whether* two adjacent
/// slabs can merge is column-specific (encoding-aware for RLE, byte-count-only
/// for a raw arena); the caller passes `try_merge_pair(a, b)` which returns
/// `true` if it actually merged slabs `a` and `b` (removing slab `b`).
///
/// Returns the adjusted range accounting for any merges that happened — the
/// end shrinks by one for every merge that collapses two in-range slabs.
pub(crate) fn try_merge_range_skeleton(
    range: Range<usize>,
    mut try_merge_pair: impl FnMut(usize, usize) -> bool,
) -> Range<usize> {
    let mut start = range.start;
    let mut end = range.end;

    if !range.is_empty() {
        // external right — keep merging outward
        while try_merge_pair(end - 1, end) {}
        // internal left
        if (start..end).len() > 1 && try_merge_pair(end - 2, end - 1) {
            end -= 1;
        }
        // internal right
        if (start..end).len() > 1 && try_merge_pair(start, start + 1) {
            end -= 1;
        }
        // external left — keep merging outward
        while start > 0 && try_merge_pair(start - 1, start) {
            start -= 1;
            end -= 1;
        }
    }

    start..end
}

// Find slab containing logical index. Returns (slab_index, offset_within_slab). O(log S).
// Uses binary lifting on the BIT.
#[inline]
pub(crate) fn find_slab_bit<W: SlabWeight>(bit: &[W], index: usize, n: usize) -> (usize, usize) {
    if n == 0 {
        return (0, 0);
    }
    let mut pos = 0usize;
    let mut idx = 0usize;
    let mut bit_k = 1;
    while bit_k <= n {
        bit_k <<= 1;
    }
    bit_k >>= 1;
    while bit_k > 0 {
        let next = idx + bit_k;
        if next <= n && pos + bit[next].len() <= index {
            pos += bit[next].len();
            idx = next;
        }
        bit_k >>= 1;
    }
    (idx, index - pos)
}

// ── Iter ─────────────────────────────────────────────────────────────────────

// ── Iter ─────────────────────────────────────────────────────────────────────

/// Forward iterator over column items.
///
/// Created by [`Column::iter`] or [`Column::iter_range`].
///
/// `nth()` is O(log S + runs_skipped) — uses the column's index structure
/// to skip directly to the target slab.
pub(crate) trait ColumnRef<T: ColumnValueRef> {
    fn find_slab(&self, index: usize) -> (usize, usize);
    fn slab_data(&self, index: usize) -> &[u8];
    fn slab_start(&self, index: usize) -> usize;
}

impl<T, WF, Idx> ColumnRef<T> for Column<T, WF, Idx>
where
    T: ColumnValueRef,
    WF: WeightFn<T>,
    Idx: ColumnIndex<WF::Weight>,
{
    fn find_slab(&self, index: usize) -> (usize, usize) {
        self.index.find_slab(index)
    }

    fn slab_start(&self, index: usize) -> usize {
        self.slab_start(index)
    }

    fn slab_data(&self, index: usize) -> &[u8] {
        &self.slabs[index].data
    }
}

pub struct Iter<'a, T: ColumnValueRef> {
    pub(crate) slabs: &'a [Slab<TailOf<T>>],
    pub(crate) col: Option<&'a dyn ColumnRef<T>>,
    pub(crate) slab_idx: usize,
    pub(crate) decoder: <T::Encoding as ColumnEncoding>::Decoder<'a>,
    pub(crate) items_left: usize,
    pub(crate) slab_remaining: usize,
    pub(crate) pos: usize,
    pub(crate) counter: usize,
}

impl<T: ColumnValueRef> Default for Iter<'_, T> {
    fn default() -> Self {
        Self {
            slabs: &[],
            col: None,
            slab_idx: 0,
            decoder: T::Encoding::decoder(&[]),
            items_left: 0,
            slab_remaining: 0,
            pos: 0,
            counter: 0,
        }
    }
}

impl<'a, T: ColumnValueRef> Iterator for Iter<'a, T> {
    type Item = T::Get<'a>;

    #[inline]
    fn next(&mut self) -> Option<T::Get<'a>> {
        if self.items_left == 0 {
            return None;
        }
        loop {
            if let Some(v) = self.decoder.next() {
                self.items_left -= 1;
                self.slab_remaining -= 1;
                self.pos += 1;
                return Some(v);
            }
            self.slab_idx += 1;
            if self.slab_idx >= self.slabs.len() {
                self.items_left = 0;
                return None;
            }
            self.slab_remaining = self.slabs[self.slab_idx].len;
            self.decoder = T::Encoding::decoder(&self.slabs[self.slab_idx].data);
        }
    }

    /// O(log S + runs_skipped) — uses the column's index for slab lookup.
    fn nth(&mut self, n: usize) -> Option<T::Get<'a>> {
        if n >= self.items_left {
            if self.items_left > 0 {
                self.nth(self.items_left - 1);
            }
            return None;
        }

        // Fast path: target is within the current slab.
        if n < self.slab_remaining {
            self.items_left -= n + 1;
            self.slab_remaining -= n + 1;
            self.pos += n + 1;
            return self.decoder.nth(n);
        }

        // Use the column's index for O(log S) slab lookup.
        let target_pos = self.pos + n;
        let col = self.col.unwrap();
        let (si, offset) = col.find_slab(target_pos);
        if !self.advance_to_slab(si, target_pos - offset) {
            return None;
        }
        self.slab_remaining -= offset + 1;
        self.items_left -= offset + 1;
        self.pos += offset + 1;
        assert_eq!(self.pos, target_pos + 1);
        self.decoder.nth(offset)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.items_left, Some(self.items_left))
    }

    fn fold<B, F>(mut self, init: B, mut f: F) -> B
    where
        F: FnMut(B, Self::Item) -> B,
    {
        let mut acc = init;
        while self.items_left > 0 {
            if let Some(run) = self.decoder.next_run() {
                let count = run.count.min(self.items_left);
                for _ in 0..count {
                    acc = f(acc, run.value);
                }
                self.items_left -= count;
                self.slab_remaining = self.slab_remaining.saturating_sub(count);
            } else {
                self.slab_idx += 1;
                if self.slab_idx >= self.slabs.len() {
                    break;
                }
                self.decoder = T::Encoding::decoder(&self.slabs[self.slab_idx].data);
                self.slab_remaining = self.slabs[self.slab_idx].len;
            }
        }
        acc
    }
}

impl<T: ColumnValueRef> ExactSizeIterator for Iter<'_, T> {}

impl<T: ColumnValueRef> std::fmt::Debug for Iter<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Iter")
            .field("pos", &self.pos)
            .field("items_left", &self.items_left)
            .finish()
    }
}

impl<T: ColumnValueRef> Clone for Iter<'_, T> {
    fn clone(&self) -> Self {
        Self {
            slabs: self.slabs,
            col: self.col,
            slab_idx: self.slab_idx,
            decoder: self.decoder.clone(),
            items_left: self.items_left,
            slab_remaining: self.slab_remaining,
            pos: self.pos,
            counter: self.counter,
        }
    }
}

impl<'a, T: ColumnValueRef> Iter<'a, T> {
    /// Set the iterator to yield items up to (but not past) `pos`.
    ///
    /// Can both shorten and extend the iterator window. If `pos` is
    /// beyond the column length, the iterator will cleanly return `None`
    /// when it reaches the end of the data.
    pub fn set_max(&mut self, pos: usize) {
        self.items_left = pos.saturating_sub(self.pos);
    }

    /// Returns the index of the current slab.
    #[inline]
    pub fn get_slab(&self) -> usize {
        self.slab_idx
    }

    /// Returns the index of the next item to be yielded.
    #[inline]
    pub fn pos(&self) -> usize {
        self.pos
    }

    #[inline]
    pub fn end_pos(&self) -> usize {
        self.pos + self.items_left
    }

    pub fn advance_to(&mut self, target: usize) {
        if target > self.pos {
            self.nth(target - self.pos - 1);
        }
    }

    pub fn advance_by(&mut self, amount: usize) {
        self.advance_to(self.pos + amount)
    }

    /// Narrow the iterator window to the contiguous run of `value` within a
    /// sorted range, returning that range.
    ///
    /// If the value is not found, returns an empty range at the insertion
    /// point and the iterator is positioned past the search area.
    ///
    /// Optimistic fast-path: if the first value of the next slab is greater
    /// than the target, the value must be on the current slab and we scan
    /// linearly with `next_run()`.  Otherwise, falls back to the column's
    /// `scope_to_value` binary search.
    /// Returns the next run of identical values, merging across slab boundaries.
    ///
    /// For repeat runs, returns the full count. For literal runs, returns
    /// count=1 per value. Null runs return the null value with the full count.
    pub fn next_run(&mut self) -> Option<super::Run<T::Get<'a>>> {
        self.next_run_max(self.items_left)
    }

    pub(crate) fn next_run_max(&mut self, mut max: usize) -> Option<super::Run<T::Get<'a>>> {
        if max == 0 {
            return None;
        }
        let run = loop {
            let _max = self.items_left.min(self.slab_remaining).min(max);
            if let Some(run) = self.decoder.next_run_max(_max) {
                break run;
            }
            self.slab_idx += 1;
            if self.slab_idx >= self.slabs.len() {
                self.items_left = 0;
                return None;
            }
            self.slab_remaining = self.slabs[self.slab_idx].len;
            self.decoder = T::Encoding::decoder(&self.slabs[self.slab_idx].data);
        };

        let value = run.value;
        let count = run.count;
        max -= count;
        self.items_left -= count;
        self.slab_remaining -= count;
        self.pos += count;
        let mut total_count = count;

        while self.slab_remaining == 0 && self.items_left > 0 {
            self.slab_idx += 1;
            if self.slab_idx >= self.slabs.len() {
                break;
            }
            self.slab_remaining = self.slabs[self.slab_idx].len;
            self.decoder = T::Encoding::decoder(&self.slabs[self.slab_idx].data);

            let _max = self.items_left.min(self.slab_remaining).min(max);
            if let Some(next_run) = self.decoder.next_run_max(_max) {
                if next_run.value == value {
                    let c = next_run.count;
                    total_count += c;
                    max -= c;
                    self.items_left -= c;
                    self.slab_remaining -= c;
                    self.pos += c;
                    continue;
                } else {
                    // Value doesn't match — reset decoder so the consumed
                    // run can be re-read by subsequent calls.
                    self.decoder = T::Encoding::decoder(&self.slabs[self.slab_idx].data);
                }
            }
            break;
        }

        Some(super::Run {
            count: total_count,
            value,
        })
    }

    /// Moves the iterator window to `range` and returns the item at `range.start`.
    ///
    /// # Panics
    ///
    /// Panics if `range.start < self.pos()`.
    pub fn shift_next(&mut self, range: Range<usize>) -> Option<T::Get<'a>> {
        assert!(range.start >= self.pos);
        self.set_max(range.end);
        self.nth(range.start - self.pos)
    }

    pub(crate) fn advance_to_slab(&mut self, si: usize, pos: usize) -> bool {
        if si >= self.slabs.len() {
            self.slab_idx = si;
            self.pos = pos;
            self.items_left = 0;
            false
        } else {
            assert!(pos >= self.pos);
            let skipped = pos - self.pos;
            self.pos = pos;
            self.slab_idx = si;
            self.slab_remaining = self.slabs[si].len;
            self.decoder = T::Encoding::decoder(&self.slabs[si].data);
            self.items_left -= skipped;
            true
        }
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
    /// [`Iter::suspend`].
    pub fn try_resume<'a, T, WF, Idx>(
        &self,
        column: &'a Column<T, WF, Idx>,
    ) -> Result<Iter<'a, T>, PackError>
    where
        T: ColumnValueRef,
        WF: WeightFn<T>,
        Idx: ColumnIndex<WF::Weight>,
    {
        if self.counter != column.counter {
            return Err(PackError::InvalidResume);
        }
        if self.num_slabs != column.slabs.len() {
            return Err(PackError::InvalidResume);
        }
        let slabs = &column.slabs[..];
        if self.slab_idx >= slabs.len() {
            return Ok(Iter {
                slabs,
                col: Some(column),
                slab_idx: slabs.len(),
                decoder: T::Encoding::decoder(&[]),
                items_left: 0,
                slab_remaining: 0,
                pos: self.pos,
                counter: self.counter,
            });
        }
        let slab = &slabs[self.slab_idx];
        let mut decoder = T::Encoding::decoder(&slab.data);
        if self.slab_consumed > 0 {
            decoder.nth(self.slab_consumed - 1);
        }
        let slab_remaining = slab.len - self.slab_consumed;
        Ok(Iter {
            slabs,
            col: Some(column),
            slab_idx: self.slab_idx,
            decoder,
            items_left: self.items_left,
            slab_remaining,
            pos: self.pos,
            counter: self.counter,
        })
    }
}

impl<'a, T: ColumnValueRef> Iter<'a, T> {
    /// Captures the current iterator position so it can be restored later.
    pub fn suspend(&self) -> IterState {
        let slab_len = if self.slab_idx < self.slabs.len() {
            self.slabs[self.slab_idx].len
        } else {
            0
        };
        IterState {
            counter: self.counter,
            num_slabs: self.slabs.len(),
            slab_idx: self.slab_idx,
            pos: self.pos,
            items_left: self.items_left,
            slab_consumed: slab_len - self.slab_remaining,
        }
    }
}

impl<'a, T: ColumnValueRef> Iter<'a, T> {
    pub fn seek_to_value(
        &mut self,
        target: T::Get<'a>,
        range: impl std::ops::RangeBounds<usize>,
    ) -> Range<usize>
    where
        for<'x> T::Get<'x>: Ord + super::AsColumnRef<T>,
    {
        let (start, end) = crate::columndata::normalize_range_max(range, self.end_pos());

        if start > self.pos {
            self.advance_to(start);
        }

        let mut checkpoint = self.clone();
        checkpoint.set_max(end);
        let range = checkpoint.scan_to_value(target);

        self.advance_to(range.start);

        range
    }

    fn scan_to_value(mut self, target: T::Get<'a>) -> Range<usize>
    where
        for<'x> T::Get<'x>: Ord,
    {
        let col = self.col.unwrap();
        let start = self.pos();
        let end = self.end_pos();

        let first_run = match self.next_run() {
            Some(r) => r,
            None => return start..start,
        };
        let si_start = self.get_slab();

        match first_run.value.cmp(&target) {
            Ordering::Equal => {
                assert!(start + first_run.count <= end);
                start..start + first_run.count
            }
            Ordering::Greater => start..start,
            Ordering::Less => {
                let (si_end, _) = col.find_slab(end - 1);

                // Binary search slabs si_start+1..=si_end by first element.
                let mut lo = si_start + 1;
                let mut hi = si_end + 1;
                let mut candidate = None;

                while lo < hi {
                    let mid = lo + (hi - lo) / 2;
                    let mut dec = T::Encoding::decoder(col.slab_data(mid));
                    let head = dec.next_run().unwrap();
                    match head.value.cmp(&target) {
                        Ordering::Less => {
                            candidate = Some(mid);
                            lo = mid + 1;
                        }
                        // there is a possible optimization here where
                        // if equal we can call tail(previous_slab)
                        // but is tricky bc if it has 1 segment we may need to
                        // keep stepping back
                        Ordering::Greater | Ordering::Equal => hi = mid,
                    }
                }

                // `candidate` is the last slab whose first element < target.
                // The target (if present) is at the end of this slab.
                if let Some(i) = candidate {
                    let pos = col.slab_start(i);
                    self.advance_to(pos);
                };

                let mut pos = self.pos();
                while let Some(run) = self.next_run() {
                    match run.value.cmp(&target) {
                        Ordering::Less => {
                            pos += run.count;
                        }
                        Ordering::Greater => return pos..pos,
                        Ordering::Equal => return pos..pos + run.count,
                    }
                }
                pos..pos
            }
        }
    }
}

// ── Column (B-tree indexed, pluggable WF + Idx) ─────────────────────────────
//
// Default `Idx` is `SlabBTree<WF::Weight>`.  Swap to `BitIndex<WF::Weight>`
// for a Fenwick-BIT backing.  `splice_inner` inlines the body of
// `ColumnEncoding::splice`'s default — that trait method is parameterised
// over the old BIT-backed Column (now deleted); this is the equivalent for
// the B-tree path, mutating via the `ColumnIndex` trait.

pub struct Column<
    T: ColumnValueRef,
    WF: WeightFn<T> = super::column::LenWeight,
    Idx = SlabBTree<<WF as WeightFn<T>>::Weight>,
> where
    Idx: ColumnIndex<WF::Weight>,
{
    pub(crate) slabs: Vec<Slab<TailOf<T>>>,
    /// Per-slab aggregate index (B-tree by default, Fenwick BIT optional).
    pub(crate) index: Idx,
    pub(crate) total_len: usize,
    pub(crate) max_segments: usize,
    pub(crate) counter: usize,
    _phantom: PhantomData<fn() -> (T, WF)>,
}

impl<T, WF, Idx> Clone for Column<T, WF, Idx>
where
    T: ColumnValueRef,
    WF: WeightFn<T>,
    Idx: ColumnIndex<WF::Weight> + Clone,
{
    fn clone(&self) -> Self {
        Self {
            slabs: self.slabs.clone(),
            index: self.index.clone(),
            total_len: self.total_len,
            max_segments: self.max_segments,
            counter: self.counter,
            _phantom: PhantomData,
        }
    }
}

impl<T, WF, Idx> std::fmt::Debug for Column<T, WF, Idx>
where
    T: ColumnValueRef,
    WF: WeightFn<T>,
    Idx: ColumnIndex<WF::Weight>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Column")
            .field("len", &self.total_len)
            .field("slabs", &self.slabs.len())
            .finish()
    }
}

impl<T, WF, Idx> Default for Column<T, WF, Idx>
where
    T: ColumnValueRef,
    WF: WeightFn<T>,
    Idx: ColumnIndex<WF::Weight>,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T, WF, Idx> Column<T, WF, Idx>
where
    T: ColumnValueRef,
    WF: WeightFn<T>,
    Idx: ColumnIndex<WF::Weight>,
{
    pub fn new() -> Self {
        Self::with_max_segments(DEFAULT_MAX_SEG)
    }

    pub fn with_max_segments(max_segments: usize) -> Self {
        Self {
            slabs: Vec::new(),
            index: Idx::default(),
            total_len: 0,
            max_segments,
            counter: 0,
            _phantom: PhantomData,
        }
    }

    pub fn from_values(values: Vec<T>) -> Self {
        Self::from_values_with_max_segments(values, DEFAULT_MAX_SEG)
    }

    pub fn from_values_with_max_segments(values: Vec<T>, max_segments: usize) -> Self {
        let mut col = Self::with_max_segments(max_segments);
        col.splice(0, 0, values);
        col
    }

    pub fn load(data: &[u8]) -> Result<Self, PackError> {
        Self::load_verified(data, DEFAULT_MAX_SEG, None)
    }

    /// Deserialize with options.  Supports:
    ///   * `with_length(n)` — validate the column has exactly `n` items.
    ///   * `with_fill(v)` — when data is empty and length is set, fill
    ///     with `v` instead of returning empty.
    ///   * `with_validation(f)` — validate each decoded value.
    ///   * `with_max_segments(n)` — override the slab segment budget.
    pub fn load_with(data: &[u8], opts: TypedLoadOpts<T>) -> Result<Self, PackError> {
        if data.is_empty() {
            return match (opts.length, opts.fill) {
                (Some(0) | None, _) => Ok(Self::new()),
                (Some(len), Some(value)) => Ok(Self::fill(len, value)),
                (Some(len), None) => Err(PackError::InvalidLength(0, len)),
            };
        }
        let col = Self::load_verified(data, opts.max_segments, opts.validate)?;
        if let Some(expected) = opts.length {
            if col.len() != expected {
                return Err(PackError::InvalidLength(col.len(), expected));
            }
        }
        Ok(col)
    }

    pub(crate) fn load_verified(
        data: &[u8],
        max_segments: usize,
        validate: Option<for<'a> fn(T::Get<'a>) -> Option<String>>,
    ) -> Result<Self, PackError> {
        let slabs = T::Encoding::load_and_verify(data, max_segments, validate)?;
        let total_len: usize = slabs.iter().map(|s| s.len).sum();
        let index = Idx::from_weights(slabs.iter().map(WF::compute));
        Ok(Self {
            slabs,
            index,
            total_len,
            max_segments,
            counter: 0,
            _phantom: PhantomData,
        })
    }

    pub(crate) fn load_verified_fold<'a, P, F>(
        data: &'a [u8],
        max_segments: usize,
        validate: Option<F>,
    ) -> Result<Self, PackError>
    where
        P: Default + Copy,
        F: Fn(P, usize, T::Get<'a>) -> Result<P, String>,
    {
        let slabs = T::Encoding::load_and_verify_fold(data, max_segments, validate)?;
        let total_len: usize = slabs.iter().map(|s| s.len).sum();
        let index = Idx::from_weights(slabs.iter().map(WF::compute));
        Ok(Self {
            slabs,
            index,
            total_len,
            max_segments,
            counter: 0,
            _phantom: PhantomData,
        })
    }

    /// Create a column of `len` copies of `value`.
    pub fn fill(len: usize, value: T::Get<'_>) -> Self {
        if len == 0 {
            return Self::new();
        }
        let slab = T::Encoding::fill(len, value);
        let index = Idx::from_weights(std::iter::once(WF::compute(&slab)));
        Self {
            slabs: vec![slab],
            index,
            total_len: len,
            max_segments: DEFAULT_MAX_SEG,
            counter: 0,
            _phantom: PhantomData,
        }
    }

    pub fn len(&self) -> usize {
        self.total_len
    }

    pub fn is_empty(&self) -> bool {
        self.total_len == 0
    }

    pub fn slab_count(&self) -> usize {
        self.slabs.len()
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
        WF::Weight: super::btree::SlabAggregate,
    {
        *self = T::Encoding::remap(self.iter(), self.max_segments, f);
    }

    /// Validate that the canonical encoding is well-formed.
    pub fn validate_encoding(&self) -> Result<(), PackError> {
        let bytes = self.save();
        T::Encoding::validate_encoding(&bytes)?;
        Ok(())
    }

    pub fn save(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.save_to(&mut out);
        out
    }

    pub fn save_to(&self, out: &mut Vec<u8>) -> Range<usize> {
        let start = out.len();
        if let Some(first) = self.slabs.first() {
            out.extend_from_slice(&first.data);
            let mut tail = first.tail;
            let mut segments = first.segments;
            let mut buf = Vec::new();
            for s in &self.slabs[1..] {
                let (new_seg, new_tail) = T::Encoding::do_merge(out, tail, segments, s, &mut buf);
                segments = new_seg;
                tail = new_tail;
                buf.clear();
            }
        }
        start..out.len()
    }

    pub fn iter(&self) -> Iter<'_, T> {
        self.iter_range(0..self.total_len)
    }

    /// Iterator over items in `range`, clamped to the column's length.
    /// O(log S) seek via the index, O(1) per item after.
    pub fn iter_range(&self, range: Range<usize>) -> Iter<'_, T> {
        let start = range.start.min(self.total_len);
        let end = range.end.min(self.total_len);
        if start >= end || self.slabs.is_empty() {
            return Iter {
                slabs: &self.slabs,
                col: Some(self),
                slab_idx: self.slabs.len(),
                decoder: T::Encoding::decoder(&[]),
                items_left: 0,
                slab_remaining: 0,
                pos: start,
                counter: self.counter,
            };
        }
        let (si, offset) = self.index.find_slab(start);
        let mut decoder = T::Encoding::decoder(&self.slabs[si].data);
        if offset > 0 {
            decoder.nth(offset - 1);
        }
        Iter {
            slabs: &self.slabs,
            col: Some(self),
            slab_idx: si,
            decoder,
            items_left: end - start,
            slab_remaining: self.slabs[si].len - offset,
            pos: start,
            counter: self.counter,
        }
    }

    /// Collect all values into a Vec.
    pub fn to_vec(&self) -> Vec<T::Get<'_>> {
        self.iter().collect()
    }

    /// Returns `true` if the column is empty or every item equals `value`.
    pub fn is_only(&self, value: T::Get<'_>) -> bool {
        self.total_len == 0
            || self.slabs.iter().all(|s| {
                if s.len == 0 {
                    return true;
                }
                let mut dec = T::Encoding::decoder(&s.data);
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
        self.slabs.iter().map(|s| s.len).collect()
    }

    pub fn slab_segments(&self) -> Vec<usize> {
        self.slabs.iter().map(|s| s.segments).collect()
    }

    pub fn dump_slabs(&self) {
        let mut offset = 0;
        for (i, s) in self.slabs.iter().enumerate() {
            let mut dec = T::Encoding::decoder(&s.data);
            let first = dec.next_run();
            let last = T::Encoding::last_run(s);
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

    /// Sum of slab lengths over `0..si`.  O(si) — used by `scope_to_value`.
    fn slab_start(&self, si: usize) -> usize {
        self.slabs.iter().take(si).map(|s| s.len).sum()
    }

    /// Narrow a range to the contiguous run of items matching `value`.
    ///
    /// Assumes values within `range` are sorted.  Behaviour is undefined
    /// if this precondition is violated.
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
        let (start, end) = crate::columndata::normalize_range_max(range, self.total_len);
        let target = value.as_column_ref();
        self.iter_range(start..end).scan_to_value(target)
    }

    // ── Mutations ───────────────────────────────────────────────────────

    pub fn insert(&mut self, index: usize, value: impl AsColumnRef<T>) {
        self.splice(index, 0, [value]);
    }

    pub fn remove(&mut self, index: usize) {
        if index < self.total_len {
            self.splice::<T, _>(index, 1, std::iter::empty());
        }
    }

    pub fn push(&mut self, value: impl AsColumnRef<T>) {
        let len = self.total_len;
        self.splice(len, 0, [value]);
    }

    pub fn clear(&mut self) {
        let len = self.total_len;
        if len > 0 {
            self.splice::<T, _>(0, len, std::iter::empty());
        }
    }

    pub fn truncate(&mut self, len: usize) {
        if len < self.total_len {
            self.splice::<T, _>(len, self.total_len - len, std::iter::empty());
        }
    }

    #[inline(never)]
    pub fn splice<V, I>(&mut self, index: usize, del: usize, values: I)
    where
        V: AsColumnRef<T>,
        I: IntoIterator<Item = V>,
    {
        let _ = self.splice_inner(index, del, values);
    }

    /// Returns the affected slab range (post-merge), mirroring
    /// `Column::splice_inner`.
    #[inline(never)]
    pub(crate) fn splice_inner<V, I>(&mut self, index: usize, del: usize, values: I) -> Range<usize>
    where
        V: AsColumnRef<T>,
        I: IntoIterator<Item = V>,
    {
        self.counter += 1;
        assert!(index + del <= self.total_len, "splice range out of bounds");

        let mut iter = values.into_iter().peekable();
        if del == 0 && iter.peek().is_none() {
            return 0..0;
        }

        if self.slabs.is_empty() {
            let empty = T::Encoding::empty_slab();
            self.index.splice(0..0, [WF::compute(&empty)]);
            self.slabs.push(empty);
        }

        let (mut si, mut offset) = self.find_slab(index);
        if si >= self.slabs.len() {
            si = self.slabs.len() - 1;
            offset = self.slabs[si].len;
        }

        let range = self.encoder_splice(si, offset, del, iter);

        if self.index.len() == self.slabs.len() && range.len() == 1 {
            // Fast path: single slab mutated, no count change.
            // update_slab is O(log n) on both BIT and B-tree.
            let new_weight = WF::compute(&self.slabs[range.start]);
            self.index.update_slab(range.start, new_weight);
        } else {
            // Structural change — splice the stale region of the index.
            let new_weights = self.slabs[range.clone()].iter().map(WF::compute);
            let old_end = range.end + self.index.len() - self.slabs.len();
            self.index.splice(range.start..old_end, new_weights);
            debug_assert_eq!(self.index.len(), self.slabs.len());
        }

        range
    }

    /// Inlined body of [`ColumnEncoding::splice_slab`]'s default, adapted to
    /// `&mut Column`.  Mutates `self.slabs` + `self.total_len` + runs
    /// `try_merge_range`; leaves the index untouched (the outer
    /// `splice_inner` reconciles it).
    #[inline(never)]
    fn encoder_splice<V, I>(
        &mut self,
        si: usize,
        offset: usize,
        del: usize,
        values: I,
    ) -> Range<usize>
    where
        V: AsColumnRef<T>,
        I: Iterator<Item = V>,
    {
        let mut range = si..(si + 1);
        let mut old_slab_len = self.slabs[si].len;

        let (overflow, overflow_del) =
            T::Encoding::splice_slab(&mut self.slabs[si], offset, del, values, self.max_segments);

        let overflow_len = overflow.len();

        // Walk subsequent slabs to satisfy overflow_del (cascade delete).
        // Don't shift the slab Vec yet — just identify what's consumed.
        let consume_start = si + 1;
        let mut consume_end = consume_start;
        let mut remaining = overflow_del;
        let mut has_partial = false;

        while remaining > 0 && consume_end < self.slabs.len() {
            let slab_len = self.slabs[consume_end].len;
            if remaining >= slab_len {
                old_slab_len += slab_len;
                remaining -= slab_len;
                consume_end += 1;
            } else {
                old_slab_len += self.slabs[consume_end].len;
                let (partial_overflow, _) = T::Encoding::splice_slab(
                    &mut self.slabs[consume_end],
                    0,
                    remaining,
                    std::iter::empty::<V>(),
                    self.max_segments,
                );
                consume_end += 1;
                has_partial = true;
                assert!(partial_overflow.is_empty());
                break;
            }
        }

        // How many slabs were fully consumed (should be removed)?
        let fully_consumed = if has_partial {
            consume_end - consume_start - 1
        } else {
            consume_end - consume_start
        };

        // Single Vec::splice replaces fully-consumed slabs with overflow
        // slabs.  For replace ops (overflow_len ≈ fully_consumed), this
        // is nearly a no-op — overwrites in place instead of shifting
        // the entire tail twice.
        self.slabs
            .splice(consume_start..consume_start + fully_consumed, overflow);

        // Compute range: landing slab + overflow + any partial slab.
        let partial_kept = if has_partial { 1 } else { 0 };
        range.end = consume_start + overflow_len + partial_kept;

        self.total_len += self.slabs[range.clone()]
            .iter()
            .map(|s| s.len)
            .sum::<usize>();
        self.total_len -= old_slab_len;
        debug_assert_eq!(
            self.total_len,
            self.slabs.iter().map(|s| s.len).sum::<usize>()
        );

        self.try_merge_range(range)
    }

    #[inline(never)]
    fn try_merge(&mut self, index_a: usize, index_b: usize) -> bool {
        let max = self.max_segments;
        let min = self.max_segments / 4;
        if let Some(a) = self.slabs.get(index_a).map(|s| s.segments) {
            if let Some(b) = self.slabs.get(index_b).map(|s| s.segments) {
                if (a < min || b < min) && a + b <= max {
                    let slab_b = self.slabs.remove(index_b);
                    T::Encoding::merge_slabs(&mut self.slabs[index_a], slab_b);
                    return true;
                }
            }
        }
        false
    }

    #[inline(never)]
    fn try_merge_range(&mut self, range: Range<usize>) -> Range<usize> {
        super::column::try_merge_range_skeleton(range, |a, b| self.try_merge(a, b))
    }
}

// ── Value-range queries (B-tree + SlabAgg weight only) ─────────────────────

impl<T, WF> Column<T, WF, SlabBTree<SlabAgg>>
where
    T: ColumnValueRef,
    WF: WeightFn<T, Weight = SlabAgg>,
{
    /// Iterator over `(slab_idx, items_before_slab)` for slabs whose
    /// prefix-sum range covers `target`.  Passes through to
    /// `SlabBTree::find_by_value` on the inner index.
    pub fn find_by_value(&self, target: i64) -> FindByValue<'_> {
        self.index.find_by_value(target)
    }

    /// Iterator over `(slab_idx, items_before_slab)` for slabs whose
    /// prefix-sum range overlaps `[lo, hi]`.
    pub fn find_by_value_range(&self, lo: i64, hi: i64) -> FindByValueRange<'_> {
        self.index.find_by_value_range(lo, hi)
    }
}

// ── Trait impls ─────────────────────────────────────────────────────────────

impl<T: ColumnValueRef> FromIterator<T> for Column<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self::from_values(iter.into_iter().collect())
    }
}

impl<V, T, WF, Idx> Extend<V> for Column<T, WF, Idx>
where
    V: AsColumnRef<T>,
    T: ColumnValueRef,
    WF: WeightFn<T>,
    Idx: ColumnIndex<WF::Weight>,
{
    fn extend<I: IntoIterator<Item = V>>(&mut self, iter: I) {
        let len = self.total_len;
        self.splice(len, 0, iter);
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v1::column::Column;
    use crate::v1::index::BitIndex;

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
        let v2: Column<u64, super::super::column::LenWeight, BitIndex<usize>> =
            Column::from_values((0u64..100).collect());
        assert_eq!(v2.len(), base.len());
        assert_eq!(v2.save(), base.save());
        for i in 0..base.len() {
            assert_eq!(v2.get(i), base.get(i));
        }
    }

    #[test]
    fn fuzz_parity() {
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

        let mut rng = Rng::new(0xFEEDFACE);
        let init: Vec<u64> = (0..40).map(|_| rng.next() % 1000).collect();
        let mut base: Column<u64> = Column::from_values(init.clone());
        let mut v2: Column<u64> = Column::from_values(init);

        for step in 0..400 {
            let op = rng.next() % 3;
            let len = base.len();
            match op {
                0 => {
                    let at = (rng.next() as usize) % (len + 1);
                    let v = rng.next() % 1000;
                    base.insert(at, v);
                    v2.insert(at, v);
                }
                1 if len > 0 => {
                    let at = (rng.next() as usize) % len;
                    base.remove(at);
                    v2.remove(at);
                }
                _ if len > 0 => {
                    let at = (rng.next() as usize) % len;
                    let del = (rng.next() as usize) % (len - at).min(4) + 1;
                    let count = (rng.next() as usize) % 4 + 1;
                    let new: Vec<u64> = (0..count).map(|_| rng.next() % 1000).collect();
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
