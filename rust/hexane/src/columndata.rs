use super::aggregate::Acc;
use super::aggregate::Agg;
use super::cursor::{ColumnCursor, HasAcc, HasMinMax, HasPos, Run, RunIter, RunIterState};
use super::encoder::Encoder;
use super::pack::{MaybePackable, PackError, Packable};
use super::raw::RawReader;
use super::slab;
use super::slab::{Slab, SlabTree, SpanTree, SpanTreeIterState};
use super::Cow;

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::{Bound, Range, RangeBounds};

/// A compressed, mutable column of optional typed values.
///
/// `ColumnData<C>` stores a sequence of `Option<C::Item>` values using the encoding
/// determined by cursor type `C`. Data is held internally in a `SpanTree` of [`Slab`]s;
/// modifications replace individual slabs, leaving the rest untouched.
///
/// # Common cursor types
///
/// [`UIntCursor`](crate::UIntCursor), [`IntCursor`](crate::IntCursor),
/// [`StrCursor`](crate::StrCursor), [`ByteCursor`](crate::ByteCursor),
/// [`BooleanCursor`](crate::BooleanCursor), [`DeltaCursor`](crate::DeltaCursor),
/// [`RawCursor`](crate::RawCursor).
///
/// # Example
///
/// ```rust
/// use hexane::{ColumnData, UIntCursor};
/// use std::borrow::Cow;
///
/// let mut col: ColumnData<UIntCursor> = ColumnData::new();
/// col.splice(0, 0, [1u64, 2, 3]);
/// assert_eq!(col.get(1), Some(Some(Cow::Owned(2))));
/// assert_eq!(col.to_vec(), vec![Some(1), Some(2), Some(3)]);
/// ```
#[derive(Debug, Clone)]
pub struct ColumnData<C: ColumnCursor> {
    pub len: usize,
    pub slabs: SpanTree<Slab, C::SlabIndex>,
    #[cfg(feature = "slow_path_assertions")]
    pub debug: Vec<C::Export>,
    counter: usize,
    _phantom: PhantomData<C>,
}

impl<C: ColumnCursor> Default for ColumnData<C> {
    fn default() -> Self {
        Self::new()
    }
}

impl<C: ColumnCursor> PartialEq for ColumnData<C> {
    fn eq(&self, other: &Self) -> bool {
        // we could use run iter execept sometimes runs are broken across slab boundaries
        // maybe a top level run_iter that glues runs together?
        self.iter().eq(other.iter())
    }
}

impl<C: ColumnCursor> ColumnData<C> {
    /// Total number of bytes used by all slabs (encoded, compressed size).
    pub fn byte_len(&self) -> usize {
        self.slabs.iter().map(|s| s.as_slice().len()).sum()
    }

    /// Returns the value at `index`, or `None` if the index is out of bounds.
    ///
    /// The inner `Option` is `None` for null entries and `Some(value)` otherwise.
    /// This is O(log n + B) where B is the number of encoded runs in the target slab.
    /// For multiple sequential reads prefer [`ColumnData::iter`] or [`ColumnData::iter_range`].
    pub fn get(&self, index: usize) -> Option<Option<Cow<'_, C::Item>>> {
        let range = index..(index + 1);
        let mut iter = self.iter_range(range);
        iter.next()
    }

    /// Returns the change in accumulator between `index1` and `index2`, together with
    /// the item at `index2`.
    ///
    /// Panics if `index1 > index2`.
    pub fn get_acc_delta(&self, index1: usize, index2: usize) -> (Acc, Option<Cow<'_, C::Item>>) {
        assert!(index1 <= index2);
        let acc1 = self.get_acc(index1);
        let mut iter = self.iter_range(index2..(index2 + 1));
        let acc2 = iter.calculate_acc();
        let item = iter.next().flatten();
        (acc2 - acc1, item)
    }

    /// Returns the cumulative [`Acc`] for all items *before* `index`
    /// (i.e. the sum of `agg(item)` for items `0..index`).
    pub fn get_acc(&self, index: usize) -> Acc {
        let range = index..(index + 1);
        let iter = self.iter_range(range);
        iter.calculate_acc()
    }

    /// Returns the item at `index` together with the [`Acc`] value immediately before it,
    /// or `None` if the index is out of bounds.
    pub fn get_with_acc(
        &self,
        index: usize,
    ) -> Option<ColGroupItem<'_, <C as ColumnCursor>::Item>> {
        let range = index..(index + 1);
        let mut iter = self.iter_range(range).with_acc();
        iter.next()
    }

    /// Returns `true` if every item in the column is null (`None`) or, for
    /// [`BooleanCursor`](crate::BooleanCursor), if every value is `false`.
    ///
    /// An empty column (`len() == 0`) is also considered empty.
    pub fn is_empty(&self) -> bool {
        let run = self.iter().next_run();
        match run {
            None => true,
            Some(run) if run.count != self.len => false,
            Some(run) => C::is_empty(run.value),
        }
    }

    pub fn dump(&self) {
        let data = self.to_vec();
        log!(" :: {:?}", data);
    }

    /// Returns a new column with every item transformed by `f`.
    ///
    /// Equivalent to consuming `self` and re-encoding all items through `f`.
    /// For an in-place version see [`ColumnData::remap`].
    // TODO: could be much faster if done a run at a time (delta runs are tricky)
    pub fn and_remap<F>(self, f: F) -> Self
    where
        F: Fn(Option<Cow<'_, C::Item>>) -> Option<Cow<'_, C::Item>>,
    {
        // TODO this could be much faster
        // if we did it a run at a time instead of an item at a time
        // but delta runs are special and don't remap easily
        let mut encoder = Encoder::new(false);
        for item in self.iter() {
            encoder.append_item(f(item));
        }
        //std::mem::swap(self, &mut col);
        encoder.into_column_data()
    }

    /// Replaces the column with a re-encoded version where every item has been
    /// transformed by `f`. For a consuming version see [`ColumnData::and_remap`].
    // TODO: could be much faster if done a run at a time (delta runs are tricky)
    pub fn remap<F>(&mut self, f: F)
    where
        F: Fn(Option<Cow<'_, C::Item>>) -> Option<Cow<'_, C::Item>>,
    {
        // TODO this could be much faster
        // if we did it a run at a time instead of an item at a time
        // but delta runs are special and don't remap easily
        let mut encoder = Encoder::new(false);
        for item in self.iter() {
            encoder.append_item(f(item));
        }
        *self = encoder.into_column_data();
    }

    /// Like [`save_to`](ColumnData::save_to) but writes nothing if [`is_empty`](ColumnData::is_empty)
    /// returns `true`, returning an empty range at the current end of `out`.
    pub fn save_to_unless_empty(&self, out: &mut Vec<u8>) -> Range<usize> {
        if self.is_empty() {
            out.len()..out.len()
        } else {
            self.save_to(out)
        }
    }

    /// Serializes the column by appending encoded bytes to `out`.
    ///
    /// Returns the byte range written (`out[range]` is the serialized column data).
    /// The output is compatible with [`ColumnData::load`]. If the column is empty (zero items),
    /// nothing is written and an empty range is returned.
    pub fn save_to(&self, out: &mut Vec<u8>) -> Range<usize> {
        let start = out.len();
        #[allow(clippy::len_zero)]
        if self.len() == 0 {
            // is_empty() considers all false to be empty
            return start..start;
        }
        if self.slabs.len() == 1 {
            let slab = self.slabs.get(0).unwrap();
            if slab.is_empty() {
                let mut encoder: Encoder<C> = Encoder::with_capacity(2, true);
                encoder.flush();
                encoder.writer.write(out);
            } else {
                out.extend(slab.as_slice())
            }
        } else {
            let mut encoder: Encoder<C> = Encoder::with_capacity(self.slabs.len() * 7, true);
            for s in &self.slabs {
                encoder.copy_slab(s);
            }
            encoder.flush();
            encoder.writer.write(out);
        }
        let end = out.len();
        start..end
    }

    pub fn raw_reader(&self, advance: usize) -> RawReader<'_, C::SlabIndex> {
        let cursor = self
            .slabs
            .get_where_or_last(|acc, next| advance < acc.pos() + next.pos());
        let current = Some((cursor.element, advance - cursor.weight.pos()));
        let slabs = slab::SpanTreeIter::new(&self.slabs, cursor);
        let pos = advance;
        RawReader {
            pos,
            slabs,
            current,
        }
    }
}

/// An iterator over items in a [`ColumnData`], with rich navigation capabilities.
///
/// Produced by [`ColumnData::iter`] and [`ColumnData::iter_range`].
///
/// Beyond standard `Iterator` usage, `ColumnDataIter` supports:
/// - [`advance_by`](ColumnDataIter::advance_by) / [`advance_to`](ColumnDataIter::advance_to)
///   — fast O(log n) forward jump.
/// - [`seek_to_value`](ColumnDataIter::seek_to_value) — binary search for a sorted value.
/// - [`advance_acc_by`](ColumnDataIter::advance_acc_by) — advance by accumulator amount.
/// - [`next_run`](ColumnDataIter::next_run) — access the raw RLE runs.
/// - [`shift_next`](ColumnDataIter::shift_next) — move the window and return the next item.
/// - [`suspend`](ColumnDataIter::suspend) / [`ColumnDataIterState::try_resume`] — serialize
///   and restore position across async boundaries or between calls.
/// - [`with_acc`](ColumnDataIter::with_acc) — wrap in a [`ColGroupIter`] that emits
///   `(acc, pos, item)` tuples.
/// - [`as_acc`](ColumnDataIter::as_acc) — wrap in a [`ColAccIter`] that emits only `Acc`.
#[derive(Debug)]
pub struct ColumnDataIter<'a, C: ColumnCursor> {
    counter: usize,
    pos: usize,
    max: usize,
    slabs: slab::SpanTreeIter<'a, Slab, C::SlabIndex>,
    slab: RunIter<'a, C>,
    run: Option<Run<'a, C::Item>>,
}

impl<C: ColumnCursor> Default for ColumnDataIter<'_, C> {
    fn default() -> Self {
        Self {
            counter: 0,
            pos: 0,
            max: 0,
            slabs: slab::SpanTreeIter::default(),
            slab: RunIter::default(),
            run: None,
        }
    }
}

impl<C: ColumnCursor> Clone for ColumnDataIter<'_, C> {
    fn clone(&self) -> Self {
        Self {
            counter: self.counter,
            pos: self.pos,
            max: self.max,
            slabs: self.slabs.clone(),
            slab: self.slab,
            run: self.run.clone(),
        }
    }
}

impl<'a, C: ColumnCursor> ColumnDataIter<'a, C> {
    pub(crate) fn new(
        slabs: &'a SlabTree<C::SlabIndex>,
        pos: usize,
        max: usize,
        counter: usize,
    ) -> Self {
        let cursor = slabs.get_where_or_last(|acc, next| pos < acc.pos() + next.pos());
        let mut slab = cursor.element.run_iter::<C>();
        let slabs = slab::SpanTreeIter::new(slabs, cursor);
        let iter_pos = slabs.weight().pos() - slab.pos_left();
        let advance = pos - iter_pos;
        let run = slab.sub_advance(advance);
        ColumnDataIter {
            counter,
            pos,
            max,
            slabs,
            slab,
            run,
        }
    }

    pub(crate) fn try_resume(
        slab_tree: &'a SlabTree<C::SlabIndex>,
        state: &ColumnDataIterState<C>,
        counter: usize,
    ) -> Result<Self, PackError> {
        if counter != state.counter {
            return Err(PackError::InvalidResume);
        }
        if slab_tree.len() != state.num_slabs {
            return Err(PackError::InvalidResume);
        }
        let counter = state.counter;
        let pos = state.pos;
        let max = state.max;
        let slabs = slab_tree.resume(state.slabs_state.clone());
        let s = slabs.current().unwrap();
        let slab = RunIter::resume(s.as_slice(), state.run_state);
        let run = slab.current().map(|r| Run {
            count: state.run.unwrap(),
            value: r.value,
        });
        Ok(ColumnDataIter {
            counter,
            pos,
            max,
            slabs,
            slab,
            run,
        })
    }

    pub(crate) fn new_at_index(
        slabs: &'a SlabTree<C::SlabIndex>,
        index: usize,
        max: usize,
        counter: usize,
    ) -> Self {
        let cursor = slabs.get_cursor(index).unwrap();
        let mut slab = cursor.element.run_iter::<C>();
        let slabs = slab::SpanTreeIter::new(slabs, cursor);
        let pos = slabs.weight().pos() - slab.pos_left();
        let run = slab.sub_advance(0);
        assert!(pos < max);
        ColumnDataIter {
            counter,
            pos,
            max,
            slabs,
            slab,
            run,
        }
    }

    pub(crate) fn new_at_acc(
        slabs: &'a SlabTree<C::SlabIndex>,
        acc: Acc,
        max: usize,
        counter: usize,
    ) -> Self {
        let cursor = slabs.get_where_or_last(|a, next| acc < a.acc() + next.acc());
        let mut slab = cursor.element.run_iter();
        let pos = cursor.weight.pos();
        let slabs = slab::SpanTreeIter::new(slabs, cursor);
        let run = slab.sub_advance(0);
        ColumnDataIter {
            counter,
            pos,
            max,
            slabs,
            slab,
            run,
        }
    }

    /// Returns the current position (index of the next item to be yielded).
    pub fn pos(&self) -> usize {
        debug_assert_eq!(
            self.slabs.weight().pos() - self.slab.pos_left() - self.run_count(),
            self.pos
        );
        std::cmp::min(self.pos, self.max)
    }

    fn check_pos(&self) {
        debug_assert_eq!(
            self.slabs.weight().pos() - self.slab.pos_left() - self.run_count(),
            self.pos
        );
    }

    /// Returns the number of items remaining in the current [`Run`].
    pub fn run_count(&self) -> usize {
        self.run.as_ref().map(|e| e.count).unwrap_or_default()
    }

    fn run_acc(&self) -> Acc {
        self.run.as_ref().map(|e| e.acc()).unwrap_or_default()
    }

    fn pop_element(&mut self) -> Option<Option<Cow<'a, C::Item>>> {
        self.slab.cursor.pop(self.run.as_mut()?)
    }

    /// Returns the next RLE [`Run`], advancing `pos` by `run.count`.
    ///
    /// More efficient than calling `next()` repeatedly when you only need run-level access.
    /// Returns `None` when the iterator is exhausted.
    pub fn next_run(&mut self) -> Option<Run<'a, C::Item>> {
        if self.pos >= self.max {
            return None;
        }
        let mut run = self.run.take().or_else(|| self.pop_run())?;
        let count = run.count;
        if self.pos + run.count > self.max {
            let remainder = self.max - self.pos;
            let overflow = run.count - remainder;
            run.count = remainder;
            self.run = Some(Run {
                value: run.value.clone(),
                count: overflow,
            });
            self.pos += remainder;
        } else {
            self.pos += count;
        }
        self.check_pos();
        if run.count == 0 {
            self.next_run()
        } else {
            Some(run)
        }
    }

    fn pop_run(&mut self) -> Option<Run<'a, C::Item>> {
        self.slab.next().or_else(|| {
            self.slab = self.slabs.next()?.run_iter();
            self.slab.next()
        })
    }

    /// Advances the iterator by `amount` items in O(log n). A no-op if `amount` is 0.
    pub fn advance_by(&mut self, amount: usize) {
        if amount > 0 {
            self.nth(amount - 1);
        }
        self.check_pos();
    }

    /// Advances the iterator to position `target` in O(log n).
    ///
    /// Panics if `target < self.pos()`.
    pub fn advance_to(&mut self, target: usize) {
        assert!(target >= self.pos());
        if target > self.pos() {
            self.advance_by(target - self.pos());
        }
        //assert_eq!(target, self.pos()); // max can stop this
    }

    fn slab_index(&self) -> usize {
        self.slabs.index() - 1
    }

    // Binary search through the span tree nodes to find the slab likely containing `target`.
    // Only valid when data is sorted within the range. Reads only the first element of each
    // node; never reads the first node because we may not be including its first element.
    fn binary_search_for<B>(&self, target: Option<B>, max: usize) -> Option<usize>
    where
        B: Borrow<C::Item> + Debug + Copy,
        C::Item: Ord,
    {
        let original_start = self.slab_index();
        let mut start = original_start;

        let next_slab_value = self.slabs.peek()?.first_value::<C>();
        match _cmp(next_slab_value.clone(), &target) {
            Ordering::Greater => {
                return None;
            }
            Ordering::Less => {
                // not in current slab
                //start += 1;
            }
            Ordering::Equal => (), // could still be in current slab
        }

        let slabs = self.slabs.span_tree()?;
        let mut end = slabs
            .get_where_or_last(|a, next| max < a.pos() + next.pos())
            .index;
        let mut mid = (start + end).div_ceil(2);
        while start < mid && mid < end {
            let value = slabs.get(mid)?.first_value::<C>();
            if _cmp(value, &target) == Ordering::Less {
                start = mid;
            } else {
                end = mid;
            }
            mid = (start + end).div_ceil(2);
        }
        if start != original_start {
            assert!(start <= end);
            Some(start)
        } else {
            None
        }
    }

    /// Returns the contiguous index range where `value` appears within `range`, positioning
    /// the iterator at the start of that range.
    ///
    /// **Requires** that values within `range` are sorted; gives undefined results otherwise.
    /// Uses B-tree binary search followed by a linear slab scan.
    /// Returns an empty range at the found position if `value` is absent.
    ///
    /// After returning, the iterator is positioned at the first index where `value` appears
    /// (or where it would appear if absent), ready for further reads.
    pub fn seek_to_value<B, R>(&mut self, value: Option<B>, range: R) -> Range<usize>
    where
        B: Borrow<C::Item> + Copy + Debug,
        C::Item: Ord,
        R: RangeBounds<usize>,
    {
        let (min, max) = normalize_range(range);
        let max = std::cmp::min(max, self.max);

        // FIXME - wasteful if we're gonna re-set
        if min > self.pos() {
            self.advance_to(min);
        }

        if let Some(index) = self.binary_search_for(value, max) {
            self.reset_iter_to_slab_index(index);
        }
        let mut end = self.pos();
        let mut first_run = self.run.take();
        let mut found = None;
        while let Some(mut run) = first_run.take().or_else(|| self.pop_run()) {
            if run.count == 0 {
                continue;
            }
            let c = run.count;
            match _cmp(value, &run.value) {
                Ordering::Equal if found.is_none() => {
                    let mut copy = self.clone();
                    copy.run = Some(run.clone());
                    found = Some(copy);
                }
                Ordering::Greater => {}
                Ordering::Equal => {}
                Ordering::Less => {
                    self.run = Some(run);
                    break;
                }
            }
            self.pos += c;
            end += c;
            if self.pos >= max {
                let delta = self.pos - max;
                self.pos -= delta;
                end -= delta;
                run.count = delta;
                self.run = Some(run);
                break;
            }
        }
        if let Some(f) = found {
            // go back
            *self = f;
        }
        let start = std::cmp::min(self.pos, max);
        let end = std::cmp::min(end, max);
        start..end
    }

    /// Returns the exclusive upper bound of the iteration range (as set by `iter_range` or `set_max`).
    pub fn end_pos(&self) -> usize {
        self.max
    }

    /// Overrides the upper bound of the iteration range.
    pub fn set_max(&mut self, max: usize) {
        self.max = max
    }

    /// Collects all remaining items into a `Vec`. Primarily useful for testing.
    pub fn to_vec(self) -> Vec<C::Export> {
        let mut result = vec![];
        C::export_splice(&mut result, 0..0, self);
        result
    }

    /// Wraps this iterator in a [`ColGroupIter`] that emits `(acc, pos, item)` tuples.
    pub fn with_acc(self) -> ColGroupIter<'a, C> {
        ColGroupIter { iter: self }
    }

    /// Wraps this iterator in a [`ColAccIter`] that emits only the [`Acc`] value for each item.
    pub fn as_acc(self) -> ColAccIter<'a, C> {
        ColAccIter { iter: self }
    }

    /// Returns the [`Acc`] value immediately before the current iterator position.
    pub fn calculate_acc(&self) -> Acc {
        self.slabs.weight().acc() - self.slab.acc_left() - self.run_acc()
    }

    fn reset_iter_to_pos(&mut self, pos: usize) -> Option<()> {
        let tree = self.slabs.span_tree()?;
        let pos = std::cmp::min(pos, self.max);
        let new_iter = Self::new(tree, pos, self.max, self.counter);
        let _ = std::mem::replace(self, new_iter);
        Some(())
    }

    fn reset_iter_to_slab_index(&mut self, index: usize) -> Option<()> {
        let tree = self.slabs.span_tree()?;
        let new_iter = Self::new_at_index(tree, index, self.max, self.counter);
        let _ = std::mem::replace(self, new_iter);
        Some(())
    }

    fn reset_iter_to_acc(&mut self, acc: Acc) -> Acc {
        if let Some(tree) = self.slabs.span_tree() {
            let _ = std::mem::replace(self, Self::new_at_acc(tree, acc, self.max, self.counter));
            let new_acc = self.calculate_acc();
            acc - new_acc
        } else {
            Acc::default()
        }
    }

    /// Moves the iterator window to `range` and returns the item at `range.start`.
    ///
    /// The iterator must already be at or before `range.start`. After this call, the
    /// iterator will yield items from `range.start` up to (exclusive) `range.end`.
    /// Subsequent calls to `shift_next` can extend the window further forward.
    ///
    /// Panics if `range.start < self.pos`.
    pub fn shift_next(&mut self, range: Range<usize>) -> Option<<Self as Iterator>::Item> {
        assert!(range.start >= self.pos);
        self.max = range.end;
        self.nth(range.start - self.pos)
    }

    fn total_acc(&self) -> Acc {
        self.slabs
            .total_weight()
            .map(|w| w.acc())
            .unwrap_or_default()
    }

    /// Advances the iterator until the cumulative [`Acc`] has grown by at least `n`.
    ///
    /// Returns the number of items consumed. If the total accumulator of the remaining
    /// items is less than `n`, the iterator is exhausted and the actual advance is returned.
    ///
    /// This is O(log n) using the slab-level accumulator index.
    pub fn advance_acc_by<A: Into<Acc>>(&mut self, n: A) -> usize {
        let mut n = n.into();
        let start_pos = self.pos();
        let start = self.calculate_acc();
        let target: Acc = self.calculate_acc() + n;

        if start + n > self.total_acc() {
            self.nth(self.max - self.pos);
        } else {
            if self.slabs.weight().acc() <= target {
                n = self.reset_iter_to_acc(target);
            }

            if let Some(r) = self.run.as_mut() {
                if r.acc() > n {
                    let advance = n / r.agg();
                    self.pos += advance;
                    r.count -= advance;
                    return self.pos() - start_pos;
                }
                self.pos += r.count;
                n -= r.acc();
                r.count = 0;
            }
            let (advance, run) = self.slab.sub_advance_acc(n);
            self.run = run;
            self.pos += advance;
            self.check_pos();
        }
        self.pos() - start_pos
    }

    /// Captures the current iterator position as a [`ColumnDataIterState`] that can be
    /// stored and later restored via [`ColumnDataIterState::try_resume`].
    ///
    /// Resumption will fail if the underlying `ColumnData` is mutated between suspend and resume.
    pub fn suspend(&self) -> ColumnDataIterState<C> {
        ColumnDataIterState {
            counter: self.counter,
            pos: self.pos,
            max: self.max,
            run_state: self.slab.suspend(),
            slabs_state: self.slabs.suspend(),
            num_slabs: self.slabs.span_tree().map_or(0, |t| t.len()),
            run: self.run.as_ref().map(|r| r.count),
        }
    }
}

/// Serializable snapshot of a [`ColumnDataIter`] position.
///
/// Created by [`ColumnDataIter::suspend`] and restored by [`try_resume`](ColumnDataIterState::try_resume).
/// Resumption returns [`PackError::InvalidResume`] if the source `ColumnData` was mutated
/// after the snapshot was taken.
pub struct ColumnDataIterState<C: ColumnCursor> {
    counter: usize,
    pos: usize,
    max: usize,
    run_state: RunIterState<C>,
    slabs_state: SpanTreeIterState<C::SlabIndex>,
    num_slabs: usize,
    run: Option<usize>,
}

impl<C: ColumnCursor> ColumnDataIterState<C> {
    /// Attempts to restore the iterator position in `column`.
    ///
    /// Returns [`PackError::InvalidResume`] if `column` was mutated since [`ColumnDataIter::suspend`].
    pub fn try_resume<'a>(
        &self,
        column: &'a ColumnData<C>,
    ) -> Result<ColumnDataIter<'a, C>, PackError> {
        ColumnDataIter::try_resume(&column.slabs, self, column.counter)
    }
}

/// An iterator adapter over [`ColumnDataIter`] that emits the [`Acc`] value after each item.
///
/// Each `next()` call yields the cumulative accumulator *after* consuming the current item.
/// Created by [`ColumnDataIter::as_acc`].
#[derive(Debug, Default, Clone)]
pub struct ColAccIter<'a, C: ColumnCursor> {
    iter: ColumnDataIter<'a, C>,
}

impl<C: ColumnCursor> ColAccIter<'_, C> {
    pub fn shift_next(&mut self, range: Range<usize>) -> Option<<Self as Iterator>::Item> {
        let _ = self.iter.shift_next(range);
        let acc = self.acc();
        Some(acc)
    }

    fn acc(&self) -> Acc {
        self.iter.slabs.weight().acc() - self.iter.slab.acc_left() - self.iter.run_acc()
    }
}

impl<C: ColumnCursor> Iterator for ColAccIter<'_, C> {
    type Item = Acc;

    fn next(&mut self) -> Option<Self::Item> {
        let _ = self.iter.next()?;
        let acc = self.acc();
        Some(acc)
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        let _ = self.iter.nth(n)?;
        let acc = self.acc();
        Some(acc)
    }
}

/// An iterator adapter over [`ColumnDataIter`] that emits [`ColGroupItem`] values.
///
/// Each `next()` yields a `ColGroupItem { acc, pos, item }` where `acc` is the accumulator
/// *before* the item and `pos` is the item's index.
///
/// **Important:** `nth(n)` advances by accumulator amount `n` rather than by item count `n`.
/// This is intentional for Automerge's internal use but deviates from the `Iterator` contract.
/// TODO: consider splitting this into two distinct types.
///
/// Created by [`ColumnDataIter::with_acc`].
#[derive(Debug, Clone)]
pub struct ColGroupIter<'a, C: ColumnCursor> {
    iter: ColumnDataIter<'a, C>,
}

impl<'a, C: ColumnCursor> ColGroupIter<'a, C> {
    pub fn advance_by(&mut self, amount: usize) {
        self.iter.advance_by(amount)
    }

    pub fn shift_acc(&mut self, n: usize) -> Option<ColGroupItem<'a, C::Item>> {
        self.iter.advance_acc_by(n);
        self.next()
    }

    pub fn run_count(&self) -> usize {
        self.iter.run_count()
    }

    pub fn unwrap(self) -> ColumnDataIter<'a, C> {
        self.iter
    }

    pub fn acc(&self) -> Acc {
        self.iter.slabs.weight().acc() - self.iter.slab.acc_left() - self.iter.run_acc()
    }
}

/// A single item from a [`ColGroupIter`], bundling the item with its position and
/// the pre-item accumulator.
///
/// - `acc`: [`Acc`] value immediately *before* this item (sum of all prior `agg` values).
/// - `pos`: zero-based index of this item in the column.
/// - `item`: the value (`None` for null entries).
#[derive(Debug, PartialEq, Clone)]
pub struct ColGroupItem<'a, P: Packable + ?Sized> {
    pub acc: Acc,
    pub pos: usize,
    pub item: Option<Cow<'a, P>>,
}

impl<P: Packable + ?Sized> ColGroupItem<'_, P> {
    /// Returns the accumulator value *after* this item (`self.acc + agg(self.item)`).
    pub fn next_acc(&self) -> Acc {
        self.acc + P::maybe_agg(&self.item)
    }
}

impl<'a, C: ColumnCursor> Iterator for ColGroupIter<'a, C> {
    type Item = ColGroupItem<'a, C::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        let acc = self.iter.slabs.weight().acc() - self.iter.slab.acc_left() - self.iter.run_acc();
        let pos = self.iter.pos;
        let item = self.iter.next()?;
        Some(ColGroupItem { item, pos, acc })
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        if n > 0 {
            self.iter.nth(n - 1);
        }
        self.next()
    }
}

impl<'a, C: ColumnCursor> Iterator for ColumnDataIter<'a, C> {
    type Item = Option<Cow<'a, C::Item>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.max {
            return None;
        }
        let result = self.pop_element().or_else(|| {
            self.run = self.pop_run();
            self.slab.cursor.pop(self.run.as_mut()?)
        })?;
        self.pos += 1;
        Some(result)
    }

    fn nth(&mut self, mut n: usize) -> Option<Self::Item> {
        if self.pos >= self.max {
            return None;
        }
        if n == 0 {
            return self.next();
        }
        let mut overflow = false;
        if self.pos + n + 1 > self.max {
            n = self.max - self.pos - 1;
            overflow = true;
        }

        let target = self.pos() + n + 1;
        let result = if self.slabs.weight().pos() < target {
            self.reset_iter_to_pos(target - 1)?;
            self.next()
        } else if self.run_count() > n {
            self.pos += n + 1;
            let result = self.slab.cursor.pop_n(self.run.as_mut()?, n + 1);
            //if self.pos > self.max {
            //if overflow {
            //    None
            //} else {
            result
            //}
        } else {
            self.pos += self.run_count();
            let n = n - self.run_count();
            if n > 0 {
                self.pos += n;
                self.run = self.slab.sub_advance(n);
            } else {
                self.run = None;
            }
            self.next()
        };
        if !overflow {
            result
        } else {
            None
        }
    }
}

impl<C: ColumnCursor> ColumnData<C> {
    /// Iterates over the raw [`Run`]s in the column.
    ///
    /// Each `Run` has a `count` and an optional `value`. This gives lower-level access to the
    /// RLE structure than `iter()` — useful for re-encoding or bulk inspection.
    pub fn run_iter(&self) -> impl Iterator<Item = Run<'_, C::Item>> {
        self.slabs.iter().flat_map(|s| s.run_iter::<C>())
    }

    /// Decodes all items into a `Vec`. Primarily useful for testing and debugging.
    pub fn to_vec(&self) -> Vec<C::Export> {
        let mut result = vec![];
        C::export_splice(&mut result, 0..0, self.iter());
        result
    }

    /// Returns a forward iterator over all items in the column.
    ///
    /// The iterator decodes one slab at a time, carrying state across items within each slab
    /// for amortized O(1) per-item cost after an O(log n) initial seek.
    /// For a sub-range use [`iter_range`](ColumnData::iter_range).
    pub fn iter(&self) -> ColumnDataIter<'_, C> {
        ColumnDataIter::new(&self.slabs, 0, self.len, self.counter)
    }

    /// Returns the contiguous index range where `value` appears within `range`.
    ///
    /// Requires that the values in `range` are sorted. Uses B-tree binary search over slabs
    /// followed by a linear scan within the target slab.
    /// Returns an empty range at the found position if `value` is not present.
    ///
    /// For repeated lookups on the same iterator use [`ColumnDataIter::seek_to_value`].
    pub fn scope_to_value<B, R>(&self, value: Option<B>, range: R) -> Range<usize>
    where
        B: Borrow<C::Item> + Copy + Debug,
        R: RangeBounds<usize>,
        C::Item: Ord,
    {
        //let (start, end) = normalize_range(range);
        //let mut iter = self.iter_range(start..end);
        self.iter().seek_to_value(value, range)
    }

    /// Returns an iterator over items in `range`, clamped to the column's length.
    pub fn iter_range(&self, range: Range<usize>) -> ColumnDataIter<'_, C> {
        let start = std::cmp::min(self.len, range.start);
        let end = std::cmp::min(self.len, range.end);
        ColumnDataIter::new(&self.slabs, start, end, self.counter)
    }

    #[cfg(feature = "slow_path_assertions")]
    fn init_debug(mut self) -> Self {
        let mut debug = vec![];
        C::export_splice(&mut debug, 0..0, self.iter());
        self.debug = debug;
        self
    }

    pub(crate) fn init(len: usize, slabs: SlabTree<C::SlabIndex>) -> Self {
        debug_assert_eq!(len, slabs.iter().map(|s| s.len()).sum::<usize>());
        let col = ColumnData {
            counter: 0,
            len,
            slabs,
            _phantom: PhantomData,
            #[cfg(feature = "slow_path_assertions")]
            debug: vec![],
        };
        #[cfg(feature = "slow_path_assertions")]
        let col = col.init_debug();
        col
    }

    /// Creates a new, empty column.
    pub fn new() -> Self {
        ColumnData {
            len: 0,
            counter: 0,
            slabs: SlabTree::new2(Slab::default()),
            _phantom: PhantomData,
            #[cfg(feature = "slow_path_assertions")]
            debug: vec![],
        }
    }

    /// Serializes the column to a new `Vec<u8>`. See also [`save_to`](ColumnData::save_to).
    pub fn save(&self) -> Vec<u8> {
        let mut data = vec![];
        self.save_to(&mut data);
        data
    }

    /// Appends a single value to the end of the column.
    ///
    /// Returns the [`Acc`] value of the appended item. For bulk appends at the end,
    /// [`extend`](ColumnData::extend) is more efficient.
    pub fn push<'b, M>(&mut self, value: M) -> Acc
    where
        M: MaybePackable<'b, C::Item> + Clone,
        C::Item: 'b,
    {
        let index = self.len();
        self.splice(index, 0, [value])
    }

    /// Appends multiple values to the end of the column.
    ///
    /// Returns the total [`Acc`] contributed by the appended values.
    pub fn extend<'b, M, I>(&mut self, values: I) -> Acc
    where
        M: MaybePackable<'b, C::Item>,
        I: IntoIterator<Item = M>,
        C::Item: 'b,
    {
        let index = self.len();
        self.splice(index, 0, values)
    }

    /// Removes `del` items starting at `index` and inserts `values` in their place.
    ///
    /// This is the primary mutation method. It finds the slab containing `index` in O(log n),
    /// re-encodes the affected slab with the deletion/insertion applied, then replaces it in
    /// the B-tree. Unaffected slabs are not touched.
    ///
    /// Returns the accumulated [`Acc`] of the inserted values.
    ///
    /// Panics if `index > self.len()`.
    pub fn splice<'b, M, I>(&mut self, index: usize, del: usize, values: I) -> Acc
    where
        M: MaybePackable<'b, C::Item>,
        I: IntoIterator<Item = M>,
        C::Item: 'b,
    {
        assert!(index <= self.len);
        assert!(!self.slabs.is_empty());
        let values = values.into_iter();

        let mut values = values.peekable();
        if values.peek().is_none() && del == 0 {
            return Acc::new(); // really none
        }

        let cursor = self
            .slabs
            .get_where_or_last(|acc, next| index < acc.pos() + next.pos());

        let mut acc = cursor.weight.acc();

        debug_assert_eq!(
            self.iter()
                .map(|i| i.as_deref().map(<C::Item>::agg).unwrap_or_default())
                .sum::<Acc>(),
            self.acc()
        );

        let subindex = index - cursor.weight.pos();

        let mut result = C::splice(
            cursor.element,
            subindex,
            del,
            values,
            #[cfg(feature = "slow_path_assertions")]
            (&mut self.debug, index..(index + del)),
        );

        acc += result.group;
        C::compute_min_max(&mut result.slabs); // this should be handled by slabwriter.finish
        self.len = self.len + result.add - result.del;
        let post_slab_index = cursor.index + result.slabs.len();
        self.slabs
            .splice(cursor.index..(cursor.index + 1), result.slabs);
        self.counter += 1;

        while result.overflow > 0 {
            if let Some(post_slab) = self.slabs.get(post_slab_index) {
                if post_slab.len() <= result.overflow {
                    result.overflow -= post_slab.len();
                    self.len -= post_slab.len();
                    self.slabs.remove(post_slab_index);
                } else {
                    let mut r = C::splice::<_, M>(
                        post_slab,
                        0,
                        result.overflow,
                        [].into_iter(),
                        #[cfg(feature = "slow_path_assertions")]
                        (&mut self.debug, 0..0),
                    );
                    self.len -= r.del;
                    C::compute_min_max(&mut r.slabs);
                    self.slabs
                        .splice(post_slab_index..(post_slab_index + 1), r.slabs);
                    break;
                }
            }
        }
        if self.slabs.is_empty() {
            assert!(self.len == 0);
            self.slabs.push(Slab::default()); // need a blank empty slab
        }

        debug_assert_eq!(
            self.iter()
                .map(|i| i.as_deref().map(<C::Item>::agg).unwrap_or_default())
                .sum::<Acc>(),
            self.acc()
        );

        #[cfg(feature = "slow_path_assertions")]
        if self.debug != self.to_vec() {
            let col = self.to_vec();
            assert_eq!(self.debug.len(), col.len());
            for (i, dbg) in col.iter().enumerate() {
                if dbg != &col[i] {
                    panic!("index={} {:?} vs {:?}", i, dbg, col[i]);
                }
            }
            panic!()
        }
        acc
    }

    /// If the column is currently empty, fills it with `len` null values and returns `true`.
    /// If the column already has items, returns `false` without modifying it.
    pub fn fill_if_empty(&mut self, len: usize) -> bool {
        if self.len == 0 && len > 0 {
            *self = Self::init_empty(len);
            true
        } else {
            false
        }
    }

    /// Creates a column of `len` null values.
    pub fn init_empty(len: usize) -> Self {
        let new_slab = C::init_empty(len);
        let mut slabs = SlabTree::default();
        slabs.push(new_slab);
        assert!(!slabs.is_empty());
        ColumnData::init(len, slabs)
    }

    /// Deserializes `data`, or returns a column of `len` nulls if `data` is empty.
    ///
    /// Returns [`PackError::InvalidLength`] if the decoded column has a different length
    /// than `len`.
    pub fn load_unless_empty(data: &[u8], len: usize) -> Result<Self, PackError> {
        if data.is_empty() {
            Ok(ColumnData::init_empty(len))
        } else {
            let c = ColumnData::load(data)?;
            if c.len() == len {
                Ok(c)
            } else {
                Err(PackError::InvalidLength(c.len(), len))
            }
        }
    }

    /// Like [`load_unless_empty`](ColumnData::load_unless_empty) but also validates each value
    /// with `test`. If `test` returns `Some(msg)`, decoding fails with
    /// [`PackError::InvalidValue`].
    pub fn load_with_unless_empty<F>(data: &[u8], len: usize, test: &F) -> Result<Self, PackError>
    where
        F: Fn(Option<&C::Item>) -> Option<String>,
    {
        if data.is_empty() {
            Ok(ColumnData::init_empty(len))
        } else {
            let c = ColumnData::load_with(data, test)?;
            if c.len() == len {
                Ok(c)
            } else {
                Err(PackError::InvalidLength(c.len(), len))
            }
        }
    }

    /// Deserializes a column from bytes produced by [`save`](ColumnData::save) /
    /// [`save_to`](ColumnData::save_to).
    ///
    /// Returns a [`PackError`] if the bytes are malformed or use the wrong encoding.
    pub fn load(data: &[u8]) -> Result<Self, PackError> {
        Self::load_with(data, &|_| None)
    }

    /// Like [`load`](ColumnData::load) but validates each decoded value with `test`.
    /// If `test` returns `Some(msg)`, decoding fails with [`PackError::InvalidValue`].
    pub fn load_with<F>(data: &[u8], test: &F) -> Result<Self, PackError>
    where
        F: Fn(Option<&C::Item>) -> Option<String>,
    {
        let col = C::load_with(data, test)?;
        debug_assert_eq!(
            col.iter()
                .map(|i| i.as_deref().map(<C::Item>::agg).unwrap_or_default())
                .sum::<Acc>(),
            col.acc()
        );
        Ok(col)
    }

    /// Returns the number of items in the column (including nulls).
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns the total accumulated [`Acc`] for the entire column
    /// (sum of `agg(item)` for every non-null item).
    pub fn acc(&self) -> Acc {
        self.slabs.weight().map(|w| w.acc()).unwrap_or_default()
    }
}

impl<C: ColumnCursor> ColumnData<C>
where
    C::SlabIndex: HasMinMax,
{
    /// Returns an iterator over the indices of items whose value falls within `range`.
    ///
    /// Uses slab-level min/max metadata to skip slabs that cannot contain matching values,
    /// making this efficient for sparse matches. Requires that the cursor type supports
    /// min/max tracking ([`HasMinMax`]).
    pub fn find_by_range(&self, range: Range<usize>) -> impl Iterator<Item = usize> + '_ {
        let start = range.start;
        let end = range.end;
        self.slabs
            .iter_where(move |_, s| s.intersects(start..end))
            .flat_map(move |cursor| {
                let pos = cursor.weight.pos();
                cursor
                    .element
                    .run_iter::<C>()
                    .containing_range(pos, start..end)
            })
    }

    /// Returns an iterator over the indices of items whose [`Agg`] value equals `agg`.
    ///
    /// Uses slab-level min/max metadata to skip non-matching slabs. Requires that the cursor
    /// type supports min/max tracking ([`HasMinMax`]).
    pub fn find_by_value<A: Into<Agg>>(&self, agg: A) -> impl Iterator<Item = usize> + '_ {
        let agg = agg.into();

        self.slabs
            .iter_where(move |_, s| agg.is_some() && agg >= s.min() && agg <= s.max())
            .flat_map(move |cursor| {
                let pos = cursor.weight.pos();
                cursor.element.run_iter::<C>().containing_agg(pos, agg)
            })
    }
}

pub(crate) fn normalize_range<R: RangeBounds<usize>>(range: R) -> (usize, usize) {
    let start = match range.start_bound() {
        Bound::Unbounded => usize::MIN,
        Bound::Included(n) => *n,
        Bound::Excluded(n) => *n - 1,
    };

    let end = match range.end_bound() {
        Bound::Unbounded => usize::MAX,
        Bound::Included(n) => *n + 1,
        Bound::Excluded(n) => *n,
    };
    (start, end)
}

impl<'a, C, M> From<Vec<M>> for ColumnData<C>
where
    C: ColumnCursor,
    M: MaybePackable<'a, C::Item>,
    C::Item: 'a,
{
    fn from(i: Vec<M>) -> Self {
        i.into_iter().collect()
    }
}

impl<'a, C, M> FromIterator<M> for ColumnData<C>
where
    C: ColumnCursor,
    M: MaybePackable<'a, C::Item>,
    C::Item: 'a,
{
    fn from_iter<I: IntoIterator<Item = M>>(iter: I) -> Self {
        let mut encoder = Encoder::new(false);
        for item in iter {
            encoder.append_item(item.maybe_packable());
        }
        encoder.into_column_data()
    }
}

fn _cmp<A, B, C>(a: Option<A>, b: &Option<B>) -> Ordering
where
    A: Borrow<C>,
    B: Borrow<C>,
    C: Ord + ?Sized,
{
    match (a, b) {
        (Some(a), Some(b)) => a.borrow().cmp(b.borrow()),
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::super::boolean::BooleanCursor;
    use super::super::delta::{DeltaCursor, DeltaCursorInternal};
    use super::super::rle::{ByteCursor, IntCursor, RleCursor, StrCursor, UIntCursor};
    use super::super::test::ColExport;
    use super::*;
    use rand::prelude::*;
    use rand::rngs::SmallRng;
    use std::cmp::{max, min};

    const FUZZ_SIZE: u32 = 1_000;

    fn test_splice<'a, C: ColumnCursor, E>(
        vec: &'a mut Vec<E>,
        col: &'a mut ColumnData<C>,
        index: usize,
        values: Vec<E>,
    ) where
        E: MaybePackable<'a, C::Item> + std::fmt::Debug + std::cmp::PartialEq<C::Export> + Clone,
    {
        test_splice_del(vec, col, index, 0, values);
    }

    fn test_splice_del<'a, C: ColumnCursor, E>(
        vec: &'a mut Vec<E>,
        col: &'a mut ColumnData<C>,
        index: usize,
        del: usize,
        values: Vec<E>,
    ) where
        E: MaybePackable<'a, C::Item> + std::fmt::Debug + std::cmp::PartialEq<C::Export> + Clone,
    {
        vec.splice(index..index + del, values.clone());
        col.splice(index, del, values);
        for slab in &col.slabs {
            let (_, c) = C::seek(slab.len(), slab);
            assert_eq!(c.min(), slab.min());
            assert_eq!(c.max(), slab.max());
        }
        assert_eq!(vec, &col.to_vec());
    }

    fn test_advance_by<'a, C: ColumnCursor>(
        rng: &mut SmallRng,
        data: &'a [C::Export],
        col: &'a mut ColumnData<C>,
    ) {
        let mut advanced_by = 0;
        let mut iter = col.iter();
        while advanced_by < data.len() - 1 {
            let advance_by = rng.random_range(1..(data.len() - advanced_by));
            iter.advance_by(advance_by);
            let expected = data[advance_by + advanced_by..].to_vec();
            let actual = iter.clone().to_vec();
            assert_eq!(expected, actual);
            advanced_by += advance_by;
        }
    }

    #[test]
    fn column_data_breaking_literal_runs_in_int_column() {
        let numbers = vec![1, 2, 3];
        let mut start = ColumnData::<UIntCursor>::new();
        start.splice(0, 0, numbers);
        assert_eq!(
            start.test_dump(),
            vec![vec![ColExport::LitRun(vec![1, 2, 3])]]
        );
        let mut col = start.clone();
        col.splice(2, 0, vec![3, 3, 3]);
        assert_eq!(
            col.test_dump(),
            vec![vec![ColExport::LitRun(vec![1, 2]), ColExport::Run(4, 3)]]
        );
        let mut col = start.clone();
        col.splice(3, 0, vec![3, 3, 3]);
        assert_eq!(
            col.test_dump(),
            vec![vec![ColExport::LitRun(vec![1, 2]), ColExport::Run(4, 3)]]
        );
        let mut col = start.clone();
        col.splice(1, 0, vec![2, 2]);
        assert_eq!(
            col.test_dump(),
            vec![vec![
                ColExport::LitRun(vec![1]),
                ColExport::Run(3, 2),
                ColExport::LitRun(vec![3]),
            ]]
        );
        let mut col = start.clone();
        col.splice(2, 0, vec![2, 2]);
        assert_eq!(
            col.test_dump(),
            vec![vec![
                ColExport::LitRun(vec![1]),
                ColExport::Run(3, 2),
                ColExport::LitRun(vec![3]),
            ]]
        );
        let mut col = start.clone();
        col.splice(0, 0, vec![1, 1]);
        assert_eq!(
            col.test_dump(),
            vec![vec![ColExport::Run(3, 1), ColExport::LitRun(vec![2, 3]),]]
        );
        let mut col = start.clone();
        col.splice(1, 0, vec![1, 1]);
        assert_eq!(
            col.test_dump(),
            vec![vec![ColExport::Run(3, 1), ColExport::LitRun(vec![2, 3]),]]
        );
    }

    #[test]
    fn column_data_breaking_runs_in_int_column() {
        let numbers = vec![2, 2, 2];
        let mut start = ColumnData::<UIntCursor>::new();
        start.splice(0, 0, numbers);
        assert_eq!(start.test_dump(), vec![vec![ColExport::Run(3, 2)]]);
        let mut col = start.clone();
        col.splice(1, 0, vec![3, 3, 3]);
        assert_eq!(
            col.test_dump(),
            vec![vec![
                ColExport::LitRun(vec![2]),
                ColExport::Run(3, 3),
                ColExport::Run(2, 2),
            ]]
        );
        let mut col = start.clone();
        col.splice(2, 0, vec![3, 3, 3]);
        assert_eq!(
            col.test_dump(),
            vec![vec![
                ColExport::Run(2, 2),
                ColExport::Run(3, 3),
                ColExport::LitRun(vec![2]),
            ]]
        );
        let mut col = start.clone();
        col.splice(0, 0, vec![3, 3, 3]);
        assert_eq!(
            col.test_dump(),
            vec![vec![ColExport::Run(3, 3), ColExport::Run(3, 2),]]
        );
        let mut col = start.clone();
        col.splice(3, 0, vec![3, 3, 3]);
        assert_eq!(
            col.test_dump(),
            vec![vec![ColExport::Run(3, 2), ColExport::Run(3, 3),]]
        );
    }

    #[test]
    fn column_data_breaking_null_runs_in_int_column() {
        let numbers = vec![None, None, Some(2), Some(2), None, None, None];
        let mut start = ColumnData::<UIntCursor>::new();
        start.splice(0, 0, numbers);
        assert_eq!(
            start.test_dump(),
            vec![vec![
                ColExport::Null(2),
                ColExport::Run(2, 2),
                ColExport::Null(3)
            ]]
        );
        assert_eq!(
            start.to_vec(),
            vec![None, None, Some(2), Some(2), None, None, None]
        );
        let mut col = start.clone();
        col.splice(2, 0, vec![None, None, Some(2), Some(2)]);
        assert_eq!(
            col.test_dump(),
            vec![vec![
                ColExport::Null(4),
                ColExport::Run(4, 2),
                ColExport::Null(3)
            ]]
        );
        assert_eq!(col.len, 11);
        assert_eq!(col.slabs.iter().map(|s| s.len()).sum::<usize>(), 11);
        col.splice(8, 0, vec![Some(2), Some(2), None, None]);
        assert_eq!(
            col.test_dump(),
            vec![vec![
                ColExport::Null(4),
                ColExport::Run(6, 2),
                ColExport::Null(5)
            ]]
        );
        col.splice(4, 0, vec![None, Some(2), Some(3)]);
        assert_eq!(
            col.test_dump(),
            vec![vec![
                ColExport::Null(5),
                ColExport::LitRun(vec![2, 3]),
                ColExport::Run(6, 2),
                ColExport::Null(5)
            ]]
        );
        col.splice(2, 0, vec![4]);
        assert_eq!(
            col.test_dump(),
            vec![vec![
                ColExport::Null(2),
                ColExport::LitRun(vec![4]),
                ColExport::Null(3),
                ColExport::LitRun(vec![2, 3]),
                ColExport::Run(6, 2),
                ColExport::Null(5)
            ]]
        );
        col.splice(6, 0, vec![None, None, Some(2), Some(2)]);
        assert_eq!(
            col.test_dump(),
            vec![vec![
                ColExport::Null(2),
                ColExport::LitRun(vec![4]),
                ColExport::Null(5),
                ColExport::Run(3, 2),
                ColExport::LitRun(vec![3]),
                ColExport::Run(6, 2),
                ColExport::Null(5)
            ]]
        );
        col.splice(
            12,
            0,
            vec![Some(3), Some(3), None, Some(7), Some(8), Some(9), Some(2)],
        );
        assert_eq!(
            col.test_dump(),
            vec![vec![
                ColExport::Null(2),
                ColExport::LitRun(vec![4]),
                ColExport::Null(5),
                ColExport::Run(3, 2),
                ColExport::Run(3, 3),
                ColExport::Null(1),
                ColExport::LitRun(vec![7, 8, 9]),
                ColExport::Run(7, 2),
                ColExport::Null(5)
            ]]
        );
        col.splice(15, 0, vec![5, 6]);
        assert_eq!(
            col.test_dump(),
            vec![vec![
                ColExport::Null(2),
                ColExport::LitRun(vec![4]),
                ColExport::Null(5),
                ColExport::Run(3, 2),
                ColExport::Run(3, 3),
                ColExport::Null(1),
                ColExport::LitRun(vec![5, 6, 7, 8, 9]),
                ColExport::Run(7, 2),
                ColExport::Null(5)
            ]]
        );
        assert_eq!(col.len, col.iter().count());
    }

    #[test]
    fn column_data_strings() {
        let strings = vec!["one", "two", "three"];
        let mut start = ColumnData::<StrCursor>::new();
        start.splice(0, 0, strings);
        assert_eq!(
            start.test_dump(),
            vec![vec![ColExport::litrun(vec!["one", "two", "three"])]]
        );
        let mut col = start.clone();
        col.splice(1, 0, vec![None, None, Some("two"), Some("two")]);
        assert_eq!(
            col.test_dump(),
            vec![vec![
                ColExport::litrun(vec!["one"]),
                ColExport::Null(2),
                ColExport::run(3, "two"),
                ColExport::litrun(vec!["three"]),
            ]]
        );
        col.splice(0, 0, vec![None, None, Some("three"), Some("one")]);
        assert_eq!(
            col.test_dump(),
            vec![vec![
                ColExport::Null(2),
                ColExport::litrun(vec!["three"]),
                ColExport::run(2, "one"),
                ColExport::Null(2),
                ColExport::run(3, "two"),
                ColExport::litrun(vec!["three"]),
            ]]
        );
    }

    #[test]
    fn column_data_bytes() {
        let bytes = vec![vec![1, 1, 1], vec![2, 2, 2], vec![3, 3, 3]];
        let mut start = ColumnData::<ByteCursor>::new();
        start.splice(0, 0, bytes);
        assert_eq!(
            start.test_dump(),
            vec![vec![ColExport::litrun(vec![
                vec![1, 1, 1],
                vec![2, 2, 2],
                vec![3, 3, 3]
            ])]]
        );
        let mut col = start.clone();
        col.splice(
            1,
            0,
            vec![None, None, Some(vec![2, 2, 2]), Some(vec![2, 2, 2])],
        );
        assert_eq!(
            col.test_dump(),
            vec![vec![
                ColExport::litrun(vec![vec![1, 1, 1]]),
                ColExport::Null(2),
                ColExport::run(3, vec![2, 2, 2]),
                ColExport::litrun(vec![vec![3, 3, 3]]),
            ]]
        );
        col.splice(
            0,
            0,
            vec![None, None, Some(vec![3, 3, 3]), Some(vec![1, 1, 1])],
        );
        assert_eq!(
            col.test_dump(),
            vec![vec![
                ColExport::Null(2),
                ColExport::litrun(vec![vec![3, 3, 3]]),
                ColExport::run(2, vec![1, 1, 1]),
                ColExport::Null(2),
                ColExport::run(3, vec![2, 2, 2]),
                ColExport::litrun(vec![vec![3, 3, 3]]),
            ]]
        );
    }

    #[test]
    fn column_data_delta() {
        let numbers = vec![1, 2, 3, 4, 5, 6, 6, 6, 6, 6, 7, 8, 9];
        let mut start = ColumnData::<DeltaCursor>::new();
        start.splice(0, 0, numbers.clone());
        assert_eq!(
            start.test_dump(),
            vec![vec![
                ColExport::Run(6, 1),
                ColExport::Run(4, 0),
                ColExport::Run(3, 1),
            ]]
        );
        let numbers1 = numbers.iter().map(|i| Some(*i)).collect::<Vec<_>>();
        let numbers2 = start.to_vec();
        assert_eq!(numbers1, numbers2);
        let mut col = start.clone();
        col.splice(1, 0, vec![2]);
        assert_eq!(
            col.test_dump(),
            vec![vec![
                ColExport::Run(2, 1),
                ColExport::LitRun(vec![0]),
                ColExport::Run(4, 1),
                ColExport::Run(4, 0),
                ColExport::Run(3, 1),
            ]]
        );
        col.splice(0, 0, vec![0]);
        assert_eq!(
            col.test_dump(),
            vec![vec![
                ColExport::LitRun(vec![0]),
                ColExport::Run(2, 1),
                ColExport::LitRun(vec![0]),
                ColExport::Run(4, 1),
                ColExport::Run(4, 0),
                ColExport::Run(3, 1),
            ]]
        );
    }

    /// v0 splice panics when del > 1 and the delete range spans a slab boundary.
    /// Build a multi-slab column (B=64) and delete 5 items near the boundary.
    #[test]
    fn splice_cross_slab_delete() {
        let mut col: ColumnData<UIntCursor> = (0..200).map(|i| i as u64).collect();
        // Position 62: last few items of first slab (64 items). del=5 crosses into the next slab.
        col.splice(62, 5, std::iter::empty::<u64>());
        assert_eq!(col.len(), 195);
    }

    #[test]
    fn column_data_big_delete() {
        let mut col: ColumnData<IntCursor> = [
            1, 2, 3, 4, 5, 6, 5, 5, 5, 4, 3, 2, 2, 2, 1, 1, 1, 1, 2, 3, 4, 4, 4,
        ]
        .into_iter()
        .collect();
        let len = col.len();
        let v: Vec<i64> = vec![];
        col.splice(0, 0, v.clone());
        col.splice(0, len, v);
    }

    // TODO - would be nice if you printed the seed on failure
    // so we could re-seed if we ever see one of these fail
    trait TestRand: Clone {
        fn index(len: usize, rng: &mut SmallRng) -> usize {
            match len {
                0 => 0,
                _ => (rng.random::<u32>() as usize) % len,
            }
        }
        fn null() -> Self;
        fn rand(rng: &mut SmallRng) -> Self;
        fn maybe_rand(rng: &mut SmallRng) -> Self {
            match rng.random::<u64>() as i64 % 10 {
                0 => Self::null(),
                _ => Self::rand(rng),
            }
        }
        fn plus(&self, index: usize) -> Self;
        fn rand_vec(rng: &mut SmallRng) -> Vec<Self>
        where
            Self: Sized,
        {
            let mut result = vec![];
            let len = rng.random::<u32>() % 40 + 1;
            for _ in 0..len {
                if rng.random::<i64>() % 3 == 0 {
                    result.push(Self::null())
                } else {
                    result.push(Self::rand(rng))
                }
            }
            result
        }
    }

    impl TestRand for Option<i64> {
        fn null() -> Option<i64> {
            None
        }

        fn rand(rng: &mut SmallRng) -> Option<i64> {
            Some((rng.random::<u64>() % 3) as i64)
        }

        fn plus(&self, index: usize) -> Option<i64> {
            self.map(|i| i + index as i64)
        }
    }

    impl TestRand for bool {
        fn null() -> bool {
            false
        }
        fn rand(rng: &mut SmallRng) -> bool {
            rng.random::<bool>()
        }
        fn plus(&self, _index: usize) -> bool {
            true
        }
    }

    impl TestRand for Option<u64> {
        fn null() -> Option<u64> {
            None
        }
        fn rand(rng: &mut SmallRng) -> Option<u64> {
            Some(rng.random::<u64>() % 3)
        }
        fn plus(&self, index: usize) -> Option<u64> {
            self.map(|i| i + index as u64)
        }
    }

    impl TestRand for Option<String> {
        fn null() -> Option<String> {
            None
        }
        fn rand(rng: &mut SmallRng) -> Option<String> {
            Some(format!("0x{:X}", rng.random::<u32>()).to_owned())
        }
        fn plus(&self, index: usize) -> Option<String> {
            self.as_ref().map(|s| format!("{}/{}", s, index).to_owned())
        }
    }

    fn make_rng() -> SmallRng {
        let seed = rand::random::<u64>();
        //let seed = 16821371807298729682;
        //let seed = 14189760879853346850;
        log!("SEED: {}", seed);
        SmallRng::seed_from_u64(seed)
    }

    /// Generate a random insert-only splice (del=0).
    fn generate_splice<T: TestRand>(len: usize, rng: &mut SmallRng) -> (usize, Vec<T>) {
        let index = T::index(len, rng);
        let patch = match rng.random::<u32>() % 4 {
            0 => vec![T::null(), T::null(), T::null()],
            1 => {
                let n = T::rand(rng);
                vec![n.clone(), n.clone(), n]
            }
            2 => {
                let n = T::rand(rng);
                let step = (rng.random::<u32>() as usize) % 4;
                vec![n.clone(), n.plus(step), n.plus(step * 2)]
            }
            _ => T::rand_vec(rng),
        };
        (index, patch)
    }

    /// Generate a random splice that may insert, delete, replace, or do a mix.
    /// Returns (index, del, values).
    fn generate_splice_del<T: TestRand>(len: usize, rng: &mut SmallRng) -> (usize, usize, Vec<T>) {
        if len == 0 {
            // Can only insert when empty.
            let (index, values) = generate_splice(len, rng);
            return (index, 0, values);
        }
        match rng.random::<u32>() % 5 {
            // Insert only (del=0).
            0 => {
                let (index, values) = generate_splice(len, rng);
                (index, 0, values)
            }
            // Delete only (no new values).
            1 => {
                let max_del = min(len, 10);
                let del = (rng.random::<u32>() as usize % max_del) + 1;
                let index = rng.random::<u32>() as usize % (len - del + 1);
                (index, del, vec![])
            }
            // Replace same count.
            2 => {
                let max_del = min(len, 10);
                let del = (rng.random::<u32>() as usize % max_del) + 1;
                let index = rng.random::<u32>() as usize % (len - del + 1);
                let values = (0..del).map(|_| T::maybe_rand(rng)).collect();
                (index, del, values)
            }
            // Replace with fewer (shrink).
            3 => {
                let max_del = min(len, 10);
                let del = (rng.random::<u32>() as usize % max_del) + 1;
                let index = rng.random::<u32>() as usize % (len - del + 1);
                let ins = rng.random::<u32>() as usize % del;
                let values = (0..ins).map(|_| T::maybe_rand(rng)).collect();
                (index, del, values)
            }
            // Replace with more (grow).
            _ => {
                let max_del = min(len, 5);
                let del = (rng.random::<u32>() as usize % max_del) + 1;
                let index = rng.random::<u32>() as usize % (len - del + 1);
                let ins = del + (rng.random::<u32>() as usize % 10) + 1;
                let values = (0..ins).map(|_| T::maybe_rand(rng)).collect();
                (index, del, values)
            }
        }
    }

    #[test]
    fn column_data_fuzz_test_int() {
        let mut data: Vec<Option<u64>> = vec![];
        let mut col = ColumnData::<RleCursor<64, u64>>::new();
        let mut rng = make_rng();
        for _ in 0..FUZZ_SIZE {
            let (index, values) = generate_splice(data.len(), &mut rng);
            test_splice(&mut data, &mut col, index, values);
        }
        let export = ColumnData::<RleCursor<64, u64>>::load(&col.save()).unwrap();
        assert_eq!(col.to_vec(), export.to_vec());

        // Phase 2: mixed insert/delete/replace.
        for _ in 0..FUZZ_SIZE {
            let (index, del, values) = generate_splice_del(data.len(), &mut rng);
            test_splice_del(&mut data, &mut col, index, del, values);
        }
        let export = ColumnData::<RleCursor<64, u64>>::load(&col.save()).unwrap();
        assert_eq!(col.to_vec(), export.to_vec());
    }

    #[test]
    fn column_data_fuzz_test_suspend_resume() {
        let mut rng = make_rng();
        test_suspend_resume::<UIntCursor, Option<u64>>(&mut rng);
        test_suspend_resume::<IntCursor, Option<i64>>(&mut rng);
        test_suspend_resume::<DeltaCursor, Option<i64>>(&mut rng);
        test_suspend_resume::<StrCursor, Option<String>>(&mut rng);
        test_suspend_resume::<BooleanCursor, bool>(&mut rng);
    }

    fn test_suspend_resume<'a, C: ColumnCursor, M: MaybePackable<'a, C::Item> + TestRand>(
        rng: &mut SmallRng,
    ) where
        C::Item: 'a,
    {
        const LEN: usize = 100;
        let col: ColumnData<C> = ((0..LEN).map(|_| M::maybe_rand(rng))).collect();
        for _ in 0..1000 {
            let index = M::index(LEN, rng);
            let mut iter1 = col.iter();
            iter1.nth(index);
            let cursor = iter1.suspend();
            let iter2 = cursor.try_resume(&col).unwrap();
            let v1: Vec<_> = iter1.collect();
            let v2: Vec<_> = iter2.collect();
            assert_eq!(v1, v2);
        }
    }

    #[test]
    fn column_data_fuzz_test_suspend_resume_error() {
        let mut rng = make_rng();

        const LEN: usize = 100;
        let mut col: ColumnData<UIntCursor> =
            ((0..LEN).map(|_| Option::<u64>::maybe_rand(&mut rng))).collect();
        for _ in 0..10 {
            let index1 = Option::<u64>::index(LEN, &mut rng);
            let index2 = Option::<u64>::index(LEN, &mut rng);
            let mut iter1 = col.iter();
            iter1.nth(index1);
            let cursor = iter1.suspend();

            col.splice(index2, 0, vec![Option::<u64>::maybe_rand(&mut rng)]);

            assert!(cursor.try_resume(&col).is_err());
        }
    }

    #[test]
    fn column_data_fuzz_test_advance_by_int() {
        let mut rng = make_rng();
        for _ in 0..FUZZ_SIZE {
            let mut col = ColumnData::<UIntCursor>::new();
            let values = Option::<u64>::rand_vec(&mut rng);
            col.splice(0, 0, values.clone());
            test_advance_by(&mut rng, &values, &mut col);
        }
    }

    #[test]
    fn column_data_str_fuzz_test() {
        let mut data: Vec<Option<String>> = vec![];
        let mut col = ColumnData::<RleCursor<64, str>>::new();
        let mut rng = make_rng();
        for _ in 0..FUZZ_SIZE {
            let (index, values) = generate_splice(data.len(), &mut rng);
            test_splice(&mut data, &mut col, index, values);
        }
        let copy: ColumnData<StrCursor> = ColumnData::load(&col.save()).unwrap();
        assert_eq!(col.to_vec(), copy.to_vec());

        // Phase 2: mixed insert/delete/replace.
        for _ in 0..FUZZ_SIZE {
            let (index, del, values) = generate_splice_del(data.len(), &mut rng);
            test_splice_del(&mut data, &mut col, index, del, values);
        }
        let copy: ColumnData<StrCursor> = ColumnData::load(&col.save()).unwrap();
        assert_eq!(col.to_vec(), copy.to_vec());
    }

    #[test]
    fn column_data_fuzz_test_advance_by_str() {
        let mut rng = make_rng();
        for _ in 0..FUZZ_SIZE {
            let mut col = ColumnData::<StrCursor>::new();
            let values = Option::<String>::rand_vec(&mut rng);
            col.splice(0, 0, values.clone());
            test_advance_by(&mut rng, &values, &mut col);
        }
    }

    #[test]
    fn column_data_fuzz_test_delta() {
        let mut data: Vec<Option<i64>> = vec![];
        let mut col = ColumnData::<DeltaCursorInternal<8>>::new();
        let mut rng = make_rng();
        for _ in 0..FUZZ_SIZE {
            let (index, values) = generate_splice(data.len(), &mut rng);
            test_splice(&mut data, &mut col, index, values);
        }
        let copy: ColumnData<DeltaCursor> = ColumnData::load(&col.save()).unwrap();
        assert_eq!(col.to_vec(), copy.to_vec());

        // Phase 2: mixed insert/delete/replace.
        for _ in 0..FUZZ_SIZE {
            let (index, del, values) = generate_splice_del(data.len(), &mut rng);
            test_splice_del(&mut data, &mut col, index, del, values);
        }
        let copy: ColumnData<DeltaCursor> = ColumnData::load(&col.save()).unwrap();
        assert_eq!(col.to_vec(), copy.to_vec());
    }

    #[test]
    fn column_data_fuzz_test_advance_by_delta() {
        let mut rng = make_rng();
        for _ in 0..100 {
            let mut col = ColumnData::<DeltaCursor>::new();
            let values = Option::<i64>::rand_vec(&mut rng);
            col.splice(0, 0, values.clone());
            test_advance_by(&mut rng, &values, &mut col);
        }
    }

    #[test]
    fn column_data_test_boolean() {
        let data: Vec<bool> = vec![true, true, true];
        let mut col = ColumnData::<BooleanCursor>::new();
        col.splice(0, 0, data.clone());
        assert_eq!(col.test_dump(), vec![vec![ColExport::Run(3, true)]]);
        col.splice(0, 0, vec![false, false, false]);
        assert_eq!(
            col.test_dump(),
            vec![vec![ColExport::Run(3, false), ColExport::Run(3, true)]]
        );
        col.splice(6, 0, vec![false, false, false]);
        assert_eq!(
            col.test_dump(),
            vec![vec![
                ColExport::Run(3, false),
                ColExport::Run(3, true),
                ColExport::Run(3, false),
            ]]
        );
        col.splice(9, 0, vec![true, true, true]);
        assert_eq!(
            col.test_dump(),
            vec![vec![
                ColExport::Run(3, false),
                ColExport::Run(3, true),
                ColExport::Run(3, false),
                ColExport::Run(3, true),
            ]]
        );
        col.splice(0, 0, vec![true, true, true]);
        assert_eq!(
            col.test_dump(),
            vec![vec![
                ColExport::Run(3, true),
                ColExport::Run(3, false),
                ColExport::Run(3, true),
                ColExport::Run(3, false),
                ColExport::Run(3, true),
            ]]
        );
        col.splice(1, 0, vec![false, false, false]);
        assert_eq!(
            col.test_dump(),
            vec![vec![
                ColExport::Run(1, true),
                ColExport::Run(3, false),
                ColExport::Run(2, true),
                ColExport::Run(3, false),
                ColExport::Run(3, true),
                ColExport::Run(3, false),
                ColExport::Run(3, true),
            ]]
        );
    }

    #[test]
    fn column_data_fuzz_test_boolean() {
        let mut data: Vec<bool> = vec![];
        let mut col = ColumnData::<BooleanCursor>::new();
        let mut rng = make_rng();
        for _ in 0..FUZZ_SIZE {
            let (index, values) = generate_splice(data.len(), &mut rng);
            test_splice(&mut data, &mut col, index, values);
        }
        let export = ColumnData::<BooleanCursor>::load(&col.save()).unwrap();
        assert_eq!(col.to_vec(), export.to_vec());

        // Phase 2: mixed insert/delete/replace.
        for _ in 0..FUZZ_SIZE {
            let (index, del, values) = generate_splice_del(data.len(), &mut rng);
            test_splice_del(&mut data, &mut col, index, del, values);
        }
        let export = ColumnData::<BooleanCursor>::load(&col.save()).unwrap();
        assert_eq!(col.to_vec(), export.to_vec());
    }

    #[test]
    fn column_data_fuzz_test_advance_by_boolean() {
        let mut rng = make_rng();
        for _ in 0..FUZZ_SIZE {
            let mut col = ColumnData::<BooleanCursor>::new();
            let values = bool::rand_vec(&mut rng);
            col.splice(0, 0, values.clone());
            test_advance_by(&mut rng, &values, &mut col);
        }
    }

    #[test]
    fn column_data_scope_to_value() {
        let data = vec![
            2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 6, 6, 6, 8, 9, 9,
        ];
        let mut col = ColumnData::<RleCursor<4, u64>>::new();
        col.splice(0, 0, data);
        let range = col.scope_to_value(Some(4), ..);
        assert_eq!(range, 7..15);

        let range = col.scope_to_value(Some(4), ..11);
        assert_eq!(range, 7..11);
        let range = col.scope_to_value(Some(4), ..8);
        assert_eq!(range, 7..8);
        let range = col.scope_to_value(Some(4), 0..1);
        assert_eq!(range, 1..1);
        let range = col.scope_to_value(Some(4), 8..9);
        assert_eq!(range, 8..9);
        let range = col.scope_to_value(Some(4), 9..);
        assert_eq!(range, 9..15);
        let range = col.scope_to_value(Some(4), 14..16);
        assert_eq!(range, 14..15);

        let range = col.scope_to_value(Some(2), ..);
        assert_eq!(range, 0..3);
        let range = col.scope_to_value(Some(7), ..);
        assert_eq!(range, 22..22);
        let range = col.scope_to_value(Some(8), ..);
        assert_eq!(range, 22..23);
        let range = col.scope_to_value(Some(9), ..);
        assert_eq!(range, 23..25);
    }

    #[test]
    fn splice_on_boundary() {
        let data = vec![1, 2, 3, 4, 5, 6];
        let mut col = ColumnData::<RleCursor<4, u64>>::new();
        col.splice(0, 0, data);
        assert_eq!(
            col.test_dump(),
            vec![
                vec![ColExport::litrun(vec![1, 2, 3])],
                vec![ColExport::litrun(vec![4, 5, 6])],
            ]
        );
        col.splice(3, 1, vec![99]);
        assert_eq!(
            col.to_vec(),
            vec![Some(1), Some(2), Some(3), Some(99), Some(5), Some(6)]
        );
    }

    #[test]
    fn iter_range() {
        let seed = rand::random::<u64>();
        let mut rng = SmallRng::seed_from_u64(seed);
        let mut data = vec![];
        for _ in 0..FUZZ_SIZE {
            let val = rng.random::<u64>() % 4;
            if val == 0 {
                data.push(None);
            } else {
                data.push(Some(val));
            }
        }
        let mut col = ColumnData::<RleCursor<8, u64>>::new();
        col.splice(0, 0, data.clone());

        for _ in 0..FUZZ_SIZE {
            let a = rng.random::<u32>() % FUZZ_SIZE;
            let b = rng.random::<u32>() % FUZZ_SIZE;
            let min = std::cmp::min(a, b) as usize;
            let max = std::cmp::max(a, b) as usize;

            assert_eq!(col.iter_range(min..max).to_vec(), data[min..max].to_vec());
        }
    }

    #[test]
    fn iter_range_with_acc() {
        let seed = rand::random::<u64>();
        let mut rng = SmallRng::seed_from_u64(seed);
        let mut data = vec![];
        const MAX: usize = FUZZ_SIZE as usize;
        for _ in 0..MAX {
            let val = rng.random::<u64>() % 4;
            if val == 0 {
                data.push(None);
            } else {
                data.push(Some(val));
            }
        }
        let mut col = ColumnData::<RleCursor<8, u64>>::new();
        col.splice(0, 0, data.clone());

        let vals_w_acc = col.iter().with_acc().collect::<Vec<_>>();

        for n in 0..(MAX - 3) {
            let m = n + 3;
            let sub = col.iter_range(n..m).with_acc().collect::<Vec<_>>();
            assert_eq!(&vals_w_acc[n..m], sub.as_slice());
        }

        let mut last_acc = Acc::new();
        let mut last_item_agg = Default::default();
        for n in 0..(col.acc().as_usize()) {
            let result = col.iter().with_acc().shift_acc(n).unwrap();
            let item = result.item;
            let acc = result.acc;
            assert!(acc <= Acc::from(n));
            assert!(acc == last_acc || acc == last_acc + last_item_agg);
            last_acc = acc;
            last_item_agg = item.map(|v| <u64 as Packable>::agg(&v)).unwrap_or_default();
        }
    }

    #[test]
    fn find_values_by_agg() {
        let seed = rand::random::<u64>();
        let mut rng = SmallRng::seed_from_u64(seed);
        let mut data_i64 = vec![];
        let mut data_u64 = vec![];
        const MAX: usize = FUZZ_SIZE as usize;
        for _ in 0..MAX {
            let val = rng.random::<u32>();
            if val == 0 {
                data_i64.push(None);
                data_u64.push(None);
            } else {
                data_i64.push(Some(val as i64));
                data_u64.push(Some(val as u64));
            }
        }

        let mut rle_col = ColumnData::<RleCursor<16, u64>>::new();
        rle_col.splice(0, 0, data_u64.clone());

        for (i, val) in data_u64.iter().enumerate() {
            if let Some(val) = val {
                assert!(rle_col.find_by_value(*val).any(|j| j == i));
            }
        }

        let mut delta_col = ColumnData::<DeltaCursorInternal<16>>::new();
        delta_col.splice(0, 0, data_i64.clone());

        for (i, val) in data_i64.iter().enumerate() {
            if let Some(val) = val {
                assert!(delta_col.find_by_value(*val).any(|j| j == i));
            }
        }
    }

    #[test]
    fn fuzz_find_by_values() {
        const N: u32 = 10_000;
        const STEP: u32 = 3;
        let mut rng = make_rng();
        let col: ColumnData<UIntCursor> = (0..N)
            .flat_map(|i| [i as u64 * 2 + 1; STEP as usize].into_iter())
            .collect();
        for _ in 0..FUZZ_SIZE {
            let roll = rng.random::<u32>() % N;
            let target1 = (roll * 2) as u64;
            let target2 = (roll * 2 + 1) as u64;

            let mut a = (rng.random::<u32>() % (N * STEP)) as usize;
            let mut b = (rng.random::<u32>() % (N * STEP)) as usize;
            if a > b {
                std::mem::swap(&mut a, &mut b);
            }

            assert!(b >= a);

            let start = (roll * 3) as usize;
            let a_start = min(b, max(start, a));
            let a_end1 = max(a_start, min(start, b));
            let a_end2 = max(a_start, min(start + 3, b));

            let answer1 = a_start..a_end1;
            let answer2 = a_start..a_end2;

            let result1 = col.scope_to_value(Some(target1), a..b);
            let result2 = col.scope_to_value(Some(target2), a..b);

            assert_eq!(answer1, result1);
            assert_eq!(answer2, result2);
        }
    }

    #[test]
    fn shift_next() {
        let col: ColumnData<UIntCursor> = [
            0, 0, 0, 1, 1, 1, 6, 6, 6, 7, 7, 7, 8, 8, 8, 9, 9, 9, 10, 10, 10,
        ]
        .iter()
        .collect();
        let mut iter = col.iter_range(1..4);
        assert_eq!(iter.next(), Some(Some(Cow::Owned(0))));
        assert_eq!(iter.next(), Some(Some(Cow::Owned(0))));
        assert_eq!(iter.next(), Some(Some(Cow::Owned(1))));
        assert_eq!(iter.next(), None);

        let next = iter.shift_next(5..7);

        assert_eq!(next, Some(Some(Cow::Owned(1))));
        assert_eq!(iter.next(), Some(Some(Cow::Owned(6))));
        assert_eq!(iter.next(), None);

        let next = iter.shift_next(8..10);

        assert_eq!(next, Some(Some(Cow::Owned(6))));
        assert_eq!(iter.next(), Some(Some(Cow::Owned(7))));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn fuzz_find_by_range() {
        const N: usize = 8;
        const STEP: u32 = 4;
        let mut rng = make_rng();
        for _ in 0..FUZZ_SIZE {
            let data = (0..N)
                .map(|_| rng.random::<u64>() % STEP as u64 + 1)
                .collect::<Vec<_>>();
            let col1: ColumnData<UIntCursor> = data.clone().into_iter().collect();
            let col2: ColumnData<DeltaCursor> =
                data.clone().into_iter().map(|i| i as i64).collect();

            let a = (rng.random::<u32>() % STEP + 1) as usize;
            let b = (rng.random::<u32>() % STEP + 1) as usize;
            let range = a.min(b)..a.max(b);

            let result1 = col1.find_by_range(range.clone()).collect::<Vec<_>>();
            let result2 = col2.find_by_range(range.clone()).collect::<Vec<_>>();
            let answer = data
                .iter()
                .enumerate()
                .filter_map(|(i, v)| {
                    if range.contains(&(*v as usize)) {
                        Some(i)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();

            assert_eq!(result1, answer);
            assert_eq!(result2, answer);
        }
    }

    #[test]
    fn iter_scope_to_value() {
        let col: ColumnData<UIntCursor> = [
            0, 0, 0, 1, 1, 1, 6, 6, 6, 7, 7, 7, 8, 8, 8, 9, 9, 9, 10, 10, 10,
        ]
        .iter()
        .collect();
        let mut iter = col.iter();
        assert_eq!(iter.seek_to_value(Some(0), ..), 0..3);
        assert_eq!(iter.seek_to_value(Some(6), ..), 6..9);
        assert_eq!(iter.seek_to_value(Some(8), ..), 12..15);

        let mut iter = col.iter();
        assert_eq!(iter.seek_to_value(Some(0), ..), 0..3);
        assert_eq!(iter.seek_to_value(Some(1), ..), 3..6);
        assert_eq!(iter.seek_to_value(Some(6), ..), 6..9);
    }

    #[test]
    fn simple_advance_by_acc() {
        type C = ColumnData<RleCursor<8, u64>>;

        let column = C::from(vec![0, 1, 1, 0, 1, 1, 0]);

        assert_eq!(column.iter().advance_acc_by(0), 1);
        assert_eq!(column.iter().advance_acc_by(1), 2);
        assert_eq!(column.iter().advance_acc_by(2), 4);
        assert_eq!(column.iter().advance_acc_by(3), 5);
        assert_eq!(column.iter().advance_acc_by(4), 7);
        assert_eq!(column.iter().advance_acc_by(100), 7);

        assert_eq!(column.iter_range(2..7).advance_acc_by(0), 0);
        assert_eq!(column.iter_range(2..7).advance_acc_by(1), 2);
        assert_eq!(column.iter_range(2..7).advance_acc_by(2), 3);
        assert_eq!(column.iter_range(2..7).advance_acc_by(3), 5);
        assert_eq!(column.iter_range(2..7).advance_acc_by(100), 5);

        let column = C::from(vec![0, 0, 1, 1, 0, 0, 1, 1, 0]);

        let mut iter = column.iter_range(1..5);
        assert_eq!(iter.advance_acc_by(0), 1);
        iter.next();
        assert_eq!(iter.advance_acc_by(0), 0);

        let mut iter = column.iter_range(0..5);
        assert_eq!(iter.advance_acc_by(1), 3);
        iter.next();
        assert_eq!(iter.advance_acc_by(0), 1);
        assert_eq!(iter.pos(), 5);

        let column = C::from(vec![0, 3, 3, 0, 3, 3, 0]);

        assert_eq!(column.iter().advance_acc_by(0), 1);
        assert_eq!(column.iter().advance_acc_by(1), 1);
        assert_eq!(column.iter().advance_acc_by(2), 1);
        assert_eq!(column.iter().advance_acc_by(3), 2);
        assert_eq!(column.iter().advance_acc_by(4), 2);
        assert_eq!(column.iter().advance_acc_by(5), 2);
        assert_eq!(column.iter().advance_acc_by(6), 4);
    }

    #[test]
    fn fuzz_advance_by_acc() {
        const SIZE: usize = 10000;
        let mut rng = make_rng();
        let mut data = vec![];
        let mut acc = vec![];
        let mut agg = 0;
        for _ in 0..SIZE {
            let val = rng.random::<u64>() % 4;
            agg += val;
            data.push(val);
            acc.push(agg);
        }
        let column: ColumnData<RleCursor<8, u64>> = data.iter().cloned().collect();
        for _ in 0..10 {
            let mut iter = column.iter();
            loop {
                let advance = rng.random::<u64>() % 8 + 1;
                let pos1 = iter.pos();
                iter.advance_acc_by(advance);
                if let Some(val) = iter.next() {
                    let pos2 = iter.pos();
                    let _acc = iter.calculate_acc();
                    assert!(pos2 > pos1);
                    assert!(Acc::from(acc[pos2 - 1]) >= _acc);
                    if pos2 > 1 {
                        assert!(Acc::from(acc[pos2 - 2]) <= _acc);
                    }
                    assert_eq!(data[pos2 - 1], val.as_deref().copied().unwrap_or_default());
                } else {
                    break;
                }
            }
        }
    }

    #[test]
    fn readme_test() {
        // columns are generally modified with a splice command
        let mut column1: ColumnData<IntCursor> = ColumnData::new();
        column1.splice(0, 0, [1, 2, 3, 4, 5, 6, 7]);

        // the columns can be created with collect()
        // data written to columns is mediated by the MaybePackable trait.
        // this allows StrCursor to use &str, String, Option<&str> and Option<String>
        // or UIntCursor to use u64, or Option<u64> interchangably
        let column2: ColumnData<UIntCursor> = [1, 2, 3].into_iter().collect();
        // data read is always Option<Cow<'_, Cursor::Item>> or Option<Cursor::Item>
        assert_eq!(column2.to_vec(), vec![Some(1), Some(2), Some(3)]);

        // save() writes the column data to a single Vec<u8> combinding the internal slabs
        // load() does the inverse and will throw an error if the data does not fit the
        // encoding standard
        let column3: ColumnData<IntCursor> = ColumnData::load(&column1.save()).unwrap();
        // TODO add a load_with() example

        // the vec representation of the data is the same
        assert_eq!(column1.to_vec(), column3.to_vec());
        // and the saved data is the same - but the internal representation may differ
        assert_eq!(column1.save(), column3.save());

        // Cursors can directly encode data into a buffer and skip the slab intermediary
        // By default encode writes nothing if all the data passed in is false or None
        // but you can force it to write the nulls if the third parameter is true
        let mut buffer = vec![];
        let _word_range = StrCursor::encode(&mut buffer, ["dog", "book", "bell"]);
        assert_eq!(_word_range, 0..15);
        let _bool_range = BooleanCursor::encode_unless_empty(&mut buffer, [false, false, false]);
        assert_eq!(_bool_range, 15..15);

        // if you need to build the data incrementally you can get access to the underlying encoder
        let mut encoder: Encoder<'_, StrCursor> = Encoder::default();
        for word in ["dog", "book", "bell"] {
            encoder.append(word);
        }
        let _word_range2 = encoder.save_to(&mut buffer);

        // accessing elements within a column

        assert_eq!(column1.get(1), Some(Some(Cow::Owned(2))));
        assert_eq!(
            column1.iter().take(3).collect::<Vec<_>>(),
            vec![
                Some(Cow::Owned(1)),
                Some(Cow::Owned(2)),
                Some(Cow::Owned(3)),
            ]
        );
        assert_eq!(
            column1.iter_range(3..5).collect::<Vec<_>>(),
            vec![Some(Cow::Owned(4)), Some(Cow::Owned(5)),]
        );

        // TODO
        // get_acc()
        // get_with_acc()
        // seek_to_value()
        // advance_acc_by()
        // iter().with_acc()
        // suspend()
    }
}
