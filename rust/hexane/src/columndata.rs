use super::aggregate::Acc;
use super::aggregate::Agg;
use super::cursor::{
    ColumnCursor, HasAcc, HasMinMax, HasPos, Run, RunIter, ScanMeta, SpliceResult,
};
use super::encoder::Encoder;
use super::pack::{MaybePackable, PackError, Packable};
use super::raw::RawReader;
use super::slab;
use super::slab::{Slab, SlabTree, SpanTree};
use super::Cow;

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::{Bound, Range, RangeBounds};

#[derive(Debug, Clone)]
pub struct ColumnData<C: ColumnCursor> {
    pub len: usize,
    pub slabs: SpanTree<Slab, C::SlabIndex>,
    #[cfg(debug_assertions)]
    pub debug: Vec<C::Export>,
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
    pub fn byte_len(&self) -> usize {
        self.slabs.iter().map(|s| s.as_slice().len()).sum()
    }

    pub fn get(&self, index: usize) -> Option<Option<Cow<'_, C::Item>>> {
        let range = index..(index + 1);
        let mut iter = self.iter_range(range);
        iter.next()
    }

    pub fn get_acc_delta(&self, index1: usize, index2: usize) -> (Acc, Option<Cow<'_, C::Item>>) {
        assert!(index1 <= index2);
        let acc1 = self.get_acc(index1);
        let mut iter = self.iter_range(index2..(index2 + 1));
        let acc2 = iter.calculate_acc();
        let item = iter.next().flatten();
        (acc2 - acc1, item)
    }

    pub fn get_acc(&self, index: usize) -> Acc {
        let range = index..(index + 1);
        let iter = self.iter_range(range);
        iter.calculate_acc()
    }

    pub fn get_with_acc(
        &self,
        index: usize,
    ) -> Option<ColGroupItem<'_, <C as ColumnCursor>::Item>> {
        let range = index..(index + 1);
        let mut iter = self.iter_range(range).with_acc();
        iter.next()
    }

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

    pub fn save_to_unless_empty(&self, out: &mut Vec<u8>) -> Range<usize> {
        if self.is_empty() {
            out.len()..out.len()
        } else {
            self.save_to(out)
        }
    }

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
        RawReader { slabs, current }
    }
}

#[derive(Debug)]
pub struct ColumnDataIter<'a, C: ColumnCursor> {
    pos: usize,
    max: usize,
    slabs: slab::SpanTreeIter<'a, Slab, C::SlabIndex>,
    slab: RunIter<'a, C>,
    run: Option<Run<'a, C::Item>>,
}

impl<C: ColumnCursor> Default for ColumnDataIter<'_, C> {
    fn default() -> Self {
        Self {
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
            pos: self.pos,
            max: self.max,
            slabs: self.slabs.clone(),
            slab: self.slab,
            run: self.run.clone(),
        }
    }
}

impl<'a, C: ColumnCursor> ColumnDataIter<'a, C> {
    pub(crate) fn new(slabs: &'a SlabTree<C::SlabIndex>, pos: usize, max: usize) -> Self {
        let cursor = slabs.get_where_or_last(|acc, next| pos < acc.pos() + next.pos());
        let mut slab = cursor.element.run_iter::<C>();
        let slabs = slab::SpanTreeIter::new(slabs, cursor);
        let iter_pos = slabs.weight().pos() - slab.pos_left();
        let advance = pos - iter_pos;
        let run = slab.sub_advance(advance);
        ColumnDataIter {
            pos,
            max,
            slabs,
            slab,
            run,
        }
    }

    pub(crate) fn new_at_index(
        slabs: &'a SlabTree<C::SlabIndex>,
        index: usize,
        max: usize,
    ) -> Self {
        let cursor = slabs.get_cursor(index).unwrap();
        let mut slab = cursor.element.run_iter::<C>();
        let slabs = slab::SpanTreeIter::new(slabs, cursor);
        let pos = slabs.weight().pos() - slab.pos_left();
        let run = slab.sub_advance(0);
        assert!(pos < max);
        ColumnDataIter {
            pos,
            max,
            slabs,
            slab,
            run,
        }
    }

    pub(crate) fn new_at_acc(slabs: &'a SlabTree<C::SlabIndex>, acc: Acc, max: usize) -> Self {
        let cursor = slabs.get_where_or_last(|a, next| acc < a.acc() + next.acc());
        let mut slab = cursor.element.run_iter();
        let pos = cursor.weight.pos();
        let slabs = slab::SpanTreeIter::new(slabs, cursor);
        let run = slab.sub_advance(0);
        ColumnDataIter {
            pos,
            max,
            slabs,
            slab,
            run,
        }
    }

    pub fn empty() -> Self {
        Self {
            pos: 0,
            max: 0,
            slabs: Default::default(),
            slab: RunIter::empty(),
            run: None,
        }
    }

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

    pub fn run_count(&self) -> usize {
        self.run.as_ref().map(|e| e.count).unwrap_or_default()
    }

    fn run_acc(&self) -> Acc {
        self.run.as_ref().map(|e| e.acc()).unwrap_or_default()
    }

    fn pop_element(&mut self) -> Option<Option<Cow<'a, C::Item>>> {
        self.slab.cursor.pop(self.run.as_mut()?)
    }

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

    pub fn advance_by(&mut self, amount: usize) {
        if amount > 0 {
            self.nth(amount - 1);
        }
        self.check_pos();
    }

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

    // binary search through the span tree nodes
    // this will only work if the data is ordered over the range
    // we only read the first element of each node and never
    // from the first node as we arent always including its first element

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

    // this function assumes all values within its range are ordered
    // will give undefined results otherwise
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

    pub fn end_pos(&self) -> usize {
        self.max
    }

    pub fn set_max(&mut self, max: usize) {
        self.max = max
    }

    pub fn to_vec(self) -> Vec<C::Export> {
        let mut result = vec![];
        C::export_splice(&mut result, 0..0, self);
        result
    }

    pub fn with_acc(self) -> ColGroupIter<'a, C> {
        ColGroupIter { iter: self }
    }

    pub fn as_acc(self) -> ColAccIter<'a, C> {
        ColAccIter { iter: self }
    }

    pub fn calculate_acc(&self) -> Acc {
        self.slabs.weight().acc() - self.slab.acc_left() - self.run_acc()
    }

    fn reset_iter_to_pos(&mut self, pos: usize) -> Option<()> {
        let tree = self.slabs.span_tree()?;
        let pos = std::cmp::min(pos, self.max);
        let _ = std::mem::replace(self, Self::new(tree, pos, self.max));
        Some(())
    }

    fn reset_iter_to_slab_index(&mut self, index: usize) -> Option<()> {
        let tree = self.slabs.span_tree()?;
        let _ = std::mem::replace(self, Self::new_at_index(tree, index, self.max));
        Some(())
    }

    fn reset_iter_to_acc(&mut self, acc: Acc) -> Acc {
        if let Some(tree) = self.slabs.span_tree() {
            let _ = std::mem::replace(self, Self::new_at_acc(tree, acc, self.max));
            let new_acc = self.calculate_acc();
            acc - new_acc
        } else {
            Acc::default()
        }
    }

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
}

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

// This iterator has a strange dual purpose implementation
// the next() method just grabs the next item on the underlying
// iterator but includes the ACC value where as the nth()
// seeks forward not n steps but an increase of n in in acc
// this is extremely useful but its confusing b/c it breaks the abstraction
// probably the best move is to split this into two different interfaces
// rather than overload this iterator with both
// something that could force the change would be to make ColGroupItem::item
// not an Option because the acc cant go up if that is None

#[derive(Debug, Clone)]
pub struct ColGroupIter<'a, C: ColumnCursor> {
    iter: ColumnDataIter<'a, C>,
}

impl<'a, C: ColumnCursor> ColGroupIter<'a, C> {
    pub fn advance_by(&mut self, amount: usize) {
        self.iter.advance_by(amount)
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

#[derive(Debug, PartialEq, Clone)]
pub struct ColGroupItem<'a, P: Packable + ?Sized> {
    pub acc: Acc,
    pub pos: usize,
    pub item: Option<Cow<'a, P>>,
}

impl<P: Packable + ?Sized> ColGroupItem<'_, P> {
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
        self.iter.advance_acc_by(n);
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
    pub fn run_iter(&self) -> impl Iterator<Item = Run<'_, C::Item>> {
        self.slabs.iter().flat_map(|s| s.run_iter::<C>())
    }

    pub fn to_vec(&self) -> Vec<C::Export> {
        let mut result = vec![];
        C::export_splice(&mut result, 0..0, self.iter());
        result
    }

    pub fn iter(&self) -> ColumnDataIter<'_, C> {
        ColumnDataIter::new(&self.slabs, 0, self.len)
    }

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

    pub fn iter_range(&self, range: Range<usize>) -> ColumnDataIter<'_, C> {
        let start = std::cmp::min(self.len, range.start);
        let end = std::cmp::min(self.len, range.end);
        ColumnDataIter::new(&self.slabs, start, end)
    }

    #[cfg(debug_assertions)]
    fn init_debug(mut self) -> Self {
        let mut debug = vec![];
        C::export_splice(&mut debug, 0..0, self.iter());
        self.debug = debug;
        self
    }

    pub(crate) fn init(len: usize, slabs: SlabTree<C::SlabIndex>) -> Self {
        debug_assert_eq!(len, slabs.iter().map(|s| s.len()).sum::<usize>());
        let col = ColumnData {
            len,
            slabs,
            _phantom: PhantomData,
            #[cfg(debug_assertions)]
            debug: vec![],
        };
        #[cfg(debug_assertions)]
        let col = col.init_debug();
        col
    }

    pub fn new() -> Self {
        ColumnData {
            len: 0,
            slabs: SlabTree::new2(Slab::default()),
            _phantom: PhantomData,
            #[cfg(debug_assertions)]
            debug: vec![],
        }
    }

    pub fn save(&self) -> Vec<u8> {
        let mut data = vec![];
        self.save_to(&mut data);
        data
    }

    pub fn push<'b, M>(&mut self, value: M) -> Acc
    where
        M: MaybePackable<'b, C::Item> + Clone,
        C::Item: 'b,
    {
        let index = self.len();
        self.splice(index, 0, [value])
    }

    pub fn extend<'b, M, I>(&mut self, values: I) -> Acc
    where
        M: MaybePackable<'b, C::Item>,
        I: IntoIterator<Item = M>,
        C::Item: 'b,
    {
        let index = self.len();
        self.splice(index, 0, values)
    }

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

        match C::splice(
            cursor.element,
            index - cursor.weight.pos(),
            del,
            values,
            #[cfg(debug_assertions)]
            (&mut self.debug, index..(index + del)),
        ) {
            SpliceResult::Replace {
                add,
                del,
                group,
                mut slabs,
            } => {
                acc += group;
                C::compute_min_max(&mut slabs); // this should be handled by slabwriter.finish
                self.len = self.len + add - del;
                self.slabs.splice(cursor.index..(cursor.index + 1), slabs);
                assert!(!self.slabs.is_empty());
            }
            SpliceResult::Noop => {}
        }

        debug_assert_eq!(
            self.iter()
                .map(|i| i.as_deref().map(<C::Item>::agg).unwrap_or_default())
                .sum::<Acc>(),
            self.acc()
        );

        #[cfg(debug_assertions)]
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

    pub fn fill_if_empty(&mut self, len: usize) -> bool {
        if self.len == 0 && len > 0 {
            *self = Self::init_empty(len);
            true
        } else {
            false
        }
    }

    pub fn init_empty(len: usize) -> Self {
        let new_slab = C::init_empty(len);
        let mut slabs = SlabTree::default();
        slabs.push(new_slab);
        assert!(!slabs.is_empty());
        ColumnData::init(len, slabs)
    }

    pub fn load(data: &[u8]) -> Result<Self, PackError> {
        Self::load_with(data, &ScanMeta::default())
    }

    pub fn load_with(data: &[u8], m: &ScanMeta) -> Result<Self, PackError> {
        let col = C::load_with(data, m)?;
        debug_assert_eq!(
            col.iter()
                .map(|i| i.as_deref().map(<C::Item>::agg).unwrap_or_default())
                .sum::<Acc>(),
            col.acc()
        );
        Ok(col)
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn acc(&self) -> Acc {
        self.slabs.weight().map(|w| w.acc()).unwrap_or_default()
    }
}

impl<C: ColumnCursor> ColumnData<C>
where
    C::SlabIndex: HasMinMax,
{
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
    use super::super::rle::{ByteCursor, RleCursor, StrCursor, UIntCursor};
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
        vec.splice(index..index, values.clone());
        col.splice(index, 0, values);
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
            Some((rng.random::<u64>() % 10) as i64)
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
            Some(rng.random::<u64>() % 10)
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
            let result = col.iter().with_acc().nth(n).unwrap();
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
}
