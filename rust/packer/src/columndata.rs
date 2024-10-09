use super::cursor::{ColumnCursor, Run, RunIter, ScanMeta, SpliceResult};
use super::pack::{MaybePackable, PackError, Packable};
use super::raw::RawReader;
use super::slab;
use super::slab::{Slab, SlabTree, SlabWriter};

use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::{Bound, Range, RangeBounds};
use std::sync::Arc;

#[derive(Debug, Default, Clone)]
pub struct ColumnData<C: ColumnCursor> {
    pub len: usize,
    pub slabs: SlabTree,
    #[cfg(debug_assertions)]
    pub debug: Vec<C::Export>,
    _phantom: PhantomData<C>,
}

impl<C: ColumnCursor> ColumnData<C> {
    pub fn byte_len(&self) -> usize {
        self.slabs.iter().map(|s| s.as_slice().len()).sum()
    }

    pub fn get(&self, index: usize) -> Option<Option<<C::Item as Packable>::Unpacked<'_>>> {
        let range = index..(index + 1);
        let mut iter = self.iter_range(range);
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
        println!(" :: {:?}", data);
    }

    pub fn write(&self, out: &mut Vec<u8>) -> Range<usize> {
        let start = out.len();
        if self.slabs.len() == 1 {
            let slab = self.slabs.get(0).unwrap();
            if slab.is_empty() {
                let state = C::State::default();
                let mut writer = SlabWriter::new(usize::MAX);
                C::flush_state(&mut writer, state);
                writer.write(out);
            } else {
                out.extend(slab.as_slice())
            }
        } else {
            let mut state = C::State::default();
            let mut writer = SlabWriter::new(usize::MAX);
            for s in &self.slabs {
                state = C::write(&mut writer, s, state);
            }
            C::flush_state(&mut writer, state);
            writer.write(out);
        }
        let end = out.len();
        start..end
    }

    pub fn raw_reader(&self, advance: usize) -> RawReader<'_> {
        let cursor = self
            .slabs
            .get_where(|acc, next| advance - acc.pos < next.pos)
            .unwrap();
        let current = Some((cursor.element, advance - cursor.weight.pos));
        let slabs = slab::Iter::new(&self.slabs, cursor);
        RawReader { slabs, current }
    }
}

#[derive(Debug, Default)]
pub struct ColumnDataIter<'a, C: ColumnCursor> {
    pos: usize,
    max: usize,
    slabs: slab::Iter<'a>,
    slab: RunIter<'a, C>,
    run: Option<Run<'a, C::Item>>,
}

impl<'a, C: ColumnCursor> Copy for ColumnDataIter<'a, C> {}

impl<'a, C: ColumnCursor> Clone for ColumnDataIter<'a, C> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, C: ColumnCursor> ColumnDataIter<'a, C> {
    pub(crate) fn new(slabs: &'a SlabTree, pos: usize, max: usize) -> Self {
        let cursor = slabs
            .get_where(|acc, next| pos - acc.pos < next.pos)
            .unwrap();
        let mut slab = cursor.element.run_iter();
        let slabs = slab::Iter::new(slabs, cursor);
        let iter_pos = slabs.weight().pos - slab.weight_left().pos;
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
        assert_eq!(
            self.slabs.weight().pos - self.slab.weight_left().pos - self.run_count(),
            self.pos
        );
        self.pos
    }

    fn check_pos(&self) {
        assert_eq!(
            self.slabs.weight().pos - self.slab.weight_left().pos - self.run_count(),
            self.pos
        );
    }

    fn run_count(&self) -> usize {
        self.run.as_ref().map(|e| e.count).unwrap_or_default()
    }

    fn run_group(&self) -> usize {
        self.run.as_ref().map(|e| e.group()).unwrap_or_default()
    }

    fn pop_element(&mut self) -> Option<Option<<C::Item as Packable>::Unpacked<'a>>> {
        self.run.as_mut()?.next()
    }

    pub fn next_run(&mut self) -> Option<Run<'a, C::Item>> {
        if self.pos >= self.max {
            return None;
        }
        let mut run = self.run.take().or_else(|| self.pop_run())?;
        let count = run.count;
        if self.pos + run.count > self.max {
            run.count = self.max - self.pos;
        }
        self.pos += count;
        self.check_pos();
        Some(run)
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

    pub fn scope_to_value(
        &mut self,
        value: Option<<C::Item as Packable>::Unpacked<'a>>,
    ) -> Range<usize> {
        let pos = self.pos();
        let mut start = pos;
        let mut end = pos;
        let mut found = false;
        while let Some(run) = self.next_run() {
            match (value, run.value) {
                (None, None) => found = true,
                (None, Some(_)) => break,
                (Some(a), Some(b)) if a < b => break,
                (Some(a), Some(b)) if a == b => found = true,
                _ => {}
            }
            if !found {
                start += run.count
            }
            end += run.count
        }
        start..end
    }

    pub fn end_pos(&self) -> usize {
        self.max
    }

    pub fn to_vec(self) -> Vec<C::Export> {
        C::to_vec(self)
    }

    pub fn with_group(self) -> ColGroupIter<'a, C> {
        ColGroupIter { iter: self }
    }

    pub fn calculate_group(&self) -> usize {
        self.slabs.weight().group - self.slab.weight_left().group - self.run_group()
    }

    fn reset_iter_to_pos(&mut self, pos: usize) -> Option<()> {
        let tree = self.slabs.span_tree()?;
        let _ = std::mem::replace(self, Self::new(tree, pos, self.max));
        Some(())
    }
}

pub struct ColGroupIter<'a, C: ColumnCursor> {
    iter: ColumnDataIter<'a, C>,
}

impl<'a, C: ColumnCursor> ColGroupIter<'a, C> {
    pub fn advance_by(&mut self, amount: usize) {
        self.iter.advance_by(amount)
    }
}

impl<'a, C: ColumnCursor> Iterator for ColGroupIter<'a, C> {
    type Item = (Option<<C::Item as Packable>::Unpacked<'a>>, usize);

    fn next(&mut self) -> Option<Self::Item> {
        let group = self.iter.slabs.weight().group
            - self.iter.slab.weight_left().group
            - self.iter.run_group();
        let item = self.iter.next()?;
        Some((item, group))
    }
}

impl<'a, C: ColumnCursor> Iterator for ColumnDataIter<'a, C> {
    type Item = Option<<C::Item as Packable>::Unpacked<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.max {
            return None;
        }
        let result = self.pop_element().or_else(|| {
            self.run = self.pop_run();
            self.run.as_mut()?.next()
        });
        if result.is_some() {
            self.pos += 1;
            Some(self.slab.cursor.transform(&self.run.unwrap()))
        } else {
            None
        }
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        let target = self.pos() + n + 1;
        if self.slabs.weight().pos < target {
            self.reset_iter_to_pos(target - 1)?;
            self.next()
        } else if self.run_count() > n {
            self.pos += n + 1;
            self.run.as_mut().and_then(|r| r.nth(n))
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
        }
    }
}

impl<C: ColumnCursor> ColumnData<C> {
    pub fn to_vec(&self) -> Vec<C::Export> {
        C::to_vec(self.iter())
    }

    pub fn iter(&self) -> ColumnDataIter<'_, C> {
        ColumnDataIter::new(&self.slabs, 0, self.len)
    }

    pub fn scope_to_value<'a, R: RangeBounds<usize>>(
        &'a mut self,
        value: Option<<C::Item as Packable>::Unpacked<'a>>,
        range: R,
    ) -> Range<usize> {
        let (start, end) = normalize_range(range);
        let mut iter = self.iter_range(start..end);
        iter.scope_to_value(value)
    }

    pub fn iter_range(&self, range: Range<usize>) -> ColumnDataIter<'_, C> {
        ColumnDataIter::new(&self.slabs, range.start, range.end)
    }

    #[cfg(debug_assertions)]
    fn init_debug(mut self) -> Self {
        let mut debug = vec![];
        C::export_splice(&mut debug, 0..0, self.iter());
        self.debug = debug;
        self
    }

    fn init(len: usize, slabs: SlabTree) -> Self {
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

    #[cfg(test)]
    pub fn export(&self) -> Vec<Vec<super::cursor::ColExport<C::Item>>> {
        self.slabs.iter().map(|s| C::export(s.as_slice())).collect()
    }

    pub fn splice<E>(&mut self, index: usize, del: usize, values: Vec<E>) -> usize
    where
        E: MaybePackable<C::Item> + Debug + Clone,
    {
        assert!(index <= self.len);
        assert!(!self.slabs.is_empty());
        //assert!(!(values.is_empty() && del == 0)); // nothing to do
        if values.is_empty() && del == 0 {
            return 0; // really none
        }

        #[cfg(debug_assertions)]
        C::export_splice(
            &mut self.debug,
            index..(index + del),
            values.iter().map(|e| e.maybe_packable()),
        );

        let cursor = self
            .slabs
            .get_where(|c, next| index - c.pos < next.pos)
            .unwrap();

        let mut group = cursor.weight.group;

        match C::splice(cursor.element, index - cursor.weight.pos, del, values) {
            SpliceResult::Replace(add, del, g, slabs) => {
                group += g;
                self.len = self.len + add - del;
                self.slabs.splice(cursor.index..(cursor.index + 1), slabs);
                assert!(!self.slabs.is_empty());
            }
        }

        #[cfg(debug_assertions)]
        if self.debug != self.to_vec() {
            println!(":: debug={:?}", self.debug);
            println!(":: col={:?}", self.to_vec());
            let col = self.to_vec();
            assert_eq!(self.debug.len(), col.len());
            for (i, dbg) in col.iter().enumerate() {
                if dbg != &col[i] {
                    panic!("index={} {:?} vs {:?}", i, dbg, col[i]);
                }
            }
            panic!()
        }
        group
    }
    pub fn init_empty(len: usize) -> Self {
        let new_slab = C::init_empty(len);
        let mut slabs = SlabTree::new();
        //slabs.splice(0..0, new_slabs);
        slabs.push(new_slab);
        assert!(!slabs.is_empty());
        ColumnData::init(len, slabs)
    }

    pub fn external(
        data: Arc<Vec<u8>>,
        range: Range<usize>,
        m: &ScanMeta,
    ) -> Result<Self, PackError> {
        let slab = Slab::external::<C>(data, range, m)?;
        let len = slab.len();
        Ok(ColumnData::init(len, SlabTree::new2(slab)))
    }

    pub fn len(&self) -> usize {
        self.len
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

#[cfg(test)]
pub(crate) mod tests {
    use super::super::boolean::BooleanCursor;
    use super::super::cursor::ColExport;
    use super::super::delta::DeltaCursor;
    use super::super::rle::{IntCursor, RleCursor, StrCursor};
    use super::*;
    use rand::prelude::*;
    use rand::rngs::SmallRng;

    fn test_splice<'a, C: ColumnCursor, E>(
        vec: &'a mut Vec<E>,
        col: &'a mut ColumnData<C>,
        index: usize,
        values: Vec<E>,
    ) where
        E: MaybePackable<C::Item> + std::fmt::Debug + std::cmp::PartialEq<C::Export> + Clone,
    {
        vec.splice(index..index, values.clone());
        col.splice(index, 0, values);
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
            let advance_by = rng.gen_range(1..(data.len() - advanced_by));
            iter.advance_by(advance_by);
            let expected = data[advance_by + advanced_by..].to_vec();
            let actual = C::to_vec(iter);
            assert_eq!(expected, actual);
            advanced_by += advance_by;
        }
    }

    #[test]
    fn column_data_breaking_literal_runs_in_int_column() {
        let numbers = vec![1, 2, 3];
        let mut start = ColumnData::<IntCursor>::new();
        start.splice(0, 0, numbers);
        assert_eq!(start.export(), vec![vec![ColExport::LitRun(vec![1, 2, 3])]]);
        let mut col = start.clone();
        col.splice(2, 0, vec![3, 3, 3]);
        assert_eq!(
            col.export(),
            vec![vec![ColExport::LitRun(vec![1, 2]), ColExport::Run(4, 3)]]
        );
        let mut col = start.clone();
        col.splice(3, 0, vec![3, 3, 3]);
        assert_eq!(
            col.export(),
            vec![vec![ColExport::LitRun(vec![1, 2]), ColExport::Run(4, 3)]]
        );
        let mut col = start.clone();
        col.splice(1, 0, vec![2, 2]);
        assert_eq!(
            col.export(),
            vec![vec![
                ColExport::LitRun(vec![1]),
                ColExport::Run(3, 2),
                ColExport::LitRun(vec![3]),
            ]]
        );
        let mut col = start.clone();
        col.splice(2, 0, vec![2, 2]);
        assert_eq!(
            col.export(),
            vec![vec![
                ColExport::LitRun(vec![1]),
                ColExport::Run(3, 2),
                ColExport::LitRun(vec![3]),
            ]]
        );
        let mut col = start.clone();
        col.splice(0, 0, vec![1, 1]);
        assert_eq!(
            col.export(),
            vec![vec![ColExport::Run(3, 1), ColExport::LitRun(vec![2, 3]),]]
        );
        let mut col = start.clone();
        col.splice(1, 0, vec![1, 1]);
        assert_eq!(
            col.export(),
            vec![vec![ColExport::Run(3, 1), ColExport::LitRun(vec![2, 3]),]]
        );
    }

    #[test]
    fn column_data_breaking_runs_in_int_column() {
        let numbers = vec![2, 2, 2];
        let mut start = ColumnData::<IntCursor>::new();
        start.splice(0, 0, numbers);
        assert_eq!(start.export(), vec![vec![ColExport::Run(3, 2)]]);
        let mut col = start.clone();
        col.splice(1, 0, vec![3, 3, 3]);
        assert_eq!(
            col.export(),
            vec![vec![
                ColExport::LitRun(vec![2]),
                ColExport::Run(3, 3),
                ColExport::Run(2, 2),
            ]]
        );
        let mut col = start.clone();
        col.splice(2, 0, vec![3, 3, 3]);
        assert_eq!(
            col.export(),
            vec![vec![
                ColExport::Run(2, 2),
                ColExport::Run(3, 3),
                ColExport::LitRun(vec![2]),
            ]]
        );
        let mut col = start.clone();
        col.splice(0, 0, vec![3, 3, 3]);
        assert_eq!(
            col.export(),
            vec![vec![ColExport::Run(3, 3), ColExport::Run(3, 2),]]
        );
        let mut col = start.clone();
        col.splice(3, 0, vec![3, 3, 3]);
        assert_eq!(
            col.export(),
            vec![vec![ColExport::Run(3, 2), ColExport::Run(3, 3),]]
        );
    }

    #[test]
    fn column_data_breaking_null_runs_in_int_column() {
        let numbers = vec![None, None, Some(2), Some(2), None, None, None];
        let mut start = ColumnData::<IntCursor>::new();
        start.splice(0, 0, numbers);
        assert_eq!(
            start.export(),
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
            col.export(),
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
            col.export(),
            vec![vec![
                ColExport::Null(4),
                ColExport::Run(6, 2),
                ColExport::Null(5)
            ]]
        );
        col.splice(4, 0, vec![None, Some(2), Some(3)]);
        assert_eq!(
            col.export(),
            vec![vec![
                ColExport::Null(5),
                ColExport::LitRun(vec![2, 3]),
                ColExport::Run(6, 2),
                ColExport::Null(5)
            ]]
        );
        col.splice(2, 0, vec![4]);
        assert_eq!(
            col.export(),
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
            col.export(),
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
            col.export(),
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
            col.export(),
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
            start.export(),
            vec![vec![ColExport::litrun(vec!["one", "two", "three"])]]
        );
        let mut col = start.clone();
        col.splice(1, 0, vec![None, None, Some("two"), Some("two")]);
        assert_eq!(
            col.export(),
            vec![vec![
                ColExport::litrun(vec!["one"]),
                ColExport::Null(2),
                ColExport::run(3, "two"),
                ColExport::litrun(vec!["three"]),
            ]]
        );
        col.splice(0, 0, vec![None, None, Some("three"), Some("one")]);
        assert_eq!(
            col.export(),
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
    fn column_data_delta() {
        let numbers = vec![1, 2, 3, 4, 5, 6, 6, 6, 6, 6, 7, 8, 9];
        let mut start = ColumnData::<DeltaCursor>::new();
        start.splice(0, 0, numbers.clone());
        assert_eq!(
            start.export(),
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
            col.export(),
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
            col.export(),
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
                _ => rng.gen::<usize>() % len,
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
            let len = rng.gen::<usize>() % 40 + 1;
            for _ in 0..len {
                if rng.gen::<i64>() % 3 == 0 {
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
            Some(rng.gen::<i64>() % 10)
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
            rng.gen::<bool>()
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
            Some(rng.gen::<u64>() % 10)
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
            Some(format!("0x{:X}", rng.gen::<usize>()).to_owned())
        }
        fn plus(&self, index: usize) -> Option<String> {
            self.as_ref().map(|s| format!("{}/{}", s, index).to_owned())
        }
    }

    fn make_rng() -> SmallRng {
        let seed = rand::random::<u64>();
        //let seed = 7796233028731974218;
        println!("SEED: {}", seed);
        SmallRng::seed_from_u64(seed)
    }

    fn generate_splice<T: TestRand>(len: usize, rng: &mut SmallRng) -> (usize, Vec<T>) {
        let index = T::index(len, rng);
        let patch = match rng.gen::<usize>() % 4 {
            0 => vec![T::null(), T::null(), T::null()],
            1 => {
                let n = T::rand(rng);
                vec![n.clone(), n.clone(), n]
            }
            2 => {
                let n = T::rand(rng);
                let step = rng.gen::<usize>() % 4;
                vec![n.clone(), n.plus(step), n.plus(step * 2)]
            }
            _ => T::rand_vec(rng),
        };
        (index, patch)
    }

    #[test]
    fn column_data_fuzz_test_int() {
        let mut data: Vec<Option<u64>> = vec![];
        let mut col = ColumnData::<RleCursor<{ usize::MAX }, u64>>::new();
        let mut rng = make_rng();
        for _ in 0..1000 {
            let (index, values) = generate_splice(data.len(), &mut rng);
            test_splice(&mut data, &mut col, index, values);
        }
    }

    #[test]
    fn column_data_fuzz_test_advance_by_int() {
        let mut rng = make_rng();
        for _ in 0..1000 {
            let mut col = ColumnData::<IntCursor>::new();
            let values = Option::<u64>::rand_vec(&mut rng);
            col.splice(0, 0, values.clone());
            test_advance_by(&mut rng, &values, &mut col);
        }
    }

    #[test]
    fn column_data_str_fuzz_test() {
        let mut data: Vec<Option<String>> = vec![];
        let mut col = ColumnData::<RleCursor<{ usize::MAX }, str>>::new();
        let mut rng = make_rng();
        for _ in 0..100 {
            let (index, values) = generate_splice(data.len(), &mut rng);
            test_splice(&mut data, &mut col, index, values);
        }
    }

    #[test]
    fn column_data_fuzz_test_advance_by_str() {
        let mut rng = make_rng();
        for _ in 0..1000 {
            let mut col = ColumnData::<StrCursor>::new();
            let values = Option::<String>::rand_vec(&mut rng);
            col.splice(0, 0, values.clone());
            test_advance_by(&mut rng, &values, &mut col);
        }
    }

    #[test]
    fn column_data_fuzz_test_delta() {
        let mut data: Vec<Option<i64>> = vec![];
        let mut col = ColumnData::<DeltaCursor>::new();
        let mut rng = make_rng();
        for _ in 0..100 {
            let (index, values) = generate_splice(data.len(), &mut rng);
            test_splice(&mut data, &mut col, index, values);
        }
    }

    #[test]
    fn column_data_fuzz_test_advance_by_delta() {
        let mut rng = make_rng();
        for _ in 0..1000 {
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
        assert_eq!(col.export(), vec![vec![ColExport::Run(3, true)]]);
        col.splice(0, 0, vec![false, false, false]);
        assert_eq!(
            col.export(),
            vec![vec![ColExport::Run(3, false), ColExport::Run(3, true)]]
        );
        col.splice(6, 0, vec![false, false, false]);
        assert_eq!(
            col.export(),
            vec![vec![
                ColExport::Run(3, false),
                ColExport::Run(3, true),
                ColExport::Run(3, false),
            ]]
        );
        col.splice(9, 0, vec![true, true, true]);
        assert_eq!(
            col.export(),
            vec![vec![
                ColExport::Run(3, false),
                ColExport::Run(3, true),
                ColExport::Run(3, false),
                ColExport::Run(3, true),
            ]]
        );
        col.splice(0, 0, vec![true, true, true]);
        assert_eq!(
            col.export(),
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
            col.export(),
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
        for _ in 0..100 {
            let (index, values) = generate_splice(data.len(), &mut rng);
            test_splice(&mut data, &mut col, index, values);
        }
    }

    #[test]
    fn column_data_fuzz_test_advance_by_boolean() {
        let mut rng = make_rng();
        for _ in 0..1000 {
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
            col.export(),
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
        for _ in 0..1000 {
            let val = rng.gen::<u64>() % 4;
            if val == 0 {
                data.push(None);
            } else {
                data.push(Some(val));
            }
        }
        let mut col = ColumnData::<RleCursor<8, u64>>::new();
        col.splice(0, 0, data.clone());

        for _ in 0..1000 {
            let a = rng.gen::<usize>() % 1000;
            let b = rng.gen::<usize>() % 1000;
            let min = std::cmp::min(a, b);
            let max = std::cmp::max(a, b);

            assert_eq!(col.iter_range(min..max).to_vec(), data[min..max].to_vec());
        }
    }

    #[test]
    fn iter_range_with_group() {
        let seed = rand::random::<u64>();
        let mut rng = SmallRng::seed_from_u64(seed);
        let mut data = vec![];
        for _ in 0..1000 {
            let val = rng.gen::<u64>() % 4;
            if val == 0 {
                data.push(None);
            } else {
                data.push(Some(val));
            }
        }
        let mut col = ColumnData::<RleCursor<8, u64>>::new();
        col.splice(0, 0, data.clone());

        let vals_w_group = col.iter().with_group().collect::<Vec<_>>();

        for n in 0..995 {
            let m = n + 3;
            let sub = col.iter_range(n..m).with_group().collect::<Vec<_>>();
            assert_eq!(&vals_w_group[n..m], &sub);
        }
    }
}
