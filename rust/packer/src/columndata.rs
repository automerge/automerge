use super::cursor::{ColumnCursor, Run, ScanMeta, SpliceResult};
use super::pack::{MaybePackable, PackError, Packable};
use super::raw::RawReader;
use super::slab::{RunStep, Seek, Slab, SlabIter, SlabTree, SlabWriter};

use std::cmp::PartialOrd;
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
        let mut iter = self.iter();
        iter.advance_by(index);
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

    // FIXME - get_on_width
    #[allow(clippy::while_let_on_iterator)]
    pub fn raw_reader(&self, mut advance: usize) -> RawReader<'_> {
        let mut reader = RawReader {
            slabs: self.slabs.iter(),
            current: None,
        };
        if advance > 0 {
            while let Some(s) = reader.slabs.next() {
                if s.len() <= advance {
                    advance -= s.len();
                } else {
                    reader.current = Some((s, advance));
                    break;
                }
            }
        }
        reader
    }
}

#[derive(Debug, Default, Copy, Clone)]
pub struct ColumnDataIter<'a, C: ColumnCursor> {
    pos: usize,
    group: usize,
    max: usize,
    slab_index: usize,
    slabs: Option<&'a SlabTree>,
    iter: Option<SlabIter<'a, C>>,
}

impl<'a, C: ColumnCursor> ColumnDataIter<'a, C> {
    pub fn end_pos(&self) -> usize {
        self.max
    }

    pub fn next_run(&mut self) -> Option<Run<'a, C::Item>> {
        if self.iter.is_none() {
            self.iter = Some(self.slabs?.get(self.slab_index)?.iter());
            self.slab_index += 1;
        }
        if self.pos() >= self.max {
            return None;
        }
        if let Some(iter) = &mut self.iter {
            if let Some(run) = iter.next_run() {
                Some(run)
            } else {
                assert_eq!(iter.pos(), iter.cursor.index());
                assert_eq!(iter.group(), iter.cursor.group());
                self.pos += iter.pos();
                self.group += iter.group();
                self.iter = None;
                self.next_run()
            }
        } else {
            None
        }
    }

    pub fn empty() -> Self {
        ColumnDataIter {
            pos: 0,
            group: 0,
            max: 0,
            slab_index: 0,
            slabs: None, //slab::SpanTreeIter::default(),
            iter: None,
        }
    }

    pub fn advance_by(&mut self, amount: usize) {
        struct SeekBy<T: ?Sized> {
            amount_left: usize,
            _phantom: PhantomData<T>,
        }

        impl<T: Packable + ?Sized> Seek<T> for SeekBy<T> {
            type Output = ();
            fn skip_slab(&mut self, r: &Slab) -> bool {
                if r.len() < self.amount_left {
                    self.amount_left -= r.len();
                    true
                } else {
                    false
                }
            }
            fn process_run<'a>(&mut self, r: &Run<'a, T>) -> RunStep<'a, T> {
                if r.count < self.amount_left {
                    self.amount_left -= r.count;
                    RunStep::Skip
                } else {
                    let left = r.pop_n(self.amount_left);
                    RunStep::Done(left)
                }
            }
            fn done<'a>(&self) -> bool {
                self.amount_left == 0
            }
            fn finish(self) -> Self::Output {}
        }

        self.seek_to(SeekBy {
            amount_left: amount,
            _phantom: PhantomData,
        });
    }

    pub fn scope_to_value<
        'b,
        V: for<'c> PartialOrd<<C::Item as Packable>::Unpacked<'c>> + Debug,
        R: RangeBounds<usize>,
    >(
        &mut self,
        value: Option<V>,
        range: R,
    ) -> Range<usize> {
        #[derive(Debug, PartialEq)]
        enum ScopeState {
            Seek,
            Found,
        }

        #[derive(Debug)]
        struct ScopeValue<T: ?Sized, V> {
            target: Option<V>,
            pos: usize,
            start: usize,
            max: usize,
            state: ScopeState,
            _phantom: PhantomData<T>,
        }

        impl<T, V> Seek<T> for ScopeValue<T, V>
        where
            T: Packable + ?Sized,
            V: for<'a> PartialOrd<T::Unpacked<'a>> + Debug, //+ PartialOrd<T::Unpacked<'a>>
        {
            type Output = Range<usize>;
            fn skip_slab(&mut self, r: &Slab) -> bool {
                if self.state == ScopeState::Seek && self.pos + r.len() <= self.start {
                    self.pos += r.len();
                    true
                } else {
                    false
                }
            }
            fn process_run<'a>(&mut self, r: &Run<'a, T>) -> RunStep<'a, T> {
                match self.state {
                    ScopeState::Seek => {
                        if self.pos + r.count <= self.start {
                            // before start
                            self.pos += r.count;
                            RunStep::Skip
                        } else if self.pos >= self.max {
                            // after max
                            RunStep::Done(None)
                        } else {
                            match (&self.target, &r.value) {
                                (None, None) => {
                                    self.state = ScopeState::Found;
                                    self.start = std::cmp::max(self.start, self.pos);
                                    self.pos += r.count;
                                    RunStep::Skip
                                }
                                (None, Some(_)) => {
                                    self.state = ScopeState::Found;
                                    self.start = std::cmp::max(self.start, self.pos);
                                    self.pos = self.start;
                                    RunStep::Done(None)
                                }
                                (Some(a), Some(b)) if a <= b => {
                                    // found target
                                    // TODO write a test where we have many objects w
                                    // one big key run
                                    self.state = ScopeState::Found;
                                    self.start = std::cmp::max(self.start, self.pos);
                                    if a == b {
                                        self.pos += r.count;
                                        RunStep::Skip
                                    } else {
                                        self.pos = self.start;
                                        RunStep::Done(None)
                                    }
                                }
                                _ => {
                                    // not found yet
                                    self.pos += r.count;
                                    RunStep::Skip
                                }
                            }
                        }
                    }
                    ScopeState::Found => {
                        if self.pos >= self.max {
                            // past max
                            RunStep::Done(None)
                        } else {
                            match (&self.target, &r.value) {
                                (Some(a), Some(b)) if a != b => RunStep::Done(None),
                                (a, b) if a.is_some() != b.is_some() => RunStep::Done(None),
                                _ => {
                                    self.pos += r.count;
                                    RunStep::Skip
                                }
                            }
                        }
                    }
                }
            }
            fn done<'b>(&self) -> bool {
                panic!()
            }
            fn finish(self) -> Self::Output {
                let end = std::cmp::min(self.pos, self.max);
                if self.state == ScopeState::Found {
                    self.start..end
                } else {
                    end..end
                }
            }
        }

        let (start, max) = normalize_range(range);
        self.seek_to(ScopeValue {
            target: value,
            state: ScopeState::Seek,
            start,
            max,
            pos: 0,
            _phantom: PhantomData,
        })
    }

    // FIXME - dont do this anymore
    pub fn seek_to<S: Seek<C::Item>>(&mut self, mut seek: S) -> S::Output {
        loop {
            if let Some(iter) = &mut self.iter {
                if iter.seek(&mut seek) {
                    return seek.finish();
                } else {
                    self.pos += iter.len();
                    self.group += iter.max_group();
                    self.iter = self
                        .slabs
                        .and_then(|s| Some(s.get(self.slab_index)?.iter()));
                    self.slab_index += 1;
                }
            } else if let Some(slab) = self.slabs.and_then(|s| s.get(self.slab_index)) {
                self.iter = Some(slab.iter());
                self.slab_index += 1;
            } else {
                return seek.finish();
            }
        }
    }

    fn compute_len(&self) -> usize {
        let completed_slabs = self.pos;
        let future_slabs = self
            .slabs
            .iter()
            .skip(self.slab_index)
            .map(|s| s.len())
            .sum::<usize>();
        let current_slab = self.iter.as_ref().map(|i| i.len()).unwrap_or(0);
        completed_slabs + future_slabs + current_slab
    }

    pub fn pos(&self) -> usize {
        if let Some(iter) = &self.iter {
            self.pos + iter.pos()
        } else {
            self.pos
        }
    }

    pub fn group(&self) -> usize {
        if let Some(iter) = &self.iter {
            self.group + iter.group()
        } else {
            self.group
        }
    }
}

impl<'a, C: ColumnCursor> Iterator for ColumnDataIter<'a, C> {
    type Item = Option<<C::Item as Packable>::Unpacked<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.iter.is_none() {
            self.iter = Some(self.slabs?.get(self.slab_index)?.iter());
            self.slab_index += 1;
        }
        if self.pos() >= self.max {
            return None;
        }
        if let Some(iter) = &mut self.iter {
            if let Some(item) = iter.next() {
                Some(item)
            } else {
                assert_eq!(iter.pos(), iter.cursor.index());
                assert_eq!(iter.group(), iter.cursor.group());
                self.pos += iter.pos();
                self.group += iter.group();
                self.iter = None;
                self.next()
            }
        } else {
            None
        }
    }
}

impl<C: ColumnCursor> ColumnData<C> {
    pub fn to_vec(&self) -> Vec<C::Export> {
        C::to_vec(self.iter())
    }

    pub fn iter(&self) -> ColumnDataIter<'_, C> {
        ColumnDataIter::<C> {
            pos: 0,
            group: 0,
            max: self.len,
            slab_index: 0,
            slabs: Some(&self.slabs),
            iter: None,
        }
    }

    pub fn iter_range<'a>(&'a self, range: &Range<usize>) -> ColumnDataIter<'a, C> {
        let mut iter = ColumnDataIter::<C> {
            pos: 0,
            group: 0,
            max: range.end,
            slab_index: 0,
            slabs: Some(&self.slabs),
            iter: None,
        };
        if range.start > 0 {
            iter.advance_by(range.start);
            debug_assert_eq!(std::cmp::min(range.start, iter.compute_len()), iter.pos());
        }
        iter
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

    pub fn splice<E>(&mut self, index: usize, del: usize, values: Vec<E>)
    where
        E: MaybePackable<C::Item> + Debug + Clone,
    {
        assert!(index <= self.len);
        assert!(!self.slabs.is_empty());
        if values.is_empty() && del == 0 {
            return;
        }
        #[cfg(debug_assertions)]
        C::export_splice(
            &mut self.debug,
            index..(index + del),
            values.iter().map(|e| e.maybe_packable()),
        );
        #[cfg(debug_assertions)]
        let tmp_values = values.clone();

        let cursor = self
            .slabs
            .get_where(|c, next| index - c.pos < next.pos)
            .unwrap();
        match C::splice(cursor.element, index - cursor.weight.pos, del, values) {
            SpliceResult::Replace(add, del, slabs) => {
                self.len = self.len + add - del;
                self.slabs.splice(cursor.index..(cursor.index + 1), slabs);
                assert!(!self.slabs.is_empty());
            }
        }

        #[cfg(debug_assertions)]
        if self.debug != self.to_vec() {
            let col = self.to_vec();
            println!(":::SPLICE FAIL (index={} del={}):::", index, del);
            println!(":::values={:?}", tmp_values);
            let range = (index - 3)..(index + 4);
            println!(":::DBG={:?}", &self.debug[range.clone()]);
            println!(":::COL={:?}", &col[range.clone()]);
            assert_eq!(self.debug.len(), col.len());
            for (i, dbg) in col.iter().enumerate() {
                if dbg != &col[i] {
                    panic!("index={} {:?} vs {:?}", i, dbg, col[i]);
                }
            }
            panic!()
        }
    }
    pub fn init_empty(len: usize) -> Self {
        let new_slabs = C::init_empty(len);
        let mut slabs = SlabTree::new();
        slabs.splice(0..0, new_slabs);
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
            let len = rng.gen::<usize>() % 4 + 1;
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
        //let seed = rand::random::<u64>();
        let seed = 7796233028731974218;
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
            2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 6, 6, 6, 7, 8, 8,
        ];
        let mut col = ColumnData::<RleCursor<4, u64>>::new();
        col.splice(0, 0, data);
        let range = col.iter().scope_to_value(Some(4), ..);
        assert_eq!(range, 7..15);

        let range = col.iter().scope_to_value(Some(4), ..11);
        assert_eq!(range, 7..11);
        let range = col.iter().scope_to_value(Some(4), ..8);
        assert_eq!(range, 7..8);
        let range = col.iter().scope_to_value(Some(4), 0..1);
        assert_eq!(range, 1..1);
        let range = col.iter().scope_to_value(Some(4), 8..9);
        assert_eq!(range, 8..9);
        let range = col.iter().scope_to_value(Some(4), 9..);
        assert_eq!(range, 9..15);
        let range = col.iter().scope_to_value(Some(4), 14..16);
        assert_eq!(range, 14..15);

        let range = col.iter().scope_to_value(Some(2), ..);
        assert_eq!(range, 0..3);
        let range = col.iter().scope_to_value(Some(7), ..);
        assert_eq!(range, 22..23);
        let range = col.iter().scope_to_value(Some(8), ..);
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
}
