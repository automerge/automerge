use crate::storage::{ColumnSpec, ColumnType};
use crate::types::ActorId;

use super::boolean::BooleanCursor;
use super::cursor::{ColumnCursor, NextSlab, Run, ScanMeta, SpliceResult};
use super::delta::DeltaCursor;
use super::meta::MetaCursor;
use super::pack::{MaybePackable, PackError, Packable};
use super::raw::{RawCursor, RawReader};
use super::rle::{IntCursor, StrCursor};
use super::slab::{RunStep, Seek, Slab, SlabIter, SlabWriter};
use super::types::{normalize_range, ActionCursor, ActorCursor};

use std::cmp::PartialOrd;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::{Range, RangeBounds};
use std::sync::Arc;

#[derive(Debug, Default, Clone)]
pub(crate) struct ColumnData<C: ColumnCursor> {
    len: usize,
    slabs: Vec<Slab>,
    _phantom: PhantomData<C>,
}

impl<C: ColumnCursor> ColumnData<C> {
    pub(crate) fn is_empty(&self) -> bool {
        let run = self.iter().next_run();
        match run {
            None => true,
            Some(run) if run.count != self.len => false,
            Some(run) => C::is_empty(run.value),
        }
    }

    pub(crate) fn dump(&self) {
        let data = self.to_vec();
        log!(" :: {:?}", data);
    }

    pub(crate) fn write(&self, out: &mut Vec<u8>) -> Range<usize> {
        let start = out.len();
        let mut state = C::State::default();
        let mut writer = SlabWriter::new(usize::MAX);
        // TODO - if just 1 slab - copy it
        for s in &self.slabs {
            state = C::write(&mut writer, s, state);
        }
        C::write_finish(out, writer, state);
        let end = out.len();
        start..end
    }

    #[allow(clippy::while_let_on_iterator)]
    pub(crate) fn raw_reader(&self, mut advance: usize) -> RawReader<'_> {
        let mut reader = RawReader {
            slabs: self.slabs.iter(),
            current: None,
        };
        if advance > 0 {
            while let Some(s) = reader.slabs.next() {
                if s.len() < advance {
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
pub(crate) struct ColumnDataIter<'a, C: ColumnCursor> {
    pos: usize,
    group: usize,
    max: usize,
    slabs: NextSlab<'a>,
    iter: Option<SlabIter<'a, C>>,
}

impl<'a, C: ColumnCursor> ColumnDataIter<'a, C> {
    pub(crate) fn end_pos(&self) -> usize {
        self.max
    }

    pub(crate) fn next_run(&mut self) -> Option<Run<'a, C::Item>> {
        if self.iter.is_none() {
            if let Some(slab) = self.slabs.next() {
                self.iter = Some(slab.iter());
            }
        }
        if self.pos() >= self.max {
            return None;
        }
        if let Some(iter) = &mut self.iter {
            if let Some(run) = iter.next_run() {
                Some(run)
            } else {
                assert_eq!(iter.pos(), iter.cursor.index());
                //assert_eq!(iter.group(), iter.cursor.group());
                self.pos += iter.pos();
                self.group += iter.group();
                self.iter = None;
                self.next_run()
            }
        } else {
            None
        }
    }

    pub(crate) fn empty() -> Self {
        ColumnDataIter {
            pos: 0,
            group: 0,
            max: 0,
            slabs: NextSlab::new(&[]),
            iter: None,
        }
    }

    pub(crate) fn advance_by(&mut self, amount: usize) {
        struct SeekBy<T: ?Sized> {
            amount_left: usize,
            _phantom: PhantomData<T>,
        }

        impl<T: Packable + ?Sized> Seek<T> for SeekBy<T> {
            type Output = ();
            fn process_slab(&mut self, r: &Slab) -> RunStep {
                if r.len() < self.amount_left {
                    self.amount_left -= r.len();
                    RunStep::Skip
                } else {
                    RunStep::Process
                }
            }
            fn process_run(&mut self, r: &Run<'_, T>) -> RunStep {
                if r.count < self.amount_left {
                    self.amount_left -= r.count;
                    RunStep::Skip
                } else {
                    RunStep::Process
                }
            }
            fn process_element(&mut self, _e: Option<<T as Packable>::Unpacked<'_>>) {
                if self.amount_left > 0 {
                    self.amount_left -= 1;
                }
            }
            fn done<'a>(&self) -> bool {
                self.amount_left == 0
            }
            fn finish(self) -> Self::Output {}
        }

        self.seek_to(SeekBy {
            amount_left: amount + 1,
            _phantom: PhantomData,
        });
    }

    pub(crate) fn scope_to_value<
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
            fn process_slab(&mut self, r: &Slab) -> RunStep {
                if self.state == ScopeState::Seek && self.pos + r.len() <= self.start {
                    self.pos += r.len();
                    return RunStep::Skip;
                }
                RunStep::Process
            }
            fn process_run(&mut self, r: &Run<'_, T>) -> RunStep {
                match self.state {
                    ScopeState::Seek => {
                        if self.pos + r.count <= self.start {
                            // before start
                            self.pos += r.count;
                            RunStep::Skip
                        } else if self.pos >= self.max {
                            // after max
                            RunStep::Done
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
                                    RunStep::Done
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
                                        RunStep::Done
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
                            RunStep::Done
                        } else {
                            match (&self.target, &r.value) {
                                (Some(a), Some(b)) if a != b => RunStep::Done,
                                (a, b) if a.is_some() != b.is_some() => RunStep::Done,
                                _ => {
                                    self.pos += r.count;
                                    RunStep::Skip
                                }
                            }
                        }
                    }
                }
            }
            fn process_element(&mut self, _e: Option<<T as Packable>::Unpacked<'_>>) {
                panic!()
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

    pub(crate) fn seek_to<S: Seek<C::Item>>(&mut self, mut seek: S) -> S::Output {
        loop {
            if let Some(iter) = &mut self.iter {
                if iter.seek(&mut seek) {
                    return seek.finish();
                } else {
                    self.pos += iter.len();
                    self.group += iter.max_group();
                    //self.iter = self.slabs.next().map(|s| s.iter());
                    self.iter = self.slabs.next().map(|s| s.iter());
                }
            } else if let Some(slab) = self.slabs.next() {
                self.iter = Some(slab.iter());
            } else {
                return seek.finish();
            }
        }
    }

    fn len(&self) -> usize {
        let completed_slabs = self.pos;
        let future_slabs = self.slabs.map(|s| s.len()).sum::<usize>();
        let current_slab = self.iter.as_ref().map(|i| i.len()).unwrap_or(0);
        completed_slabs + future_slabs + current_slab
    }

    pub(crate) fn pos(&self) -> usize {
        if let Some(iter) = &self.iter {
            self.pos + iter.pos()
        } else {
            self.pos
        }
    }

    pub(crate) fn group(&self) -> usize {
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
            if let Some(slab) = self.slabs.next() {
                self.iter = Some(slab.iter());
            }
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
    pub(crate) fn to_vec(&self) -> Vec<C::Export> {
        self.iter().map(|i| C::export_item(i)).collect()
    }

    pub(crate) fn iter(&self) -> ColumnDataIter<'_, C> {
        ColumnDataIter::<C> {
            pos: 0,
            group: 0,
            max: self.len,
            slabs: NextSlab::new(&self.slabs),
            iter: None,
        }
    }

    pub(crate) fn iter_range<'a>(&'a self, range: &Range<usize>) -> ColumnDataIter<'a, C> {
        let mut iter = ColumnDataIter::<C> {
            pos: 0,
            group: 0,
            max: range.end,
            slabs: NextSlab::new(&self.slabs),
            iter: None,
        };
        if range.start > 0 {
            iter.advance_by(range.start);
            assert_eq!(std::cmp::min(range.start, iter.len()), iter.pos());
        }
        iter
    }

    pub(crate) fn new() -> Self {
        ColumnData {
            len: 0,
            slabs: vec![Slab::default()],
            _phantom: PhantomData,
        }
    }

    #[cfg(test)]
    pub(crate) fn export(&self) -> Vec<Vec<super::cursor::ColExport<C::Item>>> {
        self.slabs.iter().map(|s| C::export(s.as_ref())).collect()
    }

    pub(crate) fn splice<E>(&mut self, index: usize, del: usize, values: Vec<E>)
    where
        E: MaybePackable<C::Item> + Debug + Clone,
    {
        assert!(index <= self.len);
        assert!(!self.slabs.is_empty());
        if values.is_empty() && del == 0 {
            return;
        }
        let before = self.to_vec();
        let tmp_values = values.clone();
        let mut slab_offset = 0;
        for (i, slab) in self.slabs.iter_mut().enumerate() {
            if slab.len() < index - slab_offset {
                slab_offset += slab.len();
            } else {
                match C::splice(slab, index - slab_offset, del, values) {
                    /*
                                        SpliceResult::Done(add, del) => {
                                            self.len = self.len + add - del;
                                        }
                                        SpliceResult::Add(add, del, slabs) => {
                                            self.len = self.len + add - del;
                                            let j = i + 1;
                                            self.slabs.splice(j..j, slabs);
                                            assert!(self.slabs.len() > 0);
                                        }
                    */
                    SpliceResult::Replace(add, del, slabs) => {
                        self.len = self.len + add - del;
                        let j = i + 1;
                        self.slabs.splice(i..j, slabs);
                        assert!(!self.slabs.is_empty());
                    }
                }
                break;
            }
        }

        let after = self.to_vec();
        let slab_len = self.slabs.iter().map(|s| s.len()).sum::<usize>();
        if self.len != after.len() || self.len != slab_len {
            log!(":::SPLICE FAIL (index={}):::", index);
            log!(
                "before.len={} after.len={} slabs.len={}",
                before.len(),
                self.len(),
                slab_len
            );
            log!("SLABS={:?}", self.slabs);
            log!(
                "::: self.len({}) != after.len({}) :::",
                self.len,
                after.len()
            );
            log!(":::before={:?}", before);
            log!(":::values={:?}", tmp_values);
            log!(":::after={:?}", after);
            panic!()
        }
    }
    pub(crate) fn init_empty(len: usize) -> Self {
        let slabs = C::init_empty(len);
        ColumnData {
            len,
            slabs,
            _phantom: PhantomData,
        }
    }

    pub(crate) fn external(
        data: Arc<Vec<u8>>,
        range: Range<usize>,
        m: &ScanMeta,
    ) -> Result<Self, PackError> {
        let slab = Slab::external::<C>(data, range, m)?;
        let len = slab.len();
        Ok(ColumnData {
            len,
            slabs: vec![slab],
            _phantom: PhantomData,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }
}

#[derive(Debug, Clone)]
pub(crate) enum Column {
    Actor(ColumnData<ActorCursor>),
    Str(ColumnData<StrCursor>),
    Integer(ColumnData<IntCursor>),
    Action(ColumnData<ActionCursor>),
    Delta(ColumnData<DeltaCursor>),
    Bool(ColumnData<BooleanCursor>),
    ValueMeta(ColumnData<MetaCursor>),
    Value(ColumnData<RawCursor>),
    Group(ColumnData<IntCursor>),
}

impl Column {
    // FIXME
    /*
        pub(crate) fn splice(&mut self, mut index: usize, op: &OpBuilder) {
            todo!()
            match self {
                Self::Actor(col) => col.write(out),
                Self::Str(col) => col.write(out),
                Self::Integer(col) => col.write(out),
                Self::Delta(col) => col.write(out),
                Self::Bool(col) => col.write(out),
                Self::ValueMeta(col) => col.write(out),
                Self::Value(col) => col.write(out),
                Self::Group(col) => col.write(out),
                Self::Action(col) => col.write(out),
            }
        }
    */

    pub(crate) fn write(&self, out: &mut Vec<u8>) -> Range<usize> {
        match self {
            Self::Actor(col) => col.write(out),
            Self::Str(col) => col.write(out),
            Self::Integer(col) => col.write(out),
            Self::Delta(col) => col.write(out),
            Self::Bool(col) => col.write(out),
            Self::ValueMeta(col) => col.write(out),
            Self::Value(col) => col.write(out),
            Self::Group(col) => col.write(out),
            Self::Action(col) => col.write(out),
        }
    }

    pub(crate) fn slabs(&self) -> &[Slab] {
        match self {
            Self::Actor(col) => col.slabs.as_slice(),
            Self::Str(col) => col.slabs.as_slice(),
            Self::Integer(col) => col.slabs.as_slice(),
            Self::Delta(col) => col.slabs.as_slice(),
            Self::Bool(col) => col.slabs.as_slice(),
            Self::ValueMeta(col) => col.slabs.as_slice(),
            Self::Value(col) => col.slabs.as_slice(),
            Self::Group(col) => col.slabs.as_slice(),
            Self::Action(col) => col.slabs.as_slice(),
        }
    }

    #[allow(unused)]
    pub(crate) fn dump(&self) {
        match self {
            Self::Actor(col) => col.dump(),
            Self::Str(col) => col.dump(),
            Self::Integer(col) => col.dump(),
            Self::Delta(col) => col.dump(),
            Self::Bool(col) => col.dump(),
            Self::ValueMeta(col) => col.dump(),
            Self::Value(col) => col.dump(),
            Self::Group(col) => col.dump(),
            Self::Action(col) => col.dump(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        match self {
            Self::Actor(col) => col.is_empty(),
            Self::Str(col) => col.is_empty(),
            Self::Integer(col) => col.is_empty(),
            Self::Delta(col) => col.is_empty(),
            Self::Bool(col) => col.is_empty(),
            Self::ValueMeta(col) => col.is_empty(),
            Self::Value(col) => col.is_empty(),
            Self::Group(col) => col.is_empty(),
            Self::Action(col) => col.is_empty(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Actor(col) => col.len,
            Self::Str(col) => col.len,
            Self::Integer(col) => col.len,
            Self::Delta(col) => col.len,
            Self::Bool(col) => col.len,
            Self::ValueMeta(col) => col.len,
            Self::Value(col) => col.len,
            Self::Group(col) => col.len,
            Self::Action(col) => col.len,
        }
    }

    pub(crate) fn new(spec: ColumnSpec) -> Self {
        match spec.col_type() {
            ColumnType::Actor => Column::Actor(ColumnData::new()),
            ColumnType::String => Column::Str(ColumnData::new()),
            ColumnType::Integer => {
                if spec.id() == super::op_set::ACTION_COL_ID {
                    Column::Action(ColumnData::new())
                } else {
                    Column::Integer(ColumnData::new())
                }
            }
            ColumnType::DeltaInteger => Column::Delta(ColumnData::new()),
            ColumnType::Boolean => Column::Bool(ColumnData::new()),
            ColumnType::Group => Column::Group(ColumnData::new()),
            ColumnType::ValueMetadata => Column::ValueMeta(ColumnData::new()),
            ColumnType::Value => Column::Value(ColumnData::new()),
        }
    }

    pub(crate) fn external(
        spec: ColumnSpec,
        data: Arc<Vec<u8>>,
        range: Range<usize>,
        actors: &[ActorId],
    ) -> Result<Self, PackError> {
        let m = ScanMeta {
            actors: actors.len(),
        };
        match spec.col_type() {
            ColumnType::Actor => Ok(Column::Actor(ColumnData::external(data, range, &m)?)),
            ColumnType::String => Ok(Column::Str(ColumnData::external(data, range, &m)?)),
            ColumnType::Integer => {
                if spec.id() == super::op_set::ACTION_COL_ID {
                    Ok(Column::Action(ColumnData::external(data, range, &m)?))
                } else {
                    Ok(Column::Integer(ColumnData::external(data, range, &m)?))
                }
            }
            ColumnType::DeltaInteger => Ok(Column::Delta(ColumnData::external(data, range, &m)?)),
            ColumnType::Boolean => Ok(Column::Bool(ColumnData::external(data, range, &m)?)),
            ColumnType::Group => Ok(Column::Group(ColumnData::external(data, range, &m)?)),
            ColumnType::ValueMetadata => {
                Ok(Column::ValueMeta(ColumnData::external(data, range, &m)?))
            }
            ColumnType::Value => Ok(Column::Value(ColumnData::external(data, range, &m)?)),
        }
    }

    pub(crate) fn init_empty(spec: ColumnSpec, len: usize) -> Self {
        match spec.col_type() {
            ColumnType::Actor => Column::Actor(ColumnData::init_empty(len)),
            ColumnType::String => Column::Str(ColumnData::init_empty(len)),
            ColumnType::Integer => {
                if spec.id() == super::op_set::ACTION_COL_ID {
                    Column::Action(ColumnData::init_empty(len))
                } else {
                    Column::Integer(ColumnData::init_empty(len))
                }
            }
            ColumnType::DeltaInteger => Column::Delta(ColumnData::init_empty(len)),
            ColumnType::Boolean => Column::Bool(ColumnData::init_empty(len)),
            ColumnType::Group => Column::Group(ColumnData::init_empty(len)),
            ColumnType::ValueMetadata => Column::ValueMeta(ColumnData::init_empty(len)),
            ColumnType::Value => Column::Value(ColumnData::init_empty(len)),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::super::cursor::ColExport;
    use super::super::rle::RleCursor;
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
            let actual = iter.map(|e| C::export_item(e)).collect::<Vec<_>>();
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
        let seed = rand::random::<u64>();
        //let seed = 7798599467530965361;
        log!("SEED: {}", seed);
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
}
