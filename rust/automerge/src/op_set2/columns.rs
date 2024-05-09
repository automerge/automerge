use crate::storage::{ColumnSpec, ColumnType};

use super::{
    BooleanCursor, DeltaCursor, GroupCursor, IntCursor, MaybePackable, MetaCursor, PackError,
    Packable, RawCursor, RleState, Slab, SlabIter, StrCursor, WritableSlab,
};

use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::Range;
use std::sync::Arc;

#[derive(Debug, Clone, Copy)]
pub(crate) struct Run<'a, T: Packable + ?Sized> {
    pub(crate) count: usize,
    pub(crate) value: Option<T::Unpacked<'a>>,
}

impl<'a, T: Packable + ?Sized> From<Run<'a, T>> for RleState<'a, T> {
    fn from(r: Run<'a, T>) -> Self {
        if r.count == 1 {
            RleState::LoneValue(r.value)
        } else {
            RleState::Run {
                count: r.count,
                value: r.value,
            }
        }
    }
}

impl<'a> Run<'a, i64> {
    pub(crate) fn delta(&self) -> i64 {
        self.count as i64 * self.value.unwrap_or(0)
    }
}

impl<'a> Run<'a, u64> {
    fn sum(&self) -> u64 {
        self.count as u64 * self.value.unwrap_or(0)
    }
}

impl<'a, T: Packable + ?Sized> Run<'a, T> {
    pub(crate) fn new(count: usize, value: Option<T::Unpacked<'a>>) -> Self {
        Run { count, value }
    }

    pub(crate) fn plus(mut self, num: usize) -> Self {
        self.count += num;
        self
    }
}

#[derive(Debug)]
pub(crate) struct Encoder<'a, C: ColumnCursor> {
    pub(crate) slab: &'a Slab,
    pub(crate) state: C::State<'a>,
    pub(crate) current: WritableSlab,
    pub(crate) post: C::PostState<'a>,
    pub(crate) cursor: C,
    pub(crate) results: Vec<Slab>,
}

impl<'a, C: ColumnCursor> Encoder<'a, C> {
    fn append(&mut self, v: Option<<C::Item as Packable>::Unpacked<'a>>) {
        C::append(&mut self.state, &mut self.current, v);
    }

    fn finish(mut self) -> Vec<Slab> {
        C::finish(
            &self.slab,
            &mut self.current,
            self.state,
            self.post,
            self.cursor,
        );
        self.current.finish()
    }
}

#[derive(Debug, Default, Clone)]
pub(crate) struct ColumnData<C: ColumnCursor> {
    len: usize,
    slabs: Vec<Slab>,
    _phantom: PhantomData<C>,
}

#[derive(Debug)]
pub(crate) struct ColumnDataIter<'a, C: ColumnCursor> {
    slabs: std::slice::Iter<'a, Slab>,
    iter: Option<SlabIter<'a, C>>,
}

impl<'a, C: ColumnCursor> Iterator for ColumnDataIter<'a, C> {
    type Item = Option<<C::Item as Packable>::Unpacked<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.iter.is_none() {
            if let Some(slab) = self.slabs.next() {
                self.iter = Some(slab.iter());
            }
        }
        if let Some(iter) = &mut self.iter {
            if let Some(item) = iter.next() {
                Some(item)
            } else {
                self.iter = None;
                self.next()
            }
        } else {
            None
        }
    }
}

impl<C: ColumnCursor> ColumnData<C> {
    pub(crate) fn to_vec<'a>(&'a self) -> Vec<C::Export> {
        self.iter().map(|i| C::export_item(i)).collect()
    }

    pub(crate) fn iter<'a>(&'a self) -> ColumnDataIter<'a, C> {
        ColumnDataIter::<C> {
            slabs: self.slabs.iter(),
            iter: None,
        }
    }

    pub(crate) fn new() -> Self {
        ColumnData {
            len: 0,
            slabs: vec![Slab::default()],
            _phantom: PhantomData,
        }
    }

    pub(crate) fn export(&self) -> Vec<Vec<ColExport<C::Item>>> {
        self.slabs.iter().map(|s| C::export(s.as_ref())).collect()
    }

    pub(crate) fn splice<E>(&mut self, mut index: usize, values: Vec<E>)
    where
        E: MaybePackable<C::Item>,
    {
        assert!(index <= self.len);
        for (i, slab) in self.slabs.iter_mut().enumerate() {
            if slab.len() < index {
                index -= slab.len();
            } else {
                self.len += values.len();
                match C::splice(slab, index, values) {
                    SpliceResult::Done => (),
                    SpliceResult::Add(slabs) => {
                        let j = i + 1;
                        self.slabs.splice(j..j, slabs);
                    }
                    SpliceResult::Replace(slabs) => {
                        let j = i + 1;
                        self.slabs.splice(i..j, slabs);
                    }
                }
                break;
            }
        }
    }

    pub(crate) fn external(data: Arc<Vec<u8>>, range: Range<usize>) -> Result<Self, PackError> {
        let slab = Slab::external::<C>(data, range)?;
        let len = 0;
        Ok(ColumnData {
            len,
            slabs: vec![slab],
            _phantom: PhantomData,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ColExport<P: Packable + ?Sized> {
    LitRun(Vec<P::Owned>),
    Run(usize, P::Owned),
    Null(usize),
}

impl<P: Packable + ?Sized> ColExport<P> {
    pub(crate) fn litrun(items: Vec<P::Unpacked<'_>>) -> Self {
        Self::LitRun(items.into_iter().map(|i| P::own(i)).collect())
    }
    pub(crate) fn run(count: usize, item: P::Unpacked<'_>) -> Self {
        Self::Run(count, P::own(item))
    }
}

pub(crate) trait ColumnCursor: Debug + Default + Copy {
    type Item: Packable + ?Sized;
    type State<'a>: Default;
    type PostState<'a>;
    type Export: Debug + PartialEq + Clone;

    fn pop<'a>(
        &self,
        mut run: Run<'a, Self::Item>,
    ) -> (
        Option<<Self::Item as Packable>::Unpacked<'a>>,
        Option<Run<'a, Self::Item>>,
    ) {
        let value = run.value;
        run.count -= 1;
        if run.count > 0 {
            (value, Some(run))
        } else {
            (value, None)
        }
    }

    fn finish<'a>(
        slab: &'a Slab,
        out: &mut WritableSlab,
        state: Self::State<'a>,
        post: Self::PostState<'a>,
        cursor: Self,
    );

    fn append<'a>(
        state: &mut Self::State<'a>,
        slab: &mut WritableSlab,
        item: Option<<Self::Item as Packable>::Unpacked<'a>>,
    );

    fn encode<'a>(index: usize, slab: &'a Slab) -> Encoder<'a, Self>;

    fn try_next<'a>(
        &self,
        data: &'a [u8],
    ) -> Result<Option<(Run<'a, Self::Item>, Self)>, PackError>;

    fn export(data: &[u8]) -> Vec<ColExport<Self::Item>>;

    fn export_item(item: Option<<Self::Item as Packable>::Unpacked<'_>>) -> Self::Export;

    fn next<'a>(&self, data: &'a [u8]) -> Option<(Run<'a, Self::Item>, Self)> {
        match self.try_next(data) {
            // need one interface that throws away zero length runs (used by bool columns)
            // and one interface that does not
            // this throws out the zero length runs to not complicate the iterator
            Ok(Some((run, cursor))) if run.count == 0 => cursor.next(data),
            Ok(result) => result,
            _ => None,
        }
    }

    fn index(&self) -> usize;

    fn seek<'a>(index: usize, data: &'a [u8]) -> (Option<Run<'a, Self::Item>>, Self) {
        if index == 0 {
            return (None, Self::default());
        } else {
            let mut cursor = Self::default();
            while let Some((val, next_cursor)) = cursor.next(data) {
                if next_cursor.index() >= index {
                    return (Some(val), next_cursor);
                }
                cursor = next_cursor;
            }
        }
        panic!() // we reached the end of the buffer without finding our item - return an error
    }

    fn scan(data: &[u8]) -> Result<Self, PackError> {
        let mut cursor = Self::default();
        while let Some((_val, next_cursor)) = cursor.try_next(data)? {
            cursor = next_cursor
        }
        Ok(cursor)
    }

    fn splice<E>(slab: &mut Slab, index: usize, values: Vec<E>) -> SpliceResult
    where
        E: MaybePackable<Self::Item>,
    {
        let mut encoder = Self::encode(index, slab);
        for v in &values {
            encoder.append(v.maybe_packable())
        }
        SpliceResult::Replace(encoder.finish())
    }
}

#[derive(Debug, Clone)]
pub(crate) enum Column {
    Actor(ColumnData<IntCursor>),
    Str(ColumnData<StrCursor>),
    Integer(ColumnData<IntCursor>),
    Delta(ColumnData<DeltaCursor>),
    Bool(ColumnData<BooleanCursor>),
    ValueMeta(ColumnData<MetaCursor>),
    Value(ColumnData<RawCursor>),
    Group(ColumnData<GroupCursor>),
}

impl Column {
    pub(crate) fn write(&self, out: &mut Vec<u8>) -> Range<usize> {
        let start = out.len();
        // FIXME more complex once i start splitting slabs
        for s in self.slabs() {
            out.extend(s.as_ref())
        }
        let end = out.len();
        start..end
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
        }
    }

    pub(crate) fn external(
        spec: ColumnSpec,
        data: Arc<Vec<u8>>,
        range: Range<usize>,
    ) -> Result<Self, PackError> {
        match spec.col_type() {
            ColumnType::Actor => Ok(Column::Actor(ColumnData::external(data, range)?)),
            ColumnType::String => Ok(Column::Str(ColumnData::external(data, range)?)),
            ColumnType::Integer => Ok(Column::Integer(ColumnData::external(data, range)?)),
            ColumnType::DeltaInteger => Ok(Column::Delta(ColumnData::external(data, range)?)),
            ColumnType::Boolean => Ok(Column::Bool(ColumnData::external(data, range)?)),
            ColumnType::Group => Ok(Column::Group(ColumnData::external(data, range)?)),
            ColumnType::ValueMetadata => Ok(Column::ValueMeta(ColumnData::external(data, range)?)),
            ColumnType::Value => Ok(Column::Value(ColumnData::external(data, range)?)),
        }
    }
}

pub(crate) enum SpliceResult {
    Done,
    Add(Vec<Slab>),
    Replace(Vec<Slab>),
}

#[cfg(test)]
pub(crate) mod tests {
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
        col.splice(index, values);
        assert_eq!(vec, &col.to_vec());
    }

    #[test]
    fn column_data_breaking_literal_runs_in_int_column() {
        let numbers = vec![1, 2, 3];
        let mut start = ColumnData::<IntCursor>::new();
        start.splice(0, numbers);
        assert_eq!(start.export(), vec![vec![ColExport::LitRun(vec![1, 2, 3])]]);
        let mut col = start.clone();
        col.splice(2, vec![3, 3, 3]);
        assert_eq!(
            col.export(),
            vec![vec![ColExport::LitRun(vec![1, 2]), ColExport::Run(4, 3)]]
        );
        let mut col = start.clone();
        col.splice(3, vec![3, 3, 3]);
        assert_eq!(
            col.export(),
            vec![vec![ColExport::LitRun(vec![1, 2]), ColExport::Run(4, 3)]]
        );
        let mut col = start.clone();
        col.splice(1, vec![2, 2]);
        assert_eq!(
            col.export(),
            vec![vec![
                ColExport::LitRun(vec![1]),
                ColExport::Run(3, 2),
                ColExport::LitRun(vec![3]),
            ]]
        );
        let mut col = start.clone();
        col.splice(2, vec![2, 2]);
        assert_eq!(
            col.export(),
            vec![vec![
                ColExport::LitRun(vec![1]),
                ColExport::Run(3, 2),
                ColExport::LitRun(vec![3]),
            ]]
        );
        let mut col = start.clone();
        col.splice(0, vec![1, 1]);
        assert_eq!(
            col.export(),
            vec![vec![ColExport::Run(3, 1), ColExport::LitRun(vec![2, 3]),]]
        );
        let mut col = start.clone();
        col.splice(1, vec![1, 1]);
        assert_eq!(
            col.export(),
            vec![vec![ColExport::Run(3, 1), ColExport::LitRun(vec![2, 3]),]]
        );
    }

    #[test]
    fn column_data_breaking_runs_in_int_column() {
        let numbers = vec![2, 2, 2];
        let mut start = ColumnData::<IntCursor>::new();
        start.splice(0, numbers);
        assert_eq!(start.export(), vec![vec![ColExport::Run(3, 2)]]);
        let mut col = start.clone();
        col.splice(1, vec![3, 3, 3]);
        assert_eq!(
            col.export(),
            vec![vec![
                ColExport::LitRun(vec![2]),
                ColExport::Run(3, 3),
                ColExport::Run(2, 2),
            ]]
        );
        let mut col = start.clone();
        col.splice(2, vec![3, 3, 3]);
        assert_eq!(
            col.export(),
            vec![vec![
                ColExport::Run(2, 2),
                ColExport::Run(3, 3),
                ColExport::LitRun(vec![2]),
            ]]
        );
        let mut col = start.clone();
        col.splice(0, vec![3, 3, 3]);
        assert_eq!(
            col.export(),
            vec![vec![ColExport::Run(3, 3), ColExport::Run(3, 2),]]
        );
        let mut col = start.clone();
        col.splice(3, vec![3, 3, 3]);
        assert_eq!(
            col.export(),
            vec![vec![ColExport::Run(3, 2), ColExport::Run(3, 3),]]
        );
    }

    #[test]
    fn column_data_breaking_null_runs_in_int_column() {
        let numbers = vec![None, None, Some(2), Some(2), None, None, None];
        let mut start = ColumnData::<IntCursor>::new();
        start.splice(0, numbers);
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
        col.splice(2, vec![None, None, Some(2), Some(2)]);
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
        col.splice(8, vec![Some(2), Some(2), None, None]);
        assert_eq!(
            col.export(),
            vec![vec![
                ColExport::Null(4),
                ColExport::Run(6, 2),
                ColExport::Null(5)
            ]]
        );
        col.splice(4, vec![None, Some(2), Some(3)]);
        assert_eq!(
            col.export(),
            vec![vec![
                ColExport::Null(5),
                ColExport::LitRun(vec![2, 3]),
                ColExport::Run(6, 2),
                ColExport::Null(5)
            ]]
        );
        col.splice(2, vec![4]);
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
        col.splice(6, vec![None, None, Some(2), Some(2)]);
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
        col.splice(15, vec![5, 6]);
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
        start.splice(0, strings);
        assert_eq!(
            start.export(),
            vec![vec![ColExport::litrun(vec!["one", "two", "three"])]]
        );
        let mut col = start.clone();
        col.splice(1, vec![None, None, Some("two"), Some("two")]);
        assert_eq!(
            col.export(),
            vec![vec![
                ColExport::litrun(vec!["one"]),
                ColExport::Null(2),
                ColExport::run(3, "two"),
                ColExport::litrun(vec!["three"]),
            ]]
        );
        col.splice(0, vec![None, None, Some("three"), Some("one")]);
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
        start.splice(0, numbers.clone());
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
        col.splice(1, vec![2]);
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
        col.splice(0, vec![0]);
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
            for i in 0..len {
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
        fn plus(&self, index: usize) -> bool {
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
        let mut col = ColumnData::<IntCursor>::new();
        let mut rng = make_rng();
        for i in 0..1000 {
            let (index, values) = generate_splice(data.len(), &mut rng);
            test_splice(&mut data, &mut col, 0, values);
        }
    }

    #[test]
    fn column_data_str_fuzz_test() {
        let mut data: Vec<Option<String>> = vec![];
        let mut col = ColumnData::<StrCursor>::new();
        let mut rng = make_rng();
        for i in 0..100 {
            let (index, values) = generate_splice(data.len(), &mut rng);
            test_splice(&mut data, &mut col, 0, values);
        }
    }

    #[test]
    fn column_data_fuzz_test_delta() {
        let mut data: Vec<Option<i64>> = vec![];
        let mut col = ColumnData::<DeltaCursor>::new();
        let mut rng = make_rng();
        for i in 0..100 {
            let (index, values) = generate_splice(data.len(), &mut rng);
            test_splice(&mut data, &mut col, 0, values);
        }
    }

    #[test]
    fn column_data_test_boolean() {
        let mut data: Vec<bool> = vec![true, true, true];
        let mut col = ColumnData::<BooleanCursor>::new();
        col.splice(0, data.clone());
        assert_eq!(col.export(), vec![vec![ColExport::Run(3, true)]]);
        col.splice(0, vec![false, false, false]);
        assert_eq!(
            col.export(),
            vec![vec![ColExport::Run(3, false), ColExport::Run(3, true)]]
        );
        col.splice(6, vec![false, false, false]);
        assert_eq!(
            col.export(),
            vec![vec![
                ColExport::Run(3, false),
                ColExport::Run(3, true),
                ColExport::Run(3, false),
            ]]
        );
        col.splice(9, vec![true, true, true]);
        assert_eq!(
            col.export(),
            vec![vec![
                ColExport::Run(3, false),
                ColExport::Run(3, true),
                ColExport::Run(3, false),
                ColExport::Run(3, true),
            ]]
        );
        col.splice(0, vec![true, true, true]);
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
        col.splice(1, vec![false, false, false]);
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
        for i in 0..100 {
            let (index, values) = generate_splice(data.len(), &mut rng);
            test_splice(&mut data, &mut col, 0, values);
        }
    }
}
