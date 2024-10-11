use super::pack::{MaybePackable, PackError, Packable};
use super::slab::{Slab, SlabWeight, SlabWriter};

use std::fmt::Debug;
use std::ops::Range;

#[derive(Debug)]
pub struct ScanMeta {
    pub actors: usize,
}

#[derive(Debug, PartialEq, Default)]
pub struct Run<'a, P: Packable + ?Sized> {
    pub count: usize,
    pub value: Option<P::Unpacked<'a>>,
}

impl<'a, P: Packable + ?Sized> Copy for Run<'a, P> {}
impl<'a, P: Packable + ?Sized> Clone for Run<'a, P> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, P: Packable + ?Sized> Iterator for Run<'a, P> {
    type Item = Option<P::Unpacked<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.count >= 1 {
            self.count -= 1;
            Some(self.value)
        } else {
            None
        }
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        if self.count > n {
            self.count -= n + 1;
            Some(self.value)
        } else {
            self.count = 0;
            None
        }
    }
}

impl<'a, T: Packable + ?Sized> Run<'a, T> {
    pub(crate) fn pop_n(&self, n: usize) -> Option<Run<'a, T>> {
        if self.count <= n {
            None
        } else {
            let count = self.count - n;
            let value = self.value;
            Some(Run { count, value })
        }
    }

    pub(crate) fn pop(&self) -> Option<Run<'a, T>> {
        self.pop_n(1)
    }

    pub fn unit_group(&self) -> usize {
        self.value.as_ref().map(|i| T::group(*i)).unwrap_or(0)
    }
    pub fn group(&self) -> usize {
        self.count * self.unit_group()
    }

    pub(crate) fn weight_left(&self) -> SlabWeight {
        SlabWeight {
            pos: self.count,
            group: self.group(),
        }
    }
}

impl<'a> Run<'a, i64> {
    pub fn delta(&self) -> i64 {
        self.count as i64 * self.value.unwrap_or(0)
    }

    pub fn delta_minus_one(&self) -> i64 {
        (self.count as i64 - 1) * self.value.unwrap_or(0)
    }
}

impl<'a, T: Packable + ?Sized> Run<'a, T> {
    pub fn new(count: usize, value: Option<T::Unpacked<'a>>) -> Self {
        Run { count, value }
    }

    pub fn plus(mut self, num: usize) -> Self {
        self.count += num;
        self
    }
}

#[derive(Debug)]
pub struct Encoder<'a, C: ColumnCursor> {
    pub slab: &'a Slab,
    pub state: C::State<'a>,
    pub current: SlabWriter<'a>,
    pub post: C::PostState<'a>,
    pub group: usize,
    pub deleted: usize,
    pub overflow: usize,
    pub cursor: C,
}

impl<'a, C: ColumnCursor> Encoder<'a, C> {
    pub(crate) fn append(&mut self, v: Option<<C::Item as Packable>::Unpacked<'a>>) -> usize {
        C::append(&mut self.state, &mut self.current, v)
    }

    pub(crate) fn finish(mut self) -> Vec<Slab> {
        if let Some(cursor) = C::finalize_state(
            self.slab,
            &mut self.current,
            self.state,
            self.post,
            self.cursor,
        ) {
            C::finish(self.slab, &mut self.current, cursor)
        }
        self.current.finish()
    }
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq)]
pub enum ColExport<P: Packable + ?Sized> {
    LitRun(Vec<P::Owned>),
    Run(usize, P::Owned),
    Raw(Vec<u8>),
    Null(usize),
}

#[cfg(test)]
impl<P: Packable + ?Sized> ColExport<P> {
    pub(crate) fn litrun(items: Vec<P::Unpacked<'_>>) -> Self {
        Self::LitRun(items.into_iter().map(|i| P::own(i)).collect())
    }
    pub(crate) fn run(count: usize, item: P::Unpacked<'_>) -> Self {
        Self::Run(count, P::own(item))
    }
}

pub trait ColumnCursor: Debug + Clone + Copy {
    type Item: Packable + ?Sized;
    type State<'a>: Default;
    type PostState<'a>;
    type Export: Debug + PartialEq + Clone;

    fn empty() -> Self;

    fn new(_: &Slab) -> Self {
        Self::empty()
    }

    fn write<'a>(
        writer: &mut SlabWriter<'a>,
        slab: &'a Slab,
        mut state: Self::State<'a>,
    ) -> Self::State<'a> {
        let mut size = slab.len();

        if slab.is_empty() {
            return state;
        }

        let (run0, c0) = Self::seek(1, slab);
        let run0 = run0.unwrap();
        size -= run0.count;
        Self::append_chunk(&mut state, writer, run0);
        if size == 0 {
            return state;
        }

        let (run1, c1) = Self::seek(slab.len(), slab);
        let run1 = run1.unwrap();
        size -= run1.count;
        if size == 0 {
            Self::append_chunk(&mut state, writer, run1);
            return state;
        }
        Self::flush_state(writer, state);

        Self::copy_between(slab, writer, c0, c1, run1, size)
    }

    fn is_empty(v: Option<<Self::Item as Packable>::Unpacked<'_>>) -> bool {
        v.is_none()
    }

    fn transform<'a>(
        &self,
        run: &Run<'a, Self::Item>,
    ) -> Option<<Self::Item as Packable>::Unpacked<'a>> {
        run.value
    }

/*
    #[allow(clippy::type_complexity)]
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
*/

    fn finalize_state<'a>(
        slab: &'a Slab,
        out: &mut SlabWriter<'a>,
        state: Self::State<'a>,
        post: Self::PostState<'a>,
        cursor: Self,
    ) -> Option<Self>;

    fn finish<'a>(slab: &'a Slab, out: &mut SlabWriter<'a>, cursor: Self);

    fn append<'a>(
        state: &mut Self::State<'a>,
        out: &mut SlabWriter<'a>,
        value: Option<<Self::Item as Packable>::Unpacked<'a>>,
    ) -> usize {
        Self::append_chunk(state, out, Run { count: 1, value })
    }

    fn append_chunk<'a>(
        state: &mut Self::State<'a>,
        out: &mut SlabWriter<'a>,
        chunk: Run<'a, Self::Item>,
    ) -> usize;

    fn copy_between<'a>(
        slab: &'a Slab,
        out: &mut SlabWriter<'a>,
        c0: Self,
        c1: Self,
        run: Run<'a, Self::Item>,
        size: usize,
    ) -> Self::State<'a>;

    fn flush_state<'a>(out: &mut SlabWriter<'a>, state: Self::State<'a>);

    fn encode(index: usize, del: usize, slab: &Slab) -> Encoder<'_, Self>;

    #[allow(clippy::type_complexity)]
    fn try_next<'a>(
        &self,
        data: &'a [u8],
    ) -> Result<Option<(Run<'a, Self::Item>, Self)>, PackError>;

    #[cfg(test)]
    fn export(data: &[u8]) -> Vec<ColExport<Self::Item>>;

    fn to_vec<'a, I>(values: I) -> Vec<Self::Export>
    where
        I: Iterator<Item = Option<<Self::Item as Packable>::Unpacked<'a>>>,
    {
        let mut result = vec![];
        Self::export_splice(&mut result, 0..0, values);
        result
    }

    fn export_splice<'a, I>(data: &mut Vec<Self::Export>, range: Range<usize>, values: I)
    where
        I: Iterator<Item = Option<<Self::Item as Packable>::Unpacked<'a>>>;

    // useful for debugging
    fn decode(data: &[u8]) {
        let mut cursor = Self::empty();
        loop {
            match cursor.try_next(data) {
                Ok(Some((_run, next_cursor))) => {
                    cursor = next_cursor;
                }
                Ok(None) => break,
                Err(_) => {
                    break;
                }
            }
        }
    }

    fn next<'a>(&self, data: &'a [u8]) -> Option<(Run<'a, Self::Item>, Self)> {
        match self.try_next(data).unwrap() {
            // need one interface that throws away zero length runs (used by bool columns)
            // and one interface that does not
            // this throws out the zero length runs to not complicate the iterator
            Some((run, cursor)) if run.count == 0 => cursor.next(data),
            result => result,
            //_ => None,
        }
    }

    fn index(&self) -> usize;

    fn group(&self) -> usize {
        0
    }

    fn seek(index: usize, slab: &Slab) -> (Option<Run<'_, Self::Item>>, Self) {
        if index == 0 {
            return (None, Self::new(slab));
        } else {
            let mut cursor = Self::new(slab);
            while let Some((val, next_cursor)) = cursor.next(slab.as_slice()) {
                if next_cursor.index() >= index {
                    return (Some(val), next_cursor);
                }
                cursor = next_cursor;
            }
        }
        panic!()
    }

    fn scan(data: &[u8], m: &ScanMeta) -> Result<Self, PackError> {
        let mut cursor = Self::empty();
        while let Some((val, next_cursor)) = cursor.try_next(data)? {
            Self::Item::validate(&val.value, m)?;
            cursor = next_cursor
        }
        Ok(cursor)
    }

    fn splice<E>(slab: &Slab, index: usize, del: usize, values: Vec<E>) -> SpliceResult
    where
        E: MaybePackable<Self::Item> + Debug,
    {
        let mut encoder = Self::encode(index, del, slab);
        let mut add = 0;
        let mut value_group = 0;
        for v in &values {
            value_group += v.group();
            add += encoder.append(v.maybe_packable());
        }
        assert!(encoder.overflow == 0);
        let deleted = encoder.deleted;
        let group = encoder.group;
        let slabs = encoder.finish();
        if deleted == 0 {
            debug_assert_eq!(
                slabs.iter().map(|s| s.group()).sum::<usize>(),
                slab.group() + value_group
            );
        }
        SpliceResult::Replace(add, deleted, group, slabs)
    }

    fn splice_delete<'a>(
        _post: Option<Run<'a, Self::Item>>,
        _cursor: Self,
        _del: usize,
        slab: &'a Slab,
    ) -> SpliceDel<'a, Self> {
        let mut cursor = _cursor;
        let mut post = _post;
        let mut del = _del;
        let mut overflow = 0;
        let mut deleted = 0;
        while del > 0 {
            match post {
                // if del is less than the current run
                Some(Run { count, value }) if del < count => {
                    deleted += del;
                    post = Some(Run {
                        count: count - del,
                        value,
                    });
                    del = 0;
                }
                // if del is greather than or equal the current run
                Some(Run { count, .. }) => {
                    del -= count;
                    deleted += count;
                    post = None;
                }
                None => {
                    if let Some((p, c)) = Self::next(&cursor, slab.as_slice()) {
                        post = Some(p);
                        cursor = c;
                    } else {
                        post = None;
                        overflow = del;
                        del = 0;
                    }
                }
            }
        }
        assert!(_del == deleted + overflow);
        SpliceDel {
            deleted,
            overflow,
            cursor,
            post,
        }
    }

    fn init_empty(len: usize) -> Slab {
        let mut writer = SlabWriter::new(usize::MAX);
        writer.flush_null(len);
        writer.finish().pop().unwrap_or_default()
    }
}

pub struct SpliceDel<'a, C: ColumnCursor> {
    pub(crate) deleted: usize,
    pub(crate) overflow: usize,
    pub(crate) cursor: C,
    pub(crate) post: Option<Run<'a, C::Item>>,
}

pub enum SpliceResult {
    //Done(usize, usize),
    //Add(usize, usize, Vec<Slab>),
    Replace(usize, usize, usize, Vec<Slab>),
}

#[derive(Debug, Clone, Default, Copy)]
pub struct RunIter<'a, C: ColumnCursor> {
    pub(crate) slab: &'a [u8],
    pub(crate) cursor: C,
    pub(crate) weight_left: SlabWeight,
}

impl<'a, C: ColumnCursor> RunIter<'a, C> {
    pub fn empty() -> Self {
        RunIter {
            slab: &[],
            cursor: C::empty(),
            weight_left: SlabWeight::default(),
        }
    }

    pub(crate) fn weight_left(&self) -> SlabWeight {
        self.weight_left
    }

    pub(crate) fn sub_advance_group(&mut self, mut n: usize) -> (usize, Option<Run<'a, C::Item>>) {
        let mut pos = 0;
        while let Some(mut run) = self.next() {
            let unit = run.unit_group();
            if unit * run.count <= n {
                n -= unit * run.count;
                pos += run.count;
            } else {
                assert!(unit > 0);
                let advance = n / unit;
                run.count -= advance;
                pos += advance;
                if run.count == 0 {
                    let tmp = self.next();
                    return (pos, tmp);
                } else {
                    return (pos, Some(run));
                }
            }
        }
        (pos, None)
    }

    pub(crate) fn sub_advance(&mut self, mut n: usize) -> Option<Run<'a, C::Item>> {
        while let Some(mut run) = self.next() {
            if run.count <= n {
                n -= run.count;
            } else {
                run.count -= n;
                if run.count == 0 {
                    let tmp = self.next();
                    return tmp;
                } else {
                    return Some(run);
                }
            }
        }
        None
    }
}

impl<'a, C: ColumnCursor> Iterator for RunIter<'a, C> {
    type Item = Run<'a, C::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        let (run, cursor) = self.cursor.next(self.slab)?;
        self.cursor = cursor;
        self.weight_left -= run.weight_left();
        Some(run)
    }
}
