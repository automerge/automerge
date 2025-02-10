use super::aggregate::{Acc, Agg};
use super::columndata::ColumnData;
use super::encoder::{Encoder, EncoderState, SpliceEncoder, Writer};
use super::pack::{MaybePackable, PackError, Packable};
use super::slab::{Slab, SlabWeight, SlabWriter, SpanWeight};
use super::Cow;

use std::fmt::Debug;
use std::ops::Range;

// this is just a hack - need a more generic validator
#[derive(Debug, Default)]
pub struct ScanMeta {
    pub actors: usize,
}

pub trait HasMinMax {
    fn min(&self) -> Agg;
    fn max(&self) -> Agg;
}

pub trait HasPos {
    fn pos(&self) -> usize;
}

pub trait HasAcc {
    fn acc(&self) -> Acc;
}

impl HasPos for SlabWeight {
    fn pos(&self) -> usize {
        self.pos
    }
}

impl HasMinMax for SlabWeight {
    fn min(&self) -> Agg {
        self.min
    }
    fn max(&self) -> Agg {
        self.max
    }
}

impl HasAcc for SlabWeight {
    fn acc(&self) -> Acc {
        self.acc
    }
}

/*
#[derive(Debug, PartialEq)]
pub enum MyCow<'a, T: PartialEq + ?Sized + ToOwned> {
  Owned(T::Owned),
  Borrowed(&'a T)
}
*/

#[derive(Debug, PartialEq, Default)]
pub struct Run<'a, P: Packable + ?Sized> {
    pub count: usize,
    pub value: Option<Cow<'a, P>>,
}

impl<'a, P: Packable + ?Sized> Copy for Run<'a, P> where Cow<'a, P>: Copy {}

impl<'a, P: Packable + ?Sized> Clone for Run<'a, P> {
    fn clone(&self) -> Self {
        Run {
            count: self.count,
            value: self.value.clone(),
        }
    }
}

impl<'a, P: Packable + ?Sized> Iterator for Run<'a, P> {
    type Item = Option<Cow<'a, P>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.count >= 1 {
            self.count -= 1;
            Some(self.value.clone())
        } else {
            None
        }
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        if self.count > n {
            self.count -= n + 1;
            Some(self.value.clone())
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
            let value = self.value.clone();
            Some(Run { count, value })
        }
    }

    pub(crate) fn pop(&self) -> Option<Run<'a, T>> {
        self.pop_n(1)
    }

    pub fn agg(&self) -> Agg {
        self.value.as_ref().map(|i| T::agg(i)).unwrap_or_default()
    }
    pub fn acc(&self) -> Acc {
        self.agg() * self.count
    }
}

impl<'a> Run<'a, i64> {
    pub fn delta(&self) -> i64 {
        self.count as i64 * self.value.as_deref().cloned().unwrap_or(0)
    }
}

impl<'a, T: Packable + ?Sized> Run<'a, T> {
    pub fn new(count: usize, value: Option<Cow<'a, T>>) -> Self {
        Run { count, value }
    }

    pub fn plus(mut self, num: usize) -> Self {
        self.count += num;
        self
    }
}

pub trait ColumnCursor: Debug + Clone + Copy + PartialEq {
    type Item: Packable + ?Sized;
    type State<'a>: EncoderState<'a, Self::Item>
    where
        <Self as ColumnCursor>::Item: 'a;
    type PostState<'a>: Debug
    where
        Self::Item: 'a;
    type Export: Debug + PartialEq + Clone;
    type SlabIndex: Debug + Clone + HasPos + HasAcc + SpanWeight<Slab>;

    // TODO: needs a test
    fn encode<'a, I>(out: &mut Vec<u8>, values: I, force: bool) -> Range<usize>
    where
        I: Iterator<Item = Option<Cow<'a, Self::Item>>>,
        Self::Item: 'a,
    {
        let start = out.len();
        let mut state = Self::State::default();
        for v in values {
            state.append(out, v);
        }
        if !force && out.len() == start && state.is_empty() {
            out.truncate(start);
            return start..start;
        }
        state.flush(out);
        let end = out.len();
        start..end
    }

    fn empty() -> Self;

    fn new(_: &Slab) -> Self {
        Self::empty()
    }

    fn iter(slab: &[u8]) -> CursorIter<'_, Self> {
        CursorIter {
            slab,
            cursor: Self::empty(),
            run: None,
        }
    }

    fn compute_min_max(_slabs: &mut [Slab]) {
        for s in _slabs {
            let (_run, c) = Self::seek(s.len(), s);
            let _next = c.clone().next(s.as_slice());
            assert!(_run.is_some());
            assert!(_next.is_none());
        }
    }

    fn is_empty(v: Option<Cow<'_, Self::Item>>) -> bool {
        v.is_none()
    }

    fn contains(&self, run: &Run<'_, Self::Item>, agg: Agg) -> bool {
        agg == <Self::Item>::maybe_agg(&run.value)
    }

    fn pop<'a>(&self, run: &mut Run<'a, Self::Item>) -> Option<Option<Cow<'a, Self::Item>>> {
        run.next()
    }

    fn pop_n<'a>(
        &self,
        run: &mut Run<'a, Self::Item>,
        n: usize,
    ) -> Option<Option<Cow<'a, Self::Item>>> {
        assert!(n > 0);
        run.nth(n - 1)
    }

    // ENCODER
    fn finalize_state<'a>(
        slab: &'a Slab,
        encoder: &mut Encoder<'a, Self>,
        post: Self::PostState<'a>,
        cursor: Self,
    ) -> Option<Self>;

    // ENCODER
    fn finish<'a>(slab: &'a Slab, writer: &mut SlabWriter<'a, Self::Item>, cursor: Self);

    fn copy_between<'a>(
        slab: &'a [u8],
        writer: &mut SlabWriter<'a, Self::Item>,
        c0: Self,
        c1: Self,
        run: Run<'a, Self::Item>,
        size: usize,
    ) -> Self::State<'a>;

    fn splice_encoder(
        index: usize,
        del: usize,
        slab: &Slab,
        capacity: usize,
    ) -> SpliceEncoder<'_, Self>;

    fn slab_size() -> usize;

    fn try_next<'a>(&mut self, data: &'a [u8]) -> Result<Option<Run<'a, Self::Item>>, PackError>;

    fn export_splice<'a, I>(data: &mut Vec<Self::Export>, range: Range<usize>, values: I)
    where
        I: Iterator<Item = Option<Cow<'a, Self::Item>>>,
        Self::Item: 'a;

    fn next<'a>(&mut self, data: &'a [u8]) -> Option<Run<'a, Self::Item>> {
        match self.try_next(data).unwrap() {
            Some(run) if run.count == 0 => self.next(data),
            result => result,
        }
    }

    fn index(&self) -> usize;

    fn offset(&self) -> usize;

    fn acc(&self) -> Acc {
        Acc::new()
    }

    fn min(&self) -> Agg {
        Agg::default()
    }
    fn max(&self) -> Agg {
        Agg::default()
    }

    fn seek(index: usize, slab: &Slab) -> (Option<Run<'_, Self::Item>>, Self) {
        if index == 0 {
            return (None, Self::new(slab));
        } else {
            let mut cursor = Self::new(slab);
            while let Some(val) = cursor.next(slab.as_slice()) {
                if cursor.index() >= index {
                    return (Some(val), cursor);
                }
            }
        }
        panic!()
    }

    fn debug_scan(data: &[u8], m: &ScanMeta) -> Result<Self, PackError> {
        let mut cursor = Self::empty();
        while let Some(val) = cursor.try_next(data)? {
            Self::Item::validate(val.value.as_deref(), m)?;
        }
        Ok(cursor)
    }

    fn load_with(data: &[u8], m: &ScanMeta) -> Result<ColumnData<Self>, PackError>;

    fn load(data: &[u8]) -> Result<ColumnData<Self>, PackError> {
        Self::load_with(data, &ScanMeta::default())
    }

    fn splice<'a, 'b, I, M>(slab: &'a Slab, index: usize, del: usize, values: I) -> SpliceResult
    where
        M: MaybePackable<'b, Self::Item>,
        I: Iterator<Item = M> + ExactSizeIterator,
        Self::Item: 'b,
    {
        let mut encoder = Self::splice_encoder(index, del, slab, values.len());
        let mut add = 0;
        let mut value_acc = Acc::new();
        for v in values {
            value_acc += v.agg();
            let opt_v = v.maybe_packable();
            add += encoder.append_item(opt_v);
        }
        assert!(encoder.overflow == 0);
        let deleted = encoder.deleted;
        let acc = encoder.acc;
        let slabs = encoder.finish();
        if deleted == 0 {
            assert_eq!(
                slabs.iter().map(|s| s.acc()).sum::<Acc>(),
                slab.acc() + value_acc
            );
            assert_eq!(
                slabs.iter().map(|s| s.len()).sum::<usize>(),
                slab.len() + add
            );
        }
        if slabs.is_empty() {
            SpliceResult::Noop
        } else {
            SpliceResult::Replace(add, deleted, acc, slabs)
        }
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
                    if let Some(p) = Self::next(&mut cursor, slab.as_slice()) {
                        post = Some(p);
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
        if len > 0 {
            let mut writer = SlabWriter::<Self::Item>::new(usize::MAX, 2, false);
            writer.flush_null(len);
            writer.finish().pop().unwrap_or_default()
        } else {
            Slab::default()
        }
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
    Replace(usize, usize, Acc, Vec<Slab>),
    Noop,
}

// TODO : this needs tests
#[derive(Debug)]
pub struct CursorIter<'a, C: ColumnCursor> {
    pub(crate) slab: &'a [u8],
    pub(crate) cursor: C,
    pub(crate) run: Option<Run<'a, C::Item>>,
}

impl<'a, C: ColumnCursor> Clone for CursorIter<'a, C> {
    fn clone(&self) -> Self {
        CursorIter {
            slab: self.slab,
            cursor: self.cursor,
            run: self.run.clone(),
        }
    }
}

impl<'a, C: ColumnCursor> CursorIter<'a, C> {
    fn next_run(&mut self) -> Result<Option<Run<'a, C::Item>>, PackError> {
        while let Some(run) = self.cursor.try_next(self.slab)? {
            if run.count > 0 {
                return Ok(Some(run));
            }
        }
        Ok(None)
    }
}

impl<'a, C: ColumnCursor> Iterator for CursorIter<'a, C>
where
    C::Item: 'a,
{
    type Item = Result<Option<Cow<'a, C::Item>>, PackError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.run.as_mut() {
            Some(Run { count, value }) if *count > 0 => {
                *count -= 1;
                Some(Ok(value.clone()))
            }
            _ => match self.next_run() {
                Ok(Some(Run { count, value })) if count > 0 => {
                    self.run = Some(Run {
                        count: count - 1,
                        value: value.clone(),
                    });
                    Some(Ok(value))
                }
                Ok(_) => None,
                Err(e) => Some(Err(e)),
            },
        }
    }
}

#[derive(Debug, Clone, Default, Copy)]
pub struct RunIter<'a, C: ColumnCursor> {
    pub(crate) slab: &'a [u8],
    pub(crate) cursor: C,
    pub(crate) pos_left: usize,
    pub(crate) acc_left: Acc,
}

impl<'a, C: ColumnCursor> RunIter<'a, C> {
    pub fn empty() -> Self {
        RunIter {
            slab: &[],
            cursor: C::empty(),
            pos_left: 0,
            acc_left: Acc::new(),
        }
    }

    pub(crate) fn pos_left(&self) -> usize {
        self.pos_left
    }
    pub(crate) fn acc_left(&self) -> Acc {
        self.acc_left
    }

    pub(crate) fn sub_advance_acc(&mut self, mut n: Acc) -> (usize, Option<Run<'a, C::Item>>) {
        let mut pos = 0;
        while let Some(mut run) = self.next() {
            let agg = run.agg();
            if agg * run.count <= n {
                n -= agg * run.count;
                pos += run.count;
            } else {
                assert!(agg.as_usize() > 0);
                let advance = n / agg;
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

    pub(crate) fn with_cursor(self) -> RunIterWithCursor<'a, C> {
        RunIterWithCursor(self)
    }
}

impl<'a, C: ColumnCursor> Iterator for RunIter<'a, C>
where
    C::Item: 'a,
{
    type Item = Run<'a, C::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        let run = self.cursor.next(self.slab)?;
        self.pos_left -= run.count;
        self.acc_left -= run.acc();
        Some(run)
    }
}

pub(crate) struct RunIterWithCursor<'a, C: ColumnCursor>(RunIter<'a, C>);

impl<'a, C: ColumnCursor> Iterator for RunIterWithCursor<'a, C>
where
    C::Item: 'a,
{
    type Item = (Run<'a, C::Item>, C);

    fn next(&mut self) -> Option<Self::Item> {
        let run = self.0.next()?;
        Some((run, self.0.cursor))
    }
}
