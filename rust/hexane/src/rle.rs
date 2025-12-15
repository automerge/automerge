use super::aggregate::{Acc, Agg};
use super::columndata::ColumnData;
use super::cursor::{ColumnCursor, HasAcc, HasPos, Run, ScanMeta, SpliceDel};
use super::encoder::{Encoder, EncoderState, SpliceEncoder};
use super::leb128::lebsize;
use super::pack::{PackError, Packable};
use super::slab::{Slab, SlabWeight, SlabWriter, SpanWeight};
use super::Cow;

use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::Range;

#[derive(Debug, PartialEq)]
pub struct RleCursor<const B: usize, P: Packable + ?Sized, X = SlabWeight> {
    pub(crate) index: usize,
    pub(crate) offset: usize,
    acc: Acc,
    min: Agg,
    max: Agg,
    last_offset: usize,
    lit: Option<LitRunCursor>,
    _phantom: PhantomData<P>,
    _phantom2: PhantomData<X>,
}

impl<const B: usize, P: Packable + ?Sized, X> Copy for RleCursor<B, P, X> {}

impl<const B: usize, P: Packable + ?Sized, X> Clone for RleCursor<B, P, X> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<const B: usize, P: Packable + ?Sized, X> Default for RleCursor<B, P, X> {
    fn default() -> Self {
        Self {
            offset: 0,
            last_offset: 0,
            index: 0,
            acc: Acc::new(),
            min: Agg::default(),
            max: Agg::default(),
            lit: None,
            _phantom: PhantomData,
            _phantom2: PhantomData,
        }
    }
}

impl<const B: usize, P: Packable + ?Sized, X: HasPos + HasAcc + SpanWeight<Slab>>
    RleCursor<B, P, X>
{
    fn valid_lit(&self) -> Option<&LitRunCursor> {
        match &self.lit {
            Some(lit) if lit.index <= lit.len => Some(lit),
            _ => None,
        }
    }

    fn lit_offset_bytes(&self) -> Option<usize> {
        match &self.lit {
            Some(lit) if lit.index <= lit.len => {
                if self.last_offset < lit.offset {
                    Some(lit.offset - self.last_offset)
                } else {
                    Some(0)
                }
            }
            _ => None,
        }
    }

    fn next_lit(&mut self, count: usize) {
        if let Some(lit) = &mut self.lit {
            if lit.index > lit.len {
                self.lit = None;
            } else {
                lit.index += count;
            }
        }
    }

    fn progress(&mut self, count: usize, bytes: usize, agg: Agg) {
        self.last_offset = self.offset;
        self.offset += bytes;
        self.index += count;
        self.acc += agg * count;
        self.min = self.min.minimize(agg);
        self.max = self.max.maximize(agg);
    }

    pub(crate) fn num_left(&self) -> usize {
        self.lit.as_ref().map(|l| l.num_left()).unwrap_or(0)
    }

    fn lit_num(&self) -> usize {
        if let Some(lit) = &self.lit {
            lit.index
        } else {
            0
        }
    }

    pub(super) fn lit_acc(&self, last_acc: Acc) -> Acc {
        if let Some(lit) = &self.lit {
            self.acc - last_acc - lit.acc
        } else {
            Acc::new()
        }
    }
    pub(super) fn lit_range(&self) -> Range<usize> {
        if let Some(lit) = &self.lit {
            lit.offset..self.last_offset
        } else {
            0..0
        }
    }

    fn copy_acc(&self, last_acc: Acc) -> Acc {
        if let Some(lit) = self.lit {
            lit.acc
        } else {
            self.acc - last_acc
        }
    }

    fn copy_range(&self) -> Range<usize> {
        if let Some(lit) = self.lit {
            // copy only before the lit run
            0..lit.header_offset()
        } else {
            // copy only before the last read
            0..self.last_offset
        }
    }

    fn copy_size(&self, last_run: usize) -> usize {
        if let Some(lit) = self.lit {
            // copy only before the lit run
            self.index - lit.index
        } else {
            // copy everything
            self.index - last_run
        }
    }

    pub(crate) fn encode_inner<'a>(
        slab: &'a [u8],
        cursor: &Self,
        run: Option<Run<'a, P>>,
        index: usize,
        locked: bool,
    ) -> (RleState<'a, P>, Option<Run<'a, P>>, Acc, SlabWriter<'a, P>) {
        let mut post = None;
        let mut acc = cursor.acc();

        let last_run = run.as_ref().map(|r| r.count).unwrap_or(0);
        let last_run_acc = run.as_ref().map(|r| r.acc()).unwrap_or_default();

        let copy_range = cursor.copy_range();
        let copy_acc = cursor.copy_acc(last_run_acc);
        let copy_size = cursor.copy_size(last_run);
        let copy_lit_range = cursor.lit_range();
        let copy_lit_acc = cursor.lit_acc(last_run_acc);
        let copy_lit_size = cursor.lit_num().saturating_sub(last_run);

        let state = match run {
            None => RleState::Empty,
            Some(Run {
                count: 1,
                value: Some(current),
            }) if cursor.lit_num() > 1 => RleState::LitRun {
                run: vec![],
                current,
            },
            Some(Run { count: 1, value }) => RleState::LoneValue(value),
            Some(Run { count, value }) if index < cursor.index => {
                let delta = cursor.index - index;
                let run = Run::new(delta, value.clone());
                acc -= run.acc();
                post = Some(run);
                let count = count - delta;
                RleState::Run { count, value }
            }
            Some(Run { count, value }) => RleState::Run { count, value },
        };

        let mut current = SlabWriter::new(B, locked);

        current.copy(slab, copy_range, 0, copy_size, copy_acc, None);

        if copy_lit_size > 0 {
            current.copy(
                slab,
                copy_lit_range,
                copy_lit_size,
                copy_lit_size,
                copy_lit_acc,
                None,
            );
        }

        (state, post, acc, current)
    }

    pub(crate) fn load_copy<'a>(
        slab: &'a [u8],
        writer: &mut SlabWriter<'a, P>,
        c0: &Self,
        c1: &Self,
    ) {
        let acc = c1.acc() - c0.acc();
        let size = c1.index - c0.index;
        if size == 0 {
            return;
        }
        match (c0.valid_lit(), c1.valid_lit()) {
            // its one big lit-run
            (Some(a), Some(b)) if a.offset == b.offset => {
                writer.copy(slab, c0.offset..c1.offset, size, size, acc, None);
            }
            // its two different lit-runs
            (Some(a), Some(b)) => {
                let lit1 = a.len - a.index;
                let lit2 = b.index;
                let b_start = b.header_offset();
                writer.copy(slab, c0.offset..b_start, lit1, size - lit2, acc, None);
                writer.copy(slab, b.offset..c1.offset, lit2, lit2, Acc::new(), None);
            }
            (Some(a), None) => {
                let lit = a.len - a.index;
                writer.copy(slab, c0.offset..c1.offset, lit, size, acc, None);
            }
            (None, Some(b)) => {
                let lit2 = b.index;
                let b_start = b.header_offset();
                writer.copy(slab, c0.offset..b_start, 0, size - lit2, Acc::new(), None);
                writer.copy(slab, b.offset..c1.offset, lit2, lit2, acc, None);
            }
            _ => {
                writer.copy(slab, c0.offset..c1.offset, 0, size, acc, None);
            }
        }
    }
}

impl<const B: usize, P: Packable + ?Sized, X: HasPos + HasAcc + SpanWeight<Slab>> ColumnCursor
    for RleCursor<B, P, X>
{
    type Item = P;
    type State<'a>
        = RleState<'a, P>
    where
        P: 'a;
    type PostState<'a>
        = Option<Run<'a, Self::Item>>
    where
        Self::Item: 'a;
    type Export = Option<P::Owned>;
    type SlabIndex = X;

    fn empty() -> Self {
        Self::default()
    }

    fn acc(&self) -> Acc {
        self.acc
    }

    fn copy_between<'a>(
        slab: &'a [u8],
        writer: &mut SlabWriter<'a, P>,
        c0: Self,
        c1: Self,
        run: Run<'a, Self::Item>,
        size: usize,
    ) -> Self::State<'a> {
        match (c0.valid_lit(), c1.valid_lit()) {
            // its one big lit-run
            (Some(a), Some(b)) if a.offset == b.offset => {
                let lit = a.len - 2; // FIXME - this number seems super wrong
                writer.copy(slab, c0.offset..c1.last_offset, lit, size, Acc::new(), None);
            }
            // its two different lit-runs
            (Some(a), Some(b)) => {
                let lit1 = a.len - 1;
                let lit2 = b.len - 1;
                let b_start = b.header_offset();
                writer.copy(
                    slab,
                    c0.offset..b_start,
                    lit1,
                    size - lit2,
                    Acc::new(),
                    None,
                );
                writer.copy(slab, b.offset..c1.last_offset, lit2, lit2, Acc::new(), None);
            }
            (Some(a), None) => {
                let lit = a.len - 1;
                writer.copy(slab, c0.offset..c1.last_offset, lit, size, Acc::new(), None);
            }
            (None, Some(b)) => {
                let lit2 = b.len - 1;
                let b_start = b.header_offset();
                writer.copy(slab, c0.offset..b_start, 0, size - lit2, Acc::new(), None);
                writer.copy(slab, b.offset..c1.last_offset, lit2, lit2, Acc::new(), None);
            }
            _ => {
                writer.copy(slab, c0.offset..c1.last_offset, 0, size, Acc::new(), None);
            }
        }

        let mut next_state = Self::State::default();
        next_state.append_chunk(writer, run);
        next_state
    }

    fn finalize_state<'a>(
        slab: &'a Slab,
        encoder: &mut Encoder<'a, Self>,
        post: Option<Run<'a, P>>,
        mut cursor: Self,
    ) -> Option<Self> {
        if let Some(run) = post {
            encoder.append_chunk(run);
        }
        if let Some(run) = cursor.next(slab.as_slice()) {
            encoder.append_chunk(run);
            encoder.flush();
            Some(cursor)
        } else {
            encoder.flush();
            None
        }
    }

    fn finish<'a>(slab: &'a Slab, writer: &mut SlabWriter<'a, P>, cursor: Self) {
        let num_left = cursor.num_left();
        let range = cursor.offset..slab.as_slice().len();
        writer.copy(
            slab.as_slice(),
            range,
            num_left,
            slab.len() - cursor.index,
            slab.acc() - cursor.acc(),
            None,
        );
    }

    fn slab_size() -> usize {
        B
    }

    fn splice_encoder(index: usize, del: usize, slab: &Slab) -> SpliceEncoder<'_, Self> {
        let (run, cursor) = Self::seek(index, slab);

        let (state, post, acc, current) =
            RleCursor::encode_inner(slab.as_slice(), &cursor, run, index, false);

        let SpliceDel {
            deleted,
            overflow,
            cursor,
            post,
        } = Self::splice_delete(post, cursor, del, slab);

        SpliceEncoder {
            slab,
            encoder: Encoder::init(current, state),
            post,
            acc,
            deleted,
            overflow,
            cursor,
        }
    }

    fn export_splice<'a, I>(data: &mut Vec<Self::Export>, range: Range<usize>, values: I)
    where
        I: Iterator<Item = Option<Cow<'a, Self::Item>>>,
        Self::Item: 'a,
    {
        data.splice(range, values.map(|e| e.map(|i| i.into_owned())));
    }

    fn compute_min_max(slabs: &mut [Slab]) {
        for s in slabs {
            let (run, c) = Self::seek(s.len(), s);
            let next = c.clone().next(s.as_slice());
            assert!(run.is_some());
            assert!(next.is_none());
            std::mem::drop(run);
            assert_eq!(s.acc(), c.acc());
            s.set_min_max(c.min(), c.max());
        }
    }

    fn min(&self) -> Agg {
        self.min
    }
    fn max(&self) -> Agg {
        self.max
    }

    fn try_next<'a>(&mut self, slab: &'a [u8]) -> Result<Option<Run<'a, Self::Item>>, PackError> {
        let data = &slab[self.offset..];
        if data.is_empty() {
            return Ok(None);
        }
        if self.num_left() > 0 {
            let (value_bytes, value) = P::unpack(data)?;
            let agg = P::agg(&value);
            self.next_lit(1);
            self.progress(1, value_bytes, agg);
            let value = Run {
                count: 1,
                value: Some(value),
            };
            Ok(Some(value))
        } else {
            let (count_bytes, count) = i64::unpack(data)?;
            let data = &data[count_bytes..];
            match count {
                count if *count > 0 => {
                    let count = *count as usize;
                    let (value_bytes, value) = P::unpack(data)?;
                    let agg = P::agg(&value);
                    self.next_lit(count);
                    self.progress(count, count_bytes + value_bytes, agg);
                    let value = Run {
                        count,
                        value: Some(value),
                    };
                    Ok(Some(value))
                }
                count if *count < 0 => {
                    let (value_bytes, value) = P::unpack(data)?;
                    assert!(-*count < slab.len() as i64);
                    self.lit = Some(LitRunCursor::new(
                        self.offset + count_bytes,
                        *count,
                        self.acc,
                    ));
                    let agg = P::agg(&value);
                    self.progress(1, count_bytes + value_bytes, agg);
                    let value = Run {
                        count: 1,
                        value: Some(value),
                    };
                    Ok(Some(value))
                }
                _ => {
                    let (null_bytes, count) = u64::unpack(data)?;
                    let count = *count as usize;
                    self.lit = None;
                    self.progress(count, count_bytes + null_bytes, Agg::default());
                    let value = Run { count, value: None };
                    Ok(Some(value))
                }
            }
        }
    }

    fn try_again<'a>(&self, slab: &'a [u8]) -> Result<Option<Run<'a, Self::Item>>, PackError> {
        let data = &slab[self.last_offset..self.offset];
        if data.is_empty() {
            return Ok(None);
        }
        if let Some(count_bytes) = self.lit_offset_bytes() {
            let data = &data[count_bytes..];
            let (_value_bytes, value) = P::unpack(data)?;
            Ok(Some(Run {
                count: 1,
                value: Some(value),
            }))
        } else {
            let (count_bytes, count) = i64::unpack(data)?;
            let data = &data[count_bytes..];
            match count {
                count if *count > 0 => {
                    let count = *count as usize;
                    let (_value_bytes, value) = P::unpack(data)?;
                    Ok(Some(Run {
                        count,
                        value: Some(value),
                    }))
                }
                count if *count < 0 => {
                    let (_value_bytes, value) = P::unpack(data)?;
                    Ok(Some(Run {
                        count: 1,
                        value: Some(value),
                    }))
                }
                _ => {
                    let (_null_bytes, count) = u64::unpack(data)?;
                    let count = *count as usize;
                    Ok(Some(Run { count, value: None }))
                }
            }
        }
    }

    fn index(&self) -> usize {
        self.index
    }

    fn offset(&self) -> usize {
        self.offset
    }

    fn load_with(data: &[u8], m: &ScanMeta) -> Result<ColumnData<Self>, PackError> {
        let mut cursor = Self::empty();
        let mut writer = SlabWriter::<P>::new(B, true);
        let mut last_copy = Self::empty();
        while let Some(run) = cursor.try_next(data)? {
            P::validate(run.value.as_deref(), m)?;
            if cursor.offset - last_copy.offset >= B {
                Self::load_copy(data, &mut writer, &last_copy, &cursor);
                writer.manual_slab_break();
                last_copy = cursor;
            }
        }
        Self::load_copy(data, &mut writer, &last_copy, &cursor);
        Ok(writer.into_column(cursor.index))
    }
}

#[derive(Debug, PartialEq, Default, Clone, Copy)]
pub(crate) struct LitRunCursor {
    index: usize,
    offset: usize,
    len: usize,
    acc: Acc,
}

impl LitRunCursor {
    fn new(offset: usize, count: i64, acc: Acc) -> Self {
        assert!(count < 0);
        let len = (-count) as usize;
        LitRunCursor {
            offset,
            index: 1,
            acc,
            len,
        }
    }

    fn header_offset(&self) -> usize {
        self.offset - lebsize(-(self.len as i64)) as usize
    }

    fn num_left(&self) -> usize {
        self.len.saturating_sub(self.index)
    }
}

pub type ByteCursor = RleCursor<128, [u8]>;
pub type StrCursor = RleCursor<128, str>;
pub type UIntCursor = RleCursor<64, u64>;
pub type IntCursor = RleCursor<64, i64>;

#[derive(Debug, Default)]
pub enum RleState<'a, P: Packable + ?Sized>
where
    P::Owned: Debug,
{
    #[default]
    Empty,
    LoneValue(Option<Cow<'a, P>>),
    Run {
        count: usize,
        value: Option<Cow<'a, P>>,
    },
    LitRun {
        run: Vec<Cow<'a, P>>,
        current: Cow<'a, P>,
    },
}

impl<P: Packable + ?Sized> Clone for RleState<'_, P> {
    fn clone(&self) -> Self {
        match self {
            Self::Empty => Self::Empty,
            Self::LoneValue(v) => Self::LoneValue(v.clone()),
            Self::Run { count, value } => Self::Run {
                count: *count,
                value: value.clone(),
            },
            Self::LitRun { run, current } => Self::LitRun {
                run: run.clone(),
                current: current.clone(),
            },
        }
    }
}

impl<'a, P: Packable + ?Sized> RleState<'a, P> {
    pub(crate) fn lit_run(a: Cow<'a, P>, b: Cow<'a, P>) -> Self {
        RleState::LitRun {
            run: vec![a],
            current: b,
        }
    }
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

#[cfg(test)]
pub(crate) mod tests {
    use super::super::columndata::{ColGroupItem, ColumnData};
    use super::super::test::ColExport;
    use super::*;

    #[test]
    fn column_data_rle_slab_splitting() {
        let mut col1: ColumnData<RleCursor<4, u64>> = ColumnData::new();
        col1.splice(0, 0, vec![1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(
            col1.test_dump(),
            vec![
                vec![ColExport::litrun(vec![1, 2, 3])],
                vec![ColExport::litrun(vec![4, 5, 6])],
                vec![ColExport::litrun(vec![7])],
            ]
        );
        let mut col2: ColumnData<RleCursor<10, str>> = ColumnData::new();
        col2.splice(0, 0, vec!["xxx1", "xxx2", "xxx3", "xxx3"]);
        assert_eq!(
            col2.test_dump(),
            vec![
                vec![ColExport::litrun(vec!["xxx1", "xxx2"])],
                vec![ColExport::run(2, "xxx3")],
            ]
        );
        col2.splice(0, 0, vec!["xxx0"]);
        assert_eq!(
            col2.test_dump(),
            vec![
                vec![ColExport::litrun(vec!["xxx0", "xxx1"])],
                vec![ColExport::litrun(vec!["xxx2"])],
                vec![ColExport::run(2, "xxx3")],
            ]
        );
        col2.splice(3, 0, vec!["xxx3", "xxx3"]);
        assert_eq!(
            col2.test_dump(),
            vec![
                vec![ColExport::litrun(vec!["xxx0", "xxx1"])],
                vec![ColExport::litrun(vec!["xxx2"])],
                vec![ColExport::run(4, "xxx3")],
            ]
        );
        assert_eq!(
            col2.iter().collect::<Vec<_>>(),
            vec![
                Some(Cow::Borrowed("xxx0")),
                Some(Cow::Borrowed("xxx1")),
                Some(Cow::Borrowed("xxx2")),
                Some(Cow::Borrowed("xxx3")),
                Some(Cow::Borrowed("xxx3")),
                Some(Cow::Borrowed("xxx3")),
                Some(Cow::Borrowed("xxx3"))
            ]
        )
    }

    #[test]
    fn column_data_rle_slab_splitting_edges() {
        let mut col1: ColumnData<RleCursor<5, u64>> = ColumnData::new();
        col1.splice(0, 0, vec![1, 2, 3, 4, 5, 6]);
        assert_eq!(
            col1.test_dump(),
            vec![
                vec![ColExport::litrun(vec![1, 2, 3, 4])],
                vec![ColExport::litrun(vec![5, 6])],
            ]
        );
        col1.splice(0, 0, vec![9, 9]);
        assert_eq!(
            col1.test_dump(),
            vec![
                vec![ColExport::run(2, 9), ColExport::litrun(vec![1])],
                vec![ColExport::litrun(vec![2, 3, 4])],
                vec![ColExport::litrun(vec![5, 6])],
            ]
        );
        col1.splice(5, 0, vec![4]);
        assert_eq!(
            col1.test_dump(),
            vec![
                vec![ColExport::run(2, 9), ColExport::litrun(vec![1])],
                vec![ColExport::litrun(vec![2, 3]), ColExport::run(2, 4)],
                vec![ColExport::litrun(vec![5, 6])],
            ]
        );
    }

    #[test]
    fn column_data_rle_split_merge_semantics() {
        // lit run spanning multiple slabs
        let mut col1: ColumnData<RleCursor<5, u64>> = ColumnData::new();
        col1.splice(0, 0, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
        assert_eq!(
            col1.test_dump(),
            vec![
                vec![ColExport::litrun(vec![1, 2, 3, 4])],
                vec![ColExport::litrun(vec![5, 6, 7, 8])],
                vec![ColExport::litrun(vec![9, 10, 11, 12])],
            ]
        );
        let mut sum = Acc::new();
        for ColGroupItem { item, acc, .. } in col1.iter().with_acc() {
            assert_eq!(sum, acc);
            if let Some(v) = item {
                sum += u64::agg(&v);
            }
        }
        assert_eq!(
            col1.save(),
            vec![116, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        );

        // lit run capped by runs
        let mut col2: ColumnData<RleCursor<5, u64>> = ColumnData::new();
        col2.splice(0, 0, vec![1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 10]);
        assert_eq!(
            col2.test_dump(),
            vec![
                vec![ColExport::run(2, 1), ColExport::litrun(vec![2, 3])],
                vec![ColExport::litrun(vec![4, 5, 6, 7])],
                vec![ColExport::litrun(vec![8, 9]), ColExport::run(2, 10)],
            ]
        );
        let mut sum = Acc::new();
        for ColGroupItem { item, acc, .. } in col2.iter().with_acc() {
            assert_eq!(sum, acc);
            if let Some(v) = item {
                sum += u64::agg(&v);
            }
        }
        assert_eq!(col2.save(), vec![2, 1, 120, 2, 3, 4, 5, 6, 7, 8, 9, 2, 10]);

        // lit run capped by runs
        let mut col3: ColumnData<RleCursor<5, u64>> = ColumnData::new();
        col3.splice(0, 0, vec![1, 2, 3, 4, 4, 5, 5, 6, 7, 8, 9, 10, 11, 11]);
        assert_eq!(
            col3.test_dump(),
            vec![
                vec![ColExport::litrun(vec![1, 2, 3]), ColExport::run(2, 4),],
                vec![ColExport::run(2, 5), ColExport::litrun(vec![6, 7]),],
                vec![ColExport::litrun(vec![8, 9, 10]), ColExport::run(2, 11)],
            ]
        );

        let mut sum = Acc::new();
        for ColGroupItem { item, acc, .. } in col3.iter().with_acc() {
            assert_eq!(sum, acc);
            if let Some(v) = item {
                sum += u64::agg(&v);
            }
        }
        assert_eq!(
            col3.save(),
            vec![125, 1, 2, 3, 2, 4, 2, 5, 123, 6, 7, 8, 9, 10, 2, 11]
        );

        // lit run capped by runs
        let mut col4: ColumnData<RleCursor<5, u64>> = ColumnData::new();
        col4.splice(
            0,
            0,
            vec![1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9],
        );
        assert_eq!(
            col4.test_dump(),
            vec![
                vec![
                    ColExport::run(2, 1),
                    ColExport::run(2, 2),
                    ColExport::run(2, 3),
                ],
                vec![
                    ColExport::run(2, 4),
                    ColExport::run(2, 5),
                    ColExport::run(2, 6),
                ],
                vec![
                    ColExport::run(2, 7),
                    ColExport::run(2, 8),
                    ColExport::run(2, 9),
                ],
            ]
        );
        let mut sum = Acc::new();
        for ColGroupItem { item, acc, .. } in col4.iter().with_acc() {
            assert_eq!(sum, acc);
            if let Some(v) = item {
                sum += u64::agg(&v);
            }
        }
        assert_eq!(
            col4.save(),
            vec![2, 1, 2, 2, 2, 3, 2, 4, 2, 5, 2, 6, 2, 7, 2, 8, 2, 9]
        );

        // empty data
        let col5: ColumnData<RleCursor<5, u64>> = ColumnData::new();
        assert_eq!(col5.test_dump(), vec![vec![]]);
        assert_eq!(col5.save(), Vec::<u8>::new());
    }

    #[test]
    fn column_data_rle_splice_delete() {
        let mut col1: ColumnData<RleCursor<1024, u64>> = ColumnData::new();
        col1.splice(0, 0, vec![1, 1, 1, 2, 3, 4, 5, 6, 9, 9]);
        assert_eq!(
            col1.test_dump(),
            vec![vec![
                ColExport::run(3, 1),
                ColExport::litrun(vec![2, 3, 4, 5, 6]),
                ColExport::run(2, 9),
            ]]
        );
        col1.splice::<u64, _>(1, 1, vec![]);
        assert_eq!(
            col1.test_dump(),
            vec![vec![
                ColExport::run(2, 1),
                ColExport::litrun(vec![2, 3, 4, 5, 6]),
                ColExport::run(2, 9),
            ]]
        );
        let mut col2 = col1.clone();
        col2.splice::<u64, _>(0, 1, vec![]);
        assert_eq!(
            col2.test_dump(),
            vec![vec![
                ColExport::litrun(vec![1, 2, 3, 4, 5, 6]),
                ColExport::run(2, 9),
            ]]
        );

        let mut col3 = col1.clone();
        col3.splice::<u64, _>(1, 7, vec![]);
        assert_eq!(col3.test_dump(), vec![vec![ColExport::litrun(vec![1, 9]),]]);
    }

    #[test]
    fn rle_breaking_runs_near_lit_runs() {
        let mut col1: ColumnData<RleCursor<1024, u64>> = ColumnData::new();
        col1.splice(0, 0, vec![1, 2, 4, 4, 4, 5, 6]);
        assert_eq!(
            col1.test_dump(),
            vec![vec![
                ColExport::litrun(vec![1, 2]),
                ColExport::run(3, 4),
                ColExport::litrun(vec![5, 6]),
            ]]
        );
        col1.splice::<u64, _>(3, 1, vec![9]);
        assert_eq!(
            col1.test_dump(),
            vec![vec![ColExport::litrun(vec![1, 2, 4, 9, 4, 5, 6]),]]
        );
    }

    #[test]
    fn uint_iter_nth() {
        let mut col: ColumnData<UIntCursor> = ColumnData::new();
        let mut data = vec![];
        for _ in 0..10000 {
            let value = rand::random::<u32>() % 10;
            if value > 0 {
                data.push(Some(value as u64 - 1));
            } else {
                data.push(None);
            }
        }
        col.splice(0, 0, data.clone());

        for _ in 0..1000 {
            let mut iter1 = data.iter();
            let mut iter2 = col.iter();
            let mut step = rand::random::<u32>() % 40;
            while let Some(val1) = iter1.nth(step as usize) {
                let val2 = iter2.nth(step as usize);
                assert_eq!(val1.as_ref(), val2.flatten().as_deref());
                step = rand::random::<u32>() % ((data.len() as u32) / 2);
            }
        }
    }

    #[test]
    fn load_empty_rle_data() {
        let col = IntCursor::load(&[]).unwrap();
        assert!(col.is_empty());
    }
}
