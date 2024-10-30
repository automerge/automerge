use super::aggregate::{Acc, Agg};
use super::cursor::{ColumnCursor, Encoder, Run, SpliceDel};
use super::leb128::lebsize;
use super::pack::{PackError, Packable};
use super::slab::{Slab, SlabWeight, SlabWriter};

use std::marker::PhantomData;
use std::ops::Range;

#[derive(Debug, PartialEq)]
pub struct RleCursor<const B: usize, P: Packable + ?Sized> {
    index: usize,
    offset: usize,
    acc: Acc,
    min: Agg,
    max: Agg,
    last_offset: usize,
    lit: Option<LitRunCursor>,
    _phantom: PhantomData<P>,
}

impl<const B: usize, P: Packable + ?Sized> Copy for RleCursor<B, P> {}

impl<const B: usize, P: Packable + ?Sized> Clone for RleCursor<B, P> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<const B: usize, P: Packable + ?Sized> Default for RleCursor<B, P> {
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
        }
    }
}

impl<const B: usize, P: Packable + ?Sized> RleCursor<B, P> {
    pub(crate) fn flush_run<'a>(
        writer: &mut SlabWriter<'a>,
        num: usize,
        value: Option<P::Unpacked<'a>>,
    ) {
        if let Some(v) = value {
            if num == 1 {
                writer.flush_lit_run(&[v]);
            } else {
                writer.flush_run(num as i64, v);
            }
        } else {
            writer.flush_null(num);
        }
    }

    fn valid_lit(&self) -> Option<&LitRunCursor> {
        match &self.lit {
            Some(lit) if lit.index <= lit.len => Some(lit),
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
        slab: &'a Slab,
        cursor: &Self,
        run: Option<Run<'a, P>>,
        index: usize,
        cap: usize,
    ) -> (RleState<'a, P>, Option<Run<'a, P>>, Acc, SlabWriter<'a>) {
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
                let run = Run::new(delta, value);
                acc -= run.acc();
                post = Some(run);
                let count = count - delta;
                RleState::Run { count, value }
            }
            Some(Run { count, value }) => RleState::Run { count, value },
        };

        let mut current = SlabWriter::new(B, cap, slab.as_slice());

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
}

impl<const B: usize, P: Packable + ?Sized> ColumnCursor for RleCursor<B, P> {
    type Item = P;
    type State<'a> = RleState<'a, P>;
    type PostState<'a> = Option<Run<'a, P>>;
    type Export = Option<P::Owned>;
    type SlabIndex = SlabWeight;

    fn empty() -> Self {
        Self::default()
    }

    fn acc(&self) -> Acc {
        self.acc
    }

    fn copy_between<'a>(
        slab: &'a Slab,
        writer: &mut SlabWriter<'a>,
        c0: Self,
        c1: Self,
        run: Run<'a, Self::Item>,
        size: usize,
    ) -> Self::State<'a> {
        match (c0.valid_lit(), c1.valid_lit()) {
            // its one big lit-run
            (Some(a), Some(b)) if a.offset == b.offset => {
                let lit = a.len - 2;
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
        Self::append_chunk(&mut next_state, writer, run);
        next_state
    }

    fn finalize_state<'a>(
        slab: &'a Slab,
        writer: &mut SlabWriter<'a>,
        mut state: Self::State<'a>,
        post: Option<Run<'a, P>>,
        mut cursor: Self,
    ) -> Option<Self> {
        if let Some(run) = post {
            Self::append_chunk(&mut state, writer, run);
        }
        if let Some(run) = cursor.next(slab.as_slice()) {
            Self::append_chunk(&mut state, writer, run);
            Self::flush_state(writer, state);
            Some(cursor)
        } else {
            Self::flush_state(writer, state);
            None
        }
    }

    fn finish<'a>(slab: &'a Slab, writer: &mut SlabWriter<'a>, cursor: Self) {
        let num_left = cursor.num_left();
        let range = cursor.offset..slab.as_slice().len();
        writer.copy(
            slab,
            range,
            num_left,
            slab.len() - cursor.index,
            slab.acc() - cursor.acc(),
            None,
        );
    }

    fn append<'a>(
        old_state: &mut Self::State<'a>,
        writer: &mut SlabWriter<'a>,
        value: Option<<Self::Item as Packable>::Unpacked<'a>>,
    ) -> usize {
        Self::append_chunk(old_state, writer, Run { count: 1, value })
    }

    fn flush_state<'a>(writer: &mut SlabWriter<'a>, state: RleState<'a, Self::Item>) {
        match state {
            RleState::Empty => (),
            RleState::LoneValue(Some(value)) => writer.flush_lit_run(&[value]),
            RleState::LoneValue(None) => writer.flush_null(1),
            RleState::Run {
                count,
                value: Some(v),
            } => writer.flush_run(count as i64, v),
            RleState::Run { count, value: None } => writer.flush_null(count),
            RleState::LitRun { mut run, current } => {
                run.push(current);
                writer.flush_lit_run(&run);
            }
        }
    }

    fn append_chunk<'a>(
        old_state: &mut RleState<'a, P>,
        writer: &mut SlabWriter<'a>,
        chunk: Run<'a, P>,
    ) -> usize {
        let mut state = RleState::Empty;
        std::mem::swap(&mut state, old_state);
        let new_state = match state {
            RleState::Empty => RleState::from(chunk),
            RleState::LoneValue(value) => match (value, chunk.value) {
                (a, b) if a == b => RleState::from(chunk.plus(1)),
                (Some(a), Some(b)) if chunk.count == 1 => RleState::lit_run(a, b),
                (a, _b) => {
                    Self::flush_run(writer, 1, a);
                    RleState::from(chunk)
                }
            },
            RleState::Run { count, value } if chunk.value == value => {
                RleState::from(chunk.plus(count))
            }
            RleState::Run { count, value } => {
                Self::flush_run(writer, count, value);
                RleState::from(chunk)
            }
            RleState::LitRun { mut run, current } => {
                match (current, chunk.value) {
                    (a, Some(b)) if a == b => {
                        // the end of the lit run merges with the next
                        writer.flush_lit_run(&run);
                        RleState::from(chunk.plus(1))
                    }
                    (a, Some(b)) if chunk.count == 1 => {
                        // its single and different - addit to the lit run
                        run.push(a);
                        RleState::LitRun { run, current: b }
                    }
                    _ => {
                        // flush this lit run (current and all) - next run replaces it
                        run.push(current);
                        writer.flush_lit_run(&run);
                        RleState::from(chunk)
                    }
                }
            }
        };
        *old_state = new_state;
        chunk.count
    }

    fn encode(index: usize, del: usize, slab: &Slab, cap: usize) -> Encoder<'_, Self> {
        let (run, cursor) = Self::seek(index, slab);

        let cap = cap * 2 + 9;
        let (state, post, acc, current) = RleCursor::encode_inner(slab, &cursor, run, index, cap);

        let SpliceDel {
            deleted,
            overflow,
            cursor,
            post,
        } = Self::splice_delete(post, cursor, del, slab);

        Encoder {
            slab,
            current,
            post,
            acc,
            state,
            deleted,
            overflow,
            cursor,
        }
    }

    fn export_splice<'a, I>(data: &mut Vec<Self::Export>, range: Range<usize>, values: I)
    where
        I: Iterator<Item = Option<<Self::Item as Packable>::Unpacked<'a>>>,
    {
        data.splice(range, values.map(|e| e.map(|i| P::own(i))));
    }

    fn compute_min_max(slabs: &mut [Slab]) {
        for s in slabs {
            let (_run, c) = Self::seek(s.len(), s);
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
            let agg = P::agg(value);
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
                count if count > 0 => {
                    let count = count as usize;
                    let (value_bytes, value) = P::unpack(data)?;
                    let agg = P::agg(value);
                    self.next_lit(count);
                    self.progress(count, count_bytes + value_bytes, agg);
                    let value = Run {
                        count,
                        value: Some(value),
                    };
                    Ok(Some(value))
                }
                count if count < 0 => {
                    let (value_bytes, value) = P::unpack(data)?;
                    assert!(-count < slab.len() as i64);
                    self.lit = Some(LitRunCursor::new(
                        self.offset + count_bytes,
                        count,
                        self.acc,
                    ));
                    let agg = P::agg(value);
                    self.progress(1, count_bytes + value_bytes, agg);
                    let value = Run {
                        count: 1,
                        value: Some(value),
                    };
                    Ok(Some(value))
                }
                _ => {
                    let (null_bytes, count) = u64::unpack(data)?;
                    let count = count as usize;
                    self.lit = None;
                    self.progress(count, count_bytes + null_bytes, Agg::default());
                    let value = Run { count, value: None };
                    Ok(Some(value))
                }
            }
        }
    }

    fn index(&self) -> usize {
        self.index
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

//pub type StrCursor = RleCursor<1024, str>;
pub type StrCursor = RleCursor<128, str>;
//pub type IntCursor = RleCursor<1024, u64>;
pub type IntCursor = RleCursor<64, u64>;

#[derive(Debug, Clone, Default)]
pub enum RleState<'a, P: Packable + ?Sized> {
    #[default]
    Empty,
    LoneValue(Option<P::Unpacked<'a>>),
    Run {
        count: usize,
        value: Option<P::Unpacked<'a>>,
    },
    LitRun {
        run: Vec<P::Unpacked<'a>>,
        current: P::Unpacked<'a>,
    },
}

impl<'a, P: Packable + ?Sized> RleState<'a, P> {
    fn lit_run(a: P::Unpacked<'a>, b: P::Unpacked<'a>) -> Self {
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
                Some("xxx0"),
                Some("xxx1"),
                Some("xxx2"),
                Some("xxx3"),
                Some("xxx3"),
                Some("xxx3"),
                Some("xxx3")
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
                sum += u64::agg(v);
            }
        }
        let mut writer = Vec::new();
        col1.write(&mut writer);
        assert_eq!(writer, vec![116, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);

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
                sum += u64::agg(v);
            }
        }
        let mut writer = Vec::new();
        col2.write(&mut writer);
        assert_eq!(writer, vec![2, 1, 120, 2, 3, 4, 5, 6, 7, 8, 9, 2, 10]);

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
                sum += u64::agg(v);
            }
        }
        let mut writer = Vec::new();
        col3.write(&mut writer);
        assert_eq!(
            writer,
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
                sum += u64::agg(v);
            }
        }
        let mut writer = Vec::new();
        col4.write(&mut writer);
        assert_eq!(
            writer,
            vec![2, 1, 2, 2, 2, 3, 2, 4, 2, 5, 2, 6, 2, 7, 2, 8, 2, 9]
        );

        // empty data
        let col5: ColumnData<RleCursor<5, u64>> = ColumnData::new();
        assert_eq!(col5.test_dump(), vec![vec![]]);
        let mut writer = Vec::new();
        col5.write(&mut writer);
        assert_eq!(writer, Vec::<u8>::new());
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
        col1.splice::<u64>(1, 1, vec![]);
        assert_eq!(
            col1.test_dump(),
            vec![vec![
                ColExport::run(2, 1),
                ColExport::litrun(vec![2, 3, 4, 5, 6]),
                ColExport::run(2, 9),
            ]]
        );
        let mut col2 = col1.clone();
        col2.splice::<u64>(0, 1, vec![]);
        assert_eq!(
            col2.test_dump(),
            vec![vec![
                ColExport::litrun(vec![1, 2, 3, 4, 5, 6]),
                ColExport::run(2, 9),
            ]]
        );

        let mut col3 = col1.clone();
        col3.splice::<u64>(1, 7, vec![]);
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
        col1.splice::<u64>(3, 1, vec![9]);
        assert_eq!(
            col1.test_dump(),
            vec![vec![ColExport::litrun(vec![1, 2, 4, 9, 4, 5, 6]),]]
        );
    }
}
