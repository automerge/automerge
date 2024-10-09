use super::cursor::{ColumnCursor, Encoder, Run, SpliceDel};
use super::leb128::lebsize;
use super::pack::{PackError, Packable};
use super::slab::{Slab, SlabWriter};

use std::marker::PhantomData;
use std::ops::Range;

#[derive(Debug)]
pub struct RleCursor<const B: usize, P: Packable + ?Sized> {
    offset: usize,
    group: usize,
    last_offset: usize,
    last_group: usize,
    index: usize,
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
            last_group: 0,
            index: 0,
            group: 0,
            lit: None,
            _phantom: PhantomData,
        }
    }
}

impl<const B: usize, P: Packable + ?Sized> RleCursor<B, P> {
    pub(crate) fn flush_run<'a>(
        out: &mut SlabWriter<'a>,
        num: usize,
        value: Option<P::Unpacked<'a>>,
    ) {
        if let Some(v) = value {
            if num == 1 {
                out.flush_lit_run(&[v]);
            } else {
                out.flush_run(num as i64, v);
            }
        } else {
            out.flush_null(num);
        }
    }

    fn valid_lit(&self) -> Option<&LitRunCursor> {
        match &self.lit {
            Some(lit) if lit.index <= lit.len => Some(lit),
            _ => None,
        }
    }

    fn next_lit(&self, count: usize) -> Option<LitRunCursor> {
        if let Some(lit) = self.lit {
            lit.next(count)
        } else {
            None
        }
    }

    fn progress(
        &self,
        count: usize,
        bytes: usize,
        lit: Option<LitRunCursor>,
        group: usize,
    ) -> Self {
        RleCursor {
            last_offset: self.offset,
            last_group: self.group,
            offset: self.offset + bytes,
            index: self.index + count,
            group: self.group + group,
            lit,
            _phantom: PhantomData,
        }
    }

    fn num_left(&self) -> usize {
        self.lit.as_ref().map(|l| l.num_left()).unwrap_or(0)
    }

    fn lit_num(&self) -> usize {
        if let Some(lit) = &self.lit {
            lit.index
        } else {
            0
        }
    }

    pub(super) fn lit_group(&self) -> usize {
        if let Some(lit) = &self.lit {
            self.last_group - lit.group
        } else {
            0
        }
    }
    pub(super) fn lit_range(&self) -> Range<usize> {
        if let Some(lit) = &self.lit {
            lit.offset..self.last_offset
        } else {
            0..0
        }
    }

    fn copy_group(&self) -> usize {
        if let Some(lit) = self.lit {
            lit.group
        } else {
            self.last_group
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
    ) -> (RleState<'a, P>, Option<Run<'a, P>>, usize, SlabWriter<'a>) {
        let mut post = None;
        let mut group = cursor.group();

        let last_run = run.as_ref().map(|r| r.count).unwrap_or(0);

        let copy_range = cursor.copy_range();
        let copy_group = cursor.copy_group();
        let copy_size = cursor.copy_size(last_run);
        let copy_lit_range = cursor.lit_range();
        let copy_lit_group = cursor.lit_group();
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
                group -= run.group();
                post = Some(run);
                let count = count - delta;
                RleState::Run { count, value }
            }
            Some(Run { count, value }) => RleState::Run { count, value },
        };

        let mut current = SlabWriter::new(B);

        current.flush_before(slab, copy_range, 0, copy_size, copy_group);

        if copy_lit_size > 0 {
            current.flush_before(
                slab,
                copy_lit_range,
                copy_lit_size,
                copy_lit_size,
                copy_lit_group,
            );
        }

        (state, post, group, current)
    }
}

impl<const B: usize, P: Packable + ?Sized> ColumnCursor for RleCursor<B, P> {
    type Item = P;
    type State<'a> = RleState<'a, P>;
    type PostState<'a> = Option<Run<'a, P>>;
    type Export = Option<P::Owned>;

    fn empty() -> Self {
        Self::default()
    }

    fn group(&self) -> usize {
        self.group
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
            (Some(a), Some(_b)) if a.len == slab.len() => {
                let lit = a.len - 2;
                writer.flush_before2(
                    slab,
                    c0.offset..c1.last_offset,
                    lit,
                    size,
                    c1.last_group - c0.group,
                );
            }
            (Some(a), Some(b)) => {
                let lit1 = a.len - 1;
                let lit2 = b.len - 1;
                let b_start = b.header_offset();
                writer.flush_before2(
                    slab,
                    c0.offset..b_start,
                    lit1,
                    size - lit2,
                    b.group - c0.group,
                );
                writer.flush_before2(
                    slab,
                    b.offset..c1.last_offset,
                    lit2,
                    lit2,
                    c1.last_group - b.group,
                );
            }
            (Some(a), None) => {
                let lit = a.len - 1;
                writer.flush_before2(
                    slab,
                    c0.offset..c1.last_offset,
                    lit,
                    size,
                    c1.last_group - c0.group,
                );
            }
            (None, Some(b)) => {
                let lit2 = b.len - 1;
                let b_start = b.header_offset();
                writer.flush_before2(slab, c0.offset..b_start, 0, size - lit2, b.group - c0.group);
                writer.flush_before2(
                    slab,
                    b.offset..c1.last_offset,
                    lit2,
                    lit2,
                    c1.last_group - b.group,
                );
            }
            _ => {
                writer.flush_before2(
                    slab,
                    c0.offset..c1.last_offset,
                    0,
                    size,
                    c1.last_group - c0.group,
                );
            }
        }

        let mut next_state = Self::State::default();
        Self::append_chunk(&mut next_state, writer, run);
        next_state
    }

    fn finalize_state<'a>(
        slab: &'a Slab,
        out: &mut SlabWriter<'a>,
        mut state: Self::State<'a>,
        post: Option<Run<'a, P>>,
        cursor: Self,
    ) -> Option<Self> {
        if let Some(run) = post {
            Self::append_chunk(&mut state, out, run);
        }
        if let Some((run, next_cursor)) = cursor.next(slab.as_slice()) {
            Self::append_chunk(&mut state, out, run);
            Self::flush_state(out, state);
            Some(next_cursor)
        } else {
            Self::flush_state(out, state);
            None
        }
    }

    fn finish<'a>(slab: &'a Slab, out: &mut SlabWriter<'a>, cursor: Self) {
        let num_left = cursor.num_left();
        out.flush_after(
            slab,
            cursor.offset,
            num_left,
            slab.len() - cursor.index,
            slab.group() - cursor.group(),
        );
    }

    fn append<'a>(
        old_state: &mut Self::State<'a>,
        out: &mut SlabWriter<'a>,
        value: Option<<Self::Item as Packable>::Unpacked<'a>>,
    ) -> usize {
        Self::append_chunk(old_state, out, Run { count: 1, value })
    }

    fn flush_state<'a>(out: &mut SlabWriter<'a>, state: RleState<'a, Self::Item>) {
        match state {
            RleState::Empty => (),
            RleState::LoneValue(Some(value)) => out.flush_lit_run(&[value]),
            RleState::LoneValue(None) => out.flush_null(1),
            RleState::Run {
                count,
                value: Some(v),
            } => out.flush_run(count as i64, v),
            RleState::Run { count, value: None } => out.flush_null(count),
            RleState::LitRun { mut run, current } => {
                run.push(current);
                out.flush_lit_run(&run);
            }
        }
    }

    fn append_chunk<'a>(
        old_state: &mut RleState<'a, P>,
        out: &mut SlabWriter<'a>,
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
                    Self::flush_run(out, 1, a);
                    RleState::from(chunk)
                }
            },
            RleState::Run { count, value } if chunk.value == value => {
                RleState::from(chunk.plus(count))
            }
            RleState::Run { count, value } => {
                Self::flush_run(out, count, value);
                RleState::from(chunk)
            }
            RleState::LitRun { mut run, current } => {
                match (current, chunk.value) {
                    (a, Some(b)) if a == b => {
                        // the end of the lit run merges with the next
                        out.flush_lit_run(&run);
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
                        out.flush_lit_run(&run);
                        RleState::from(chunk)
                    }
                }
            }
        };
        *old_state = new_state;
        chunk.count
    }

    fn encode(index: usize, del: usize, slab: &Slab) -> Encoder<'_, Self> {
        // FIXME encode
        let (run, cursor) = Self::seek(index, slab);

        let (state, post, group, current) = RleCursor::encode_inner(slab, &cursor, run, index);

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
            group,
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

    #[cfg(test)]
    fn export(data: &[u8]) -> Vec<super::ColExport<Self::Item>> {
        let mut cursor = Self::default();
        let mut current = None;
        let mut result = vec![];
        while let Some((run, next)) = cursor.next(data) {
            match run {
                Run { count, value: None } => {
                    if let Some(run) = current.take() {
                        result.push(super::ColExport::litrun(run))
                    }
                    result.push(super::ColExport::Null(count))
                }
                Run {
                    count: 1,
                    value: Some(v),
                } => {
                    if next.num_left() == 0 {
                        let mut run = current.take().unwrap_or_default();
                        run.push(v);
                        result.push(super::ColExport::litrun(run))
                    } else if let Some(run) = &mut current {
                        run.push(v);
                    } else {
                        current = Some(vec![v]);
                    }
                }
                Run {
                    count,
                    value: Some(v),
                } => {
                    if let Some(run) = current.take() {
                        result.push(super::ColExport::litrun(run))
                    }
                    result.push(super::ColExport::run(count, v))
                }
            }
            cursor = next;
        }
        if let Some(run) = current.take() {
            result.push(super::ColExport::litrun(run))
        }
        result
    }

    fn try_next<'a>(
        &self,
        slab: &'a [u8],
    ) -> Result<Option<(Run<'a, Self::Item>, Self)>, PackError> {
        let data = &slab[self.offset..];
        if data.is_empty() {
            return Ok(None);
        }
        if self.num_left() > 0 {
            let (value_bytes, value) = P::unpack(data)?;
            let group = P::group(value);
            let lit = self.next_lit(1);
            let cursor = self.progress(1, value_bytes, lit, group);
            let value = Run {
                count: 1,
                value: Some(value),
            };
            Ok(Some((value, cursor)))
        } else {
            let (count_bytes, count) = i64::unpack(data)?;
            let data = &data[count_bytes..];
            match count {
                count if count > 0 => {
                    let count = count as usize;
                    let (value_bytes, value) = P::unpack(data)?;
                    let group = P::group(value) * count;
                    let lit = self.next_lit(count);
                    let cursor = self.progress(count, count_bytes + value_bytes, lit, group);
                    let value = Run {
                        count,
                        value: Some(value),
                    };
                    Ok(Some((value, cursor)))
                }
                count if count < 0 => {
                    let (value_bytes, value) = P::unpack(data)?;
                    assert!(-count < slab.len() as i64);
                    let lit = Some(LitRunCursor::new(
                        self.offset + count_bytes,
                        count,
                        self.group,
                    ));
                    let group = P::group(value);
                    let cursor = self.progress(1, count_bytes + value_bytes, lit, group);
                    let value = Run {
                        count: 1,
                        value: Some(value),
                    };
                    Ok(Some((value, cursor)))
                }
                _ => {
                    let (null_bytes, count) = u64::unpack(data)?;
                    let count = count as usize;
                    let cursor = self.progress(count, count_bytes + null_bytes, None, 0);
                    let value = Run { count, value: None };
                    Ok(Some((value, cursor)))
                }
            }
        }
    }

    fn index(&self) -> usize {
        self.index
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct LitRunCursor {
    index: usize,
    offset: usize,
    len: usize,
    group: usize,
}

impl LitRunCursor {
    fn new(offset: usize, count: i64, group: usize) -> Self {
        assert!(count < 0);
        let len = (-count) as usize;
        LitRunCursor {
            offset,
            index: 1,
            group,
            len,
        }
    }

    fn header_offset(&self) -> usize {
        self.offset - lebsize(-(self.len as i64)) as usize
    }

    fn num_left(&self) -> usize {
        self.len.saturating_sub(self.index)
    }

    fn next(&self, count: usize) -> Option<Self> {
        if self.index > self.len {
            None
        } else {
            Some(LitRunCursor {
                index: self.index + count,
                offset: self.offset,
                group: self.group,
                len: self.len,
            })
        }
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
    use super::super::columndata::ColumnData;
    use super::super::cursor::ColExport;
    use super::*;

    #[test]
    fn column_data_rle_slab_splitting() {
        let mut col1: ColumnData<RleCursor<4, u64>> = ColumnData::new();
        col1.splice(0, 0, vec![1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(
            col1.export(),
            vec![
                vec![ColExport::litrun(vec![1, 2, 3])],
                vec![ColExport::litrun(vec![4, 5, 6])],
                vec![ColExport::litrun(vec![7])],
            ]
        );
        let mut col2: ColumnData<RleCursor<10, str>> = ColumnData::new();
        col2.splice(0, 0, vec!["xxx1", "xxx2", "xxx3", "xxx3"]);
        assert_eq!(
            col2.export(),
            vec![
                vec![ColExport::litrun(vec!["xxx1", "xxx2"])],
                vec![ColExport::run(2, "xxx3")],
            ]
        );
        col2.splice(0, 0, vec!["xxx0"]);
        assert_eq!(
            col2.export(),
            vec![
                vec![ColExport::litrun(vec!["xxx0", "xxx1"])],
                vec![ColExport::litrun(vec!["xxx2"])],
                vec![ColExport::run(2, "xxx3")],
            ]
        );
        col2.splice(3, 0, vec!["xxx3", "xxx3"]);
        assert_eq!(
            col2.export(),
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
            col1.export(),
            vec![
                vec![ColExport::litrun(vec![1, 2, 3, 4])],
                vec![ColExport::litrun(vec![5, 6])],
            ]
        );
        col1.splice(0, 0, vec![9, 9]);
        assert_eq!(
            col1.export(),
            vec![
                vec![ColExport::run(2, 9), ColExport::litrun(vec![1])],
                vec![ColExport::litrun(vec![2, 3, 4])],
                vec![ColExport::litrun(vec![5, 6])],
            ]
        );
        col1.splice(5, 0, vec![4]);
        assert_eq!(
            col1.export(),
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
            col1.export(),
            vec![
                vec![ColExport::litrun(vec![1, 2, 3, 4])],
                vec![ColExport::litrun(vec![5, 6, 7, 8])],
                vec![ColExport::litrun(vec![9, 10, 11, 12])],
            ]
        );
        let mut sum = 0;
        for (val, g) in col1.iter().with_group() {
            assert_eq!(sum, g);
            if let Some(v) = val {
                sum += u64::group(v);
            }
        }
        let mut out = Vec::new();
        col1.write(&mut out);
        assert_eq!(out, vec![116, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);

        // lit run capped by runs
        let mut col2: ColumnData<RleCursor<5, u64>> = ColumnData::new();
        col2.splice(0, 0, vec![1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 10]);
        assert_eq!(
            col2.export(),
            vec![
                vec![ColExport::run(2, 1), ColExport::litrun(vec![2, 3])],
                vec![ColExport::litrun(vec![4, 5, 6, 7])],
                vec![ColExport::litrun(vec![8, 9]), ColExport::run(2, 10)],
            ]
        );
        let mut sum = 0;
        for (val, g) in col2.iter().with_group() {
            assert_eq!(sum, g);
            if let Some(v) = val {
                sum += u64::group(v);
            }
        }
        let mut out = Vec::new();
        col2.write(&mut out);
        assert_eq!(out, vec![2, 1, 120, 2, 3, 4, 5, 6, 7, 8, 9, 2, 10]);

        // lit run capped by runs
        let mut col3: ColumnData<RleCursor<5, u64>> = ColumnData::new();
        col3.splice(0, 0, vec![1, 2, 3, 4, 4, 5, 5, 6, 7, 8, 9, 10, 11, 11]);
        assert_eq!(
            col3.export(),
            vec![
                vec![ColExport::litrun(vec![1, 2, 3]), ColExport::run(2, 4),],
                vec![ColExport::run(2, 5), ColExport::litrun(vec![6, 7]),],
                vec![ColExport::litrun(vec![8, 9, 10]), ColExport::run(2, 11)],
            ]
        );
        let mut sum = 0;

        for (val, g) in col3.iter().with_group() {
            assert_eq!(sum, g);
            if let Some(v) = val {
                sum += u64::group(v);
            }
        }
        let mut out = Vec::new();
        col3.write(&mut out);
        assert_eq!(
            out,
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
            col4.export(),
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
        let mut sum = 0;
        for (val, g) in col4.iter().with_group() {
            assert_eq!(sum, g);
            if let Some(v) = val {
                sum += u64::group(v);
            }
        }
        let mut out = Vec::new();
        col4.write(&mut out);
        assert_eq!(
            out,
            vec![2, 1, 2, 2, 2, 3, 2, 4, 2, 5, 2, 6, 2, 7, 2, 8, 2, 9]
        );

        // empty data
        let col5: ColumnData<RleCursor<5, u64>> = ColumnData::new();
        assert_eq!(col5.export(), vec![vec![]]);
        let mut out = Vec::new();
        col5.write(&mut out);
        assert_eq!(out, Vec::<u8>::new());
    }

    #[test]
    fn column_data_rle_splice_delete() {
        let mut col1: ColumnData<RleCursor<1024, u64>> = ColumnData::new();
        col1.splice(0, 0, vec![1, 1, 1, 2, 3, 4, 5, 6, 9, 9]);
        assert_eq!(
            col1.export(),
            vec![vec![
                ColExport::run(3, 1),
                ColExport::litrun(vec![2, 3, 4, 5, 6]),
                ColExport::run(2, 9),
            ]]
        );
        col1.splice::<u64>(1, 1, vec![]);
        assert_eq!(
            col1.export(),
            vec![vec![
                ColExport::run(2, 1),
                ColExport::litrun(vec![2, 3, 4, 5, 6]),
                ColExport::run(2, 9),
            ]]
        );
        let mut col2 = col1.clone();
        col2.splice::<u64>(0, 1, vec![]);
        assert_eq!(
            col2.export(),
            vec![vec![
                ColExport::litrun(vec![1, 2, 3, 4, 5, 6]),
                ColExport::run(2, 9),
            ]]
        );

        let mut col3 = col1.clone();
        col3.splice::<u64>(1, 7, vec![]);
        assert_eq!(col3.export(), vec![vec![ColExport::litrun(vec![1, 9]),]]);
    }

    #[test]
    fn rle_breaking_runs_near_lit_runs() {
        let mut col1: ColumnData<RleCursor<1024, u64>> = ColumnData::new();
        col1.splice(0, 0, vec![1, 2, 4, 4, 4, 5, 6]);
        assert_eq!(
            col1.export(),
            vec![vec![
                ColExport::litrun(vec![1, 2]),
                ColExport::run(3, 4),
                ColExport::litrun(vec![5, 6]),
            ]]
        );
        col1.splice::<u64>(3, 1, vec![9]);
        assert_eq!(
            col1.export(),
            vec![vec![ColExport::litrun(vec![1, 2, 4, 9, 4, 5, 6]),]]
        );
    }
}
