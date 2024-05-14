use super::{ColExport, ColumnCursor, Encoder, PackError, Packable, Run, Slab, SlabWriter};
use crate::columnar::encoding::leb128::ulebsize;
use std::marker::PhantomData;
use std::ops::Range;

#[derive(Debug)]
pub(crate) struct RleCursor<const B: usize, P: Packable + ?Sized> {
    offset: usize,
    last_offset: usize,
    index: usize,
    lit: Option<LitRunCursor>,
    _phantom: PhantomData<P>,
}

// FIXME phantom data <str> seems to mess up the clone copy macros

impl<const B: usize, P: Packable + ?Sized> Copy for RleCursor<B, P> {}

impl<const B: usize, P: Packable + ?Sized> Clone for RleCursor<B, P> {
    fn clone(&self) -> Self {
        Self {
            offset: self.offset,
            last_offset: self.last_offset,
            index: self.index,
            lit: self.lit,
            _phantom: PhantomData,
        }
    }
}

impl<const B: usize, P: Packable + ?Sized> Default for RleCursor<B, P> {
    fn default() -> Self {
        Self {
            offset: 0,
            last_offset: 0,
            index: 0,
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

    fn active_lit(&self) -> Option<&LitRunCursor> {
        match &self.lit {
            Some(lit) if lit.num_left() > 0 => Some(lit),
            _ => None,
        }
    }

    fn lit_final(&self) -> bool {
        match &self.lit {
            Some(lit) if lit.num_left() == 0 => true,
            _ => false,
        }
    }

    fn progress(&self, count: usize, bytes: usize, lit: Option<LitRunCursor>) -> Self {
        RleCursor {
            last_offset: self.offset,
            offset: self.offset + bytes,
            index: self.index + count,
            lit,
            _phantom: PhantomData,
        }
    }

    fn num_left(&self) -> usize {
        self.lit.as_ref().map(|l| l.num_left()).unwrap_or(0)
    }

    fn copy<'a>(&self, slab: &'a Slab) -> &'a [u8] {
        if let Some(lit) = &self.lit {
            if self.last_offset > lit.offset {
                &slab[lit.offset..self.last_offset]
            } else {
                &[]
            }
        } else {
            &[]
        }
    }

    fn lit_num(&self) -> usize {
        if let Some(lit) = &self.lit {
            lit.index
        } else {
            0
        }
    }

    fn lit_range(&self) -> Range<usize> {
        if let Some(lit) = &self.lit {
            lit.offset..self.last_offset
        } else {
            0..0
        }
    }

    pub(crate) fn start_copy<'a>(&self, slab: &'a Slab, last_run_count: usize) -> SlabWriter<'a> {
        let (range, size) = if let Some(lit) = self.lit {
            let end = lit.offset - ulebsize(lit.len as u64) as usize;
            let size = self.index - lit.index;
            (0..end, size)
        } else {
            (0..self.last_offset, self.index - last_run_count)
        };
        let mut out = SlabWriter::new(B);
        out.flush_before(slab, range, 0, size);
        out
    }

    pub(crate) fn encode_inner<'a>(
        cursor: &Self,
        run: Option<Run<'a, P>>,
        index: usize,
        slab: &'a Slab,
    ) -> (RleState<'a, P>, Option<Run<'a, P>>) {
        let mut post = None;

        let state = match run {
            None => RleState::Empty,
            Some(Run {
                count: 1,
                value: Some(value),
            }) if cursor.lit_num() > 1 => RleState::LitRun {
                run: vec![],
                current: value,
            },
            Some(Run { count: 1, value }) => RleState::LoneValue(value),
            Some(Run { count, value }) if index < cursor.index => {
                let run_delta = cursor.index - index;
                post = Some(Run {
                    count: run_delta,
                    value: value,
                });
                RleState::Run {
                    count: count - run_delta,
                    value,
                }
            }
            Some(Run { count, value }) => RleState::Run { count, value },
        };

        (state, post)
    }
}

impl<const B: usize, P: Packable + ?Sized> ColumnCursor for RleCursor<B, P> {
    type Item = P;
    type State<'a> = RleState<'a, P>;
    type PostState<'a> = Option<Run<'a, P>>;
    type Export = Option<P::Owned>;

    fn copy_between<'a>(
        slab: &'a Slab,
        writer: &mut SlabWriter<'a>,
        c0: Self,
        c1: Self,
        run: Run<'a, Self::Item>,
        size: usize,
    ) -> Self::State<'a> {
        match (&c0.lit, &c1.lit) {
            (Some(a), Some(b)) if a.len == slab.len() => {
                let lit = a.len - 2;
                writer.flush_before2(slab, c0.offset..c1.last_offset, lit, size);
            }
            (Some(a), Some(b)) => {
                let lit1 = a.len - 1;
                let lit2 = b.len - 1;
                writer.flush_before2(slab, c0.offset..b.offset, lit1, size - lit2);
                writer.flush_before2(slab, b.offset..c1.last_offset, lit2, lit2);
            }
            (Some(a), None) => {
                let lit = a.len - 1;
                writer.flush_before2(slab, c0.offset..c1.last_offset, lit, size);
            }
            (None, Some(b)) => {
                let lit2 = b.len - 1;
                writer.flush_before2(slab, c0.offset..b.offset, 0, size - lit2);
                writer.flush_before2(slab, b.offset..c1.last_offset, lit2, lit2);
            }
            _ => {
                writer.flush_before2(slab, c0.offset..c1.last_offset, 0, size);
            }
        }

        let mut next_state = Self::State::default();
        Self::append_chunk(&mut next_state, writer, run);
        next_state
    }

    fn finish<'a>(
        slab: &'a Slab,
        out: &mut SlabWriter<'a>,
        mut state: Self::State<'a>,
        post: Option<Run<'a, P>>,
        mut cursor: Self,
    ) {
        if let Some(run) = post {
            Self::append_chunk(&mut state, out, run);
        } else if let Some((run, next_cursor)) = cursor.next(slab.as_ref()) {
            Self::append_chunk(&mut state, out, run);
            cursor = next_cursor;
        }

        let num_left = cursor.num_left();

        match state {
            RleState::LoneValue(Some(value)) if num_left > 0 => {
                out.flush_lit_run(&[value]);
            }
            RleState::LitRun { mut run, current } if num_left > 0 => {
                run.push(current);
                out.flush_lit_run(&run);
            }
            state => {
                Self::flush_state(out, state);
            }
        }
        out.flush_after(slab, cursor.offset, num_left, slab.len() - cursor.index);
    }

    fn append<'a>(
        old_state: &mut Self::State<'a>,
        out: &mut SlabWriter<'a>,
        value: Option<<Self::Item as Packable>::Unpacked<'a>>,
    ) {
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
    ) {
        let mut state = RleState::Empty;
        std::mem::swap(&mut state, old_state);
        let new_state = match state {
            RleState::Empty => RleState::from(chunk),
            RleState::LoneValue(value) => match (value, chunk.value) {
                (a, b) if a == b => RleState::from(chunk.plus(1)),
                (Some(a), Some(b)) if chunk.count == 1 => RleState::lit_run(a, b),
                (a, b) => {
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
    }

    fn encode<'a>(index: usize, slab: &'a Slab) -> Encoder<'a, Self> {
        let (run, cursor) = Self::seek(index, slab.as_ref());

        let last_run_count = run.as_ref().map(|r| r.count).unwrap_or(0);

        let (state, post) = RleCursor::encode_inner(&cursor, run, index, slab);

        let mut current = cursor.start_copy(slab, last_run_count);

        if cursor.lit_num() > 1 {
            let num = cursor.lit_num() - 1;
            current.flush_before(slab, cursor.lit_range(), num, num);
        }

        Encoder {
            slab,
            current,
            post,
            state,
            cursor,
        }
    }

    fn export_item(item: Option<<Self::Item as Packable>::Unpacked<'_>>) -> Option<P::Owned> {
        item.map(|i| P::own(i))
    }

    fn export(data: &[u8]) -> Vec<ColExport<Self::Item>> {
        let mut cursor = Self::default();
        let mut current = None;
        let mut result = vec![];
        while let Some((run, next)) = cursor.next(data) {
            match run {
                Run { count, value: None } => {
                    if let Some(run) = current.take() {
                        result.push(ColExport::litrun(run))
                    }
                    result.push(ColExport::Null(count))
                }
                Run {
                    count: 1,
                    value: Some(v),
                } => {
                    if next.lit_final() {
                        let mut run = current.take().unwrap_or_default();
                        run.push(v);
                        result.push(ColExport::litrun(run))
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
                        result.push(ColExport::litrun(run))
                    }
                    result.push(ColExport::run(count, v))
                }
            }
            cursor = next;
        }
        if let Some(run) = current.take() {
            result.push(ColExport::litrun(run))
        }
        result
    }

    fn try_next<'a>(
        &self,
        slab: &'a [u8],
    ) -> Result<Option<(Run<'a, Self::Item>, Self)>, PackError> {
        // not an error for going past the end?
        let data = &slab[self.offset..];
        if data.len() == 0 {
            return Ok(None);
        }
        if let Some(lit) = self.active_lit() {
            let (value_bytes, value) = P::unpack(data)?;
            let cursor = self.progress(1, value_bytes, lit.next());
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
                    let cursor = self.progress(count, count_bytes + value_bytes, None);
                    let value = Run {
                        count,
                        value: Some(value),
                    };
                    Ok(Some((value, cursor)))
                }
                count if count < 0 => {
                    let (value_bytes, value) = P::unpack(data)?;
                    let lit = Some(LitRunCursor::new(self.offset + count_bytes, count));
                    let cursor = self.progress(1, count_bytes + value_bytes, lit);
                    let value = Run {
                        count: 1,
                        value: Some(value),
                    };
                    Ok(Some((value, cursor)))
                }
                _ => {
                    let (null_bytes, count) = u64::unpack(data)?;
                    let count = count as usize;
                    let cursor = self.progress(count, count_bytes + null_bytes, None);
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
}

impl LitRunCursor {
    fn new(offset: usize, count: i64) -> Self {
        let len = (count * -1) as usize;
        LitRunCursor {
            offset,
            index: 1,
            len,
        }
    }

    fn num_left(&self) -> usize {
        self.len - self.index
    }

    fn next(&self) -> Option<Self> {
        let index = self.index + 1;
        if index > self.len {
            None
        } else {
            Some(LitRunCursor {
                index,
                offset: self.offset,
                len: self.len,
            })
        }
    }
}

pub(crate) type StrCursor = RleCursor<{ usize::MAX }, str>;
pub(crate) type IntCursor = RleCursor<{ usize::MAX }, u64>;
pub(crate) type ActorCursor = RleCursor<{ usize::MAX }, super::types::ActorIdx>;
pub(crate) type ActionCursor = RleCursor<{ usize::MAX }, super::types::Action>;

#[derive(Debug, Clone)]
pub(crate) enum RleState<'a, P: Packable + ?Sized> {
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

impl<'a, P: Packable + ?Sized> Default for RleState<'a, P> {
    fn default() -> Self {
        RleState::Empty
    }
}

impl<'a, P: Packable + ?Sized> RleState<'a, P> {
    fn lit_run(a: P::Unpacked<'a>, b: P::Unpacked<'a>) -> Self {
        RleState::LitRun {
            run: vec![a],
            current: b,
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::super::columns::{ColExport, ColumnData};
    use super::*;

    #[test]
    fn column_data_rle_slab_splitting() {
        let mut col1: ColumnData<RleCursor<4, u64>> = ColumnData::new();
        col1.splice(0, vec![1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(
            col1.export(),
            vec![
                vec![ColExport::litrun(vec![1, 2, 3])],
                vec![ColExport::litrun(vec![4, 5, 6])],
                vec![ColExport::litrun(vec![7])],
            ]
        );
        let mut col2: ColumnData<RleCursor<10, str>> = ColumnData::new();
        col2.splice(0, vec!["xxx1", "xxx2", "xxx3", "xxx3"]);
        assert_eq!(
            col2.export(),
            vec![
                vec![ColExport::litrun(vec!["xxx1", "xxx2"])],
                vec![ColExport::run(2, "xxx3")],
            ]
        );
        col2.splice(0, vec!["xxx0"]);
        assert_eq!(
            col2.export(),
            vec![
                vec![ColExport::litrun(vec!["xxx0", "xxx1"])],
                vec![ColExport::litrun(vec!["xxx2"])],
                vec![ColExport::run(2, "xxx3")],
            ]
        );
        col2.splice(3, vec!["xxx3", "xxx3"]);
        assert_eq!(
            col2.export(),
            vec![
                vec![ColExport::litrun(vec!["xxx0", "xxx1"])],
                vec![ColExport::litrun(vec!["xxx2"]), ColExport::run(2, "xxx3")],
                vec![ColExport::run(2, "xxx3")],
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
        col1.splice(0, vec![1, 2, 3, 4, 5, 6]);
        assert_eq!(
            col1.export(),
            vec![
                vec![ColExport::litrun(vec![1, 2, 3, 4])],
                vec![ColExport::litrun(vec![5, 6])],
            ]
        );
        col1.splice(0, vec![9, 9]);
        assert_eq!(
            col1.export(),
            vec![
                vec![ColExport::run(2, 9), ColExport::litrun(vec![1])],
                vec![ColExport::litrun(vec![2, 3, 4])],
                vec![ColExport::litrun(vec![5, 6])],
            ]
        );
        col1.splice(5, vec![4]);
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
        col1.splice(0, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
        assert_eq!(
            col1.export(),
            vec![
                vec![ColExport::litrun(vec![1, 2, 3, 4])],
                vec![ColExport::litrun(vec![5, 6, 7, 8])],
                vec![ColExport::litrun(vec![9, 10, 11, 12])],
            ]
        );
        let mut out = Vec::new();
        col1.write(&mut out);
        assert_eq!(out, vec![116, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);

        // lit run capped by runs
        let mut col2: ColumnData<RleCursor<5, u64>> = ColumnData::new();
        col2.splice(0, vec![1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 10]);
        assert_eq!(
            col2.export(),
            vec![
                vec![ColExport::run(2, 1), ColExport::litrun(vec![2, 3])],
                vec![ColExport::litrun(vec![4, 5, 6, 7])],
                vec![ColExport::litrun(vec![8, 9]), ColExport::run(2, 10)],
            ]
        );
        let mut out = Vec::new();
        col2.write(&mut out);
        assert_eq!(out, vec![2, 1, 120, 2, 3, 4, 5, 6, 7, 8, 9, 2, 10]);

        // lit run capped by runs
        let mut col3: ColumnData<RleCursor<5, u64>> = ColumnData::new();
        col3.splice(0, vec![1, 2, 3, 4, 4, 5, 5, 6, 7, 8, 9, 10, 11, 11]);
        assert_eq!(
            col3.export(),
            vec![
                vec![ColExport::litrun(vec![1, 2, 3]), ColExport::run(2, 4),],
                vec![ColExport::run(2, 5), ColExport::litrun(vec![6, 7]),],
                vec![ColExport::litrun(vec![8, 9, 10]), ColExport::run(2, 11)],
            ]
        );
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
        let mut out = Vec::new();
        col4.write(&mut out);
        assert_eq!(
            out,
            vec![2, 1, 2, 2, 2, 3, 2, 4, 2, 5, 2, 6, 2, 7, 2, 8, 2, 9]
        );

        // empty data
        let mut col5: ColumnData<RleCursor<5, u64>> = ColumnData::new();
        assert_eq!(col5.export(), vec![vec![]]);
        let mut out = Vec::new();
        col5.write(&mut out);
        assert_eq!(out, Vec::<u8>::new());
    }
}
