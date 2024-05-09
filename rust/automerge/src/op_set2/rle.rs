use super::{ColExport, ColumnCursor, Encoder, PackError, Packable, Run, Slab, WritableSlab};
use crate::columnar::encoding::leb128::ulebsize;
use std::marker::PhantomData;

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

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct LitRunCopy<'a> {
    count: usize,
    extras: usize,
    bytes: &'a [u8],
}

impl<'a> LitRunCopy<'a> {
    fn new(count: usize, bytes: &'a [u8]) -> Self {
        LitRunCopy {
            count,
            extras: 0,
            bytes,
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
    fn split_lit_run(
        bytes_left: usize,
        mut size: usize,
        run: &[P::Unpacked<'_>],
    ) -> (usize, usize) {
        // figures out how many items can flit in the slab
        // doesnt count the leading i64
        let mut i = 0;
        let header = 1; // this is bigger than 1 for large runs but isnt worth computing
        for value in run {
            let width = P::width(*value);
            // alwasy write at least one item - even if its over
            if size > 0 && header + size >= bytes_left {
                break;
            }
            size += width;
            i += 1;
        }
        (i, bytes_left.saturating_sub(size + header))
    }

    fn flush_lit_run(
        out: &mut WritableSlab,
        before: Option<LitRunCopy<'_>>,
        run: &[P::Unpacked<'_>],
        after: Option<LitRunCopy<'_>>,
    ) {
        let before = before.unwrap_or_default();
        let after = after.unwrap_or_default();
        let num_left = after.count;
        let extras = after.extras;

        let (len, bytes_left) = Self::split_lit_run(out.bytes_left(B), before.bytes.len(), run);
        if bytes_left > after.bytes.len() {
            let total = (before.count + run.len() + after.count) as i64;
            out.append_i64(-1 * total);
            out.append_bytes(before.bytes);
            for value in run {
                out.append(*value);
            }
            out.append_bytes(after.bytes);
            out.add_len(total as usize + after.extras);
        } else {
            let total = (before.count + len) as i64;
            out.append_i64(-1 * total);
            out.append_bytes(before.bytes);
            for value in &run[0..len] {
                out.append(*value);
            }
            out.add_len(total as usize);
            if len < run.len() {
                out.next_slab();
                Self::flush_lit_run(out, None, &run[len..], Some(after));
            } else {
                out.next_slab();
                if after.count > 0 {
                    out.append_i64(-1 * after.count as i64);
                }
                out.append_bytes(after.bytes);
                out.add_len(after.count + after.extras);
            }
        }
    }

    pub(crate) fn flush_state(slab: &mut WritableSlab, state: RleState<'_, P>) {
        match state {
            RleState::Empty => (),
            RleState::LoneValue(value) => Self::flush_run(slab, 1, value),
            RleState::Run { count, value } => Self::flush_run(slab, count, value),
            RleState::LitRun {
                before,
                mut run,
                current,
            } => {
                run.push(current);
                Self::flush_lit_run(slab, Some(before), &run, None);
            }
        }
    }

    pub(crate) fn flush_run(out: &mut WritableSlab, num: usize, value: Option<P::Unpacked<'_>>) {
        if let Some(v) = value {
            if num == 1 {
                out.append_i64(-1);
            } else {
                out.append_i64(num as i64);
            }
            out.append(v);
            out.add_len(num as usize)
        } else {
            out.append_i64(0);
            out.append_usize(num);
            out.add_len(num as usize)
        }
        if out.bytes_left(B) == 0 {
            out.next_slab();
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
                &slab.as_ref()[lit.offset..self.last_offset]
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

    pub(crate) fn start_copy(&self, slab: &Slab, last_run_count: usize) -> WritableSlab {
        let (range, size) = if let Some(lit) = self.lit {
            let end = lit.offset - ulebsize(lit.len as u64) as usize;
            let size = self.index - lit.index;
            (0..end, size)
        } else {
            (0..self.last_offset, self.index - last_run_count)
        };
        WritableSlab::new(&slab.as_ref()[range], size)
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
                before: LitRunCopy::new(cursor.lit_num() - 1, cursor.copy(slab)),
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

    fn append_chunk<'a>(
        old_state: &mut RleState<'a, P>,
        slab: &mut WritableSlab,
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
                    Self::flush_run(slab, 1, a);
                    RleState::from(chunk)
                }
            },
            RleState::Run { count, value } if chunk.value == value => {
                RleState::from(chunk.plus(count))
            }
            RleState::Run { count, value } => {
                Self::flush_run(slab, count, value);
                RleState::from(chunk)
            }
            RleState::LitRun {
                before,
                mut run,
                current,
            } => {
                match (current, chunk.value) {
                    (a, Some(b)) if a == b => {
                        // the end of the lit run merges with the next
                        Self::flush_lit_run(slab, Some(before), &run, None);
                        RleState::from(chunk.plus(1))
                    }
                    (a, Some(b)) if chunk.count == 1 => {
                        // its single and different - addit to the lit run
                        run.push(a);
                        RleState::LitRun {
                            before,
                            run,
                            current: b,
                        }
                    }
                    _ => {
                        // flush this lit run (current and all) - next run replaces it
                        run.push(current);
                        Self::flush_lit_run(slab, Some(before), &run, None);
                        RleState::from(chunk)
                    }
                }
            }
        };
        *old_state = new_state;
    }
}

impl<const B: usize, P: Packable + ?Sized> ColumnCursor for RleCursor<B, P> {
    type Item = P;
    type State<'a> = RleState<'a, P>;
    type PostState<'a> = Option<Run<'a, P>>;
    type Export = Option<P::Owned>;

    fn finish<'a>(
        slab: &'a Slab,
        out: &mut WritableSlab,
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
        let after = LitRunCopy {
            count: num_left,
            extras: slab.len() - cursor.index - num_left,
            bytes: &slab.as_ref()[cursor.offset..],
        };

        match state {
            RleState::LoneValue(Some(value)) if num_left > 0 => {
                Self::flush_lit_run(out, None, &[value], Some(after));
            }
            RleState::LitRun {
                before,
                mut run,
                current,
            } if num_left > 0 => {
                run.push(current);
                Self::flush_lit_run(out, Some(before), &run, Some(after));
            }
            state => {
                Self::flush_state(out, state);
                if num_left > 0 {
                    Self::flush_lit_run(out, None, &[], Some(after));
                } else {
                    out.append_bytes(after.bytes);
                    out.add_len(after.extras);
                }
            }
        }
    }

    fn append<'a>(
        old_state: &mut Self::State<'a>,
        slab: &mut WritableSlab,
        value: Option<<Self::Item as Packable>::Unpacked<'a>>,
    ) {
        Self::append_chunk(old_state, slab, Run { count: 1, value })
    }

    fn encode<'a>(index: usize, slab: &'a Slab) -> Encoder<'a, Self> {
        let (run, cursor) = Self::seek(index, slab.as_ref());

        let last_run_count = run.as_ref().map(|r| r.count).unwrap_or(0);

        let (state, post) = RleCursor::encode_inner(&cursor, run, index, slab);

        let current = cursor.start_copy(slab, last_run_count);

        Encoder {
            slab,
            results: vec![],
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

#[derive(Debug, Clone)]
pub(crate) enum RleState<'a, P: Packable + ?Sized> {
    Empty,
    LoneValue(Option<P::Unpacked<'a>>),
    Run {
        count: usize,
        value: Option<P::Unpacked<'a>>,
    },
    LitRun {
        before: LitRunCopy<'a>,
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
            before: LitRunCopy::default(),
            run: vec![a],
            current: b,
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::super::columns::{ColExport, ColumnData};
    use super::*;
    //use rand::prelude::*;
    //use rand::rngs::SmallRng;

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
                vec![ColExport::Run(2, String::from("xxx3"))],
            ]
        );
        col2.splice(0, vec!["xxx0"]);
        assert_eq!(
            col2.export(),
            vec![
                vec![ColExport::litrun(vec!["xxx0", "xxx1"])],
                vec![ColExport::litrun(vec!["xxx2"])],
                vec![ColExport::Run(2, String::from("xxx3"))],
            ]
        );
        col2.splice(3, vec!["xxx3", "xxx3"]);
        assert_eq!(
            col2.export(),
            vec![
                vec![ColExport::litrun(vec!["xxx0", "xxx1"])],
                vec![
                    ColExport::litrun(vec!["xxx2"]),
                    ColExport::Run(2, String::from("xxx3"))
                ],
                vec![ColExport::Run(2, String::from("xxx3"))],
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
}
