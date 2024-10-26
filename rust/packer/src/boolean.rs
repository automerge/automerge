use super::aggregate::Acc;
use super::cursor::{ColumnCursor, Encoder, Run, SpliceDel};
use super::pack::{PackError, Packable};
use super::slab::{Slab, SlabWriter};
use super::ulebsize;

use std::ops::Range;

#[derive(Debug, PartialEq, Default, Clone)]
pub struct BooleanState {
    value: bool,
    count: usize,
}

impl BooleanState {
    fn acc(&self) -> Acc {
        if self.value {
            Acc::from(self.count)
        } else {
            Acc::new()
        }
    }
}

impl<'a> From<Run<'a, bool>> for BooleanState {
    fn from(run: Run<'a, bool>) -> Self {
        Self {
            count: run.count,
            value: run.value.unwrap_or(false),
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct BooleanCursorInternal<const B: usize> {
    value: bool,
    index: usize,
    offset: usize,
    acc: Acc,
    last_offset: usize,
}

pub type BooleanCursor = BooleanCursorInternal<64>;

impl<const B: usize> ColumnCursor for BooleanCursorInternal<B> {
    type Item = bool;
    type State<'a> = BooleanState;
    type PostState<'a> = Option<BooleanState>;
    type Export = bool;

    fn empty() -> Self {
        Self::default()
    }

    fn finish<'a>(slab: &'a Slab, out: &mut SlabWriter<'a>, cursor: Self) {
        out.flush_after(
            slab,
            cursor.offset,
            0,
            slab.len() - cursor.index,
            slab.acc() - cursor.acc,
            Some(cursor.value),
        );
    }

    fn finalize_state<'a>(
        slab: &'a Slab,
        out: &mut SlabWriter<'a>,
        mut state: Self::State<'a>,
        post: Self::PostState<'a>,
        mut cursor: Self,
    ) -> Option<Self> {
        if let Some(post) = post {
            if post.value == state.value {
                state.count += post.count;
                Self::finalize_state(slab, out, state, None, cursor)
            } else {
                out.flush_bool_run(state.count, state.value);
                out.flush_bool_run(post.count, post.value);
                Some(cursor)
            }
        } else {
            let old_cursor = cursor;
            if let Ok(Some(val)) = cursor.try_next(slab.as_slice()) {
                if val.value == Some(state.value) {
                    out.flush_bool_run(state.count + val.count, state.value);
                    Some(cursor)
                } else {
                    out.flush_bool_run(state.count, state.value);
                    Some(old_cursor)
                }
            } else {
                out.flush_bool_run(state.count, state.value);
                None
            }
        }
    }

    fn is_empty<'a>(v: Option<bool>) -> bool {
        v != Some(true)
    }

    fn copy_between<'a>(
        slab: &'a Slab,
        out: &mut SlabWriter<'a>,
        c0: Self,
        c1: Self,
        run: Run<'a, bool>,
        size: usize,
    ) -> Self::State<'a> {
        let c1_last_offset = c1.offset - ulebsize(run.count as u64) as usize;
        assert_eq!(c1_last_offset, c1.last_offset);
        out.flush_before2(slab, c0.offset..c1_last_offset, 0, size, Acc::new());
        let mut next_state = BooleanState {
            value: run.value.unwrap_or_default(),
            count: 0,
        };
        Self::append_chunk(&mut next_state, out, run);
        next_state
    }

    fn flush_state<'a>(out: &mut SlabWriter<'a>, state: Self::State<'a>) {
        out.flush_bool_run(state.count, state.value);
    }

    fn append_chunk<'a>(
        state: &mut Self::State<'a>,
        out: &mut SlabWriter<'a>,
        run: Run<'_, bool>,
    ) -> usize {
        let item = run.value.unwrap_or_default();
        if state.value == item {
            state.count += run.count;
        } else {
            if state.count > 0 {
                out.flush_bool_run(state.count, state.value);
            }
            state.value = item;
            state.count = run.count;
        }
        run.count
    }

    fn encode(index: usize, del: usize, slab: &Slab, cap: usize) -> Encoder<'_, Self> {
        // FIXME encode
        let (run, cursor) = Self::seek(index, slab);

        let count = run.map(|r| r.count).unwrap_or(0);
        let value = run.map(|r| r.value.unwrap_or(false)).unwrap_or(false);

        let mut state = BooleanState { count, value };
        let acc = cursor.acc - state.acc();
        let state2 = BooleanState::from(run.unwrap_or_default());
        assert_eq!(state, state2);
        let mut post = None;

        let delta = cursor.index - index;
        if delta > 0 {
            state.count -= delta;
            post = Some(Run {
                count: delta,
                value: Some(value),
            });
        }

        let range = 0..cursor.last_offset;
        let size = cursor.index - count;
        let mut current = SlabWriter::new(B, cap + 8);
        current.flush_before(slab, range, 0, size, acc);

        let SpliceDel {
            deleted,
            overflow,
            cursor,
            post,
        } = Self::splice_delete(post, cursor, del, slab);
        let post = post.map(BooleanState::from);
        let acc = Acc::new();

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
        data.splice(range, values.map(|e| e.unwrap_or(false)));
    }

    fn try_next<'a>(&mut self, slab: &'a [u8]) -> Result<Option<Run<'a, Self::Item>>, PackError> {
        if self.offset >= slab.len() {
            return Ok(None);
        }
        let data = &slab[self.offset..];
        let (bytes, count) = u64::unpack(data)?;
        let count = count as usize;
        let value = self.value;
        self.value = !value;
        self.index += count;
        self.last_offset = self.offset;
        self.offset += bytes;
        if value {
            self.acc += Acc::from(count); // agg(1) * count
        }
        let run = Run {
            count,
            value: Some(value),
        };
        Ok(Some(run))
    }

    fn index(&self) -> usize {
        self.index
    }

    fn init_empty(len: usize) -> Slab {
        if len > 0 {
            let mut writer = SlabWriter::new(usize::MAX, 2);
            writer.flush_bool_run(len, false);
            writer.finish().pop().unwrap_or_default()
        } else {
            Slab::default()
        }
    }

    fn acc(&self) -> Acc {
        self.acc
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::super::columndata::ColumnData;
    use super::super::test::ColExport;
    use super::*;

    #[test]
    fn column_data_boolean_split_merge_semantics() {
        // lit run spanning multiple slabs
        let mut col1: ColumnData<BooleanCursorInternal<4>> = ColumnData::new();
        col1.splice(
            0,
            0,
            vec![
                true, false, true, false, true, false, true, false, true, false,
            ],
        );
        assert_eq!(
            col1.test_dump(),
            vec![
                vec![
                    ColExport::run(1, true),
                    ColExport::run(1, false),
                    ColExport::run(1, true),
                ],
                vec![
                    ColExport::run(1, false),
                    ColExport::run(1, true),
                    ColExport::run(1, false),
                    ColExport::run(1, true),
                ],
                vec![
                    ColExport::run(1, false),
                    ColExport::run(1, true),
                    ColExport::run(1, false),
                ]
            ]
        );
        let mut out = Vec::new();
        col1.write(&mut out);
        assert_eq!(out, vec![0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]);

        let mut col2: ColumnData<BooleanCursorInternal<4>> = ColumnData::new();
        col2.splice(
            0,
            0,
            vec![
                false, false, false, true, true, true, false, false, false, true, true, true,
                false, false, false, true, true, true, false, false, false, true, true, true,
                false, false, false, true, true, true, false, false, false, true, true, true,
            ],
        );
        assert_eq!(
            col2.test_dump(),
            vec![
                vec![
                    ColExport::run(3, false),
                    ColExport::run(3, true),
                    ColExport::run(3, false),
                    ColExport::run(3, true),
                ],
                vec![
                    ColExport::run(3, false),
                    ColExport::run(3, true),
                    ColExport::run(3, false),
                    ColExport::run(3, true),
                ],
                vec![
                    ColExport::run(3, false),
                    ColExport::run(3, true),
                    ColExport::run(3, false),
                    ColExport::run(3, true),
                ],
            ]
        );
        let mut out = Vec::new();
        col2.write(&mut out);
        assert_eq!(out, vec![3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3]);

        // empty data
        let col5: ColumnData<BooleanCursor> = ColumnData::new();
        assert_eq!(col5.test_dump(), vec![vec![]]);
        let mut out = Vec::new();
        col5.write(&mut out);
        assert_eq!(out, vec![0]);
    }

    #[test]
    fn column_data_boolean_splice_del() {
        let mut col1: ColumnData<BooleanCursorInternal<4>> = ColumnData::new();
        col1.splice(
            0,
            0,
            vec![
                true, true, true, true, false, false, false, false, true, true,
            ],
        );
        assert_eq!(
            col1.test_dump(),
            vec![vec![
                ColExport::run(4, true),
                ColExport::run(4, false),
                ColExport::run(2, true),
            ]]
        );

        let mut col2 = col1.clone();
        col2.splice::<bool>(2, 2, vec![]);

        assert_eq!(
            col2.test_dump(),
            vec![vec![
                ColExport::run(2, true),
                ColExport::run(4, false),
                ColExport::run(2, true),
            ]]
        );

        let mut col3 = col1.clone();
        col3.splice::<bool>(2, 2, vec![false, false]);

        assert_eq!(
            col3.test_dump(),
            vec![vec![
                ColExport::run(2, true),
                ColExport::run(6, false),
                ColExport::run(2, true),
            ]]
        );

        let mut col4 = col1.clone();
        col4.splice::<bool>(2, 4, vec![]);

        assert_eq!(
            col4.test_dump(),
            vec![vec![
                ColExport::run(2, true),
                ColExport::run(2, false),
                ColExport::run(2, true),
            ]]
        );

        let mut col5 = col1.clone();
        col5.splice::<bool>(2, 7, vec![]);

        assert_eq!(col5.test_dump(), vec![vec![ColExport::run(3, true),]]);

        let mut col6 = col1.clone();
        col6.splice::<bool>(0, 4, vec![]);

        assert_eq!(
            col6.test_dump(),
            vec![vec![ColExport::run(4, false), ColExport::run(2, true),]]
        );

        let mut col7 = col1.clone();
        col7.splice::<bool>(0, 10, vec![false]);

        assert_eq!(col7.test_dump(), vec![vec![ColExport::run(1, false),]]);

        let mut col8 = col1.clone();
        col8.splice::<bool>(4, 4, vec![]);

        assert_eq!(col8.test_dump(), vec![vec![ColExport::run(6, true),]]);
    }
}
