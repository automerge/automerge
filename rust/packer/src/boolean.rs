use super::cursor::{ColumnCursor, Encoder, Run, SpliceDel};
use super::pack::{PackError, Packable};
use super::slab::{Slab, SlabWriter};

use std::ops::Range;

#[derive(Debug, PartialEq, Default, Clone)]
pub struct BooleanState {
    value: bool,
    count: usize,
}

impl<'a> From<Run<'a, bool>> for BooleanState {
    fn from(run: Run<'a, bool>) -> Self {
        Self {
            count: run.count,
            value: run.value.unwrap_or(false),
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct BooleanCursorInternal<const B: usize> {
    value: bool,
    index: usize,
    offset: usize,
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
        out.flush_after(slab, cursor.offset, 0, slab.len() - cursor.index, 0);
    }

    fn finalize_state<'a>(
        slab: &'a Slab,
        out: &mut SlabWriter<'a>,
        mut state: Self::State<'a>,
        post: Self::PostState<'a>,
        cursor: Self,
    ) -> Option<Self> {
        if let Some(post) = post {
            if post.value == state.value {
                state.count += post.count;
                Self::finalize_state(slab, out, state, None, cursor)
            } else {
                out.flush_bool_run(state.count);
                out.flush_bool_run(post.count);
                Some(cursor)
            }
        } else if let Ok(Some((val, next_cursor))) = cursor.try_next(slab.as_slice()) {
            if val.value == Some(state.value) {
                out.flush_bool_run(state.count + val.count);
                Some(next_cursor)
            } else {
                out.flush_bool_run(state.count);
                Some(cursor)
            }
        } else {
            out.flush_bool_run(state.count);
            None
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
        out.flush_before2(slab, c0.offset..c1.last_offset, 0, size, 0);
        let mut next_state = BooleanState {
            value: run.value.unwrap_or_default(),
            count: 0,
        };
        Self::append_chunk(&mut next_state, out, run);
        next_state
    }

    fn flush_state<'a>(out: &mut SlabWriter<'a>, state: Self::State<'a>) {
        out.flush_bool_run(state.count);
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
            out.flush_bool_run(state.count);
            state.value = item;
            state.count = run.count;
        }
        run.count
    }

    fn encode(index: usize, del: usize, slab: &Slab) -> Encoder<'_, Self> {
        // FIXME encode
        let (run, cursor) = Self::seek(index, slab);

        let count = run.map(|r| r.count).unwrap_or(0);
        let value = run.map(|r| r.value.unwrap_or(false)).unwrap_or(false);

        let mut state = BooleanState { count, value };
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
        let mut current = SlabWriter::new(B);
        current.flush_before(slab, range, 0, size, 0);

        let SpliceDel {
            deleted,
            overflow,
            cursor,
            post,
        } = Self::splice_delete(post, cursor, del, slab);
        let post = post.map(BooleanState::from);
        let group = 0;

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
        data.splice(range, values.map(|e| e.unwrap_or(false)));
    }

    #[cfg(test)]
    fn export(data: &[u8]) -> Vec<super::ColExport<bool>> {
        let mut result = vec![];
        let mut cursor = Self::default();
        while let Ok(Some((Run { count, value }, next_cursor))) = cursor.try_next(data) {
            cursor = next_cursor;
            if count > 0 {
                result.push(super::ColExport::Run(count, value.unwrap()))
            }
        }
        result
    }

    fn try_next<'a>(
        &self,
        slab: &'a [u8],
    ) -> Result<Option<(Run<'a, Self::Item>, Self)>, PackError> {
        if self.offset >= slab.len() {
            return Ok(None);
        }
        let data = &slab[self.offset..];
        let (bytes, count) = u64::unpack(data)?;
        let count = count as usize;
        let mut cursor = *self;
        cursor.value = !self.value;
        cursor.index += count;
        cursor.last_offset = self.offset;
        cursor.offset += bytes;
        let run = Run {
            count,
            value: Some(self.value),
        };
        Ok(Some((run, cursor)))
    }

    fn index(&self) -> usize {
        self.index
    }

    fn init_empty(len: usize) -> Slab {
        let mut writer = SlabWriter::new(usize::MAX);
        writer.flush_bool_run(len);
        writer.finish().pop().unwrap_or_default()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::super::columndata::ColumnData;
    use super::super::cursor::ColExport;
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
            col1.export(),
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
            col2.export(),
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
        assert_eq!(col5.export(), vec![vec![]]);
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
            col1.export(),
            vec![vec![
                ColExport::run(4, true),
                ColExport::run(4, false),
                ColExport::run(2, true),
            ]]
        );

        let mut col2 = col1.clone();
        col2.splice::<bool>(2, 2, vec![]);

        assert_eq!(
            col2.export(),
            vec![vec![
                ColExport::run(2, true),
                ColExport::run(4, false),
                ColExport::run(2, true),
            ]]
        );

        let mut col3 = col1.clone();
        col3.splice::<bool>(2, 2, vec![false, false]);

        assert_eq!(
            col3.export(),
            vec![vec![
                ColExport::run(2, true),
                ColExport::run(6, false),
                ColExport::run(2, true),
            ]]
        );

        let mut col4 = col1.clone();
        col4.splice::<bool>(2, 4, vec![]);

        assert_eq!(
            col4.export(),
            vec![vec![
                ColExport::run(2, true),
                ColExport::run(2, false),
                ColExport::run(2, true),
            ]]
        );

        let mut col5 = col1.clone();
        col5.splice::<bool>(2, 7, vec![]);

        assert_eq!(col5.export(), vec![vec![ColExport::run(3, true),]]);

        let mut col6 = col1.clone();
        col6.splice::<bool>(0, 4, vec![]);

        assert_eq!(
            col6.export(),
            vec![vec![ColExport::run(4, false), ColExport::run(2, true),]]
        );

        let mut col7 = col1.clone();
        col7.splice::<bool>(0, 10, vec![false]);

        assert_eq!(col7.export(), vec![vec![ColExport::run(1, false),]]);

        let mut col8 = col1.clone();
        col8.splice::<bool>(4, 4, vec![]);

        assert_eq!(col8.export(), vec![vec![ColExport::run(6, true),]]);
    }
}
