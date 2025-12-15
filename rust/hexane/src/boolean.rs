use super::aggregate::Acc;
use super::columndata::ColumnData;
use super::cursor::{ColumnCursor, Run, ScanMeta, SpliceDel};
use super::encoder::{Encoder, EncoderState, SpliceEncoder, Writer};
use super::pack::{PackError, Packable};
use super::slab::{Slab, SlabWeight, SlabWriter};
use super::Cow;

use std::ops::Range;

#[derive(Debug, PartialEq, Default, Clone)]
pub struct BooleanState {
    pub(crate) value: bool,
    pub(crate) count: usize,
    pub(crate) flushed: bool,
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
        let count = run.count;
        let value = *run.value.as_deref().unwrap_or(&false);
        let flushed = true;
        Self {
            count,
            value,
            flushed,
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
    type SlabIndex = SlabWeight;

    fn empty() -> Self {
        Self::default()
    }

    fn load_with(data: &[u8], m: &ScanMeta) -> Result<ColumnData<Self>, PackError> {
        let mut cursor = Self::empty();
        let mut last_cursor = Self::empty();
        let mut writer = SlabWriter::<bool>::new(B, true);
        let mut last_copy = Self::empty();
        while let Some(run) = cursor.try_next(data)? {
            bool::validate(run.value.as_deref(), m)?;
            if cursor.offset - last_copy.offset >= B {
                if !cursor.value {
                    cursor_copy(data, &mut writer, &last_copy, &cursor);
                    last_copy = cursor;
                } else {
                    cursor_copy(data, &mut writer, &last_copy, &last_cursor);
                    last_copy = last_cursor;
                }
                writer.manual_slab_break();
            }
            last_cursor = cursor;
        }
        cursor_copy(data, &mut writer, &last_copy, &cursor);
        Ok(writer.into_column(cursor.index))
    }

    fn finish<'a>(slab: &'a Slab, writer: &mut SlabWriter<'a, bool>, cursor: Self) {
        writer.copy(
            slab.as_slice(),
            cursor.offset..slab.as_slice().len(),
            0,
            slab.len() - cursor.index,
            slab.acc() - cursor.acc,
            Some(cursor.value),
        );
    }

    fn finalize_state<'a>(
        slab: &'a Slab,
        encoder: &mut Encoder<'a, Self>,
        post: Self::PostState<'a>,
        mut cursor: Self,
    ) -> Option<Self> {
        if let Some(post) = post {
            if post.value == encoder.state.value {
                encoder.state.count += post.count;
                Self::finalize_state(slab, encoder, None, cursor)
            } else {
                encoder
                    .writer
                    .flush_bool_run(encoder.state.count, encoder.state.value);
                encoder.writer.flush_bool_run(post.count, post.value);
                Some(cursor)
            }
        } else {
            let old_cursor = cursor;
            if let Ok(Some(val)) = cursor.try_next(slab.as_slice()) {
                if val.count == 0 {
                    Self::finalize_state(slab, encoder, None, cursor)
                } else if val.value == Some(Cow::Owned(encoder.state.value)) {
                    encoder
                        .writer
                        .flush_bool_run(encoder.state.count + val.count, encoder.state.value);
                    Some(cursor)
                } else {
                    encoder
                        .writer
                        .flush_bool_run(encoder.state.count, encoder.state.value);
                    Some(old_cursor)
                }
            } else {
                encoder
                    .writer
                    .flush_bool_run(encoder.state.count, encoder.state.value);
                None
            }
        }
    }

    fn is_empty(v: Option<Cow<'_, bool>>) -> bool {
        v.as_deref() != Some(&true)
    }

    fn copy_between<'a>(
        slab: &'a [u8],
        writer: &mut SlabWriter<'a, bool>,
        c0: Self,
        c1: Self,
        run: Run<'a, bool>,
        size: usize,
    ) -> Self::State<'a> {
        writer.copy(slab, c0.offset..c1.last_offset, 0, size, Acc::new(), None);
        let mut next_state = BooleanState {
            value: run.value.as_deref().copied().unwrap_or_default(),
            count: 0,
            flushed: true,
        };
        next_state.append_chunk(writer, run);
        next_state
    }

    fn slab_size() -> usize {
        B
    }

    fn splice_encoder(index: usize, del: usize, slab: &Slab) -> SpliceEncoder<'_, Self> {
        // FIXME encode
        let (run, cursor) = Self::seek(index, slab);

        let flushed = run.is_some();
        let count = run.as_ref().map(|r| r.count).unwrap_or(0);
        let value = run
            .as_ref()
            .and_then(|r| r.value.as_deref().cloned())
            .unwrap_or_default();

        let mut state = BooleanState {
            count,
            value,
            flushed,
        };
        let acc = cursor.acc - state.acc();
        //let state2 = BooleanState::from(run.unwrap_or_default());
        //assert_eq!(state, state2);
        let mut post = None;

        let delta = cursor.index - index;
        if delta > 0 {
            state.count -= delta;
            post = Some(Run {
                count: delta,
                value: Some(Cow::Owned(value)),
            });
        }

        let range = 0..cursor.last_offset;
        let size = cursor.index - count;
        let mut current = SlabWriter::new(B, false);
        current.copy(slab.as_slice(), range, 0, size, acc, None);

        let SpliceDel {
            deleted,
            overflow,
            cursor,
            post,
        } = Self::splice_delete(post, cursor, del, slab);
        let post = post.map(BooleanState::from);
        let acc = Acc::new();

        SpliceEncoder {
            encoder: Encoder::init(current, state),
            slab,
            post,
            acc,
            deleted,
            overflow,
            cursor,
        }
    }

    fn export_splice<'a, I>(data: &mut Vec<Self::Export>, range: Range<usize>, values: I)
    where
        I: Iterator<Item = Option<Cow<'a, bool>>>,
    {
        data.splice(range, values.map(|e| *e.unwrap_or_default()));
    }

    fn try_next<'a>(&mut self, slab: &'a [u8]) -> Result<Option<Run<'a, Self::Item>>, PackError> {
        if self.offset >= slab.len() {
            return Ok(None);
        }
        let data = &slab[self.offset..];
        let (bytes, count) = u64::unpack(data)?;
        let count = *count as usize;
        let value = self.value;
        self.value = !value;
        self.index += count;
        self.last_offset = self.offset;
        self.offset += bytes;
        /*
                if value {
                    self.acc += Acc::from(count); // agg(1) * count
                }
        */
        let run = Run {
            count,
            value: Some(Cow::Owned(value)),
        };
        self.acc += run.acc();
        Ok(Some(run))
    }

    fn try_again<'a>(&self, slab: &'a [u8]) -> Result<Option<Run<'a, Self::Item>>, PackError> {
        let data = &slab[self.last_offset..self.offset];
        if data.is_empty() {
            return Ok(None);
        }
        let (_bytes, count) = u64::unpack(data)?;
        let count = *count as usize;
        let value = Some(Cow::Owned(!self.value));
        Ok(Some(Run { count, value }))
    }

    fn index(&self) -> usize {
        self.index
    }

    fn offset(&self) -> usize {
        self.offset
    }

    fn init_empty(len: usize) -> Slab {
        if len > 0 {
            let mut writer = SlabWriter::<bool>::new(usize::MAX, false);
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

fn cursor_copy<'a, const B: usize>(
    data: &'a [u8],
    writer: &mut SlabWriter<'a, bool>,
    from: &BooleanCursorInternal<B>,
    to: &BooleanCursorInternal<B>,
) {
    if from.offset == to.offset {
        return;
    }
    writer.copy(
        data,
        from.offset..to.offset,
        0,
        to.index - from.index,
        to.acc - from.acc,
        None,
    );
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
        assert_eq!(col1.save(), vec![0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]);

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
        assert_eq!(col2.save(), vec![3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3]);

        // empty data
        let col5: ColumnData<BooleanCursor> = ColumnData::new();
        assert_eq!(col5.test_dump(), vec![vec![]]);
        assert_eq!(col5.save(), vec![]);
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
        col2.splice::<bool, _>(2, 2, vec![]);

        assert_eq!(
            col2.test_dump(),
            vec![vec![
                ColExport::run(2, true),
                ColExport::run(4, false),
                ColExport::run(2, true),
            ]]
        );

        let mut col3 = col1.clone();
        col3.splice(2, 2, vec![false, false]);

        assert_eq!(
            col3.test_dump(),
            vec![vec![
                ColExport::run(2, true),
                ColExport::run(6, false),
                ColExport::run(2, true),
            ]]
        );

        let mut col4 = col1.clone();
        col4.splice::<bool, _>(2, 4, vec![]);

        assert_eq!(
            col4.test_dump(),
            vec![vec![
                ColExport::run(2, true),
                ColExport::run(2, false),
                ColExport::run(2, true),
            ]]
        );

        let mut col5 = col1.clone();
        col5.splice::<bool, _>(2, 7, vec![]);

        assert_eq!(col5.test_dump(), vec![vec![ColExport::run(3, true),]]);

        let mut col6 = col1.clone();
        col6.splice::<bool, _>(0, 4, vec![]);

        assert_eq!(
            col6.test_dump(),
            vec![vec![ColExport::run(4, false), ColExport::run(2, true),]]
        );

        let mut col7 = col1.clone();
        col7.splice(0, 10, vec![false]);

        assert_eq!(col7.test_dump(), vec![vec![ColExport::run(1, false),]]);

        let mut col8 = col1.clone();
        col8.splice::<bool, _>(4, 4, vec![]);

        assert_eq!(col8.test_dump(), vec![vec![ColExport::run(6, true),]]);
    }

    #[test]
    fn load_empty_bool_data() {
        let col = BooleanCursor::load(&[]).unwrap();
        assert!(col.is_empty());
    }
}
