use super::{
    ColExport, ColumnCursor, Encoder, PackError, Packable, Run, Slab, SlabWriter, SpliceDel,
};

#[derive(Debug, PartialEq, Default, Clone)]
pub(crate) struct BooleanState {
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
pub(crate) struct BooleanCursorInternal<const B: usize> {
    value: bool,
    index: usize,
    offset: usize,
    last_offset: usize,
}

pub(crate) type BooleanCursor = BooleanCursorInternal<1024>;

impl<const B: usize> ColumnCursor for BooleanCursorInternal<B> {
    type Item = bool;
    type State<'a> = BooleanState;
    type PostState<'a> = Option<BooleanState>;
    type Export = bool;

    fn finish<'a>(
        slab: &'a Slab,
        out: &mut SlabWriter<'a>,
        mut state: Self::State<'a>,
        post: Self::PostState<'a>,
        cursor: Self,
    ) {
        if let Some(post) = post {
            if post.value == state.value {
                state.count += post.count;
                Self::finish(slab, out, state, None, cursor);
            } else {
                out.flush_bool_run(state.count);
                out.flush_bool_run(post.count);
                out.flush_after(slab, cursor.offset, 0, slab.len() - cursor.index, 0);
            }
        } else {
            if let Ok(Some((val, next_cursor))) = cursor.try_next(slab.as_ref()) {
                if val.value == Some(state.value) {
                    out.flush_bool_run(state.count + val.count);
                    out.flush_after(
                        slab,
                        next_cursor.offset,
                        0,
                        slab.len() - next_cursor.index,
                        0,
                    );
                } else {
                    out.flush_bool_run(state.count);
                    out.flush_after(slab, cursor.offset, 0, slab.len() - cursor.index, 0);
                }
            } else {
                out.flush_bool_run(state.count);
            }
        }
    }

    fn write_finish<'a>(out: &mut Vec<u8>, mut writer: SlabWriter<'a>, state: Self::State<'a>) {
        // write nothing if its all false
        if !(state.value == false && writer.len() == 0) {
            Self::flush_state(&mut writer, state);
        }
        writer.write(out);
    }

    fn copy_between<'a>(
        slab: &'a Slab,
        out: &mut SlabWriter<'a>,
        c0: Self,
        c1: Self,
        run: Run<'a, bool>,
        size: usize,
    ) -> Self::State<'a> {
        out.flush_before2(slab, c0.offset..c1.last_offset, 0, size);
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

    fn encode<'a>(index: usize, del: usize, slab: &'a Slab) -> Encoder<'a, Self> {
        // FIXME encode
        let (run, cursor) = Self::seek(index, slab.as_ref());

        let count = run.map(|r| r.count).unwrap_or(0);
        let value = run.map(|r| r.value.unwrap_or(false)).unwrap_or(false);

        let mut state = BooleanState { count, value };
        let mut state2 = BooleanState::from(run.unwrap_or_default());
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
        current.flush_before(slab, range, 0, size);

        let SpliceDel {
            deleted,
            overflow,
            cursor,
            post,
        } = Self::splice_delete(post, cursor, del, slab);
        let post = post.map(|r| BooleanState::from(r));

        Encoder {
            slab,
            current,
            post,
            state,
            deleted,
            overflow,
            cursor,
        }
    }

    fn export_item(item: Option<bool>) -> bool {
        item.unwrap_or(false)
    }

    fn export(data: &[u8]) -> Vec<ColExport<bool>> {
        let mut result = vec![];
        let mut cursor = Self::default();
        while let Ok(Some((Run { count, value }, next_cursor))) = cursor.try_next(data) {
            cursor = next_cursor;
            if count > 0 {
                result.push(ColExport::Run(count, value.unwrap()))
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
}

/*
struct SpliceDel<'a, C: ColumnCursor> {
  deleted: usize,
  overflow: usize,
  cursor: C,
  post: Option<Run<'a, C::Item>>
}

fn splice_delete<'a, C: ColumnCursor>(_post: Option<Run<'a, C::Item>>, _cursor: C, _del: usize, slab: &'a Slab) -> SpliceDel<'a, C> {
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
          post = Some(Run { count: count - del, value });
          del = 0;
        },
        // if del is greather than or equal the current run
        Some(Run { count, .. }) => {
          del -= count;
          deleted += count;
          post = None;
        },
        None => {
          if let Some((p, c)) = C::next(&cursor, slab.as_ref()) {
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
    assert!(_del == deleted + overflow );
    SpliceDel { deleted, overflow, cursor, post }
}
*/

#[cfg(test)]
pub(crate) mod tests {
    use super::super::columns::{ColExport, ColumnData};
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
        let mut col5: ColumnData<BooleanCursor> = ColumnData::new();
        assert_eq!(col5.export(), vec![vec![]]);
        let mut out = Vec::new();
        col5.write(&mut out);
        assert_eq!(out, Vec::<u8>::new());
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
