use super::{ColExport, ColumnCursor, Encoder, PackError, Packable, Run, Slab, WritableSlab};

#[derive(Debug, Default, Clone)]
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
pub(crate) struct BooleanCursor {
    value: bool,
    index: usize,
    offset: usize,
    last_offset: usize,
}

impl ColumnCursor for BooleanCursor {
    type Item = bool;
    type State<'a> = BooleanState;
    type PostState<'a> = Option<BooleanState>;
    type Export = bool;

    fn finish<'a>(
        slab: &'a Slab,
        out: &mut WritableSlab,
        mut state: Self::State<'a>,
        post: Self::PostState<'a>,
        cursor: Self,
    ) {
        if let Some(post) = post {
            if post.value == state.value {
                state.count += post.count;
                Self::finish(slab, out, state, None, cursor);
            } else {
                out.append_usize(state.count);
                out.add_len(state.count);
                out.append_usize(post.count);
                out.add_len(post.count);
                out.append_bytes(&slab.as_ref()[cursor.offset..]);
                out.add_len(slab.len() - cursor.index);
            }
        } else {
            if let Ok(Some((val, next_cursor))) = cursor.try_next(slab.as_ref()) {
                if val.value == Some(state.value) {
                    out.append_usize(state.count + val.count);
                    out.add_len(state.count + val.count);
                    out.append_bytes(&slab.as_ref()[next_cursor.offset..]);
                    out.add_len(slab.len() - next_cursor.index);
                } else {
                    out.append_usize(state.count);
                    out.add_len(state.count);
                    out.append_bytes(&slab.as_ref()[cursor.offset..]);
                    out.add_len(slab.len() - cursor.index);
                }
            } else {
                out.append_usize(state.count);
                out.add_len(state.count);
            }
        }
    }

    fn append<'a>(state: &mut Self::State<'a>, slab: &mut WritableSlab, item: Option<bool>) {
        let item = item.unwrap_or_default();
        if state.value == item {
            state.count += 1;
        } else {
            slab.append_usize(state.count);
            slab.add_len(state.count);
            state.value = item;
            state.count = 1;
        }
    }

    fn encode<'a>(index: usize, slab: &'a Slab) -> Encoder<'a, Self> {
        let (run, cursor) = Self::seek(index, slab.as_ref());

        let count = run.map(|r| r.count).unwrap_or(0);
        let value = run.map(|r| r.value.unwrap_or(false)).unwrap_or(false);

        let mut state = BooleanState { count, value };
        let mut post = None;

        let delta = cursor.index - index;
        if delta > 0 {
            state.count -= delta;
            post = Some(BooleanState {
                count: delta,
                value,
            });
        }

        let range = 0..cursor.last_offset;
        let size = cursor.index - count;
        let current = WritableSlab::new(&slab.as_ref()[range], size);

        Encoder {
            slab,
            results: vec![],
            current,
            post,
            state,
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
