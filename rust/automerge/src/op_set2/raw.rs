use super::{ColExport, ColumnCursor, Encoder, PackError, Run, Slab, SlabWriter};

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct RawCursorInternal<const B: usize> {
    offset: usize,
}

pub(crate) type RawCursor = RawCursorInternal<2048>;

impl<const B: usize> ColumnCursor for RawCursorInternal<B> {
    type Item = [u8];
    type State<'a> = ();
    type PostState<'a> = &'a [u8];
    type Export = Vec<u8>;

    fn write<'a>(writer: &mut SlabWriter<'a>, slab: &'a Slab, state: ()) -> () {}

    fn finish<'a>(
        slab: &'a Slab,
        out: &mut SlabWriter<'a>,
        state: (),
        post: Self::PostState<'a>,
        cursor: Self,
    ) {
        out.flush_bytes(post, post.len())
    }

    fn flush_state<'a>(out: &mut SlabWriter<'a>, state: Self::State<'a>) {}

    fn copy_between<'a>(
        _slab: &'a Slab,
        _out: &mut SlabWriter<'a>,
        _c0: Self,
        _c1: Self,
        _run: Run<'a, [u8]>,
        _size: usize,
    ) -> Self::State<'a> {
        // only called from write and we override that
    }

    fn append_chunk<'a>(
        state: &mut Self::State<'a>,
        slab: &mut SlabWriter<'a>,
        run: Run<'a, [u8]>,
    ) -> usize {
        let mut len = 0;
        for _ in 0..run.count {
            if let Some(i) = run.value {
                len += i.len();
                slab.flush_bytes(i, i.len());
            }
        }
        len
    }

    fn encode<'a>(index: usize, mut del: usize, slab: &'a Slab) -> Encoder<'a, Self> {
        let state = ();
        let cursor = Self { offset: index };

        // everything before...
        let mut current = SlabWriter::new(B);
        current.flush_bytes(&slab.as_ref()[0..index], index);

        let post;
        let deleted;
        if index + del < slab.as_ref().len() {
            // everything after
            post = &slab.as_ref()[(index + del)..];
            deleted = del;
        } else {
            // nothing left
            post = &[];
            deleted = slab.as_ref().len() - index;
        }
        let overflow = del - deleted;

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

    fn export_item(item: Option<&[u8]>) -> Vec<u8> {
        item.unwrap_or(&[]).to_vec()
    }

    fn export(data: &[u8]) -> Vec<ColExport<[u8]>> {
        vec![ColExport::Raw(data.to_vec())]
    }

    fn scan(data: &[u8]) -> Result<Self, PackError> {
        Ok(Self { offset: data.len() })
    }

    // dont think this has any real use
    // this column will always use get_slice
    fn try_next<'a>(
        &self,
        slab: &'a [u8],
    ) -> Result<Option<(Run<'a, Self::Item>, Self)>, PackError> {
        let next_offset = self.offset + 1;
        if next_offset > slab.len() {
            return Err(PackError::IndexOutOfRange(self.offset));
        }
        let data = &slab[self.offset..next_offset];
        Ok(Some((
            Run {
                count: 1,
                value: Some(data),
            },
            Self {
                offset: next_offset,
            },
        )))
    }

    fn index(&self) -> usize {
        self.offset
    }
}

impl<const B: usize> RawCursorInternal<B> {
    fn get_slice(slab: &Slab, offset: usize, len: usize) -> Result<&[u8], PackError> {
        let end = offset + len;
        if offset > slab.len() || end > slab.len() {
            return Err(PackError::SliceOutOfRange(offset, end));
        }
        Ok(&slab.as_ref()[offset..end])
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::super::columns::{ColExport, ColumnData};
    use super::*;

    #[test]
    fn column_data_raw_splice() {
        let mut col1: ColumnData<RawCursorInternal<6>> = ColumnData::new();
        col1.splice(0, 0, vec![vec![1, 1, 1]]);
        assert_eq!(col1.export(), vec![vec![ColExport::Raw(vec![1, 1, 1])]]);
        col1.splice(0, 0, vec![vec![2, 2, 2]]);
        assert_eq!(
            col1.export(),
            vec![vec![ColExport::Raw(vec![2, 2, 2, 1, 1, 1])]]
        );
        col1.splice(3, 0, vec![vec![3, 3, 3]]);
        assert_eq!(
            col1.export(),
            vec![
                vec![ColExport::Raw(vec![2, 2, 2, 3, 3, 3])],
                vec![ColExport::Raw(vec![1, 1, 1])],
            ]
        );
        col1.splice(3, 0, vec![vec![4, 4, 4]]);
        assert_eq!(
            col1.export(),
            vec![
                vec![ColExport::Raw(vec![2, 2, 2, 4, 4, 4])],
                vec![ColExport::Raw(vec![3, 3, 3])],
                vec![ColExport::Raw(vec![1, 1, 1])],
            ]
        );
        col1.splice::<Vec<u8>>(3, 1, vec![]);
        assert_eq!(
            col1.export(),
            vec![
                vec![ColExport::Raw(vec![2, 2, 2, 4, 4])],
                vec![ColExport::Raw(vec![3, 3, 3])],
                vec![ColExport::Raw(vec![1, 1, 1])],
            ]
        );
        col1.splice(3, 2, vec![vec![5, 5, 5, 5, 5, 5], vec![6, 6, 6]]);
        assert_eq!(
            col1.export(),
            vec![
                vec![ColExport::Raw(vec![2, 2, 2, 5, 5, 5, 5, 5, 5])],
                vec![ColExport::Raw(vec![6, 6, 6])],
                vec![ColExport::Raw(vec![3, 3, 3])],
                vec![ColExport::Raw(vec![1, 1, 1])],
            ]
        );
    }
}
