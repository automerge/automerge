use super::{ColExport, ColumnCursor, Encoder, PackError, Run, Slab, SlabWriter};

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct RawCursor {
    offset: usize,
}

impl ColumnCursor for RawCursor {
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

    fn encode<'a>(index: usize, slab: &'a Slab) -> Encoder<'a, Self> {
        let state = ();
        let cursor = Self { offset: index };

        // everything before...
        let mut current = SlabWriter::new(usize::MAX);
        current.flush_bytes(&slab.as_ref()[0..index], index);

        // everything after
        let post = &slab.as_ref()[index..];

        Encoder {
            slab,
            current,
            post,
            state,
            cursor,
        }
    }

    fn export_item(item: Option<&[u8]>) -> Vec<u8> {
        item.unwrap_or(&[]).to_vec()
    }

    fn export(data: &[u8]) -> Vec<ColExport<[u8]>> {
        vec![ColExport::Run(1, data.to_vec())]
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

impl RawCursor {
    fn get_slice(slab: &Slab, offset: usize, len: usize) -> Result<&[u8], PackError> {
        let end = offset + len;
        if offset > slab.len() || end > slab.len() {
            return Err(PackError::SliceOutOfRange(offset, end));
        }
        Ok(&slab.as_ref()[offset..end])
    }
}
