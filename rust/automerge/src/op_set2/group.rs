use super::{
    ColExport, ColumnCursor, Encoder, PackError, RleCursor, RleState, Run, Slab, SlabWriter,
};

const B: usize = usize::MAX;

type SubCursor = RleCursor<B, u64>;

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct GroupCursor {
    sum: u64,
    rle: SubCursor,
}

impl ColumnCursor for GroupCursor {
    type Item = u64;
    type State<'a> = RleState<'a, u64>;
    type PostState<'a> = Option<Run<'a, u64>>;
    type Export = Option<u64>;

    fn finish<'a>(
        slab: &'a Slab,
        out: &mut SlabWriter<'a>,
        state: Self::State<'a>,
        post: Self::PostState<'a>,
        cursor: Self,
    ) {
        SubCursor::finish(slab, out, state, post, cursor.rle)
    }

    fn group(&self) -> usize {
        self.sum as usize
    }

    fn flush_state<'a>(out: &mut SlabWriter<'a>, state: Self::State<'a>) {
        SubCursor::flush_state(out, state)
    }

    fn copy_between<'a>(
        slab: &'a Slab,
        out: &mut SlabWriter<'a>,
        c0: Self,
        c1: Self,
        run: Run<'a, u64>,
        size: usize,
    ) -> Self::State<'a> {
        SubCursor::copy_between(slab, out, c0.rle, c1.rle, run, size)
    }

    fn append_chunk<'a>(state: &mut Self::State<'a>, slab: &mut SlabWriter<'a>, run: Run<'a, u64>) {
        SubCursor::append_chunk(state, slab, run)
    }

    fn encode<'a>(index: usize, slab: &'a Slab) -> Encoder<'a, Self> {
        let (run, cursor) = Self::seek(index, slab.as_ref());

        let last_run_count = run.as_ref().map(|r| r.count).unwrap_or(0);

        let (state, post) = SubCursor::encode_inner(&cursor.rle, run, index, slab);

        let current = cursor.rle.start_copy(slab, last_run_count);

        Encoder {
            slab,
            current,
            post,
            state,
            cursor,
        }
    }

    fn export_item(item: Option<u64>) -> Option<u64> {
        item
    }

    fn export(data: &[u8]) -> Vec<ColExport<u64>> {
        SubCursor::export(data)
    }

    fn try_next<'a>(
        &self,
        slab: &'a [u8],
    ) -> Result<Option<(Run<'a, Self::Item>, Self)>, PackError> {
        if let Some((run, rle)) = self.rle.try_next(slab)? {
            let delta = run.count as u64 * run.value.unwrap_or(0);
            Ok(Some((
                run,
                Self {
                    sum: self.sum + delta,
                    rle,
                },
            )))
        } else {
            Ok(None)
        }
    }

    fn index(&self) -> usize {
        self.rle.index()
    }
}
