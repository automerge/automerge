use super::{
    ColExport, ColumnCursor, Encoder, PackError, RleCursor, RleState, Run, Slab, WritableSlab,
};

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct GroupCursor {
    sum: u64,
    rle: RleCursor<u64>,
}

impl ColumnCursor for GroupCursor {
    type Item = u64;
    type State<'a> = RleState<'a, u64>;
    type PostState<'a> = Option<Run<'a, u64>>;
    type Export = Option<u64>;

    fn finish<'a>(
        slab: &'a Slab,
        out: &mut WritableSlab,
        state: Self::State<'a>,
        post: Self::PostState<'a>,
        cursor: Self,
    ) {
        RleCursor::finish(slab, out, state, post, cursor.rle)
    }

    fn append<'a>(state: &mut Self::State<'a>, slab: &mut WritableSlab, item: Option<u64>) {
        RleCursor::append(state, slab, item)
    }

    fn encode<'a>(index: usize, slab: &'a Slab) -> Encoder<'a, Self> {
        let (run, cursor) = Self::seek(index, slab.as_ref());

        let last_run_count = run.as_ref().map(|r| r.count).unwrap_or(0);

        let (state, post) = RleCursor::encode_inner(&cursor.rle, run, index, slab);

        let current = cursor.rle.start_copy(slab, last_run_count);

        Encoder {
            slab,
            results: vec![],
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
        RleCursor::<u64>::export(data)
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
