use super::{
    ColExport, ColumnCursor, Encoder, PackError, Packable, RleCursor, RleState, Run, Slab,
    WritableSlab,
};

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct DeltaCursor {
    abs: i64,
    rle: RleCursor<i64>,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct DeltaState<'a> {
    abs: i64,
    rle: RleState<'a, i64>,
}

impl<'a> DeltaState<'a> {
    fn new(abs: i64) -> Self {
        DeltaState {
            abs,
            rle: RleState::Empty,
        }
    }
}

impl ColumnCursor for DeltaCursor {
    type Item = i64;
    type State<'a> = DeltaState<'a>;
    type PostState<'a> = Option<Run<'a, i64>>;
    type Export = Option<i64>;

    fn finish<'a>(
        slab: &'a Slab,
        out: &mut WritableSlab,
        mut state: Self::State<'a>,
        post: Self::PostState<'a>,
        cursor: Self,
    ) {
        match post {
            Some(Run {
                count: 1,
                value: Some(_),
            }) => {
                //let delta = cursor.abs - state.abs;
                Self::append(&mut state, out, Some(cursor.abs));
                RleCursor::finish(slab, out, RleState::Empty, None, cursor.rle);
            }
            Some(Run {
                count,
                value: Some(v),
            }) => {
                Self::append(&mut state, out, Some(cursor.abs - (count as i64 - 1) * v));
                let next_post = Some(Run::new(count - 1, Some(v)));
                RleCursor::finish(slab, out, state.rle, next_post, cursor.rle);
            }
            Some(Run { count, value: None }) => {
                let next_state = DeltaState::new(state.abs);
                RleCursor::flush_state(out, state.rle);
                RleCursor::<i64>::flush_run(out, count, None);
                Self::finish(slab, out, next_state, None, cursor);
            }
            None => {
                if let Some((run, next_cursor)) = cursor.next(slab.as_ref()) {
                    match run {
                        Run { count, value: None } => {
                            let next_state = DeltaState::new(state.abs);
                            RleCursor::flush_state(out, state.rle);
                            RleCursor::<i64>::flush_run(out, count, None);
                            Self::finish(slab, out, next_state, None, next_cursor);
                        }
                        Run {
                            count: 1,
                            value: Some(_),
                        } => {
                            Self::append(&mut state, out, Some(next_cursor.abs));
                            RleCursor::finish(slab, out, state.rle, None, next_cursor.rle);
                        }
                        run => {
                            let run = Run::new(run.count - 1, run.value);
                            Self::append(&mut state, out, Some(next_cursor.abs - run.delta()));
                            RleCursor::finish(slab, out, state.rle, Some(run), next_cursor.rle);
                        }
                    }
                } else {
                    RleCursor::flush_state(out, state.rle);
                }
            }
        }
    }

    fn pop<'a>(
        &self,
        mut run: Run<'a, Self::Item>,
    ) -> (
        Option<<Self::Item as Packable>::Unpacked<'a>>,
        Option<Run<'a, Self::Item>>,
    ) {
        run.count -= 1;
        let value = run.value.map(|_| self.abs - run.delta());
        if run.count > 0 {
            (value, Some(run))
        } else {
            (value, None)
        }
    }

    fn append<'a>(state: &mut Self::State<'a>, slab: &mut WritableSlab, item: Option<i64>) {
        if let Some(item) = item {
            let delta = item - state.abs;
            state.abs = item;
            RleCursor::append(&mut state.rle, slab, Some(delta));
        } else {
            RleCursor::append(&mut state.rle, slab, None);
        }
    }

    fn encode<'a>(index: usize, slab: &'a Slab) -> Encoder<'a, Self> {
        let (run, cursor) = Self::seek(index, slab.as_ref());

        let last_run_count = run.as_ref().map(|r| r.count).unwrap_or(0);

        let (rle, post) = RleCursor::encode_inner(&cursor.rle, run, index, slab);

        let abs_delta = post.as_ref().map(|run| run.delta()).unwrap_or(0);
        let abs = cursor.abs - abs_delta;
        let state = DeltaState { abs, rle };

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

    fn export_item(item: Option<i64>) -> Option<i64> {
        item
    }

    fn export(data: &[u8]) -> Vec<ColExport<i64>> {
        RleCursor::<i64>::export(data)
    }

    fn try_next<'a>(
        &self,
        slab: &'a [u8],
    ) -> Result<Option<(Run<'a, Self::Item>, Self)>, PackError> {
        if let Some((run, rle)) = self.rle.try_next(slab)? {
            let delta = run.delta();
            let abs = self.abs.saturating_add(delta);
            Ok(Some((run, Self { abs, rle })))
        } else {
            Ok(None)
        }
    }

    fn index(&self) -> usize {
        self.rle.index()
    }
}
