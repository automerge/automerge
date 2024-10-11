use super::cursor::{ColumnCursor, Encoder, Run, SpliceDel};
use super::pack::{PackError, Packable};
use super::rle::{RleCursor, RleState};
use super::slab::{Slab, SlabWriter};

#[cfg(test)]
use super::ColExport;

use std::ops::Range;

type SubCursor<const B: usize> = RleCursor<B, i64>;

#[derive(Debug, Default, Clone, Copy)]
pub struct DeltaCursorInternal<const B: usize> {
    abs: i64,
    rle: SubCursor<B>,
}

pub type DeltaCursor = DeltaCursorInternal<64>;

impl<'a> DeltaState<'a> {
    fn pending_delta(&self) -> i64 {
        match &self.rle {
            RleState::LoneValue(Some(n)) => *n,
            RleState::Run {
                count,
                value: Some(v),
            } => *count as i64 * *v,
            RleState::LitRun { current, run } => run.iter().sum::<i64>() + *current,
            _ => 0,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct DeltaState<'a> {
    abs: i64,
    rle: RleState<'a, i64>,
}

impl<const B: usize> ColumnCursor for DeltaCursorInternal<B> {
    type Item = i64;
    type State<'a> = DeltaState<'a>;
    type PostState<'a> = Option<Run<'a, i64>>;
    type Export = Option<i64>;

    fn empty() -> Self {
        Self::default()
    }

    fn new(slab: &Slab) -> Self {
        Self {
            abs: slab.abs(),
            rle: Default::default(),
        }
    }

    fn finish<'a>(slab: &'a Slab, out: &mut SlabWriter<'a>, cursor: Self) {
        out.set_abs(cursor.abs);
        SubCursor::<B>::finish(slab, out, cursor.rle)
    }

    fn finalize_state<'a>(
        slab: &'a Slab,
        out: &mut SlabWriter<'a>,
        mut state: Self::State<'a>,
        post: Self::PostState<'a>,
        cursor: Self,
    ) -> Option<Self> {
        match post {
            Some(run) if run.value.is_some() => {
                // we need to flush at least two elements to make sure
                // we're connected to prior and post lit runs
                Self::flush_twice(slab, out, state, run, cursor)
            }
            Some(run) => {
                // Nulls do not affect ABS - so the post does not connect us to the copy afterward
                // clear the post and try again
                Self::append_chunk(&mut state, out, run);
                Self::finalize_state(slab, out, state, None, cursor)
            }
            None => {
                if let Some((run, next)) = cursor.next(slab.as_slice()) {
                    if run.value.is_some() {
                        // we need to flush at least two elements to make sure
                        // we're connected to prior and post lit runs
                        Self::flush_twice(slab, out, state, run, next)
                    } else {
                        // Nulls do not affect ABS - so the post does not connect us to the copy afterward
                        // clear the post and try again
                        Self::append_chunk(&mut state, out, run);
                        Self::finalize_state(slab, out, state, None, next)
                    }
                } else {
                    Self::flush_state(out, state);
                    None
                }
            }
        }
    }

    fn transform(&self, run: &Run<'_, i64>) -> Option<i64> {
        if run.value.is_some() {
            //Some(self.abs - run.delta_minus_one())
            Some(self.abs - run.delta())
        } else {
            None
        }
    }

/*
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
*/

    fn flush_state<'a>(out: &mut SlabWriter<'a>, state: Self::State<'a>) {
        SubCursor::<B>::flush_state(out, state.rle)
    }

    fn copy_between<'a>(
        slab: &'a Slab,
        out: &mut SlabWriter<'a>,
        c0: Self,
        c1: Self,
        run: Run<'a, i64>,
        size: usize,
    ) -> Self::State<'a> {
        SubCursor::<B>::copy_between(slab, out, c0.rle, c1.rle, run, size);
        let mut rle = RleState::Empty;
        SubCursor::<B>::append_chunk(&mut rle, out, run);
        DeltaState { abs: c1.abs, rle }
    }

    fn append<'a>(
        state: &mut Self::State<'a>,
        slab: &mut SlabWriter<'a>,
        item: Option<i64>,
    ) -> usize {
        let value = item.map(|i| i - state.abs);
        Self::append_chunk(state, slab, Run { count: 1, value })
    }

    fn append_chunk<'a>(
        state: &mut Self::State<'a>,
        slab: &mut SlabWriter<'a>,
        run: Run<'a, i64>,
    ) -> usize {
        state.abs += run.delta();
        SubCursor::<B>::append_chunk(&mut state.rle, slab, run)
    }

    fn encode(index: usize, del: usize, slab: &Slab) -> Encoder<'_, Self> {
        // FIXME encode
        let (run, cursor) = Self::seek(index, slab);

        let (rle, post, group, mut current) =
            SubCursor::<B>::encode_inner(slab, &cursor.rle, run, index);

        let abs_delta = post.as_ref().map(|run| run.delta()).unwrap_or(0);
        let abs = cursor.abs - abs_delta;
        let state = DeltaState { abs, rle };
        let init_abs = slab.abs();
        current.set_init_abs(init_abs);
        current.set_abs(abs - state.pending_delta());

        let SpliceDel {
            deleted,
            overflow,
            cursor,
            post,
        } = Self::splice_delete(post, cursor, del, slab);

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
        //data.splice(range, values.map(|e| e.map(|i| P::own(i))));
        data.splice(range, values);
    }

    #[cfg(test)]
    fn export(data: &[u8]) -> Vec<ColExport<i64>> {
        SubCursor::<B>::export(data)
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

impl<const B: usize> DeltaCursorInternal<B> {
    fn flush_twice<'a>(
        slab: &'a Slab,
        out: &mut SlabWriter<'a>,
        mut state: DeltaState<'a>,
        run: Run<'a, i64>,
        cursor: Self,
    ) -> Option<Self> {
        if let Some(run) = run.pop() {
            Self::append(&mut state, out, Some(cursor.abs - run.delta()));
            Self::append_chunk(&mut state, out, run);
            if let Some((run, next)) = cursor.next(slab.as_slice()) {
                Self::append_chunk(&mut state, out, run);
                Self::flush_state(out, state);
                Some(next)
            } else {
                Self::flush_state(out, state);
                Some(cursor)
            }
        } else {
            Self::append(&mut state, out, Some(cursor.abs));
            if let Some((run, next)) = cursor.next(slab.as_slice()) {
                Self::append_chunk(&mut state, out, run);
                Self::flush_state(out, state);
                Some(next)
            } else {
                Self::flush_state(out, state);
                Some(cursor)
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::super::columndata::ColumnData;
    use super::super::cursor::ColExport;
    use super::*;

    #[test]
    fn column_data_delta_simple() {
        let mut col1: ColumnData<DeltaCursorInternal<5>> = ColumnData::new();
        col1.splice(0, 0, vec![1]);
        assert_eq!(col1.export()[0], vec![ColExport::litrun(vec![1])],);
        col1.splice(0, 0, vec![1]);
        assert_eq!(col1.export()[0], vec![ColExport::litrun(vec![1, 0])],);
        col1.splice(1, 0, vec![1]);
        assert_eq!(
            col1.export()[0],
            vec![ColExport::litrun(vec![1]), ColExport::run(2, 0)],
        );
        col1.splice(2, 0, vec![1]);
        assert_eq!(
            col1.export()[0],
            vec![ColExport::litrun(vec![1]), ColExport::run(3, 0)],
        );

        let mut col2: ColumnData<DeltaCursorInternal<100>> = ColumnData::new();
        col2.splice(0, 0, vec![2, 3, 1]);
        assert_eq!(col2.to_vec(), vec![Some(2), Some(3), Some(1)]);
        col2.splice(2, 0, vec![4]);
        assert_eq!(col2.to_vec(), vec![Some(2), Some(3), Some(4), Some(1)]);
    }

    #[test]
    fn column_data_delta_split_merge_semantics() {
        // lit run spanning multiple slabs
        let mut col1: ColumnData<DeltaCursorInternal<5>> = ColumnData::new();
        let mut col1a: ColumnData<DeltaCursorInternal<{ usize::MAX }>> = ColumnData::new();
        let col1_data = vec![1, 10, 2, 11, 4, 27, 19, 3, 21, 14, 2, 8];
        col1.splice(0, 0, col1_data.clone());
        col1a.splice(0, 0, col1_data.clone());
        assert_eq!(
            col1.export(),
            vec![
                vec![ColExport::litrun(vec![1, 9, -8, 9])],
                vec![ColExport::litrun(vec![-7, 23, -8, -16])],
                vec![ColExport::litrun(vec![18, -7, -12, 6])],
            ]
        );
        let mut out = Vec::new();
        let mut outa = Vec::new();
        col1.write(&mut out);
        col1a.write(&mut outa);
        assert_eq!(out, outa);
        for i in 0..col1.len() {
            assert_eq!(col1.get(i), col1a.get(i));
        }

        // lit run capped by runs
        let mut col2: ColumnData<DeltaCursorInternal<5>> = ColumnData::new();
        col2.splice(0, 0, vec![1, 2, 10, 11, 4, 27, 19, 3, 21, 14, 15, 16]);
        assert_eq!(
            col2.export(),
            vec![
                vec![ColExport::run(2, 1), ColExport::litrun(vec![8, 1])],
                vec![ColExport::litrun(vec![-7, 23, -8, -16])],
                vec![ColExport::litrun(vec![18, -7]), ColExport::run(2, 1)],
            ]
        );
        let mut out = Vec::new();
        col2.write(&mut out);

        assert_eq!(out, vec![2, 1, 120, 8, 1, 121, 23, 120, 112, 18, 121, 2, 1]);

        // lit run capped by runs
        let mut col3: ColumnData<DeltaCursorInternal<5>> = ColumnData::new();
        col3.splice(0, 0, vec![1, 10, 5, 6, 7, 9, 11, 20, 25, 19, 10, 9, 19, 29]);
        assert_eq!(
            col3.export(),
            vec![
                vec![ColExport::litrun(vec![1, 9, -5]), ColExport::run(2, 1),],
                vec![ColExport::run(2, 2), ColExport::litrun(vec![9, 5]),],
                vec![ColExport::litrun(vec![-6, -9, -1]), ColExport::run(2, 10)],
            ]
        );
        let mut out = Vec::new();
        col3.write(&mut out);
        assert_eq!(
            out,
            vec![125, 1, 9, 123, 2, 1, 2, 2, 123, 9, 5, 122, 119, 127, 2, 10]
        );

        // lit run capped by runs
        let mut col4: ColumnData<DeltaCursorInternal<5>> = ColumnData::new();
        col4.splice(
            0,
            0,
            vec![
                1, 2, 4, 6, 9, 12, 16, 20, 25, 30, 36, 42, 49, 56, 64, 72, 81, 90,
            ],
        );
        assert_eq!(
            col4.export(),
            vec![
                vec![
                    ColExport::run(2, 1),
                    ColExport::run(2, 2),
                    ColExport::run(2, 3),
                ],
                vec![
                    ColExport::run(2, 4),
                    ColExport::run(2, 5),
                    ColExport::run(2, 6),
                ],
                vec![
                    ColExport::run(2, 7),
                    ColExport::run(2, 8),
                    ColExport::run(2, 9),
                ],
            ]
        );
        let mut out = Vec::new();
        col4.write(&mut out);
        assert_eq!(
            out,
            vec![2, 1, 2, 2, 2, 3, 2, 4, 2, 5, 2, 6, 2, 7, 2, 8, 2, 9]
        );

        // empty data
        let col5: ColumnData<DeltaCursorInternal<5>> = ColumnData::new();
        assert_eq!(col5.export(), vec![vec![]]);
        let mut out = Vec::new();
        col5.write(&mut out);
        assert_eq!(out, Vec::<u8>::new());
    }

    #[test]
    fn column_data_delta_splice_delete() {
        // lit run spanning multiple slabs
        let mut col1: ColumnData<DeltaCursor> = ColumnData::new();
        col1.splice(0, 0, vec![1, 2, 3, 10, 2, 11, 8, 8, 8]);
        assert_eq!(
            col1.to_vec(),
            vec![
                Some(1),
                Some(2),
                Some(3),
                Some(10),
                Some(2),
                Some(11),
                Some(8),
                Some(8),
                Some(8)
            ],
        );
        col1.splice::<i64>(2, 1, vec![]);
        assert_eq!(
            col1.to_vec(),
            vec![
                Some(1),
                Some(2),
                Some(10),
                Some(2),
                Some(11),
                Some(8),
                Some(8),
                Some(8)
            ],
        );

        col1.splice::<i64>(4, 3, vec![]);

        assert_eq!(
            col1.to_vec(),
            vec![Some(1), Some(2), Some(10), Some(2), Some(8)],
        );

        assert_eq!(
            col1.to_vec(),
            vec![Some(1), Some(2), Some(10), Some(2), Some(8)],
        );
    }

    #[test]
    fn delta_cross_boundary() {
        let mut col: ColumnData<DeltaCursorInternal<5>> = ColumnData::new();
        let mut data = vec![Some(1), Some(2), Some(3), Some(4), Some(10), Some(20)];
        col.splice(0, 0, data.clone());

        let patch = vec![Some(32), Some(16), Some(100), Some(99), Some(204)];

        col.splice(3, 0, patch.clone());
        data.splice(3..3, patch);

        assert_eq!(data, col.to_vec());
    }

    #[test]
    fn delta_flush_twice() {
        let mut col: ColumnData<DeltaCursor> = ColumnData::new();
        let mut data = vec![
            None,
            Some(0),
            Some(2),
            Some(3),
            Some(4),
            Some(4),
            Some(5),
            Some(6),
            Some(7),
            Some(7),
            Some(8),
            Some(9),
        ];
        col.splice(0, 0, data.clone());

        let patch = vec![Some(6)];

        col.splice(7, 0, patch.clone());
        data.splice(7..7, patch);

        assert_eq!(data, col.to_vec());

        assert_eq!(
            col.export(),
            vec![vec![
                ColExport::Null(1),
                ColExport::litrun(vec![0, 2]),
                ColExport::run(2, 1),
                ColExport::litrun(vec![0]),
                ColExport::run(2, 1),
                // if you dont flush twice this lit run gets broken in two
                ColExport::litrun(vec![0, 1, 0]),
                ColExport::run(2, 1),
            ],]
        );
    }
}
