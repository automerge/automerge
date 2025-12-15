use super::aggregate::Agg;
use super::columndata::ColumnData;
use super::cursor::{ColumnCursor, Run, ScanMeta, SpliceDel};
use super::encoder::{Encoder, SpliceEncoder};
use super::pack::{PackError, Packable};
use super::rle::{RleCursor, RleState};
use super::slab::{Slab, SlabWeight, SlabWriter};
use super::Cow;

use std::ops::Range;

pub(crate) type SubCursor<const B: usize> = RleCursor<B, i64>;

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct DeltaCursorInternal<const B: usize> {
    abs: i64,
    min: Agg,
    max: Agg,
    rle: SubCursor<B>,
}

pub type DeltaCursor = DeltaCursorInternal<64>;

impl DeltaState<'_> {
    fn pending_delta(&self) -> i64 {
        match &self.rle {
            RleState::LoneValue(Some(n)) => **n,
            RleState::Run {
                count,
                value: Some(v),
            } => *count as i64 * **v,
            RleState::LitRun { current, run } => run.iter().map(|a| **a).sum::<i64>() + **current,
            _ => 0,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct DeltaState<'a> {
    pub(crate) abs: i64,
    pub(crate) rle: RleState<'a, i64>,
}

impl<const B: usize> ColumnCursor for DeltaCursorInternal<B> {
    type Item = i64;
    type State<'a> = DeltaState<'a>;
    type PostState<'a> = Option<Run<'a, i64>>;
    type Export = Option<i64>;
    type SlabIndex = SlabWeight;

    fn empty() -> Self {
        Self::default()
    }

    fn new(slab: &Slab) -> Self {
        let abs = slab.abs();
        Self {
            abs,
            min: Agg::default(),
            max: Agg::default(),
            rle: Default::default(),
        }
    }

    fn finish<'a>(slab: &'a Slab, writer: &mut SlabWriter<'a, i64>, cursor: Self) {
        writer.set_abs(cursor.abs);
        SubCursor::<B>::finish(slab, writer, cursor.rle)
    }

    fn finalize_state<'a>(
        slab: &'a Slab,
        encoder: &mut Encoder<'a, Self>,
        post: Self::PostState<'a>,
        mut cursor: Self,
    ) -> Option<Self> {
        match post {
            Some(run) if run.value.is_some() => {
                // we need to flush at least two elements to make sure
                // we're connected to prior and post lit runs
                Self::flush_twice(slab, encoder, run, cursor)
            }
            Some(run) => {
                // Nulls do not affect ABS - so the post does not connect us to the copy afterward
                // clear the post and try again
                encoder.append_chunk(run);
                Self::finalize_state(slab, encoder, None, cursor)
            }
            None => {
                if let Some(run) = cursor.next(slab.as_slice()) {
                    if run.value.is_some() {
                        // we need to flush at least two elements to make sure
                        // we're connected to prior and post lit runs
                        Self::flush_twice(slab, encoder, run, cursor)
                    } else {
                        // Nulls do not affect ABS - so the post does not connect us to the copy afterward
                        // clear the post and try again
                        encoder.append_chunk(run);
                        Self::finalize_state(slab, encoder, None, cursor)
                    }
                } else {
                    encoder.flush();
                    None
                }
            }
        }
    }

    fn contains_range(&self, run: &Run<'_, i64>, target: &Range<usize>) -> Option<Range<usize>> {
        let step = run.value.as_deref().cloned()?;
        let count = run.count as i64;
        let valid = (target.start as i64)..(target.end as i64);
        let start = self.abs - step * (count - 1);

        let result = _contains_range(start, step, count, valid);
        if result.is_empty() {
            None
        } else {
            Some((result.start.max(0) as usize)..(result.end.min(count) as usize))
        }
    }

    fn contains_agg(&self, run: &Run<'_, i64>, target: Agg) -> Option<Range<usize>> {
        if target.is_none() && run.value.is_none() {
            return Some(0..run.count);
        }

        let target = target.as_i64()?;
        let value = run.value.as_deref().cloned()?;
        let icount = run.count as i64;
        let abs = self.abs;

        let sign = (value > 0) as i64 * 2 - 1; // +1 or -1
        let delta = sign * (abs - target);
        let value = value * sign;

        if value != 0 && delta % value == 0 && delta >= 0 && delta / value < icount {
            let n = (icount - delta / value - 1) as usize;
            Some(n..n + 1)
        } else if value == 0 && target == abs {
            Some(0..run.count)
        } else {
            None
        }
    }

    fn pop(&self, run: &mut Run<'_, i64>) -> Option<Option<Cow<'static, i64>>> {
        if run.next()?.is_some() {
            Some(Some(Cow::Owned(self.abs - run.delta())))
        } else {
            Some(None)
        }
    }

    fn pop_n<'a>(
        &self,
        run: &mut Run<'a, Self::Item>,
        n: usize,
    ) -> Option<Option<Cow<'a, Self::Item>>> {
        assert!(n > 0);
        if run.nth(n - 1)?.is_some() {
            Some(Some(Cow::Owned(self.abs - run.delta())))
        } else {
            Some(None)
        }
    }

    fn copy_between<'a>(
        slab: &'a [u8],
        writer: &mut SlabWriter<'a, i64>,
        c0: Self,
        c1: Self,
        run: Run<'a, i64>,
        size: usize,
    ) -> Self::State<'a> {
        let rle = SubCursor::<B>::copy_between(slab, writer, c0.rle, c1.rle, run, size);
        DeltaState { abs: c1.abs, rle }
    }

    fn slab_size() -> usize {
        B
    }

    fn splice_encoder(index: usize, del: usize, slab: &Slab) -> SpliceEncoder<'_, Self> {
        // FIXME encode
        let (run, cursor) = Self::seek(index, slab);

        let (rle, post, acc, mut current) =
            SubCursor::<B>::encode_inner(slab.as_slice(), &cursor.rle, run, index, true);

        let abs_delta = post.as_ref().map(|run| run.delta()).unwrap_or(0);
        let abs = cursor.abs - abs_delta;
        let state = DeltaState { abs, rle };
        let init_abs = slab.abs();
        current.set_init_abs(init_abs);
        current.set_abs(abs - state.pending_delta());
        current.unlock();

        let SpliceDel {
            deleted,
            overflow,
            cursor,
            post,
        } = Self::splice_delete(post, cursor, del, slab);

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
        I: Iterator<Item = Option<Cow<'a, Self::Item>>>,
    {
        data.splice(range, values.map(|i| i.as_deref().cloned()));
    }

    fn try_next<'a>(&mut self, slab: &'a [u8]) -> Result<Option<Run<'a, Self::Item>>, PackError> {
        if let Some(run) = self.rle.try_next(slab)? {
            let delta = run.delta();
            let abs = self.abs.saturating_add(delta);
            let first_step = self
                .abs
                .saturating_add(run.value.as_deref().cloned().unwrap_or(0));
            let min = std::cmp::min(abs, first_step);
            let max = std::cmp::max(abs, first_step);
            let min = self.min.minimize(Agg::from(min));
            let max = self.max.maximize(Agg::from(max));
            self.abs = abs;
            self.min = min;
            self.max = max;
            Ok(Some(run))
        } else {
            Ok(None)
        }
    }

    fn try_again<'a>(&self, slab: &'a [u8]) -> Result<Option<Run<'a, Self::Item>>, PackError> {
        if let Some(run) = self.rle.try_again(slab)? {
            Ok(Some(run))
        } else {
            Ok(None)
        }
    }

    // FIXME - this only saves min/max >= 1
    // you will get strange results in searches if looking for zero or negative
    fn compute_min_max(slabs: &mut [Slab]) {
        for s in slabs {
            let (_run, c) = Self::seek(s.len(), s);
            let _next = c.clone().next(s.as_slice());
            assert!(_run.is_some());
            assert!(_next.is_none());
            s.set_min_max(c.min(), c.max());
        }
    }

    fn min(&self) -> Agg {
        self.min
    }
    fn max(&self) -> Agg {
        self.max
    }

    fn index(&self) -> usize {
        self.rle.index()
    }

    fn offset(&self) -> usize {
        self.rle.offset()
    }

    fn load_with(data: &[u8], m: &ScanMeta) -> Result<ColumnData<Self>, PackError> {
        let mut cursor = Self::empty();
        let mut writer = SlabWriter::<i64>::new(B, true);
        let mut last_copy = Self::empty();
        while let Some(run) = cursor.try_next(data)? {
            i64::validate(run.value.as_deref(), m)?;
            if cursor.rle.offset - last_copy.rle.offset >= B {
                SubCursor::load_copy(data, &mut writer, &last_copy.rle, &cursor.rle);
                writer.set_abs(cursor.abs);
                writer.manual_slab_break(); // have to do this before not after
                last_copy = cursor;
            }
        }
        SubCursor::load_copy(data, &mut writer, &last_copy.rle, &cursor.rle);
        Ok(writer.into_column(cursor.rle.index))
    }
}

impl<const B: usize> DeltaCursorInternal<B> {
    fn flush_twice<'a>(
        slab: &'a Slab,
        encoder: &mut Encoder<'a, Self>,
        run: Run<'a, i64>,
        mut cursor: Self,
    ) -> Option<Self> {
        if let Some(run) = run.pop() {
            encoder.append_item(Some(Cow::Owned(cursor.abs - run.delta())));
            encoder.append_chunk(run);
            if let Some(run) = cursor.next(slab.as_slice()) {
                encoder.append_chunk(run);
                encoder.flush();
                Some(cursor)
            } else {
                encoder.flush();
                Some(cursor)
            }
        } else {
            encoder.append_item(Some(Cow::Owned(cursor.abs)));
            if let Some(run) = cursor.next(slab.as_slice()) {
                encoder.append_chunk(run);
                encoder.flush();
                Some(cursor)
            } else {
                encoder.flush();
                Some(cursor)
            }
        }
    }
}

fn _contains_range(start: i64, step: i64, count: i64, valid: Range<i64>) -> Range<i64> {
    if count == 0 || valid.is_empty() {
        return 0..0;
    }

    if step < 0 {
        let neg_valid = -(valid.end - 1)..-(valid.start - 1);
        return _contains_range(-start, -step, count, neg_valid);
    }

    let end = start + step * (count - 1);

    if valid.contains(&start) && valid.contains(&end) {
        0..count
    } else if valid.end <= start || valid.start > end || step == 0 {
        0..0
    } else {
        let a = (valid.start - start + (step - 1)) / step;
        let b = (valid.end - start - 1) / step + 1;
        if a >= b {
            0..0
        } else {
            a.max(0)..b.min(count)
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::super::columndata::ColumnData;
    use super::super::test::ColExport;
    use super::*;

    #[test]
    fn test_contains_range() {
        assert_eq!(0..3, _contains_range(10, 5, 3, 0..1000));
        assert_eq!(0..1, _contains_range(10, 5, 3, 10..11));
        assert_eq!(0..2, _contains_range(10, 5, 3, 10..16));
        assert_eq!(0..3, _contains_range(10, 5, 3, 10..21));
        assert_eq!(0..2, _contains_range(10, 5, 3, 10..20));

        assert_eq!(0..3, _contains_range(10, -5, 3, 0..1000));
        assert_eq!(0..1, _contains_range(10, -5, 3, 10..11));
        assert_eq!(1..2, _contains_range(10, -5, 3, 5..6));
        assert_eq!(2..3, _contains_range(10, -5, 3, 0..1));
        assert_eq!(1..3, _contains_range(10, -5, 3, 0..6));
        assert_eq!(0..3, _contains_range(10, -5, 3, 0..11));

        assert_eq!(0..2, _contains_range(-10, -5, 3, -15..0));
        assert_eq!(1..3, _contains_range(-10, -5, 3, -20..-14));
        assert_eq!(2..3, _contains_range(-10, -5, 3, -20..-19));

        assert_eq!(0..5, _contains_range(10, -5, 5, -20..20));
        assert_eq!(2..5, _contains_range(10, -5, 5, -20..1));
        assert_eq!(3..5, _contains_range(10, -5, 5, -20..0));
        assert_eq!(0..3, _contains_range(10, -5, 5, 0..11));
        assert_eq!(0..2, _contains_range(10, -5, 5, 1..11));
        assert_eq!(1..3, _contains_range(10, -5, 5, 0..10));
    }

    #[test]
    fn column_data_delta_simple() {
        let mut col1: ColumnData<DeltaCursorInternal<5>> = ColumnData::new();
        col1.splice(0, 0, vec![1]);
        assert_eq!(col1.test_dump()[0], vec![ColExport::litrun(vec![1])],);
        col1.splice(0, 0, vec![1]);
        assert_eq!(col1.test_dump()[0], vec![ColExport::litrun(vec![1, 0])],);
        col1.splice(1, 0, vec![1]);
        assert_eq!(
            col1.test_dump()[0],
            vec![ColExport::litrun(vec![1]), ColExport::run(2, 0)],
        );
        col1.splice(2, 0, vec![1]);
        assert_eq!(
            col1.test_dump()[0],
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
            col1.test_dump(),
            vec![
                vec![ColExport::litrun(vec![1, 9, -8, 9])],
                vec![ColExport::litrun(vec![-7, 23, -8, -16])],
                vec![ColExport::litrun(vec![18, -7, -12, 6])],
            ]
        );
        assert_eq!(col1.save(), col1a.save());
        for i in 0..col1.len() {
            assert_eq!(col1.get(i), col1a.get(i));
        }

        // lit run capped by runs
        let mut col2: ColumnData<DeltaCursorInternal<5>> = ColumnData::new();
        col2.splice(0, 0, vec![1, 2, 10, 11, 4, 27, 19, 3, 21, 14, 15, 16]);
        assert_eq!(
            col2.test_dump(),
            vec![
                vec![ColExport::run(2, 1), ColExport::litrun(vec![8, 1])],
                vec![ColExport::litrun(vec![-7, 23, -8, -16])],
                vec![ColExport::litrun(vec![18, -7]), ColExport::run(2, 1)],
            ]
        );

        assert_eq!(
            col2.save(),
            vec![2, 1, 120, 8, 1, 121, 23, 120, 112, 18, 121, 2, 1]
        );

        // lit run capped by runs
        let mut col3: ColumnData<DeltaCursorInternal<5>> = ColumnData::new();
        col3.splice(0, 0, vec![1, 10, 5, 6, 7, 9, 11, 20, 25, 19, 10, 9, 19, 29]);
        assert_eq!(
            col3.test_dump(),
            vec![
                vec![ColExport::litrun(vec![1, 9, -5]), ColExport::run(2, 1),],
                vec![ColExport::run(2, 2), ColExport::litrun(vec![9, 5]),],
                vec![ColExport::litrun(vec![-6, -9, -1]), ColExport::run(2, 10)],
            ]
        );
        assert_eq!(
            col3.save(),
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
            col4.test_dump(),
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
        assert_eq!(
            col4.save(),
            vec![2, 1, 2, 2, 2, 3, 2, 4, 2, 5, 2, 6, 2, 7, 2, 8, 2, 9]
        );

        // empty data
        let col5: ColumnData<DeltaCursorInternal<5>> = ColumnData::new();
        assert_eq!(col5.test_dump(), vec![vec![]]);
        assert_eq!(col5.save(), Vec::<u8>::new());
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
        col1.splice::<i64, _>(2, 1, vec![]);
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

        col1.splice::<i64, _>(4, 3, vec![]);

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
            col.test_dump(),
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

    #[test]
    fn delta_export_join_complex() {
        // here we create a slab that starts with a null
        // and has a different abs than the cursor from the previous slab
        let mut col: ColumnData<DeltaCursorInternal<5>> = ColumnData::new();
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
        assert_eq!(col.to_vec(), data);

        col.splice(1, 2, vec![Some(10), None]);
        data.splice(1..3, vec![Some(10), None]);
        assert_eq!(col.to_vec(), data);

        col.splice::<Option<i64>, _>(3, 1, vec![None]);
        data.splice(3..4, vec![None]);
        assert_eq!(col.to_vec(), data);

        assert_eq!(
            col.test_dump(),
            vec![
                vec![
                    ColExport::Null(1),
                    ColExport::litrun(vec![10]),
                    ColExport::Null(1),
                ],
                vec![ColExport::Null(1), ColExport::litrun(vec![2, 0]),],
                vec![ColExport::run(3, 1),],
                vec![ColExport::litrun(vec![0]), ColExport::run(2, 1),],
            ]
        );
        let copy: ColumnData<DeltaCursor> = ColumnData::load(&col.save()).unwrap();
        assert_eq!(col.to_vec(), copy.to_vec());
    }

    #[test]
    fn delta_export_join_simple() {
        // here we create a slab that starts with a null
        // and has a different abs than the cursor from the previous slab
        let mut col: ColumnData<DeltaCursorInternal<5>> = ColumnData::new();
        let data = vec![
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
        assert_eq!(col.to_vec(), data);

        assert_eq!(
            col.test_dump(),
            vec![
                vec![ColExport::Null(1), ColExport::litrun(vec![0, 2]),],
                vec![
                    ColExport::Run(2, 1),
                    ColExport::litrun(vec![0]),
                    ColExport::run(3, 1)
                ],
                vec![ColExport::litrun(vec![0]), ColExport::run(2, 1),],
            ]
        );
        let copy: ColumnData<DeltaCursor> = ColumnData::load(&col.save()).unwrap();
        assert_eq!(col.to_vec(), copy.to_vec());
    }

    #[test]
    fn delta_iter_nth() {
        let mut col: ColumnData<DeltaCursor> = ColumnData::new();
        let mut data = vec![];
        for _ in 0..10000 {
            let value = rand::random::<u32>() % 10;
            if value > 0 {
                data.push(Some(value as i64 - 1));
            } else {
                data.push(None);
            }
        }
        col.splice(0, 0, data.clone());

        for _ in 0..1000 {
            let mut iter1 = data.iter();
            let mut iter2 = col.iter();
            let mut step = rand::random::<u32>() % 40;
            while let Some(val1) = iter1.nth(step as usize) {
                let val2 = iter2.nth(step as usize);
                assert_eq!(val1.as_ref(), val2.flatten().as_deref());
                step = rand::random::<u32>() % ((data.len() as u32) / 2);
            }
        }
    }

    #[test]
    fn delta_contains() {
        let cursor = DeltaCursor {
            abs: 100,
            ..Default::default()
        };

        assert_eq!(cursor.contains(&Run::init(1, 3), 100.into()), Some(0..1));

        assert_eq!(cursor.contains(&Run::init(10, 3), 100.into()), Some(9..10));
        assert_eq!(cursor.contains(&Run::init(10, 3), 97.into()), Some(8..9));
        assert_eq!(cursor.contains(&Run::init(10, 3), 94.into()), Some(7..8));
        assert_eq!(cursor.contains(&Run::init(10, 3), 103.into()), None);
        assert_eq!(cursor.contains(&Run::init(10, 3), 73.into()), Some(0..1));
        assert_eq!(cursor.contains(&Run::init(10, 3), 70.into()), None);

        assert_eq!(cursor.contains(&Run::init(10, 0), 100.into()), Some(0..10));
        assert_eq!(cursor.contains(&Run::init(10, 0), 101.into()), None);

        assert_eq!(cursor.contains(&Run::init(1, -1), 100.into()), Some(0..1));
        assert_eq!(cursor.contains(&Run::init(3, -1), 100.into()), Some(2..3));

        assert_eq!(cursor.contains(&Run::init(3, -10), 110.into()), Some(1..2));
        assert_eq!(cursor.contains(&Run::init(3, -10), 120.into()), Some(0..1));
        assert_eq!(cursor.contains(&Run::init(3, -10), 130.into()), None);
        assert_eq!(cursor.contains(&Run::init(3, -10), 90.into()), None);
        assert_eq!(cursor.contains(&Run::init(3, -10), 101.into()), None);
    }

    #[test]
    fn delta_cursor_iter() {
        let data = vec![1, 20, 3, 4, 5, 6, 10, 12, 14];
        let col: ColumnData<DeltaCursor> = data.into();
        let saved = col.save();
        let iter1 = col.iter().collect::<Vec<_>>();
        let iter2 = DeltaCursor::iter(&saved)
            .map(|d| d.unwrap())
            .collect::<Vec<_>>();
        assert_eq!(iter1, iter2);
    }

    // there is currently a bug where literal runs on slab boundaries save wrong
    // currently not fixed as save_to is only used on locked encoders
    #[test]
    #[ignore]
    fn delta_cursor_encoder_bug() {
        let bad_data = vec![
            83, 84, 30, 37, 76, 77, 78, 79, 26, 26, 87, 88, 89, 90, 36, 97, 98, 99, 100, 41, 44,
            120, 121, 122, 123, 27, 103, 104, 105, 106, 38, 31, 31, 71, 72, 73, 74, 15, 92, 93, 94,
            95, 61, 108, 109, 110, 111, 62, 37, 35, 58, 114, 115, 116, 117, 30, 33,
        ];
        let mut enc: Encoder<'_, DeltaCursor> = Encoder::new(false);
        for d in bad_data {
            enc.append(d);
        }
        let mut data1 = vec![];
        let mut data2 = vec![];
        enc.clone().into_column_data().save_to(&mut data1);
        enc.save_to(&mut data2);

        let _c1 = DeltaCursor::iter(&data1);
        let _c2 = DeltaCursor::iter(&data2);
        assert_eq!(data1, data2);
    }
}
