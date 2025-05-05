use super::boolean::BooleanCursorInternal;
use super::cursor::{ColumnCursor, Run};
use super::delta::DeltaCursorInternal;
use super::pack::Packable;
use super::raw::RawCursorInternal;
use super::rle::RleCursor;
use crate::ColumnData;
use std::borrow::Borrow;

pub(crate) trait TestDumpable<T: Packable + ?Sized> {
    fn test_dump(data: &[u8]) -> Vec<ColExport<T>>;
}

impl<const B: usize, P: Packable + ?Sized> TestDumpable<P> for RleCursor<B, P> {
    fn test_dump(data: &[u8]) -> Vec<ColExport<P>> {
        let mut cursor = Self::default();
        let mut current = None;
        let mut result = vec![];
        while let Some(run) = cursor.next(data) {
            match run {
                Run { count, value: None } => {
                    if let Some(run) = current.take() {
                        result.push(ColExport::litrun(run))
                    }
                    result.push(ColExport::Null(count))
                }
                Run {
                    count: 1,
                    value: Some(v),
                } => {
                    if cursor.num_left() == 0 {
                        let mut run = current.take().unwrap_or_default();
                        run.push(v);
                        result.push(ColExport::litrun(run))
                    } else if let Some(run) = &mut current {
                        run.push(v);
                    } else {
                        current = Some(vec![v]);
                    }
                }
                Run {
                    count,
                    value: Some(v),
                } => {
                    if let Some(run) = current.take() {
                        result.push(ColExport::litrun(run))
                    }
                    result.push(ColExport::run(count, v))
                }
            }
        }
        if let Some(run) = current.take() {
            result.push(ColExport::litrun(run))
        }
        result
    }
}

impl<const B: usize> TestDumpable<bool> for BooleanCursorInternal<B> {
    fn test_dump(data: &[u8]) -> Vec<ColExport<bool>> {
        let mut result = vec![];
        let mut cursor = Self::default();
        while let Ok(Some(Run { count, value })) = cursor.try_next(data) {
            if count > 0 {
                result.push(ColExport::Run(count, *value.unwrap()))
            }
        }
        result
    }
}
impl<const B: usize> TestDumpable<i64> for DeltaCursorInternal<B> {
    fn test_dump(data: &[u8]) -> Vec<ColExport<i64>> {
        super::delta::SubCursor::<B>::test_dump(data)
    }
}

impl<const B: usize> TestDumpable<[u8]> for RawCursorInternal<B> {
    fn test_dump(data: &[u8]) -> Vec<ColExport<[u8]>> {
        vec![ColExport::Raw(data.to_vec())]
    }
}

#[allow(private_bounds)]
impl<C: ColumnCursor + TestDumpable<<C as ColumnCursor>::Item>> ColumnData<C> {
    pub(crate) fn test_dump(&self) -> Vec<Vec<ColExport<C::Item>>> {
        self.slabs
            .iter()
            .map(|s| <C as TestDumpable<C::Item>>::test_dump(s.as_slice()))
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ColExport<P: Packable + ?Sized> {
    LitRun(Vec<P::Owned>),
    Run(usize, P::Owned),
    Raw(Vec<u8>),
    Null(usize),
}

impl<P: Packable + ?Sized> ColExport<P> {
    pub(crate) fn litrun<X: Borrow<P>>(items: Vec<X>) -> Self {
        Self::LitRun(items.into_iter().map(|i| i.borrow().to_owned()).collect())
    }
    pub(crate) fn run<X: Borrow<P>>(count: usize, item: X) -> Self {
        Self::Run(count, item.borrow().to_owned())
    }
}
