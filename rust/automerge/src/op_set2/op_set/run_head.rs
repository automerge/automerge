use std::marker::PhantomData;

use crate::op_set2::ReadOpError;

/// A run of equal (or, for delta columns, evenly stepping) values that
/// can be consumed from the front.
pub(crate) trait SplitRun: Copy {
    fn len(&self) -> usize;
    /// Drops the first `n` rows; `n <= len`.
    fn advance(&mut self, n: usize);
}

impl<T: Copy> SplitRun for hexane::Run<T> {
    fn len(&self) -> usize {
        self.count
    }

    fn advance(&mut self, n: usize) {
        self.count -= n;
    }
}

impl SplitRun for hexane::DeltaRun {
    fn len(&self) -> usize {
        self.count
    }

    fn advance(&mut self, n: usize) {
        self.prefix += self.delta.unwrap_or(0) * n as i64;
        self.count -= n;
    }
}

pub(crate) trait RunSource {
    type Run: SplitRun;
    fn next_run(&mut self) -> Result<Option<Self::Run>, ReadOpError>;
}

/// Adapts a [`hexane::RunSrc`] (an RLE, boolean or prefix column).
#[derive(Clone)]
pub(crate) struct Rle<'a, T, S>(S, PhantomData<&'a T>);

impl<'a, T, S> RunSource for Rle<'a, T, S>
where
    T: hexane::ColumnValueRef,
    T::Get<'a>: Copy,
    S: hexane::RunSrc<'a, T>,
{
    type Run = hexane::Run<T::Get<'a>>;

    fn next_run(&mut self) -> Result<Option<Self::Run>, ReadOpError> {
        Ok(self.0.try_next_run()?)
    }
}

impl<T: hexane::DeltaValue, C: hexane::Codec> RunSource for hexane::DeltaIter<'_, T, C> {
    type Run = hexane::DeltaRun;

    fn next_run(&mut self) -> Result<Option<Self::Run>, ReadOpError> {
        Ok(hexane::DeltaIter::next_run(self))
    }
}

/// One column's runs, with the unconsumed rest of the current run held
/// back, so that columns with different run boundaries can be walked
/// together: [`Self::peek`] each, consume the shortest length from all.
#[derive(Clone)]
pub(crate) struct RunHead<S: RunSource> {
    src: S,
    head: Option<S::Run>,
    /// names the column in errors
    pub(super) name: &'static str,
}

impl<'a, T, S> RunHead<Rle<'a, T, S>>
where
    T: hexane::ColumnValueRef,
    T::Get<'a>: Copy,
    S: hexane::RunSrc<'a, T>,
{
    pub(crate) fn rle(src: S, name: &'static str) -> Self {
        Self::new(Rle(src, PhantomData), name)
    }
}

impl<S: RunSource> RunHead<S> {
    pub(crate) fn new(src: S, name: &'static str) -> Self {
        Self {
            src,
            head: None,
            name,
        }
    }

    /// The rest of the current run; `None` at the end of the column.
    #[inline]
    pub(crate) fn peek(&mut self) -> Result<Option<S::Run>, ReadOpError> {
        if self.head.is_none() {
            self.head = self.src.next_run()?;
        }
        Ok(self.head)
    }

    /// [`Self::peek`], where running out is malformed input.
    #[inline]
    pub(crate) fn peek_some(&mut self) -> Result<S::Run, ReadOpError> {
        self.peek()?.ok_or(ReadOpError::MissingValue(self.name))
    }

    /// Consumes `n` rows of the run [`Self::peek`] returned.
    #[inline]
    pub(crate) fn consume(&mut self, n: usize) {
        let head = self.head.as_mut().expect("consume follows peek");
        debug_assert!(n <= head.len());
        head.advance(n);
        if head.len() == 0 {
            self.head = None;
        }
    }

    /// Skips `n` rows, across runs.
    pub(crate) fn skip(&mut self, mut n: usize) -> Result<(), ReadOpError> {
        while n > 0 {
            let take = self.peek_some()?.len().min(n);
            self.consume(take);
            n -= take;
        }
        Ok(())
    }
}
