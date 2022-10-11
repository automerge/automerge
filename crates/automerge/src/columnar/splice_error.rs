use std::convert::Infallible;

/// Represents an error which occurred when splicing.
///
/// When splicing values into existing column storage there are two kinds of errors which can
/// occur, those caused by iterating over the existing items, and those caused by iterating over
/// the replacement items.
#[derive(Debug)]
pub(crate) enum SpliceError<E, R> {
    /// There was an error reading from the existing column storage
    ReadExisting(E),
    /// There was an error reading from the iterator of new rows
    ReadReplace(R),
}

impl<E> SpliceError<E, Infallible> {
    /// Map a spliceerror which is infallible in it's `Replace` error type into a different error.
    ///
    /// This is used when you have performed a splice with a `replace` iterator which is
    /// infallible and need to return a more general `SpliceError`
    pub(crate) fn existing<R>(self) -> SpliceError<E, R> {
        match self {
            SpliceError::ReadExisting(e) => SpliceError::ReadExisting(e),
            SpliceError::ReadReplace(_) => unreachable!("absurd"),
        }
    }
}

impl<E, R> std::error::Error for SpliceError<E, R>
where
    E: std::error::Error,
    R: std::error::Error,
{
}

impl<E, R> std::fmt::Display for SpliceError<E, R>
where
    E: std::fmt::Display,
    R: std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ReadExisting(e) => write!(f, "error reading from existing rows: {}", e),
            Self::ReadReplace(e) => write!(f, "error reading from replacement rows: {}", e),
        }
    }
}
