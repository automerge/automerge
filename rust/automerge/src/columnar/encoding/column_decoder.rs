use crate::{
    columnar::{
        column_range::{KeyIter, ObjIdIter, OpIdListIter, ValueIter},
        encoding, Key,
    },
    types::{ObjId, OpId},
    ScalarValue,
};

pub(crate) trait IntoColError: std::error::Error {
    fn into_col_error<S: AsRef<str>>(self, col_name: S) -> encoding::DecodeColumnError;
}

impl IntoColError for encoding::raw::Error {
    fn into_col_error<S: AsRef<str>>(self, col_name: S) -> encoding::DecodeColumnError {
        encoding::DecodeColumnError::decode_raw(col_name, self)
    }
}

impl IntoColError for encoding::DecodeColumnError {
    fn into_col_error<S: AsRef<str>>(self, col_name: S) -> encoding::DecodeColumnError {
        self.in_column(col_name)
    }
}

/// A helper trait which allows users to annotate decoders with errors containing a column name
///
/// Frequently we have an iterator which decodes values from some underlying column storage, e.g.
/// we might have a `BooleanDecoder` which decodes items from an `insert` column. In the context
/// where we are reading from this column we would like to produce errors which describe which
/// column the error occurred in - to this end we require that the error produced by the underlying
/// decoder implement `IntoColError` and we provide the `next_in_col` method to call
/// `into_col_error` on any errors produced by the decoder.
pub(crate) trait ColumnDecoder<T>: Iterator<Item = Result<T, Self::Error>> {
    type Error: IntoColError;
    type Value;

    fn maybe_next_in_col<S: AsRef<str>>(
        &mut self,
        col_name: S,
    ) -> Result<Option<Self::Value>, encoding::DecodeColumnError>;

    /// Decode the next value from this decoder, annotating any error with the `col_name`
    fn next_in_col<S: AsRef<str>>(
        &mut self,
        col_name: S,
    ) -> Result<Self::Value, encoding::DecodeColumnError> {
        self.maybe_next_in_col(&col_name)?
            .ok_or_else(|| encoding::DecodeColumnError::unexpected_null(col_name))
    }
}

impl ColumnDecoder<bool> for encoding::BooleanDecoder<'_> {
    type Error = encoding::raw::Error;
    type Value = bool;

    fn maybe_next_in_col<S: AsRef<str>>(
        &mut self,
        col_name: S,
    ) -> Result<Option<bool>, encoding::DecodeColumnError> {
        self.next()
            .transpose()
            .map_err(|e| e.into_col_error(col_name))
    }
}

impl<I, T, E> ColumnDecoder<Option<T>> for I
where
    I: Iterator<Item = Result<Option<T>, E>>,
    E: IntoColError,
{
    type Error = E;
    type Value = T;

    fn maybe_next_in_col<S: AsRef<str>>(
        &mut self,
        col_name: S,
    ) -> Result<Option<T>, encoding::DecodeColumnError> {
        Ok(self
            .next()
            .transpose()
            .map_err(|e| e.into_col_error(col_name))?
            .flatten())
    }
}

impl ColumnDecoder<Vec<OpId>> for OpIdListIter<'_> {
    type Error = encoding::DecodeColumnError;
    type Value = Vec<OpId>;

    fn maybe_next_in_col<S: AsRef<str>>(
        &mut self,
        col_name: S,
    ) -> Result<Option<Vec<OpId>>, encoding::DecodeColumnError> {
        self.next().transpose().map_err(|e| e.in_column(col_name))
    }
}

impl ColumnDecoder<ScalarValue> for ValueIter<'_> {
    type Error = encoding::DecodeColumnError;
    type Value = ScalarValue;

    fn maybe_next_in_col<S: AsRef<str>>(
        &mut self,
        col_name: S,
    ) -> Result<Option<Self::Value>, encoding::DecodeColumnError> {
        self.next().transpose().map_err(|e| e.in_column(col_name))
    }
}

impl ColumnDecoder<Key> for KeyIter<'_> {
    type Error = encoding::DecodeColumnError;
    type Value = Key;

    fn maybe_next_in_col<S: AsRef<str>>(
        &mut self,
        col_name: S,
    ) -> Result<Option<Self::Value>, encoding::DecodeColumnError> {
        self.next().transpose().map_err(|e| e.in_column(col_name))
    }
}

impl ColumnDecoder<ObjId> for ObjIdIter<'_> {
    type Value = ObjId;
    type Error = encoding::DecodeColumnError;

    fn maybe_next_in_col<S: AsRef<str>>(
        &mut self,
        col_name: S,
    ) -> Result<Option<Self::Value>, encoding::DecodeColumnError> {
        self.next().transpose().map_err(|e| e.in_column(col_name))
    }
}
