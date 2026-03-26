use super::{ColumnDefault, ColumnValueRef};

/// Options for [`Column::load_with`], [`PrefixColumn::load_with`], etc.
///
/// Use the builder methods to configure loading behavior:
///
/// - [`with_length`](LoadOpts::with_length) — validate length and produce a
///   default column when data is empty (requires [`ColumnDefault`]).
/// - [`with_validation`](LoadOpts::with_validation) — validate each decoded
///   value with a function pointer.
/// - [`with_max_segments`](LoadOpts::with_max_segments) — override the default
///   slab segment budget (default: 16).
pub struct LoadOpts<T: ColumnValueRef> {
    pub(crate) length: Option<usize>,
    pub(crate) validate: Option<for<'a> fn(T::Get<'a>) -> Option<String>>,
    pub(crate) max_segments: usize,
}

impl<T: ColumnValueRef> Clone for LoadOpts<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: ColumnValueRef> Copy for LoadOpts<T> {}

impl<T: ColumnValueRef> LoadOpts<T> {
    pub fn new() -> Self {
        Self {
            length: None,
            validate: None,
            max_segments: 16,
        }
    }

    /// Validate each decoded value with `f`. If `f` returns `Some(msg)`,
    /// loading fails with [`PackError::InvalidValue`](crate::PackError::InvalidValue).
    pub fn with_validation(mut self, f: for<'a> fn(T::Get<'a>) -> Option<String>) -> Self {
        self.validate = Some(f);
        self
    }

    /// Override the maximum number of segments per slab (default: 16).
    pub fn with_max_segments(mut self, n: usize) -> Self {
        self.max_segments = n;
        self
    }
}

impl<T: ColumnDefault> LoadOpts<T> {
    /// Require the loaded column to have exactly `len` items.
    ///
    /// If the data is empty, produces a default column of `len` items
    /// (all-null for `Option<T>`, all-false for `bool`) instead of erroring.
    /// If the data is non-empty and decodes to a different length, returns
    /// [`PackError::InvalidLength`](crate::PackError::InvalidLength).
    pub fn with_length(mut self, len: usize) -> Self {
        self.length = Some(len);
        self
    }
}

impl<T: ColumnValueRef> Default for LoadOpts<T> {
    fn default() -> Self {
        Self::new()
    }
}
