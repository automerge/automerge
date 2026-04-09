use super::column::DEFAULT_MAX_SEG;
use super::ColumnValueRef;

/// Options for [`Column::load_with`](super::Column::load_with), [`PrefixColumn::load_with`](super::PrefixColumn::load_with), etc.
///
/// Use the builder methods to configure loading behavior:
///
/// - [`with_length`](LoadOpts::with_length) — validate that the loaded column
///   has the expected number of items.
/// - [`with_fill`](LoadOpts::with_fill) — when data is empty and `length` is
///   set, create a column filled with this value instead of returning empty.
/// - [`with_validation`](LoadOpts::with_validation) — validate each decoded
///   value with a function pointer.
/// - [`with_max_segments`](LoadOpts::with_max_segments) — override the
///   slab segment budget.
pub struct LoadOpts<T: ColumnValueRef> {
    pub(crate) length: Option<usize>,
    pub(crate) fill: Option<T::Get<'static>>,
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
            fill: None,
            validate: None,
            max_segments: DEFAULT_MAX_SEG,
        }
    }

    /// Require the loaded column to have exactly `len` items.
    ///
    /// If the data is non-empty and decodes to a different length, returns
    /// [`PackError::InvalidLength`](crate::PackError::InvalidLength).
    ///
    /// To produce a filled column when data is empty, combine with
    /// [`with_fill`](Self::with_fill).
    pub fn with_length(mut self, len: usize) -> Self {
        self.length = Some(len);
        self
    }

    /// When data is empty and [`with_length`](Self::with_length) is also set,
    /// create a column of `length` copies of `value` instead of returning
    /// an empty column.
    ///
    /// Has no effect unless `with_length` is also called.
    pub fn with_fill(mut self, value: T::Get<'static>) -> Self {
        self.fill = Some(value);
        self
    }

    /// Validate each decoded value with `f`. If `f` returns `Some(msg)`,
    /// loading fails with [`PackError::InvalidValue`](crate::PackError::InvalidValue).
    pub fn with_validation(mut self, f: for<'a> fn(T::Get<'a>) -> Option<String>) -> Self {
        self.validate = Some(f);
        self
    }

    /// Override the maximum number of segments per slab.
    pub fn with_max_segments(mut self, n: usize) -> Self {
        self.max_segments = n;
        self
    }
}

impl<T: ColumnValueRef> Default for LoadOpts<T> {
    fn default() -> Self {
        Self::new()
    }
}
