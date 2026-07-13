use crate::column::DEFAULT_MAX_SEG;

/// Options for [`Column::load_with`](crate::Column::load_with),
/// [`Column::load_iter`](crate::Column::load_iter), etc.
///
/// A plain `LoadOpts` (no fill) can be built once and passed to loads of
/// any column type; [`with_fill`](LoadOpts::with_fill) produces a
/// `LoadOpts<Fill<A>>` pinned to the fill value's type:
///
/// - [`with_length`](LoadOpts::with_length) — validate that the loaded column
///   has the expected number of items.
/// - [`with_fill`](LoadOpts::with_fill) — when data is empty and `length` is
///   set, the load reads as `length` copies of this value instead of
///   an empty column.
/// - [`with_max_segments`](LoadOpts::with_max_segments) — override the
///   slab segment budget.
#[derive(Clone, Copy)]
pub struct LoadOpts<F = NoFill> {
    pub(crate) length: Option<usize>,
    pub(crate) fill: F,
    pub(crate) max_segments: usize,
}

/// The default (absent) fill state of [`LoadOpts`].
#[derive(Clone, Copy, Default)]
pub struct NoFill;

/// A fill value of type `A` carried by [`LoadOpts`].
#[derive(Clone, Copy)]
pub struct Fill<A>(pub(crate) A);

/// The fill state of a [`LoadOpts`]: either [`NoFill`] or a [`Fill`]
/// carrying a value usable as the column's item type.
pub trait MaybeFill<G> {
    fn fill_value(self) -> Option<G>;
}

impl<G> MaybeFill<G> for NoFill {
    fn fill_value(self) -> Option<G> {
        None
    }
}

impl<G> MaybeFill<G> for Fill<G> {
    fn fill_value(self) -> Option<G> {
        Some(self.0)
    }
}

impl LoadOpts<NoFill> {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            length: None,
            fill: NoFill,
            max_segments: DEFAULT_MAX_SEG,
        }
    }

    /// When data is empty and [`with_length`](Self::with_length) is also set,
    /// the load reads as `length` copies of `value` instead of an empty
    /// column.
    ///
    /// Has no effect unless `with_length` is also called.
    pub fn with_fill<A>(self, value: A) -> LoadOpts<Fill<A>> {
        LoadOpts {
            length: self.length,
            fill: Fill(value),
            max_segments: self.max_segments,
        }
    }
}

impl Default for LoadOpts<NoFill> {
    fn default() -> Self {
        Self::new()
    }
}

impl<F> LoadOpts<F> {
    /// Require the loaded column to have exactly `len` items.
    ///
    /// If the data is non-empty and decodes to a different length, returns
    /// [`PackError::InvalidLength`](crate::PackError::InvalidLength).
    ///
    /// To produce a filled column when data is empty, combine with
    /// [`with_fill`](LoadOpts::with_fill).
    pub fn with_length(mut self, len: usize) -> Self {
        self.length = Some(len);
        self
    }

    /// Override the maximum number of segments per slab.
    pub fn with_max_segments(mut self, n: usize) -> Self {
        self.max_segments = n;
        self
    }

}
