use std::ops::Range;

use crate::columnar::column_range::generic::GenericColumnRange;

use super::{ColumnId, ColumnSpec, ColumnType};

/// A combination of a column specification and the range of data associated with it. Note that
/// multiple (adjacent) ranges can be associated with one column as some columns are composite.
/// This is encapsulated in the `GenericColumnRange` type.
#[derive(Clone, Debug)]
pub(crate) struct Column {
    spec: ColumnSpec,
    range: GenericColumnRange,
}

impl Column {
    pub(crate) fn new(spec: ColumnSpec, range: GenericColumnRange) -> Column {
        Self { spec, range }
    }
}

impl Column {
    pub(crate) fn range(&self) -> Range<usize> {
        self.range.range()
    }

    pub(crate) fn into_ranges(self) -> GenericColumnRange {
        self.range
    }

    pub(crate) fn col_type(&self) -> ColumnType {
        self.spec.col_type()
    }

    pub(crate) fn id(&self) -> ColumnId {
        self.spec.id()
    }

    pub(crate) fn spec(&self) -> ColumnSpec {
        self.spec
    }
}
