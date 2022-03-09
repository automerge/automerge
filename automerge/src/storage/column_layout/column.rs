use std::ops::Range;

use crate::columnar_2::column_range::generic::GenericColumnRange;

use super::{ColumnId, ColumnSpec, ColumnType};

/// A "logical" column, which is to say a column that produces a single value. A "logical" column
/// can be composed of multiple primtiive columns, access to these individual columns is via the
/// `range` function.
///
/// The type parameter `T` is a witness to what we know about whether this column is compressed. If
/// `T: compression::Uncompressed` then we know that all the columns in this layout are not
/// compressed, otherwise, this column may be compressed. There are two ways to obtain an
/// `Uncompressed` column: either `Column::uncompressed`, which returns `Some(self)` if this column
/// is uncompressed, or `Column::inflate`, which decompresses this column into a buffer and returns
/// the resulting column.
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
