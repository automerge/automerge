use std::ops::Range;

use crate::{columnar::encoding::DecodeColumnError, ScalarValue};

use super::{ValueIter, ValueRange};
mod simple;
use simple::SimpleColIter;
pub(crate) use simple::SimpleColRange;
mod group;
use group::GroupIter;
pub(crate) use group::{GroupRange, GroupedColumnRange};

/// A range which can represent any column which is valid with respect to the data model of the
/// column oriented storage format. This is primarily intended to be used in two cases:
///
/// 1. As an intermediate step when parsing binary storage. We parse the column metadata into
///    GenericColumnRange, then from there into more specific range types.
/// 2. when we encounter a column which we don't expect but which we still need to retain and
///    re-encode when writing new changes.
///
/// The generic data model is represented by `CellValue`, an iterator over a generic column will
/// produce a `CellValue` for each row in the column.
#[derive(Debug, Clone)]
pub(crate) enum GenericColumnRange {
    /// A "simple" column is one which directly corresponds to a single column in the raw format
    Simple(SimpleColRange),
    /// A value range consists of two columns and produces `ScalarValue`s
    Value(ValueRange),
    /// A "group" range consists of zero or more grouped columns and produces `CellValue::Group`s
    Group(GroupRange),
}

impl GenericColumnRange {
    pub(crate) fn range(&self) -> Range<usize> {
        match self {
            Self::Simple(sc) => sc.range(),
            Self::Value(v) => v.range(),
            Self::Group(g) => g.range(),
        }
    }
}

/// The type of values which can be stored in a generic column
#[allow(dead_code)]
pub(crate) enum CellValue {
    /// The contents of a simple column
    Simple(SimpleValue),
    /// The values in a set of grouped columns
    Group(Vec<Vec<SimpleValue>>),
}

#[allow(dead_code)]
pub(crate) enum SimpleValue {
    Uint(Option<u64>),
    Int(Option<i64>),
    String(Option<smol_str::SmolStr>),
    Bool(bool),
    /// The contents of a value metadata and value raw column
    Value(ScalarValue),
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) enum GenericColIter<'a> {
    Simple(SimpleColIter<'a>),
    Value(ValueIter<'a>),
    Group(GroupIter<'a>),
}

impl<'a> GenericColIter<'a> {
    fn try_next(&mut self) -> Result<Option<CellValue>, DecodeColumnError> {
        match self {
            Self::Simple(s) => s
                .next()
                .transpose()
                .map_err(|e| DecodeColumnError::decode_raw("a simple column", e))
                .map(|v| v.map(CellValue::Simple)),
            Self::Value(v) => v
                .next()
                .transpose()
                .map(|v| v.map(|v| CellValue::Simple(SimpleValue::Value(v)))),
            Self::Group(g) => g.next().transpose(),
        }
    }
}

impl<'a> Iterator for GenericColIter<'a> {
    type Item = Result<CellValue, DecodeColumnError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}
