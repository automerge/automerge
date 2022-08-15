use std::ops::Range;

use super::{CellValue, SimpleColIter, SimpleColRange, SimpleValue};
use crate::columnar::{
    column_range::{RleRange, ValueIter, ValueRange},
    encoding::{col_error::DecodeColumnError, RleDecoder},
};

/// A group column range is one with a "num" column and zero or more "grouped" columns. The "num"
/// column contains RLE encoded u64s, each `u64` represents the number of values to read from each
/// of the grouped columns in order to produce a `CellValue::Group` for the current row.
#[derive(Debug, Clone)]
pub(crate) struct GroupRange {
    pub(crate) num: RleRange<u64>,
    pub(crate) values: Vec<GroupedColumnRange>,
}

impl GroupRange {
    pub(crate) fn new(num: RleRange<u64>, values: Vec<GroupedColumnRange>) -> Self {
        Self { num, values }
    }

    #[allow(dead_code)]
    pub(crate) fn iter<'a>(&self, data: &'a [u8]) -> GroupIter<'a> {
        GroupIter {
            num: self.num.decoder(data),
            values: self.values.iter().map(|v| v.iter(data)).collect(),
        }
    }

    pub(crate) fn range(&self) -> Range<usize> {
        let start = self.num.start();
        let end = self
            .values
            .last()
            .map(|v| v.range().end)
            .unwrap_or_else(|| self.num.end());
        start..end
    }
}

/// The type of ranges which can be the "grouped" columns in a `GroupRange`
#[derive(Debug, Clone)]
pub(crate) enum GroupedColumnRange {
    Value(ValueRange),
    Simple(SimpleColRange),
}

impl GroupedColumnRange {
    fn iter<'a>(&self, data: &'a [u8]) -> GroupedColIter<'a> {
        match self {
            Self::Value(vr) => GroupedColIter::Value(vr.iter(data)),
            Self::Simple(sc) => GroupedColIter::Simple(sc.iter(data)),
        }
    }

    pub(crate) fn range(&self) -> Range<usize> {
        match self {
            Self::Value(vr) => vr.range(),
            Self::Simple(s) => s.range(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct GroupIter<'a> {
    num: RleDecoder<'a, u64>,
    values: Vec<GroupedColIter<'a>>,
}

impl<'a> GroupIter<'a> {
    fn try_next(&mut self) -> Result<Option<CellValue>, DecodeColumnError> {
        let num = self
            .num
            .next()
            .transpose()
            .map_err(|e| DecodeColumnError::decode_raw("num", e))?;
        match num {
            None => Ok(None),
            Some(None) => Err(DecodeColumnError::unexpected_null("num")),
            Some(Some(num)) => {
                let mut row = Vec::new();
                for _ in 0..num {
                    let mut inner_row = Vec::new();
                    for (index, value_col) in self.values.iter_mut().enumerate() {
                        match value_col.next().transpose()? {
                            None => {
                                return Err(DecodeColumnError::unexpected_null(format!(
                                    "col {}",
                                    index
                                )))
                            }
                            Some(v) => {
                                inner_row.push(v);
                            }
                        }
                    }
                    row.push(inner_row);
                }
                Ok(Some(CellValue::Group(row)))
            }
        }
    }
}

impl<'a> Iterator for GroupIter<'a> {
    type Item = Result<CellValue, DecodeColumnError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

#[derive(Debug, Clone)]
enum GroupedColIter<'a> {
    Value(ValueIter<'a>),
    Simple(SimpleColIter<'a>),
}

impl<'a> GroupedColIter<'a> {
    fn try_next(&mut self) -> Result<Option<SimpleValue>, DecodeColumnError> {
        match self {
            Self::Value(viter) => Ok(viter.next().transpose()?.map(SimpleValue::Value)),
            Self::Simple(siter) => siter
                .next()
                .transpose()
                .map_err(|e| DecodeColumnError::decode_raw("a simple column", e)),
        }
    }
}

impl<'a> Iterator for GroupedColIter<'a> {
    type Item = Result<SimpleValue, DecodeColumnError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}
