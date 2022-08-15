use std::ops::Range;

use crate::columnar::{
    column_range::{BooleanRange, DeltaRange, RleRange},
    encoding::{raw, BooleanDecoder, DeltaDecoder, RleDecoder},
};

use super::SimpleValue;

/// The four types of "simple" column defined in the raw format
#[derive(Debug, Clone)]
pub(crate) enum SimpleColRange {
    /// A column containing RLE encoded u64's
    RleInt(RleRange<u64>),
    /// A column containing RLE encoded strings
    RleString(RleRange<smol_str::SmolStr>),
    /// A column containing delta -> RLE encoded i64s
    Delta(DeltaRange),
    /// A column containing boolean values
    Boolean(BooleanRange),
}

impl SimpleColRange {
    pub(super) fn iter<'a>(&self, data: &'a [u8]) -> SimpleColIter<'a> {
        match self {
            Self::RleInt(r) => SimpleColIter::RleInt(r.decoder(data)),
            Self::RleString(r) => SimpleColIter::RleString(r.decoder(data)),
            Self::Delta(r) => SimpleColIter::Delta(r.decoder(data)),
            Self::Boolean(r) => SimpleColIter::Boolean(r.decoder(data)),
        }
    }

    pub(crate) fn range(&self) -> Range<usize> {
        match self {
            Self::RleInt(r) => r.clone().into(),
            Self::RleString(r) => r.clone().into(),
            Self::Delta(r) => r.clone().into(),
            Self::Boolean(r) => r.clone().into(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum SimpleColIter<'a> {
    RleInt(RleDecoder<'a, u64>),
    RleString(RleDecoder<'a, smol_str::SmolStr>),
    Delta(DeltaDecoder<'a>),
    Boolean(BooleanDecoder<'a>),
}

impl<'a> SimpleColIter<'a> {
    fn try_next(&mut self) -> Result<Option<SimpleValue>, raw::Error> {
        match self {
            Self::RleInt(d) => read_col(d, SimpleValue::Uint),
            Self::RleString(d) => read_col(d, SimpleValue::String),
            Self::Delta(d) => read_col(d, SimpleValue::Int),
            Self::Boolean(d) => Ok(d.next().transpose()?.map(SimpleValue::Bool)),
        }
    }
}

fn read_col<C, T, F, U>(mut col: C, f: F) -> Result<Option<U>, raw::Error>
where
    C: Iterator<Item = Result<Option<T>, raw::Error>>,
    F: Fn(Option<T>) -> U,
{
    col.next().transpose().map(|v| v.map(f))
}

impl<'a> Iterator for SimpleColIter<'a> {
    type Item = Result<SimpleValue, raw::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}
