use std::ops::Range;

use crate::columnar_2::{
    column_range::{BooleanRange, DeltaRange, RleRange},
    encoding::{BooleanDecoder, DeltaDecoder, RleDecoder},
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

impl<'a> Iterator for SimpleColIter<'a> {
    type Item = SimpleValue;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::RleInt(d) => d.next().map(SimpleValue::Uint),
            Self::RleString(d) => d.next().map(SimpleValue::String),
            Self::Delta(d) => d.next().map(SimpleValue::Int),
            Self::Boolean(d) => d.next().map(SimpleValue::Bool),
        }
    }
}
