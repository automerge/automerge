use std::{borrow::Cow, ops::Range};

use crate::columnar::encoding::{
    BooleanDecoder, BooleanEncoder, MaybeBooleanDecoder, MaybeBooleanEncoder,
};

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct BooleanRange(Range<usize>);

impl BooleanRange {
    pub(crate) fn decoder<'a>(&self, data: &'a [u8]) -> BooleanDecoder<'a> {
        BooleanDecoder::from(Cow::Borrowed(&data[self.0.clone()]))
    }

    pub(crate) fn encode<I: Iterator<Item = bool>>(items: I, out: &mut Vec<u8>) -> Self {
        let start = out.len();
        let mut encoder = BooleanEncoder::from(out);
        for i in items {
            encoder.append(i);
        }
        let (_, len) = encoder.finish();
        (start..(start + len)).into()
    }
}

impl AsRef<Range<usize>> for BooleanRange {
    fn as_ref(&self) -> &Range<usize> {
        &self.0
    }
}

impl From<Range<usize>> for BooleanRange {
    fn from(r: Range<usize>) -> BooleanRange {
        BooleanRange(r)
    }
}

impl From<BooleanRange> for Range<usize> {
    fn from(r: BooleanRange) -> Range<usize> {
        r.0
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct MaybeBooleanRange(Range<usize>);

impl MaybeBooleanRange {
    pub(crate) fn decoder<'a>(&self, data: &'a [u8]) -> MaybeBooleanDecoder<'a> {
        MaybeBooleanDecoder::from(Cow::Borrowed(&data[self.0.clone()]))
    }

    pub(crate) fn encode<I: Iterator<Item = bool>>(items: I, out: &mut Vec<u8>) -> Self {
        let start = out.len();
        let mut encoder = MaybeBooleanEncoder::from_sink(out);
        for i in items {
            encoder.append(i);
        }
        let (_, len) = encoder.finish();
        (start..(start + len)).into()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl AsRef<Range<usize>> for MaybeBooleanRange {
    fn as_ref(&self) -> &Range<usize> {
        &self.0
    }
}

impl From<Range<usize>> for MaybeBooleanRange {
    fn from(r: Range<usize>) -> MaybeBooleanRange {
        MaybeBooleanRange(r)
    }
}

impl From<MaybeBooleanRange> for Range<usize> {
    fn from(r: MaybeBooleanRange) -> Range<usize> {
        r.0
    }
}
