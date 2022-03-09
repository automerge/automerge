use std::{borrow::Cow, ops::Range};

use crate::columnar_2::encoding::{BooleanDecoder, BooleanEncoder};

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
        let len = encoder.finish();
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
