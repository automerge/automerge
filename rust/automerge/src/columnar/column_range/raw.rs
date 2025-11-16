use std::{borrow::Cow, ops::Range};

use crate::columnar::encoding::RawDecoder;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RawRange(Range<usize>);

impl RawRange {
    pub(crate) fn decoder<'a>(&self, data: &'a [u8]) -> RawDecoder<'a> {
        RawDecoder::from(Cow::Borrowed(&data[self.0.clone()]))
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn end(&self) -> usize {
        self.0.end
    }
}

impl AsRef<Range<usize>> for RawRange {
    fn as_ref(&self) -> &Range<usize> {
        &self.0
    }
}

impl From<Range<usize>> for RawRange {
    fn from(r: Range<usize>) -> RawRange {
        RawRange(r)
    }
}

impl From<RawRange> for Range<usize> {
    fn from(r: RawRange) -> Range<usize> {
        r.0
    }
}

impl<'a> From<&'a RawRange> for Range<usize> {
    fn from(r: &'a RawRange) -> Range<usize> {
        r.0.clone()
    }
}
