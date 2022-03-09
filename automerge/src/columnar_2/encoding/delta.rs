use std::borrow::Cow;

use super::{RleDecoder, RleEncoder};

/// Encodes integers as the change since the previous value.
///
/// The initial value is 0 encoded as u64. Deltas are encoded as i64.
///
/// Run length encoding is then applied to the resulting sequence.
pub(crate) struct DeltaEncoder<'a> {
    rle: RleEncoder<'a, i64>,
    absolute_value: i64,
}

impl<'a> DeltaEncoder<'a> {
    pub(crate) fn new(output: &'a mut Vec<u8>) -> DeltaEncoder<'a> {
        DeltaEncoder {
            rle: RleEncoder::new(output),
            absolute_value: 0,
        }
    }

    pub(crate) fn append_value(&mut self, value: i64) {
        self.rle
            .append_value(&(value.saturating_sub(self.absolute_value)));
        self.absolute_value = value;
    }

    pub(crate) fn append_null(&mut self) {
        self.rle.append_null();
    }

    pub(crate) fn append(&mut self, val: Option<i64>) {
        match val {
            Some(v) => self.append_value(v),
            None => self.append_null(),
        }
    }

    pub(crate) fn finish(self) -> usize {
        self.rle.finish()
    }
}

impl<'a> From<&'a mut Vec<u8>> for DeltaEncoder<'a> {
    fn from(output: &'a mut Vec<u8>) -> Self {
        DeltaEncoder::new(output)
    }
}

/// See discussion on [`DeltaEncoder`] for the format data is stored in.
#[derive(Debug, Clone)]
pub(crate) struct DeltaDecoder<'a> {
    rle: RleDecoder<'a, i64>,
    absolute_val: i64,
}

impl<'a> DeltaDecoder<'a> {
    pub(crate) fn done(&self) -> bool {
        self.rle.done()
    }
}

impl<'a> From<Cow<'a, [u8]>> for DeltaDecoder<'a> {
    fn from(bytes: Cow<'a, [u8]>) -> Self {
        DeltaDecoder {
            rle: RleDecoder::from(bytes),
            absolute_val: 0,
        }
    }
}

impl<'a> From<&'a [u8]> for DeltaDecoder<'a> {
    fn from(d: &'a [u8]) -> Self {
        Cow::Borrowed(d).into()
    }
}

impl<'a> Iterator for DeltaDecoder<'a> {
    type Item = Option<i64>;

    fn next(&mut self) -> Option<Option<i64>> {
        match self.rle.next() {
            Some(Some(delta)) => {
                self.absolute_val = self.absolute_val.saturating_add(delta);
                Some(Some(self.absolute_val))
            }
            Some(None) => Some(None),
            None => None,
        }
    }
}
