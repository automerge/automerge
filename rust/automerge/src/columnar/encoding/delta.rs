use std::borrow::Cow;

use super::{raw, RleDecoder, RleEncoder, Sink};

/// Encodes integers as the change since the previous value.
///
/// The initial value is 0 encoded as u64. Deltas are encoded as i64.
///
/// Run length encoding is then applied to the resulting sequence.
pub(crate) struct DeltaEncoder<S> {
    rle: RleEncoder<S, i64>,
    absolute_value: i64,
}

impl<S: Sink> DeltaEncoder<S> {
    pub(crate) fn new(output: S) -> DeltaEncoder<S> {
        DeltaEncoder {
            rle: RleEncoder::new(output),
            absolute_value: 0,
        }
    }

    pub(crate) fn append_value(&mut self, value: i64) {
        self.rle
            .append_value(value.saturating_sub(self.absolute_value));
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

    pub(crate) fn finish(self) -> (S, usize) {
        self.rle.finish()
    }
}

impl<S: Sink> From<S> for DeltaEncoder<S> {
    fn from(output: S) -> Self {
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
    type Item = Result<Option<i64>, raw::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.rle.next() {
            Some(Ok(next)) => match next {
                Some(delta) => {
                    self.absolute_val = self.absolute_val.saturating_add(delta);
                    Some(Ok(Some(self.absolute_val)))
                }
                None => Some(Ok(None)),
            },
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}
