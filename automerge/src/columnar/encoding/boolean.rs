use std::borrow::Cow;

use super::{raw, Encodable, RawDecoder, Sink};

/// Encodes booleans by storing the count of the same value.
///
/// The sequence of numbers describes the count of false values on even indices (0-indexed) and the
/// count of true values on odd indices (0-indexed).
///
/// Counts are encoded as usize.
pub(crate) struct BooleanEncoder<S> {
    written: usize,
    //buf: &'a mut Vec<u8>,
    buf: S,
    last: bool,
    count: usize,
}

impl BooleanEncoder<Vec<u8>> {
    pub(crate) fn new() -> BooleanEncoder<Vec<u8>> {
        BooleanEncoder::from_sink(Vec::new())
    }
}

impl<S: Sink> BooleanEncoder<S> {
    pub(crate) fn from_sink(sink: S) -> Self {
        BooleanEncoder {
            written: 0,
            buf: sink,
            last: false,
            count: 0,
        }
    }

    pub(crate) fn append(&mut self, value: bool) {
        if value == self.last {
            self.count += 1;
        } else {
            self.written += self.count.encode(&mut self.buf);
            self.last = value;
            self.count = 1;
        }
    }

    pub(crate) fn finish(mut self) -> (S, usize) {
        if self.count > 0 {
            self.written += self.count.encode(&mut self.buf);
        }
        (self.buf, self.written)
    }
}

impl<S: Sink> From<S> for BooleanEncoder<S> {
    fn from(output: S) -> Self {
        BooleanEncoder::from_sink(output)
    }
}

/// See the discussion of [`BooleanEncoder`] for details on this encoding
#[derive(Clone, Debug)]
pub(crate) struct BooleanDecoder<'a> {
    decoder: RawDecoder<'a>,
    last_value: bool,
    count: usize,
}

impl<'a> From<Cow<'a, [u8]>> for BooleanDecoder<'a> {
    fn from(bytes: Cow<'a, [u8]>) -> Self {
        BooleanDecoder {
            decoder: RawDecoder::from(bytes),
            last_value: true,
            count: 0,
        }
    }
}

impl<'a> From<&'a [u8]> for BooleanDecoder<'a> {
    fn from(d: &'a [u8]) -> Self {
        Cow::Borrowed(d).into()
    }
}

// this is an endless iterator that returns false after input is exhausted
impl<'a> Iterator for BooleanDecoder<'a> {
    type Item = Result<bool, raw::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.count == 0 {
            if self.decoder.done() && self.count == 0 {
                return None;
            }
            self.count = match self.decoder.read() {
                Ok(c) => c,
                Err(e) => return Some(Err(e)),
            };
            self.last_value = !self.last_value;
        }
        self.count -= 1;
        Some(Ok(self.last_value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use proptest::prelude::*;

    fn encode(vals: &[bool]) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut encoder = BooleanEncoder::from_sink(&mut buf);
        for val in vals {
            encoder.append(*val);
        }
        encoder.finish();
        buf
    }

    fn decode(buf: &[u8]) -> Vec<bool> {
        BooleanDecoder::from(buf)
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    proptest! {
        #[test]
        fn encode_decode_bools(vals in proptest::collection::vec(any::<bool>(), 0..100)) {
            assert_eq!(vals, decode(&encode(&vals)))
        }
    }
}
