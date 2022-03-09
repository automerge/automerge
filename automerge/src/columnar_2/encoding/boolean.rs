use std::{borrow::Cow, ops::Range};

use super::{Encodable, RawDecoder};

/// Encodes booleans by storing the count of the same value.
///
/// The sequence of numbers describes the count of false values on even indices (0-indexed) and the
/// count of true values on odd indices (0-indexed).
///
/// Counts are encoded as usize.
pub(crate) struct BooleanEncoder<'a> {
    written: usize,
    buf: &'a mut Vec<u8>,
    last: bool,
    count: usize,
}

impl<'a> BooleanEncoder<'a> {
    pub(crate) fn new(output: &'a mut Vec<u8>) -> BooleanEncoder<'a> {
        BooleanEncoder {
            written: 0,
            buf: output,
            last: false,
            count: 0,
        }
    }

    pub(crate) fn append(&mut self, value: bool) {
        if value == self.last {
            self.count += 1;
        } else {
            self.written += self.count.encode(self.buf);
            self.last = value;
            self.count = 1;
        }
    }

    pub(crate) fn finish(mut self) -> usize {
        if self.count > 0 {
            self.written += self.count.encode(self.buf);
        }
        self.written
    }
}

impl<'a> From<&'a mut Vec<u8>> for BooleanEncoder<'a> {
    fn from(output: &'a mut Vec<u8>) -> Self {
        BooleanEncoder::new(output)
    }
}

/// See the discussion of [`BooleanEncoder`] for details on this encoding
#[derive(Clone, Debug)]
pub(crate) struct BooleanDecoder<'a> {
    decoder: RawDecoder<'a>,
    last_value: bool,
    count: usize,
}

impl<'a> BooleanDecoder<'a> {
    #[allow(dead_code)]
    pub(crate) fn done(&self) -> bool {
        self.decoder.done()
    }

    #[allow(dead_code)]
    pub(crate) fn splice<I: Iterator<Item = bool>>(
        &mut self,
        replace: Range<usize>,
        mut replace_with: I,
        out: &mut Vec<u8>,
    ) -> Range<usize> {
        let start = out.len();
        let mut encoder = BooleanEncoder::new(out);
        let mut idx = 0;
        while idx < replace.start {
            match self.next() {
                Some(elem) => encoder.append(elem),
                None => panic!("out of bounds"),
            }
            idx += 1;
        }
        for _ in 0..replace.len() {
            self.next();
            if let Some(next) = replace_with.next() {
                encoder.append(next);
            }
        }
        for next in replace_with.by_ref() {
            encoder.append(next);
        }
        for next in self.by_ref() {
            encoder.append(next);
        }
        start..(start + encoder.finish())
    }
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
    type Item = bool;

    fn next(&mut self) -> Option<bool> {
        while self.count == 0 {
            if self.decoder.done() && self.count == 0 {
                return None;
            }
            self.count = self.decoder.read().unwrap_or_default();
            self.last_value = !self.last_value;
        }
        self.count -= 1;
        Some(self.last_value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::columnar_2::encoding::properties::splice_scenario;

    use proptest::prelude::*;

    fn encode(vals: &[bool]) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut encoder = BooleanEncoder::new(&mut buf);
        for val in vals {
            encoder.append(*val);
        }
        encoder.finish();
        buf
    }

    fn decode(buf: &[u8]) -> Vec<bool> {
        BooleanDecoder::from(buf).collect()
    }

    proptest! {
        #[test]
        fn encode_decode_bools(vals in proptest::collection::vec(any::<bool>(), 0..100)) {
            assert_eq!(vals, decode(&encode(&vals)))
        }

        #[test]
        fn splice_bools(scenario in splice_scenario(any::<bool>())) {
            let encoded = encode(&scenario.initial_values);
            let mut decoder = BooleanDecoder::from(&encoded[..]);
            let mut out = Vec::new();
            let r = decoder.splice(scenario.replace_range.clone(), scenario.replacements.iter().copied(), &mut out);
            let result = decode(&out);
            scenario.check(result);
            assert_eq!(r.len(), out.len());
        }
    }
}
