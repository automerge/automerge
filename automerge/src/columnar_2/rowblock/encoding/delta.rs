use std::{borrow::Cow, ops::Range};

use super::{RleEncoder, RleDecoder};

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
    pub fn new(output: &'a mut Vec<u8>) -> DeltaEncoder<'a> {
        DeltaEncoder {
            rle: RleEncoder::new(output),
            absolute_value: 0,
        }
    }

    pub fn append_value(&mut self, value: i64) {
        self.rle
            .append_value(&(value.saturating_sub(self.absolute_value)));
        self.absolute_value = value;
    }

    pub fn append_null(&mut self) {
        self.rle.append_null();
    }

    pub fn append(&mut self, val: Option<i64>) {
        match val {
            Some(v) => self.append_value(v),
            None => self.append_null(),
        }
    }

    pub fn finish(self) -> usize {
        self.rle.finish()
    }
}

impl<'a> From<&'a mut Vec<u8>> for DeltaEncoder<'a> {
    fn from(output: &'a mut Vec<u8>) -> Self {
        DeltaEncoder::new(output) 
    }
}

/// See discussion on [`DeltaEncoder`] for the format data is stored in.
#[derive(Clone)]
pub(crate) struct DeltaDecoder<'a> {
    rle: RleDecoder<'a, i64>,
    absolute_val: i64,
}

impl<'a> DeltaDecoder<'a> {
    pub(crate) fn done(&self) -> bool {
        self.rle.done()
    }

    pub(crate) fn encode<I>(items: I, out: &mut Vec<u8>) -> Range<usize> 
    where
        I: Iterator<Item=i64>
    {
        let mut decoder = DeltaDecoder::from(&[] as &[u8]);
        decoder.splice(0..0, items.map(Some), out)
    }

    pub(crate) fn splice<I: Iterator<Item=Option<i64>>>(&mut self, replace: Range<usize>, mut replace_with: I, out: &mut Vec<u8>) -> Range<usize> {
        let start = out.len();
        let mut encoder = DeltaEncoder::new(out);
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
        while let Some(next) = replace_with.next() {
            encoder.append(next);
        }
        while let Some(next) = self.next() {
            encoder.append(next);
        }
        start..(start + encoder.finish())
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
            },
            Some(None) => Some(None),
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use crate::columnar_2::rowblock::encoding::properties::splice_scenario;

    fn encode(vals: &[Option<i64>]) -> Vec<u8> {
        let mut buf = Vec::<u8>::new();
        let mut encoder = DeltaEncoder::from(&mut buf);
        for val in vals {
            encoder.append(val.clone());
        }
        encoder.finish();
        buf
    }

    fn decode(buf: &[u8]) -> Vec<Option<i64>> {
        DeltaDecoder::from(buf).collect()
    }

    fn encodable_int() -> impl Strategy<Value = i64> + Clone {
        0..(i64::MAX / 2)
    }

    proptest!{
        #[test]
        fn encode_decode_delta(vals in proptest::collection::vec(proptest::option::of(encodable_int()), 0..100)) {
            assert_eq!(vals, decode(&encode(&vals)));
        }

        #[test]
        fn splice_delta(scenario in splice_scenario(proptest::option::of(encodable_int()))) {
            let encoded = encode(&scenario.initial_values);
            let mut decoder = DeltaDecoder::from(&encoded[..]);
            let mut out = Vec::new();
            let r = decoder.splice(scenario.replace_range.clone(), scenario.replacements.iter().cloned(), &mut out);
            let decoded = decode(&out[..]);
            scenario.check(decoded);
            assert_eq!(r.len(), out.len());
        }
    }
}
