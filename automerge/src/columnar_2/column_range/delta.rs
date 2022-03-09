use std::{borrow::Cow, ops::Range};

use crate::columnar_2::encoding::{DeltaDecoder, DeltaEncoder};

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct DeltaRange(Range<usize>);

impl DeltaRange {
    pub(crate) fn decoder<'a>(&self, data: &'a [u8]) -> DeltaDecoder<'a> {
        DeltaDecoder::from(Cow::Borrowed(&data[self.0.clone()]))
    }

    pub(crate) fn encoder<'a>(&self, output: &'a mut Vec<u8>) -> DeltaEncoder<'a> {
        DeltaEncoder::from(output)
    }

    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    pub(crate) fn encode<I: Iterator<Item = Option<i64>>>(items: I, out: &mut Vec<u8>) -> Self {
        Self::from(0..0).splice(&[], 0..0, items, out)
    }

    pub(crate) fn splice<I: Iterator<Item = Option<i64>>>(
        &self,
        data: &[u8],
        replace: Range<usize>,
        mut replace_with: I,
        out: &mut Vec<u8>,
    ) -> Self {
        let start = out.len();
        let mut decoder = self.decoder(data);
        let mut encoder = self.encoder(out);
        let mut idx = 0;
        while idx < replace.start {
            match decoder.next() {
                Some(elem) => encoder.append(elem),
                None => panic!("out of bounds"),
            }
            idx += 1;
        }
        for _ in 0..replace.len() {
            decoder.next();
            if let Some(next) = replace_with.next() {
                encoder.append(next);
            }
        }
        for next in replace_with {
            encoder.append(next);
        }
        for next in decoder {
            encoder.append(next);
        }
        (start..(start + encoder.finish())).into()
    }
}

impl AsRef<Range<usize>> for DeltaRange {
    fn as_ref(&self) -> &Range<usize> {
        &self.0
    }
}

impl From<Range<usize>> for DeltaRange {
    fn from(r: Range<usize>) -> DeltaRange {
        DeltaRange(r)
    }
}

impl From<DeltaRange> for Range<usize> {
    fn from(r: DeltaRange) -> Range<usize> {
        r.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::columnar_2::encoding::properties::option_splice_scenario;
    use proptest::prelude::*;

    fn encode<I: Iterator<Item = Option<i64>>>(vals: I) -> (DeltaRange, Vec<u8>) {
        let mut buf = Vec::<u8>::new();
        let range = DeltaRange::encode(vals, &mut buf);
        (range, buf)
    }

    fn decode(range: DeltaRange, buf: &[u8]) -> Vec<Option<i64>> {
        range.decoder(buf).collect()
    }

    fn encodable_int() -> impl Strategy<Value = i64> + Clone {
        0..(i64::MAX / 2)
    }

    proptest! {
        #[test]
        fn encode_decode_delta(vals in proptest::collection::vec(proptest::option::of(encodable_int()), 0..100)) {
            let (r, encoded) = encode(vals.iter().copied());
            if vals.iter().all(|v| v.is_none()) {
                assert_eq!(encoded.len(), 0);
                let decoded = decode(r, &encoded);
                assert_eq!(Vec::<Option<i64>>::new(), decoded)
            } else {
                let decoded = decode(r, &encoded);
                assert_eq!(vals, decoded)
            }
        }

        #[test]
        fn splice_delta(scenario in option_splice_scenario(proptest::option::of(encodable_int()))) {
            let (range, encoded) = encode(scenario.initial_values.iter().copied());
            let mut out = Vec::new();
            let new_range = range.splice(&encoded, scenario.replace_range.clone(), scenario.replacements.iter().cloned(), &mut out);
            let decoded = decode(new_range, &out);
            scenario.check_optional(decoded);
        }
    }

    #[test]
    fn bugbug() {
        let vals: Vec<i64> = vec![6, 5, 8, 9, 10, 11, 12, 13];
        let (r, encoded) = encode(vals.iter().copied().map(Some));
        let decoded = decode(r, &encoded)
            .into_iter()
            .map(Option::unwrap)
            .collect::<Vec<_>>();
        assert_eq!(decoded, vals);
    }
}
