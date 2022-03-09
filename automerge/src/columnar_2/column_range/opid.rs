use std::ops::Range;

use super::{DeltaRange, RleRange};
use crate::{
    columnar_2::encoding::{DecodeColumnError, DeltaDecoder, RleDecoder},
    convert,
    types::OpId,
};

#[derive(Debug, Clone)]
pub(crate) struct OpIdRange {
    actor: RleRange<u64>,
    counter: DeltaRange,
}

impl OpIdRange {
    pub(crate) fn new(actor: RleRange<u64>, counter: DeltaRange) -> Self {
        Self { actor, counter }
    }

    pub(crate) fn actor_range(&self) -> &RleRange<u64> {
        &self.actor
    }

    pub(crate) fn counter_range(&self) -> &DeltaRange {
        &self.counter
    }

    pub(crate) fn iter<'a>(&self, data: &'a [u8]) -> OpIdIter<'a> {
        OpIdIter {
            actor: self.actor.decoder(data),
            counter: self.counter.decoder(data),
        }
    }

    pub(crate) fn encode<I, O>(opids: I, out: &mut Vec<u8>) -> Self
    where
        O: convert::OpId<usize>,
        I: Iterator<Item = O> + Clone,
    {
        let actor = RleRange::encode(opids.clone().map(|o| Some(o.actor() as u64)), out);
        let counter = DeltaRange::encode(opids.map(|o| Some(o.counter() as i64)), out);
        Self { actor, counter }
    }

    #[allow(dead_code)]
    pub(crate) fn splice<I, O>(
        &self,
        data: &[u8],
        replace: Range<usize>,
        replace_with: I,
        out: &mut Vec<u8>,
    ) -> Self
    where
        O: convert::OpId<usize>,
        I: Iterator<Item = O> + Clone,
    {
        let actor = self.actor.splice(
            data,
            replace.clone(),
            replace_with.clone().map(|i| Some(i.actor() as u64)),
            out,
        );
        let counter = self.counter.splice(
            data,
            replace,
            replace_with.map(|i| Some(i.counter() as i64)),
            out,
        );
        Self { actor, counter }
    }
}

#[derive(Clone)]
pub(crate) struct OpIdIter<'a> {
    actor: RleDecoder<'a, u64>,
    counter: DeltaDecoder<'a>,
}

impl<'a> OpIdIter<'a> {
    pub(crate) fn done(&self) -> bool {
        self.counter.done()
    }
}

impl<'a> Iterator for OpIdIter<'a> {
    type Item = Result<OpId, DecodeColumnError>;

    fn next(&mut self) -> Option<Self::Item> {
        match (self.actor.next(), self.counter.next()) {
            (Some(Some(a)), Some(Some(c))) => match c.try_into() {
                Ok(c) => Some(Ok(OpId(c, a as usize))),
                Err(_) => Some(Err(DecodeColumnError::InvalidValue {
                    column: "counter".to_string(),
                    description: "negative value encountered".to_string(),
                })),
            },
            (Some(None), _) => Some(Err(DecodeColumnError::UnexpectedNull("actor".to_string()))),
            (_, Some(None)) => Some(Err(DecodeColumnError::UnexpectedNull("actor".to_string()))),
            (Some(_), None) => Some(Err(DecodeColumnError::UnexpectedNull("ctr".to_string()))),
            (None, Some(_)) => Some(Err(DecodeColumnError::UnexpectedNull("actor".to_string()))),
            (None, None) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::columnar_2::encoding::properties::{opid, splice_scenario};
    use proptest::prelude::*;

    fn encode(vals: &[OpId]) -> (Vec<u8>, OpIdRange) {
        let mut out = Vec::new();
        let r = OpIdRange::encode(vals.iter().copied(), &mut out);
        (out, r)
    }

    fn decode(buf: &[u8], range: OpIdRange) -> Vec<OpId> {
        range.iter(buf).map(|c| c.unwrap()).collect()
    }

    proptest! {
        #[test]
        fn encode_decode_opid(opids in proptest::collection::vec(opid(), 0..100)) {
            let (encoded, range) = encode(&opids);
            assert_eq!(opids, decode(&encoded[..], range));
        }

        #[test]
        fn splice_opids(scenario in splice_scenario(opid())) {
            let (encoded, range) = encode(&scenario.initial_values);
            let mut out = Vec::new();
            let new_range = range.splice(
                &encoded,
                scenario.replace_range.clone(),
                scenario.replacements.iter().copied(),
                &mut out
            );
            let result = decode(&out[..], new_range);
            scenario.check(result);
        }
    }
}
