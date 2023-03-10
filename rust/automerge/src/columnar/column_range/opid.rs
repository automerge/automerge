use std::ops::Range;

use super::{DeltaRange, RleRange};
use crate::{
    columnar::{
        encoding::{
            raw, DecodeColumnError, DeltaDecoder, DeltaEncoder, RleDecoder, RleEncoder, Sink,
        },
        SpliceError,
    },
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
    pub(crate) fn splice<I, E, O>(
        &self,
        data: &[u8],
        replace: Range<usize>,
        replace_with: I,
        out: &mut Vec<u8>,
    ) -> Result<Self, SpliceError<raw::Error, E>>
    where
        O: convert::OpId<usize>,
        E: std::error::Error,
        I: Iterator<Item = Result<O, E>> + Clone,
    {
        let actor = self.actor.splice(
            data,
            replace.clone(),
            replace_with
                .clone()
                .map(|i| i.map(|i| Some(i.actor() as u64))),
            out,
        )?;
        let counter = self.counter.splice(
            data,
            replace,
            replace_with.map(|i| i.map(|i| Some(i.counter() as i64))),
            out,
        )?;
        Ok(Self { actor, counter })
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

impl<'a> OpIdIter<'a> {
    fn try_next(&mut self) -> Result<Option<OpId>, DecodeColumnError> {
        let actor = self
            .actor
            .next()
            .transpose()
            .map_err(|e| DecodeColumnError::decode_raw("actor", e))?;
        let counter = self
            .counter
            .next()
            .transpose()
            .map_err(|e| DecodeColumnError::decode_raw("counter", e))?;
        match (actor, counter) {
            (Some(Some(a)), Some(Some(c))) => match u32::try_from(c) {
                Ok(c) => Ok(Some(OpId::new(c as u64, a as usize))),
                Err(_) => Err(DecodeColumnError::invalid_value(
                    "counter",
                    "negative or large value encountered",
                )),
            },
            (Some(None), _) => Err(DecodeColumnError::unexpected_null("actor")),
            (_, Some(None)) => Err(DecodeColumnError::unexpected_null("actor")),
            (Some(_), None) => Err(DecodeColumnError::unexpected_null("ctr")),
            (None, Some(_)) => Err(DecodeColumnError::unexpected_null("actor")),
            (None, None) => Ok(None),
        }
    }
}

impl<'a> Iterator for OpIdIter<'a> {
    type Item = Result<OpId, DecodeColumnError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

pub(crate) struct OpIdEncoder<S> {
    actor: RleEncoder<S, u64>,
    counter: DeltaEncoder<S>,
}

impl<S: Sink> OpIdEncoder<S> {
    pub(crate) fn append<O: convert::OpId<usize>>(&mut self, opid: O) {
        self.actor.append_value(opid.actor() as u64);
        self.counter.append_value(opid.counter() as i64);
    }
}

impl OpIdEncoder<Vec<u8>> {
    pub(crate) fn new() -> Self {
        Self {
            actor: RleEncoder::from(Vec::new()),
            counter: DeltaEncoder::from(Vec::new()),
        }
    }

    pub(crate) fn finish(self, out: &mut Vec<u8>) -> OpIdRange {
        let start = out.len();
        let (actor, _) = self.actor.finish();
        out.extend(actor);
        let actor_end = out.len();

        let (counter, _) = self.counter.finish();
        out.extend(counter);
        let counter_end = out.len();

        OpIdRange {
            actor: (start..actor_end).into(),
            counter: (actor_end..counter_end).into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        columnar::encoding::properties::{opid, splice_scenario},
        types::OpId,
    };
    use proptest::prelude::*;
    use std::convert::Infallible;

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
            let replacements: Vec<Result<OpId, Infallible>> = scenario.replacements.iter().cloned().map(Ok).collect();
            let new_range = range.splice(
                &encoded,
                scenario.replace_range.clone(),
                replacements.into_iter(),
                &mut out
            ).unwrap();
            let result = decode(&out[..], new_range);
            scenario.check(result);
        }
    }
}
