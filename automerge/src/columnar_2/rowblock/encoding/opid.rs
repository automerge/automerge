use std::{borrow::Cow, ops::Range};

use crate::types::OpId;

use super::{DecodeColumnError, DeltaDecoder, RleDecoder};

pub(crate) struct OpIdDecoder<'a> {
    actor: RleDecoder<'a, u64>,
    ctr: DeltaDecoder<'a>,
}

impl Default for OpIdDecoder<'static> {
    fn default() -> Self {
        Self::new(
            RleDecoder::from(Cow::Owned(Vec::new())),
            DeltaDecoder::from(Cow::Owned(Vec::new())),
        )
    }
}

impl<'a> OpIdDecoder<'a> {
    pub(crate) fn new(actor: RleDecoder<'a, u64>, ctr: DeltaDecoder<'a>) -> Self {
        Self { actor, ctr }
    }

    pub(crate) fn done(&self) -> bool {
        self.actor.done() && self.ctr.done()
    }

    /// Splice new operations into this set of operations, encoding the resulting actor and counter
    /// columns in `out`. The result is (actor, ctr) where actor is the range of the output which
    /// contains the new actor column and ctr the counter column.
    pub(crate) fn splice<'b, I: Iterator<Item = &'b OpId> + Clone>(
        &mut self,
        replace: Range<usize>,
        replace_with: I,
        out: &mut Vec<u8>,
    ) -> (Range<usize>, Range<usize>) {
        // first splice actors, then counters
        let actor = self.actor.splice(
            replace.clone(),
            replace_with.clone().map(|i| Some(i.actor() as u64)),
            out,
        );
        let counter = self
            .ctr
            .splice(replace, replace_with.map(|i| Some(i.counter() as i64)), out);
        (actor, counter)
    }
}

impl<'a> Iterator for OpIdDecoder<'a> {
    type Item = Result<OpId, DecodeColumnError>;

    fn next(&mut self) -> Option<Self::Item> {
        match (self.actor.next(), self.ctr.next()) {
            (Some(Some(a)), Some(Some(c))) => match c.try_into() {
                Ok(c) => Some(Ok(OpId(c, a as usize))),
                Err(e) => Some(Err(DecodeColumnError::InvalidValue{
                    column: "counter".to_string(),
                    description: "negative value encountered".to_string(),
                }))
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
    use crate::columnar_2::rowblock::encoding::properties::{opid, splice_scenario};
    use proptest::prelude::*;

    fn encode(vals: &[OpId]) -> (Vec<u8>, Range<usize>, Range<usize>) {
        let mut out = Vec::new();
        let mut decoder = OpIdDecoder::default();
        let (actor, ctr) = decoder.splice(0..0, vals.into_iter(), &mut out);
        (out, actor, ctr)
    }

    fn decode(buf: &[u8], actor: Range<usize>, ctr: Range<usize>) -> Vec<OpId> {
        OpIdDecoder::new(RleDecoder::from(&buf[actor]), DeltaDecoder::from(&buf[ctr]))
            .map(|c| c.unwrap())
            .collect()
    }

    proptest! {
        #[test]
        fn encode_decode_opid(opids in proptest::collection::vec(opid(), 0..100)) {
            let (encoded, actor, ctr) = encode(&opids);
            assert_eq!(opids, decode(&encoded[..], actor, ctr));
        }

        #[test]
        fn splice_opids(scenario in splice_scenario(opid())) {
            let (encoded, actor, ctr) = encode(&scenario.initial_values);
            let mut decoder = OpIdDecoder::new(RleDecoder::from(&encoded[actor]), DeltaDecoder::from(&encoded[ctr]));
            let mut out = Vec::new();
            let (actor, ctr) = decoder.splice(scenario.replace_range.clone(), scenario.replacements.iter(), &mut out);
            let result = decode(&out[..], actor, ctr.clone());
            scenario.check(result);
            assert_eq!(ctr.end, out.len());
        }
    }
}
