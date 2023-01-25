use std::{convert::Infallible, ops::Range};

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

/// A collection of ranges which decode to lists of OpIds
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct OpIdListRange {
    num: RleRange<u64>,
    actor: RleRange<u64>,
    counter: DeltaRange,
}

impl OpIdListRange {
    pub(crate) fn new(num: RleRange<u64>, actor: RleRange<u64>, counter: DeltaRange) -> Self {
        Self {
            num,
            actor,
            counter,
        }
    }

    pub(crate) fn group_range(&self) -> &RleRange<u64> {
        &self.num
    }

    pub(crate) fn actor_range(&self) -> &RleRange<u64> {
        &self.actor
    }

    pub(crate) fn counter_range(&self) -> &DeltaRange {
        &self.counter
    }

    pub(crate) fn iter<'a>(&self, data: &'a [u8]) -> OpIdListIter<'a> {
        OpIdListIter {
            num: self.num.decoder(data),
            actor: self.actor.decoder(data),
            counter: self.counter.decoder(data),
        }
    }

    pub(crate) fn encode<O, I, II, IE>(opids: I, out: &mut Vec<u8>) -> Self
    where
        O: convert::OpId<usize>,
        II: IntoIterator<Item = O, IntoIter = IE>,
        IE: Iterator<Item = O> + ExactSizeIterator,
        I: Iterator<Item = II> + Clone,
    {
        let num = RleRange::encode(
            opids.clone().map(|os| Some(os.into_iter().len() as u64)),
            out,
        );
        let actor = RleRange::encode(
            opids
                .clone()
                .flat_map(|os| os.into_iter().map(|o| Some(o.actor() as u64))),
            out,
        );
        let counter = DeltaRange::encode(
            opids.flat_map(|os| os.into_iter().map(|o| Some(o.counter() as i64))),
            out,
        );
        Self {
            num,
            actor,
            counter,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn splice<I, II, IE, R>(
        &self,
        data: &[u8],
        replace: Range<usize>,
        replace_with: I,
        out: &mut Vec<u8>,
    ) -> Result<Self, SpliceError<raw::Error, R>>
    where
        R: std::error::Error + Clone,
        II: IntoIterator<Item = OpId, IntoIter = IE>,
        IE: Iterator<Item = OpId> + ExactSizeIterator,
        I: Iterator<Item = Result<II, R>> + Clone,
    {
        let group_replace = group_replace_range(replace.clone(), self.num.decoder(data))
            .map_err(|e| e.existing())?;
        let num = self.num.splice(
            data,
            replace,
            replace_with
                .clone()
                .map(|elems| elems.map(|elems| Some(elems.into_iter().len() as u64))),
            out,
        )?;
        let actor = self.actor.splice(
            data,
            group_replace.clone(),
            replace_with.clone().flat_map(|elem| match elem {
                Err(e) => SplicingIter::Failed(e),
                Ok(i) => SplicingIter::Iter(i.into_iter(), |oid: OpId| oid.actor() as u64),
            }),
            out,
        )?;
        let counter = self.counter.splice(
            data,
            group_replace,
            replace_with.flat_map(|elem| match elem {
                Err(e) => SplicingIter::Failed(e),
                Ok(i) => SplicingIter::Iter(i.into_iter(), |oid: OpId| oid.counter() as i64),
            }),
            out,
        )?;
        Ok(Self {
            num,
            actor,
            counter,
        })
    }
}

enum SplicingIter<E, I, F> {
    Failed(E),
    Iter(I, F),
}

impl<E, I, F, U> Iterator for SplicingIter<E, I, F>
where
    E: std::error::Error + Clone,
    I: Iterator<Item = OpId>,
    F: Fn(OpId) -> U,
{
    type Item = Result<Option<U>, E>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Failed(e) => Some(Err(e.clone())),
            Self::Iter(i, f) => i.next().map(|oid| Ok(Some(f(oid)))),
        }
    }
}

/// Find the replace range for the grouped columns.
fn group_replace_range(
    replace: Range<usize>,
    mut num: RleDecoder<'_, u64>,
) -> Result<Range<usize>, SpliceError<raw::Error, Infallible>> {
    let mut idx = 0;
    let mut grouped_replace_start: usize = 0;
    let mut grouped_replace_len: usize = 0;
    while idx < replace.start {
        if let Some(Some(count)) = num.next().transpose().map_err(SpliceError::ReadExisting)? {
            grouped_replace_start += count as usize;
        }
        idx += 1;
    }
    for _ in 0..replace.len() {
        if let Some(Some(count)) = num.next().transpose().map_err(SpliceError::ReadExisting)? {
            grouped_replace_len += count as usize;
        }
    }
    Ok(grouped_replace_start..(grouped_replace_start + grouped_replace_len))
}

#[derive(Clone)]
pub(crate) struct OpIdListIter<'a> {
    num: RleDecoder<'a, u64>,
    actor: RleDecoder<'a, u64>,
    counter: DeltaDecoder<'a>,
}

impl<'a> OpIdListIter<'a> {
    fn try_next(&mut self) -> Result<Option<Vec<OpId>>, DecodeColumnError> {
        let num = match self
            .num
            .next()
            .transpose()
            .map_err(|e| DecodeColumnError::decode_raw("num", e))?
        {
            Some(Some(n)) => n,
            Some(None) => return Err(DecodeColumnError::unexpected_null("num")),
            None => return Ok(None),
        };

        // We cannot trust `num` because it is provided over the network,
        // but in the common case it will be correct and small (so we
        // use with_capacity to make sure the vector is precisely the right
        // size).
        let mut p = Vec::with_capacity(std::cmp::min(num, 100) as usize);
        for _ in 0..num {
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
                (Some(Some(a)), Some(Some(ctr))) => match ctr.try_into() {
                    Ok(ctr) => p.push(OpId::new(ctr, a as usize)),
                    Err(_e) => {
                        return Err(DecodeColumnError::invalid_value(
                            "counter",
                            "negative value for counter",
                        ))
                    }
                },
                (Some(None) | None, _) => return Err(DecodeColumnError::unexpected_null("actor")),
                (_, Some(None) | None) => {
                    return Err(DecodeColumnError::unexpected_null("counter"))
                }
            }
        }
        Ok(Some(p))
    }
}

impl<'a> Iterator for OpIdListIter<'a> {
    type Item = Result<Vec<OpId>, DecodeColumnError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

pub(crate) struct OpIdListEncoder<S> {
    num: RleEncoder<S, u64>,
    actor: RleEncoder<S, u64>,
    counter: DeltaEncoder<S>,
}

impl<S: Sink> OpIdListEncoder<S> {
    pub(crate) fn append<I, O>(&mut self, ids: I)
    where
        I: Iterator<Item = O> + ExactSizeIterator,
        O: convert::OpId<usize>,
    {
        self.num.append_value(ids.len() as u64);
        for id in ids {
            self.actor.append_value(id.actor() as u64);
            self.counter.append_value(id.counter() as i64);
        }
    }
}

impl OpIdListEncoder<Vec<u8>> {
    pub(crate) fn new() -> Self {
        Self {
            num: RleEncoder::from(Vec::new()),
            actor: RleEncoder::from(Vec::new()),
            counter: DeltaEncoder::from(Vec::new()),
        }
    }

    pub(crate) fn finish(self, out: &mut Vec<u8>) -> OpIdListRange {
        let start = out.len();
        let (num, _) = self.num.finish();
        out.extend(num);
        let num_end = out.len();

        let (actor, _) = self.actor.finish();
        out.extend(actor);
        let actor_end = out.len();

        let (counter, _) = self.counter.finish();
        out.extend(counter);
        let counter_end = out.len();

        OpIdListRange {
            num: (start..num_end).into(),
            actor: (num_end..actor_end).into(),
            counter: (actor_end..counter_end).into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::collection::vec as propvec;
    use proptest::prelude::*;

    use crate::columnar::encoding::properties::{opid, splice_scenario};

    fn encode(opids: Vec<Vec<OpId>>) -> (OpIdListRange, Vec<u8>) {
        let mut out = Vec::new();
        let range = OpIdListRange::encode(opids.iter(), &mut out);
        (range, out)
    }

    fn decode(range: OpIdListRange, buf: &[u8]) -> Vec<Vec<OpId>> {
        range.iter(buf).map(|c| c.unwrap()).collect()
    }

    proptest! {
        #[test]
        fn encode_decode_opid_list(opids in propvec(propvec(opid(), 0..100), 0..100)){
            let (range, encoded) = encode(opids.clone());
            let result = decode(range, &encoded);
            assert_eq!(opids, result)
        }

        #[test]
        fn splice_opid_list(scenario in splice_scenario(propvec(opid(), 0..100))) {
            let (range, encoded) = encode(scenario.initial_values.clone());
            let mut out = Vec::new();
            let replacements: Vec<Result<Vec<OpId>, Infallible>> = scenario.replacements.iter().cloned().map(Ok).collect();
            let new_range = range.splice(
                &encoded,
                scenario.replace_range.clone(),
                replacements.into_iter(),
                &mut out
                ).unwrap();
            let result = decode(new_range, &out[..]);
            scenario.check(result);
        }
    }
}
