use std::ops::Range;

use super::{DeltaRange, RleRange};
use crate::{
    columnar_2::encoding::{DecodeColumnError, DeltaDecoder, RleDecoder},
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
    pub(crate) fn splice<I, II, IE>(
        &self,
        data: &[u8],
        replace: Range<usize>,
        replace_with: I,
        out: &mut Vec<u8>,
    ) -> Self
    where
        II: IntoIterator<Item = OpId, IntoIter = IE>,
        IE: Iterator<Item = OpId> + ExactSizeIterator,
        I: Iterator<Item = II> + Clone,
    {
        let group_replace = group_replace_range(replace.clone(), self.num.decoder(data));
        // first nums
        let num = self.num.splice(
            data,
            replace,
            replace_with
                .clone()
                .map(|elems| Some(elems.into_iter().len() as u64)),
            out,
        );
        let actor = self.actor.splice(
            data,
            group_replace.clone(),
            replace_with
                .clone()
                .flat_map(|elem| elem.into_iter().map(|oid| Some(oid.actor() as u64))),
            out,
        );
        let counter = self.counter.splice(
            data,
            group_replace,
            replace_with.flat_map(|elem| elem.into_iter().map(|oid| Some(oid.counter() as i64))),
            out,
        );
        Self {
            num,
            actor,
            counter,
        }
    }
}

/// Find the replace range for the grouped columns.
fn group_replace_range(replace: Range<usize>, mut num: RleDecoder<'_, u64>) -> Range<usize> {
    let mut idx = 0;
    let mut grouped_replace_start: usize = 0;
    let mut grouped_replace_len: usize = 0;
    while idx < replace.start {
        if let Some(Some(count)) = num.next() {
            grouped_replace_start += count as usize;
        }
        idx += 1;
    }
    for _ in 0..replace.len() {
        if let Some(Some(count)) = num.next() {
            grouped_replace_len += count as usize;
        }
    }
    grouped_replace_start..(grouped_replace_start + grouped_replace_len)
}

#[derive(Clone)]
pub(crate) struct OpIdListIter<'a> {
    num: RleDecoder<'a, u64>,
    actor: RleDecoder<'a, u64>,
    counter: DeltaDecoder<'a>,
}

impl<'a> Iterator for OpIdListIter<'a> {
    type Item = Result<Vec<OpId>, DecodeColumnError>;

    fn next(&mut self) -> Option<Self::Item> {
        let num = match self.num.next() {
            Some(Some(n)) => n,
            Some(None) => return Some(Err(DecodeColumnError::UnexpectedNull("num".to_string()))),
            None => return None,
        };
        let mut p = Vec::with_capacity(num as usize);
        for _ in 0..num {
            match (self.actor.next(), self.counter.next()) {
                (Some(Some(a)), Some(Some(ctr))) => match ctr.try_into() {
                    Ok(ctr) => p.push(OpId(ctr, a as usize)),
                    Err(_e) => {
                        return Some(Err(DecodeColumnError::InvalidValue {
                            column: "counter".to_string(),
                            description: "negative value for counter".to_string(),
                        }))
                    }
                },
                (Some(None) | None, _) => {
                    return Some(Err(DecodeColumnError::UnexpectedNull("actor".to_string())))
                }
                (_, Some(None) | None) => {
                    return Some(Err(DecodeColumnError::UnexpectedNull("ctr".to_string())))
                }
            }
        }
        Some(Ok(p))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::collection::vec as propvec;
    use proptest::prelude::*;

    use crate::columnar_2::encoding::properties::{opid, splice_scenario};

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
            let new_range = range.splice(
                &encoded,
                scenario.replace_range.clone(),
                scenario.replacements.clone().into_iter(),
                &mut out
                );
            let result = decode(new_range, &out[..]);
            scenario.check(result);
        }
    }
}
