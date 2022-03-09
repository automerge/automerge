use std::{borrow::Cow, ops::Range};

use crate::types::OpId;

use super::{DecodeColumnError, DeltaDecoder, RleDecoder};

pub(crate) struct OpIdListDecoder<'a> {
    num: RleDecoder<'a, u64>,
    actor: RleDecoder<'a, u64>,
    ctr: DeltaDecoder<'a>,
}

impl<'a> OpIdListDecoder<'a> {
    pub(crate) fn new(
        num: RleDecoder<'a, u64>,
        actor: RleDecoder<'a, u64>,
        ctr: DeltaDecoder<'a>,
    ) -> Self {
        Self { num, actor, ctr }
    }

    /// A decoder which references empty arrays, therefore has no elements
    pub(crate) fn empty() -> OpIdListDecoder<'static> {
        OpIdListDecoder {
            num: RleDecoder::from(Cow::Owned(Vec::new())),
            actor: RleDecoder::from(Cow::Owned(Vec::new())),
            ctr: DeltaDecoder::from(Cow::Owned(Vec::new())),
        }
    }

    pub(crate) fn done(&self) -> bool {
        self.num.done()
    }

    /// Splice new lists of opids into this set of lists of opids, encoding the resulting num, actor and counter
    /// columns in `out`. The result is (num, actor, ctr) where num is the range of the output which
    /// contains the new num column, actor the actor column, and ctr the counter column
    pub(crate) fn splice<'b, I, II, IE>(
        &mut self,
        replace: Range<usize>,
        replace_with: I,
        out: &mut Vec<u8>,
    ) -> (Range<usize>, Range<usize>, Range<usize>)
    where
        II: IntoIterator<Item = OpId, IntoIter=IE>,
        IE: Iterator<Item=OpId> + ExactSizeIterator,
        I: Iterator<Item = II> + Clone,
    {
        let group_replace = group_replace_range(replace.clone(), self.num.clone());
        // first nums
        let num = self.num.splice(
            replace.clone(),
            replace_with.clone().map(|elems| Some(elems.into_iter().len() as u64)),
            out,
        );
        let actor = self.actor.splice(
            group_replace.clone(),
            replace_with
                .clone()
                .flat_map(|elem| elem.into_iter().map(|oid| Some(oid.actor() as u64))),
            out,
        );
        let ctr = self.ctr.splice(
            group_replace,
            replace_with.flat_map(|elem| elem.into_iter().map(|oid| Some(oid.counter() as i64))),
            out,
        );
        (num, actor, ctr)
    }
}

/// Find the replace range for the grouped columns.
fn group_replace_range(replace: Range<usize>, mut num: RleDecoder<u64>) -> Range<usize> {
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

impl<'a> Iterator for OpIdListDecoder<'a> {
    type Item = Result<Vec<OpId>, DecodeColumnError>;

    fn next(&mut self) -> Option<Self::Item> {
        let num = match self.num.next() {
            Some(Some(n)) => n,
            Some(None) => return Some(Err(DecodeColumnError::UnexpectedNull("num".to_string()))),
            None => return None,
        };
        let mut p = Vec::with_capacity(num as usize);
        for _ in 0..num {
            match (self.actor.next(), self.ctr.next()) {
                (Some(Some(a)), Some(Some(ctr))) => match ctr.try_into() {
                    Ok(ctr) => p.push(OpId(ctr, a as usize)),
                    Err(e) => return Some(Err(DecodeColumnError::InvalidValue{
                        column: "counter".to_string(),
                        description: "negative value for counter".to_string(),
                    }))
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

    use crate::columnar_2::rowblock::encoding::properties::{opid, splice_scenario};

    fn encode(opids: Vec<Vec<OpId>>) -> (Vec<u8>, Range<usize>, Range<usize>, Range<usize>) {
        let mut out = Vec::new();
        let mut decoder = OpIdListDecoder::empty();
        let (num, actor, ctr) = decoder.splice(
            0..0,
            opids.into_iter(),
            &mut out,
        );
        (out, num, actor, ctr)
    }

    fn decode(
        buf: &[u8],
        num: Range<usize>,
        actor: Range<usize>,
        ctr: Range<usize>,
    ) -> Vec<Vec<OpId>> {
        let decoder = OpIdListDecoder::new(
            RleDecoder::from(&buf[num]),
            RleDecoder::from(&buf[actor]),
            DeltaDecoder::from(&buf[ctr]),
        );
        decoder.map(|c| c.unwrap()).collect()
    }

    proptest! {
        #[test]
        fn encode_decode_opid_list(opids in propvec(propvec(opid(), 0..100), 0..100)){
            let (encoded, num, actor, ctr) = encode(opids.clone());
            let result = decode(&encoded, num, actor, ctr);
            assert_eq!(opids, result)
        }

        #[test]
        fn splice_opid_list(scenario in splice_scenario(propvec(opid(), 0..100))) {
            let (encoded, num, actor, ctr) = encode(scenario.initial_values.clone());
            let mut decoder = OpIdListDecoder::new(
                RleDecoder::from(&encoded[num]),
                RleDecoder::from(&encoded[actor]),
                DeltaDecoder::from(&encoded[ctr]),
            );
            let mut out = Vec::new();
            let (num, actor, ctr) = decoder.splice(
                scenario.replace_range.clone(),
                scenario.replacements.clone().into_iter(),
                &mut out
                );
            let result = decode(&out[..], num, actor, ctr.clone());
            scenario.check(result);
            assert_eq!(ctr.end, out.len())
        }
    }
}
