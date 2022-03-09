use std::{borrow::Cow, ops::Range};

use smol_str::SmolStr;

use super::{DecodeColumnError, DeltaDecoder, RleDecoder};
use crate::types::{ElemId, OpId};

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Key {
    Prop(smol_str::SmolStr),
    Elem(ElemId),
}

pub(crate) struct KeyDecoder<'a> {
    actor: RleDecoder<'a, u64>,
    ctr: DeltaDecoder<'a>,
    str: RleDecoder<'a, SmolStr>,
}

impl<'a> KeyDecoder<'a> {
    pub(crate) fn new(
        actor: RleDecoder<'a, u64>,
        ctr: DeltaDecoder<'a>,
        str: RleDecoder<'a, SmolStr>,
    ) -> Self {
        Self { actor, ctr, str }
    }

    pub(crate) fn empty() -> KeyDecoder<'static> {
        KeyDecoder {
            actor: RleDecoder::from(Cow::Owned(Vec::new())),
            ctr: DeltaDecoder::from(Cow::Owned(Vec::new())),
            str: RleDecoder::from(Cow::Owned(Vec::new())),
        }
    }

    pub(crate) fn done(&self) -> bool {
        self.actor.done() && self.ctr.done() && self.str.done()
    }

    /// Splice new keys into this set of keys, encoding the resulting actor, counter, and str
    /// columns in `out`. The result is (actor, ctr, str) where actor is the range of the output which
    /// contains the new actor column, ctr the counter column, and str the str column.
    pub(crate) fn splice<'b, I: Iterator<Item = &'b Key> + Clone>(
        &mut self,
        replace: Range<usize>,
        replace_with: I,
        out: &mut Vec<u8>,
    ) -> (Range<usize>, Range<usize>, Range<usize>) {
        panic!()
    }
}

impl<'a> Iterator for KeyDecoder<'a> {
    type Item = Result<Key, DecodeColumnError>;

    fn next(&mut self) -> Option<Self::Item> {
        match (self.actor.next(), self.ctr.next(), self.str.next()) {
            (Some(Some(_)), Some(Some(_)), Some(Some(_))) => {
                Some(Err(DecodeColumnError::InvalidValue {
                    column: "key".to_string(),
                    description: "too many values".to_string(),
                }))
            }
            (Some(None), Some(None), Some(Some(string))) => Some(Ok(Key::Prop(string))),
            (Some(None), Some(Some(0)), Some(None)) => Some(Ok(Key::Elem(ElemId(OpId(0, 0))))),
            (Some(Some(actor)), Some(Some(ctr)), Some(None)) => match ctr.try_into() {
                Ok(ctr) => Some(Ok(Key::Elem(ElemId(OpId(ctr, actor as usize))))),
                Err(e) => Some(Err(DecodeColumnError::InvalidValue{
                    column: "counter".to_string(),
                    description: "negative value for counter".to_string(),
                })),
            }
            (None, None, None) => None,
            (None | Some(None), _, _) => {
                Some(Err(DecodeColumnError::UnexpectedNull("actor".to_string())))
            }
            (_, None | Some(None), _) => {
                Some(Err(DecodeColumnError::UnexpectedNull("ctr".to_string())))
            }
            (_, _, None) => Some(Err(DecodeColumnError::UnexpectedNull("str".to_string()))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn encode(vals: &[Key]) -> (Vec<u8>, Range<usize>, Range<usize>, Range<usize>) {
        let mut out = Vec::new();
        let mut decoder = KeyDecoder::empty();
        let (actor, ctr, string) = decoder.splice(0..0, vals.iter(), &mut out);
        (out, actor, ctr, string)
    }

    //proptest! {
    //#[test]
    //fn splice_key(scenario in splice_scenario(row_op_key())) {
    //let (buf, actor, ctr, string) = encode(&scenario.initial_values[..]);
    //let mut decoder = KeyDecoder::new(
    //RleDecoder::from(&buf[actor]),
    //DeltaDecoder::from(&buf[ctr]),
    //RleDecoder::from(&buf[string]),
    //);
    //let mut out = Vec::new();
    //let (actor, ctr, string) = decoder.splice(scenario.replace_range.clone(), scenario.replacements.iter(), &mut out);
    //let decoder = KeyDecoder::new(
    //RleDecoder::from(&buf[actor]),
    //DeltaDecoder::from(&buf[ctr]),
    //RleDecoder::from(&buf[string.clone()]),
    //);
    //let result = decoder.map(|c| c.unwrap()).collect();
    //scenario.check(result);
    //assert_eq!(string.end, out.len());
    //}
    //}
}
