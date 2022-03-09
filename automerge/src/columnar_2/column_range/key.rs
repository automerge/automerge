use std::ops::Range;

use super::{DeltaRange, RleRange};
use crate::{
    columnar_2::encoding::{DecodeColumnError, DeltaDecoder, RleDecoder},
    convert,
    types::{ElemId, OpId},
};

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Key {
    Prop(smol_str::SmolStr),
    Elem(ElemId),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct KeyRange {
    actor: RleRange<u64>,
    counter: DeltaRange,
    string: RleRange<smol_str::SmolStr>,
}

impl KeyRange {
    pub(crate) fn new(
        actor: RleRange<u64>,
        counter: DeltaRange,
        string: RleRange<smol_str::SmolStr>,
    ) -> Self {
        Self {
            actor,
            counter,
            string,
        }
    }

    pub(crate) fn actor_range(&self) -> &RleRange<u64> {
        &self.actor
    }

    pub(crate) fn counter_range(&self) -> &DeltaRange {
        &self.counter
    }

    pub(crate) fn string_range(&self) -> &RleRange<smol_str::SmolStr> {
        &self.string
    }

    pub(crate) fn iter<'a>(&self, data: &'a [u8]) -> KeyIter<'a> {
        KeyIter {
            actor: self.actor.decoder(data),
            counter: self.counter.decoder(data),
            string: self.string.decoder(data),
        }
    }

    pub(crate) fn encode<'b, O, I: Iterator<Item = convert::Key<'b, O>> + Clone>(
        items: I,
        out: &mut Vec<u8>,
    ) -> Self
    where
        O: convert::OpId<usize>,
    {
        Self {
            actor: (0..0).into(),
            counter: (0..0).into(),
            string: (0..0).into(),
        }
        .splice(&[], 0..0, items, out)
    }

    /// Splice new keys into this set of keys, encoding the resulting actor, counter, and str
    /// columns in `out`.
    pub(crate) fn splice<'b, O, I: Iterator<Item = convert::Key<'b, O>> + Clone>(
        &mut self,
        data: &[u8],
        replace: Range<usize>,
        replace_with: I,
        out: &mut Vec<u8>,
    ) -> Self
    where
        O: convert::OpId<usize>,
    {
        let actor = self.actor.splice(
            data,
            replace.clone(),
            replace_with.clone().map(|k| match k {
                convert::Key::Prop(_) => None,
                convert::Key::Elem(convert::ElemId::Head) => None,
                convert::Key::Elem(convert::ElemId::Op(o)) => Some(o.actor() as u64),
            }),
            out,
        );

        let counter = self.counter.splice(
            data,
            replace.clone(),
            replace_with.clone().map(|k| match k {
                convert::Key::Prop(_) => None,
                convert::Key::Elem(convert::ElemId::Head) => Some(0),
                convert::Key::Elem(convert::ElemId::Op(o)) => Some(o.counter() as i64),
            }),
            out,
        );

        let string = self.string.splice(
            data,
            replace,
            replace_with.map(|k| match k {
                convert::Key::Prop(s) => Some(s),
                convert::Key::Elem(_) => None,
            }),
            out,
        );

        Self {
            actor,
            counter,
            string,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct KeyIter<'a> {
    actor: RleDecoder<'a, u64>,
    counter: DeltaDecoder<'a>,
    string: RleDecoder<'a, smol_str::SmolStr>,
}

impl<'a> Iterator for KeyIter<'a> {
    type Item = Result<Key, DecodeColumnError>;

    fn next(&mut self) -> Option<Self::Item> {
        match (self.actor.next(), self.counter.next(), self.string.next()) {
            (Some(Some(_)), Some(Some(_)), Some(Some(_))) => {
                Some(Err(DecodeColumnError::InvalidValue {
                    column: "key".to_string(),
                    description: "too many values".to_string(),
                }))
            }
            (Some(None) | None, Some(None) | None, Some(Some(string))) => {
                Some(Ok(Key::Prop(string)))
            }
            (Some(None) | None, Some(Some(0)), Some(None) | None) => {
                Some(Ok(Key::Elem(ElemId(OpId(0, 0)))))
            }
            (Some(Some(actor)), Some(Some(ctr)), Some(None) | None) => match ctr.try_into() {
                //Ok(ctr) => Some(Ok(Key::Elem(ElemId(OpId(ctr, actor as usize))))),
                Ok(ctr) => Some(Ok(Key::Elem(ElemId(OpId::new(actor as usize, ctr))))),
                Err(_) => Some(Err(DecodeColumnError::InvalidValue {
                    column: "counter".to_string(),
                    description: "negative value for counter".to_string(),
                })),
            },
            (None | Some(None), None | Some(None), None | Some(None)) => None,
            (None | Some(None), k, _) => {
                tracing::error!(key=?k, "unexpected null actor");
                Some(Err(DecodeColumnError::UnexpectedNull("actor".to_string())))
            }
            (_, None | Some(None), _) => {
                Some(Err(DecodeColumnError::UnexpectedNull("ctr".to_string())))
            }
        }
    }
}
