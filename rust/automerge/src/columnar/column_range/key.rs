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
        // SAFETY: The incoming iterator is infallible and there are no existing items
        Self {
            actor: (0..0).into(),
            counter: (0..0).into(),
            string: (0..0).into(),
        }
        .splice::<_, Infallible, _>(&[], 0..0, items.map(Ok), out)
        .unwrap()
    }

    /// Splice new keys into this set of keys, encoding the resulting actor, counter, and str
    /// columns in `out`.
    pub(crate) fn splice<'b, O, E, I>(
        &mut self,
        data: &[u8],
        replace: Range<usize>,
        replace_with: I,
        out: &mut Vec<u8>,
    ) -> Result<Self, SpliceError<raw::Error, E>>
    where
        O: convert::OpId<usize>,
        E: std::error::Error,
        I: Iterator<Item = Result<convert::Key<'b, O>, E>> + Clone,
    {
        let actor = self.actor.splice(
            data,
            replace.clone(),
            replace_with.clone().map(|k| {
                k.map(|k| match k {
                    convert::Key::Prop(_) => None,
                    convert::Key::Elem(convert::ElemId::Head) => None,
                    convert::Key::Elem(convert::ElemId::Op(o)) => Some(o.actor() as u64),
                })
            }),
            out,
        )?;

        let counter = self.counter.splice(
            data,
            replace.clone(),
            replace_with.clone().map(|k| {
                k.map(|k| match k {
                    convert::Key::Prop(_) => None,
                    convert::Key::Elem(convert::ElemId::Head) => Some(0),
                    convert::Key::Elem(convert::ElemId::Op(o)) => Some(o.counter() as i64),
                })
            }),
            out,
        )?;

        let string = self.string.splice(
            data,
            replace,
            replace_with.map(|k| {
                k.map(|k| match k {
                    convert::Key::Prop(s) => Some(s),
                    convert::Key::Elem(_) => None,
                })
            }),
            out,
        )?;

        Ok(Self {
            actor,
            counter,
            string,
        })
    }
}

#[derive(Clone, Debug)]
pub(crate) struct KeyIter<'a> {
    actor: RleDecoder<'a, u64>,
    counter: DeltaDecoder<'a>,
    string: RleDecoder<'a, smol_str::SmolStr>,
}

impl<'a> KeyIter<'a> {
    fn try_next(&mut self) -> Result<Option<Key>, DecodeColumnError> {
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
        let string = self
            .string
            .next()
            .transpose()
            .map_err(|e| DecodeColumnError::decode_raw("string", e))?;
        match (actor, counter, string) {
            (Some(Some(_)), Some(Some(_)), Some(Some(_))) => {
                Err(DecodeColumnError::invalid_value("key", "too many values"))
            }
            (Some(None) | None, Some(None) | None, Some(Some(string))) => {
                Ok(Some(Key::Prop(string)))
            }
            (Some(None) | None, Some(Some(0)), Some(None) | None) => {
                Ok(Some(Key::Elem(ElemId(OpId::new(0, 0)))))
            }
            (Some(Some(actor)), Some(Some(ctr)), Some(None) | None) => match ctr.try_into() {
                //Ok(ctr) => Some(Ok(Key::Elem(ElemId(OpId(ctr, actor as usize))))),
                Ok(ctr) => Ok(Some(Key::Elem(ElemId(OpId::new(ctr, actor as usize))))),
                Err(_) => Err(DecodeColumnError::invalid_value(
                    "counter",
                    "negative value for counter",
                )),
            },
            (None | Some(None), None | Some(None), None | Some(None)) => Ok(None),
            (None | Some(None), k, _) => {
                tracing::error!(key=?k, "unexpected null actor");
                Err(DecodeColumnError::unexpected_null("actor"))
            }
            (_, None | Some(None), _) => Err(DecodeColumnError::unexpected_null("counter")),
        }
    }
}

impl<'a> Iterator for KeyIter<'a> {
    type Item = Result<Key, DecodeColumnError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

pub(crate) struct KeyEncoder<S> {
    actor: RleEncoder<S, u64>,
    counter: DeltaEncoder<S>,
    string: RleEncoder<S, smol_str::SmolStr>,
}

impl KeyEncoder<Vec<u8>> {
    pub(crate) fn new() -> KeyEncoder<Vec<u8>> {
        KeyEncoder {
            actor: RleEncoder::new(Vec::new()),
            counter: DeltaEncoder::new(Vec::new()),
            string: RleEncoder::new(Vec::new()),
        }
    }

    pub(crate) fn finish(self, out: &mut Vec<u8>) -> KeyRange {
        let actor_start = out.len();
        let (actor, _) = self.actor.finish();
        out.extend(actor);
        let actor_end = out.len();

        let (counter, _) = self.counter.finish();
        out.extend(counter);
        let counter_end = out.len();

        let (string, _) = self.string.finish();
        out.extend(string);
        let string_end = out.len();

        KeyRange {
            actor: (actor_start..actor_end).into(),
            counter: (actor_end..counter_end).into(),
            string: (counter_end..string_end).into(),
        }
    }
}

impl<S: Sink> KeyEncoder<S> {
    pub(crate) fn append<O>(&mut self, key: convert::Key<'_, O>)
    where
        O: convert::OpId<usize>,
    {
        match key {
            convert::Key::Prop(p) => {
                self.string.append_value(p.clone());
                self.actor.append_null();
                self.counter.append_null();
            }
            convert::Key::Elem(convert::ElemId::Head) => {
                self.string.append_null();
                self.actor.append_null();
                self.counter.append_value(0);
            }
            convert::Key::Elem(convert::ElemId::Op(o)) => {
                self.string.append_null();
                self.actor.append_value(o.actor() as u64);
                self.counter.append_value(o.counter() as i64);
            }
        }
    }
}
