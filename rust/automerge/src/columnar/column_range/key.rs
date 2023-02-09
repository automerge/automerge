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
pub(crate) struct ElemRange {
    actor: RleRange<u64>,
    counter: DeltaRange,
}

impl ElemRange {
    pub(crate) fn new(actor: RleRange<u64>, counter: DeltaRange) -> Self {
        Self { actor, counter }
    }

    pub(crate) fn actor_range(&self) -> &RleRange<u64> {
        &self.actor
    }

    pub(crate) fn counter_range(&self) -> &DeltaRange {
        &self.counter
    }

    //    pub(crate) fn string_range(&self) -> &RleRange<smol_str::SmolStr> {
    //        &self.string
    //    }

    pub(crate) fn iter<'a>(&self, data: &'a [u8]) -> ElemIter<'a> {
        ElemIter {
            actor: self.actor.decoder(data),
            counter: self.counter.decoder(data),
        }
    }

/*
    pub(crate) fn encode<'b, O, I: Iterator<Item = convert::ElemId<O>> + Clone>(
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
        }
        .splice::<_, Infallible, _>(&[], 0..0, items.map(Ok), out)
        .unwrap()
    }
*/

    pub(crate) fn maybe_encode<'b, O, I: Iterator<Item = Option<convert::ElemId<O>>> + Clone>(
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
        I: Iterator<Item = Result<Option<convert::ElemId<O>>, E>> + Clone,
    {
        let actor = self.actor.splice(
            data,
            replace.clone(),
            replace_with.clone().map(|k| {
                k.map(|k| match k {
                    Some(convert::ElemId::Head) => None,
                    Some(convert::ElemId::Op(o)) => Some(o.actor() as u64),
                    None => None,
                })
            }),
            out,
        )?;

        let counter = self.counter.splice(
            data,
            replace.clone(),
            replace_with.clone().map(|k| {
                k.map(|k| match k {
                    Some(convert::ElemId::Head) => Some(0),
                    Some(convert::ElemId::Op(o)) => Some(o.counter() as i64),
                    None => None,
                })
            }),
            out,
        )?;

        Ok(Self { actor, counter })
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ElemIter<'a> {
    actor: RleDecoder<'a, u64>,
    counter: DeltaDecoder<'a>,
}

impl<'a> ElemIter<'a> {
    fn try_next(&mut self) -> Result<Option<Option<ElemId>>, DecodeColumnError> {
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
            (Some(None) | None, Some(Some(0))) => Ok(Some(Some(ElemId(OpId::new(0, 0))))),
            (Some(Some(actor)), Some(Some(ctr))) => match ctr.try_into() {
                Ok(ctr) => Ok(Some(Some(ElemId(OpId::new(ctr, actor as usize))))),
                Err(_) => Err(DecodeColumnError::invalid_value(
                    "counter",
                    "negative value for counter",
                )),
            },
            (Some(None), Some(None)) => Ok(Some(None)),
            (None, None) => Ok(None),
            (None | Some(None), k) => {
                tracing::error!(key=?k, "unexpected null actor");
                Err(DecodeColumnError::unexpected_null("actor"))
            }
            (_, None | Some(None)) => Err(DecodeColumnError::unexpected_null("counter")),
        }
    }
}

impl<'a> Iterator for ElemIter<'a> {
    type Item = Result<Option<ElemId>, DecodeColumnError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

pub(crate) struct ElemEncoder<S> {
    actor: RleEncoder<S, u64>,
    counter: DeltaEncoder<S>,
    //string: RleEncoder<S, smol_str::SmolStr>,
}

impl ElemEncoder<Vec<u8>> {
    pub(crate) fn new() -> ElemEncoder<Vec<u8>> {
        ElemEncoder {
            actor: RleEncoder::new(Vec::new()),
            counter: DeltaEncoder::new(Vec::new()),
        }
    }

    pub(crate) fn finish(self, out: &mut Vec<u8>) -> ElemRange {
        let actor_start = out.len();
        let (actor, _) = self.actor.finish();
        out.extend(actor);
        let actor_end = out.len();

        let (counter, _) = self.counter.finish();
        out.extend(counter);
        let counter_end = out.len();

        ElemRange {
            actor: (actor_start..actor_end).into(),
            counter: (actor_end..counter_end).into(),
        }
    }
}

impl<S: Sink> ElemEncoder<S> {
    pub(crate) fn append<O>(&mut self, elem: Option<convert::ElemId<O>>)
    where
        O: convert::OpId<usize>,
    {
        match elem {
            Some(convert::ElemId::Head) => {
                self.actor.append_null();
                self.counter.append_value(0);
            }
            Some(convert::ElemId::Op(o)) => {
                self.actor.append_value(o.actor() as u64);
                self.counter.append_value(o.counter() as i64);
            }
            None => { 
                self.actor.append_null();
                self.counter.append_null();
            }
        }
    }
}
