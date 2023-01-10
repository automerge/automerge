use std::{convert::Infallible, ops::Range};

use crate::{
    columnar::{
        encoding::{raw, DecodeColumnError, RleDecoder, RleEncoder, Sink},
        SpliceError,
    },
    convert,
    types::{ObjId, OpId},
};

use super::RleRange;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ObjIdRange {
    actor: RleRange<u64>,
    counter: RleRange<u64>,
}

impl ObjIdRange {
    pub(crate) fn new(actor: RleRange<u64>, counter: RleRange<u64>) -> Option<Self> {
        if actor.is_empty() || counter.is_empty() {
            None
        } else {
            Some(Self { actor, counter })
        }
    }

    pub(crate) fn actor_range(&self) -> &RleRange<u64> {
        &self.actor
    }

    pub(crate) fn counter_range(&self) -> &RleRange<u64> {
        &self.counter
    }

    pub(crate) fn encode<O, I: Iterator<Item = convert::ObjId<O>> + Clone>(
        ids: I,
        out: &mut Vec<u8>,
    ) -> Option<Self>
    where
        O: convert::OpId<usize>,
    {
        // SAFETY: the incoming iterator is infallible and there are no existing elements
        Self {
            actor: (0..0).into(),
            counter: (0..0).into(),
        }
        .splice::<_, Infallible, _>(&[], 0..0, ids.map(Ok), out)
        .unwrap()
    }

    /// Given some existing columns of object IDs splice a new set of object IDs in with the
    /// existing ones
    ///
    /// Note that this returns `None` if the resulting range is empty (which will only occur if the
    /// replace range is larger than the input iterator and `ids` is an empty iterator).
    pub(crate) fn splice<
        O,
        E: std::error::Error,
        I: Iterator<Item = Result<convert::ObjId<O>, E>> + Clone,
    >(
        &self,
        data: &[u8],
        replace: Range<usize>,
        ids: I,
        out: &mut Vec<u8>,
    ) -> Result<Option<Self>, SpliceError<raw::Error, E>>
    where
        O: convert::OpId<usize>,
    {
        let actor = self.actor.splice(
            data,
            replace.clone(),
            ids.clone().map(|id| id.map(encoded_actor)),
            out,
        )?;

        if actor.is_empty() {
            return Ok(None);
        }

        let counter = self.counter.splice(
            data,
            replace,
            ids.map(|i| {
                i.map(|i| match i {
                    convert::ObjId::Root => None,
                    convert::ObjId::Op(o) => Some(o.counter()),
                })
            }),
            out,
        )?;

        Ok(Some(Self { actor, counter }))
    }

    pub(crate) fn iter<'a>(&self, data: &'a [u8]) -> ObjIdIter<'a> {
        ObjIdIter {
            actor: self.actor.decoder(data),
            counter: self.counter.decoder(data),
        }
    }
}

fn encoded_actor<O>(id: convert::ObjId<O>) -> Option<u64>
where
    O: convert::OpId<usize>,
{
    match id {
        convert::ObjId::Root => None,
        convert::ObjId::Op(o) => Some(o.actor() as u64),
    }
}

#[derive(Clone)]
pub(crate) struct ObjIdIter<'a> {
    actor: RleDecoder<'a, u64>,
    counter: RleDecoder<'a, u64>,
}

impl<'a> ObjIdIter<'a> {
    fn try_next(&mut self) -> Result<Option<ObjId>, DecodeColumnError> {
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
            (None | Some(None), None | Some(None)) => Ok(Some(ObjId::root())),
            (Some(Some(a)), Some(Some(c))) => Ok(Some(ObjId(OpId::new(c, a as usize)))),
            (_, Some(Some(0))) => Ok(Some(ObjId::root())),
            (Some(None) | None, _) => Err(DecodeColumnError::unexpected_null("actor")),
            (_, Some(None) | None) => Err(DecodeColumnError::unexpected_null("counter")),
        }
    }
}

impl<'a> Iterator for ObjIdIter<'a> {
    type Item = Result<ObjId, DecodeColumnError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

pub(crate) struct ObjIdEncoder<S> {
    actor: RleEncoder<S, u64>,
    counter: RleEncoder<S, u64>,
}

impl<S: Sink> ObjIdEncoder<S> {
    pub(crate) fn append<O>(&mut self, id: convert::ObjId<O>)
    where
        O: convert::OpId<usize>,
    {
        match id {
            convert::ObjId::Root => {
                self.actor.append_null();
                self.counter.append_null();
            }
            convert::ObjId::Op(o) => {
                self.actor.append_value(o.actor() as u64);
                self.counter.append_value(o.counter());
            }
        }
    }
}

impl ObjIdEncoder<Vec<u8>> {
    pub(crate) fn new() -> Self {
        Self {
            actor: RleEncoder::from(Vec::new()),
            counter: RleEncoder::from(Vec::new()),
        }
    }

    pub(crate) fn finish(self, out: &mut Vec<u8>) -> Option<ObjIdRange> {
        let start = out.len();
        let (actor, _) = self.actor.finish();
        out.extend(actor);
        let actor_end = out.len();

        let (counter, _) = self.counter.finish();
        out.extend(counter);
        let counter_end = out.len();

        if start == counter_end {
            None
        } else {
            Some(ObjIdRange {
                actor: (start..actor_end).into(),
                counter: (actor_end..counter_end).into(),
            })
        }
    }
}
