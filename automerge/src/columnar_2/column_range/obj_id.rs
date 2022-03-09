use std::ops::Range;

use crate::{
    columnar_2::encoding::{DecodeColumnError, RleDecoder},
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
        Self {
            actor: (0..0).into(),
            counter: (0..0).into(),
        }
        .splice(&[], 0..0, ids, out)
    }

    /// Given some existing columns of object IDs splice a new set of object IDs in with the
    /// existing ones
    ///
    /// Note that this returns `None` if the resulting range is empty (which will only occur if the
    /// replace range is larger than the input iterator and `ids` is an empty iterator).
    pub(crate) fn splice<O, I: Iterator<Item = convert::ObjId<O>> + Clone>(
        &self,
        data: &[u8],
        replace: Range<usize>,
        ids: I,
        out: &mut Vec<u8>,
    ) -> Option<Self>
    where
        O: convert::OpId<usize>,
    {
        let actor = self
            .actor
            .splice(data, replace.clone(), ids.clone().map(encoded_actor), out);

        if actor.is_empty() {
            return None;
        }

        let counter = self.counter.splice(
            data,
            replace,
            ids.map(|i| match i {
                convert::ObjId::Root => None,
                convert::ObjId::Op(o) => Some(o.counter()),
            }),
            out,
        );

        Some(Self { actor, counter })
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

impl<'a> Iterator for ObjIdIter<'a> {
    type Item = Result<ObjId, DecodeColumnError>;

    fn next(&mut self) -> Option<Self::Item> {
        match (self.actor.next(), self.counter.next()) {
            (None | Some(None), None | Some(None)) => Some(Ok(ObjId::root())),
            (Some(Some(a)), Some(Some(c))) => Some(Ok(ObjId(OpId(c, a as usize)))),
            (_, Some(Some(0))) => Some(Ok(ObjId::root())),
            (Some(None) | None, _) => {
                Some(Err(DecodeColumnError::UnexpectedNull("actor".to_string())))
            }
            (_, Some(None) | None) => Some(Err(DecodeColumnError::UnexpectedNull(
                "counter".to_string(),
            ))),
        }
    }
}
