use crate::types::{ElemId, Key, OpId};

use super::{DeltaDecoder, RleDecoder};

pub(crate) struct InternedKeyDecoder<'a> {
    actor: RleDecoder<'a, u64>,
    ctr: DeltaDecoder<'a>,
    str_idx: RleDecoder<'a, u64>,
}

impl<'a> InternedKeyDecoder<'a> {
    pub(crate) fn new(
        actor: RleDecoder<'a, u64>,
        ctr: DeltaDecoder<'a>,
        str_idx: RleDecoder<'a, u64>,
    ) -> Self {
        Self {
            actor,
            ctr,
            str_idx,
        }
    }

    pub(crate) fn done(&self) -> bool {
        self.actor.done() && self.ctr.done() && self.str_idx.done()
    }
}

impl<'a> Iterator for InternedKeyDecoder<'a> {
    type Item = Key;

    fn next(&mut self) -> Option<Key> {
        match (self.actor.next(), self.ctr.next(), self.str_idx.next()) {
            (None, None, Some(Some(key_idx))) => Some(Key::Map(key_idx as usize)),
            (None, Some(Some(0)), None) => Some(Key::Seq(ElemId(OpId(0, 0)))),
            (Some(Some(actor)), Some(Some(ctr)), None) => Some(Key::Seq(OpId(actor, ctr as usize).into())),
            // TODO: This should be fallible and throw here
            _ => None,
        }
    }
}
