use crate::types::{OpId, ObjId};

use super::{DecodeColumnError, RleDecoder};

pub(crate) struct ObjDecoder<'a> {
    actor: RleDecoder<'a, u64>,
    ctr: RleDecoder<'a, u64>,
}

impl<'a> ObjDecoder<'a> {
    pub(crate) fn new(actor: RleDecoder<'a, u64>, ctr: RleDecoder<'a, u64>) -> Self {
        Self{
            actor,
            ctr,
        }
    }

    pub(crate) fn done(&self) -> bool {
        self.actor.done() || self.ctr.done()
    }
}

impl<'a> Iterator for ObjDecoder<'a> {
    type Item = Result<ObjId, DecodeColumnError>;

    fn next(&mut self) -> Option<Self::Item> {
        match (self.actor.next(), self.ctr.next()) {
            (None, None) => None,
            (Some(None), Some(None)) => Some(Ok(ObjId::root())),
            (Some(Some(a)), Some(Some(c))) => Some(Ok(ObjId(OpId(c, a as usize)))),
            (Some(None), _) | (None, _) => Some(Err(DecodeColumnError::UnexpectedNull("actor".to_string()))),
            (_, Some(None)) | (_, None) => Some(Err(DecodeColumnError::UnexpectedNull("counter".to_string()))),
        }
    }
}
