use super::{DecodeColumnError, DeltaDecoder, Key, OpType, RleDecoder, ScalarValue};
use crate::op_set;
use crate::types::{ObjId, OpId};

#[derive(Debug, Copy, Clone)]
pub(crate) struct Op<'a> {
    pub(crate) id: OpId,
    pub(crate) action: u64,
    pub(crate) obj: ObjId,
    pub(crate) key: Key<'a>,
    pub(crate) insert: bool,
    pub(crate) value: ScalarValue<'a>,
    pub(crate) succ: SuccIter<'a>,
    pub(crate) expand: bool,
    pub(crate) mark_name: Option<&'a [u8]>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SuccIterIter<'a> {
    num: RleDecoder<'a, u64>,
    actor: RleDecoder<'a, u64>,
    counter: DeltaDecoder<'a>,
}

impl<'a> SuccIterIter<'a> {
    pub(crate) fn new(num: &'a [u8], actor: &'a [u8], counter: &'a [u8]) -> Self {
        SuccIterIter {
            num: RleDecoder::from(num),
            actor: RleDecoder::from(actor),
            counter: DeltaDecoder::from(counter),
        }
    }
}

impl<'a> Iterator for SuccIterIter<'a> {
    type Item = Result<SuccIter<'a>, DecodeColumnError>;

    fn next(&mut self) -> Option<Self::Item> {
        let actor = self.actor;
        let counter = self.counter;
        match self.num.next() {
            Some(Ok(Some(num))) => {
                for _ in 0..num {
                    // throw away results :(
                    self.actor.next();
                    self.counter.next();
                }
                Some(Ok(SuccIter {
                    num,
                    actor,
                    counter,
                }))
            }
            Some(Ok(None)) | None => Some(Ok(SuccIter {
                num: 0,
                actor,
                counter,
            })),
            Some(Err(e)) => Some(Err(DecodeColumnError::decode_raw("succ_num", e))),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SuccIter<'a> {
    num: u64,
    actor: RleDecoder<'a, u64>,
    counter: DeltaDecoder<'a>,
}

impl<'a> Iterator for SuccIter<'a> {
    type Item = Result<OpId, DecodeColumnError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.num > 0 {
            match (self.actor.next(), self.counter.next()) {
                (Some(Ok(Some(actor))), Some(Ok(Some(counter)))) => {
                    self.num -= 1;
                    Some(Ok(OpId::new(counter as u64, actor as usize)))
                }
                _ => Some(Err(DecodeColumnError::unexpected_null("succ"))),
            }
        } else {
            None
        }
    }
}

impl<'a> PartialEq<op_set::Op<'_>> for Op<'a> {
    fn eq(&self, other: &op_set::Op<'_>) -> bool {
        let action =
            OpType::from_action_and_value(self.action, self.value, self.mark_name, self.expand);
        self.id == *other.id()
            && self.obj == *other.obj()
            && self.key == other.ex_key()
            && self.insert == other.insert()
            && &action == other.action()
            && self
                .succ
                .filter_map(|n| n.ok())
                .eq(other.succ().map(|n| *n.id()))
    }
}
