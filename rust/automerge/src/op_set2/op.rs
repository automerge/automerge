use super::columns::ColumnDataIter;
use super::rle::ActorCursor;
use super::types::{Action, Key, OpType, ScalarValue};
use super::DeltaCursor;
use crate::op_set;
use crate::types::{ObjId, OpId};

#[derive(Debug)]
pub(crate) struct Op<'a> {
    pub(crate) id: OpId,
    pub(crate) action: Action,
    pub(crate) obj: ObjId,
    pub(crate) key: Key<'a>,
    pub(crate) insert: bool,
    pub(crate) value: ScalarValue<'a>,
    pub(crate) expand: bool,
    pub(crate) mark_name: Option<&'a str>,
    pub(super) succ_cursors: SuccCursors<'a>,
}

impl<'a> Clone for Op<'_> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            action: self.action,
            obj: self.obj,
            key: self.key,
            insert: self.insert,
            value: self.value,
            expand: self.expand,
            mark_name: self.mark_name,
            succ_cursors: self.succ_cursors.clone(),
        }
    }
}

#[derive(Clone)]
pub(super) struct SuccCursors<'a> {
    pub(super) len: usize,
    pub(super) succ_actor: ColumnDataIter<'a, ActorCursor>,
    pub(super) succ_counter: ColumnDataIter<'a, DeltaCursor>,
}

impl<'a> std::fmt::Debug for SuccCursors<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SuccCursors")
            .field("len", &self.len)
            .finish()
    }
}

impl<'a> Iterator for SuccCursors<'a> {
    type Item = OpId;

    fn next(&mut self) -> Option<Self::Item> {
        if self.len == 0 {
            None
        } else {
            let Some(Some(counter)) = self.succ_counter.next() else {
                return None;
            };
            let Some(Some(actor)) = self.succ_actor.next() else {
                return None;
            };
            self.len -= 1;
            Some(OpId::new(counter as u64, u64::from(actor) as usize))
        }
    }
}

impl<'a> ExactSizeIterator for SuccCursors<'a> {
    fn len(&self) -> usize {
        self.len
    }
}

impl<'a> Op<'a> {
    pub(crate) fn succ(&self) -> impl Iterator<Item = OpId> + ExactSizeIterator + 'a {
        self.succ_cursors.clone()
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
            && self.succ().eq(other.succ().map(|n| *n.id()))
    }
}
