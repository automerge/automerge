use super::columns::ColumnDataIter;
use super::op_set::{KeyIter, OpIter, Verified};
use super::rle::ActorCursor;
use super::types::{Action, Key, OpType, ScalarValue};
use super::DeltaCursor;
use crate::op_set;
use crate::text_value::TextValue;
use crate::types::{ListEncoding, Clock, ElemId, ObjId, OpId};
use std::collections::HashSet;

#[derive(Debug, Copy, Clone)]
pub(crate) struct Op<'a> {
    pub(crate) index: usize,
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

#[derive(Clone, Copy)]
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
    pub(crate) fn as_str(&self) -> &'a str {
        if let ScalarValue::Str(s) = &self.value {
            s
        } else if self.is_mark() {
            ""
        } else {
            "\u{fffc}"
        }
    }

    pub(crate) fn width(&self, encoding: ListEncoding) -> usize {
        if encoding == ListEncoding::List {
            1
        } else {
            TextValue::width(self.to_str()) // FASTER
        }
    }

    pub(crate) fn op_type(&self) -> OpType<'_> {
        OpType::from_action_and_value(self.action, self.value, self.mark_name, self.expand)
    }

    pub(crate) fn succ(&self) -> impl Iterator<Item = OpId> + ExactSizeIterator + 'a {
        self.succ_cursors.clone()
    }

    pub(crate) fn elemid_or_key(&self) -> Key<'a> {
        if self.insert {
            Key::Seq(ElemId(self.id))
        } else {
            self.key
        }
    }

    pub(crate) fn visible_at(&self, clock: Option<&Clock>, iter: &OpIter<'a, Verified>) -> bool {
        if let Some(clock) = clock {
            if self.is_inc() || self.is_mark() {
                false
            } else {
                clock.covers(&self.id) && !self.succ().any(|i| clock.covers(&i))
            }
        } else {
            self.visible(iter)
        }
    }

    pub(crate) fn visible(&self, iter: &OpIter<'a, Verified>) -> bool {
        if self.is_inc() || self.is_mark() {
            false
        } else if self.is_counter() {
            let key_iter = KeyIter::new(*self, iter.clone());
            let sub_ops = key_iter.map(|op| op.id).collect::<HashSet<_>>();
            self.succ().all(|id| sub_ops.contains(&id))
        } else {
            self.succ().len() == 0
        }
    }

    pub(crate) fn is_inc(&self) -> bool {
        self.action == Action::Increment
    }

    pub(crate) fn is_counter(&self) -> bool {
        matches!(&self.value, ScalarValue::Counter(_))
    }

    pub(crate) fn is_mark(&self) -> bool {
        self.action == Action::Mark
    }
}

impl<'a> PartialEq<Op<'_>> for Op<'a> {
    fn eq(&self, other: &Op<'_>) -> bool {
        self.id == other.id
    }
}

impl<'a> std::hash::Hash for Op<'a> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state)
    }
}

impl<'a> Eq for Op<'a> {}

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

// TODO:
// needs tests around counter value and visability
