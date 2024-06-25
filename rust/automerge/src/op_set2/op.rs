use super::columns::ColumnDataIter;
use super::op_set::{KeyIter, OpIter, Verified};
use super::rle::ActorCursor;
use super::types::{Action, Key, OpType, ScalarValue};
use super::{DeltaCursor, Value};
use crate::exid::ExId;
use crate::text_value::TextValue;
use crate::types;
use crate::types::{Clock, ElemId, ListEncoding, ObjId, OpId};
use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::Arc;

#[derive(Debug, Copy, Clone)]
pub(crate) struct Op<'a> {
    pub(crate) index: usize,
    pub(crate) conflict: bool,
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
            TextValue::width(self.as_str()) // FASTER
        }
    }

    pub(crate) fn op_type(&self) -> OpType<'a> {
        OpType::from_action_and_value(self.action, self.value, self.mark_name, self.expand)
    }

    pub(crate) fn succ(&self) -> impl Iterator<Item = OpId> + ExactSizeIterator + 'a {
        self.succ_cursors.clone()
    }

    /*
        pub(crate) fn exid(&self, op_set: &OpSet) -> ExId {
                ExId::Id(
                    self.id.counter(),
                    0, // FIXME
                    self.id.actor(),
                )
        }
    */

    pub(crate) fn elemid_or_key(&self) -> Key<'a> {
        if self.insert {
            Key::Seq(ElemId(self.id))
        } else {
            self.key
        }
    }

    pub(crate) fn predates(&self, clock: &Clock) -> bool {
        clock.covers(&self.id)
    }

    pub(crate) fn was_deleted_before(&self, clock: &Clock) -> bool {
        todo!()
        //self.succ_iter().any(|op| clock.covers(op.id()))
    }

    pub(crate) fn tagged_value(&self, clock: Option<&Clock>) -> (types::Value<'a>, ExId) {
        todo!()
    }

    pub(crate) fn inc_at(&self, clock: &Clock) -> i64 {
        todo!()
        /*
                self.succ()
                    .filter_map(|o| {
                        if clock.covers(o.id()) {
                            o.op().get_increment_value()
                        } else {
                            None
                        }
                    })
                    .sum()
        */
    }

    pub(crate) fn is_put(&self) -> bool {
        todo!()
        //matches!(&self.action(), OpType::Put(_))
    }

    pub(crate) fn value(&self) -> Value<'a> {
        match &self.action() {
            OpType::Make(obj_type) => Value::Object(*obj_type),
            OpType::Put(scalar) => Value::Scalar(*scalar),
            OpType::MarkBegin(_, mark) => Value::Scalar(ScalarValue::Str("markBegin")),
            OpType::MarkEnd(_) => Value::Scalar(ScalarValue::Str("markEnd")),
            _ => panic!("cant convert op into a value - {:?}", self),
        }
    }

    pub(crate) fn value_at(&self, clock: Option<&Clock>) -> Value<'a> {
        todo!()
        /*
                if let Some(clock) = clock {
                    if let OpType::Put(ScalarValue::Counter(c)) = &self.op().action {
                        return Value::counter(c.start + self.inc_at(clock));
                    }
                }
                self.value()
        */
    }

    pub(crate) fn visible_at(&self, clock: Option<&Clock>) -> bool {
        if let Some(clock) = clock {
            if self.is_inc() || self.is_mark() {
                false
            } else {
                clock.covers(&self.id) && !self.succ().any(|i| clock.covers(&i))
            }
        } else {
            self.visible()
        }
    }

    pub(crate) fn visible_or_mark(&self, clock: Option<&Clock>) -> bool {
        todo!()
        /*
                if self.is_inc() {
                    false
                } else if let Some(clock) = clock {
                    clock.covers(&self.op().id) && self.succ().all(|o| o.is_inc() || !clock.covers(o.id()))
                } else if self.is_counter() {
                    self.succ().all(|op| op.is_inc())
                } else {
                    self.succ().len() == 0
                }
        */
    }

    pub(crate) fn action(&self) -> OpType<'a> {
        self.op_type()
    }

    pub(crate) fn is_noop(&self, action: &OpType) -> bool {
        todo!()
        //matches!((&self.action, action), (OpType::Put(n), OpType::Put(m)) if n == m)
    }

    pub(crate) fn visible(&self) -> bool {
        if self.is_inc() || self.is_mark() {
            false
        } else if self.is_counter() {
            todo!()
        /*
                    let key_iter = KeyIter::new(*self, self.iter.clone());
                    let sub_ops = key_iter.map(|op| op.id).collect::<HashSet<_>>();
                    self.succ().all(|id| sub_ops.contains(&id))
        */
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

/*
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
*/

// TODO:
// needs tests around counter value and visability
