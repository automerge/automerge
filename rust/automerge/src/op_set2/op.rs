use super::columns::ColumnDataIter;
use super::op_set::{KeyIter, OpIter, OpSet};
use super::rle::ActorCursor;
use super::types::{Action, Key, OpType, ScalarValue};
use super::{DeltaCursor, Value};
use crate::exid::ExId;
use crate::text_value::TextValue;
use crate::types;
use crate::types::{Clock, ElemId, ListEncoding, ObjId, OpId};
use std::collections::HashSet;
use std::sync::Arc;

#[derive(Debug, Copy, Clone)]
pub(crate) struct Op<'a> {
    pub(crate) index: usize, // rename to pos
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
    pub(super) op_set: &'a OpSet,
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

    pub(crate) fn exid(&self) -> ExId {
        let id = self.id;
        if id == types::ROOT {
            ExId::Root
        } else {
            ExId::Id(
                id.counter(),
                self.op_set.get_actor(id.actor()).clone(),
                id.actor(),
            )
        }
    }

    pub(crate) fn elemid_or_key(&self) -> Key<'a> {
        if self.insert {
            Key::Seq(ElemId(self.id))
        } else {
            self.key
        }
    }

    pub(crate) fn tagged_value(&self) -> (types::Value<'static>, ExId) {
        (self.value().into_owned(), self.exid())
    }

    pub(crate) fn get_increment_value(&self) -> Option<i64> {
        match (self.action, self.value) {
            (Action::Increment, ScalarValue::Int(i)) => Some(i),
            (Action::Increment, ScalarValue::Uint(i)) => Some(i as i64),
            _ => None,
        }
    }

    pub(crate) fn is_put(&self) -> bool {
        self.action == Action::Set
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

    pub(crate) fn action(&self) -> OpType<'a> {
        self.op_type()
    }

    pub(crate) fn is_noop(&self, action: &OpType) -> bool {
        matches!((&self.action(), action), (OpType::Put(n), OpType::Put(m)) if n == m)
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

// TODO:
// needs tests around counter value and visability
