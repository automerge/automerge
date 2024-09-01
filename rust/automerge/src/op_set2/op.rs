use super::columns::ColumnDataIter;
use super::op_set::ids::*;
use super::op_set::{KeyIter, OpIter, OpLike, OpSet};
use super::rle::ActorCursor;
use super::types::{Action, Key, KeyRef, OpType, PropRef, ScalarValue};
use super::{ActorIdx, DeltaCursor, Value, ValueMeta};

//use crate::storage::document::doc_op_columns::AsDocOp;
use crate::convert;
use crate::storage::document::AsDocOp;

use crate::error::AutomergeError;
use crate::exid::ExId;
use crate::hydrate;
use crate::storage::ColumnSpec;
use crate::text_value::TextValue;
use crate::types;
use crate::types::{ActorId, Clock, ElemId, ListEncoding, ObjId, ObjMeta, OpId};

use std::borrow::{Borrow, Cow};
use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub(crate) struct ChangeOp {
    pub(crate) id: OpId,
    pub(crate) obj: ObjId,
    pub(crate) key: Key,
    pub(crate) insert: bool,
    pub(crate) action: u64,
    pub(crate) val: types::ScalarValue,
    pub(crate) mark_name: Option<smol_str::SmolStr>,
    pub(crate) expand: bool,
    pub(crate) pred: Vec<OpId>,
}

impl ChangeOp {
    pub(crate) fn build(self, pos: usize, obj: ObjMeta) -> OpBuilder2 {
        OpBuilder2 {
            id: self.id,
            obj,
            pos,
            index: 0,
            key: self.key,
            action: crate::types::OpType::from_action_and_value(
                self.action,
                self.val,
                self.mark_name,
                self.expand,
            ),
            insert: self.insert,
            pred: self.pred,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct OpBuilder2 {
    pub(crate) id: OpId,
    pub(crate) obj: ObjMeta,
    pub(crate) pos: usize,
    pub(crate) index: usize,
    pub(crate) key: Key,
    pub(crate) action: types::OpType,
    pub(crate) insert: bool,
    pub(crate) pred: Vec<OpId>,
}

impl OpBuilder2 {
    pub(crate) fn del(id: OpId, obj: ObjMeta, key: Key, pred: Vec<OpId>) -> Self {
        OpBuilder2 {
            id,
            obj,
            pos: 0,
            index: 0,
            action: types::OpType::Delete,
            key,
            insert: false,
            pred,
        }
    }

    pub(crate) fn get_int(&self, spec: &ColumnSpec) -> Option<i64> {
        match *spec {
            super::op_set::ID_COUNTER_COL_SPEC => Some(0),
            super::op_set::KEY_COUNTER_COL_SPEC => Some(0),
            s => {
                log!("unknown col spec ({:?}) passed to get_int()", spec);
                None
            }
        }
    }

    pub(crate) fn get_group_int(&self, spec: &ColumnSpec) -> Vec<Option<i64>> {
        if *spec == super::op_set::SUCC_COUNTER_COL_SPEC {
            vec![]
        } else {
            log!("unknown col spec ({:?}) passed to get_group_int()", spec);
            vec![]
        }
    }

    pub(crate) fn get_actor(&self, spec: &ColumnSpec) -> Option<ActorIdx> {
        match *spec {
            super::op_set::ID_ACTOR_COL_SPEC => Some(ActorIdx(0)),
            super::op_set::KEY_ACTOR_COL_SPEC => Some(ActorIdx(0)),
            s => {
                log!("unknown col spec ({:?}) passed to get_actor()", spec);
                None
            }
        }
    }

    pub(crate) fn get_group_actor(&self, spec: &ColumnSpec) -> Vec<Option<ActorIdx>> {
        if *spec == super::op_set::SUCC_ACTOR_COL_SPEC {
            vec![]
        } else {
            log!("unknown col spec ({:?}) passed to get_group_actor()", spec);
            vec![]
        }
    }

    //fn op_type(&self) -> OpType<'_> {
    //OpType::from_action_and_value(self.action, self.value, self.mark_name, self.expand)
    //OpType::from_action_and_value(self.action, self.value, None, false)
    //}

    pub(crate) fn prop(&self) -> PropRef<'_> {
        if let Key::Map(s) = &self.key {
            PropRef::Map(s)
        } else {
            PropRef::Seq(self.index)
        }
    }

    pub(crate) fn value(&self) -> hydrate::Value {
        match &self.action {
            types::OpType::Make(obj_type) => hydrate::Value::from(*obj_type), // hydrate::Value::Object(*obj_type),
            types::OpType::Put(scalar) => hydrate::Value::from(scalar.clone()),
            types::OpType::MarkBegin(_, mark) => hydrate::Value::from(mark.value.clone()),
            types::OpType::MarkEnd(_) => hydrate::Value::Scalar("markEnd".into()),
            _ => panic!("cant convert op into a value"),
        }
    }

    pub(crate) fn get_increment_value(&self) -> Option<i64> {
        if let types::OpType::Increment(i) = &self.action {
            Some(*i)
        } else {
            None
        }
    }

    pub(crate) fn is_delete(&self) -> bool {
        self.action == OpType::Delete
    }

    pub(crate) fn as_str(&self) -> &str {
        self.action.to_str()
    }

    pub(crate) fn is_mark(&self) -> bool {
        self.action.is_mark()
    }

    pub(crate) fn width(&self, encoding: ListEncoding) -> usize {
        if encoding == ListEncoding::List {
            1
        } else {
            TextValue::width(self.as_str()) // FASTER
        }
    }

    pub(crate) fn is_list_op(&self) -> bool {
        self.elemid().is_some()
    }

    pub(crate) fn obj(&self) -> ObjMeta {
        self.obj
    }
}

impl OpLike for OpBuilder2 {
    fn id(&self) -> OpId {
        self.id
    }
    fn obj(&self) -> ObjId {
        self.obj.id
    }
    fn action(&self) -> Action {
        self.action.action()
    }

    fn elemid(&self) -> Option<ElemId> {
        match &self.key {
            Key::Seq(e) => Some(*e),
            _ => None,
        }
    }
    fn map_key(&self) -> Option<&str> {
        match &self.key {
            Key::Map(s) => Some(s),
            _ => None,
        }
    }
    fn raw_value(&self) -> Option<Cow<'_, [u8]>> {
        self.action.to_raw()
    }
    fn meta_value(&self) -> ValueMeta {
        ValueMeta::from(self.action.value().as_ref())
    }
    fn insert(&self) -> bool {
        self.insert
    }
}

impl PartialEq<OpBuilder2> for OpBuilder2 {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for OpBuilder2 {}

impl PartialOrd for OpBuilder2 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OpBuilder2 {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl<'a> OpLike for Op<'a> {
    fn id(&self) -> OpId {
        self.id
    }
    fn obj(&self) -> ObjId {
        self.obj
    }
    fn action(&self) -> Action {
        self.action
    }

    fn elemid(&self) -> Option<ElemId> {
        self.key.elemid()
    }
    fn map_key(&self) -> Option<&str> {
        self.key.map_key()
    }
    fn raw_value(&self) -> Option<Cow<'_, [u8]>> {
        self.value.to_raw()
    }
    fn meta_value(&self) -> ValueMeta {
        ValueMeta::from(&self.value)
    }
    fn succ(&self) -> Vec<OpId> {
        self.succ().collect()
    }
    fn insert(&self) -> bool {
        self.insert
    }
    fn expand(&self) -> bool {
        self.expand
    }
    fn mark_name(&self) -> Option<&str> {
        self.mark_name
    }
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct Op<'a> {
    pub(crate) pos: usize,
    pub(crate) index: usize,
    pub(crate) conflict: bool,
    pub(crate) id: OpId,
    pub(crate) action: Action,
    pub(crate) obj: ObjId,
    pub(crate) key: KeyRef<'a>,
    pub(crate) insert: bool,
    pub(crate) value: ScalarValue<'a>,
    pub(crate) expand: bool,
    pub(crate) mark_name: Option<&'a str>,
    pub(super) succ_cursors: SuccCursors<'a>,
    pub(super) op_set: &'a OpSet,
}

#[derive(Clone, Default, Copy)]
pub(crate) struct SuccCursors<'a> {
    pub(super) len: usize,
    pub(super) succ_actor: ColumnDataIter<'a, ActorCursor>,
    pub(super) succ_counter: ColumnDataIter<'a, DeltaCursor>,
}

impl<'a> SuccCursors<'a> {
    pub(crate) fn pos(&self) -> usize {
        self.succ_actor.pos()
    }
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

#[derive(Debug)]
pub(crate) struct SuccInsert {
    pub(crate) pos: usize,
    pub(crate) len: u64,
    pub(crate) sub_pos: usize,
}

impl<'a> Op<'a> {
    /*
        pub(crate) fn pred(&self) -> impl Iterator<Item = OpId> + ExactSizeIterator {
            // FIXME
            vec![].into_iter()
        }
    */

    pub(crate) fn add_succ(&self, id: OpId) -> SuccInsert {
        let pos = self.pos;
        let mut succ = self.succ_cursors.clone();
        let len = succ.len() as u64;
        let mut sub_pos = succ.pos();
        while let Some(i) = succ.next() {
            if i > id {
                break;
            }
            sub_pos = succ.pos();
        }
        SuccInsert { pos, len, sub_pos }
    }

    pub(crate) fn op_set(&self) -> &'a OpSet {
        self.op_set
    }

    pub(crate) fn as_str(&self) -> &'a str {
        if let ScalarValue::Str(s) = &self.value {
            s
        } else if self.action == Action::Mark {
            ""
        } else {
            "\u{fffc}"
        }
    }

    pub(crate) fn width(&self, encoding: ListEncoding) -> usize {
        if encoding == ListEncoding::List {
            1
        } else {
            if self.action == Action::Mark {
                0
            } else {
                TextValue::width(self.as_str()) // FASTER
            }
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

    pub(crate) fn elemid_or_key(&self) -> KeyRef<'a> {
        if self.insert {
            KeyRef::Seq(ElemId(self.id))
        } else {
            self.key
        }
    }

    pub(crate) fn cursor(&self) -> Result<ElemId, AutomergeError> {
        if self.insert {
            Ok(ElemId(self.id))
        } else {
            match self.key {
                KeyRef::Seq(e) => Ok(e),
                _ => Err(AutomergeError::InvalidCursorOp(self.exid())),
            }
        }
    }

    pub(crate) fn raw_elemid(&self) -> Option<ElemId> {
        if let KeyRef::Seq(e) = self.key {
            Some(e)
        } else {
            None
        }
    }

    pub(crate) fn map_key(&self) -> Option<&'a str> {
        if let KeyRef::Map(s) = self.key {
            Some(s)
        } else {
            None
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

    pub(crate) fn is_noop(&self, action: &types::OpType) -> bool {
        matches!((&self.action(), action), (OpType::Put(n), types::OpType::Put(m)) if n == m)
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

    pub(crate) fn build(&self, pred: Option<Vec<OpId>>) -> OpBuilder2 {
        OpBuilder2 {
            id: self.id,
            obj: self.obj.into(),
            pos: 0,
            index: 0,
            action: self.op_type().into_owned(),
            key: self.key.into_owned(),
            insert: self.insert,
            pred: pred.unwrap_or_default(),
        }
    }
}

impl<'a> PartialEq<Op<'_>> for Op<'a> {
    fn eq(&self, other: &Op<'_>) -> bool {
        self.id == other.id
    }
}

impl<'a> PartialEq<Op<'_>> for crate::op_set::Op<'a> {
    fn eq(&self, other: &Op<'_>) -> bool {
        self.id() == &other.id
            && self.obj() == &other.obj
            && self.action() == &other.action()
            && self.map_key() == other.map_key()
            && self.raw_elemid() == other.raw_elemid()
            && self.insert() == other.insert
    }
}

impl<'a> PartialEq<crate::op_set::Op<'_>> for Op<'a> {
    fn eq(&self, other: &crate::op_set::Op<'_>) -> bool {
        other.eq(self)
    }
}

impl<'a> PartialOrd for Op<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for Op<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl<'a> std::hash::Hash for Op<'a> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state)
    }
}

impl<'a> Eq for Op<'a> {}

impl<'a> AsDocOp<'a> for Op<'a> {
    type ActorId = usize;
    type OpId = OpId;
    type SuccIter = SuccCursors<'a>;

    fn obj(&self) -> convert::ObjId<Self::OpId> {
        self.obj.into()
    }
    fn id(&self) -> Self::OpId {
        self.id
    }
    fn key(&self) -> convert::Key<'a, Self::OpId> {
        match self.key {
            KeyRef::Map(s) => convert::Key::Prop(Cow::Owned(smol_str::SmolStr::from(s))),
            KeyRef::Seq(e) if e.is_head() => convert::Key::Elem(convert::ElemId::Head),
            KeyRef::Seq(ElemId(op)) => convert::Key::Elem(convert::ElemId::Op(op)),
        }
    }
    fn insert(&self) -> bool {
        self.insert
    }
    fn action(&self) -> u64 {
        self.action.into()
    }
    fn val(&self) -> Cow<'a, crate::value::ScalarValue> {
        Cow::Owned(self.value.into())
    }
    fn succ(&self) -> Self::SuccIter {
        self.succ_cursors.clone()
    }
    fn expand(&self) -> bool {
        self.expand
    }
    fn mark_name(&self) -> Option<Cow<'a, smol_str::SmolStr>> {
        self.mark_name
            .map(|s| Cow::Owned(smol_str::SmolStr::from(s)))
    }
}

/*
pub(crate) trait AsDocOp<'a> {
    /// The type of the Actor ID component of the op IDs for this impl. This is typically either
    /// `&'a ActorID` or `usize`
    type ActorId;
    /// The type of the op IDs this impl produces.
    type OpId: convert::OpId<Self::ActorId>;
    /// The type of the successor iterator returned by `Self::pred`. This can often be omitted
    type SuccIter: Iterator<Item = Self::OpId> + ExactSizeIterator;

    fn obj(&self) -> convert::ObjId<Self::OpId>;
    fn id(&self) -> Self::OpId;
    fn key(&self) -> convert::Key<'a, Self::OpId>;
    fn insert(&self) -> bool;
    fn action(&self) -> u64;
    fn val(&self) -> Cow<'a, ScalarValue>;
    fn succ(&self) -> Self::SuccIter;
    fn expand(&self) -> bool;
    fn mark_name(&self) -> Option<Cow<'a, smol_str::SmolStr>>;
}
*/

// TODO:
// needs tests around counter value and visability
