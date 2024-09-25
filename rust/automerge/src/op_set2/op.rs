use super::op_set::{OpLike, OpSet};
use super::packer::{ColumnDataIter, DeltaCursor};
use super::types::{Action, ActorCursor, Key, KeyRef, OpType, PropRef, ScalarValue};
use super::{Value, ValueMeta};

use crate::convert;
use crate::storage::document::AsDocOp;

use crate::error::AutomergeError;
use crate::exid::ExId;
use crate::hydrate;
use crate::text_value::TextValue;
use crate::types;
use crate::types::{ElemId, ListEncoding, ObjId, ObjMeta, OpId};

use std::borrow::Cow;
use std::cmp::Ordering;

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

    pub(crate) fn prop(&self) -> PropRef<'_> {
        if let Key::Map(s) = &self.key {
            PropRef::Map(s)
        } else {
            PropRef::Seq(self.index)
        }
    }

    pub(crate) fn hydrate_value(&self) -> hydrate::Value {
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
    fn mark_name(&self) -> Option<&str> {
        self.action.mark_name()
    }
    fn expand(&self) -> bool {
        self.action.expand()
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
    //pub(crate) index: usize,
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
    pub(crate) fn add_succ(&self, id: OpId) -> SuccInsert {
        let pos = self.pos;
        let mut succ = self.succ_cursors;
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
        } else if self.action == Action::Mark {
            0
        } else {
            TextValue::width(self.as_str()) // FASTER
        }
    }

    pub(crate) fn op_type(&self) -> OpType<'a> {
        OpType::from_action_and_value(self.action, self.value, self.mark_name, self.expand)
    }

    pub(crate) fn succ(&self) -> impl ExactSizeIterator<Item = OpId> + 'a {
        self.succ_cursors
    }

    pub(crate) fn exid(&self, op_set: &OpSet) -> ExId {
        let id = self.id;
        if id == types::ROOT {
            ExId::Root
        } else {
            ExId::Id(
                id.counter(),
                op_set.get_actor(id.actor()).clone(),
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
                _ => Err(AutomergeError::InvalidCursorOp),
            }
        }
    }

    pub(crate) fn tagged_value(&self, op_set: &'a OpSet) -> (types::Value<'static>, ExId) {
        (self.value().into_owned(), self.exid(op_set))
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
            OpType::MarkBegin(_, _) => Value::Scalar(ScalarValue::Str("markBegin")),
            OpType::MarkEnd(_) => Value::Scalar(ScalarValue::Str("markEnd")),
            _ => panic!("cant convert op into a value - {:?}", self),
        }
    }

    pub(crate) fn hydrate_value(&self) -> hydrate::Value {
        match &self.action() {
            OpType::Make(obj_type) => hydrate::Value::from(*obj_type), // hydrate::Value::Object(*obj_type),
            OpType::Put(scalar) => hydrate::Value::from(scalar.into_owned()),
            OpType::MarkBegin(_, mark) => hydrate::Value::from(mark.value),
            OpType::MarkEnd(_) => hydrate::Value::Scalar("markEnd".into()),
            _ => panic!("cant convert op into a value"),
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
        self.succ_cursors
    }
    fn expand(&self) -> bool {
        self.expand
    }
    fn mark_name(&self) -> Option<Cow<'a, smol_str::SmolStr>> {
        self.mark_name
            .map(|s| Cow::Owned(smol_str::SmolStr::from(s)))
    }
}
