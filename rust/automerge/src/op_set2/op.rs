use super::op_set::{MarkIndexValue, OpSet};
use super::packer::{ColumnDataIter, DeltaCursor, IntCursor};
use super::types::{Action, ActorCursor, ActorIdx, KeyRef, OpType, PropRef, ScalarValue};
use super::{Value, ValueMeta};

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
    pub(crate) key: KeyRef<'static>,
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
    pub(crate) key: KeyRef<'static>,
    pub(crate) action: types::OpType,
    pub(crate) insert: bool,
    pub(crate) pred: Vec<OpId>,
}

#[derive(Debug, Clone)]
pub(crate) struct OpBuilder3<'a> {
    pub(crate) id: OpId,
    pub(crate) obj: ObjId,
    pub(crate) action: Action,
    pub(crate) key: KeyRef<'a>,
    pub(crate) value: ScalarValue<'a>,
    pub(crate) insert: bool,
    pub(crate) expand: bool,
    pub(crate) mark_name: Option<Cow<'a, str>>,
    pub(super) pred: Vec<OpId>,
}

impl OpBuilder2 {
    pub(crate) fn mark_index(&self) -> Option<MarkIndexValue> {
        match &self.action {
            types::OpType::MarkBegin(_, _) => Some(MarkIndexValue::Start(self.id())),
            types::OpType::MarkEnd(_) => Some(MarkIndexValue::End(self.id().prev())),
            _ => None,
        }
    }

    pub(crate) fn del<I: Iterator<Item = OpId>>(
        id: OpId,
        obj: ObjMeta,
        key: KeyRef<'static>,
        pred: I,
    ) -> Self {
        OpBuilder2 {
            id,
            obj,
            pos: 0,
            index: 0,
            action: types::OpType::Delete,
            key,
            insert: false,
            pred: pred.collect(),
        }
    }

    pub(crate) fn width(&self, encoding: ListEncoding) -> usize {
        if encoding == ListEncoding::List {
            1
        } else if self.is_mark() {
            0
        } else {
            TextValue::width(self.as_str()) // FASTER
        }
    }

    pub(crate) fn prop(&self) -> PropRef<'_> {
        if let KeyRef::Map(s) = &self.key {
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
        self.key().elemid().is_some()
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

    fn key(&self) -> KeyRef<'_> {
        match &self.key {
            KeyRef::Map(Cow::Owned(s)) => KeyRef::Map(Cow::Borrowed(s)),
            _ => self.key.clone(),
        }
    }
    /*
        fn elemid(&self) -> Option<ElemId> {
            match &self.key {
                KeyRef::Seq(e) => Some(*e),
                _ => None,
            }
        }
        fn map_key(&self) -> Option<&str> {
            match &self.key {
                KeyRef::Map(s) => Some(s),
                _ => None,
            }
        }
    */

    fn raw_value(&self) -> Option<Cow<'_, [u8]>> {
        self.action.to_raw()
    }
    fn meta_value(&self) -> ValueMeta {
        ValueMeta::from(self.action.value().as_ref())
    }
    fn insert(&self) -> bool {
        self.insert
    }
    fn mark_name(&self) -> Option<Cow<'_, str>> {
        self.action.mark_name().map(Cow::Borrowed)
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

    fn key(&self) -> KeyRef<'_> {
        self.key.clone()
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
    fn mark_name(&self) -> Option<Cow<'_, str>> {
        self.mark_name.clone()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Op<'a> {
    pub(crate) pos: usize,
    pub(crate) conflict: bool,
    pub(crate) id: OpId,
    pub(crate) action: Action,
    pub(crate) obj: ObjId,
    pub(crate) key: KeyRef<'a>,
    pub(crate) insert: bool,
    pub(crate) value: ScalarValue<'a>,
    pub(crate) expand: bool,
    pub(crate) mark_name: Option<Cow<'a, str>>,
    pub(super) succ_cursors: SuccCursors<'a>,
}

#[derive(Clone, Default)]
pub(crate) struct SuccCursors<'a> {
    pub(super) len: usize,
    pub(super) succ_actor: ColumnDataIter<'a, ActorCursor>,
    pub(super) succ_counter: ColumnDataIter<'a, DeltaCursor>,
    pub(super) inc_values: ColumnDataIter<'a, IntCursor>,
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

struct SuccIncCursors<'a>(SuccCursors<'a>);

impl<'a> Iterator for SuccCursors<'a> {
    type Item = OpId;

    fn next(&mut self) -> Option<Self::Item> {
        if self.len == 0 {
            None
        } else {
            let counter = self.succ_counter.next()??;
            let actor = self.succ_actor.next()??;
            self.len -= 1;
            Some(OpId::new(*counter as u64, u64::from(*actor) as usize))
        }
    }
}

impl<'a> ExactSizeIterator for SuccCursors<'a> {
    fn len(&self) -> usize {
        self.len
    }
}

impl<'a> Iterator for SuccIncCursors<'a> {
    type Item = (OpId, Option<i64>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.0.len == 0 {
            None
        } else {
            let counter = self.0.succ_counter.next()??;
            let actor = self.0.succ_actor.next()??;
            self.0.len -= 1;
            let inc = self.0.inc_values.next()?.as_deref().copied();
            let id = OpId::new(*counter as u64, u64::from(*actor) as usize);
            Some((id, inc))
        }
    }
}

impl<'a> ExactSizeIterator for SuccIncCursors<'a> {
    fn len(&self) -> usize {
        self.0.len()
    }
}

#[derive(Debug)]
pub(crate) struct SuccInsert {
    pub(crate) pos: usize,
    pub(crate) inc: Option<i64>,
    pub(crate) len: u64,
    pub(crate) sub_pos: usize,
}

impl<'a> Op<'a> {
    /*
        pub(crate) fn new() -> Self {
            Self {
                pos: 0,
                conflict: false,
                id: OpId::default(),
                action: Action::default(),
                obj: ObjId::root(),
                key: KeyRef::Seq(ElemId::head()),
                insert: false,
                value: ScalarValue::Null,
                expand: false,
                mark_name: None,
                succ_cursors: SuccCursors::default(),
            }
        }
    */

    pub(crate) fn mark_index(&self) -> Option<MarkIndexValue> {
        match (&self.action, &self.mark_name) {
            (Action::Mark, Some(_)) => Some(MarkIndexValue::Start(self.id)),
            (Action::Mark, None) => Some(MarkIndexValue::End(self.id.prev())),
            _ => None,
        }
    }
    pub(crate) fn add_succ(&self, id: OpId, inc: Option<i64>) -> SuccInsert {
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
        SuccInsert {
            pos,
            inc,
            len,
            sub_pos,
        }
    }

    pub(crate) fn as_str_cow(&self) -> Cow<'a, str> {
        if self.action == Action::Mark {
            Cow::Borrowed("")
        } else if let ScalarValue::Str(s) = &self.value {
            s.clone()
        } else {
            Cow::Borrowed("\u{fffc}")
        }
    }

    pub(crate) fn as_str(&self) -> &str {
        if self.action == Action::Mark {
            ""
        } else if let ScalarValue::Str(s) = &self.value {
            s.as_ref()
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
        OpType::from_action_and_value(self.action, &self.value, &self.mark_name, self.expand)
    }

    pub(crate) fn succ(&self) -> impl ExactSizeIterator<Item = OpId> + 'a {
        self.succ_cursors.clone()
    }

    pub(crate) fn succ_inc(&self) -> impl ExactSizeIterator<Item = (OpId, Option<i64>)> + 'a {
        SuccIncCursors(self.succ_cursors.clone())
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
            self.key.clone()
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
        match (self.action, &self.value) {
            (Action::Increment, ScalarValue::Int(i)) => Some(*i),
            (Action::Increment, ScalarValue::Uint(i)) => Some(*i as i64),
            _ => None,
        }
    }

    pub(crate) fn is_put(&self) -> bool {
        self.action == Action::Set
    }

    pub(crate) fn value(&self) -> Value<'a> {
        match &self.action() {
            OpType::Make(obj_type) => Value::Object(*obj_type),
            OpType::Put(scalar) => Value::Scalar(scalar.clone()),
            OpType::MarkBegin(_, _) => Value::Scalar(ScalarValue::Str(Cow::Borrowed("markBegin"))),
            OpType::MarkEnd(_) => Value::Scalar(ScalarValue::Str(Cow::Borrowed("markEnd"))),
            _ => panic!("cant convert op into a value - {:?}", self),
        }
    }

    pub(crate) fn hydrate_value(&self) -> hydrate::Value {
        match &self.action() {
            OpType::Make(obj_type) => hydrate::Value::from(*obj_type), // hydrate::Value::Object(*obj_type),
            OpType::Put(scalar) => hydrate::Value::from(scalar.to_owned()),
            OpType::MarkBegin(_, mark) => hydrate::Value::from(&mark.value),
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

    pub(crate) fn build3(self, pred: Vec<OpId>) -> OpBuilder3<'a> {
        OpBuilder3 {
            id: self.id,
            obj: self.obj,
            action: self.action,
            value: self.value,
            key: self.key,
            insert: self.insert,
            expand: self.expand,
            mark_name: self.mark_name,
            pred,
        }
    }

    pub(crate) fn del(id: OpId, obj: ObjId, key: KeyRef<'a>) -> Self {
        Op {
            pos: 0,
            conflict: false,
            id,
            action: Action::Delete,
            obj,
            key,
            insert: false,
            value: ScalarValue::Null,
            expand: false,
            mark_name: None,
            succ_cursors: SuccCursors::default(),
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

pub(crate) trait AsChangeOp {
    fn obj_actor(op: &Self) -> Option<Cow<'_, ActorIdx>>;
    fn obj_ctr(op: &Self) -> Option<Cow<'_, u64>>;
    fn key_actor(op: &Self) -> Option<Cow<'_, ActorIdx>>;
    fn key_ctr(op: &Self) -> Option<Cow<'_, i64>>;
    fn key_str(op: &Self) -> Option<Cow<'_, str>>;
    fn insert(op: &Self) -> Option<Cow<'_, bool>>;
    fn action(op: &Self) -> Option<Cow<'_, Action>>;
    fn value(op: &Self) -> Option<Cow<'_, [u8]>>;
    fn value_meta(op: &Self) -> Option<Cow<'_, ValueMeta>>;
    fn pred_count(op: &Self) -> Option<Cow<'_, u64>>;
    fn expand(op: &Self) -> Option<Cow<'_, bool>>;
    fn mark_name(op: &Self) -> Option<Cow<'_, str>>;
    fn op_id_ctr(op: &Self) -> u64;
    fn size_estimate(op: &Self) -> usize;
    fn pred(op: &Self) -> &[OpId];

    fn id_actor(id: &OpId) -> Option<Cow<'_, ActorIdx>> {
        Some(Cow::Owned(id.actoridx()))
    }

    fn id_ctr(id: &OpId) -> Option<Cow<'_, i64>> {
        Some(Cow::Owned(id.icounter()))
    }
}

impl<T: AsChangeOp> AsChangeOp for Option<T> {
    fn obj_actor(op: &Self) -> Option<Cow<'_, ActorIdx>> {
        T::obj_actor(op.as_ref()?)
    }
    fn obj_ctr(op: &Self) -> Option<Cow<'_, u64>> {
        T::obj_ctr(op.as_ref()?)
    }
    fn key_actor(op: &Self) -> Option<Cow<'_, ActorIdx>> {
        T::key_actor(op.as_ref()?)
    }
    fn key_ctr(op: &Self) -> Option<Cow<'_, i64>> {
        T::key_ctr(op.as_ref()?)
    }
    fn key_str(op: &Self) -> Option<Cow<'_, str>> {
        T::key_str(op.as_ref()?)
    }
    fn insert(op: &Self) -> Option<Cow<'_, bool>> {
        T::insert(op.as_ref()?)
    }
    fn action(op: &Self) -> Option<Cow<'_, Action>> {
        T::action(op.as_ref()?)
    }
    fn value(op: &Self) -> Option<Cow<'_, [u8]>> {
        T::value(op.as_ref()?)
    }
    fn value_meta(op: &Self) -> Option<Cow<'_, ValueMeta>> {
        T::value_meta(op.as_ref()?)
    }
    fn pred_count(op: &Self) -> Option<Cow<'_, u64>> {
        T::pred_count(op.as_ref()?)
    }
    fn expand(op: &Self) -> Option<Cow<'_, bool>> {
        T::expand(op.as_ref()?)
    }
    fn mark_name(op: &Self) -> Option<Cow<'_, str>> {
        T::mark_name(op.as_ref()?)
    }
    fn op_id_ctr(op: &Self) -> u64 {
        op.as_ref().map(|o| T::op_id_ctr(o)).unwrap_or(0)
    }
    fn pred(op: &Self) -> &[OpId] {
        op.as_ref().map(T::pred).unwrap_or(&[])
    }
    fn size_estimate(op: &Self) -> usize {
        op.as_ref().map(|o| T::size_estimate(o)).unwrap_or(0)
    }
}

impl<'a> AsChangeOp for OpBuilder3<'a> {
    fn obj_actor(op: &Self) -> Option<Cow<'_, ActorIdx>> {
        op.obj.actor().map(Cow::Owned)
    }

    fn obj_ctr(op: &Self) -> Option<Cow<'_, u64>> {
        op.obj.counter().map(Cow::Owned)
    }

    fn key_actor(op: &Self) -> Option<Cow<'_, ActorIdx>> {
        op.key.actor().map(Cow::Owned)
    }
    fn key_ctr(op: &Self) -> Option<Cow<'_, i64>> {
        op.key.icounter().map(Cow::Owned)
    }
    fn key_str(op: &Self) -> Option<Cow<'_, str>> {
        op.key.key_str()
    }
    fn insert(op: &Self) -> Option<Cow<'_, bool>> {
        Some(Cow::Owned(op.insert))
    }
    fn action(op: &Self) -> Option<Cow<'_, Action>> {
        Some(Cow::Owned(op.action))
    }
    fn value(op: &Self) -> Option<Cow<'_, [u8]>> {
        op.value.to_raw()
    }
    fn value_meta(op: &Self) -> Option<Cow<'_, ValueMeta>> {
        Some(Cow::Owned(ValueMeta::from(&op.value)))
    }
    fn pred_count(op: &Self) -> Option<Cow<'_, u64>> {
        Some(Cow::Owned(op.pred.len() as u64))
    }
    fn expand(op: &Self) -> Option<Cow<'_, bool>> {
        Some(Cow::Owned(op.expand))
    }
    fn mark_name(op: &Self) -> Option<Cow<'_, str>> {
        op.mark_name.clone()
    }
    fn op_id_ctr(op: &Self) -> u64 {
        op.id.counter()
    }
    fn pred(op: &Self) -> &[OpId] {
        op.pred.as_slice()
    }
    fn size_estimate(op: &Self) -> usize {
        // largest in our bestiary was 23
        op.value.to_raw().map(|s| s.len()).unwrap_or(0) + 25
    }
}

impl AsChangeOp for OpBuilder2 {
    fn obj_actor(op: &Self) -> Option<Cow<'_, ActorIdx>> {
        op.obj.id.actor().map(Cow::Owned)
    }

    fn obj_ctr(op: &Self) -> Option<Cow<'_, u64>> {
        op.obj.id.counter().map(Cow::Owned)
    }

    fn key_actor(op: &Self) -> Option<Cow<'_, ActorIdx>> {
        op.key.actor().map(Cow::Owned)
    }
    fn key_ctr(op: &Self) -> Option<Cow<'_, i64>> {
        op.key.icounter().map(Cow::Owned)
    }
    fn key_str(op: &Self) -> Option<Cow<'_, str>> {
        op.key.key_str()
    }
    fn insert(op: &Self) -> Option<Cow<'_, bool>> {
        Some(Cow::Owned(op.insert))
    }
    fn action(op: &Self) -> Option<Cow<'_, Action>> {
        Some(Cow::Owned(op.action.action()))
    }
    fn value(op: &Self) -> Option<Cow<'_, [u8]>> {
        op.action.to_raw()
    }
    fn value_meta(op: &Self) -> Option<Cow<'_, ValueMeta>> {
        Some(Cow::Owned(ValueMeta::from(op.action.value().as_ref())))
    }
    fn pred_count(op: &Self) -> Option<Cow<'_, u64>> {
        Some(Cow::Owned(op.pred.len() as u64))
    }
    fn expand(op: &Self) -> Option<Cow<'_, bool>> {
        Some(Cow::Owned(op.action.expand()))
    }
    fn mark_name(op: &Self) -> Option<Cow<'_, str>> {
        op.action.mark_name().map(Cow::Borrowed)
    }
    fn op_id_ctr(op: &Self) -> u64 {
        op.id.counter()
    }
    fn pred(op: &Self) -> &[OpId] {
        op.pred.as_slice()
    }
    fn size_estimate(op: &Self) -> usize {
        op.action.to_raw().map(|s| s.len()).unwrap_or(0) + 25
    }
}

pub(super) trait OpLike: std::fmt::Debug {
    fn id(&self) -> OpId;
    fn obj(&self) -> ObjId;
    fn action(&self) -> Action;
    fn key(&self) -> KeyRef<'_>;
    fn raw_value(&self) -> Option<Cow<'_, [u8]>>; // allocation
    fn meta_value(&self) -> ValueMeta;
    fn insert(&self) -> bool;
    fn expand(&self) -> bool;
    // allocation
    fn succ(&self) -> Vec<OpId> {
        vec![]
    }
    fn mark_name(&self) -> Option<Cow<'_, str>>;
}
