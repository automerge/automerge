use super::hexane::{ColumnDataIter, DeltaCursor, IntCursor};
use super::op_set::{MarkIndexBuilder, ObjInfo, OpSet, ResolvedAction};
use super::types::{Action, ActorCursor, ActorIdx, KeyRef, MarkData, OpType, PropRef, ScalarValue};
use super::{ValueMeta, ValueRef};

use crate::clock::Clock;
use crate::error::AutomergeError;
use crate::exid::ExId;
use crate::types;
use crate::types::{ElemId, ObjId, ObjMeta, ObjType, OpId, SequenceType};
use crate::{hydrate, TextEncoding};

use std::borrow::Cow;
use std::cmp::Ordering;
use std::fmt::Debug;

pub(crate) trait AsBuilder: Debug {
    fn as_builder(&self) -> &OpBuilder<'_>;
}

impl AsBuilder for ChangeOp {
    fn as_builder(&self) -> &OpBuilder<'static> {
        &self.bld
    }
}

impl<'a> AsBuilder for OpBuilder<'a> {
    fn as_builder(&self) -> &OpBuilder<'a> {
        self
    }
}

impl<'a> AsBuilder for &OpBuilder<'a> {
    fn as_builder(&self) -> &OpBuilder<'a> {
        self
    }
}

impl AsBuilder for TxOp {
    fn as_builder(&self) -> &OpBuilder<'static> {
        &self.bld
    }
}

impl AsBuilder for &TxOp {
    fn as_builder(&self) -> &OpBuilder<'static> {
        &self.bld
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ChangeOp {
    pub(crate) succ: Vec<(OpId, Option<i64>)>,
    pub(crate) pos: Option<usize>,
    pub(crate) subsort: usize,
    pub(crate) conflicted: bool,
    pub(crate) bld: OpBuilder<'static>,
}

impl ChangeOp {
    pub(crate) fn prop_static(&self) -> Option<PropRef<'static>> {
        match &self.bld.key {
            KeyRef::Map(s) => Some(PropRef::Map(Cow::Owned(String::from(s.as_ref())))),
            _ => None,
        }
    }

    pub(crate) fn prop(&self) -> Option<PropRef<'_>> {
        match &self.bld.key {
            KeyRef::Map(Cow::Owned(s)) => Some(PropRef::Map(Cow::Borrowed(s))),
            KeyRef::Map(Cow::Borrowed(s)) => Some(PropRef::Map(Cow::Borrowed(s))),
            _ => None,
        }
    }

    pub(crate) fn mark_data(&self) -> Option<MarkData<'static>> {
        let name = self.bld.mark_name.as_ref()?.clone();
        let value = self.bld.value.clone();
        Some(MarkData { name, value })
    }

    pub(crate) fn hydrate_value(&self, text_encoding: TextEncoding) -> hydrate::Value {
        self.bld.hydrate_value(text_encoding)
    }

    pub(crate) fn hydrate_value_and_fix_counters(
        &self,
        text_encoding: TextEncoding,
    ) -> hydrate::Value {
        if self.bld.action == Action::Set {
            if let ScalarValue::Counter(c) = &self.bld.value {
                let inc: i64 = self.succ.iter().filter_map(|(_, inc)| *inc).sum();
                hydrate::Value::Scalar(types::ScalarValue::counter(c + inc))
            } else {
                hydrate::Value::Scalar(self.bld.value.to_owned())
            }
        } else {
            self.bld.hydrate_value(text_encoding)
        }
    }

    pub(crate) fn width(&self, seq_type: SequenceType, text_encoding: TextEncoding) -> usize {
        self.bld.width(seq_type, text_encoding)
    }

    pub(crate) fn visible(&self) -> bool {
        !(self.bld.is_inc() || self.bld.is_delete() || self.has_succ())
    }

    pub(crate) fn has_succ(&self) -> bool {
        self.succ.iter().any(|(_, inc)| inc.is_none())
    }

    pub(crate) fn insert(&self) -> bool {
        self.bld.insert
    }

    pub(crate) fn is_set_or_make(&self) -> bool {
        matches!(
            self.bld.action,
            Action::Set | Action::MakeMap | Action::MakeList | Action::MakeText | Action::MakeTable
        )
    }

    pub(crate) fn action(&self) -> Action {
        self.bld.action
    }
    pub(crate) fn value(&self) -> &ScalarValue<'static> {
        &self.bld.value
    }

    pub(crate) fn key(&self) -> &KeyRef<'static> {
        &self.bld.key
    }

    pub(crate) fn pred(&self) -> &[OpId] {
        &self.bld.pred
    }

    pub(crate) fn id(&self) -> OpId {
        self.bld.id
    }

    pub(crate) fn elemid_or_key(&self) -> KeyRef<'_> {
        if self.bld.insert {
            KeyRef::Seq(ElemId(self.bld.id))
        } else {
            match &self.bld.key {
                KeyRef::Map(Cow::Owned(s)) => KeyRef::Map(Cow::Borrowed(s)),
                _ => self.bld.key.clone(),
            }
        }
    }

    pub(crate) fn get_increment_value(&self) -> Option<i64> {
        match (self.bld.action, &self.bld.value) {
            (Action::Increment, ScalarValue::Int(i)) => Some(*i),
            (Action::Increment, ScalarValue::Uint(i)) => Some(*i as i64),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TxOp {
    pub(crate) obj_type: ObjType,
    pub(crate) index: usize,
    pub(crate) pos: usize,
    pub(crate) noop: bool,
    pub(crate) bld: OpBuilder<'static>,
}

#[derive(Debug, Clone)]
pub(crate) struct OpBuilder<'a> {
    pub(crate) id: OpId,
    pub(crate) obj: ObjId,
    pub(crate) action: Action,
    pub(crate) key: KeyRef<'a>,
    pub(crate) value: ScalarValue<'a>,
    pub(crate) insert: bool,
    pub(crate) expand: bool,
    pub(crate) mark_name: Option<Cow<'a, str>>,
    pub(crate) pred: Vec<OpId>,
}

impl OpBuilder<'_> {
    pub(crate) fn mark_index(&self) -> Option<MarkIndexBuilder> {
        match (self.action, &self.mark_name) {
            (Action::Mark, Some(name)) => {
                let name = Cow::Owned(name.to_string());
                let value = self.value.clone().into_owned();
                let data = MarkData { name, value };
                Some(MarkIndexBuilder::Start(self.id, data))
            }
            (Action::Mark, None) => Some(MarkIndexBuilder::End(self.id.prev())),
            _ => None,
        }
    }

    pub(crate) fn width(&self, seq_type: SequenceType, text_encoding: TextEncoding) -> usize {
        match seq_type {
            SequenceType::List => 1,
            SequenceType::Text if self.is_mark() => 0,
            SequenceType::Text => text_encoding.width(self.as_str()),
        }
    }

    pub(crate) fn is_inc(&self) -> bool {
        self.action == Action::Increment
    }

    pub(crate) fn is_mark(&self) -> bool {
        self.action == Action::Mark
    }

    pub(crate) fn as_str(&self) -> &str {
        match (self.action, &self.value) {
            (Action::Set, ScalarValue::Str(s)) => s,
            (Action::Mark, _) => "",
            _ => "\u{fffc}",
        }
    }

    pub(crate) fn is_delete(&self) -> bool {
        self.action == Action::Delete
    }

    pub(crate) fn get_increment_value(&self) -> Option<i64> {
        match (self.action, &self.value) {
            (Action::Increment, ScalarValue::Int(i)) => Some(*i),
            (Action::Increment, ScalarValue::Uint(i)) => Some(*i as i64),
            _ => None,
        }
    }

    pub(crate) fn hydrate_value(&self, text_encoding: TextEncoding) -> hydrate::Value {
        // FIXME
        match self.action {
            Action::Set => hydrate::Value::Scalar(self.value.to_owned()),
            Action::MakeMap => hydrate::Value::map(),
            Action::MakeList => hydrate::Value::list(),
            Action::MakeText => hydrate::Value::new(ObjType::Text, text_encoding),
            Action::MakeTable => hydrate::Value::new(ObjType::Table, text_encoding),
            //Action::Mark if self.mark_name.is_some() => hydrate::Value::new(&self.value, text_rep),
            //Action::Mark => hydrate::Value::Scalar("markEnd".into()),
            _ => panic!("cant convert op into a value"),
        }
    }
}

impl TxOp {
    pub(crate) fn id(&self) -> OpId {
        self.bld.id
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn list(
        id: OpId,
        obj: ObjMeta,
        pos: usize,
        index: usize,
        action: ResolvedAction,
        elemid: ElemId,
        pred: Vec<OpId>,
    ) -> Self {
        let (op_type, noop) = match action {
            ResolvedAction::ConflictResolution(action) => (action, true),
            ResolvedAction::VisibleUpdate(action) => (action, false),
        };
        let (action, value, expand, mark_name) = op_type.decompose();
        TxOp {
            obj_type: obj.typ,
            pos,
            index,
            noop,
            bld: OpBuilder {
                id,
                obj: obj.id,
                action,
                value,
                expand,
                mark_name,
                key: KeyRef::Seq(elemid),
                insert: false,
                pred,
            },
        }
    }

    pub(crate) fn map(
        id: OpId,
        obj: ObjMeta,
        pos: usize,
        action: ResolvedAction,
        prop: String,
        pred: Vec<OpId>,
    ) -> Self {
        let (action, noop) = match action {
            ResolvedAction::ConflictResolution(action) => (action, true),
            ResolvedAction::VisibleUpdate(action) => (action, false),
        };
        let (action, value, expand, mark_name) = action.clone().decompose();
        TxOp {
            obj_type: obj.typ,
            index: 0,
            pos,
            noop,
            bld: OpBuilder {
                id,
                obj: obj.id,
                value,
                action,
                expand,
                mark_name,
                key: KeyRef::Map(Cow::Owned(prop)),
                insert: false,
                pred,
            },
        }
    }

    pub(crate) fn insert(
        id: OpId,
        obj: ObjMeta,
        pos: usize,
        index: usize,
        _action: types::OpType,
        elemid: ElemId,
    ) -> Self {
        let (action, value, expand, mark_name) = _action.clone().decompose();
        TxOp {
            obj_type: obj.typ,
            pos,
            index,
            noop: false,
            bld: OpBuilder {
                id,
                obj: obj.id,
                action,
                value,
                expand,
                mark_name,
                key: KeyRef::Seq(elemid),
                insert: true,
                pred: vec![],
            },
        }
    }

    pub(crate) fn insert_val(
        id: OpId,
        obj: ObjMeta,
        pos: usize,
        value: types::ScalarValue,
        elemid: ElemId,
    ) -> Self {
        let _action = types::OpType::Put(value);
        let (action, value, expand, mark_name) = _action.clone().decompose();
        TxOp {
            pos,
            index: 0,
            obj_type: obj.typ,
            noop: false,
            bld: OpBuilder {
                id,
                obj: obj.id,
                action,
                value,
                expand,
                mark_name,
                key: KeyRef::Seq(elemid),
                insert: true,
                pred: vec![],
            },
        }
    }

    pub(crate) fn insert_obj(
        id: OpId,
        obj: ObjMeta,
        pos: usize,
        index: usize,
        obj_type: types::ObjType,
        elemid: ElemId,
    ) -> Self {
        let _action = types::OpType::Make(obj_type);
        let (action, value, expand, mark_name) = _action.clone().decompose();
        TxOp {
            obj_type: obj.typ,
            pos,
            index,
            noop: false,
            bld: OpBuilder {
                id,
                obj: obj.id,
                action,
                value,
                expand,
                mark_name,
                key: elemid.into(),
                insert: true,
                pred: vec![],
            },
        }
    }

    pub(crate) fn list_del<I: IntoIterator<Item = OpId>>(
        id: OpId,
        obj: ObjMeta,
        index: usize,
        elemid: ElemId,
        pred: I,
    ) -> Self {
        let _action = types::OpType::Delete;
        let (action, value, expand, mark_name) = _action.clone().decompose();
        TxOp {
            obj_type: obj.typ,
            pos: 0,
            index,
            noop: false,
            bld: OpBuilder {
                id,
                obj: obj.id,
                action,
                value,
                expand,
                mark_name,
                key: elemid.into(),
                insert: false,
                pred: pred.into_iter().collect(),
            },
        }
    }

    pub(crate) fn prop(&self) -> PropRef<'_> {
        if let KeyRef::Map(s) = &self.bld.key {
            PropRef::Map(s.clone())
        } else {
            PropRef::Seq(self.index)
        }
    }

    pub(crate) fn hydrate_value(&self, text_encoding: TextEncoding) -> hydrate::Value {
        self.bld.hydrate_value(text_encoding)
    }

    pub(crate) fn get_increment_value(&self) -> Option<i64> {
        self.bld.get_increment_value()
    }

    pub(crate) fn is_delete(&self) -> bool {
        self.bld.is_delete()
    }

    pub(crate) fn as_str(&self) -> &str {
        self.bld.as_str()
    }

    pub(crate) fn is_mark(&self) -> bool {
        self.bld.is_mark()
    }
}

impl OpLike for &TxOp {
    type SuccIter<'b>
        = std::array::IntoIter<OpId, 0>
    where
        Self: 'b;

    fn mark_index(op: &Self) -> Option<MarkIndexBuilder> {
        op.bld.mark_index()
    }

    fn width(op: &Self, seq_type: SequenceType, text_encoding: TextEncoding) -> u64 {
        op.bld.width(seq_type, text_encoding) as u64
    }

    fn visible(op: &Self) -> bool {
        !op.bld.is_inc()
    }

    fn obj_info(&self) -> Option<ObjInfo> {
        let obj_type = ObjType::try_from(self.bld.action).ok()?;
        let parent = self.bld.obj;
        Some(ObjInfo { parent, obj_type })
    }

    fn id_actor(op: &Self) -> ActorIdx {
        op.as_builder().id.actoridx()
    }
    fn id_ctr(op: &Self) -> i64 {
        op.as_builder().id.icounter()
    }

    fn succ_inc(_op: &Self) -> Box<dyn Iterator<Item = Option<i64>> + '_> {
        let v: Vec<Option<i64>> = vec![];
        Box::new(v.into_iter())
    }

    fn succ(&self) -> Self::SuccIter<'_> {
        [].into_iter()
    }

    fn id(&self) -> OpId {
        self.as_builder().id
    }
    fn obj(&self) -> ObjId {
        self.as_builder().obj
    }
    fn action(o: &Self) -> Action {
        o.as_builder().action
    }

    fn key_str(o: &Self) -> Option<&str> {
        match &o.as_builder().key {
            KeyRef::Map(Cow::Owned(s)) => Some(s),
            KeyRef::Map(Cow::Borrowed(s)) => Some(*s),
            _ => None,
        }
    }

    fn key(&self) -> KeyRef<'_> {
        match &self.as_builder().key {
            KeyRef::Map(Cow::Owned(s)) => KeyRef::Map(Cow::Borrowed(s)),
            _ => self.as_builder().key.clone(),
        }
    }

    fn raw_value(&self) -> Option<Cow<'_, [u8]>> {
        self.as_builder().value.to_raw()
    }
    fn meta_value(&self) -> ValueMeta {
        ValueMeta::from(&self.as_builder().value)
    }
    fn insert(o: &Self) -> bool {
        o.as_builder().insert
    }
    fn mark_name(o: &Self) -> Option<Cow<'_, str>> {
        o.as_builder().mark_name.as_deref().map(Cow::Borrowed)
    }
    fn expand(o: &Self) -> bool {
        o.as_builder().expand
    }
}

impl OpLike for TxOp {
    type SuccIter<'b> = std::array::IntoIter<OpId, 0>;

    fn mark_index(op: &Self) -> Option<MarkIndexBuilder> {
        op.bld.mark_index()
    }

    fn width(op: &Self, seq_type: SequenceType, text_encoding: TextEncoding) -> u64 {
        op.bld.width(seq_type, text_encoding) as u64
    }

    fn visible(op: &Self) -> bool {
        !op.bld.is_inc()
    }

    fn obj_info(&self) -> Option<ObjInfo> {
        let obj_type = ObjType::try_from(self.bld.action).ok()?;
        let parent = self.bld.obj;
        Some(ObjInfo { parent, obj_type })
    }

    fn id_actor(op: &Self) -> ActorIdx {
        op.as_builder().id.actoridx()
    }
    fn id_ctr(op: &Self) -> i64 {
        op.as_builder().id.icounter()
    }

    fn succ_inc(_op: &Self) -> Box<dyn Iterator<Item = Option<i64>>> {
        let v: Vec<Option<i64>> = vec![];
        Box::new(v.into_iter())
    }

    fn succ(&self) -> Self::SuccIter<'_> {
        [].into_iter()
    }

    fn id(&self) -> OpId {
        self.as_builder().id
    }
    fn obj(&self) -> ObjId {
        self.as_builder().obj
    }
    fn action(o: &Self) -> Action {
        o.as_builder().action
    }

    fn key_str(o: &Self) -> Option<&str> {
        match &o.as_builder().key {
            KeyRef::Map(Cow::Owned(s)) => Some(s),
            KeyRef::Map(Cow::Borrowed(s)) => Some(*s),
            _ => None,
        }
    }

    fn key(&self) -> KeyRef<'_> {
        match &self.as_builder().key {
            KeyRef::Map(Cow::Owned(s)) => KeyRef::Map(Cow::Borrowed(s)),
            _ => self.as_builder().key.clone(),
        }
    }

    fn raw_value(&self) -> Option<Cow<'_, [u8]>> {
        self.as_builder().value.to_raw()
    }
    fn meta_value(&self) -> ValueMeta {
        ValueMeta::from(&self.as_builder().value)
    }
    fn insert(o: &Self) -> bool {
        o.as_builder().insert
    }
    fn mark_name(o: &Self) -> Option<Cow<'_, str>> {
        o.as_builder().mark_name.as_deref().map(Cow::Borrowed)
    }
    fn expand(o: &Self) -> bool {
        o.as_builder().expand
    }
}

impl OpLike for ChangeOp {
    type SuccIter<'b> = Box<dyn ExactSizeIterator<Item = OpId> + 'b>;

    fn mark_index(op: &Self) -> Option<MarkIndexBuilder> {
        op.bld.mark_index()
    }

    fn width(op: &Self, seq_type: SequenceType, text_encoding: TextEncoding) -> u64 {
        if Self::visible(op) {
            op.bld.width(seq_type, text_encoding) as u64
        } else {
            0
        }
    }

    fn visible(op: &Self) -> bool {
        !(op.bld.is_inc() || op.bld.is_delete() || op.succ.iter().any(|(_, inc)| inc.is_none()))
    }

    fn top(op: &Self) -> bool {
        !op.conflicted && Self::visible(op)
    }

    fn obj_info(&self) -> Option<ObjInfo> {
        let obj_type = ObjType::try_from(self.bld.action).ok()?;
        let parent = self.bld.obj;
        Some(ObjInfo { parent, obj_type })
    }

    fn id_actor(op: &Self) -> ActorIdx {
        op.as_builder().id.actoridx()
    }
    fn id_ctr(op: &Self) -> i64 {
        op.as_builder().id.icounter()
    }

    fn succ_inc(op: &Self) -> Box<dyn Iterator<Item = Option<i64>> + '_> {
        Box::new(op.succ.iter().map(|o| o.1))
    }

    fn succ(&self) -> Self::SuccIter<'_> {
        Box::new(self.succ.iter().map(|o| o.0))
    }

    fn id(&self) -> OpId {
        self.as_builder().id
    }
    fn obj(&self) -> ObjId {
        self.as_builder().obj
    }
    fn action(o: &Self) -> Action {
        o.as_builder().action
    }

    fn key_str(o: &Self) -> Option<&str> {
        match &o.as_builder().key {
            KeyRef::Map(Cow::Owned(s)) => Some(s),
            KeyRef::Map(Cow::Borrowed(s)) => Some(*s),
            _ => None,
        }
    }

    fn key(&self) -> KeyRef<'_> {
        match &self.as_builder().key {
            KeyRef::Map(Cow::Owned(s)) => KeyRef::Map(Cow::Borrowed(s)),
            _ => self.as_builder().key.clone(),
        }
    }

    fn raw_value(&self) -> Option<Cow<'_, [u8]>> {
        self.as_builder().value.to_raw()
    }
    fn meta_value(&self) -> ValueMeta {
        ValueMeta::from(&self.as_builder().value)
    }
    fn insert(o: &Self) -> bool {
        o.as_builder().insert
    }
    fn mark_name(o: &Self) -> Option<Cow<'_, str>> {
        o.as_builder().mark_name.as_deref().map(Cow::Borrowed)
    }
    fn expand(o: &Self) -> bool {
        o.as_builder().expand
    }
}

impl PartialEq<TxOp> for TxOp {
    fn eq(&self, other: &Self) -> bool {
        self.bld.id == other.bld.id
    }
}

impl Eq for TxOp {}

impl PartialOrd for TxOp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TxOp {
    fn cmp(&self, other: &Self) -> Ordering {
        self.bld.id.cmp(&other.bld.id)
    }
}

impl<'a> OpLike for Op<'a> {
    type SuccIter<'b>
        = SuccCursors<'a>
    where
        Self: 'b;

    fn mark_index(op: &Self) -> Option<MarkIndexBuilder> {
        op.mark_index()
    }

    fn width(op: &Self, seq_type: SequenceType, text_encoding: TextEncoding) -> u64 {
        op.width(seq_type, text_encoding) as u64
    }

    fn visible(_op: &Self) -> bool {
        true // FIXME
    }

    fn obj_info(&self) -> Option<ObjInfo> {
        let obj_type = ObjType::try_from(self.action).ok()?;
        let parent = self.obj;
        Some(ObjInfo { parent, obj_type })
    }

    fn id_actor(op: &Self) -> ActorIdx {
        op.id.actoridx()
    }
    fn id_ctr(op: &Self) -> i64 {
        op.id.icounter()
    }

    fn id(&self) -> OpId {
        self.id
    }

    fn obj(&self) -> ObjId {
        self.obj
    }

    fn action(o: &Self) -> Action {
        o.action
    }

    fn key_str(o: &Self) -> Option<&str> {
        match &o.key {
            KeyRef::Map(Cow::Owned(s)) => Some(s),
            KeyRef::Map(Cow::Borrowed(s)) => Some(*s),
            _ => None,
        }
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

    fn succ_inc(op: &Self) -> Box<dyn Iterator<Item = Option<i64>> + '_> {
        Box::new(IncCursors(op.succ_cursors.clone()))
    }

    fn succ(&self) -> Self::SuccIter<'_> {
        self.succ()
    }

    fn insert(o: &Self) -> bool {
        o.insert
    }

    fn expand(o: &Self) -> bool {
        o.expand
    }

    fn mark_name(o: &Self) -> Option<Cow<'_, str>> {
        o.mark_name.clone()
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
    pub(crate) fn with_inc(self) -> SuccIncCursors<'a> {
        SuccIncCursors(self)
    }
}

impl std::fmt::Debug for SuccCursors<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SuccCursors")
            .field("len", &self.len)
            .finish()
    }
}

pub(crate) struct SuccIncCursors<'a>(SuccCursors<'a>);

struct IncCursors<'a>(SuccCursors<'a>);

impl Iterator for SuccCursors<'_> {
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

impl ExactSizeIterator for SuccCursors<'_> {
    fn len(&self) -> usize {
        self.len
    }
}

impl Iterator for SuccIncCursors<'_> {
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

impl Iterator for IncCursors<'_> {
    type Item = Option<i64>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.0.len == 0 {
            None
        } else {
            self.0.len -= 1;
            let inc = self.0.inc_values.next()?.as_deref().copied();
            Some(inc)
        }
    }
}

impl ExactSizeIterator for SuccIncCursors<'_> {
    fn len(&self) -> usize {
        self.0.len()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SuccInsert {
    pub(crate) id: OpId,
    pub(crate) pos: usize,
    pub(crate) inc: Option<i64>,
    pub(crate) len: u64,
    pub(crate) sub_pos: usize,
}

impl<'a> Op<'a> {
    pub(crate) fn mark_index(&self) -> Option<MarkIndexBuilder> {
        match (&self.action, &self.mark_name) {
            (Action::Mark, Some(name)) => {
                let name = Cow::Owned(name.to_string());
                let value = self.value.clone().into_owned();
                let data = MarkData { name, value };
                Some(MarkIndexBuilder::Start(self.id, data))
            }
            (Action::Mark, None) => Some(MarkIndexBuilder::End(self.id.prev())),
            _ => None,
        }
    }

    pub(crate) fn add_succ(&self, id: OpId, mut inc: Option<i64>) -> SuccInsert {
        let pos = self.pos;
        let mut succ = self.succ_cursors.clone();
        if inc.is_some() && !self.is_counter() {
            inc = None;
        }
        let len = succ.len() as u64;
        let mut sub_pos = succ.pos();
        while let Some(i) = succ.next() {
            if i > id {
                break;
            }
            sub_pos = succ.pos();
        }
        SuccInsert {
            id,
            pos,
            inc,
            len,
            sub_pos,
        }
    }

    pub(crate) fn fix_counter(&mut self, clock: Option<&Clock>) {
        if let ScalarValue::Counter(n) = self.value {
            let mut inc = 0;
            for (i, val) in self.succ_inc() {
                if let Some(v) = val {
                    if let Some(c) = clock {
                        if c.covers(&i) {
                            inc += v;
                        }
                    } else {
                        inc += v;
                    }
                }
            }
            self.value = ScalarValue::Counter(n + inc);
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

    pub(crate) fn width(&self, seq_type: SequenceType, text_encoding: TextEncoding) -> usize {
        match seq_type {
            SequenceType::List => 1,
            SequenceType::Text if self.action == Action::Mark => 0,
            SequenceType::Text => text_encoding.width(self.as_str()),
        }
    }

    pub(crate) fn op_type(&self) -> OpType<'a> {
        OpType::from_action_and_value(self.action, &self.value, &self.mark_name, self.expand)
    }

    pub(crate) fn succ(&self) -> SuccCursors<'a> {
        self.succ_cursors.clone()
    }

    pub(crate) fn succ_inc(&self) -> impl ExactSizeIterator<Item = (OpId, Option<i64>)> + 'a {
        self.succ_cursors.clone().with_inc()
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

    pub(crate) fn step(&self, stepper: &mut OpStepper<'a>) -> bool {
        if self.obj != stepper.obj {
            let ok = self.obj > stepper.obj;
            stepper.obj = self.obj;
            stepper.key = self.elemid_or_key();
            stepper.id = self.id;
            ok
        } else {
            let ok = if self.elemid_or_key() == stepper.key {
                self.id > stepper.id
            } else {
                match (&self.key, &stepper.key) {
                    (KeyRef::Map(s1), KeyRef::Map(s2)) => s1 > s2,
                    (KeyRef::Seq(e1), KeyRef::Seq(e2)) if self.insert => {
                        e1 == e2 || ElemId(self.id) < *e2
                    }
                    _ => false,
                }
            };
            stepper.key = self.elemid_or_key();
            stepper.id = self.id;
            ok
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
        (self.value().into_value(), self.exid(op_set))
    }

    pub(crate) fn get_increment_value(&self) -> Option<i64> {
        match (self.action, &self.value) {
            (Action::Increment, ScalarValue::Int(i)) => Some(*i),
            (Action::Increment, ScalarValue::Uint(i)) => Some(*i as i64),
            _ => None,
        }
    }

    pub(crate) fn value(&self) -> ValueRef<'a> {
        match &self.action() {
            OpType::Make(obj_type) => ValueRef::Object(*obj_type),
            OpType::Put(scalar) => ValueRef::Scalar(scalar.clone()),
            OpType::MarkBegin(_, _) => {
                ValueRef::Scalar(ScalarValue::Str(Cow::Borrowed("markBegin")))
            }
            OpType::MarkEnd(_) => ValueRef::Scalar(ScalarValue::Str(Cow::Borrowed("markEnd"))),
            _ => panic!("cant convert op into a value - {:?}", self),
        }
    }

    pub(crate) fn hydrate_value(&self, text_encoding: TextEncoding) -> hydrate::Value {
        match &self.action() {
            OpType::Make(obj_type) => hydrate::Value::new(*obj_type, text_encoding),
            OpType::Put(scalar) => hydrate::Value::Scalar(scalar.to_owned()),
            OpType::MarkBegin(_, mark) => hydrate::Value::new(&mark.value, text_encoding),
            OpType::MarkEnd(_) => hydrate::Value::Scalar("markEnd".into()),
            _ => panic!("cant convert op into a value"),
        }
    }

    pub(crate) fn action(&self) -> OpType<'a> {
        self.op_type()
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

    pub(crate) fn build(self, pred: Vec<OpId>) -> OpBuilder<'a> {
        OpBuilder {
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

    pub(crate) fn visible(&self) -> bool {
        if self.is_inc() {
            false
        } else if self.is_counter() {
            !self.succ_inc().any(|(_, inc)| inc.is_none())
        } else {
            self.succ_cursors.len() == 0
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

    pub(crate) fn prop(&self) -> Option<PropRef<'a>> {
        let key_str = self.key.key_str()?;
        Some(PropRef::Map(key_str))
    }
}

impl PartialEq<Op<'_>> for Op<'_> {
    fn eq(&self, other: &Op<'_>) -> bool {
        self.id == other.id
    }
}

impl PartialOrd for Op<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Op<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl std::hash::Hash for Op<'_> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state)
    }
}

impl Eq for Op<'_> {}

// TODO - AS ChangeOp and OpLike fill almost the exact same function

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
}

impl<B: AsBuilder> AsChangeOp for B {
    fn obj_actor(op: &Self) -> Option<Cow<'_, ActorIdx>> {
        op.as_builder().obj.actor().map(Cow::Owned)
    }

    fn obj_ctr(op: &Self) -> Option<Cow<'_, u64>> {
        op.as_builder().obj.counter().map(Cow::Owned)
    }

    fn key_actor(op: &Self) -> Option<Cow<'_, ActorIdx>> {
        op.as_builder().key.actor().map(Cow::Owned)
    }
    fn key_ctr(op: &Self) -> Option<Cow<'_, i64>> {
        op.as_builder().key.icounter().map(Cow::Owned)
    }
    fn key_str(op: &Self) -> Option<Cow<'_, str>> {
        op.as_builder().key.key_str()
    }
    fn insert(op: &Self) -> Option<Cow<'_, bool>> {
        Some(Cow::Owned(op.as_builder().insert))
    }
    fn action(op: &Self) -> Option<Cow<'_, Action>> {
        Some(Cow::Owned(op.as_builder().action))
    }
    fn value(op: &Self) -> Option<Cow<'_, [u8]>> {
        op.as_builder().value.to_raw()
    }
    fn value_meta(op: &Self) -> Option<Cow<'_, ValueMeta>> {
        Some(Cow::Owned(ValueMeta::from(&op.as_builder().value)))
    }
    fn pred_count(op: &Self) -> Option<Cow<'_, u64>> {
        Some(Cow::Owned(op.as_builder().pred.len() as u64))
    }
    fn expand(op: &Self) -> Option<Cow<'_, bool>> {
        Some(Cow::Owned(op.as_builder().expand))
    }
    fn mark_name(op: &Self) -> Option<Cow<'_, str>> {
        op.as_builder().mark_name.clone()
    }
    fn op_id_ctr(op: &Self) -> u64 {
        op.as_builder().id.counter()
    }
    fn pred(op: &Self) -> &[OpId] {
        op.as_builder().pred.as_slice()
    }
}

pub(crate) trait OpLike: Debug {
    type SuccIter<'b>: ExactSizeIterator<Item = OpId> + 'b
    where
        Self: 'b;
    fn id_actor(op: &Self) -> ActorIdx;
    fn id_ctr(op: &Self) -> i64;
    fn id(&self) -> OpId;
    fn obj(&self) -> ObjId;
    fn obj_actor(op: &Self) -> Option<ActorIdx> {
        op.obj().actor()
    }
    fn obj_ctr(op: &Self) -> Option<u64> {
        op.obj().counter()
    }
    fn action(o: &Self) -> Action;
    fn key_str(o: &Self) -> Option<&str>;
    fn key_actor(op: &Self) -> Option<ActorIdx> {
        op.key().actor()
    }
    fn key_ctr(op: &Self) -> Option<i64> {
        op.key().icounter()
    }
    fn key(&self) -> KeyRef<'_>;
    fn raw_value(&self) -> Option<Cow<'_, [u8]>>; // allocation
    fn meta_value(&self) -> ValueMeta;
    fn insert(op: &Self) -> bool;
    fn expand(op: &Self) -> bool;
    fn succ(&self) -> Self::SuccIter<'_>;
    fn succ_inc(op: &Self) -> Box<dyn Iterator<Item = Option<i64>> + '_>;
    fn mark_name(op: &Self) -> Option<Cow<'_, str>>;
    fn mark_index(op: &Self) -> Option<MarkIndexBuilder>;
    fn width(op: &Self, seq_type: SequenceType, text_encoding: TextEncoding) -> u64;
    fn visible(op: &Self) -> bool;
    fn top(op: &Self) -> bool {
        Self::visible(op)
    }
    fn obj_info(&self) -> Option<ObjInfo>;
}

#[derive(Clone, Debug)]
pub(crate) struct OpStepper<'a> {
    obj: ObjId,
    key: KeyRef<'a>,
    id: OpId,
}

impl Default for OpStepper<'_> {
    fn default() -> Self {
        OpStepper {
            obj: ObjId::root(),
            key: KeyRef::Map(Cow::Borrowed("")),
            id: OpId::default(),
        }
    }
}
