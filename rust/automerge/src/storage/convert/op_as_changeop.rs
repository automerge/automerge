use smol_str::SmolStr;
/// Types for converting an OpTree op into a `ChangeOp` or a `DocOp`
use std::borrow::Cow;

use crate::{
    convert,
    op_set2::{Key, KeyRef, MarkData, Op, OpBuilder2, OpSet, OpType},
    storage::AsChangeOp,
    types,
    types::{ActorId, ElemId, OldMarkData, OpId},
    value,
};

pub(crate) fn ob_as_actor_id<'a>(op_set: &'a OpSet, op: &'a OpBuilder2) -> ObWithMetadata<'a> {
    ObWithMetadata { op, op_set }
}

#[derive(Clone, Debug, Eq, Ord)]
pub(crate) struct OpWithMetadata<'a> {
    op: Op<'a>,
    pred: Vec<OpId>,
}

impl<'a> PartialOrd for OpWithMetadata<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.op.partial_cmp(&other.op)
    }
}

impl<'a> PartialEq for OpWithMetadata<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.op.id == other.op.id
    }
}

pub(crate) struct ObWithMetadata<'a> {
    op: &'a OpBuilder2,
    op_set: &'a OpSet,
}

impl<'a> std::fmt::Debug for ObWithMetadata<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.op.fmt(f)
    }
}

impl<'a> OpWithMetadata<'a> {
    //pub(crate) fn new(op: Op<'a>, pred: Vec<OpId>) -> Self {
    //    Self { op, pred }
    //}

    fn wrap(&self, opid: OpId) -> OpIdWithMetadata<'a> {
        OpIdWithMetadata::new(opid, self.op.op_set())
    }
}

pub(crate) struct OpIdWithMetadata<'a> {
    opid: OpId,
    actor: &'a ActorId,
}

impl<'a> OpIdWithMetadata<'a> {
    fn new(opid: OpId, osd: &'a OpSet) -> Self {
        Self {
            opid,
            actor: osd.get_actor(opid.actor()),
        }
    }
}

impl<'a> convert::OpId<&'a ActorId> for OpIdWithMetadata<'a> {
    fn counter(&self) -> u64 {
        self.opid.counter()
    }

    fn actor(&self) -> &'a ActorId {
        self.actor
    }
}

pub(crate) struct PredWithMetadata<'a> {
    op_set: &'a OpSet,
    pred: Vec<OpId>,
    offset: usize,
}

impl<'a> ExactSizeIterator for PredWithMetadata<'a> {
    fn len(&self) -> usize {
        self.pred.len()
    }
}

impl<'a> Iterator for PredWithMetadata<'a> {
    type Item = OpIdWithMetadata<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(id) = self.pred.get(self.offset) {
            self.offset += 1;
            Some(OpIdWithMetadata::new(*id, self.op_set))
        } else {
            None
        }
    }
}

pub(crate) struct PredWithMetadata2<'a> {
    ops: &'a [OpId],
    op_set: &'a OpSet,
    offset: usize,
}

impl<'a> ExactSizeIterator for PredWithMetadata2<'a> {
    fn len(&self) -> usize {
        self.ops.len()
    }
}

impl<'a> Iterator for PredWithMetadata2<'a> {
    type Item = OpIdWithMetadata<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset < self.ops.len() {
            let id = self.ops[self.offset];
            self.offset += 1;
            Some(OpIdWithMetadata::new(id, self.op_set))
        } else {
            None
        }
    }
}

/*
impl<'a> AsChangeOp<'a> for Op<'a> {
    type ActorId = &'a ActorId;
    type OpId = OpIdWithMetadata<'a>;
    type PredIter = PredWithMetadata<'a>;

    fn action(&self) -> u64 {
        self.action.into()
    }

    fn insert(&self) -> bool {
        self.insert
    }

    fn val(&self) -> Cow<'a, value::ScalarValue> {
        match self.action() {
            OpType::Make(..) | OpType::Delete | OpType::MarkEnd(..) => {
                Cow::Owned(value::ScalarValue::Null)
            }
            OpType::Increment(i) => Cow::Owned(value::ScalarValue::Int(i)),
            OpType::Put(s) => Cow::Owned(s.into_owned()),
            OpType::MarkBegin(_, MarkData { value, .. }) => Cow::Owned(value.into()),
        }
    }

    fn obj(&self) -> convert::ObjId<Self::OpId> {
        if self.obj.is_root() {
            convert::ObjId::Root
        } else {
            convert::ObjId::Op(OpIdWithMetadata::new(*self.obj.opid(), self.op_set()))
        }
    }

    fn pred(&self) -> Self::PredIter {
        PredWithMetadata {
            op: *self,
            offset: 0,
        }
    }

    fn key(&self) -> convert::Key<'a, Self::OpId> {
        match self.key {
            KeyRef::Map(k) => convert::Key::Prop(Cow::Owned(SmolStr::from(k))),
            KeyRef::Seq(e) if e.is_head() => convert::Key::Elem(convert::ElemId::Head),
            KeyRef::Seq(e) => {
                let id = OpIdWithMetadata::new(e.0, self.op_set());
                convert::Key::Elem(convert::ElemId::Op(id))
            }
        }
    }

    fn expand(&self) -> bool {
        matches!(
            self.action(),
            OpType::MarkBegin(true, _) | OpType::MarkEnd(true)
        )
    }

    fn mark_name(&self) -> Option<Cow<'a, smol_str::SmolStr>> {
        if let OpType::MarkBegin(_, MarkData { name, .. }) = self.action() {
            Some(Cow::Owned(SmolStr::from(name)))
        } else {
            None
        }
    }
}
*/

impl<'a> AsChangeOp<'a> for OpWithMetadata<'a> {
    type ActorId = &'a ActorId;
    type OpId = OpIdWithMetadata<'a>;
    //type PredIter = Iterator<Item = Self::OpId>;
    type PredIter = PredWithMetadata<'a>;

    fn action(&self) -> u64 {
        self.op.action.into()
    }

    fn insert(&self) -> bool {
        self.op.insert
    }

    fn val(&self) -> Cow<'a, value::ScalarValue> {
        match self.op.action() {
            OpType::Make(..) | OpType::Delete | OpType::MarkEnd(..) => {
                Cow::Owned(value::ScalarValue::Null)
            }
            OpType::Increment(i) => Cow::Owned(value::ScalarValue::Int(i)),
            OpType::Put(s) => Cow::Owned(s.into_owned()),
            OpType::MarkBegin(_, MarkData { value, .. }) => Cow::Owned(value.into()),
        }
    }

    fn obj(&self) -> convert::ObjId<Self::OpId> {
        if let Some(id) = self.op.obj.id() {
            convert::ObjId::Op(OpIdWithMetadata::new(*id, self.op.op_set()))
        } else {
            convert::ObjId::Root
        }
    }

    fn pred(&self) -> Self::PredIter {
        PredWithMetadata {
            op_set: self.op.op_set(),
            pred: self.pred.clone(), // FIXME clone
            offset: 0,
        }
    }

    fn key(&self) -> convert::Key<'a, Self::OpId> {
        match self.op.key {
            KeyRef::Map(k) => convert::Key::Prop(Cow::Owned(SmolStr::from(k))),
            KeyRef::Seq(e) if e.is_head() => convert::Key::Elem(convert::ElemId::Head),
            KeyRef::Seq(e) => convert::Key::Elem(convert::ElemId::Op(self.wrap(e.0))),
        }
    }

    fn expand(&self) -> bool {
        matches!(
            self.op.action(),
            OpType::MarkBegin(true, _) | OpType::MarkEnd(true)
        )
    }

    fn mark_name(&self) -> Option<Cow<'a, smol_str::SmolStr>> {
        if let OpType::MarkBegin(_, MarkData { name, .. }) = self.op.action() {
            Some(Cow::Owned(SmolStr::from(name)))
        } else {
            None
        }
    }
}

impl<'a> AsChangeOp<'a> for ObWithMetadata<'a> {
    type ActorId = &'a ActorId;
    type OpId = OpIdWithMetadata<'a>;
    type PredIter = PredWithMetadata2<'a>;

    fn action(&self) -> u64 {
        self.op.action.action().into()
    }

    fn insert(&self) -> bool {
        self.op.insert
    }

    fn val(&self) -> Cow<'a, value::ScalarValue> {
        match &self.op.action {
            types::OpType::Make(..) | types::OpType::Delete | types::OpType::MarkEnd(..) => {
                Cow::Owned(value::ScalarValue::Null)
            }
            types::OpType::Increment(i) => Cow::Owned(value::ScalarValue::Int(*i)),
            types::OpType::Put(s) => Cow::Owned(s.clone()), // borrow
            types::OpType::MarkBegin(_, OldMarkData { value, .. }) => Cow::Owned(value.clone()),
        }
    }

    fn obj(&self) -> convert::ObjId<Self::OpId> {
        if let Some(id) = self.op.obj.id.id() {
            convert::ObjId::Op(OpIdWithMetadata::new(*id, self.op_set))
        } else {
            convert::ObjId::Root
        }
    }

    fn pred(&self) -> Self::PredIter {
        Self::PredIter {
            ops: &self.op.pred,
            offset: 0,
            op_set: self.op_set,
        }
    }

    fn key(&self) -> convert::Key<'a, Self::OpId> {
        match &self.op.key {
            Key::Map(k) => convert::Key::Prop(Cow::Owned(SmolStr::from(k))),
            Key::Seq(e) if e.is_head() => convert::Key::Elem(convert::ElemId::Head),
            Key::Seq(ElemId(id)) => {
                let id = OpIdWithMetadata::new(*id, self.op_set);
                convert::Key::Elem(convert::ElemId::Op(id))
            }
        }
    }

    fn expand(&self) -> bool {
        matches!(
            &self.op.action,
            types::OpType::MarkBegin(true, _) | types::OpType::MarkEnd(true)
        )
    }

    fn mark_name(&self) -> Option<Cow<'a, smol_str::SmolStr>> {
        if let types::OpType::MarkBegin(_, OldMarkData { name, .. }) = &self.op.action {
            Some(Cow::Borrowed(name))
        } else {
            None
        }
    }
}
