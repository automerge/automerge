/// Types for converting an OpTree op into a `ChangeOp` or a `DocOp`
use std::borrow::Cow;

use crate::{
    convert,
    op_set::OpSetData,
    storage::AsChangeOp,
    types::{ActorId, Key, MarkData, Op, OpId, OpType, ScalarValue},
};

/// Wrap an op in an implementation of `AsChangeOp` which represents actor IDs using a reference to
/// the actor ID stored in the opset data.
///
/// Note that the methods of `AsChangeOp` will panic if the actor is missing from the OpSetData
pub(crate) fn op_as_actor_id(op: Op<'_>) -> OpWithMetadata<'_> {
    OpWithMetadata { op }
}

pub(crate) struct OpWithMetadata<'a> {
    op: Op<'a>,
}

impl<'a> OpWithMetadata<'a> {
    fn wrap(&self, opid: OpId) -> OpIdWithMetadata<'a> {
        OpIdWithMetadata {
            opid,
            osd: self.op.osd(),
        }
    }
}

pub(crate) struct OpIdWithMetadata<'a> {
    opid: OpId,
    osd: &'a OpSetData,
}

impl<'a> convert::OpId<&'a ActorId> for OpIdWithMetadata<'a> {
    fn counter(&self) -> u64 {
        self.opid.counter()
    }

    fn actor(&self) -> &'a ActorId {
        self.osd.actors.get(self.opid.actor())
    }
}

pub(crate) struct PredWithMetadata<'a> {
    op: Op<'a>,
    offset: usize,
    osd: &'a OpSetData,
}

impl<'a> ExactSizeIterator for PredWithMetadata<'a> {
    fn len(&self) -> usize {
        self.op.pred().len()
    }
}

impl<'a> Iterator for PredWithMetadata<'a> {
    type Item = OpIdWithMetadata<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(opid) = self.op.pred().nth(self.offset).map(|o| *o.id()) {
            self.offset += 1;
            Some(OpIdWithMetadata {
                opid,
                osd: self.osd,
            })
        } else {
            None
        }
    }
}

impl<'a> AsChangeOp<'a> for OpWithMetadata<'a> {
    type ActorId = &'a ActorId;
    type OpId = OpIdWithMetadata<'a>;
    type PredIter = PredWithMetadata<'a>;

    fn action(&self) -> u64 {
        self.op.action().action_index()
    }

    fn insert(&self) -> bool {
        self.op.insert()
    }

    fn val(&self) -> Cow<'a, ScalarValue> {
        match &self.op.action() {
            OpType::Make(..) | OpType::Delete | OpType::MarkEnd(..) => {
                Cow::Owned(ScalarValue::Null)
            }
            OpType::Increment(i) => Cow::Owned(ScalarValue::Int(*i)),
            OpType::Put(s) => Cow::Borrowed(s),
            OpType::MarkBegin(_, MarkData { value, .. }) => Cow::Borrowed(value),
        }
    }

    fn obj(&self) -> convert::ObjId<Self::OpId> {
        if self.op.obj().is_root() {
            convert::ObjId::Root
        } else {
            convert::ObjId::Op(OpIdWithMetadata {
                opid: *self.op.obj().opid(),
                osd: self.op.osd(),
            })
        }
    }

    fn pred(&self) -> Self::PredIter {
        PredWithMetadata {
            op: self.op,
            offset: 0,
            osd: self.op.osd(),
        }
    }

    fn key(&self) -> convert::Key<'a, Self::OpId> {
        match &self.op.key() {
            Key::Map(idx) => convert::Key::Prop(Cow::Owned(self.op.osd().props.get(*idx).into())),
            Key::Seq(e) if e.is_head() => convert::Key::Elem(convert::ElemId::Head),
            Key::Seq(e) => convert::Key::Elem(convert::ElemId::Op(self.wrap(e.0))),
        }
    }

    fn expand(&self) -> bool {
        matches!(
            self.op.action(),
            OpType::MarkBegin(true, _) | OpType::MarkEnd(true)
        )
    }

    fn mark_name(&self) -> Option<Cow<'a, smol_str::SmolStr>> {
        if let OpType::MarkBegin(_, MarkData { name, .. }) = &self.op.action() {
            Some(Cow::Owned(name.clone()))
        } else {
            None
        }
    }
}
