/// Types for converting an OpTree op into a `ChangeOp` or a `DocOp`
use std::borrow::Cow;

use crate::{
    convert,
    op_set::OpSetMetadata,
    storage::AsChangeOp,
    types::{ActorId, Key, MarkData, ObjId, Op, OpId, OpType, ScalarValue},
};

/// Wrap an op in an implementation of `AsChangeOp` which represents actor IDs using a reference to
/// the actor ID stored in the metadata.
///
/// Note that the methods of `AsChangeOp` will panic if the actor is missing from the metadata
pub(crate) fn op_as_actor_id<'a>(
    obj: &'a ObjId,
    op: &'a Op,
    metadata: &'a OpSetMetadata,
) -> OpWithMetadata<'a> {
    OpWithMetadata { obj, op, metadata }
}

pub(crate) struct OpWithMetadata<'a> {
    obj: &'a ObjId,
    op: &'a Op,
    metadata: &'a OpSetMetadata,
}

impl<'a> OpWithMetadata<'a> {
    fn wrap(&self, opid: &'a OpId) -> OpIdWithMetadata<'a> {
        OpIdWithMetadata {
            opid,
            metadata: self.metadata,
        }
    }
}

pub(crate) struct OpIdWithMetadata<'a> {
    opid: &'a OpId,
    metadata: &'a OpSetMetadata,
}

impl<'a> convert::OpId<&'a ActorId> for OpIdWithMetadata<'a> {
    fn counter(&self) -> u64 {
        self.opid.counter()
    }

    fn actor(&self) -> &'a ActorId {
        self.metadata.actors.get(self.opid.actor())
    }
}

pub(crate) struct PredWithMetadata<'a> {
    op: &'a Op,
    offset: usize,
    metadata: &'a OpSetMetadata,
}

impl<'a> ExactSizeIterator for PredWithMetadata<'a> {
    fn len(&self) -> usize {
        self.op.pred.len()
    }
}

impl<'a> Iterator for PredWithMetadata<'a> {
    type Item = OpIdWithMetadata<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(op) = self.op.pred.get(self.offset) {
            self.offset += 1;
            Some(OpIdWithMetadata {
                opid: op,
                metadata: self.metadata,
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
        self.op.action.action_index()
    }

    fn insert(&self) -> bool {
        self.op.insert
    }

    fn val(&self) -> Cow<'a, ScalarValue> {
        match &self.op.action {
            OpType::Make(..) | OpType::Delete | OpType::MarkEnd(..) => {
                Cow::Owned(ScalarValue::Null)
            }
            OpType::Increment(i) => Cow::Owned(ScalarValue::Int(*i)),
            OpType::Put(s) => Cow::Borrowed(s),
            OpType::MarkBegin(MarkData { value, .. }) => Cow::Borrowed(value),
        }
    }

    fn obj(&self) -> convert::ObjId<Self::OpId> {
        if self.obj.is_root() {
            convert::ObjId::Root
        } else {
            convert::ObjId::Op(OpIdWithMetadata {
                opid: self.obj.opid(),
                metadata: self.metadata,
            })
        }
    }

    fn pred(&self) -> Self::PredIter {
        PredWithMetadata {
            op: self.op,
            offset: 0,
            metadata: self.metadata,
        }
    }

    fn key(&self) -> convert::Key<'a, Self::OpId> {
        match &self.op.key {
            Key::Map(idx) => convert::Key::Prop(Cow::Owned(self.metadata.props.get(*idx).into())),
            Key::Seq(e) if e.is_head() => convert::Key::Elem(convert::ElemId::Head),
            Key::Seq(e) => convert::Key::Elem(convert::ElemId::Op(self.wrap(&e.0))),
        }
    }

    fn expand(&self) -> bool {
        matches!(
            self.op.action,
            OpType::MarkBegin(MarkData { expand: true, .. }) | OpType::MarkEnd(true)
        )
    }

    fn mark_name(&self) -> Option<Cow<'a, smol_str::SmolStr>> {
        if let OpType::MarkBegin(MarkData { name, .. }) = &self.op.action {
            let name = self.metadata.props.get(name.props_index());
            Some(Cow::Owned(name.into()))
        } else {
            None
        }
    }
}
