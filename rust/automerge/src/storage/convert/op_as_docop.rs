use std::{borrow::Cow, collections::HashMap};

use crate::{
    convert,
    indexed_cache::IndexedCache,
    storage::AsDocOp,
    types::{ElemId, Key, MarkData, Op, OpId, OpType, ScalarValue},
};

/// Create an [`AsDocOp`] implementation for a [`crate::types::Op`]
///
/// # Arguments
/// * actors - A hashmap where the key is the actor index in the automerge::Automerge
///            we are saving and the value is the index of that same actor in the
///            order the actors will be encoded in the saved document
/// * props - An indexed cache containing the properties in this op_as_docop
/// * obj - The object ID this op refers too
/// * op - The op itself
///
/// # Panics
///
/// The methods of the resulting `AsDocOp` implementation will panic if any actor ID in the op
/// references an index not in `actors` or a property not in `props`
pub(crate) fn op_as_docop<'a>(
    actors: &'a HashMap<usize, usize>,
    props: &'a IndexedCache<String>,
    op: Op<'a>,
) -> OpAsDocOp<'a> {
    OpAsDocOp {
        op,
        actor_lookup: actors,
        props,
    }
}

pub(crate) struct OpAsDocOp<'a> {
    op: Op<'a>,
    actor_lookup: &'a HashMap<usize, usize>,
    props: &'a IndexedCache<String>,
}

#[derive(Debug)]
pub(crate) struct DocOpId {
    actor: usize,
    counter: u64,
}

impl convert::OpId<usize> for DocOpId {
    fn actor(&self) -> usize {
        self.actor
    }

    fn counter(&self) -> u64 {
        self.counter
    }
}

impl<'a> OpAsDocOp<'a> {}

impl<'a> AsDocOp<'a> for OpAsDocOp<'a> {
    type ActorId = usize;
    type OpId = DocOpId;
    type SuccIter = OpAsDocOpSuccIter<'a>;

    fn id(&self) -> Self::OpId {
        translate(self.actor_lookup, self.op.id())
    }

    fn obj(&self) -> convert::ObjId<Self::OpId> {
        if self.op.obj().is_root() {
            convert::ObjId::Root
        } else {
            convert::ObjId::Op(translate(self.actor_lookup, self.op.obj().opid()))
        }
    }

    fn key(&self) -> convert::Key<'a, Self::OpId> {
        match self.op.key() {
            Key::Map(idx) => convert::Key::Prop(Cow::Owned(self.props.get(*idx).into())),
            Key::Seq(e) if e.is_head() => convert::Key::Elem(convert::ElemId::Head),
            Key::Seq(ElemId(o)) => {
                convert::Key::Elem(convert::ElemId::Op(translate(self.actor_lookup, o)))
            }
        }
    }

    fn val(&self) -> Cow<'a, crate::ScalarValue> {
        match &self.op.action() {
            OpType::Put(v) => Cow::Borrowed(v),
            OpType::Increment(i) => Cow::Owned(ScalarValue::Int(*i)),
            OpType::MarkBegin(_, MarkData { value, .. }) => Cow::Borrowed(value),
            _ => Cow::Owned(ScalarValue::Null),
        }
    }

    fn succ(&self) -> Self::SuccIter {
        OpAsDocOpSuccIter {
            op: self.op,
            offset: 0,
            actor_index: self.actor_lookup,
        }
    }

    fn insert(&self) -> bool {
        self.op.insert()
    }

    fn action(&self) -> u64 {
        self.op.action().action_index()
    }

    fn expand(&self) -> bool {
        if let OpType::MarkBegin(expand, _) | OpType::MarkEnd(expand) = &self.op.action() {
            *expand
        } else {
            false
        }
    }

    fn mark_name(&self) -> Option<Cow<'a, smol_str::SmolStr>> {
        if let OpType::MarkBegin(_, MarkData { name, .. }) = &self.op.action() {
            Some(Cow::Owned(name.clone()))
        } else {
            None
        }
    }
}

pub(crate) struct OpAsDocOpSuccIter<'a> {
    op: Op<'a>,
    offset: usize,
    actor_index: &'a HashMap<usize, usize>,
}

impl<'a> Iterator for OpAsDocOpSuccIter<'a> {
    type Item = DocOpId;

    fn next(&mut self) -> Option<Self::Item> {
        // FIXME - nth() is no longer fast - rewrite to replace offset with a Op iterator
        if let Some(s) = self.op.succ().nth(self.offset).map(|op| op.id()) {
            self.offset += 1;
            Some(translate(self.actor_index, s))
        } else {
            None
        }
    }
}

impl<'a> ExactSizeIterator for OpAsDocOpSuccIter<'a> {
    fn len(&self) -> usize {
        self.op.succ().len()
    }
}

fn translate<'a>(actor_lookup: &'a HashMap<usize, usize>, op: &'a OpId) -> DocOpId {
    let index = actor_lookup[&op.actor()];
    DocOpId {
        actor: index,
        counter: op.counter(),
    }
}
