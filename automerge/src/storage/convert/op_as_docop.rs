use std::borrow::Cow;

use crate::{
    convert,
    indexed_cache::IndexedCache,
    storage::AsDocOp,
    types::{ElemId, Key, ObjId, Op, OpId, OpType, ScalarValue},
};

pub(crate) fn op_as_docop<'a>(
    actors: &'a [usize],
    props: &'a IndexedCache<String>,
    obj: &'a ObjId,
    op: &'a Op,
) -> OpAsDocOp<'a> {
    OpAsDocOp {
        op,
        obj,
        actor_lookup: actors,
        props,
    }
}

pub(crate) struct OpAsDocOp<'a> {
    op: &'a Op,
    obj: &'a ObjId,
    actor_lookup: &'a [usize],
    props: &'a IndexedCache<String>,
}

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
        translate(self.actor_lookup, &self.op.id)
    }

    fn obj(&self) -> convert::ObjId<Self::OpId> {
        if self.obj.is_root() {
            convert::ObjId::Root
        } else {
            convert::ObjId::Op(translate(self.actor_lookup, self.obj.opid()))
        }
    }

    fn key(&self) -> convert::Key<'a, Self::OpId> {
        match self.op.key {
            Key::Map(idx) => convert::Key::Prop(Cow::Owned(self.props.get(idx).into())),
            Key::Seq(e) if e.is_head() => convert::Key::Elem(convert::ElemId::Head),
            Key::Seq(ElemId(o)) => {
                convert::Key::Elem(convert::ElemId::Op(translate(self.actor_lookup, &o)))
            }
        }
    }

    fn val(&self) -> Cow<'a, crate::ScalarValue> {
        match &self.op.action {
            OpType::Put(v) => Cow::Borrowed(v),
            OpType::Increment(i) => Cow::Owned(ScalarValue::Int(*i)),
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
        self.op.insert
    }

    fn action(&self) -> u64 {
        self.op.action.action_index()
    }
}

pub(crate) struct OpAsDocOpSuccIter<'a> {
    op: &'a Op,
    offset: usize,
    actor_index: &'a [usize],
}

impl<'a> Iterator for OpAsDocOpSuccIter<'a> {
    type Item = DocOpId;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(s) = self.op.succ.get(self.offset) {
            self.offset += 1;
            Some(translate(self.actor_index, s))
        } else {
            None
        }
    }
}

impl<'a> ExactSizeIterator for OpAsDocOpSuccIter<'a> {
    fn len(&self) -> usize {
        self.op.succ.len()
    }
}

fn translate<'a>(actor_lookup: &'a [usize], op: &'a OpId) -> DocOpId {
    let index = actor_lookup.iter().position(|e| *e == op.actor()).unwrap();
    DocOpId {
        actor: index,
        counter: op.counter(),
    }
}
