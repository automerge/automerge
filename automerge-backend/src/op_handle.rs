use std::{
    borrow::Cow,
    hash::{Hash, Hasher},
    num::NonZeroU64,
    ops::Deref,
};

use automerge_protocol as amp;

use crate::{
    actor_map::ActorMap,
    internal::{InternalOp, InternalOpType, Key, ObjectId, OpId},
    Change,
};

#[derive(Clone, Debug)]
pub(crate) struct OpHandle {
    pub id: OpId,
    pub op: InternalOp,
    pub delta: i64,
}

impl OpHandle {
    pub fn extract(change: &Change, actors: &mut ActorMap) -> Vec<OpHandle> {
        let mut opnum = change.start_op;
        let actor = actors.import_actor(change.actor_id());
        change
            .iter_ops()
            .map(|op| {
                let internal_op = actors.import_op(op);
                let id = OpId(opnum, actor);
                opnum = NonZeroU64::new(opnum.get() + 1).unwrap();
                OpHandle {
                    id,
                    op: internal_op,
                    delta: 0,
                }
            })
            .collect()
    }

    pub fn adjusted_value(&self) -> amp::ScalarValue {
        match &self.action {
            InternalOpType::Set(amp::ScalarValue::Counter(a)) => {
                amp::ScalarValue::Counter(a + self.delta)
            }
            InternalOpType::Set(val) => val.clone(),
            _ => amp::ScalarValue::Null,
        }
    }

    pub fn child(&self) -> Option<ObjectId> {
        match &self.action {
            InternalOpType::Make(_) => Some(self.id.into()),
            _ => None,
        }
    }

    pub fn operation_key(&self) -> Cow<Key> {
        if self.insert {
            Cow::Owned(self.id.into())
        } else {
            Cow::Borrowed(&self.key)
        }
    }

    pub fn maybe_increment(&mut self, inc: &OpHandle) -> bool {
        if let InternalOpType::Inc(amount) = inc.action {
            if inc.pred.contains(&self.id) {
                if let InternalOpType::Set(amp::ScalarValue::Counter(_)) = self.action {
                    self.delta += amount;
                    return true;
                }
            }
        }
        false
    }
}

impl Hash for OpHandle {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl PartialEq for OpHandle {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}

impl Eq for OpHandle {}

impl Deref for OpHandle {
    type Target = InternalOp;

    fn deref(&self) -> &Self::Target {
        &self.op
    }
}
