use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::rc::Rc;

use crate::actor_map::ActorMap;
use crate::internal::{
    InternalOpType, InternalOperation, InternalUndoOperation, Key, ObjectID, OpID,
};
use crate::Change;
use automerge_protocol as amp;

#[derive(Clone)]
pub(crate) struct OpHandle {
    pub id: OpID,
    op: InternalOperation,
    //change: Rc<Change>,
    //index: usize,
    delta: i64,
}

impl OpHandle {
    pub fn extract(change: Rc<Change>, actors: &mut ActorMap) -> Vec<OpHandle> {
        change
            .iter_ops()
            //            .iter()
            .enumerate()
            .map(|(index, op)| {
                let id = OpID(
                    change.start_op + (index as u64),
                    actors.import_actor(change.actor_id()),
                );
                let op = actors.import_op(op);
                OpHandle {
                    id,
                    op,
                    //change: change.clone(),
                    //index,
                    delta: 0,
                }
            })
            .collect()
    }

    pub fn generate_undos(&self, overwritten: &[OpHandle]) -> Vec<InternalUndoOperation> {
        let key = self.operation_key();

        if let InternalOpType::Inc(value) = self.action {
            vec![InternalUndoOperation {
                action: InternalOpType::Inc(-value),
                obj: self.obj,
                key,
            }]
        } else if overwritten.is_empty() {
            vec![InternalUndoOperation {
                action: InternalOpType::Del,
                obj: self.obj,
                key,
            }]
        } else {
            overwritten.iter().map(|o| o.invert(&key)).collect()
        }
    }

    pub fn invert(&self, field_key: &Key) -> InternalUndoOperation {
        let base_op = &self.op;
        let mut action = base_op.action.clone();
        let mut key = &base_op.key;
        if self.insert {
            key = field_key
        }
        if let InternalOpType::Make(_) = base_op.action {
            action = InternalOpType::Link(self.id.into());
        }
        if let InternalOpType::Set(amp::ScalarValue::Counter(_)) = base_op.action {
            action = InternalOpType::Set(self.adjusted_value());
        }
        InternalUndoOperation {
            action,
            obj: base_op.obj,
            key: key.clone(),
        }
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

    pub fn child(&self) -> Option<ObjectID> {
        match &self.action {
            InternalOpType::Make(_) => Some(self.id.into()),
            InternalOpType::Link(obj) => Some(*obj),
            _ => None,
        }
    }

    pub fn operation_key(&self) -> Key {
        if self.insert {
            self.id.into()
        } else {
            self.key.clone()
        }
    }

    pub fn maybe_increment(&mut self, inc: &OpHandle) {
        if let InternalOpType::Inc(amount) = inc.action {
            if inc.pred.contains(&self.id) {
                if let InternalOpType::Set(amp::ScalarValue::Counter(_)) = self.action {
                    self.delta += amount;
                }
            }
        }
    }
}

impl fmt::Debug for OpHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpHandle")
            .field("id", &self.id)
            .field("action", &self.action)
            .field("obj", &self.obj)
            .field("key", &self.key)
            .finish()
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
    type Target = InternalOperation;

    fn deref(&self) -> &Self::Target {
        &self.op
    }
}
