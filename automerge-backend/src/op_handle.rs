use std::cmp::{Ordering, PartialOrd};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::rc::Rc;

use crate::protocol::{Change, Key, ObjectID, OpType, Operation, UndoOperation, Value};
use automerge_protocol::OpID;

#[derive(Clone)]
pub(crate) struct OpHandle {
    pub id: OpID,
    change: Rc<Change>,
    index: usize,
    delta: i64,
}

impl OpHandle {
    pub fn extract(change: Rc<Change>) -> Vec<OpHandle> {
        change
            .operations
            .iter()
            .enumerate()
            .map(|(index, _)| {
                let id = OpID(change.start_op + (index as u64), change.actor_id.0.clone());
                OpHandle {
                    id,
                    change: change.clone(),
                    index,
                    delta: 0,
                }
            })
            .collect()
    }

    pub fn generate_undos(&self, overwritten: &[OpHandle]) -> Vec<UndoOperation> {
        let key = self.operation_key();

        if let OpType::Inc(value) = self.action {
            vec![UndoOperation {
                action: OpType::Inc(-value),
                obj: self.obj.clone(),
                key,
            }]
        } else if overwritten.is_empty() {
            vec![UndoOperation {
                action: OpType::Del,
                obj: self.obj.clone(),
                key,
            }]
        } else {
            overwritten.iter().map(|o| o.invert(&key)).collect()
        }
    }

    pub fn invert(&self, field_key: &Key) -> UndoOperation {
        let base_op = &self.change.operations[self.index];
        let mut action = base_op.action.clone();
        let mut key = &base_op.key;
        if self.insert {
            key = field_key
        }
        if let OpType::Make(_) = base_op.action {
            action = OpType::Link(ObjectID::from(&self.id));
        }
        if let OpType::Set(Value::Counter(_)) = base_op.action {
            action = OpType::Set(self.adjusted_value());
        }
        UndoOperation {
            action,
            obj: base_op.obj.clone(),
            key: key.clone(),
        }
    }

    pub fn adjusted_value(&self) -> Value {
        match &self.action {
            OpType::Set(Value::Counter(a)) => Value::Counter(a + self.delta),
            OpType::Set(val) => val.clone(),
            _ => Value::Null,
        }
    }

    pub fn child(&self) -> Option<ObjectID> {
        match &self.action {
            OpType::Make(_) => Some(ObjectID::from(&self.id)),
            OpType::Link(obj) => Some(obj.clone()),
            _ => None,
        }
    }

    pub fn operation_key(&self) -> Key {
        if self.insert {
            self.id.clone().into()
        } else {
            self.key.clone()
        }
    }

    pub fn maybe_increment(&mut self, inc: &OpHandle) {
        if let OpType::Inc(amount) = inc.action {
            if inc.pred.contains(&self.id) {
                if let OpType::Set(Value::Counter(_)) = self.action {
                    self.delta += amount;
                }
            }
        }
    }
}

impl fmt::Debug for OpHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpHandle")
            .field("id", &self.id.to_string())
            .field("action", &self.action)
            .field("obj", &self.obj)
            .field("key", &self.key)
            .finish()
    }
}

impl Ord for OpHandle {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl Hash for OpHandle {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl PartialOrd for OpHandle {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for OpHandle {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}

impl Eq for OpHandle {}

impl Deref for OpHandle {
    type Target = Operation;

    fn deref(&self) -> &Self::Target {
        &self.change.operations[self.index]
    }
}
