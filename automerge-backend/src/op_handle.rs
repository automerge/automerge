use std::cmp::{Ordering, PartialOrd};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::rc::Rc;

use crate::op::Operation;
use crate::op_type::OpType;
use crate::undo_operation::UndoOperation;
use crate::Change;
use automerge_protocol as amp;

#[derive(Clone)]
pub(crate) struct OpHandle {
    pub id: amp::OpID,
    op: Operation,
    //change: Rc<Change>,
    //index: usize,
    delta: i64,
}

impl OpHandle {
    pub fn extract(change: Rc<Change>) -> Vec<OpHandle> {
        change
            .iter_ops()
            //            .iter()
            .enumerate()
            .map(|(index, op)| {
                let id = amp::OpID::new(change.start_op + (index as u64), &change.actor_id());
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

    pub fn invert(&self, field_key: &amp::Key) -> UndoOperation {
        let base_op = &self.op;
        let mut action = base_op.action.clone();
        let mut key = &base_op.key;
        if self.insert {
            key = field_key
        }
        if let OpType::Make(_) = base_op.action {
            action = OpType::Link(amp::ObjectID::from(&self.id));
        }
        if let OpType::Set(amp::Value::Counter(_)) = base_op.action {
            action = OpType::Set(self.adjusted_value());
        }
        UndoOperation {
            action,
            obj: base_op.obj.clone(),
            key: key.clone(),
        }
    }

    pub fn adjusted_value(&self) -> amp::Value {
        match &self.action {
            OpType::Set(amp::Value::Counter(a)) => amp::Value::Counter(a + self.delta),
            OpType::Set(val) => val.clone(),
            _ => amp::Value::Null,
        }
    }

    pub fn child(&self) -> Option<amp::ObjectID> {
        match &self.action {
            OpType::Make(_) => Some(amp::ObjectID::from(&self.id)),
            OpType::Link(obj) => Some(obj.clone()),
            _ => None,
        }
    }

    pub fn operation_key(&self) -> amp::Key {
        if self.insert {
            self.id.clone().into()
        } else {
            self.key.clone()
        }
    }

    pub fn maybe_increment(&mut self, inc: &OpHandle) {
        if let OpType::Inc(amount) = inc.action {
            if inc.pred.contains(&self.id) {
                if let OpType::Set(amp::Value::Counter(_)) = self.action {
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
        &self.op
    }
}
