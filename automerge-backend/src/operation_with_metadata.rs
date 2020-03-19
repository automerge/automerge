use crate::protocol::{
    ActorID, DataType, ElementID, Key, ObjType, OpID, Operation, PrimitiveValue,
};
use std::cmp::{Ordering, PartialOrd};
use std::hash::{Hash, Hasher};
use std::str::FromStr;

/// We deserialize individual operations as part of the `Change` structure, but
/// we need access to the actor ID and sequence when applying each individual
/// operation, so we copy the operation, actor ID, and sequence into this
/// struct.
#[derive(Debug, Clone)]
pub struct OperationWithMetadata {
    pub opid: OpID,
    pub seq: u32,
    pub actor_id: ActorID,
    //pub start_op: u64,
    pub operation: Operation,
}

impl Ord for OperationWithMetadata {
    fn cmp(&self, other: &Self) -> Ordering {
        self.opid.cmp(&other.opid)
    }
}

impl PartialOrd for OperationWithMetadata {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for OperationWithMetadata {
    fn eq(&self, other: &Self) -> bool {
        self.opid.eq(&other.opid)
    }
}

impl Eq for OperationWithMetadata {}

impl Hash for OperationWithMetadata {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.opid.hash(state);
    }
}

impl OperationWithMetadata {
    //    pub fn opid(&self) -> OpID {
    //        OpID::ID(self.start_op, self.actor_id.0.clone())
    //    }

    pub fn make_type(&self) -> Option<ObjType> {
        match self.operation {
            Operation::MakeMap { .. } => Some(ObjType::Map),
            Operation::MakeTable { .. } => Some(ObjType::Table),
            Operation::MakeList { .. } => Some(ObjType::List),
            Operation::MakeText { .. } => Some(ObjType::Text),
            _ => None,
        }
    }

    pub fn child(&self) -> Option<&OpID> {
        match self.operation {
            Operation::MakeMap { .. }
            | Operation::MakeList { .. }
            | Operation::MakeText { .. }
            | Operation::MakeTable { .. } => Some(&self.opid),
            Operation::Link { ref value, .. } => panic!("not implemented"),
            _ => None,
        }
    }

    pub fn is_inc(&self) -> bool {
        if let Operation::Increment { .. } = self.operation {
            true
        } else {
            false
        }
    }

    pub fn maybe_increment(&mut self, inc: &OperationWithMetadata) {
        if inc.pred().contains(&self.opid) {
            if let Operation::Increment { value: n, .. } = inc.operation {
                if let Operation::Set {
                    value: PrimitiveValue::Number(ref mut val),
                    datatype: Some(DataType::Counter),
                    ..
                } = self.operation
                {
                    *val += n;
                }
            }
        }
    }

    pub fn is_make(&self) -> bool {
        match self.operation {
            Operation::MakeMap { .. }
            | Operation::MakeList { .. }
            | Operation::MakeText { .. }
            | Operation::MakeTable { .. } => true,
            _ => false,
        }
    }

    pub fn object_id(&self) -> &OpID {
        match self.operation {
            Operation::MakeMap { ref object_id, .. }
            | Operation::MakeList { ref object_id, .. }
            | Operation::MakeText { ref object_id, .. }
            | Operation::MakeTable { ref object_id, .. }
            | Operation::Set { ref object_id, .. }
            | Operation::Delete { ref object_id, .. }
            | Operation::Increment { ref object_id, .. }
            | Operation::Link { ref object_id, .. } => object_id,
        }
    }

    pub fn key(&self) -> &Key {
        match self.operation {
            Operation::MakeMap { ref key, .. }
            | Operation::MakeList { ref key, .. }
            | Operation::MakeText { ref key, .. }
            | Operation::MakeTable { ref key, .. }
            | Operation::Delete { ref key, .. }
            | Operation::Increment { ref key, .. }
            | Operation::Set { ref key, .. } 
            | Operation::Link { ref key, .. } => key,
        }
    }

    pub fn insert(&self) -> Option<ElementID> {
        match self.operation {
            Operation::Set {
                ref key,
                insert: Some(true),
                ..
            } => key.as_element_id().ok(),
            _ => None,
        }
    }

    pub fn pred(&self) -> &[OpID] {
        match self.operation {
            Operation::MakeMap { ref pred, .. }
            | Operation::MakeList { ref pred, .. }
            | Operation::MakeText { ref pred, .. }
            | Operation::MakeTable { ref pred, .. }
            | Operation::Set { ref pred, .. }
            | Operation::Delete { ref pred, .. }
            | Operation::Increment { ref pred, .. }
            | Operation::Link { ref pred, .. } => pred,
        }
    }

    pub fn is_link(&self) -> bool {
        match self.operation {
            Operation::MakeMap { .. }
            | Operation::MakeList { .. }
            | Operation::MakeText { .. }
            | Operation::MakeTable { .. }
            | Operation::Link { .. } => true,
            _ => false,
        }
    }

    pub fn is_insert(&self) -> bool {
        match self.operation {
            Operation::Set { insert, .. } => insert.unwrap_or(false),
            _ => false,
        }
    }

    /*
        pub fn is_concurrent(&self, ops: &[OperationWithMetadata]) -> bool {
            !ops.iter()
                .map(|op| op.opid)
                .eq(self.pred().iter().cloned())
        }
    */
}

/*
/// Note, we can't implement Ord because the Operation contains floating point
/// elements
impl PartialOrd for OperationWithMetadata {
    fn partial_cmp(&self, other: &OperationWithMetadata) -> Option<Ordering> {
        if self.actor_id == other.actor_id {
            Some(self.seq.cmp(&other.seq))
        } else {
            Some(self.actor_id.cmp(&other.actor_id))
        }
    }
}
*/
