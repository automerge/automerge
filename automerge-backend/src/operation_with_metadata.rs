use crate::protocol::{ActorID, Key, OpID, Operation};
use std::cmp::{Ordering, PartialOrd};

/// We deserialize individual operations as part of the `Change` structure, but
/// we need access to the actor ID and sequence when applying each individual
/// operation, so we copy the operation, actor ID, and sequence into this
/// struct.
#[derive(PartialEq, Debug, Clone)]
pub struct OperationWithMetadata {
    pub seq: u32,
    pub actor_id: ActorID,
    pub start_op: u64,
    pub operation: Operation,
}

impl OperationWithMetadata {
    pub fn opid(&self) -> OpID {
        OpID::ID(self.start_op, self.actor_id.0.clone())
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

    pub fn object_id(&self) -> Option<&OpID> {
        match self.operation {
            Operation::MakeMap { ref object_id, .. }
            | Operation::MakeList { ref object_id, .. }
            | Operation::MakeText { ref object_id, .. }
            | Operation::MakeTable { ref object_id, .. }
            | Operation::Set { ref object_id, .. }
            | Operation::Delete { ref object_id, .. }
            | Operation::Increment { ref object_id, .. }
            | Operation::Link { ref object_id, .. } => Some(object_id),
            Operation::Insert { .. } => None,
        }
    }

    pub fn key(&self) -> Option<&Key> {
        match self.operation {
            Operation::MakeMap { ref key, .. }
            | Operation::MakeList { ref key, .. }
            | Operation::MakeText { ref key, .. }
            | Operation::MakeTable { ref key, .. }
            | Operation::Set { ref key, .. }
            | Operation::Delete { ref key, .. }
            | Operation::Increment { ref key, .. }
            | Operation::Link { ref key, .. } => Some(key),
            Operation::Insert { .. } => None,
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
}

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
