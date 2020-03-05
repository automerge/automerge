use crate::protocol::{ActorID, Operation};
use std::cmp::{Ordering, PartialOrd};

/// We deserialize individual operations as part of the `Change` structure, but
/// we need access to the actor ID and sequence when applying each individual
/// operation, so we copy the operation, actor ID, and sequence into this
/// struct.
#[derive(PartialEq, Debug, Clone)]
pub struct OperationWithMetadata {
    pub sequence: u32,
    pub actor_id: ActorID,
    pub operation: Operation,
}

/// Note, we can't implement Ord because the Operation contains floating point
/// elements
impl PartialOrd for OperationWithMetadata {
    fn partial_cmp(&self, other: &OperationWithMetadata) -> Option<Ordering> {
        if self.actor_id == other.actor_id {
            Some(self.sequence.cmp(&other.sequence))
        } else {
            Some(self.actor_id.cmp(&other.actor_id))
        }
    }
}
