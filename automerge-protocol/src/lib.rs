mod actor_id;
mod change_hash;

mod serde_impls;
mod utility_impls;
mod error;

pub use actor_id::ActorID;
pub use change_hash::ChangeHash;

use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Copy, Hash)]
#[serde(rename_all = "camelCase")]
pub enum ObjType {
    Map,
    Table,
    Text,
    List,
}

#[derive(Eq, PartialEq, Hash, Clone)]
pub struct OpID(pub u64, pub String);

impl OpID {
    pub fn new(seq: u64, actor: &ActorID) -> OpID {
        OpID(seq, actor.0.clone())
    }

    pub fn counter(&self) -> u64 {
        self.0
    }
}
