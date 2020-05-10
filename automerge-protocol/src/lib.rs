mod actor_id;
mod change_hash;

mod serde_impls;

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
