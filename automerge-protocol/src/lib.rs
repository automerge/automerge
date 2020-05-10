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

#[derive(Eq, PartialEq, Debug, Hash, Clone)]
pub enum ObjectID {
    ID(OpID),
    Root,
}

#[derive(PartialEq, Eq, Debug, Hash, Clone)]
pub enum ElementID {
    Head,
    ID(OpID),
}

impl ElementID {
    pub fn as_opid(&self) -> Option<&OpID> {
        match self {
            ElementID::Head => None,
            ElementID::ID(opid) => Some(opid),
        }
    }

    pub fn into_key(self) -> Key {
        Key::Seq(self)
    }

    pub fn not_head(&self) -> bool {
        match self {
            ElementID::Head => false,
            ElementID::ID(_) => true,
        }
    }
}

#[derive(Serialize, PartialEq, Eq, Debug, Hash, Clone)]
#[serde(untagged)]
pub enum Key {
    Map(String),
    Seq(ElementID),
}

impl Key {
    pub fn head() -> Key {
        Key::Seq(ElementID::Head)
    }

    pub fn as_element_id(&self) -> Option<ElementID> {
        match self {
            Key::Map(_) => None,
            Key::Seq(eid) => Some(eid.clone()),
        }
    }

    pub fn to_opid(&self) -> Option<OpID> {
        match self.as_element_id()? {
            ElementID::ID(id) => Some(id),
            ElementID::Head => None,
        }
    }
}




