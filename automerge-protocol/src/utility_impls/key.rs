use crate::{Key, ElementID, OpID};

impl From<&str> for Key {
    fn from(s: &str) -> Self {
        Key::Map(s.into())
    }
}

impl From<OpID> for Key {
    fn from(id: OpID) -> Self {
        Key::Seq(ElementID::ID(id))
    }
}

impl From<&OpID> for Key {
    fn from(id: &OpID) -> Self {
        Key::Seq(ElementID::ID(id.clone()))
    }
}

