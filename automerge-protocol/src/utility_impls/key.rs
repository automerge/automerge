use crate::{ElementID, Key, OpID};

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

impl From<ElementID> for Key {
    fn from(id: ElementID) -> Self {
        Key::Seq(id)
    }
}
