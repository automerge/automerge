use crate::{ElementID, Key, OpID};

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

impl<S> From<S> for Key
where
    S: AsRef<str>,
{
    fn from(s: S) -> Self {
        Key::Map(s.as_ref().to_string())
    }
}
