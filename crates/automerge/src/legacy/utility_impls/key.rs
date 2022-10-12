use std::cmp::{Ordering, PartialOrd};

use smol_str::SmolStr;

use crate::legacy::{ElementId, Key, OpId};

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Key::Map(a), Key::Map(b)) => a.cmp(b),
            (Key::Seq(a), Key::Seq(b)) => a.cmp(b),
            (Key::Map(_), _) => Ordering::Less,
            (_, Key::Map(_)) => Ordering::Greater,
        }
    }
}

impl From<OpId> for Key {
    fn from(id: OpId) -> Self {
        Key::Seq(ElementId::Id(id))
    }
}

impl From<&OpId> for Key {
    fn from(id: &OpId) -> Self {
        Key::Seq(ElementId::Id(id.clone()))
    }
}

impl From<ElementId> for Key {
    fn from(id: ElementId) -> Self {
        Key::Seq(id)
    }
}

impl<S> From<S> for Key
where
    S: AsRef<str>,
{
    fn from(s: S) -> Self {
        Key::Map(SmolStr::new(s))
    }
}
