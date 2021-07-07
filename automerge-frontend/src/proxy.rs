mod list;
mod map;

pub use map::MapProxy;

use crate::state_tree::{StateTreeComposite, StateTreeValue};

pub struct ValueProxy<'a> {
    stv: &'a StateTreeValue,
}

impl<'a> ValueProxy<'a> {
    pub(crate) fn new(stv: &'a StateTreeValue) -> Self {
        Self { stv }
    }

    pub fn map(&self) -> Option<MapProxy<'a>> {
        match self.stv {
            StateTreeValue::Leaf(_) => None,
            StateTreeValue::Composite(c) => match c {
                StateTreeComposite::Map(m) => Some(MapProxy::new(m)),
                StateTreeComposite::Table(_)
                | StateTreeComposite::Text(_)
                | StateTreeComposite::List(_) => None,
            },
        }
    }
}
