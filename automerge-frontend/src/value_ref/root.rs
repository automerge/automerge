use super::{MapRef, SortedMapRef};
use crate::{state_tree::StateTreeRoot, Value};

#[derive(Clone, Debug)]
pub struct RootRef<'a> {
    st: &'a StateTreeRoot,
}

impl<'a> RootRef<'a> {
    pub(crate) fn new(st: &'a StateTreeRoot) -> Self {
        Self { st }
    }

    pub fn map(&self) -> Option<MapRef<'a>> {
        match self.st {
            StateTreeRoot::Map(m) => Some(MapRef::new(m)),
            StateTreeRoot::SortedMap(_) => None,
        }
    }

    pub fn sorted_map(&self) -> Option<SortedMapRef<'a>> {
        match self.st {
            StateTreeRoot::Map(_) => None,
            StateTreeRoot::SortedMap(m) => Some(SortedMapRef::new(m)),
        }
    }

    pub fn value(&self) -> Value {
        self.st.value()
    }
}
