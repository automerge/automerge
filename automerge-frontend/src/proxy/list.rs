use crate::{proxy::ValueProxy, state_tree::StateTreeList};

pub struct ListProxy<'a> {
    stl: &'a StateTreeList,
}

impl<'a> ListProxy<'a> {
    pub(crate) fn new(stl: &'a StateTreeList) -> Self {
        Self { stl }
    }

    pub fn len(&self) -> usize {
        self.stl.elements.len()
    }

    pub fn is_empty(&self) -> bool {
        self.stl.elements.is_empty()
    }

    pub fn get(&self, index: usize) -> Option<ValueProxy<'a>> {
        self.stl
            .elements
            .get(index)
            .map(|(_, mv)| ValueProxy::new(mv.default_statetree_value()))
    }

    pub fn iter(&self) -> impl Iterator<Item = ValueProxy<'a>> {
        self.stl
            .elements
            .iter()
            .map(|mv| ValueProxy::new(mv.default_statetree_value()))
    }
}
