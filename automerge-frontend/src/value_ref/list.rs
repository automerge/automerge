use crate::{state_tree::StateTreeList, value_ref::ValueRef, Value};

#[derive(Clone, Debug)]
pub struct ListRef<'a> {
    stl: &'a StateTreeList,
}

impl<'a> ListRef<'a> {
    pub(crate) fn new(stl: &'a StateTreeList) -> Self {
        Self { stl }
    }

    pub fn len(&self) -> usize {
        self.stl.elements.len()
    }

    pub fn is_empty(&self) -> bool {
        self.stl.elements.is_empty()
    }

    pub fn get(&self, index: usize) -> Option<ValueRef<'a>> {
        self.stl
            .elements
            .get(index)
            .map(|(_, mv)| ValueRef::new(mv.default_statetree_value()))
    }

    pub fn iter(&self) -> impl Iterator<Item = ValueRef<'a>> {
        self.stl
            .elements
            .iter()
            .map(|mv| ValueRef::new(mv.default_statetree_value()))
    }

    pub fn value(&self) -> Value {
        let mut v = Vec::new();
        for e in self.stl.elements.iter() {
            v.push(e.default_value())
        }
        Value::List(v)
    }
}
