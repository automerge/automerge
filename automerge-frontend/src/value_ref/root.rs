use smol_str::SmolStr;

use super::ValueRef;
use crate::{state_tree::StateTree, Value};

#[derive(Clone, Debug)]
pub struct RootRef<'a> {
    st: &'a StateTree,
}

impl<'a> RootRef<'a> {
    pub(crate) fn new(st: &'a StateTree) -> Self {
        Self { st }
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.st.root_props.contains_key(key)
    }

    pub fn len(&self) -> usize {
        self.st.root_props.len()
    }

    pub fn is_empty(&self) -> bool {
        self.st.root_props.is_empty()
    }

    pub fn get(&self, key: &str) -> Option<ValueRef<'a>> {
        self.st
            .root_props
            .get(key)
            .map(|mv| ValueRef::new(mv.default_statetree_value()))
    }

    pub fn keys(&self) -> impl Iterator<Item = &SmolStr> {
        self.st.root_props.keys()
    }

    pub fn values(&self) -> impl Iterator<Item = ValueRef<'a>> {
        self.st
            .root_props
            .values()
            .map(|v| ValueRef::new(v.default_statetree_value()))
    }

    pub fn iter(&self) -> impl Iterator<Item = (&SmolStr, ValueRef<'a>)> {
        self.st
            .root_props
            .iter()
            .map(|(k, v)| (k, ValueRef::new(v.default_statetree_value())))
    }

    pub fn value(&self) -> Value {
        self.st.value()
    }
}
