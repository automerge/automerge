use std::collections::BTreeMap;

use smol_str::SmolStr;

use crate::{state_tree::StateTreeSortedMap, value_ref::ValueRef, Value};

#[derive(Clone, Debug)]
pub struct SortedMapRef<'a> {
    stm: &'a StateTreeSortedMap,
}

impl<'a> SortedMapRef<'a> {
    pub(crate) fn new(stm: &'a StateTreeSortedMap) -> Self {
        Self { stm }
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.stm.props.contains_key(key)
    }

    pub fn len(&self) -> usize {
        self.stm.props.len()
    }

    pub fn is_empty(&self) -> bool {
        self.stm.props.is_empty()
    }

    pub fn get(&self, key: &str) -> Option<ValueRef<'a>> {
        self.stm
            .props
            .get(key)
            .map(|mv| ValueRef::new(mv.default_statetree_value()))
    }

    pub fn keys(&self) -> impl Iterator<Item = &SmolStr> {
        self.stm.props.keys()
    }

    pub fn values(&self) -> impl Iterator<Item = ValueRef<'a>> {
        self.stm
            .props
            .values()
            .map(|v| ValueRef::new(v.default_statetree_value()))
    }

    pub fn iter(&self) -> impl Iterator<Item = (&SmolStr, ValueRef<'a>)> {
        self.stm
            .props
            .iter()
            .map(|(k, v)| (k, ValueRef::new(v.default_statetree_value())))
    }

    pub fn value(&self) -> Value {
        let mut m = BTreeMap::new();
        for (k, v) in &self.stm.props {
            m.insert(k.clone(), v.default_value());
        }
        Value::SortedMap(m)
    }
}
