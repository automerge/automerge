use std::collections::HashMap;

use smol_str::SmolStr;

use crate::{state_tree::StateTreeTable, value_ref::ValueRef, Value};

#[derive(Clone, Debug)]
pub struct TableRef<'a> {
    stt: &'a StateTreeTable,
}

impl<'a> TableRef<'a> {
    pub(crate) fn new(stt: &'a StateTreeTable) -> Self {
        Self { stt }
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.stt.props.contains_key(key)
    }

    pub fn len(&self) -> usize {
        self.stt.props.len()
    }

    pub fn is_empty(&self) -> bool {
        self.stt.props.is_empty()
    }

    pub fn get(&self, key: &str) -> Option<ValueRef<'a>> {
        self.stt
            .props
            .get(key)
            .map(|mv| ValueRef::new(mv.default_statetree_value()))
    }

    pub fn keys(&self) -> impl Iterator<Item = &SmolStr> {
        self.stt.props.keys()
    }

    pub fn values(&self) -> impl Iterator<Item = ValueRef<'a>> {
        self.stt
            .props
            .values()
            .map(|v| ValueRef::new(v.default_statetree_value()))
    }

    pub fn iter(&self) -> impl Iterator<Item = (&SmolStr, ValueRef<'a>)> {
        self.stt
            .props
            .iter()
            .map(|(k, v)| (k, ValueRef::new(v.default_statetree_value())))
    }

    pub fn value(&self) -> Value {
        let mut m = HashMap::new();
        for (k, v) in &self.stt.props {
            m.insert(k.clone(), v.default_value());
        }
        Value::Map(m)
    }
}
