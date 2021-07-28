use std::{borrow::Borrow, collections::BTreeMap, ops::RangeBounds};

use smol_str::SmolStr;

use crate::{state_tree::StateTreeSortedMap, value_ref::ValueRef, Value};

#[derive(Clone, Debug)]
pub struct SortedMapRef<'a> {
    stsm: &'a StateTreeSortedMap,
}

impl<'a> SortedMapRef<'a> {
    pub(crate) fn new(stm: &'a StateTreeSortedMap) -> Self {
        Self { stsm: stm }
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.stsm.props.contains_key(key)
    }

    pub fn len(&self) -> usize {
        self.stsm.props.len()
    }

    pub fn is_empty(&self) -> bool {
        self.stsm.props.is_empty()
    }

    pub fn get(&self, key: &str) -> Option<ValueRef<'a>> {
        self.stsm
            .props
            .get(key)
            .map(|mv| ValueRef::new(mv.default_statetree_value()))
    }

    pub fn keys(&self) -> impl Iterator<Item = &SmolStr> {
        self.stsm.props.keys()
    }

    pub fn values(&self) -> impl Iterator<Item = ValueRef<'a>> {
        self.stsm
            .props
            .values()
            .map(|v| ValueRef::new(v.default_statetree_value()))
    }

    pub fn iter(&self) -> impl Iterator<Item = (&SmolStr, ValueRef<'a>)> {
        self.stsm
            .props
            .iter()
            .map(|(k, v)| (k, ValueRef::new(v.default_statetree_value())))
    }

    pub fn range<T, R>(&self, range: R) -> impl Iterator<Item = (&SmolStr, ValueRef<'a>)>
    where
        T: Ord + ?Sized,
        R: RangeBounds<T>,
        SmolStr: Borrow<T>,
    {
        self.stsm
            .props
            .range(range)
            .map(|(k, v)| (k, ValueRef::new(v.default_statetree_value())))
    }

    pub fn value(&self) -> Value {
        let mut m = BTreeMap::new();
        for (k, v) in &self.stsm.props {
            m.insert(k.clone(), v.default_value());
        }
        Value::SortedMap(m)
    }
}
