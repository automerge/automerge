use smol_str::SmolStr;

use crate::{proxy::ValueProxy, state_tree::StateTreeMap};

pub struct MapProxy<'a> {
    stm: &'a StateTreeMap,
}

impl<'a> MapProxy<'a> {
    pub(crate) fn new(stm: &'a StateTreeMap) -> Self {
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

    pub fn get(&self, key: &str) -> Option<ValueProxy<'a>> {
        self.stm
            .props
            .get(key)
            .map(|mv| ValueProxy::new(mv.default_statetree_value()))
    }

    pub fn keys(&self) -> impl Iterator<Item = &SmolStr> {
        self.stm.props.keys()
    }

    pub fn values(&self) -> impl Iterator<Item = ValueProxy<'a>> {
        self.stm
            .props
            .values()
            .map(|v| ValueProxy::new(v.default_statetree_value()))
    }

    pub fn iter(&self) -> impl Iterator<Item = (&SmolStr, ValueProxy<'a>)> {
        self.stm
            .props
            .iter()
            .map(|(k, v)| (k, ValueProxy::new(v.default_statetree_value())))
    }
}
