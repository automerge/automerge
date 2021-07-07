use smol_str::SmolStr;

use super::ValueProxy;
use crate::state_tree::StateTreeMap;

pub struct MapProxy<'a> {
    stm: &'a StateTreeMap,
}

impl<'a> MapProxy<'a> {
    pub(crate) fn new(stm: &'a StateTreeMap) -> Self {
        Self { stm }
    }

    pub fn contains_key(&self, key: &SmolStr) -> bool {
        self.stm.props.contains_key(key)
    }

    pub fn get(&self, key: &SmolStr) -> Option<ValueProxy<'a>> {
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
