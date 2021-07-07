use smol_str::SmolStr;

use crate::{proxy::ValueProxy, state_tree::StateTreeTable};

#[derive(Clone, Debug)]
pub struct TableProxy<'a> {
    stt: &'a StateTreeTable,
}

impl<'a> TableProxy<'a> {
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

    pub fn get(&self, key: &str) -> Option<ValueProxy<'a>> {
        self.stt
            .props
            .get(key)
            .map(|mv| ValueProxy::new(mv.default_statetree_value()))
    }

    pub fn keys(&self) -> impl Iterator<Item = &SmolStr> {
        self.stt.props.keys()
    }

    pub fn values(&self) -> impl Iterator<Item = ValueProxy<'a>> {
        self.stt
            .props
            .values()
            .map(|v| ValueProxy::new(v.default_statetree_value()))
    }

    pub fn iter(&self) -> impl Iterator<Item = (&SmolStr, ValueProxy<'a>)> {
        self.stt
            .props
            .iter()
            .map(|(k, v)| (k, ValueProxy::new(v.default_statetree_value())))
    }
}
