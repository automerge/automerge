use std::collections::HashMap;

use super::ValueSchema;

#[derive(Debug, Clone, Default)]
pub struct TextSchema {
    default: Option<Box<ValueSchema>>,
    kvs: HashMap<u32, ValueSchema>,
}

impl TextSchema {
    pub fn get_index(&self, key: u32) -> Option<&ValueSchema> {
        if let Some(value) = self.kvs.get(&key) {
            Some(value)
        } else {
            self.default.as_ref().map(|d| d.as_ref())
        }
    }

    pub fn set_default<D: Into<ValueSchema>>(&mut self, default: D) -> &mut Self {
        self.default = Some(Box::new(default.into()));
        self
    }

    pub fn with_default<D: Into<ValueSchema>>(mut self, default: D) -> Self {
        self.default = Some(Box::new(default.into()));
        self
    }

    pub fn set_kvs(&mut self, kvs: HashMap<u32, ValueSchema>) -> &mut Self {
        self.kvs = kvs;
        self
    }

    pub fn with_kvs(mut self, kvs: HashMap<u32, ValueSchema>) -> Self {
        self.kvs = kvs;
        self
    }

    pub fn set_kv<V: Into<ValueSchema>>(&mut self, k: u32, v: V) -> &mut Self {
        self.kvs.insert(k, v.into());
        self
    }

    pub fn with_kv<V: Into<ValueSchema>>(mut self, k: u32, v: V) -> Self {
        self.kvs.insert(k, v.into());
        self
    }
}
