use std::collections::HashMap;

use smol_str::SmolStr;

use super::ValueSchema;

#[derive(Debug, Clone, Default)]
pub struct TableSchema {
    default: Option<Box<ValueSchema>>,
    kvs: HashMap<SmolStr, ValueSchema>,
}

impl TableSchema {
    pub fn get_key(&self, key: &str) -> Option<&ValueSchema> {
        if let Some(value) = self.kvs.get(key) {
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

    pub fn set_kvs(&mut self, kvs: HashMap<SmolStr, ValueSchema>) -> &mut Self {
        self.kvs = kvs;
        self
    }

    pub fn with_kvs(mut self, kvs: HashMap<SmolStr, ValueSchema>) -> Self {
        self.kvs = kvs;
        self
    }

    pub fn set_kv<K: Into<SmolStr>, V: Into<ValueSchema>>(&mut self, k: K, v: V) -> &mut Self {
        self.kvs.insert(k.into(), v.into());
        self
    }

    pub fn with_kv<K: Into<SmolStr>, V: Into<ValueSchema>>(mut self, k: K, v: V) -> Self {
        self.kvs.insert(k.into(), v.into());
        self
    }
}
