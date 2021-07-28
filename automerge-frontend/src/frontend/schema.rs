use std::collections::HashMap;

use smol_str::SmolStr;

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum KeyMatcher {
    Any,
    Key(SmolStr),
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum IndexMatcher {
    Any,
    Index(u32),
}

#[derive(Debug, Clone)]
pub enum SchemaValue {
    Map(Option<Box<SchemaValue>>, HashMap<SmolStr, SchemaValue>),
    SortedMap(Option<Box<SchemaValue>>, HashMap<SmolStr, SchemaValue>),
    Table(Option<Box<SchemaValue>>, HashMap<SmolStr, SchemaValue>),
    List(Option<Box<SchemaValue>>, HashMap<u32, SchemaValue>),
    Text(Option<Box<SchemaValue>>, HashMap<u32, SchemaValue>),
    Primitive(SchemaPrimitive),
}

#[derive(Debug, Clone)]
pub enum SchemaPrimitive {
    Bytes,
    Str,
    Int,
    Uint,
    F64,
    Counter,
    Timestamp,
    Boolean,
    Cursor,
    Null,
}

impl SchemaValue {
    pub(crate) fn is_sorted_map(&self) -> bool {
        matches!(self, Self::SortedMap(_, _))
    }

    pub(crate) fn get_key(&self, key: &SmolStr) -> Option<&SchemaValue> {
        match self {
            SchemaValue::Map(default, map)
            | SchemaValue::SortedMap(default, map)
            | SchemaValue::Table(default, map) => {
                if let Some(value) = map.get(key) {
                    Some(value)
                } else {
                    default.as_ref().map(|d| d.as_ref())
                }
            }
            SchemaValue::List(_, _) | SchemaValue::Text(_, _) | SchemaValue::Primitive(_) => None,
        }
    }

    pub(crate) fn get_index(&self, index: u32) -> Option<&SchemaValue> {
        match self {
            SchemaValue::List(default, map) | SchemaValue::Text(default, map) => {
                if let Some(value) = map.get(&index) {
                    Some(value)
                } else {
                    default.as_ref().map(|d| d.as_ref())
                }
            }
            SchemaValue::Map(_, _)
            | SchemaValue::SortedMap(_, _)
            | SchemaValue::Table(_, _)
            | SchemaValue::Primitive(_) => None,
        }
    }
}
