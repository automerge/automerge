use std::collections::HashMap;

use smol_str::SmolStr;

#[derive(Debug, Clone)]
pub enum ValueSchema {
    Map(Option<Box<ValueSchema>>, HashMap<SmolStr, ValueSchema>),
    SortedMap(Option<Box<ValueSchema>>, HashMap<SmolStr, ValueSchema>),
    Table(Option<Box<ValueSchema>>, HashMap<SmolStr, ValueSchema>),
    List(Option<Box<ValueSchema>>, HashMap<u32, ValueSchema>),
    Text(Option<Box<ValueSchema>>, HashMap<u32, ValueSchema>),
    Primitive(PrimitiveSchema),
}

#[derive(Debug, Clone)]
pub enum PrimitiveSchema {
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

impl ValueSchema {
    pub(crate) fn is_sorted_map(&self) -> bool {
        matches!(self, Self::SortedMap(_, _))
    }

    pub(crate) fn get_key(&self, key: &SmolStr) -> Option<&ValueSchema> {
        match self {
            ValueSchema::Map(default, map)
            | ValueSchema::SortedMap(default, map)
            | ValueSchema::Table(default, map) => {
                if let Some(value) = map.get(key) {
                    Some(value)
                } else {
                    default.as_ref().map(|d| d.as_ref())
                }
            }
            ValueSchema::List(_, _) | ValueSchema::Text(_, _) | ValueSchema::Primitive(_) => None,
        }
    }

    pub(crate) fn get_index(&self, index: u32) -> Option<&ValueSchema> {
        match self {
            ValueSchema::List(default, map) | ValueSchema::Text(default, map) => {
                if let Some(value) = map.get(&index) {
                    Some(value)
                } else {
                    default.as_ref().map(|d| d.as_ref())
                }
            }
            ValueSchema::Map(_, _)
            | ValueSchema::SortedMap(_, _)
            | ValueSchema::Table(_, _)
            | ValueSchema::Primitive(_) => None,
        }
    }
}
