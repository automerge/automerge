mod list;
mod map;
mod root;
mod sorted_map;
mod table;
mod text;

pub use self::{
    list::ListSchema, map::MapSchema, root::RootSchema, sorted_map::SortedMapSchema,
    table::TableSchema, text::TextSchema,
};

#[derive(Debug, Clone)]
pub enum ValueSchema {
    Map(MapSchema),
    SortedMap(SortedMapSchema),
    Table(TableSchema),
    List(ListSchema),
    Text(TextSchema),
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
        matches!(self, Self::SortedMap(_))
    }

    pub(crate) fn get_key(&self, key: &str) -> Option<&ValueSchema> {
        match self {
            ValueSchema::Map(map) => map.get_key(key),
            ValueSchema::SortedMap(map) => map.get_key(key),
            ValueSchema::Table(table) => table.get_key(key),
            ValueSchema::List(_) | ValueSchema::Text(_) | ValueSchema::Primitive(_) => None,
        }
    }

    pub(crate) fn get_index(&self, index: u32) -> Option<&ValueSchema> {
        match self {
            ValueSchema::List(list) => list.get_index(index),
            ValueSchema::Text(text) => text.get_index(index),
            ValueSchema::Map(_)
            | ValueSchema::SortedMap(_)
            | ValueSchema::Table(_)
            | ValueSchema::Primitive(_) => None,
        }
    }
}

impl From<MapSchema> for ValueSchema {
    fn from(m: MapSchema) -> Self {
        Self::Map(m)
    }
}

impl From<SortedMapSchema> for ValueSchema {
    fn from(m: SortedMapSchema) -> Self {
        Self::SortedMap(m)
    }
}

impl From<TableSchema> for ValueSchema {
    fn from(m: TableSchema) -> Self {
        Self::Table(m)
    }
}

impl From<ListSchema> for ValueSchema {
    fn from(m: ListSchema) -> Self {
        Self::List(m)
    }
}

impl From<TextSchema> for ValueSchema {
    fn from(m: TextSchema) -> Self {
        Self::Text(m)
    }
}

impl From<PrimitiveSchema> for ValueSchema {
    fn from(m: PrimitiveSchema) -> Self {
        Self::Primitive(m)
    }
}
