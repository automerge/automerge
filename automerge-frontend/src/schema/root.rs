use super::{MapSchema, SortedMapSchema, ValueSchema};

#[derive(Debug, Clone)]
pub enum RootSchema {
    Map(MapSchema),
    SortedMap(SortedMapSchema),
}

impl RootSchema {
    pub(crate) fn is_sorted_map(&self) -> bool {
        matches!(self, Self::SortedMap(_))
    }

    pub(crate) fn get_key(&self, key: &str) -> Option<&ValueSchema> {
        match self {
            Self::Map(map) => map.get_key(key),
            Self::SortedMap(map) => map.get_key(key),
        }
    }
}

impl From<MapSchema> for RootSchema {
    fn from(m: MapSchema) -> Self {
        Self::Map(m)
    }
}

impl From<SortedMapSchema> for RootSchema {
    fn from(m: SortedMapSchema) -> Self {
        Self::SortedMap(m)
    }
}
