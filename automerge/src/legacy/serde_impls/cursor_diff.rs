use serde::{ser::SerializeStruct, Serialize, Serializer};

use crate::legacy::CursorDiff;

impl Serialize for CursorDiff {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_struct("CursorDiff", 4)?;
        map.serialize_field("refObjectId", &self.object_id)?;
        map.serialize_field("elemId", &self.elem_id)?;
        map.serialize_field("index", &self.index)?;
        map.serialize_field("datatype", "cursor")?;
        map.end()
    }
}
