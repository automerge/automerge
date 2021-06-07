use serde::{
    ser::{SerializeStruct, Serializer},
    Serialize,
};

use crate::DiffEdit;

// Normally, we would use `#[derive(Serialize)]`, but...
impl Serialize for DiffEdit {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            DiffEdit::SingleElementInsert {
                index,
                elem_id,
                op_id,
                value,
            } => {
                let mut ss = serializer.serialize_struct("DiffEdit", 5)?;
                ss.serialize_field("action", "insert")?;
                ss.serialize_field("index", index)?;
                ss.serialize_field("elemId", elem_id)?;
                ss.serialize_field("opId", op_id)?;
                ss.serialize_field("value", value)?;
                ss.end()
            }
            // This field requires custom serialization logic since we want to add a "datatype"
            DiffEdit::MultiElementInsert {
                index,
                elem_id,
                values,
            } => {
                let datatype = values[0].as_numerical_datatype();
                let mut ss = serializer.serialize_struct("DiffEdit", datatype.map_or(4, |_| 5))?;
                ss.serialize_field("action", "multi-insert")?;
                ss.serialize_field("index", index)?;
                ss.serialize_field("elemId", elem_id)?;
                if let Some(datatype) = datatype {
                    ss.serialize_field("datatype", &datatype)?;
                }
                ss.serialize_field("values", values)?;
                ss.end()
            }
            DiffEdit::Update {
                index,
                op_id,
                value,
            } => {
                let mut ss = serializer.serialize_struct("DiffEdit", 4)?;
                ss.serialize_field("action", "update")?;
                ss.serialize_field("index", index)?;
                ss.serialize_field("opId", op_id)?;
                ss.serialize_field("value", value)?;
                ss.end()
            }
            DiffEdit::Remove { index, count } => {
                let mut ss = serializer.serialize_struct("DiffEdit", 3)?;
                ss.serialize_field("action", "remove")?;
                ss.serialize_field("index", index)?;
                ss.serialize_field("count", count)?;
                ss.end()
            }
        }
    }
}
