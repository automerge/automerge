use std::convert::TryInto;

use crate::{
    serde_impls::read_field, DataType, ElementId, MultiElementInsert, ScalarValue, ScalarValues,
};
use serde::{
    de::{Error, MapAccess, Unexpected, Visitor},
    ser::{SerializeStruct, Serializer},
    Deserialize, Serialize,
};

impl Serialize for MultiElementInsert {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        //serializer.serialize_newtype_variant("foo", 0, "bar", value)
        let datatype = self.values.as_numerical_datatype();
        let mut ss =
            serializer.serialize_struct("MultiElementInsert", datatype.map_or(4, |_| 5))?;
        ss.serialize_field("index", &self.index)?;
        ss.serialize_field("elemId", &self.elem_id)?;
        if let Some(datatype) = datatype {
            ss.serialize_field("datatype", &datatype)?;
        }
        ss.serialize_field("values", &self.values.vec)?;
        ss.end()
    }
}

impl<'de> Deserialize<'de> for MultiElementInsert {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &["index", "elem_id", "datatype", "values"];
        struct MultiElementInsertVisitor;
        impl<'de> Visitor<'de> for MultiElementInsertVisitor {
            type Value = MultiElementInsert;

            fn visit_map<V>(self, mut map: V) -> Result<MultiElementInsert, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut index: Option<u64> = None;
                let mut elem_id: Option<ElementId> = None;
                let mut datatype: Option<DataType> = None;
                let mut values: Option<Vec<ScalarValue>> = None;

                while let Some(field) = map.next_key::<String>()? {
                    match field.as_ref() {
                        "index" => read_field("index", &mut index, &mut map)?,
                        "elemId" => read_field("elemId", &mut elem_id, &mut map)?,
                        "datatype" => read_field("datatype", &mut datatype, &mut map)?,
                        "values" => read_field("values", &mut values, &mut map)?,
                        _ => return Err(Error::unknown_field(&field, FIELDS)),
                    }
                }

                let index = index.ok_or_else(|| Error::missing_field("index"))?;
                let elem_id = elem_id.ok_or_else(|| Error::missing_field("elemId"))?;
                let values = values.ok_or_else(|| Error::missing_field("values"))?;
                let values = ScalarValues::from_values_and_datatype::<V>(values, datatype)?;

                Ok(MultiElementInsert {
                    index,
                    elem_id,
                    values,
                })
            }

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("A MultiElementInsert")
            }
        }

        Ok(MultiElementInsert {
            index: 0,
            elem_id: crate::ElementId::Head,
            values: vec![ScalarValue::Str("one".into())].try_into().unwrap(),
        })
    }
}

// Normally, we would use `#[derive(Serialize)]`, but...
//impl Serialize for DiffEdit {
//    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
//    where
//        S: Serializer,
//    {
//        match self {
//            DiffEdit::SingleElementInsert {
//                index,
//                elem_id,
//                op_id,
//                value,
//            } => {
//                let mut ss = serializer.serialize_struct("DiffEdit", 5)?;
//                ss.serialize_field("action", "insert")?;
//                ss.serialize_field("index", index)?;
//                ss.serialize_field("elemId", elem_id)?;
//                ss.serialize_field("opId", op_id)?;
//                ss.serialize_field("value", value)?;
//                ss.end()
//            }
//            // This field requires custom serialization logic since we want to add a "datatype"
//            DiffEdit::MultiElementInsert {
//                index,
//                elem_id,
//                values,
//            } => {
//                let datatype = values.as_numerical_datatype();
//                let mut ss = serializer.serialize_struct("DiffEdit", datatype.map_or(4, |_| 5))?;
//                ss.serialize_field("action", "multi-insert")?;
//                ss.serialize_field("index", index)?;
//                ss.serialize_field("elemId", elem_id)?;
//                if let Some(datatype) = datatype {
//                    ss.serialize_field("datatype", &datatype)?;
//                }
//                ss.serialize_field("values", values)?;
//                ss.end()
//            }
//            DiffEdit::Update {
//                index,
//                op_id,
//                value,
//            } => {
//                let mut ss = serializer.serialize_struct("DiffEdit", 4)?;
//                ss.serialize_field("action", "update")?;
//                ss.serialize_field("index", index)?;
//                ss.serialize_field("opId", op_id)?;
//                ss.serialize_field("value", value)?;
//                ss.end()
//            }
//            DiffEdit::Remove { index, count } => {
//                let mut ss = serializer.serialize_struct("DiffEdit", 3)?;
//                ss.serialize_field("action", "remove")?;
//                ss.serialize_field("index", index)?;
//                ss.serialize_field("count", count)?;
//                ss.end()
//            }
//        }
//    }
//}
//
