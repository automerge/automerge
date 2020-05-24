use super::read_field;
use crate::{DataType, Diff, DiffEdit, MapDiff, ObjDiff, ObjType, SeqDiff, Value};
use serde::{
    de,
    de::{Error, MapAccess, Unexpected},
    ser::SerializeStruct,
    Deserialize, Deserializer, Serialize, Serializer,
};
use std::collections::HashMap;
use std::fmt;

impl Serialize for Diff {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Diff::Map(diff) => diff.serialize(serializer),
            Diff::Seq(diff) => diff.serialize(serializer),
            Diff::Unchanged(diff) => diff.serialize(serializer),
            Diff::Value(val) => match val {
                Value::Counter(_) => {
                    let mut op = serializer.serialize_struct("Value", 2)?;
                    op.serialize_field("value", &val)?;
                    op.serialize_field("datatype", "counter")?;
                    op.end()
                }
                Value::Timestamp(_) => {
                    let mut op = serializer.serialize_struct("Value", 2)?;
                    op.serialize_field("value", &val)?;
                    op.serialize_field("datatype", "timestamp")?;
                    op.end()
                }
                _ => {
                    let mut op = serializer.serialize_struct("Value", 1)?;
                    op.serialize_field("value", &val)?;
                    op.end()
                }
            },
        }
    }
}

impl<'de> Deserialize<'de> for Diff {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DiffVisitor;
        const FIELDS: &[&str] = &["edits", "objType", "objectId", "props", "value", "datatype"];

        impl<'de> de::Visitor<'de> for DiffVisitor {
            type Value = Diff;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("A diff")
            }

            fn visit_map<V>(self, mut map: V) -> Result<Self::Value, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut edits: Option<Vec<DiffEdit>> = None;
                let mut object_id: Option<String> = None;
                let mut obj_type: Option<ObjType> = None;
                let mut props: Option<HashMap<String, HashMap<String, Diff>>> = None;
                let mut value: Option<Value> = None;
                let mut datatype: Option<DataType> = None;

                while let Some(field) = map.next_key::<String>()? {
                    match field.as_ref() {
                        "edits" => read_field("edits", &mut edits, &mut map)?,
                        "objectId" => read_field("objectId", &mut object_id, &mut map)?,
                        "type" => read_field("type", &mut obj_type, &mut map)?,
                        "props" => read_field("props", &mut props, &mut map)?,
                        "value" => read_field("value", &mut value, &mut map)?,
                        "datatype" => read_field("datatype", &mut datatype, &mut map)?,
                        _ => return Err(Error::unknown_field(&field, FIELDS)),
                    }
                }
                if value.is_some() || datatype.is_some() {
                    let datatype = datatype.unwrap_or(DataType::Undefined);
                    let value = value.ok_or_else(|| Error::missing_field("value"))?;
                    let value_with_datatype = maybe_add_datatype_to_value(value, datatype);
                    Ok(Diff::Value(value_with_datatype))
                } else {
                    let object_id = object_id.ok_or_else(|| Error::missing_field("objectId"))?;
                    let obj_type = obj_type.ok_or_else(|| Error::missing_field("type"))?;
                    if let Some(mut props) = props {
                        match obj_type {
                            ObjType::Sequence(seq_type) => {
                                let edits = edits.ok_or_else(|| Error::missing_field("edits"))?;
                                let mut new_props = HashMap::new();
                                for (k, v) in props.drain() {
                                    let index = k.parse().map_err(|_| {
                                        Error::invalid_type(Unexpected::Str(&k), &"an integer")
                                    })?;
                                    new_props.insert(index, v);
                                }
                                Ok(Diff::Seq(SeqDiff {
                                    object_id,
                                    obj_type: seq_type,
                                    edits,
                                    props: new_props,
                                }))
                            }
                            ObjType::Map(map_type) => Ok(Diff::Map(MapDiff {
                                object_id,
                                obj_type: map_type,
                                props,
                            })),
                        }
                    } else {
                        Ok(Diff::Unchanged(ObjDiff {
                            object_id,
                            obj_type,
                        }))
                    }
                }
            }
        }
        deserializer.deserialize_struct("Diff", &FIELDS, DiffVisitor)
    }
}

fn maybe_add_datatype_to_value(value: Value, datatype: DataType) -> Value {
    match datatype {
        DataType::Counter => {
            if let Some(n) = value.to_i64() {
                Value::Counter(n)
            } else {
                value
            }
        }
        DataType::Timestamp => {
            if let Some(n) = value.to_i64() {
                Value::Timestamp(n)
            } else {
                value
            }
        }
        _ => value,
    }
}

#[cfg(test)]
mod tests {
    use crate::{Diff, MapDiff, MapType, SeqDiff, SequenceType};
    use maplit::hashmap;

    #[test]
    fn map_diff_serialization_round_trip() {
        let json = serde_json::json!({
            "objectId": "1@6121f875-7d5d-4660-9b66-5218b2b3a141",
            "type": "map",
            "props": {
                "key": {
                    "1@4a093244-de2b-4fd0-a420-3724e15dfc16": {
                        "value": "value"
                    }
                }
            }
        });
        let diff = Diff::Map(MapDiff {
            object_id: "1@6121f875-7d5d-4660-9b66-5218b2b3a141".to_string(),
            obj_type: MapType::Map,
            props: hashmap! {
                "key".to_string() => hashmap!{
                    "1@4a093244-de2b-4fd0-a420-3724e15dfc16".to_string() => "value".into()
                }
            },
        });

        assert_eq!(json.clone(), serde_json::to_value(diff.clone()).unwrap());
        assert_eq!(serde_json::from_value::<Diff>(json).unwrap(), diff);
    }

    #[test]
    fn seq_diff_serialization_round_trip() {
        let json = serde_json::json!({
            "objectId": "1@6121f875-7d5d-4660-9b66-5218b2b3a141",
            "type": "list",
            "edits": [],
            "props": {
                "0": {
                    "1@4a093244-de2b-4fd0-a420-3724e15dfc16": {
                        "value": "value"
                    }
                }
            }
        });
        let diff = Diff::Seq(SeqDiff {
            object_id: "1@6121f875-7d5d-4660-9b66-5218b2b3a141".to_string(),
            obj_type: SequenceType::List,
            edits: Vec::new(),
            props: hashmap! {
                0 => hashmap!{
                    "1@4a093244-de2b-4fd0-a420-3724e15dfc16".to_string() => "value".into()
                }
            },
        });

        assert_eq!(json.clone(), serde_json::to_value(diff.clone()).unwrap());
        assert_eq!(serde_json::from_value::<Diff>(json).unwrap(), diff);
    }
}
