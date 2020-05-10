use crate::patch::{Diff, DiffEdit, MapDiff, ObjDiff, SeqDiff};
use crate::protocol::UndoOperation;
use serde::de;
use serde::de::{Error, MapAccess, Unexpected};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::fmt;
use automerge_protocol::{ObjType, Value, OpType, DataType};

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

impl Serialize for UndoOperation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 3;

        match &self.action {
            OpType::Set(Value::Counter(_)) => fields += 2,
            OpType::Set(Value::Timestamp(_)) => fields += 2,
            OpType::Link(_) | OpType::Inc(_) | OpType::Set(_) => fields += 1,
            _ => {}
        }

        let mut op = serializer.serialize_struct("UndoOperation", fields)?;
        op.serialize_field("action", &self.action)?;
        op.serialize_field("obj", &self.obj)?;
        op.serialize_field("key", &self.key)?;
        match &self.action {
            OpType::Link(child) => op.serialize_field("child", &child)?,
            OpType::Inc(n) => op.serialize_field("value", &n)?,
            OpType::Set(Value::Timestamp(value)) => {
                op.serialize_field("value", &value)?;
                op.serialize_field("datatype", &DataType::Timestamp)?;
            }
            OpType::Set(Value::Counter(value)) => {
                op.serialize_field("value", &value)?;
                op.serialize_field("datatype", &DataType::Counter)?;
            }
            OpType::Set(value) => op.serialize_field("value", &value)?,
            _ => {}
        }
        op.end()
    }
}


fn read_field<'de, T, M>(
    name: &'static str,
    data: &mut Option<T>,
    map: &mut M,
) -> Result<(), M::Error>
where
    M: MapAccess<'de>,
    T: Deserialize<'de>,
{
    if data.is_some() {
        Err(Error::duplicate_field(name))
    } else {
        data.replace(map.next_value()?);
        Ok(())
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
                    let value = value
                        .ok_or_else(|| Error::missing_field("value"))?;
                    let value_with_datatype = maybe_add_datatype_to_value(value, datatype);
                    Ok(Diff::Value(value_with_datatype))
                } else {
                    let object_id = object_id.ok_or_else(|| Error::missing_field("objectId"))?;
                    let obj_type = obj_type.ok_or_else(|| Error::missing_field("type"))?;
                    if let Some(mut props) = props {
                        if obj_type == ObjType::Text || obj_type == ObjType::List {
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
                                obj_type,
                                edits,
                                props: new_props,
                            }))
                        } else {
                            Ok(Diff::Map(MapDiff {
                                object_id,
                                obj_type,
                                props,
                            }))
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
