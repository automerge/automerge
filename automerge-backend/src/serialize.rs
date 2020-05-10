use crate::patch::{Diff, DiffEdit, MapDiff, ObjDiff, SeqDiff};
use crate::protocol::{
    DataType, OpType, Operation, ReqOpType, RequestKey,
    UndoOperation, Value
};
use serde::de;
use serde::de::{Error, MapAccess, Unexpected, Visitor};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::fmt;
use automerge_protocol::{ObjType, OpID, ObjectID, Key};

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

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ValueVisitor;
        impl<'de> Visitor<'de> for ValueVisitor {
            type Value = Value;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a number, string, bool, or null")
            }

            fn visit_bool<E>(self, value: bool) -> Result<Value, E>
            where
                E: de::Error,
            {
                Ok(Value::Boolean(value))
            }

            fn visit_u64<E>(self, value: u64) -> Result<Value, E>
            where
                E: de::Error,
            {
                Ok(Value::Uint(value))
            }

            fn visit_i64<E>(self, value: i64) -> Result<Value, E>
            where
                E: de::Error,
            {
                Ok(Value::Int(value))
            }

            fn visit_f64<E>(self, value: f64) -> Result<Value, E>
            where
                E: de::Error,
            {
                Ok(Value::F64(value))
            }

            fn visit_str<E>(self, value: &str) -> Result<Value, E>
            where
                E: de::Error,
            {
                Ok(Value::Str(value.to_string()))
            }
        }
        deserializer.deserialize_any(ValueVisitor)
    }
}

impl<'de> Deserialize<'de> for RequestKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct RequestKeyVisitor;
        impl<'de> Visitor<'de> for RequestKeyVisitor {
            type Value = RequestKey;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a number or string")
            }

            fn visit_u64<E>(self, value: u64) -> Result<RequestKey, E>
            where
                E: de::Error,
            {
                Ok(RequestKey::Num(value))
            }

            fn visit_str<E>(self, value: &str) -> Result<RequestKey, E>
            where
                E: de::Error,
            {
                Ok(RequestKey::Str(value.to_string()))
            }
        }
        deserializer.deserialize_any(RequestKeyVisitor)
    }
}

impl Serialize for OpType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = match self {
            OpType::Make(ObjType::Map) => "makeMap",
            OpType::Make(ObjType::Table) => "makeTable",
            OpType::Make(ObjType::List) => "makeList",
            OpType::Make(ObjType::Text) => "makeText",
            OpType::Del => "del",
            OpType::Link(_) => "link",
            OpType::Inc(_) => "inc",
            OpType::Set(_) => "set",
        };
        serializer.serialize_str(s)
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

impl Serialize for Operation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 4;

        if self.insert {
            fields += 1
        }

        match &self.action {
            OpType::Set(Value::Timestamp(_)) => fields += 2,
            OpType::Set(Value::Counter(_)) => fields += 2,
            OpType::Link(_) | OpType::Inc(_) | OpType::Set(_) => fields += 1,
            _ => {}
        }

        let mut op = serializer.serialize_struct("Operation", fields)?;
        op.serialize_field("action", &self.action)?;
        op.serialize_field("obj", &self.obj)?;
        op.serialize_field("key", &self.key)?;
        if self.insert {
            op.serialize_field("insert", &self.insert)?;
        }
        match &self.action {
            OpType::Link(child) => op.serialize_field("child", &child)?,
            OpType::Inc(n) => op.serialize_field("value", &n)?,
            OpType::Set(Value::Counter(value)) => {
                op.serialize_field("value", &value)?;
                op.serialize_field("datatype", &DataType::Counter)?;
            }
            OpType::Set(Value::Timestamp(value)) => {
                op.serialize_field("value", &value)?;
                op.serialize_field("datatype", &DataType::Timestamp)?;
            }
            OpType::Set(value) => op.serialize_field("value", &value)?,
            _ => {}
        }
        op.serialize_field("pred", &self.pred)?;
        op.end()
    }
}

impl<'de> Deserialize<'de> for Operation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        const FIELDS: &[&str] = &["ops", "deps", "message", "seq", "actor", "requestType"];
        struct OperationVisitor;
        impl<'de> Visitor<'de> for OperationVisitor {
            type Value = Operation;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("An operation object")
            }

            fn visit_map<V>(self, mut map: V) -> Result<Operation, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut action: Option<ReqOpType> = None;
                let mut obj: Option<ObjectID> = None;
                let mut key: Option<Key> = None;
                let mut pred: Option<Vec<OpID>> = None;
                let mut insert: Option<bool> = None;
                let mut datatype: Option<DataType> = None;
                let mut value: Option<Value> = None;
                let mut child: Option<ObjectID> = None;
                while let Some(field) = map.next_key::<String>()? {
                    match field.as_ref() {
                        "action" => read_field("action", &mut action, &mut map)?,
                        "obj" => read_field("obj", &mut obj, &mut map)?,
                        "key" => read_field("key", &mut key, &mut map)?,
                        "pred" => read_field("pred", &mut pred, &mut map)?,
                        "insert" => read_field("insert", &mut insert, &mut map)?,
                        "datatype" => read_field("datatype", &mut datatype, &mut map)?,
                        "value" => read_field("value", &mut value, &mut map)?,
                        "child" => read_field("child", &mut child, &mut map)?,
                        _ => return Err(Error::unknown_field(&field, FIELDS)),
                    }
                }
                let action = action.ok_or_else(|| Error::missing_field("action"))?;
                let obj = obj.ok_or_else(|| Error::missing_field("obj"))?;
                let key = key.ok_or_else(|| Error::missing_field("key"))?;
                let pred = pred.ok_or_else(|| Error::missing_field("pred"))?;
                let insert = insert.unwrap_or(false);
                let value = Value::from(value, datatype);
                let action = match action {
                    ReqOpType::MakeMap => OpType::Make(ObjType::Map),
                    ReqOpType::MakeTable => OpType::Make(ObjType::Table),
                    ReqOpType::MakeList => OpType::Make(ObjType::List),
                    ReqOpType::MakeText => OpType::Make(ObjType::Text),
                    ReqOpType::Del => OpType::Del,
                    ReqOpType::Link => {
                        OpType::Link(child.ok_or_else(|| Error::missing_field("pred"))?)
                    }
                    ReqOpType::Set => OpType::Set(
                        Value::from(value, datatype)
                            .ok_or_else(|| Error::missing_field("value"))?,
                    ),
                    ReqOpType::Inc => match value {
                        Some(Value::Int(n)) => Ok(OpType::Inc(n)),
                        Some(Value::Uint(n)) => Ok(OpType::Inc(n as i64)),
                        Some(Value::F64(n)) => Ok(OpType::Inc(n as i64)),
                        Some(Value::F32(n)) => Ok(OpType::Inc(n as i64)),
                        Some(Value::Counter(n)) => Ok(OpType::Inc(n)),
                        Some(Value::Timestamp(n)) => Ok(OpType::Inc(n)),
                        Some(Value::Str(s)) => {
                            Err(Error::invalid_value(Unexpected::Str(&s), &"a number"))
                        }
                        Some(Value::Boolean(b)) => {
                            Err(Error::invalid_value(Unexpected::Bool(b), &"a number"))
                        }
                        Some(Value::Null) => {
                            Err(Error::invalid_value(Unexpected::Other("null"), &"a number"))
                        }
                        None => Err(Error::missing_field("value")),
                    }?,
                };
                Ok(Operation {
                    action,
                    obj,
                    key,
                    insert,
                    pred,
                })
            }
        }
        deserializer.deserialize_struct("Operation", &FIELDS, OperationVisitor)
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
                        .ok_or_else(|| Error::missing_field("value"))?
                        .adjust(datatype);
                    Ok(Diff::Value(value))
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

