use crate::{Operation, OpType, Value, DataType, ObjectID, ReqOpType, Key, OpID, ObjType};
use serde::{Serialize, Serializer, Deserialize, Deserializer, de::{Visitor, MapAccess, Unexpected, Error}};
use serde::ser::SerializeStruct;
use super::read_field;


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

