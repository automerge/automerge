// FIXME
use crate::op_type::OpType;
use automerge_protocol as amp;
use serde::ser::SerializeStruct;
use serde::{
    de::{Error, MapAccess, Unexpected, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};

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

#[derive(PartialEq, Debug, Clone)]
pub struct Operation {
    pub action: OpType,
    pub obj: amp::ObjectID,
    pub key: amp::Key,
    pub pred: Vec<amp::OpID>,
    pub insert: bool,
}

impl Operation {
    pub fn set(
        obj: amp::ObjectID,
        key: amp::Key,
        value: amp::Value,
        pred: Vec<amp::OpID>,
    ) -> Operation {
        Operation {
            action: OpType::Set(value),
            obj,
            key,
            insert: false,
            pred,
        }
    }

    pub fn insert(
        obj: amp::ObjectID,
        key: amp::Key,
        value: amp::Value,
        pred: Vec<amp::OpID>,
    ) -> Operation {
        Operation {
            action: OpType::Set(value),
            obj,
            key,
            insert: true,
            pred,
        }
    }

    pub fn inc(obj: amp::ObjectID, key: amp::Key, value: i64, pred: Vec<amp::OpID>) -> Operation {
        Operation {
            action: OpType::Inc(value),
            obj,
            key,
            insert: false,
            pred,
        }
    }

    pub fn del(obj: amp::ObjectID, key: amp::Key, pred: Vec<amp::OpID>) -> Operation {
        Operation {
            action: OpType::Del,
            obj,
            key,
            insert: false,
            pred,
        }
    }

    pub fn is_make(&self) -> bool {
        self.obj_type().is_some()
    }

    pub fn is_basic_assign(&self) -> bool {
        !self.insert
            && match self.action {
                OpType::Del | OpType::Set(_) | OpType::Inc(_) | OpType::Link(_) => true,
                _ => false,
            }
    }

    pub fn is_inc(&self) -> bool {
        match self.action {
            OpType::Inc(_) => true,
            _ => false,
        }
    }

    pub fn obj_type(&self) -> Option<amp::ObjType> {
        match self.action {
            OpType::Make(t) => Some(t),
            _ => None,
        }
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
            OpType::Set(amp::Value::Timestamp(_)) => fields += 2,
            OpType::Set(amp::Value::Counter(_)) => fields += 2,
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
            OpType::Set(amp::Value::Counter(value)) => {
                op.serialize_field("value", &value)?;
                op.serialize_field("datatype", &amp::DataType::Counter)?;
            }
            OpType::Set(amp::Value::Timestamp(value)) => {
                op.serialize_field("value", &value)?;
                op.serialize_field("datatype", &amp::DataType::Timestamp)?;
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
                let mut action: Option<amp::OpType> = None;
                let mut obj: Option<amp::ObjectID> = None;
                let mut key: Option<amp::Key> = None;
                let mut pred: Option<Vec<amp::OpID>> = None;
                let mut insert: Option<bool> = None;
                let mut datatype: Option<amp::DataType> = None;
                let mut value: Option<amp::Value> = None;
                let mut child: Option<amp::ObjectID> = None;
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
                let value = amp::Value::from(value, datatype);
                let action = match action {
                    amp::OpType::MakeMap => OpType::Make(amp::ObjType::Map),
                    amp::OpType::MakeTable => OpType::Make(amp::ObjType::Table),
                    amp::OpType::MakeList => OpType::Make(amp::ObjType::List),
                    amp::OpType::MakeText => OpType::Make(amp::ObjType::Text),
                    amp::OpType::Del => OpType::Del,
                    amp::OpType::Link => {
                        OpType::Link(child.ok_or_else(|| Error::missing_field("pred"))?)
                    }
                    amp::OpType::Set => OpType::Set(
                        amp::Value::from(value, datatype)
                            .ok_or_else(|| Error::missing_field("value"))?,
                    ),
                    amp::OpType::Inc => match value {
                        Some(amp::Value::Int(n)) => Ok(OpType::Inc(n)),
                        Some(amp::Value::Uint(n)) => Ok(OpType::Inc(n as i64)),
                        Some(amp::Value::F64(n)) => Ok(OpType::Inc(n as i64)),
                        Some(amp::Value::F32(n)) => Ok(OpType::Inc(n as i64)),
                        Some(amp::Value::Counter(n)) => Ok(OpType::Inc(n)),
                        Some(amp::Value::Timestamp(n)) => Ok(OpType::Inc(n)),
                        Some(amp::Value::Str(s)) => {
                            Err(Error::invalid_value(Unexpected::Str(&s), &"a number"))
                        }
                        Some(amp::Value::Boolean(b)) => {
                            Err(Error::invalid_value(Unexpected::Bool(b), &"a number"))
                        }
                        Some(amp::Value::Null) => {
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
