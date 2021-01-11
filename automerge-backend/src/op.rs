// FIXME
use crate::error::InvalidChangeError;
use crate::op_type::OpType;
use automerge_protocol as amp;
use serde::ser::SerializeStruct;
use serde::{
    de::{Error, MapAccess, Unexpected, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};
use std::convert::TryFrom;
use std::str::FromStr;

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
        value: amp::ScalarValue,
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
        value: amp::ScalarValue,
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
            OpType::Set(amp::ScalarValue::Timestamp(_)) => fields += 2,
            OpType::Set(amp::ScalarValue::Counter(_)) => fields += 2,
            OpType::Inc(_) | OpType::Set(_) => fields += 1,
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
            OpType::Inc(n) => op.serialize_field("value", &n)?,
            OpType::Set(amp::ScalarValue::Counter(value)) => {
                op.serialize_field("value", &value)?;
                op.serialize_field("datatype", &amp::DataType::Counter)?;
            }
            OpType::Set(amp::ScalarValue::Timestamp(value)) => {
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
                let mut value: Option<Option<amp::ScalarValue>> = None;
                while let Some(field) = map.next_key::<String>()? {
                    match field.as_ref() {
                        "action" => read_field("action", &mut action, &mut map)?,
                        "obj" => read_field("obj", &mut obj, &mut map)?,
                        "key" => read_field("key", &mut key, &mut map)?,
                        "pred" => read_field("pred", &mut pred, &mut map)?,
                        "insert" => read_field("insert", &mut insert, &mut map)?,
                        "datatype" => read_field("datatype", &mut datatype, &mut map)?,
                        "value" => read_field("value", &mut value, &mut map)?,
                        _ => return Err(Error::unknown_field(&field, FIELDS)),
                    }
                }
                let action = action.ok_or_else(|| Error::missing_field("action"))?;
                let obj = obj.ok_or_else(|| Error::missing_field("obj"))?;
                let key = key.ok_or_else(|| Error::missing_field("key"))?;
                let pred = pred.ok_or_else(|| Error::missing_field("pred"))?;
                let insert = insert.unwrap_or(false);
                let value = amp::ScalarValue::from(value.flatten(), datatype);
                let action = match action {
                    amp::OpType::MakeMap => OpType::Make(amp::ObjType::Map(amp::MapType::Map)),
                    amp::OpType::MakeTable => OpType::Make(amp::ObjType::Map(amp::MapType::Table)),
                    amp::OpType::MakeList => {
                        OpType::Make(amp::ObjType::Sequence(amp::SequenceType::List))
                    }
                    amp::OpType::MakeText => {
                        OpType::Make(amp::ObjType::Sequence(amp::SequenceType::Text))
                    }
                    amp::OpType::Del => OpType::Del,
                    amp::OpType::Set => OpType::Set(value.unwrap_or(amp::ScalarValue::Null)),
                    amp::OpType::Inc => match value {
                        Some(amp::ScalarValue::Int(n)) => Ok(OpType::Inc(n)),
                        Some(amp::ScalarValue::Uint(n)) => Ok(OpType::Inc(n as i64)),
                        Some(amp::ScalarValue::F64(n)) => Ok(OpType::Inc(n as i64)),
                        Some(amp::ScalarValue::F32(n)) => Ok(OpType::Inc(n as i64)),
                        Some(amp::ScalarValue::Counter(n)) => Ok(OpType::Inc(n)),
                        Some(amp::ScalarValue::Timestamp(n)) => Ok(OpType::Inc(n)),
                        Some(amp::ScalarValue::Str(s)) => {
                            Err(Error::invalid_value(Unexpected::Str(&s), &"a number"))
                        }
                        Some(amp::ScalarValue::Boolean(b)) => {
                            Err(Error::invalid_value(Unexpected::Bool(b), &"a number"))
                        }
                        Some(amp::ScalarValue::Null) => {
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

impl TryFrom<&amp::Op> for Operation {
    type Error = InvalidChangeError;
    fn try_from(op: &amp::Op) -> Result<Self, Self::Error> {
        let op_type = OpType::try_from(op)?;
        let obj_id = amp::ObjectID::from_str(&op.obj)?;
        Ok(Operation {
            action: op_type,
            obj: obj_id,
            key: op.key.clone(),
            pred: op.pred.clone(),
            insert: op.insert,
        })
    }
}

impl Into<amp::Op> for &Operation {
    fn into(self) -> amp::Op {
        let value = match &self.action {
            OpType::Del => None,
            OpType::Set(v) => Some(v.clone()),
            OpType::Make(_) => None,
            OpType::Inc(i) => Some(amp::ScalarValue::Counter(*i)),
        };
        let datatype = value
            .as_ref()
            .and_then(|v| match (v.datatype(), &self.action) {
                (Some(d), _) => Some(d),
                (None, OpType::Set(..)) => Some(amp::DataType::Undefined),
                _ => None,
            });
        amp::Op {
            obj: self.obj.to_string(),
            value,
            action: match self.action {
                OpType::Inc(_) => amp::OpType::Inc,
                OpType::Make(amp::ObjType::Map(amp::MapType::Map)) => amp::OpType::MakeMap,
                OpType::Make(amp::ObjType::Map(amp::MapType::Table)) => amp::OpType::MakeTable,
                OpType::Make(amp::ObjType::Sequence(amp::SequenceType::List)) => {
                    amp::OpType::MakeList
                }
                OpType::Make(amp::ObjType::Sequence(amp::SequenceType::Text)) => {
                    amp::OpType::MakeText
                }
                OpType::Set(..) => amp::OpType::Set,
                OpType::Del => amp::OpType::Del,
            },
            pred: self.pred.clone(),
            insert: self.insert,
            key: self.key.clone(),
            datatype,
        }
    }
}
