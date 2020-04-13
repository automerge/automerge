use serde::de;
use serde::de::{Error, MapAccess, Unexpected, Visitor};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

use crate::patch::{Diff, DiffEdit, DiffLink, DiffValue};
use crate::protocol::{
    DataType, ElementID, Key, ObjType, ObjectID, OpID, OpType, Operation, PrimitiveValue,
    ReqOpType, RequestKey, UndoOperation,
};

impl Serialize for ObjectID {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            ObjectID::ID(id) => id.serialize(serializer),
            ObjectID::Str(s) => s.serialize(serializer),
            ObjectID::Root => serializer.serialize_str("00000000-0000-0000-0000-000000000000"),
        }
    }
}

impl<'de> Deserialize<'de> for ObjectID {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if s == "00000000-0000-0000-0000-000000000000" {
            Ok(ObjectID::Root)
        } else if let Some(id) = OpID::from_str(&s).ok() {
            Ok(ObjectID::ID(id))
        } else {
            Ok(ObjectID::Str(s))
        }
    }
}

impl<'de> Deserialize<'de> for OpID {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        OpID::from_str(&s)
            .map_err(|_| de::Error::invalid_value(de::Unexpected::Str(&s), &"A valid OpID"))
    }
}

impl Serialize for OpID {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.to_string().as_str())
    }
}

impl<'de> Deserialize<'de> for ElementID {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        ElementID::from_str(&s).map_err(|_| de::Error::custom("invalid element ID"))
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
            OpType::Set(_, _) => "set",
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
            OpType::Link(_) | OpType::Inc(_) | OpType::Set(_, DataType::Undefined) => fields += 1,
            OpType::Set(_, _) => fields += 2,
            _ => {}
        }

        let mut op = serializer.serialize_struct("UndoOperation", fields)?;
        op.serialize_field("action", &self.action)?;
        op.serialize_field("obj", &self.obj)?;
        op.serialize_field("key", &self.key)?;
        match &self.action {
            OpType::Link(child) => op.serialize_field("child", &child)?,
            OpType::Inc(n) => op.serialize_field("value", &n)?,
            OpType::Set(value, DataType::Undefined) => op.serialize_field("value", &value)?,
            OpType::Set(value, datatype) => {
                op.serialize_field("value", &value)?;
                op.serialize_field("datatype", &datatype)?;
            }
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
            OpType::Link(_) | OpType::Inc(_) | OpType::Set(_, DataType::Undefined) => fields += 1,
            OpType::Set(_, _) => fields += 2,
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
            OpType::Set(value, DataType::Undefined) => op.serialize_field("value", &value)?,
            OpType::Set(value, datatype) => {
                op.serialize_field("value", &value)?;
                op.serialize_field("datatype", &datatype)?;
            }
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
                let mut value: Option<PrimitiveValue> = None;
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
                        value.ok_or_else(|| Error::missing_field("value"))?,
                        datatype.unwrap_or(DataType::Undefined),
                    ),
                    ReqOpType::Inc => match value {
                        Some(PrimitiveValue::Number(f)) => Ok(OpType::Inc(f)),
                        Some(PrimitiveValue::Str(s)) => {
                            Err(Error::invalid_value(Unexpected::Str(&s), &"a number"))
                        }
                        Some(PrimitiveValue::Boolean(b)) => {
                            Err(Error::invalid_value(Unexpected::Bool(b), &"a number"))
                        }
                        Some(PrimitiveValue::Null) => {
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

impl<'de> Deserialize<'de> for DiffLink {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DiffLinkVisitor;
        const FIELDS: &[&str] = &["edits", "objType", "objectId", "props", "value", "datatype"];

        impl<'de> de::Visitor<'de> for DiffLinkVisitor {
            type Value = DiffLink;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("A difflink")
            }

            fn visit_map<V>(self, mut map: V) -> Result<Self::Value, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut edits: Option<Vec<DiffEdit>> = None;
                let mut object_id: Option<ObjectID> = None;
                let mut obj_type: Option<ObjType> = None;
                let mut props: Option<HashMap<Key, HashMap<OpID, DiffLink>>> = None;
                let mut value: Option<PrimitiveValue> = None;
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
                    let value = value.ok_or_else(|| Error::missing_field("value"))?;
                    let datatype = datatype.unwrap_or(DataType::Undefined);
                    Ok(DiffLink::Val(DiffValue { value, datatype }))
                } else {
                    let object_id = object_id.ok_or_else(|| Error::missing_field("objectId"))?;
                    let obj_type = obj_type.ok_or_else(|| Error::missing_field("type"))?;
                    Ok(DiffLink::Link(Diff {
                        object_id,
                        obj_type,
                        edits,
                        props,
                    }))
                }
            }
        }
        deserializer.deserialize_struct("DiffLink", &FIELDS, DiffLinkVisitor)
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

#[cfg(test)]
mod tests {
    use crate::{
        DataType, Diff, DiffEdit, DiffLink, DiffValue, Key, ObjType, ObjectID, OpID, PrimitiveValue,
    };
    use std::collections::HashMap;
    use std::str::FromStr;

    #[test]
    fn test_difflink_round_trip() {
        let difflink = DiffLink::Link(Diff {
            edits: Some(vec![
                DiffEdit::Insert { index: 1 },
                DiffEdit::Remove { index: 2 },
            ]),
            object_id: ObjectID::Root,
            obj_type: ObjType::Map,
            props: Some(
                vec![(
                    Key("somekey".into()),
                    vec![(
                        OpID::from_str("1@00d737c7-acf2-4447-bd99-57f4219e3bb2").unwrap(),
                        DiffLink::Val(DiffValue {
                            value: PrimitiveValue::Boolean(false),
                            datatype: DataType::Undefined,
                        }),
                    )]
                    .into_iter()
                    .collect::<HashMap<OpID, DiffLink>>(),
                )]
                .into_iter()
                .collect(),
            ),
        });
        let json = serde_json::to_value(difflink.clone()).unwrap();
        let deserialized_difflink: DiffLink = serde_json::from_value(json).unwrap();
        assert_eq!(difflink, deserialized_difflink);
    }
}
