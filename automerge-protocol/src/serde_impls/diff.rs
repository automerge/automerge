use std::{collections::HashMap, fmt};

use serde::{
    de,
    de::{Error, MapAccess},
    ser::SerializeStruct,
    Deserialize, Deserializer, Serialize, Serializer,
};

use super::read_field;
use crate::{
    CursorDiff, DataType, Diff, DiffEdit, MapDiff, ObjType, ObjectId, OpId, ScalarValue, SeqDiff,
};

impl Serialize for Diff {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Diff::Map(diff) => diff.serialize(serializer),
            Diff::Seq(diff) => diff.serialize(serializer),
            Diff::Value(val) => match val {
                ScalarValue::Counter(_) => {
                    let mut op = serializer.serialize_struct("Value", 3)?;
                    op.serialize_field("value", &val)?;
                    op.serialize_field("datatype", "counter")?;
                    op.serialize_field("type", "value")?;
                    op.end()
                }
                ScalarValue::Timestamp(_) => {
                    let mut op = serializer.serialize_struct("Value", 3)?;
                    op.serialize_field("value", &val)?;
                    op.serialize_field("datatype", "timestamp")?;
                    op.serialize_field("type", "value")?;
                    op.end()
                }
                _ => {
                    let mut op = serializer.serialize_struct("Value", 2)?;
                    op.serialize_field("value", &val)?;
                    op.serialize_field("type", "value")?;
                    op.end()
                }
            },
            Diff::Cursor(diff) => diff.serialize(serializer),
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
enum RawDiffType {
    Value,
    Map,
    Text,
    List,
    Table,
}

impl RawDiffType {
    fn obj_type(&self) -> Option<ObjType> {
        match self {
            RawDiffType::Map => Some(ObjType::map()),
            RawDiffType::Table => Some(ObjType::table()),
            RawDiffType::List => Some(ObjType::list()),
            RawDiffType::Text => Some(ObjType::text()),
            RawDiffType::Value => None,
        }
    }
}

impl<'de> Deserialize<'de> for Diff {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DiffVisitor;
        const FIELDS: &[&str] = &[
            "edits",
            "objType",
            "objectId",
            "props",
            "value",
            "datatype",
            "refObjectId",
            "elemId",
            "index",
        ];

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
                let mut object_id: Option<ObjectId> = None;
                let mut diff_type: Option<RawDiffType> = None;
                //let mut obj_type: Option<ObjType> = None;
                let mut props: Option<HashMap<String, HashMap<OpId, Diff>>> = None;
                let mut value: Option<ScalarValue> = None;
                let mut datatype: Option<DataType> = None;
                let mut elem_id: Option<OpId> = None;
                let mut index: Option<u32> = None;
                let mut ref_object_id: Option<ObjectId> = None;

                while let Some(field) = map.next_key::<String>()? {
                    match field.as_ref() {
                        "edits" => read_field("edits", &mut edits, &mut map)?,
                        "objectId" => read_field("objectId", &mut object_id, &mut map)?,
                        "type" => read_field("type", &mut diff_type, &mut map)?,
                        "props" => read_field("props", &mut props, &mut map)?,
                        "value" => read_field("value", &mut value, &mut map)?,
                        "datatype" => read_field("datatype", &mut datatype, &mut map)?,
                        "refObjectId" => read_field("refObjectId", &mut ref_object_id, &mut map)?,
                        "elemId" => read_field("elemId", &mut elem_id, &mut map)?,
                        "index" => read_field("index", &mut index, &mut map)?,
                        _ => return Err(Error::unknown_field(&field, FIELDS)),
                    }
                }
                if value.is_some() || datatype.is_some() {
                    let datatype = datatype.unwrap_or(DataType::Undefined);
                    match datatype {
                        DataType::Cursor => {
                            let ref_object_id =
                                ref_object_id.ok_or_else(|| Error::missing_field("refObjectId"))?;
                            let elem_id = elem_id.ok_or_else(|| Error::missing_field("elemId"))?;
                            let index = index.ok_or_else(|| Error::missing_field("index"))?;
                            Ok(Diff::Cursor(CursorDiff {
                                object_id: ref_object_id,
                                elem_id,
                                index,
                            }))
                        }
                        _ => {
                            let value = value.ok_or_else(|| Error::missing_field("value"))?;
                            let value_with_datatype = maybe_add_datatype_to_value(value, datatype);
                            Ok(Diff::Value(value_with_datatype))
                        }
                    }
                } else {
                    let object_id = object_id.ok_or_else(|| Error::missing_field("objectId"))?;
                    let diff_type = diff_type.ok_or_else(|| Error::missing_field("type"))?;
                    match diff_type.obj_type() {
                        Some(obj_type) => match obj_type {
                            ObjType::Sequence(seq_type) => {
                                let edits = edits.ok_or_else(|| Error::missing_field("edits"))?;
                                Ok(Diff::Seq(SeqDiff {
                                    object_id,
                                    obj_type: seq_type,
                                    edits,
                                }))
                            },
                            ObjType::Map(map_type) => {
                                let props = props.ok_or_else(|| Error::missing_field("props"))?;
                                Ok(Diff::Map(MapDiff{
                                    object_id,
                                    obj_type: map_type,
                                    props,
                                }))
                            },
                        }
                        None => Err(Error::custom("'type' field must be one of ['list', 'text', 'table', 'map'] for an object diff"))
                    }

                    //if let Some(props) = props {
                    //match obj_type {
                    //ObjType::Map(map_type) => Ok(Diff::Map(MapDiff {
                    //object_id,
                    //obj_type: map_type,
                    //props,
                    //})),
                    //_ => Err(Error::invalid_value(Unexpected::Str(&obj_type.to_string()), &"'map' or 'table'"))
                    //}
                    //} else if let Some(edits) = edits {
                    //match obj_type {
                    //ObjType::Sequence(seq_type) => {
                    //let edits = edits.ok_or_else(|| Error::missing_field("edits"))?;
                    //Ok(Diff::Seq(SeqDiff {
                    //object_id,
                    //obj_type: seq_type,
                    //edits,
                    //}))
                    //}
                    //_ => Err(Error::invalid_value(Unexpected::Str(&obj_type.to_string()), &"'list' or 'text'"))
                    //}
                    //} else {
                    //Ok(Diff::Unchanged(ObjDiff {
                    //object_id,
                    //obj_type,
                    //}))
                    //}
                }
            }
        }
        deserializer.deserialize_struct("Diff", &FIELDS, DiffVisitor)
    }
}

fn maybe_add_datatype_to_value(value: ScalarValue, datatype: DataType) -> ScalarValue {
    match datatype {
        DataType::Counter => {
            if let Some(n) = value.to_i64() {
                ScalarValue::Counter(n)
            } else {
                value
            }
        }
        DataType::Timestamp => {
            if let Some(n) = value.to_i64() {
                ScalarValue::Timestamp(n)
            } else {
                value
            }
        }
        _ => value,
    }
}

#[cfg(test)]
mod tests {
    use std::{convert::TryInto, str::FromStr};

    use maplit::hashmap;

    use crate::{CursorDiff, Diff, MapDiff, MapType, ObjectId, OpId, SeqDiff, SequenceType};

    #[test]
    fn map_diff_serialization_round_trip() {
        let json = serde_json::json!({
            "objectId": "1@6121f8757d5d46609b665218b2b3a141",
            "type": "map",
            "props": {
                "key": {
                    "1@4a093244de2b4fd0a4203724e15dfc16": {
                        "type": "value",
                        "value": "value",
                    }
                }
            }
        });
        let diff = Diff::Map(MapDiff {
            object_id: ObjectId::from_str("1@6121f8757d5d46609b665218b2b3a141").unwrap(),
            obj_type: MapType::Map,
            props: hashmap! {
                "key".to_string() => hashmap!{
                    OpId::from_str("1@4a093244de2b4fd0a4203724e15dfc16").unwrap() => "value".into()
                }
            },
        });

        assert_eq!(json, serde_json::to_value(diff.clone()).unwrap());
        assert_eq!(serde_json::from_value::<Diff>(json).unwrap(), diff);
    }

    #[test]
    fn seq_diff_serialization_round_trip() {
        let json = serde_json::json!({
            "objectId": "1@6121f8757d5d46609b665218b2b3a141",
            "type": "list",
            "edits": []
                //{
                    //"action": "insert",
                    //"index": 1,
                    //"elemId": "1@6121f8757d5d46609b665218b2b3a141",
                    //"value": {"type": "value", "value": 1},
                //},
                //{
                    //"action": "multi-insert",
                    //"index": 1,
                    //"opId": "1@6121f8757d5d46609b665218b2b3a141",
                    //"values": [1, 2],
                //},
                //{
                    //"action": "update",
                    //"index": 1,
                    //"opId": "1@6121f8757d5d46609b665218b2b3a141",
                    //"value": {"type": "value", "value": 1},
                //},
                //{
                    //"action": "remove",
                    //"index": 1,
                    //"count": 2,
                //}
            //],
        });
        let diff = Diff::Seq(SeqDiff {
            object_id: ObjectId::from_str("1@6121f8757d5d46609b665218b2b3a141").unwrap(),
            obj_type: SequenceType::List,
            edits: vec![], //DiffEdit::SingleElementInsert{
                           //index: 1,
                           //elem_id: ElementId::from_str("1@6121f8757d5d46609b665218b2b3a141").unwrap(),
                           //value: Diff::Value(1.into()),
                           //},
                           //DiffEdit::MultiElementInsert{
                           //index: 1,
                           //first_opid: OpId::from_str("1@6121f8757d5d46609b665218b2b3a141").unwrap(),
                           //values: vec![
                           //1.into(),
                           //2.into(),
                           //],
                           //},
                           //DiffEdit::Update{
                           //index: 1,
                           //value: Diff::Value(1.into()),
                           //opid: OpId::from_str("1@6121f8757d5d46609b665218b2b3a141").unwrap(),
                           //},
                           //DiffEdit::Remove {
                           //index: 1,
                           //count: 2,
                           //}
                           //]
        });

        assert_eq!(json, serde_json::to_value(diff.clone()).unwrap());
        assert_eq!(serde_json::from_value::<Diff>(json).unwrap(), diff);
    }

    #[test]
    fn cursor_diff_serialization_round_trip() {
        let json = serde_json::json!({
            "datatype": "cursor",
            "refObjectId": "1@4a093244de2b4fd0a4203724e15dfc16",
            "elemId": "2@4a093244de2b4fd0a4203724e15dfc16",
            "index": 0,
        });
        let diff = Diff::Cursor(CursorDiff {
            object_id: "1@4a093244de2b4fd0a4203724e15dfc16".try_into().unwrap(),
            elem_id: "2@4a093244de2b4fd0a4203724e15dfc16".try_into().unwrap(),
            index: 0,
        });
        assert_eq!(json, serde_json::to_value(diff.clone()).unwrap());
        assert_eq!(serde_json::from_value::<Diff>(json).unwrap(), diff);
    }
}
