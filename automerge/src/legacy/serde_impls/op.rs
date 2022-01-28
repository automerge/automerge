use serde::{
    de::{Error, MapAccess, Unexpected, Visitor},
    ser::SerializeStruct,
    Deserialize, Deserializer, Serialize, Serializer,
};

use super::read_field;
use crate::legacy::{DataType, Key, ObjType, ObjectId, Op, OpId, OpType, ScalarValue, SortedVec};

impl Serialize for Op {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 4;

        if self.insert {
            fields += 1
        }

        let numerical_datatype = match &self.action {
            OpType::Set(value) => value.as_numerical_datatype(),
            _ => None,
        };

        if numerical_datatype.is_some() {
            fields += 2
        } else if !matches!(&self.action, OpType::Make(..)) {
            fields += 1
        };

        let mut op = serializer.serialize_struct("Operation", fields)?;
        op.serialize_field("action", &self.action)?;
        op.serialize_field("obj", &self.obj)?;
        op.serialize_field(
            if self.key.is_map_key() {
                "key"
            } else {
                "elemId"
            },
            &self.key,
        )?;
        if self.insert {
            op.serialize_field("insert", &self.insert)?;
        }
        if let Some(datatype) = numerical_datatype {
            op.serialize_field("datatype", &datatype)?;
        }
        match &self.action {
            OpType::Inc(n) => op.serialize_field("value", &n)?,
            OpType::Set(value) => op.serialize_field("value", &value)?,
            OpType::MarkBegin(m) => {
                op.serialize_field("name", &m.name)?;
                op.serialize_field("expand", &m.expand)?;
                op.serialize_field("value", &m.value)?;
            }
            OpType::MarkEnd(s) => op.serialize_field("expand", &s)?,
            _ => {}
        }
        op.serialize_field("pred", &self.pred)?;
        op.end()
    }
}

// We need to manually implement deserialization for `RawOpType`
// b/c by default rmp-serde (serde msgpack integration) serializes enums as maps with a
// single KV pair where the key is an integer representing the enum variant & the value
// is the associated data
// But we serialize `RawOpType` as a string, causing rmp-serde to choke on deserialization
#[derive(PartialEq, Debug, Clone, Copy)]
pub(crate) enum RawOpType {
    MakeMap,
    MakeTable,
    MakeList,
    MakeText,
    Del,
    Inc,
    Set,
    MarkBegin,
    MarkEnd,
}

impl Serialize for RawOpType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = match self {
            RawOpType::MakeMap => "makeMap",
            RawOpType::MakeTable => "makeTable",
            RawOpType::MakeList => "makeList",
            RawOpType::MakeText => "makeText",
            RawOpType::Del => "del",
            RawOpType::Inc => "inc",
            RawOpType::Set => "set",
            RawOpType::MarkBegin => "mark_begin",
            RawOpType::MarkEnd => "mark_end",
        };
        serializer.serialize_str(s)
    }
}

impl<'de> Deserialize<'de> for RawOpType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        const VARIANTS: &[&str] = &[
            "makeMap",
            "makeTable",
            "makeList",
            "makeText",
            "del",
            "inc",
            "set",
            "mark",
            "unmark",
        ];
        // TODO: Probably more efficient to deserialize to a `&str`
        let raw_type = String::deserialize(deserializer)?;
        match raw_type.as_str() {
            "makeMap" => Ok(RawOpType::MakeMap),
            "makeTable" => Ok(RawOpType::MakeTable),
            "makeList" => Ok(RawOpType::MakeList),
            "makeText" => Ok(RawOpType::MakeText),
            "del" => Ok(RawOpType::Del),
            "inc" => Ok(RawOpType::Inc),
            "set" => Ok(RawOpType::Set),
            "mark_begin" => Ok(RawOpType::MarkBegin),
            "mark_end" => Ok(RawOpType::MarkEnd),
            other => Err(Error::unknown_variant(other, VARIANTS)),
        }
    }
}

impl<'de> Deserialize<'de> for Op {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        const FIELDS: &[&str] = &["ops", "deps", "message", "seq", "actor", "requestType"];
        struct OperationVisitor;
        impl<'de> Visitor<'de> for OperationVisitor {
            type Value = Op;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("An operation object")
            }

            fn visit_map<V>(self, mut map: V) -> Result<Op, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut action: Option<RawOpType> = None;
                let mut obj: Option<ObjectId> = None;
                let mut key: Option<Key> = None;
                let mut pred: Option<SortedVec<OpId>> = None;
                let mut insert: Option<bool> = None;
                let mut datatype: Option<DataType> = None;
                let mut value: Option<Option<ScalarValue>> = None;
                let mut name: Option<String> = None;
                let mut expand: Option<bool> = None;
                let mut ref_id: Option<OpId> = None;
                while let Some(field) = map.next_key::<String>()? {
                    match field.as_ref() {
                        "action" => read_field("action", &mut action, &mut map)?,
                        "obj" => read_field("obj", &mut obj, &mut map)?,
                        "key" => {
                            if key.is_some() {
                                return Err(Error::duplicate_field("key"));
                            } else {
                                key = Some(Key::Map(map.next_value()?));
                            }
                        }
                        "elemId" => {
                            if key.is_some() {
                                return Err(Error::duplicate_field("elemId"));
                            } else {
                                key = Some(Key::Seq(map.next_value()?))
                            }
                        }
                        "pred" => read_field("pred", &mut pred, &mut map)?,
                        "insert" => read_field("insert", &mut insert, &mut map)?,
                        "datatype" => read_field("datatype", &mut datatype, &mut map)?,
                        "value" => read_field("value", &mut value, &mut map)?,
                        "name" => read_field("name", &mut name, &mut map)?,
                        "expand" => read_field("expand", &mut expand, &mut map)?,
                        "ref" => read_field("ref", &mut ref_id, &mut map)?,
                        _ => return Err(Error::unknown_field(&field, FIELDS)),
                    }
                }
                let action = action.ok_or_else(|| Error::missing_field("action"))?;
                let obj = obj.ok_or_else(|| Error::missing_field("obj"))?;
                let key = key.ok_or_else(|| Error::missing_field("key"))?;
                let pred = pred.ok_or_else(|| Error::missing_field("pred"))?;
                let insert = insert.unwrap_or(false);
                let action = match action {
                    RawOpType::MakeMap => OpType::Make(ObjType::Map),
                    RawOpType::MakeTable => OpType::Make(ObjType::Table),
                    RawOpType::MakeList => OpType::Make(ObjType::List),
                    RawOpType::MakeText => OpType::Make(ObjType::Text),
                    RawOpType::Del => OpType::Del,
                    RawOpType::MarkBegin => {
                        let name = name.ok_or_else(|| Error::missing_field("mark(name)"))?;
                        let expand = expand.unwrap_or(false);
                        let value = if let Some(datatype) = datatype {
                            let raw_value = value
                                .ok_or_else(|| Error::missing_field("value"))?
                                .unwrap_or(ScalarValue::Null);
                            raw_value.as_datatype(datatype).map_err(|e| {
                                Error::invalid_value(
                                    Unexpected::Other(e.unexpected.as_str()),
                                    &e.expected.as_str(),
                                )
                            })?
                        } else {
                            value
                                .ok_or_else(|| Error::missing_field("value"))?
                                .unwrap_or(ScalarValue::Null)
                        };
                        OpType::mark(name, expand, value)
                    }
                    RawOpType::MarkEnd => {
                        let expand = expand.unwrap_or(true);
                        OpType::MarkEnd(expand)
                    }
                    RawOpType::Set => {
                        let value = if let Some(datatype) = datatype {
                            let raw_value = value
                                .ok_or_else(|| Error::missing_field("value"))?
                                .unwrap_or(ScalarValue::Null);
                            raw_value.as_datatype(datatype).map_err(|e| {
                                Error::invalid_value(
                                    Unexpected::Other(e.unexpected.as_str()),
                                    &e.expected.as_str(),
                                )
                            })?
                        } else {
                            value
                                .ok_or_else(|| Error::missing_field("value"))?
                                .unwrap_or(ScalarValue::Null)
                        };
                        OpType::Set(value)
                    }
                    RawOpType::Inc => match value.flatten() {
                        Some(ScalarValue::Int(n)) => Ok(OpType::Inc(n)),
                        Some(ScalarValue::Uint(n)) => Ok(OpType::Inc(n as i64)),
                        Some(ScalarValue::F64(n)) => Ok(OpType::Inc(n as i64)),
                        Some(ScalarValue::Counter(n)) => Ok(OpType::Inc(n.into())),
                        Some(ScalarValue::Timestamp(n)) => Ok(OpType::Inc(n)),
                        Some(ScalarValue::Bytes(s)) => {
                            Err(Error::invalid_value(Unexpected::Bytes(&s), &"a number"))
                        }
                        Some(ScalarValue::Str(s)) => {
                            Err(Error::invalid_value(Unexpected::Str(&s), &"a number"))
                        }
                        Some(ScalarValue::Boolean(b)) => {
                            Err(Error::invalid_value(Unexpected::Bool(b), &"a number"))
                        }
                        Some(ScalarValue::Null) => {
                            Err(Error::invalid_value(Unexpected::Other("null"), &"a number"))
                        }
                        None => Err(Error::missing_field("value")),
                    }?,
                };
                Ok(Op {
                    action,
                    obj,
                    key,
                    pred,
                    insert,
                })
            }
        }
        deserializer.deserialize_struct("Operation", FIELDS, OperationVisitor)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use crate::legacy as amp;

    #[test]
    fn test_deserialize_action() {
        struct Scenario {
            name: &'static str,
            json: serde_json::Value,
            expected: Result<Op, serde_json::Error>,
        }
        let scenarios: Vec<Scenario> = vec![
            Scenario {
                name: "Set with Uint",
                json: serde_json::json!({
                    "action": "set",
                    "obj": "_root",
                    "key": "somekey",
                    "datatype": "uint",
                    "value": 123,
                    "pred": []
                }),
                expected: Ok(Op {
                    action: OpType::Set(ScalarValue::Uint(123)),
                    obj: ObjectId::Root,
                    key: "somekey".into(),
                    insert: false,
                    pred: SortedVec::new(),
                }),
            },
            Scenario {
                name: "Set with Int",
                json: serde_json::json!({
                    "action": "set",
                    "obj": "_root",
                    "key": "somekey",
                    "value": -123,
                    "datatype": "int",
                    "pred": []
                }),
                expected: Ok(Op {
                    action: OpType::Set(ScalarValue::Int(-123)),
                    obj: ObjectId::Root,
                    key: "somekey".into(),
                    insert: false,
                    pred: SortedVec::new(),
                }),
            },
            Scenario {
                name: "Set with F64",
                json: serde_json::json!({
                    "action": "set",
                    "obj": "_root",
                    "key": "somekey",
                    "value": -123,
                    "datatype": "float64",
                    "pred": []
                }),
                expected: Ok(Op {
                    action: OpType::Set(ScalarValue::F64(-123.0)),
                    obj: ObjectId::Root,
                    key: "somekey".into(),
                    insert: false,
                    pred: Vec::new().into(),
                }),
            },
            Scenario {
                name: "Set with string",
                json: serde_json::json!({
                    "action": "set",
                    "obj": "_root",
                    "key": "somekey",
                    "value": "somestring",
                    "pred": []
                }),
                expected: Ok(Op {
                    action: OpType::Set(ScalarValue::Str("somestring".into())),
                    obj: ObjectId::Root,
                    key: "somekey".into(),
                    insert: false,
                    pred: SortedVec::new(),
                }),
            },
            Scenario {
                name: "Set with f64",
                json: serde_json::json!({
                    "action": "set",
                    "obj": "_root",
                    "key": "somekey",
                    "value": 1.23,
                    "pred": []
                }),
                expected: Ok(Op {
                    action: OpType::Set(ScalarValue::F64(1.23)),
                    obj: ObjectId::Root,
                    key: "somekey".into(),
                    insert: false,
                    pred: SortedVec::new(),
                }),
            },
            Scenario {
                name: "Set with boolean",
                json: serde_json::json!({
                    "action": "set",
                    "obj": "_root",
                    "key": "somekey",
                    "value": true,
                    "pred": []
                }),
                expected: Ok(Op {
                    action: OpType::Set(ScalarValue::Boolean(true)),
                    obj: ObjectId::Root,
                    key: "somekey".into(),
                    insert: false,
                    pred: SortedVec::new(),
                }),
            },
            Scenario {
                name: "Set without value",
                json: serde_json::json!({
                    "action": "set",
                    "obj": "_root",
                    "key": "somekey",
                    "datatype": "counter",
                    "pred": []
                }),
                expected: Err(serde_json::Error::missing_field("value")),
            },
            Scenario {
                name: "Set with counter",
                json: serde_json::json!({
                    "action": "set",
                    "obj": "_root",
                    "key": "somekey",
                    "value": 123,
                    "datatype": "counter",
                    "pred": []
                }),
                expected: Ok(Op {
                    action: OpType::Set(ScalarValue::Counter(123.into())),
                    obj: ObjectId::Root,
                    key: "somekey".into(),
                    insert: false,
                    pred: SortedVec::new(),
                }),
            },
            Scenario {
                name: "Set with counter datatype and string value",
                json: serde_json::json!({
                    "action": "set",
                    "obj": "_root",
                    "key": "somekey",
                    "value": "somestring",
                    "datatype": "counter",
                    "pred": []
                }),
                expected: Err(serde_json::Error::invalid_value(
                    Unexpected::Other("\"somestring\""),
                    &"an integer",
                )),
            },
            Scenario {
                name: "Set with timestamp datatype and string value",
                json: serde_json::json!({
                    "action": "set",
                    "obj": "_root",
                    "key": "somekey",
                    "value": "somestring",
                    "datatype": "timestamp",
                    "pred": []
                }),
                expected: Err(serde_json::Error::invalid_value(
                    Unexpected::Other("\"somestring\""),
                    &"an integer",
                )),
            },
            Scenario {
                name: "Inc with counter",
                json: serde_json::json!({
                    "action": "inc",
                    "obj": "_root",
                    "key": "somekey",
                    "value": 12,
                    "datatype": "counter",
                    "pred": []
                }),
                expected: Ok(Op {
                    action: OpType::Inc(12),
                    obj: ObjectId::Root,
                    key: "somekey".into(),
                    insert: false,
                    pred: SortedVec::new(),
                }),
            },
            Scenario {
                name: "Inc without counter",
                json: serde_json::json!({
                    "action": "inc",
                    "obj": "_root",
                    "key": "somekey",
                    "value": 12,
                    "pred": []
                }),
                expected: Ok(Op {
                    action: OpType::Inc(12),
                    obj: ObjectId::Root,
                    key: "somekey".into(),
                    insert: false,
                    pred: SortedVec::new(),
                }),
            },
            Scenario {
                name: "Inc without value",
                json: serde_json::json!({
                    "action": "inc",
                    "obj": "_root",
                    "key": "somekey",
                    "pred": []
                }),
                expected: Err(serde_json::Error::missing_field("value")),
            },
            Scenario {
                name: "Set with null",
                json: serde_json::json!({
                    "action": "set",
                    "obj": "_root",
                    "key": "somekey",
                    "value": null,
                    "pred": []
                }),
                expected: Ok(Op {
                    action: OpType::Set(ScalarValue::Null),
                    obj: ObjectId::Root,
                    key: "somekey".into(),
                    insert: false,
                    pred: SortedVec::new(),
                }),
            },
        ];

        for scenario in scenarios.into_iter() {
            let result: serde_json::Result<Op> = serde_json::from_value(scenario.json);
            match (result, scenario.expected) {
                (Ok(result_op), Ok(expected_op)) => assert_eq!(
                    result_op, expected_op,
                    "Scenario {}: Expected Ok({:?}) but got Ok({:?})",
                    scenario.name, expected_op, result_op
                ),
                (Ok(result_op), Err(e)) => panic!(
                    "Scenario {}: expected Err({:?}) but got Ok({:?})",
                    scenario.name, e, result_op
                ),
                (Err(result_e), Err(expected_e)) => assert_eq!(
                    result_e.to_string(),
                    expected_e.to_string(),
                    "Scenario {}: expected Err({:?}) but got Err({:?})",
                    scenario.name,
                    expected_e,
                    result_e
                ),
                (Err(result_e), Ok(expected)) => panic!(
                    "Scenario {}: expected Ok({:?}) but got Err({:?})",
                    scenario.name, expected, result_e
                ),
            }
        }
    }

    #[test]
    fn test_deserialize_obj() {
        let root: Op = serde_json::from_value(serde_json::json!({
            "action": "inc",
            "obj": "_root",
            "key": "somekey",
            "value": 1,
            "pred": []
        }))
        .unwrap();
        assert_eq!(root.obj, amp::ObjectId::Root);

        let opid: Op = serde_json::from_value(serde_json::json!({
            "action": "inc",
            "obj": "1@7ef48769b04d47e9a88e98a134d62716",
            "key": "somekey",
            "value": 1,
            "pred": []
        }))
        .unwrap();
        assert_eq!(
            opid.obj,
            amp::ObjectId::from_str("1@7ef48769b04d47e9a88e98a134d62716").unwrap()
        );

        let invalid: Result<Op, serde_json::Error> = serde_json::from_value(serde_json::json!({
            "action": "inc",
            "obj": "notanobject",
            "key": "somekey",
            "value": 1,
            "pred": []
        }));
        match invalid {
            Ok(_) => panic!("Parsing an invalid object id should fail"),
            Err(e) => assert!(e.to_string().contains("A valid ObjectID")),
        }
    }

    #[test]
    fn test_serialize_key() {
        let map_key = Op {
            action: OpType::Inc(12),
            obj: ObjectId::Root,
            key: "somekey".into(),
            insert: false,
            pred: SortedVec::new(),
        };
        let json = serde_json::to_value(map_key).unwrap();
        let expected: serde_json::Value = "somekey".into();
        assert_eq!(json.as_object().unwrap().get("key"), Some(&expected));

        let elemid_key = Op {
            action: OpType::Inc(12),
            obj: ObjectId::Root,
            key: OpId::from_str("1@7ef48769b04d47e9a88e98a134d62716")
                .unwrap()
                .into(),
            insert: false,
            pred: SortedVec::new(),
        };
        let json = serde_json::to_value(elemid_key).unwrap();
        let expected: serde_json::Value = "1@7ef48769b04d47e9a88e98a134d62716".into();
        assert_eq!(json.as_object().unwrap().get("elemId"), Some(&expected));
    }

    #[test]
    fn test_round_trips() {
        let testcases = vec![
            Op {
                action: OpType::Set(ScalarValue::Uint(12)),
                obj: ObjectId::Root,
                key: "somekey".into(),
                insert: false,
                pred: SortedVec::new(),
            },
            Op {
                action: OpType::Inc(12),
                obj: ObjectId::from_str("1@7ef48769b04d47e9a88e98a134d62716").unwrap(),
                key: "somekey".into(),
                insert: false,
                pred: SortedVec::new(),
            },
            Op {
                action: OpType::Set(ScalarValue::Uint(12)),
                obj: ObjectId::from_str("1@7ef48769b04d47e9a88e98a134d62716").unwrap(),
                key: "somekey".into(),
                insert: false,
                pred: vec![OpId::from_str("1@7ef48769b04d47e9a88e98a134d62716").unwrap()].into(),
            },
            Op {
                action: OpType::Inc(12),
                obj: ObjectId::Root,
                key: "somekey".into(),
                insert: false,
                pred: SortedVec::new(),
            },
            Op {
                action: OpType::Set("seomthing".into()),
                obj: ObjectId::from_str("1@7ef48769b04d47e9a88e98a134d62716").unwrap(),
                key: OpId::from_str("1@7ef48769b04d47e9a88e98a134d62716")
                    .unwrap()
                    .into(),
                insert: false,
                pred: vec![OpId::from_str("1@7ef48769b04d47e9a88e98a134d62716").unwrap()].into(),
            },
        ];
        for (testcase_num, testcase) in testcases.iter().enumerate() {
            #[allow(clippy::expect_fun_call)]
            let serialized = serde_json::to_string(testcase)
                .expect(format!("Failed to serialize testcase {}", testcase_num).as_str());
            #[allow(clippy::expect_fun_call)]
            let deserialized: Op = serde_json::from_str(&serialized)
                .expect(format!("Failed to deserialize testcase {}", testcase_num).as_str());
            assert_eq!(testcase, &deserialized, "Testcase {} failed", testcase_num);
        }
    }
}
