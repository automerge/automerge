use crate::{ChangeRequest, ChangeRequestType, ActorID, Clock, Operation};
use serde::de::{Error, MapAccess, Unexpected, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

impl Serialize for ChangeRequest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map_serializer = serializer.serialize_map(None)?;
        map_serializer.serialize_entry("actor", &self.actor_id)?;
        map_serializer.serialize_entry("deps", &self.dependencies)?;
        map_serializer.serialize_entry("message", &self.message)?;
        map_serializer.serialize_entry("seq", &self.seq)?;
        match &self.request_type {
            ChangeRequestType::Change(ops) => {
                map_serializer.serialize_entry("requestType", "change")?;
                map_serializer.serialize_entry("ops", &ops)?;
            },
            ChangeRequestType::Undo => map_serializer.serialize_entry("requestType", "undo")?,
            ChangeRequestType::Redo => map_serializer.serialize_entry("requestType", "redo")?,
        };
        map_serializer.end()
    }
}

impl<'de> Deserialize<'de> for ChangeRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        const FIELDS: &[&str] = &["ops", "deps", "message", "seq", "actor", "requestType"];
        struct ChangeRequestVisitor;
        impl<'de> Visitor<'de> for ChangeRequestVisitor {
            type Value = ChangeRequest;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("A change request object")
            }

            fn visit_map<V>(self, mut map: V) -> Result<ChangeRequest, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut actor: Option<ActorID> = None;
                let mut deps: Option<Clock> = None;
                let mut message: Option<Option<String>> = None;
                let mut seq: Option<u32> = None;
                let mut ops: Option<Vec<Operation>> = None;
                let mut request_type_str: Option<String> = None;

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_ref() {
                        "actor" => {
                            if actor.is_some() {
                                return Err(Error::duplicate_field("actor"));
                            }
                            actor = Some(map.next_value()?);
                        }
                        "deps" => {
                            if deps.is_some() {
                                return Err(Error::duplicate_field("deps"));
                            }
                            deps = Some(map.next_value()?);
                        }
                        "message" => {
                            if message.is_some() {
                                return Err(Error::duplicate_field("message"));
                            }
                            message = map.next_value()?;
                        }
                        "seq" => {
                            if seq.is_some() {
                                return Err(Error::duplicate_field("seq"));
                            }
                            seq = Some(map.next_value()?);
                        }
                        "ops" => {
                            if ops.is_some() {
                                return Err(Error::duplicate_field("ops"));
                            }
                            ops = Some(map.next_value()?);
                        }
                        "requestType" => {
                            if request_type_str.is_some() {
                                return Err(Error::duplicate_field("requestType"));
                            }
                            request_type_str = Some(map.next_value()?);
                        }
                        _ => return Err(Error::unknown_field(&key, FIELDS)),
                    }
                };

                let actor = actor.ok_or_else(|| Error::missing_field("actor"))?;
                let deps = deps.ok_or_else(|| Error::missing_field("deps"))?;
                let seq = seq.ok_or_else(||  Error::missing_field("seq"))?;
                let request_type_str = request_type_str.ok_or_else(|| Error::missing_field("requestType"))?;

                let request_type = match request_type_str.as_ref() {
                    "change" => {
                        let ops = ops.ok_or_else(|| Error::missing_field("ops"))?;
                        ChangeRequestType::Change(ops)
                    },
                    "undo" => ChangeRequestType::Undo,
                    "redo" => ChangeRequestType::Redo,
                    _ => {
                        return Err(Error::invalid_value(
                            Unexpected::Str(&request_type_str),
                            &"A valid change request type",
                        ))
                    }
                };

                Ok(ChangeRequest{actor_id: actor, dependencies: deps, seq, request_type, message: message.unwrap_or(None)})
            }
        }
        deserializer.deserialize_struct("ChangeReqest", &FIELDS, ChangeRequestVisitor)
    }
}

#[cfg(test)]
mod tests {
    //use super::*;
    use crate::{ActorID, ChangeRequest, ChangeRequestType, Clock, ObjectID, Operation};
    use serde_json;

    struct TestCase {
        name: &'static str,
        change_request: ChangeRequest,
        json: serde_json::Value,
    }

    #[test]
    fn do_tests() {
        let actor = ActorID("actor1".to_string());
        let birds = ObjectID::ID("birds".to_string());
        let testcases: Vec<TestCase> = vec![
            TestCase {
                name: "change",
                change_request: ChangeRequest {
                    actor_id: actor.clone(),
                    seq: 1,
                    message: None,
                    dependencies: Clock::empty().with_dependency(&actor, 1),
                    request_type: ChangeRequestType::Change(vec![Operation::MakeMap {
                        object_id: birds,
                    }]),
                },
                json: serde_json::from_str(
                    r#"
                        {
                            "actor": "actor1",
                            "seq": 1,
                            "message": null,
                            "deps": {"actor1": 1},
                            "requestType": "change",
                            "ops": [{
                                "action": "makeMap",
                                "obj": "birds"
                            }]
                        }
                        "#,
                )
                .unwrap(),
            },
            TestCase {
                name: "undo",
                change_request: ChangeRequest {
                    actor_id: actor.clone(),
                    seq: 1,
                    message: None,
                    dependencies: Clock::empty().with_dependency(&actor, 1),
                    request_type: ChangeRequestType::Undo,
                },
                json: serde_json::from_str(
                    r#"
                        {
                            "actor": "actor1",
                            "seq": 1,
                            "message": null,
                            "deps": {"actor1": 1},
                            "requestType": "undo"
                        }
                        "#,
                )
                .unwrap(),
            },
            TestCase {
                name: "redo",
                change_request: ChangeRequest {
                    actor_id: actor.clone(),
                    seq: 1,
                    message: None,
                    dependencies: Clock::empty().with_dependency(&actor, 1),
                    request_type: ChangeRequestType::Redo,
                },
                json: serde_json::from_str(
                    r#"
                        {
                            "actor": "actor1",
                            "seq": 1,
                            "message": null,
                            "deps": {"actor1": 1},
                            "requestType": "redo"
                        }
                        "#,
                )
                .unwrap(),
            },
        ];
        for ref testcase in testcases {
            let serialized = serde_json::to_value(testcase.change_request.clone())
                .expect(&std::format!("Failed to deserialize {}", testcase.name));
            assert_eq!(
                testcase.json, serialized,
                "TestCase {} did not match",
                testcase.name
            );
            let deserialized: ChangeRequest = serde_json::from_value(serialized)
                .expect(&std::format!("Failed to deserialize for {}", testcase.name));
            assert_eq!(
                testcase.change_request, deserialized,
                "TestCase {} failed the round trip",
                testcase.name
            );
        }
    }
}
