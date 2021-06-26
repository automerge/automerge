extern crate automerge_protocol as amp;
use maplit::hashmap;

#[test]
fn test_msgpack_roundtrip_change() {
    let c = amp::Change {
        operations: vec![amp::Op {
            action: amp::OpType::Set(0.into()),
            obj: amp::ObjectId::Root,
            key: amp::Key::Seq(amp::ElementId::Id(amp::OpId(0, amp::ActorId::random()))),
            insert: false,
            pred: amp::SortedVec::new(),
        }],
        actor_id: amp::ActorId::random(),
        hash: None,
        seq: 0,
        start_op: 0,
        time: 0,
        message: None,
        deps: vec![],
        extra_bytes: vec![],
    };
    let serialized = rmp_serde::to_vec_named(&c).unwrap();
    let deserialized: amp::Change = rmp_serde::from_slice(&serialized).unwrap();
    assert_eq!(c, deserialized);
}

// Update: See comment in map_type.rs for
// why this test is disabled
// This was not caught in the proptests
#[test]
fn test_msgpack_roundtrip_diff() {
    let actor = amp::ActorId::from_bytes("bd1850df21004038a8141a98473ff142".as_bytes());
    let diff = amp::RootDiff {
        props: hashmap! {
            "bird".into() => hashmap! {
                actor.op_id_at(1) => "magpie".into()
            }
        },
    };
    let serialized = rmp_serde::to_vec_named(&diff).unwrap();
    let deserialized: amp::RootDiff = rmp_serde::from_slice(&serialized).unwrap();
    assert_eq!(diff, deserialized);
}

const PATCH_JSON: &str = r#"
{
  "clock": { "8c8a54b01ce24c3a8dd9e05af04c862a": 1 },
  "deps": ["9013fe6e020884f6fc44934cfc553e4e698e8aa5a1b04512a8b230f28057c8db"],
  "diffs": {
    "objectId": "_root",
    "type": "map",
    "props": {
      "hello": {
        "1@8c8a54b01ce24c3a8dd9e05af04c862a": {
          "type": "value",
          "value": "world"
        }
      },
      "list1": {
        "2@8c8a54b01ce24c3a8dd9e05af04c862a": {
          "objectId": "2@8c8a54b01ce24c3a8dd9e05af04c862a",
          "type": "list",
          "edits": [
            {
              "action": "insert",
              "index": 0,
              "elemId": "3@8c8a54b01ce24c3a8dd9e05af04c862a",
              "opId": "3@8c8a54b01ce24c3a8dd9e05af04c862a",
              "value": { "type": "value", "value": 1, "datatype": "int" }
            },
            {
              "action": "insert",
              "index": 1,
              "elemId": "4@8c8a54b01ce24c3a8dd9e05af04c862a",
              "opId": "4@8c8a54b01ce24c3a8dd9e05af04c862a",
              "value": { "type": "value", "value": 2.2, "datatype": "float64" }
            },
            {
              "action": "insert",
              "index": 2,
              "elemId": "5@8c8a54b01ce24c3a8dd9e05af04c862a",
              "opId": "5@8c8a54b01ce24c3a8dd9e05af04c862a",
              "value": {
                "objectId": "5@8c8a54b01ce24c3a8dd9e05af04c862a",
                "type": "map",
                "props": {
                  "n": {
                    "6@8c8a54b01ce24c3a8dd9e05af04c862a": {
                      "type": "value",
                      "value": -1,
                      "datatype": "int"
                    }
                  },
                  "v": {
                    "7@8c8a54b01ce24c3a8dd9e05af04c862a": {
                      "type": "value",
                      "value": "three"
                    }
                  }
                }
              }
            }
          ]
        }
      },
      "list2": {
        "8@8c8a54b01ce24c3a8dd9e05af04c862a": {
          "objectId": "8@8c8a54b01ce24c3a8dd9e05af04c862a",
          "type": "list",
          "edits": [
            {
              "action": "multi-insert",
              "index": 0,
              "elemId": "9@8c8a54b01ce24c3a8dd9e05af04c862a",
              "datatype": "int",
              "values": [0, 1, 2, 3, 4, 5]
            }
          ]
        }
      },
      "map": {
        "15@8c8a54b01ce24c3a8dd9e05af04c862a": {
          "objectId": "15@8c8a54b01ce24c3a8dd9e05af04c862a",
          "type": "map",
          "props": {
            "submap": {
              "16@8c8a54b01ce24c3a8dd9e05af04c862a": {
                "objectId": "16@8c8a54b01ce24c3a8dd9e05af04c862a",
                "type": "map",
                "props": {
                  "value": {
                    "17@8c8a54b01ce24c3a8dd9e05af04c862a": {
                      "type": "value",
                      "value": "value"
                    }
                  }
                }
              }
            }
          }
        }
      },
      "counter": {
        "18@8c8a54b01ce24c3a8dd9e05af04c862a": {
          "type": "value",
          "value": 100,
          "datatype": "counter"
        }
      },
      "time": {
        "19@8c8a54b01ce24c3a8dd9e05af04c862a": {
          "type": "value",
          "value": 1624294015745,
          "datatype": "timestamp"
        }
      }
    }
  },
  "maxOp": 19,
  "pendingChanges": 0
}
"#;

#[test]
fn patch_roundtrip_json() {
    let patch: amp::Patch = serde_json::from_str(PATCH_JSON).unwrap();
    let new_patch_json = serde_json::to_string_pretty(&patch).unwrap();
    let new_patch: amp::Patch = serde_json::from_str(&new_patch_json).unwrap();
    assert_eq!(patch, new_patch);
}

#[test]
fn patch_roundtrip_msgpack() {
    let patch: amp::Patch = serde_json::from_str(PATCH_JSON).unwrap();
    let new_patch_mpack = rmp_serde::to_vec_named(&patch).unwrap();
    let new_patch: amp::Patch = rmp_serde::from_slice(&new_patch_mpack).unwrap();
    assert_eq!(patch, new_patch);
}
