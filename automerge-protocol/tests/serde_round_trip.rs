extern crate automerge_protocol as amp;
use std::num::NonZeroU64;

use maplit::hashmap;

// This was not caught in the proptests
#[test]
fn test_msgpack_roundtrip_diff() {
    let actor = amp::ActorId::from("bd1850df21004038a8141a98473ff142".as_bytes());
    let diff = amp::RootDiff {
        props: hashmap! {
            "bird".into() => hashmap! {
                actor.op_id_at(NonZeroU64::new(1).unwrap()) => "magpie".into()
            }
        },
    };
    let serialized = rmp_serde::to_vec_named(&diff).unwrap();
    let deserialized: amp::RootDiff = rmp_serde::from_slice(&serialized).unwrap();
    assert_eq!(diff, deserialized);
}

#[test]
fn patch_roundtrip() {
    let patch_json = r#"{
  "clock": {
    "7b7723afd9e6480397a4d467b7693156": 1
  },
  "deps": [
    "822845b4bac583c5fc67fb60937cfb814cd79d85e8dfdbdafc75424ec573d898"
  ],
  "maxOp": 4,
  "pendingChanges": 0,
  "diffs": {
    "objectId": "_root",
    "type": "map",
    "props": {
      "todos": {
        "1@7b7723afd9e6480397a4d467b7693156": {
          "objectId": "1@7b7723afd9e6480397a4d467b7693156",
          "type": "list",
          "edits": [
            {
              "action": "multi-insert",
              "index": 0,
              "elemId": "2@7b7723afd9e6480397a4d467b7693156",
              "datatype": "int",
              "values": [
                1,
                2,
                3
              ]
            }
          ]
        }
      }
    }
  }
}"#;
    let patch: amp::Patch = serde_json::from_str(patch_json).unwrap();
    let new_patch_json = serde_json::to_string_pretty(&patch).unwrap();
    let new_patch: amp::Patch = serde_json::from_str(&new_patch_json).unwrap();
    assert_eq!(patch, new_patch);
}
