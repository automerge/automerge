extern crate automerge_backend;
use automerge_backend::{Backend, Change, Patch};

#[test]
fn test_include_most_recent_value_for_key() {
    let change1: Change = serde_json::from_str(
        r#"
        {
           "actor": "ec28cfbc-db9e-4f32-ad24-b3c776e651b0",
           "seq": 1,
           "startOp": 1,
           "time": 0,
           "deps": {},
           "ops": [
              {
                 "action": "set",
                 "obj": "00000000-0000-0000-0000-000000000000",
                 "key": "bird",
                 "value": "magpie",
                 "pred": []
              }
           ]
        }
    "#,
    )
    .unwrap();

    let change2: Change = serde_json::from_str(
        r#"
        {
           "actor": "ec28cfbc-db9e-4f32-ad24-b3c776e651b0",
           "seq": 2,
           "startOp": 2,
           "time": 0,
           "deps": {},
           "ops": [
              {
                 "action": "set",
                 "obj": "00000000-0000-0000-0000-000000000000",
                 "key": "bird",
                 "value": "blackbird",
                 "pred": [
                    "1@ec28cfbc-db9e-4f32-ad24-b3c776e651b0"
                 ]
              }
           ]
        }
    "#,
    )
    .unwrap();

    let expected_patch: Patch = serde_json::from_str(
        r#"
        {
           "version": 0,
           "clock": {
              "ec28cfbc-db9e-4f32-ad24-b3c776e651b0": 2
           },
           "canUndo": false,
           "canRedo": false,
           "diffs": {
              "objectId": "00000000-0000-0000-0000-000000000000",
              "type": "map",
              "props": {
                 "bird": {
                    "2@ec28cfbc-db9e-4f32-ad24-b3c776e651b0": {
                       "value": "blackbird"
                    }
                 }
              }
           }
        }
    "#,
    )
    .unwrap();

    let mut backend = Backend::init();
    backend.load_changes(vec![change1, change2]).unwrap();
    let patch = backend.get_patch().unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_includes_conflicting_values_for_key() {
    let change1: Change = serde_json::from_str(
        r#"
        {
           "actor": "actor1",
           "seq": 1,
           "startOp": 1,
           "time": 0,
           "deps": {},
           "ops": [
              {
                 "action": "set",
                 "obj": "00000000-0000-0000-0000-000000000000",
                 "key": "bird",
                 "value": "magpie",
                 "pred": []
              }
           ]
        }
    "#,
    )
    .unwrap();

    let change2: Change = serde_json::from_str(
        r#"
        {
           "actor": "actor2",
           "seq": 1,
           "startOp": 1,
           "time": 0,
           "deps": {},
           "ops": [
              {
                 "action": "set",
                 "obj": "00000000-0000-0000-0000-000000000000",
                 "key": "bird",
                 "value": "blackbird",
                 "pred": []
              }
           ]
        }
    "#,
    )
    .unwrap();

    let expected_patch: Patch = serde_json::from_str(
        r#"
        {
           "version": 0,
           "clock": {
              "actor1": 1,
              "actor2": 1
           },
           "canUndo": false,
           "canRedo": false,
           "diffs": {
              "objectId": "00000000-0000-0000-0000-000000000000",
              "type": "map",
              "props": {
                 "bird": {
                    "1@actor1": {
                       "value": "magpie"
                    },
                    "1@actor2": {
                       "value": "blackbird"
                    }
                 }
              }
           }
        }
    "#,
    )
    .unwrap();

    let mut backend = Backend::init();
    backend.load_changes(vec![change1, change2]).unwrap();
    let patch = backend.get_patch().unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_handles_counter_increment_at_keys_in_a_map() {
    let change1: Change = serde_json::from_str(
        r#"
        {
           "actor": "46c92088-e448-4ae5-945d-c63bf606a4a5",
           "seq": 1,
           "startOp": 1,
           "time": 0,
           "deps": {},
           "ops": [
              {
                 "action": "set",
                 "obj": "00000000-0000-0000-0000-000000000000",
                 "key": "counter",
                 "value": 1,
                 "datatype": "counter",
                 "pred": []
              }
           ]
        }
    "#,
    )
    .unwrap();

    let change2: Change = serde_json::from_str(
        r#"
        {
           "actor": "46c92088-e448-4ae5-945d-c63bf606a4a5",
           "seq": 2,
           "startOp": 2,
           "time": 0,
           "deps": {},
           "ops": [
              {
                 "action": "inc",
                 "obj": "00000000-0000-0000-0000-000000000000",
                 "key": "counter",
                 "value": 2,
                 "pred": [
                    "1@46c92088-e448-4ae5-945d-c63bf606a4a5"
                 ]
              }
           ]
        }
    "#,
    )
    .unwrap();

    let expected_patch: Patch = serde_json::from_str(
        r#"
        {
           "version": 0,
           "clock": {
              "46c92088-e448-4ae5-945d-c63bf606a4a5": 2
           },
           "canUndo": false,
           "canRedo": false,
           "diffs": {
              "objectId": "00000000-0000-0000-0000-000000000000",
              "type": "map",
              "props": {
                 "counter": {
                    "1@46c92088-e448-4ae5-945d-c63bf606a4a5": {
                       "value": 3,
                       "datatype": "counter"
                    }
                 }
              }
           }
        }
    "#,
    )
    .unwrap();

    let mut backend = Backend::init();
    backend.load_changes(vec![change1, change2]).unwrap();
    let patch = backend.get_patch().unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_creates_nested_maps() {
    let change1: Change = serde_json::from_str(
        r#"
        {
           "actor": "06148f94-22cb-4057-9fd0-2f1975c34a51",
           "seq": 1,
           "startOp": 1,
           "time": 0,
           "deps": {},
           "ops": [
              {
                 "action": "makeMap",
                 "obj": "00000000-0000-0000-0000-000000000000",
                 "key": "birds",
                 "pred": []
              },
              {
                 "action": "set",
                 "obj": "1@06148f94-22cb-4057-9fd0-2f1975c34a51",
                 "key": "wrens",
                 "value": 3,
                 "pred": []
              }
           ]
        }
    "#,
    )
    .unwrap();

    let change2: Change = serde_json::from_str(
        r#"
        {
           "actor": "06148f94-22cb-4057-9fd0-2f1975c34a51",
           "seq": 2,
           "startOp": 3,
           "time": 0,
           "deps": {},
           "ops": [
              {
                 "action": "del",
                 "obj": "1@06148f94-22cb-4057-9fd0-2f1975c34a51",
                 "key": "wrens",
                 "pred": [
                    "2@06148f94-22cb-4057-9fd0-2f1975c34a51"
                 ]
              },
              {
                 "action": "set",
                 "obj": "1@06148f94-22cb-4057-9fd0-2f1975c34a51",
                 "key": "sparrows",
                 "value": 15,
                 "pred": []
              }
           ]
        }
    "#,
    )
    .unwrap();

    let expected_patch: Patch = serde_json::from_str(
        r#"
        {
           "version": 0,
           "clock": {
              "06148f94-22cb-4057-9fd0-2f1975c34a51": 2
           },
           "canUndo": false,
           "canRedo": false,
           "diffs": {
              "objectId": "00000000-0000-0000-0000-000000000000",
              "type": "map",
              "props": {
                 "birds": {
                    "1@06148f94-22cb-4057-9fd0-2f1975c34a51": {
                       "objectId": "1@06148f94-22cb-4057-9fd0-2f1975c34a51",
                       "type": "map",
                       "props": {
                          "sparrows": {
                             "4@06148f94-22cb-4057-9fd0-2f1975c34a51": {
                                "value": 15
                             }
                          }
                       }
                    }
                 }
              }
           }
        }
    "#,
    )
    .unwrap();

    let mut backend = Backend::init();
    backend.load_changes(vec![change1, change2]).unwrap();
    let patch = backend.get_patch().unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_create_lists() {
    let change1: Change = serde_json::from_str(
        r#"
        {
           "actor": "90bf7df6-82f7-47fa-82ac-604b35010906",
           "seq": 1,
           "startOp": 1,
           "time": 0,
           "deps": {},
           "ops": [
              {
                 "action": "makeList",
                 "obj": "00000000-0000-0000-0000-000000000000",
                 "key": "birds",
                 "pred": []
              },
              {
                 "action": "set",
                 "obj": "1@90bf7df6-82f7-47fa-82ac-604b35010906",
                 "key": "_head",
                 "insert": true,
                 "value": "chaffinch",
                 "pred": []
              }
           ]
        }
    "#,
    )
    .unwrap();

    let expected_patch: Patch = serde_json::from_str(
        r#"
        {
           "version": 0,
           "clock": {
              "90bf7df6-82f7-47fa-82ac-604b35010906": 1
           },
           "canUndo": false,
           "canRedo": false,
           "diffs": {
              "objectId": "00000000-0000-0000-0000-000000000000",
              "type": "map",
              "props": {
                 "birds": {
                    "1@90bf7df6-82f7-47fa-82ac-604b35010906": {
                       "objectId": "1@90bf7df6-82f7-47fa-82ac-604b35010906",
                       "type": "list",
                       "edits": [
                          {
                             "action": "insert",
                             "index": 0
                          }
                       ],
                       "props": {
                          "0": {
                             "2@90bf7df6-82f7-47fa-82ac-604b35010906": {
                                "value": "chaffinch"
                             }
                          }
                       }
                    }
                 }
              }
           }
        }
    "#,
    )
    .unwrap();

    let mut backend = Backend::init();
    backend.load_changes(vec![change1]).unwrap();
    let patch = backend.get_patch().unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_includes_latests_state_of_list() {
        let change1: Change = serde_json::from_str(
            r#"
            {
               "actor": "6caaa2e4-33de-42ae-9c3f-a65c9ff3f03e",
               "seq": 1,
               "startOp": 1,
               "time": 0,
               "deps": {},
               "ops": [
                  {
                     "action": "makeList",
                     "obj": "00000000-0000-0000-0000-000000000000",
                     "key": "todos",
                     "pred": []
                  },
                  {
                     "action": "makeMap",
                     "obj": "1@6caaa2e4-33de-42ae-9c3f-a65c9ff3f03e",
                     "key": "_head",
                     "insert": true,
                     "pred": []
                  },
                  {
                     "action": "set",
                     "obj": "2@6caaa2e4-33de-42ae-9c3f-a65c9ff3f03e",
                     "key": "title",
                     "value": "water plants",
                     "pred": []
                  },
                  {
                     "action": "set",
                     "obj": "2@6caaa2e4-33de-42ae-9c3f-a65c9ff3f03e",
                     "key": "done",
                     "value": false,
                     "pred": []
                  }
               ]
            }
        "#,
        )
        .unwrap();

        let expected_patch: Patch = serde_json::from_str(
            r#"
            {
               "version": 0,
               "clock": {
                  "6caaa2e4-33de-42ae-9c3f-a65c9ff3f03e": 1
               },
               "canUndo": false,
               "canRedo": false,
               "diffs": {
                  "objectId": "00000000-0000-0000-0000-000000000000",
                  "type": "map",
                  "props": {
                     "todos": {
                        "1@6caaa2e4-33de-42ae-9c3f-a65c9ff3f03e": {
                           "objectId": "1@6caaa2e4-33de-42ae-9c3f-a65c9ff3f03e",
                           "type": "list",
                           "edits": [
                              {
                                 "action": "insert",
                                 "index": 0
                              }
                           ],
                           "props": {
                              "0": {
                                 "2@6caaa2e4-33de-42ae-9c3f-a65c9ff3f03e": {
                                    "objectId": "2@6caaa2e4-33de-42ae-9c3f-a65c9ff3f03e",
                                    "type": "map",
                                    "props": {
                                       "title": {
                                          "3@6caaa2e4-33de-42ae-9c3f-a65c9ff3f03e": {
                                             "value": "water plants"
                                          }
                                       },
                                       "done": {
                                          "4@6caaa2e4-33de-42ae-9c3f-a65c9ff3f03e": {
                                             "value": false
                                          }
                                       }
                                    }
                                 }
                              }
                           }
                        }
                     }
                  }
               }
            }
        "#,
        )
        .unwrap();

        let mut backend = Backend::init();
        backend.load_changes(vec![change1]).unwrap();
        let patch = backend.get_patch().unwrap();
        assert_eq!(patch, expected_patch)
    }

    #[test]
    fn test_includes_date_objects_at_root() {
        let change1: Change = serde_json::from_str(
            r#"
            {
               "actor": "90f5dd5d-4f52-4e95-ad59-29e08d1194f1",
               "seq": 1,
               "startOp": 1,
               "time": 0,
               "deps": {},
               "ops": [
                  {
                     "action": "set",
                     "obj": "00000000-0000-0000-0000-000000000000",
                     "key": "now",
                     "value": 1586541033457,
                     "datatype": "timestamp",
                     "pred": []
                  }
               ]
            }
        "#,
        )
        .unwrap();

        let expected_patch: Patch = serde_json::from_str(
            r#"
            {
               "version": 0,
               "clock": {
                  "90f5dd5d-4f52-4e95-ad59-29e08d1194f1": 1
               },
               "canUndo": false,
               "canRedo": false,
               "diffs": {
                  "objectId": "00000000-0000-0000-0000-000000000000",
                  "type": "map",
                  "props": {
                     "now": {
                        "1@90f5dd5d-4f52-4e95-ad59-29e08d1194f1": {
                           "value": 1586541033457,
                           "datatype": "timestamp"
                        }
                     }
                  }
               }
            }
        "#,
        )
        .unwrap();

        let mut backend = Backend::init();
        backend.load_changes(vec![change1]).unwrap();
        let patch = backend.get_patch().unwrap();
        assert_eq!(patch, expected_patch)
    }

    #[test]
    fn test_includes_date_objects_in_a_list() {
        let change1: Change = serde_json::from_str(
            r#"
            {
               "actor": "08b050f9-76a2-4934-9021-a2e63d99c8e8",
               "seq": 1,
               "startOp": 1,
               "time": 0,
               "deps": {},
               "ops": [
                  {
                     "action": "makeList",
                     "obj": "00000000-0000-0000-0000-000000000000",
                     "key": "list",
                     "pred": []
                  },
                  {
                     "action": "set",
                     "obj": "1@08b050f9-76a2-4934-9021-a2e63d99c8e8",
                     "key": "_head",
                     "insert": true,
                     "value": 1586541089595,
                     "datatype": "timestamp",
                     "pred": []
                  }
               ]
            }
        "#,
        )
        .unwrap();

        let expected_patch: Patch = serde_json::from_str(
            r#"
            {
               "version": 0,
               "clock": {
                  "08b050f9-76a2-4934-9021-a2e63d99c8e8": 1
               },
               "canUndo": false,
               "canRedo": false,
               "diffs": {
                  "objectId": "00000000-0000-0000-0000-000000000000",
                  "type": "map",
                  "props": {
                     "list": {
                        "1@08b050f9-76a2-4934-9021-a2e63d99c8e8": {
                           "objectId": "1@08b050f9-76a2-4934-9021-a2e63d99c8e8",
                           "type": "list",
                           "edits": [
                              {
                                 "action": "insert",
                                 "index": 0
                              }
                           ],
                           "props": {
                              "0": {
                                 "2@08b050f9-76a2-4934-9021-a2e63d99c8e8": {
                                    "value": 1586541089595,
                                    "datatype": "timestamp"
                                 }
                              }
                           }
                        }
                     }
                  }
               }
            }
        "#,
        )
        .unwrap();

        let mut backend = Backend::init();
        backend.load_changes(vec![change1]).unwrap();
        let patch = backend.get_patch().unwrap();
        assert_eq!(patch, expected_patch)
}
