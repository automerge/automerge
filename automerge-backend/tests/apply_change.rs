extern crate automerge_backend;
use automerge_backend::{Backend, Change, Patch};

#[test]
fn test_incremental_diffs_in_a_map() {
    let change: Change = serde_json::from_str(
        r#"{
        "actor": "7b7723af-d9e6-4803-97a4-d467b7693156", 
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
    }"#,
    )
    .unwrap();
    let mut backend = Backend::init();
    let patch = backend.apply_changes(vec![change]).unwrap();
    let expected_patch: Patch = serde_json::from_str(
        r#"{
        "version": 1,
        "clock": {
            "7b7723af-d9e6-4803-97a4-d467b7693156": 1
        },
        "canUndo": false,
        "canRedo": false,
        "diffs": {
            "objectId": "00000000-0000-0000-0000-000000000000", 
            "type": "map", 
            "props": {
                "bird": {
                    "1@7b7723af-d9e6-4803-97a4-d467b7693156": {"value": "magpie"}
                }
            }
        }
    }"#,
    )
    .unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_increment_key_in_map() {
    let change1: Change = serde_json::from_str(
        r#"{
        "actor": "cdee6963-c166-4645-920b-e8b41a933c2b", 
        "seq": 1, 
        "startOp": 1,
        "time": 0, 
        "deps": {},
        "ops": [{
          "action": "set",
          "obj": "00000000-0000-0000-0000-000000000000",
          "key": "counter",
          "value": 1,
          "datatype": "counter",
          "pred": []
        }]
    }"#,
    )
    .unwrap();
    let change2: Change = serde_json::from_str(
        r#"{
        "actor": "cdee6963-c166-4645-920b-e8b41a933c2b",
        "seq": 2,
        "startOp": 2, 
        "time": 0, 
        "deps": {}, 
        "ops": [{
          "action": "inc",
          "obj": "00000000-0000-0000-0000-000000000000",
          "key": "counter",
          "value": 2,
          "pred": ["1@cdee6963-c166-4645-920b-e8b41a933c2b"]
        }]
    }"#,
    )
    .unwrap();
    let expected_patch: Patch = serde_json::from_str(r#"{
        "version": 2,
        "clock": {"cdee6963-c166-4645-920b-e8b41a933c2b": 2}, 
        "canUndo": false,
        "canRedo": false,
        "diffs": {
            "objectId": "00000000-0000-0000-0000-000000000000",
            "type": "map",
            "props": {
                "counter": {"1@cdee6963-c166-4645-920b-e8b41a933c2b": {"value": 3, "datatype": "counter"}}
            }
        }
    }"#).unwrap();
    let mut backend = Backend::init();
    backend.apply_changes(vec![change1]).unwrap();
    let patch = backend.apply_changes(vec![change2]).unwrap();
    assert_eq!(patch, expected_patch);
}

#[test]
fn test_conflict_on_assignment_to_same_map_key() {
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
            "startOp": 2,
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
            "version": 2,
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
                        "2@actor2": {
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
    backend.apply_changes(vec![change1]).unwrap();
    let patch = backend.apply_changes(vec![change2]).unwrap();
    assert_eq!(patch, expected_patch);
}

#[test]
fn delete_key_from_map() {
    let change1: Change = serde_json::from_str(
        r#"
        {
           "actor": "cd86c07f-1093-48f4-94af-5be30fdc4c71",
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
           "actor": "cd86c07f-1093-48f4-94af-5be30fdc4c71",
           "seq": 2,
           "startOp": 2,
           "time": 0,
           "deps": {},
           "ops": [
              {
                 "action": "del",
                 "obj": "00000000-0000-0000-0000-000000000000",
                 "key": "bird",
                 "pred": [
                    "1@cd86c07f-1093-48f4-94af-5be30fdc4c71"
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
           "version": 2,
           "clock": {
              "cd86c07f-1093-48f4-94af-5be30fdc4c71": 2
           },
           "canUndo": false,
           "canRedo": false,
           "diffs": {
              "objectId": "00000000-0000-0000-0000-000000000000",
              "type": "map",
              "props": {
                 "bird": {}
              }
           }
        }
    "#,
    )
    .unwrap();

    let mut backend = Backend::init();
    backend.apply_changes(vec![change1]).unwrap();
    let patch = backend.apply_changes(vec![change2]).unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn create_nested_maps() {
    let change: Change = serde_json::from_str(
        r#"
        {
           "actor": "d6226fcd-5520-4b82-b396-f2473da3e26f",
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
                 "obj": "1@d6226fcd-5520-4b82-b396-f2473da3e26f",
                 "key": "wrens",
                 "value": 3,
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
           "version": 1,
           "clock": {
              "d6226fcd-5520-4b82-b396-f2473da3e26f": 1
           },
           "canUndo": false,
           "canRedo": false,
           "diffs": {
              "objectId": "00000000-0000-0000-0000-000000000000",
              "type": "map",
              "props": {
                 "birds": {
                    "1@d6226fcd-5520-4b82-b396-f2473da3e26f": {
                       "objectId": "1@d6226fcd-5520-4b82-b396-f2473da3e26f",
                       "type": "map",
                       "props": {
                          "wrens": {
                             "2@d6226fcd-5520-4b82-b396-f2473da3e26f": {
                                "value": 3
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
    let patch = backend.apply_changes(vec![change]).unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_assign_to_nested_keys_in_map() {
    let change1: Change = serde_json::from_str(
        r#"
        {
           "actor": "3c39c994-0390-4277-8f47-79a01a59a917",
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
                 "obj": "1@3c39c994-0390-4277-8f47-79a01a59a917",
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
           "actor": "3c39c994-0390-4277-8f47-79a01a59a917",
           "seq": 2,
           "startOp": 3,
           "time": 0,
           "deps": {},
           "ops": [
              {
                 "action": "set",
                 "obj": "1@3c39c994-0390-4277-8f47-79a01a59a917",
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
           "version": 2,
           "clock": {
              "3c39c994-0390-4277-8f47-79a01a59a917": 2
           },
           "canUndo": false,
           "canRedo": false,
           "diffs": {
              "objectId": "00000000-0000-0000-0000-000000000000",
              "type": "map",
              "props": {
                 "birds": {
                    "1@3c39c994-0390-4277-8f47-79a01a59a917": {
                       "objectId": "1@3c39c994-0390-4277-8f47-79a01a59a917",
                       "type": "map",
                       "props": {
                          "sparrows": {
                             "3@3c39c994-0390-4277-8f47-79a01a59a917": {
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
    backend.apply_changes(vec![change1]).unwrap();
    let patch = backend.apply_changes(vec![change2]).unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_create_lists() {
    let change: Change = serde_json::from_str(
        r#"
        {
           "actor": "f82cb62d-abe6-4372-ab87-466b77792010",
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
                 "obj": "1@f82cb62d-abe6-4372-ab87-466b77792010",
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
           "version": 1,
           "clock": {
              "f82cb62d-abe6-4372-ab87-466b77792010": 1
           },
           "canUndo": false,
           "canRedo": false,
           "diffs": {
              "objectId": "00000000-0000-0000-0000-000000000000",
              "type": "map",
              "props": {
                 "birds": {
                    "1@f82cb62d-abe6-4372-ab87-466b77792010": {
                       "objectId": "1@f82cb62d-abe6-4372-ab87-466b77792010",
                       "type": "list",
                       "edits": [
                          {
                             "action": "insert",
                             "index": 0
                          }
                       ],
                       "props": {
                          "0": {
                             "2@f82cb62d-abe6-4372-ab87-466b77792010": {
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
    let patch = backend.apply_changes(vec![change]).unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_apply_updates_inside_lists() {
    let change1: Change = serde_json::from_str(
        r#"
        {
           "actor": "4ee4a0d0-33b8-41c4-b26d-73d70a879547",
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
                 "obj": "1@4ee4a0d0-33b8-41c4-b26d-73d70a879547",
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

    let change2: Change = serde_json::from_str(
        r#"
        {
           "actor": "4ee4a0d0-33b8-41c4-b26d-73d70a879547",
           "seq": 2,
           "startOp": 3,
           "time": 0,
           "deps": {},
           "ops": [
              {
                 "action": "set",
                 "obj": "1@4ee4a0d0-33b8-41c4-b26d-73d70a879547",
                 "key": "2@4ee4a0d0-33b8-41c4-b26d-73d70a879547",
                 "value": "greenfinch",
                 "pred": [
                    "2@4ee4a0d0-33b8-41c4-b26d-73d70a879547"
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
           "version": 2,
           "clock": {
              "4ee4a0d0-33b8-41c4-b26d-73d70a879547": 2
           },
           "canUndo": false,
           "canRedo": false,
           "diffs": {
              "objectId": "00000000-0000-0000-0000-000000000000",
              "type": "map",
              "props": {
                 "birds": {
                    "1@4ee4a0d0-33b8-41c4-b26d-73d70a879547": {
                       "objectId": "1@4ee4a0d0-33b8-41c4-b26d-73d70a879547",
                       "type": "list",
                       "edits": [],
                       "props": {
                          "0": {
                             "3@4ee4a0d0-33b8-41c4-b26d-73d70a879547": {
                                "value": "greenfinch"
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
    backend.apply_changes(vec![change1]).unwrap();
    let patch = backend.apply_changes(vec![change2]).unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_delete_list_elements() {
    let change1: Change = serde_json::from_str(
        r#"
        {
           "actor": "8a3d4716-fdca-49f4-aa58-35901f2034c7",
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
                 "obj": "1@8a3d4716-fdca-49f4-aa58-35901f2034c7",
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

    let change2: Change = serde_json::from_str(
        r#"
        {
           "actor": "8a3d4716-fdca-49f4-aa58-35901f2034c7",
           "seq": 2,
           "startOp": 3,
           "time": 0,
           "deps": {},
           "ops": [
              {
                 "action": "del",
                 "obj": "1@8a3d4716-fdca-49f4-aa58-35901f2034c7",
                 "key": "2@8a3d4716-fdca-49f4-aa58-35901f2034c7",
                 "pred": [
                    "2@8a3d4716-fdca-49f4-aa58-35901f2034c7"
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
           "version": 2,
           "clock": {
              "8a3d4716-fdca-49f4-aa58-35901f2034c7": 2
           },
           "canUndo": false,
           "canRedo": false,
           "diffs": {
              "objectId": "00000000-0000-0000-0000-000000000000",
              "type": "map",
              "props": {
                 "birds": {
                    "1@8a3d4716-fdca-49f4-aa58-35901f2034c7": {
                       "objectId": "1@8a3d4716-fdca-49f4-aa58-35901f2034c7",
                       "type": "list",
                       "props": {},
                       "edits": [
                          {
                             "action": "remove",
                             "index": 0
                          }
                       ]
                    }
                 }
              }
           }
        }
    "#,
    )
    .unwrap();

    let mut backend = Backend::init();
    backend.apply_changes(vec![change1]).unwrap();
    let patch = backend.apply_changes(vec![change2]).unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_handle_list_element_insertion_and_deletion_in_same_change() {
    let change1: Change = serde_json::from_str(
        r#"
        {
           "actor": "ca95bc75-9404-486b-be7b-9dd2be779fa8",
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
              }
           ]
        }
    "#,
    )
    .unwrap();

    let change2: Change = serde_json::from_str(
        r#"
        {
           "actor": "ca95bc75-9404-486b-be7b-9dd2be779fa8",
           "seq": 2,
           "startOp": 2,
           "time": 0,
           "deps": {},
           "ops": [
              {
                 "action": "set",
                 "obj": "1@ca95bc75-9404-486b-be7b-9dd2be779fa8",
                 "key": "_head",
                 "insert": true,
                 "value": "chaffinch",
                 "pred": []
              },
              {
                 "action": "del",
                 "obj": "1@ca95bc75-9404-486b-be7b-9dd2be779fa8",
                 "key": "2@ca95bc75-9404-486b-be7b-9dd2be779fa8",
                 "pred": [
                    "2@ca95bc75-9404-486b-be7b-9dd2be779fa8"
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
           "version": 2,
           "clock": {
              "ca95bc75-9404-486b-be7b-9dd2be779fa8": 2
           },
           "canUndo": false,
           "canRedo": false,
           "diffs": {
              "objectId": "00000000-0000-0000-0000-000000000000",
              "type": "map",
              "props": {
                 "birds": {
                    "1@ca95bc75-9404-486b-be7b-9dd2be779fa8": {
                       "objectId": "1@ca95bc75-9404-486b-be7b-9dd2be779fa8",
                       "type": "list",
                       "edits": [
                          {
                             "action": "insert",
                             "index": 0
                          },
                          {
                             "action": "remove",
                             "index": 0
                          }
                       ],
                       "props": {}
                    }
                 }
              }
           }
        }
    "#,
    )
    .unwrap();

    let mut backend = Backend::init();
    backend.apply_changes(vec![change1]).unwrap();
    let patch = backend.apply_changes(vec![change2]).unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_handle_changes_within_conflicted_objects() {
    let change1: Change = serde_json::from_str(
        r#"
        {
           "actor": "9f175175-23e5-4ee8-88e9-cd51dfd7a572",
           "seq": 1,
           "startOp": 1,
           "time": 0,
           "deps": {},
           "ops": [
              {
                 "action": "makeList",
                 "obj": "00000000-0000-0000-0000-000000000000",
                 "key": "conflict",
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
           "actor": "83768a19-a138-42be-b6dd-e8c68a662fad",
           "seq": 1,
           "startOp": 1,
           "time": 0,
           "deps": {},
           "ops": [
              {
                 "action": "makeMap",
                 "obj": "00000000-0000-0000-0000-000000000000",
                 "key": "conflict",
                 "pred": []
              }
           ]
        }
    "#,
    )
    .unwrap();

    let change3: Change = serde_json::from_str(
        r#"
        {
            "actor": "83768a19-a138-42be-b6dd-e8c68a662fad",
            "seq": 2,
            "startOp": 2,
            "time": 0,
            "deps": {},
            "ops": [
                {
                    "action": "set",
                    "obj": "1@83768a19-a138-42be-b6dd-e8c68a662fad",
                    "key": "sparrows",
                    "value": 12,
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
           "version": 3,
           "clock": {
              "9f175175-23e5-4ee8-88e9-cd51dfd7a572": 1,
              "83768a19-a138-42be-b6dd-e8c68a662fad": 2
           },
           "canUndo": false,
           "canRedo": false,
           "diffs": {
              "objectId": "00000000-0000-0000-0000-000000000000",
              "type": "map",
              "props": {
                 "conflict": {
                    "1@9f175175-23e5-4ee8-88e9-cd51dfd7a572": {
                       "objectId": "1@9f175175-23e5-4ee8-88e9-cd51dfd7a572",
                       "type": "list"
                    },
                    "1@83768a19-a138-42be-b6dd-e8c68a662fad": {
                       "objectId": "1@83768a19-a138-42be-b6dd-e8c68a662fad",
                       "type": "map",
                       "props": {
                          "sparrows": {
                             "2@83768a19-a138-42be-b6dd-e8c68a662fad": {
                                "value": 12
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
    backend.apply_changes(vec![change1]).unwrap();
    backend.apply_changes(vec![change2]).unwrap();
    let patch = backend.apply_changes(vec![change3]).unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_support_date_objects_at_root() {
    let change: Change = serde_json::from_str(
        r#"
        {
           "actor": "955afa3b-bcc1-40b3-b4ba-c8836479d650",
           "seq": 1,
           "startOp": 1,
           "time": 0,
           "deps": {},
           "ops": [
              {
                 "action": "set",
                 "obj": "00000000-0000-0000-0000-000000000000",
                 "key": "now",
                 "value": 1586528122277,
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
           "version": 1,
           "clock": {
              "955afa3b-bcc1-40b3-b4ba-c8836479d650": 1
           },
           "canUndo": false,
           "canRedo": false,
           "diffs": {
              "objectId": "00000000-0000-0000-0000-000000000000",
              "type": "map",
              "props": {
                 "now": {
                    "1@955afa3b-bcc1-40b3-b4ba-c8836479d650": {
                       "value": 1586528122277,
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
    let patch = backend.apply_changes(vec![change]).unwrap();
    assert_eq!(patch, expected_patch)
}

#[test]
fn test_support_date_objects_in_a_list() {
    let change: Change = serde_json::from_str(
        r#"
        {
           "actor": "27d467ec-b1a6-40fb-9bed-448ce7cf6a44",
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
                 "obj": "1@27d467ec-b1a6-40fb-9bed-448ce7cf6a44",
                 "key": "_head",
                 "insert": true,
                 "value": 1586528191421,
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
           "version": 1,
           "clock": {
              "27d467ec-b1a6-40fb-9bed-448ce7cf6a44": 1
           },
           "canUndo": false,
           "canRedo": false,
           "diffs": {
              "objectId": "00000000-0000-0000-0000-000000000000",
              "type": "map",
              "props": {
                 "list": {
                    "1@27d467ec-b1a6-40fb-9bed-448ce7cf6a44": {
                       "objectId": "1@27d467ec-b1a6-40fb-9bed-448ce7cf6a44",
                       "type": "list",
                       "edits": [
                          {
                             "action": "insert",
                             "index": 0
                          }
                       ],
                       "props": {
                          "0": {
                             "2@27d467ec-b1a6-40fb-9bed-448ce7cf6a44": {
                                "value": 1586528191421,
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
    let patch = backend.apply_changes(vec![change]).unwrap();
    assert_eq!(patch, expected_patch)
}
