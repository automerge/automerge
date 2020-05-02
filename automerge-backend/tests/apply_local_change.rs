extern crate automerge_backend;
use automerge_backend::{ActorID, Backend, Change, ChangeRequest, Clock, Patch};
use std::str::FromStr;

#[test]
fn test_apply_local_change() {
    let change_request: ChangeRequest = serde_json::from_str(
        r#"
        {
           "requestType": "change",
           "actor": "eb738e04-ef88-48ce-8b77-309b6c7f7e39",
           "seq": 1,
           "version": 0,
           "ops": [
              {
                 "action": "set",
                 "obj": "00000000-0000-0000-0000-000000000000",
                 "key": "bird",
                 "value": "magpie"
              }
           ],
           "deps": {}
        }
    "#,
    )
    .unwrap();

    let expected_patch: Patch = serde_json::from_str(
        r#"
        {
           "actor": "eb738e04-ef88-48ce-8b77-309b6c7f7e39",
           "seq": 1,
           "version": 1,
           "clock": {
              "eb738e04-ef88-48ce-8b77-309b6c7f7e39": 1
           },
           "canUndo": true,
           "canRedo": false,
           "diffs": {
              "objectId": "00000000-0000-0000-0000-000000000000",
              "type": "map",
              "props": {
                 "bird": {
                    "1@eb738e04-ef88-48ce-8b77-309b6c7f7e39": {
                       "value": "magpie"
                    }
                 }
              }
           }
        }
    "#,
    )
    .unwrap();

    let mut backend = Backend::init();
    let patch = backend.apply_local_change(change_request).unwrap();
    assert_eq!(patch, expected_patch);

    let mut expected_change: Change = serde_json::from_str(
        r#"
        {
            "actor": "eb738e04-ef88-48ce-8b77-309b6c7f7e39",
            "seq": 1,
            "startOp": 1,
            "time": 1586528629051,
            "message": null,
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
    let backend2 = Backend::init();
    let changes = backend.get_missing_changes(&backend2.clock); //.unwrap();
                                                                //let changes = backend2.get_missing_changes(&backend.clock);//.unwrap();
    let change = changes.get(0).unwrap();
    expected_change.time = change.time;
    assert_eq!(change, &&expected_change);
}

#[test]
fn test_error_on_duplicate_requests() {
    let change_request1: ChangeRequest = serde_json::from_str(
        r#"
        {
            "requestType": "change",
            "actor": "37704788-917a-499c-b020-6fa8519ac4d9",
            "seq": 1,
            "version": 0,
            "ops": [
                {
                    "action": "set",
                    "obj": "00000000-0000-0000-0000-000000000000",
                    "key": "bird",
                    "value": "magpie"
                }
            ]
        }
    "#,
    )
    .unwrap();
    let change_request2: ChangeRequest = serde_json::from_str(
        r#"
        {
            "requestType": "change",
            "actor": "37704788-917a-499c-b020-6fa8519ac4d9",
            "seq": 2,
            "version": 0,
            "ops": [
                {
                    "action": "set",
                    "obj": "00000000-0000-0000-0000-000000000000",
                    "key": "bird",
                    "value": "jay"
                }
            ]
        }
    "#,
    )
    .unwrap();
    let mut backend = Backend::init();
    backend.apply_local_change(change_request1.clone()).unwrap();
    backend.apply_local_change(change_request2.clone()).unwrap();
    assert!(backend.apply_local_change(change_request1).is_err());
    assert!(backend.apply_local_change(change_request2).is_err());
}

#[test]
fn test_handle_concurrent_frontend_and_backend_changes() {
    let local1: ChangeRequest = serde_json::from_str(
        r#"
        {
            "requestType": "change",
            "actor": "cb55260e-9d7e-4578-86a4-fc73fd949202",
            "seq": 1,
            "version": 0,
            "ops": [
                {
                    "action": "set",
                    "obj": "00000000-0000-0000-0000-000000000000",
                    "key": "bird",
                    "value": "magpie"
                }
            ],
            "deps": {}
        }
    "#,
    )
    .unwrap();
    let local2: ChangeRequest = serde_json::from_str(
        r#"
        {
            "requestType": "change",
            "actor": "cb55260e-9d7e-4578-86a4-fc73fd949202",
            "seq": 2,
            "version": 0,
            "ops": [
                {
                    "action": "set",
                    "obj": "00000000-0000-0000-0000-000000000000",
                    "key": "bird",
                    "value": "jay"
                }
            ],
            "deps": {}
        }
    "#,
    )
    .unwrap();
    let remote1: Change = serde_json::from_str(
        r#"
        {
            "actor": "6d48a013-1864-4eed-9045-5d2cb68ac657",
            "seq": 1,
            "startOp": 1,
            "time": 0,
            "deps": {},
            "ops": [
                {
                    "action": "set",
                    "obj": "00000000-0000-0000-0000-000000000000",
                    "key": "fish",
                    "value": "goldfish",
                    "pred": []
                }
            ]
        }
    "#,
    )
    .unwrap();
    let mut expected_change1: Change = serde_json::from_str(
        r#"
        {
            "actor": "cb55260e-9d7e-4578-86a4-fc73fd949202",
            "seq": 1,
            "startOp": 1,
            "time": 1586530445105,
            "message": null,
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
    let mut expected_change2: Change = serde_json::from_str(
        r#"
        {
            "actor": "6d48a013-1864-4eed-9045-5d2cb68ac657",
            "seq": 1,
            "startOp": 1,
            "time": 0,
            "message": null,
            "deps": {},
            "ops": [
                {
                    "action": "set",
                    "obj": "00000000-0000-0000-0000-000000000000",
                    "key": "fish",
                    "value": "goldfish",
                    "pred": []
                }
            ]
        }
    "#,
    )
    .unwrap();
    let mut expected_change3: Change = serde_json::from_str(
        r#"
        {
            "actor": "cb55260e-9d7e-4578-86a4-fc73fd949202",
            "seq": 2,
            "startOp": 2,
            "time": 1586530445107,
            "message": null,
            "deps": {},
            "ops": [
                {
                    "action": "set",
                    "obj": "00000000-0000-0000-0000-000000000000",
                    "key": "bird",
                    "value": "jay",
                    "pred": [
                        "1@cb55260e-9d7e-4578-86a4-fc73fd949202"
                    ]
                }
            ]
        }
    "#,
    )
    .unwrap();
    let mut backend = Backend::init();
    backend.apply_local_change(local1).unwrap();
    let backend_after_first = backend.clone();
    let changes1 = backend_after_first.get_missing_changes(&Clock::empty());
    let change01 = changes1.get(0).unwrap();

    backend.apply_changes(vec![remote1]).unwrap();
    let backend_after_second = backend.clone();
    let changes2 = backend_after_second.get_missing_changes(&Clock::empty().with(
        &ActorID::from_str("cb55260e-9d7e-4578-86a4-fc73fd949202").unwrap(),
        1,
    ));
    let change12 = changes2.get(0).unwrap();

    backend.apply_local_change(local2).unwrap();
    let changes3 = backend.get_missing_changes(
        &Clock::empty()
            .with(
                &ActorID::from_str("cb55260e-9d7e-4578-86a4-fc73fd949202").unwrap(),
                1,
            )
            .with(
                &ActorID::from_str("6d48a013-1864-4eed-9045-5d2cb68ac657").unwrap(),
                1,
            ),
    );
    let change23 = changes3.get(0).unwrap();
    expected_change1.time = change01.time;
    expected_change2.time = change12.time;
    expected_change3.time = change23.time;

    assert_eq!(change01, &&expected_change1);
    assert_eq!(change12, &&expected_change2);
    assert_eq!(change23, &&expected_change3);
}

#[test]
fn test_transform_list_indexes_into_element_ids() {
    let actor1 = ActorID::from_str("8f389df8-fecb-4ddc-9891-02321af3578e").unwrap();
    let actor2 = ActorID::from_str("9ba21574-dc44-411b-8ce3-7bc6037a9687").unwrap();
    let remote1: Change = serde_json::from_str(
        r#"
        {
            "actor": "9ba21574-dc44-411b-8ce3-7bc6037a9687",
            "seq": 1,
            "startOp": 1,
            "time": 0,
            "deps": {},
            "ops": [
                {
                    "obj": "00000000-0000-0000-0000-000000000000",
                    "action": "makeList",
                    "key": "birds",
                    "pred": []
                }
            ]
        }
    "#,
    )
    .unwrap();
    let remote2: Change = serde_json::from_str(
        r#"
        {
            "actor": "9ba21574-dc44-411b-8ce3-7bc6037a9687",
            "seq": 2,
            "startOp": 2,
            "time": 0,
            "deps": {},
            "ops": [
                {
                    "obj": "1@9ba21574-dc44-411b-8ce3-7bc6037a9687",
                    "action": "set",
                    "key": "_head",
                    "insert": true,
                    "value": "magpie",
                    "pred": []
                }
            ]
        }
    "#,
    )
    .unwrap();
    let local1: ChangeRequest = serde_json::from_str(
        r#"
        {
            "requestType": "change",
            "actor": "8f389df8-fecb-4ddc-9891-02321af3578e",
            "seq": 1,
            "version": 1,
            "ops": [
                {
                    "obj": "1@9ba21574-dc44-411b-8ce3-7bc6037a9687",
                    "action": "set",
                    "key": 0,
                    "insert": true,
                    "value": "goldfinch"
                }
            ]
        }
    "#,
    )
    .unwrap();
    let local2: ChangeRequest = serde_json::from_str(
        r#"
        {
            "requestType": "change",
            "actor": "8f389df8-fecb-4ddc-9891-02321af3578e",
            "seq": 2,
            "version": 1,
            "ops": [
                {
                    "obj": "1@9ba21574-dc44-411b-8ce3-7bc6037a9687",
                    "action": "set",
                    "key": 1,
                    "insert": true,
                    "value": "wagtail"
                }
            ]
        }
    "#,
    )
    .unwrap();
    let local3: ChangeRequest = serde_json::from_str(
        r#"
        {
            "requestType": "change",
            "actor": "8f389df8-fecb-4ddc-9891-02321af3578e",
            "seq": 3,
            "version": 4,
            "ops": [
                {
                    "obj": "1@9ba21574-dc44-411b-8ce3-7bc6037a9687",
                    "action": "set",
                    "key": 0,
                    "value": "Magpie"
                },
                {
                    "obj": "1@9ba21574-dc44-411b-8ce3-7bc6037a9687",
                    "action": "set",
                    "key": 1,
                    "value": "Goldfinch"
                }
            ]
        }
    "#,
    )
    .unwrap();
    let mut expected_change1: Change = serde_json::from_str(
        r#"
        {
            "actor": "8f389df8-fecb-4ddc-9891-02321af3578e",
            "seq": 1,
            "startOp": 2,
            "time": 1586533839882,
            "message": null,
            "deps": {
                "9ba21574-dc44-411b-8ce3-7bc6037a9687": 1
            },
            "ops": [
                {
                    "obj": "1@9ba21574-dc44-411b-8ce3-7bc6037a9687",
                    "action": "set",
                    "key": "_head",
                    "insert": true,
                    "value": "goldfinch",
                    "pred": []
                }
            ]
        }
    "#,
    )
    .unwrap();
    let mut expected_change2: Change = serde_json::from_str(
        r#"
        {
            "actor": "8f389df8-fecb-4ddc-9891-02321af3578e",
            "seq": 2,
            "startOp": 3,
            "time": 1586533839884,
            "message": null,
            "deps": {},
            "ops": [
                {
                    "obj": "1@9ba21574-dc44-411b-8ce3-7bc6037a9687",
                    "action": "set",
                    "key": "2@8f389df8-fecb-4ddc-9891-02321af3578e",
                    "insert": true,
                    "value": "wagtail",
                    "pred": []
                }
            ]
        }
    "#,
    )
    .unwrap();
    let mut expected_change3: Change = serde_json::from_str(
        r#"
        {
            "actor": "8f389df8-fecb-4ddc-9891-02321af3578e",
            "seq": 3,
            "startOp": 4,
            "time": 1586533839887,
            "message": null,
            "deps": {
                "9ba21574-dc44-411b-8ce3-7bc6037a9687": 2
            },
            "ops": [
                {
                    "obj": "1@9ba21574-dc44-411b-8ce3-7bc6037a9687",
                    "action": "set",
                    "key": "2@9ba21574-dc44-411b-8ce3-7bc6037a9687",
                    "value": "Magpie",
                    "pred": [
                        "2@9ba21574-dc44-411b-8ce3-7bc6037a9687"
                    ]
                },
                {
                    "obj": "1@9ba21574-dc44-411b-8ce3-7bc6037a9687",
                    "action": "set",
                    "key": "2@8f389df8-fecb-4ddc-9891-02321af3578e",
                    "value": "Goldfinch",
                    "pred": [
                        "2@8f389df8-fecb-4ddc-9891-02321af3578e"
                    ]
                }
            ]
        }
    "#,
    )
    .unwrap();

    let mut backend = Backend::init();
    backend.apply_changes(vec![remote1]).unwrap();
    backend.apply_local_change(local1).unwrap();
    let backend_after_first = backend.clone();
    let changes1 = backend_after_first.get_missing_changes(&Clock::empty().with(&actor2, 1));
    let change12 = changes1.get(0).unwrap();

    backend.apply_changes(vec![remote2]).unwrap();
    backend.apply_local_change(local2).unwrap();
    let backend_after_second = backend.clone();
    let changes2 =
        backend_after_second.get_missing_changes(&Clock::empty().with(&actor1, 1).with(&actor2, 2));
    let change23 = changes2.get(0).unwrap();

    backend.apply_local_change(local3).unwrap();
    let changes3 = backend.get_missing_changes(&Clock::empty().with(&actor1, 2).with(&actor2, 2));
    let change34 = changes3.get(0).unwrap();

    expected_change1.time = change12.time;
    expected_change2.time = change23.time;
    expected_change3.time = change34.time;

    assert_eq!(change12, &&expected_change1);
    assert_eq!(change23, &&expected_change2);
    assert_eq!(change34, &&expected_change3);
}

#[test]
fn test_handle_list_insertion_and_deletion_in_same_change() {
    let local1: ChangeRequest = serde_json::from_str(
        r#"
        {
            "requestType": "change",
            "actor": "0723d2a1-9407-4486-8ffd-6b294ada813f",
            "seq": 1,
            "startOp": 1,
            "version": 0,
            "ops": [
                {
                    "obj": "00000000-0000-0000-0000-000000000000",
                    "action": "makeList",
                    "key": "birds"
                }
            ]
        }
    "#,
    )
    .unwrap();
    let local2: ChangeRequest = serde_json::from_str(
        r#"
        {
            "requestType": "change",
            "actor": "0723d2a1-9407-4486-8ffd-6b294ada813f",
            "seq": 2,
            "startOp": 2,
            "version": 0,
            "ops": [
                {
                    "obj": "1@0723d2a1-9407-4486-8ffd-6b294ada813f",
                    "action": "set",
                    "key": 0,
                    "insert": true,
                    "value": "magpie"
                },
                {
                    "obj": "1@0723d2a1-9407-4486-8ffd-6b294ada813f",
                    "action": "del",
                    "key": 0
                }
            ]
        }
    "#,
    )
    .unwrap();
    let expected_patch: Patch = serde_json::from_str(
        r#"
        {
            "actor": "0723d2a1-9407-4486-8ffd-6b294ada813f",
            "seq": 2,
            "version": 2,
            "clock": {
                "0723d2a1-9407-4486-8ffd-6b294ada813f": 2
            },
            "canUndo": true,
            "canRedo": false,
            "diffs": {
                "objectId": "00000000-0000-0000-0000-000000000000",
                "type": "map",
                "props": {
                    "birds": {
                        "1@0723d2a1-9407-4486-8ffd-6b294ada813f": {
                            "objectId": "1@0723d2a1-9407-4486-8ffd-6b294ada813f",
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
    let mut expected_change: Change = serde_json::from_str(
        r#"
        {
            "actor": "0723d2a1-9407-4486-8ffd-6b294ada813f",
            "seq": 2,
            "startOp": 2,
            "time": 1586540158974,
            "message": null,
            "deps": {},
            "ops": [
                {
                    "obj": "1@0723d2a1-9407-4486-8ffd-6b294ada813f",
                    "action": "set",
                    "key": "_head",
                    "insert": true,
                    "value": "magpie",
                    "pred": []
                },
                {
                    "obj": "1@0723d2a1-9407-4486-8ffd-6b294ada813f",
                    "action": "del",
                    "key": "2@0723d2a1-9407-4486-8ffd-6b294ada813f",
                    "pred": [
                        "2@0723d2a1-9407-4486-8ffd-6b294ada813f"
                    ]
                }
            ]
        }
    "#,
    )
    .unwrap();

    let mut backend = Backend::init();
    backend.apply_local_change(local1).unwrap();
    let patch = backend.apply_local_change(local2).unwrap();
    assert_eq!(patch, expected_patch);

    let changes = backend.get_missing_changes(&Clock::empty().with(
        &ActorID::from_str("0723d2a1-9407-4486-8ffd-6b294ada813f").unwrap(),
        1,
    ));
    let change = changes.get(0).unwrap();
    expected_change.time = change.time;

    assert_eq!(change, &&expected_change);
}
