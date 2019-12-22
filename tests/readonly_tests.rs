extern crate automerge;
use automerge::{Document, Change};

#[test]
fn test_concurrent_ops() {
    let changes1: Vec<Change>  = serde_json::from_str(
        r#"
            [
                {
                    "ops": [
                        {
                            "action": "makeList",
                            "obj": "79a4d939-09e9-4dc9-a4c6-0bffb98ee0d5"
                        },
                        {
                            "action": "link",
                            "obj": "00000000-0000-0000-0000-000000000000",
                            "key": "cards",
                            "value": "79a4d939-09e9-4dc9-a4c6-0bffb98ee0d5"
                        },
                        {
                            "action": "makeMap",
                            "obj": "a092dea1-6fa5-4459-91d4-f7aebf0c0a77"
                        },
                        {
                            "action": "link",
                            "obj": "00000000-0000-0000-0000-000000000000",
                            "key": "cards_by_id",
                            "value": "a092dea1-6fa5-4459-91d4-f7aebf0c0a77"
                        },
                        {
                            "action": "set",
                            "obj": "00000000-0000-0000-0000-000000000000",
                            "key": "numRounds",
                            "value": 0,
                            "datatype": "counter"
                        }
                    ],
                    "actor": "fc6c6433-296a-4e7d-983b-589cde8b78ef",
                    "seq": 1,
                    "deps": {},
                    "message": "Initialization"
                },
                {
                    "ops": [
                        {
                            "action": "ins",
                            "obj": "79a4d939-09e9-4dc9-a4c6-0bffb98ee0d5",
                            "key": "_head",
                            "elem": 1
                        },
                        {
                            "action": "makeMap",
                            "obj": "003000cf-2d2d-4d37-9fb0-10f8ec70975c"
                        },
                        {
                            "action": "set",
                            "obj": "003000cf-2d2d-4d37-9fb0-10f8ec70975c",
                            "key": "title",
                            "value": "Rewrite everything in clojure"
                        },
                        {
                            "action": "set",
                            "obj": "003000cf-2d2d-4d37-9fb0-10f8ec70975c",
                            "key": "done",
                            "value": false
                        },
                        {
                            "action": "link",
                            "obj": "79a4d939-09e9-4dc9-a4c6-0bffb98ee0d5",
                            "key": "fc6c6433-296a-4e7d-983b-589cde8b78ef:1",
                            "value": "003000cf-2d2d-4d37-9fb0-10f8ec70975c"
                        }
                    ],
                    "actor": "fc6c6433-296a-4e7d-983b-589cde8b78ef",
                    "seq": 2,
                    "deps": {},
                    "message": "Add card"
                },
                {
                    "ops": [
                        {
                            "action": "ins",
                            "obj": "79a4d939-09e9-4dc9-a4c6-0bffb98ee0d5",
                            "key": "fc6c6433-296a-4e7d-983b-589cde8b78ef:1",
                            "elem": 2
                        },
                        {
                            "action": "makeMap",
                            "obj": "21ca2b86-e9a5-4a7f-9cf5-3a7112d3948d"
                        },
                        {
                            "action": "set",
                            "obj": "21ca2b86-e9a5-4a7f-9cf5-3a7112d3948d",
                            "key": "title",
                            "value": "concurrent op 1"
                        },
                        {
                            "action": "set",
                            "obj": "21ca2b86-e9a5-4a7f-9cf5-3a7112d3948d",
                            "key": "done",
                            "value": false
                        },
                        {
                            "action": "link",
                            "obj": "79a4d939-09e9-4dc9-a4c6-0bffb98ee0d5",
                            "key": "fc6c6433-296a-4e7d-983b-589cde8b78ef:2",
                            "value": "21ca2b86-e9a5-4a7f-9cf5-3a7112d3948d"
                        }
                    ],
                    "actor": "fc6c6433-296a-4e7d-983b-589cde8b78ef",
                    "seq": 3,
                    "deps": {},
                    "message": "concurrently add card (op 1)"
                }
            ]
        "#,
    ).unwrap();

    let changes2: Vec<Change> = serde_json::from_str(
        r#"
            [
                {
                    "ops": [
                        {
                            "action": "makeList",
                            "obj": "79a4d939-09e9-4dc9-a4c6-0bffb98ee0d5"
                        },
                        {
                            "action": "link",
                            "obj": "00000000-0000-0000-0000-000000000000",
                            "key": "cards",
                            "value": "79a4d939-09e9-4dc9-a4c6-0bffb98ee0d5"
                        },
                        {
                            "action": "makeMap",
                            "obj": "a092dea1-6fa5-4459-91d4-f7aebf0c0a77"
                        },
                        {
                            "action": "link",
                            "obj": "00000000-0000-0000-0000-000000000000",
                            "key": "cards_by_id",
                            "value": "a092dea1-6fa5-4459-91d4-f7aebf0c0a77"
                        },
                        {
                            "action": "set",
                            "obj": "00000000-0000-0000-0000-000000000000",
                            "key": "numRounds",
                            "value": 0,
                            "datatype": "counter"
                        }
                    ],
                    "actor": "fc6c6433-296a-4e7d-983b-589cde8b78ef",
                    "seq": 1,
                    "deps": {},
                    "message": "Initialization"
                },
                {
                    "ops": [
                        {
                            "action": "ins",
                            "obj": "79a4d939-09e9-4dc9-a4c6-0bffb98ee0d5",
                            "key": "_head",
                            "elem": 1
                        },
                        {
                            "action": "makeMap",
                            "obj": "003000cf-2d2d-4d37-9fb0-10f8ec70975c"
                        },
                        {
                            "action": "set",
                            "obj": "003000cf-2d2d-4d37-9fb0-10f8ec70975c",
                            "key": "title",
                            "value": "Rewrite everything in clojure"
                        },
                        {
                            "action": "set",
                            "obj": "003000cf-2d2d-4d37-9fb0-10f8ec70975c",
                            "key": "done",
                            "value": false
                        },
                        {
                            "action": "link",
                            "obj": "79a4d939-09e9-4dc9-a4c6-0bffb98ee0d5",
                            "key": "fc6c6433-296a-4e7d-983b-589cde8b78ef:1",
                            "value": "003000cf-2d2d-4d37-9fb0-10f8ec70975c"
                        }
                    ],
                    "actor": "fc6c6433-296a-4e7d-983b-589cde8b78ef",
                    "seq": 2,
                    "deps": {},
                    "message": "Add card"
                },
                {
                    "ops": [
                        {
                            "action": "ins",
                            "obj": "79a4d939-09e9-4dc9-a4c6-0bffb98ee0d5",
                            "key": "fc6c6433-296a-4e7d-983b-589cde8b78ef:1",
                            "elem": 2
                        },
                        {
                            "action": "makeMap",
                            "obj": "21ca2b86-e9a5-4a7f-9cf5-3a7112d3948d"
                        },
                        {
                            "action": "set",
                            "obj": "21ca2b86-e9a5-4a7f-9cf5-3a7112d3948d",
                            "key": "title",
                            "value": "concurrent op 1"
                        },
                        {
                            "action": "set",
                            "obj": "21ca2b86-e9a5-4a7f-9cf5-3a7112d3948d",
                            "key": "done",
                            "value": false
                        },
                        {
                            "action": "link",
                            "obj": "79a4d939-09e9-4dc9-a4c6-0bffb98ee0d5",
                            "key": "fc6c6433-296a-4e7d-983b-589cde8b78ef:2",
                            "value": "21ca2b86-e9a5-4a7f-9cf5-3a7112d3948d"
                        }
                    ],
                    "actor": "fc6c6433-296a-4e7d-983b-589cde8b78ef",
                    "seq": 3,
                    "deps": {},
                    "message": "concurrently add card (op 1)"
                },
                {
                    "ops": [
                        {
                            "action": "ins",
                            "obj": "79a4d939-09e9-4dc9-a4c6-0bffb98ee0d5",
                            "key": "fc6c6433-296a-4e7d-983b-589cde8b78ef:2",
                            "elem": 3
                        },
                        {
                            "action": "makeMap",
                            "obj": "3c5e415e-392d-4bd8-8fee-4f75a78d38e4"
                        },
                        {
                            "action": "set",
                            "obj": "3c5e415e-392d-4bd8-8fee-4f75a78d38e4",
                            "key": "title",
                            "value": "concurrent op 2"
                        },
                        {
                            "action": "set",
                            "obj": "3c5e415e-392d-4bd8-8fee-4f75a78d38e4",
                            "key": "done",
                            "value": false
                        },
                        {
                            "action": "link",
                            "obj": "79a4d939-09e9-4dc9-a4c6-0bffb98ee0d5",
                            "key": "e3b27fb8-574f-43c2-94eb-d41a22c8b30c:3",
                            "value": "3c5e415e-392d-4bd8-8fee-4f75a78d38e4"
                        }
                    ],
                    "actor": "e3b27fb8-574f-43c2-94eb-d41a22c8b30c",
                    "seq": 1,
                    "deps": {
                        "fc6c6433-296a-4e7d-983b-589cde8b78ef": 3
                    },
                    "message": "concurrently add card (op 2)"
                }
            ]
        "#
    ).unwrap();

    let mut doc = Document::load(changes1).unwrap();
    for change in changes2 {
        doc.apply_change(change).unwrap()
    }
    let expected: serde_json::Value = serde_json::from_str(
        r#"
        {
            "cards_by_id": {},
            "numRounds": 0.0,
            "cards": [
                {"title": "Rewrite everything in clojure", "done": false},
                {"title": "concurrent op 1", "done": false},
                {"title": "concurrent op 2", "done": false}
            ]
        }
    "#,
    ).unwrap();
    let actual = doc.state().unwrap();
    assert_eq!(expected, actual);
}
