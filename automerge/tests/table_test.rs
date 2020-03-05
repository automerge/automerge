extern crate automerge;
use automerge::{Change, Document};

#[test]
fn test_table_column_order() {
    let changes1: Vec<Change> = serde_json::from_str(
        r#"
            [
                {
                    "ops": [
                        {
                            "action": "makeTable",
                            "obj": "a9de13ee-9b2f-43f6-b167-12823931245b"
                        },
                        {
                            "action": "makeList",
                            "obj": "de41fdb3-fdf9-4146-a1d3-7049c983aacb"
                        },
                        {
                            "action": "ins",
                            "obj": "de41fdb3-fdf9-4146-a1d3-7049c983aacb",
                            "key": "_head",
                            "elem": 1
                        },
                        {
                            "action": "set",
                            "obj": "de41fdb3-fdf9-4146-a1d3-7049c983aacb",
                            "key": "c01d1a3b-2abe-481b-994f-3f37aa4fbb07:1",
                            "value": "authors"
                        },
                        {
                            "action": "ins",
                            "obj": "de41fdb3-fdf9-4146-a1d3-7049c983aacb",
                            "key": "c01d1a3b-2abe-481b-994f-3f37aa4fbb07:1",
                            "elem": 2
                        },
                        {
                            "action": "set",
                            "obj": "de41fdb3-fdf9-4146-a1d3-7049c983aacb",
                            "key": "c01d1a3b-2abe-481b-994f-3f37aa4fbb07:2",
                            "value": "title"
                        },
                        {
                            "action": "ins",
                            "obj": "de41fdb3-fdf9-4146-a1d3-7049c983aacb",
                            "key": "c01d1a3b-2abe-481b-994f-3f37aa4fbb07:2",
                            "elem": 3
                        },
                        {
                            "action": "set",
                            "obj": "de41fdb3-fdf9-4146-a1d3-7049c983aacb",
                            "key": "c01d1a3b-2abe-481b-994f-3f37aa4fbb07:3",
                            "value": "isbn"
                        },
                        {
                            "action": "link",
                            "obj": "a9de13ee-9b2f-43f6-b167-12823931245b",
                            "key": "columns",
                            "value": "de41fdb3-fdf9-4146-a1d3-7049c983aacb"
                        },
                        {
                            "action": "link",
                            "obj": "00000000-0000-0000-0000-000000000000",
                            "key": "books",
                            "value": "a9de13ee-9b2f-43f6-b167-12823931245b"
                        },
                        {
                            "action": "makeMap",
                            "obj": "b822bb61-1046-4faf-8719-ef479f4b6ca5"
                        },
                        {
                            "action": "makeList",
                            "obj": "8fbabf41-64e5-41e8-b82b-b23c668f8f51"
                        },
                        {
                            "action": "ins",
                            "obj": "8fbabf41-64e5-41e8-b82b-b23c668f8f51",
                            "key": "_head",
                            "elem": 1
                        },
                        {
                            "action": "set",
                            "obj": "8fbabf41-64e5-41e8-b82b-b23c668f8f51",
                            "key": "c01d1a3b-2abe-481b-994f-3f37aa4fbb07:1",
                            "value": "Kleppmann, Martin"
                        },
                        {
                            "action": "link",
                            "obj": "b822bb61-1046-4faf-8719-ef479f4b6ca5",
                            "key": "authors",
                            "value": "8fbabf41-64e5-41e8-b82b-b23c668f8f51"
                        },
                        {
                            "action": "set",
                            "obj": "b822bb61-1046-4faf-8719-ef479f4b6ca5",
                            "key": "title",
                            "value": "Designing Data-Intensive Applications"
                        },
                        {
                            "action": "set",
                            "obj": "b822bb61-1046-4faf-8719-ef479f4b6ca5",
                            "key": "isbn",
                            "value": "1449373321"
                        },
                        {
                            "action": "link",
                            "obj": "a9de13ee-9b2f-43f6-b167-12823931245b",
                            "key": "b822bb61-1046-4faf-8719-ef479f4b6ca5",
                            "value": "b822bb61-1046-4faf-8719-ef479f4b6ca5"
                        }
                    ],
                    "actor": "c01d1a3b-2abe-481b-994f-3f37aa4fbb07",
                    "seq": 1,
                    "deps": {}
                }
            ]
        "#,
    )
    .unwrap();

    let doc = Document::load(changes1).unwrap();
    let expected: serde_json::Value = serde_json::from_str(
        r#"
        {
            "books": {
                "columns": ["authors", "title", "isbn"],
                "b822bb61-1046-4faf-8719-ef479f4b6ca5": {
                    "authors": ["Kleppmann, Martin"],
                    "isbn": "1449373321",
                    "title": "Designing Data-Intensive Applications"
                }
            }
        }
    "#,
    )
    .unwrap();
    let actual = doc.state().to_json();
    assert_eq!(expected, actual);
}
