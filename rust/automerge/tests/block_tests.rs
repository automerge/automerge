use std::sync::Arc;

use automerge::{
    hydrate_list, hydrate_map, hydrate_text,
    iter::Span,
    marks::{ExpandMark, Mark, UpdateSpansConfig},
    transaction::Transactable,
    AutoCommit, ObjType, Patch, PatchAction, Prop, ReadDoc, ScalarValue, Value, ROOT,
};
use test_log::test;

fn markset(values: Vec<(&'static str, ScalarValue)>) -> Option<Arc<automerge::marks::MarkSet>> {
    Some(Arc::new(
        values
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect::<automerge::marks::MarkSet>(),
    ))
}

#[test]
fn update_blocks_change_block_properties() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    let block1 = doc.split_block(&text, 0).unwrap();
    doc.update_object(
        &block1,
        &hydrate_map! {
            "parents" => hydrate_list![],
            "type" => "ordered-list-item",
            "attrs" => hydrate_map!{}
        }
        .into(),
    )
    .unwrap();
    doc.splice_text(&text, 1, 0, "item 1").unwrap();
    let block2 = doc.split_block(&text, 7).unwrap();
    doc.update_object(
        &block2,
        &hydrate_map! {
            "parents" => hydrate_list![],
            "type" => "ordered-list-item",
            "attrs" => hydrate_map!{}
        }
        .into(),
    )
    .unwrap();
    doc.splice_text(&text, 8, 0, "item 2").unwrap();

    doc.update_diff_cursor();

    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        [
            Span::Block(hydrate_map! {
                "type" => "paragraph",
                "parents" => hydrate_list![],
                "attrs" => hydrate_map!{}
            }),
            Span::Text {
                text: "item 1".into(),
                marks: Default::default(),
            },
            Span::Block(hydrate_map! {
                "type" => "unordered-list-item",
                "parents" => hydrate_list!["ordered-list-item"],
                "attrs" => hydrate_map!{
                    "key" => 1,
                },
            }),
            Span::Text {
                text: "item 2".into(),
                marks: Default::default(),
            },
        ],
    )
    .unwrap();

    let spans = doc.spans(&text).unwrap().collect::<Vec<_>>();
    assert_eq!(
        spans,
        vec![
            Span::Block(hydrate_map! {
                "type" => "paragraph",
                "parents" => hydrate_list![],
                "attrs" => hydrate_map!{}
            }),
            Span::Text {
                text: "item 1".into(),
                marks: Default::default()
            },
            Span::Block(hydrate_map! {
                "type" => "unordered-list-item",
                "parents" => hydrate_list!["ordered-list-item"],
                "attrs" => hydrate_map!{"key" => 1}
            }),
            Span::Text {
                text: "item 2".into(),
                marks: Default::default()
            },
        ]
    );
}

#[test]
fn update_blocks_updates_text() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    let _block1 = doc.split_block(&text, 0).unwrap();
    doc.splice_text(&text, 1, 0, "first thing").unwrap();
    let _block2 = doc.split_block(&text, 12).unwrap();
    doc.splice_text(&text, 13, 0, "second thing").unwrap();

    doc.update_diff_cursor();

    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        [
            Span::Block(hydrate_map! {
                "type" => "ordered-list-item",
                "parents" => hydrate_list![],
                "attrs" => hydrate_map!{}
            }),
            Span::Text {
                text: "the first thing".into(),
                marks: Default::default(),
            },
            Span::Block(hydrate_map! {
                "type" => "paragraph",
                "parents" => hydrate_list![],
                "attrs" => hydrate_map!{}
            }),
            Span::Text {
                text: "the things are done".into(),
                marks: Default::default(),
            },
        ],
    )
    .unwrap();

    //let spans = doc.spans(&text).unwrap().collect::<Vec<_>>();
    let spans = doc.spans(&text).unwrap().collect::<Vec<_>>();
    assert_eq!(
        spans,
        vec![
            Span::Block(hydrate_map! {
                "type" => "ordered-list-item",
                "parents" => hydrate_list![],
                "attrs" => hydrate_map!{}
            }),
            Span::Text {
                text: "the first thing".into(),
                marks: Default::default()
            },
            Span::Block(hydrate_map! {
                "type" => "paragraph",
                "parents" => hydrate_list![],
                "attrs" => hydrate_map!{}
            }),
            Span::Text {
                text: "the things are done".into(),
                marks: Default::default()
            },
        ]
    );
}

#[test]
fn update_blocks_updates_marks() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "onetwo").unwrap();
    let _block2 = doc.split_block(&text, 6).unwrap();
    doc.splice_text(&text, 7, 0, "threefour").unwrap();

    doc.update_diff_cursor();

    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        [
            Span::Text {
                text: "one".into(),
                marks: Default::default(),
            },
            Span::Text {
                text: "two".into(),
                marks: markset(vec![("bold", true.into())]),
            },
            Span::Block(hydrate_map! {}),
            Span::Text {
                text: "three".into(),
                marks: markset(vec![("bold", true.into())]),
            },
            Span::Text {
                text: "four".into(),
                marks: Default::default(),
            },
            Span::Block(hydrate_map! {}),
        ],
    )
    .unwrap();

    //let spans = doc.spans(&text).unwrap().collect::<Vec<_>>();
    let spans = doc.spans(&text).unwrap().collect::<Vec<_>>();
    assert_eq!(
        spans,
        vec![
            Span::Text {
                text: "one".into(),
                marks: Default::default()
            },
            Span::Text {
                text: "two".into(),
                marks: markset(vec![("bold", true.into())])
            },
            Span::Block(hydrate_map! {}),
            Span::Text {
                text: "three".into(),
                marks: markset(vec![("bold", true.into())])
            },
            Span::Text {
                text: "four".into(),
                marks: Default::default()
            },
            Span::Block(hydrate_map! {}),
        ],
    );
}

#[test]
fn update_blocks_noop() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    let block1 = doc.split_block(&text, 0).unwrap();
    doc.update_object(
        &block1,
        &hydrate_map! {
            "parents" => hydrate_list![],
            "type" => "ordered-list-item",
            "attrs" => hydrate_map!{}
        }
        .into(),
    )
    .unwrap();
    doc.splice_text(&text, 1, 0, "item 1").unwrap();

    doc.update_diff_cursor();

    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        [
            Span::Block(hydrate_map! {
                "type" => "ordered-list-item",
                "parents" => hydrate_list![],
                "attrs" => hydrate_map!{}
            }),
            Span::Text {
                text: "item 1".into(),
                marks: Default::default(),
            },
        ],
    )
    .unwrap();

    let patches = doc.diff_incremental();
    assert_eq!(patches.len(), 0, "expected no patches");
}

#[test]
fn update_blocks_updates_text_and_blocks_at_once() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    //let block1 = doc.split_block(&text, 0, NewBlock::new("paragraph"))
    let block1 = doc.split_block(&text, 0).unwrap();
    doc.update_object(
        &block1,
        &hydrate_map! {
            "parents" => hydrate_list![],
            "type" => "paragraph",
            "attrs" => hydrate_map!{}
        }
        .into(),
    )
    .unwrap();
    doc.splice_text(&text, 1, 0, "hello world").unwrap();

    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        vec![
            Span::Block(hydrate_map! {
                "type" => "unordered-list-item",
                "parents" => hydrate_list![],
                "attrs" => hydrate_map!{}
            }),
            Span::Text {
                text: "goodbye world".into(),
                marks: Default::default(),
            },
        ],
    )
    .unwrap();

    let spans_after = doc.spans(&text).unwrap().collect::<Vec<_>>();
    assert_eq!(
        spans_after,
        vec![
            automerge::iter::Span::Block(hydrate_map! {
                "type" => "unordered-list-item",
                "parents" => hydrate_list![],
                "attrs" => hydrate_map!{}
            }),
            automerge::iter::Span::Text {
                text: "goodbye world".into(),
                marks: None
            },
        ]
    );
}

#[test]
fn text_complex_block_properties() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    //let block = doc.split_block(&text, 0, NewBlock::new("ordered-list-item"))
    //.unwrap();
    let block1 = doc.split_block(&text, 0).unwrap();
    doc.update_object(
        &block1,
        &hydrate_map! {
            "type" => hydrate_text!("ordered-list-item"),
            "parents" => hydrate_list![hydrate_text!("div")],
        }
        .into(),
    )
    .unwrap();

    let (text_obj, text_id) = doc.get(&block1, "type").unwrap().unwrap();
    assert_eq!(text_obj, automerge::Value::Object(automerge::ObjType::Text));
    let value = doc.text(text_id).unwrap();
    assert_eq!(value, "ordered-list-item");

    let (list_obj, list_id) = doc.get(&block1, "parents").unwrap().unwrap();
    assert_eq!(list_obj, automerge::Value::Object(automerge::ObjType::List));
    let len = doc.length(&list_id);
    assert_eq!(len, 1);
    let (elem, elem_id) = doc.get(&list_id, 0).unwrap().unwrap();
    assert_eq!(elem, automerge::Value::Object(automerge::ObjType::Text));
    let elem_text = doc.text(elem_id).unwrap();
    assert_eq!(elem_text, "div");
}

#[test]
fn update_spans_delete_attribute() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    //let block = doc.split_block(&text, 0, NewBlock::new("ordered-list-item"))
    //.unwrap();
    let block1 = doc.split_block(&text, 0).unwrap();
    doc.update_object(
        &block1,
        &hydrate_map! {
            "type" => hydrate_text!("ordered-list-item"),
            "parents" => hydrate_list![hydrate_text!("div")],
        }
        .into(),
    )
    .unwrap();

    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        [Span::Block(hydrate_map! {
            "type" => "ordered-list-item",
            "parents" => hydrate_list![],
        })],
    )
    .unwrap();

    let spans = doc.spans(&text).unwrap().collect::<Vec<_>>();
    assert_eq!(
        spans,
        vec![automerge::iter::Span::Block(hydrate_map! {
            "type" => "ordered-list-item",
            "parents" => hydrate_list![],
        })]
    );
}

#[test]
fn marks_on_spans_respect_heads() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "hello world").unwrap();

    doc.mark(
        &text,
        Mark::new("bold".to_string(), true, 0, 5),
        ExpandMark::After,
    )
    .unwrap();

    let heads = doc.get_heads();

    doc.mark(
        &text,
        Mark::new("italic".to_string(), true, 5, 11),
        ExpandMark::After,
    )
    .unwrap();

    let spans = doc.spans_at(&text, &heads).unwrap().collect::<Vec<_>>();
    assert_eq!(
        spans,
        vec![
            Span::Text {
                text: "hello".to_string(),
                marks: markset(vec![("bold", ScalarValue::Boolean(true))]),
            },
            Span::Text {
                text: " world".to_string(),
                marks: None,
            }
        ]
    );
}

#[test]
fn marks_in_spans_cross_block_markers() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    doc.splice_text(&text, 0, 0, "lix").unwrap();
    doc.mark(
        &text,
        Mark::new("bold".to_string(), true, 0, 3),
        ExpandMark::After,
    )
    .unwrap();
    let _block = doc.split_block(&text, 1).unwrap();
    let spans = doc.spans(&text).unwrap().collect::<Vec<_>>();

    assert_eq!(
        spans,
        vec![
            Span::Text {
                text: "l".to_string(),
                marks: markset(vec![("bold", true.into())])
            },
            Span::Block(hydrate_map! {}),
            Span::Text {
                text: "ix".to_string(),
                marks: markset(vec![("bold", true.into())])
            },
        ]
    );
}

#[test]
fn test_mark_behavior_on_delete_insert() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    // Insert text with a bold mark
    doc.splice_text(&text, 0, 0, "hello").unwrap();
    doc.mark(
        &text,
        Mark::new("bold".to_string(), true, 0, 5),
        ExpandMark::Both,
    )
    .unwrap();

    // Delete the text and insert new text at the same position
    doc.splice_text(&text, 0, 5, "").unwrap();
    doc.splice_text(&text, 0, 0, "hi").unwrap();

    // Check what marks are present
    let spans = doc.spans(&text).unwrap().collect::<Vec<_>>();
    eprintln!("After delete and insert: {:?}", spans);

    // The bold mark should not apply to the new text
    assert_eq!(
        spans,
        vec![Span::Text {
            text: "hi".to_string(),
            marks: None
        }]
    );
}

#[test]
fn spans_consolidates_marks_which_are_empty_due_to_deleted_marks() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    // Insert text with a bold mark
    doc.splice_text(&text, 0, 0, "hello middle world").unwrap();

    // Bold up to the second 'd' in middle
    doc.mark(
        &text,
        Mark::new("bold".to_string(), true, 0, 9),
        ExpandMark::None,
    )
    .unwrap();
    // Italic from the second 'd' in middle to the end
    doc.mark(
        &text,
        Mark::new("italic".to_string(), true, 9, 18),
        ExpandMark::None,
    )
    .unwrap();

    // Now delete the bold range on middle
    doc.mark(
        &text,
        Mark::new("bold".to_string(), ScalarValue::Null, 6, 9),
        ExpandMark::None,
    )
    .unwrap();
    // And delete the italic range on middle
    doc.mark(
        &text,
        Mark::new("italic".to_string(), ScalarValue::Null, 9, 12),
        ExpandMark::None,
    )
    .unwrap();

    // Check what marks are present
    let spans = doc.spans(&text).unwrap().collect::<Vec<_>>();
    assert_eq!(
        spans,
        vec![
            Span::Text {
                text: "hello ".to_string(),
                marks: markset(vec![("bold", true.into())])
            },
            Span::Text {
                text: "middle".to_string(),
                marks: None
            },
            Span::Text {
                text: " world".to_string(),
                marks: markset(vec![("italic", true.into())])
            },
        ]
    );
}

#[test]
fn spans_consolidates_marks_with_deleted_marks_followed_by_empty_marks() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    doc.splice_text(&text, 0, 0, "hello world").unwrap();

    // Bold world
    doc.mark(
        &text,
        Mark::new("bold".to_string(), true, 0, 6),
        ExpandMark::None,
    )
    .unwrap();
    // Now unbold it
    doc.mark(
        &text,
        Mark::new("bold".to_string(), ScalarValue::Null, 0, 6),
        ExpandMark::None,
    )
    .unwrap();

    // Check what marks are present
    let spans = doc.spans(&text).unwrap().collect::<Vec<_>>();
    assert_eq!(
        spans,
        vec![Span::Text {
            text: "hello world".to_string(),
            marks: None
        },]
    );
}

#[test]
fn spans_consolidates_marks_with_empty_marks_followed_by_deleted_marks() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    doc.splice_text(&text, 0, 0, "hello world").unwrap();

    // Bold world
    doc.mark(
        &text,
        Mark::new("bold".to_string(), true, 6, 11),
        ExpandMark::None,
    )
    .unwrap();
    // Now unbold it
    doc.mark(
        &text,
        Mark::new("bold".to_string(), ScalarValue::Null, 6, 11),
        ExpandMark::None,
    )
    .unwrap();

    // Check what marks are present
    let spans = doc.spans(&text).unwrap().collect::<Vec<_>>();
    assert_eq!(
        spans,
        vec![Span::Text {
            text: "hello world".to_string(),
            marks: None
        },]
    );
}

#[test]
fn update_spans_diffs_marks() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    // Initial text with some marks
    doc.splice_text(&text, 0, 0, "hello world").unwrap();
    doc.mark(
        &text,
        Mark::new("bold".to_string(), true, 0, 5),
        ExpandMark::Both,
    )
    .unwrap();

    // Update spans with different marks
    doc.update_spans(
        &text,
        UpdateSpansConfig::default(),
        [
            Span::Text {
                text: "hello".into(),
                marks: markset(vec![("italic", true.into())]),
            },
            Span::Text {
                text: " ".into(),
                marks: Default::default(),
            },
            Span::Text {
                text: "world".into(),
                marks: markset(vec![("bold", true.into()), ("italic", true.into())]),
            },
        ],
    )
    .unwrap();

    let spans = doc.spans(&text).unwrap().collect::<Vec<_>>();
    assert_eq!(
        spans,
        vec![
            Span::Text {
                text: "hello".to_string(),
                marks: markset(vec![("italic", true.into())])
            },
            Span::Text {
                text: " ".to_string(),
                marks: None
            },
            Span::Text {
                text: "world".to_string(),
                marks: markset(vec![("bold", true.into()), ("italic", true.into())])
            },
        ]
    );
}

#[test]
fn update_spans_uses_expand_config() {
    let mut doc = automerge::AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    // Create custom config with different expand behaviors
    let config = UpdateSpansConfig::default()
        .with_default_expand(ExpandMark::None)
        .with_mark_expand("bold", ExpandMark::After);

    // Apply marks with the config
    doc.update_spans(
        &text,
        config,
        [
            Span::Text {
                text: "hello".into(),
                marks: markset(vec![("bold", true.into())]),
            },
            Span::Text {
                text: " world".into(),
                marks: Default::default(),
            },
        ],
    )
    .unwrap();

    // Insert text after "hello" - should be marked because ExpandMark::After
    doc.splice_text(&text, 5, 0, "!").unwrap();

    // Insert text before "hello" - should NOT be marked because ExpandMark::After
    doc.splice_text(&text, 0, 0, "Oh ").unwrap();

    let spans = doc.spans(&text).unwrap().collect::<Vec<_>>();
    assert_eq!(
        spans,
        vec![
            Span::Text {
                text: "Oh ".to_string(),
                marks: None
            },
            Span::Text {
                text: "hello!".to_string(),
                marks: markset(vec![("bold", true.into())])
            },
            Span::Text {
                text: " world".to_string(),
                marks: None
            },
        ]
    );
}

#[test]
fn diff_emits_block_updates() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    // Disable patch log to make sure we are testing the diff not the patch log
    doc.reset_diff_cursor();
    let block = doc.split_block(&text, 0).unwrap();
    let parents = doc.put_object(&block, "parents", ObjType::List).unwrap();

    let heads = doc.get_heads();
    let patches = doc.diff(&[], &heads);

    let expected_patches = vec![
        Patch {
            action: automerge::PatchAction::PutMap {
                key: "text".to_string(),
                value: (Value::Object(ObjType::Text), text.clone()),
                conflict: false,
            },
            path: vec![],
            obj: ROOT,
        },
        Patch {
            action: automerge::PatchAction::Insert {
                index: 0,
                values: [(Value::Object(ObjType::Map), block.clone(), false)]
                    .into_iter()
                    .collect(),
            },
            path: vec![(ROOT, Prop::Map("text".to_string()))],
            obj: text.clone(),
        },
        Patch {
            action: automerge::PatchAction::PutMap {
                key: "parents".to_string(),
                value: (Value::Object(ObjType::List), parents.clone()),
                conflict: false,
            },
            path: vec![
                (ROOT, Prop::Map("text".to_string())),
                (text.clone(), Prop::Seq(0)),
            ],
            obj: block,
        },
    ];

    assert_eq!(patches, expected_patches);

    // Now make a new change to the document so that diff has to use clocks,
    // which exercises a different code path
    doc.splice_text(&text, 0, 0, "hello world").unwrap();

    let patches = doc.diff(&[], &heads);
    assert_eq!(patches, expected_patches);
}

#[test]
fn merge_produces_block_insertion_diffs() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    let mut doc2 = doc.fork();

    let block1 = doc.split_block(&text, 0).unwrap();

    doc2.update_diff_cursor();
    let heads_before = doc2.get_heads();
    doc2.merge(&mut doc).unwrap();
    let heads_after = doc2.get_heads();
    let patches = doc2.diff(&heads_before, &heads_after);
    for patch in &patches {
        println!("{:?}", patch);
    }

    let patch = patches[0].clone();
    assert_eq!(patch.obj, text);
    assert_eq!(
        patch.action,
        PatchAction::Insert {
            index: 0,
            values: vec![(Value::Object(ObjType::Map), block1, false)]
                .into_iter()
                .collect()
        }
    );
}

#[test]
fn test_splice_with_mark() {
    // Reproduces the issue in https://github.com/automerge/automerge/issues/935
    // The problem was that replacing some marked text using `splice_text` would
    // not preserve marks around the text if the mark boundaries were on the ends
    // of the text that was being replaced.

    let s1 = "abc";
    let m1 = automerge::marks::Mark::new(
        "some_nonexpanding_mark_type".into(),
        "marked".to_string(),
        1,
        2,
    );
    let m2 = automerge::marks::Mark::new(
        "some_expanding_mark_type".into(),
        "marked".to_string(),
        1,
        2,
    );
    let mut doc = AutoCommit::new();
    let txt = doc
        .put_object(&automerge::ROOT, "txt", ObjType::Text)
        .unwrap();
    doc.splice_text(&txt, 0, 0, s1).unwrap();
    doc.mark(&txt, m1, automerge::marks::ExpandMark::None)
        .unwrap();
    doc.mark(&txt, m2, automerge::marks::ExpandMark::Both)
        .unwrap();

    let spans_before = doc.spans(&txt).unwrap().collect::<Vec<_>>();
    assert_eq!(
        spans_before,
        vec![
            Span::Text {
                text: "a".to_string(),
                marks: None
            },
            Span::Text {
                text: "b".to_string(),
                marks: markset(vec![
                    ("some_nonexpanding_mark_type", "marked".into()),
                    ("some_expanding_mark_type", "marked".into())
                ])
            },
            Span::Text {
                text: "c".to_string(),
                marks: None
            },
        ]
    );

    doc.splice_text(&txt, 1, 1, "d").unwrap();

    let spans_after = doc.spans(&txt).unwrap().collect::<Vec<_>>();
    assert_eq!(
        spans_after,
        vec![
            Span::Text {
                text: "a".to_string(),
                marks: None
            },
            Span::Text {
                text: "d".to_string(),
                marks: markset(vec![("some_expanding_mark_type", "marked".into())])
            },
            Span::Text {
                text: "c".to_string(),
                marks: None
            },
        ]
    );
}
