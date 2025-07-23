use std::sync::Arc;

use automerge::{
    hydrate_list, hydrate_map, hydrate_text,
    iter::Span,
    marks::{ExpandMark, Mark},
    transaction::Transactable,
    AutoCommit, BlockOrText, ObjType, ReadDoc, ScalarValue, ROOT,
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
        [
            BlockOrText::Block(hydrate_map! {
                "type" => "paragraph",
                "parents" => hydrate_list![],
                "attrs" => hydrate_map!{}
            }),
            BlockOrText::Text("item 1".into()),
            BlockOrText::Block(hydrate_map! {
                "type" => "unordered-list-item",
                "parents" => hydrate_list!["ordered-list-item"],
                "attrs" => hydrate_map!{
                    "key" => 1,
                },
            }),
            BlockOrText::Text("item 2".into()),
        ],
    )
    .unwrap();

    let spans = doc
        .spans(&text)
        .unwrap()
        .map(|s| match s {
            automerge::iter::Span::Block(b) => BlockOrText::Block(b),
            automerge::iter::Span::Text(t, _) => BlockOrText::Text(std::borrow::Cow::Owned(t)),
        })
        .collect::<Vec<_>>();
    assert_eq!(
        spans,
        vec![
            BlockOrText::Block(hydrate_map! {
                "type" => "paragraph",
                "parents" => hydrate_list![],
                "attrs" => hydrate_map!{}
            }),
            BlockOrText::Text("item 1".into()),
            BlockOrText::Block(hydrate_map! {
                "type" => "unordered-list-item",
                "parents" => hydrate_list!["ordered-list-item"],
                "attrs" => hydrate_map!{"key" => 1}
            }),
            BlockOrText::Text("item 2".into()),
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
        [
            BlockOrText::Block(hydrate_map! {
                "type" => "ordered-list-item",
                "parents" => hydrate_list![],
                "attrs" => hydrate_map!{}
            }),
            BlockOrText::Text("the first thing".into()),
            BlockOrText::Block(hydrate_map! {
                "type" => "paragraph",
                "parents" => hydrate_list![],
                "attrs" => hydrate_map!{}
            }),
            BlockOrText::Text("the things are done".into()),
        ],
    )
    .unwrap();

    //let spans = doc.spans(&text).unwrap().collect::<Vec<_>>();
    let spans = doc
        .spans(&text)
        .unwrap()
        .map(|s| match s {
            automerge::iter::Span::Block(b) => BlockOrText::Block(b),
            automerge::iter::Span::Text(t, _) => BlockOrText::Text(std::borrow::Cow::Owned(t)),
        })
        .collect::<Vec<_>>();
    assert_eq!(
        spans,
        vec![
            BlockOrText::Block(hydrate_map! {
                "type" => "ordered-list-item",
                "parents" => hydrate_list![],
                "attrs" => hydrate_map!{}
            }),
            BlockOrText::Text("the first thing".into()),
            BlockOrText::Block(hydrate_map! {
                "type" => "paragraph",
                "parents" => hydrate_list![],
                "attrs" => hydrate_map!{}
            }),
            BlockOrText::Text("the things are done".into()),
        ]
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
        [
            BlockOrText::Block(hydrate_map! {
                "type" => "ordered-list-item",
                "parents" => hydrate_list![],
                "attrs" => hydrate_map!{}
            }),
            BlockOrText::Text("item 1".into()),
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
        vec![
            BlockOrText::Block(hydrate_map! {
                "type" => "unordered-list-item",
                "parents" => hydrate_list![],
                "attrs" => hydrate_map!{}
            }),
            BlockOrText::Text("goodbye world".into()),
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
            automerge::iter::Span::Text("goodbye world".into(), None),
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
        [BlockOrText::Block(hydrate_map! {
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
            Span::Text(
                "hello".to_string(),
                Some(Arc::new(
                    vec![("bold".to_string(), ScalarValue::Boolean(true))]
                        .into_iter()
                        .collect()
                ))
            ),
            Span::Text(" world".to_string(), None,)
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
            Span::Text(
                "l".to_string(),
                Some(Arc::new(
                    vec![("bold".to_string(), true.into())]
                        .into_iter()
                        .collect()
                ))
            ),
            Span::Block(hydrate_map! {}),
            Span::Text(
                "ix".to_string(),
                Some(Arc::new(
                    vec![("bold".to_string(), true.into())]
                        .into_iter()
                        .collect()
                ))
            ),
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
    doc.splice_text(&text, 0, 5, "hi").unwrap();

    // Check what marks are present
    let spans = doc.spans(&text).unwrap().collect::<Vec<_>>();
    eprintln!("After delete and insert: {:?}", spans);

    // The bold mark should not apply to the new text
    assert_eq!(spans, vec![Span::Text("hi".to_string(), None)]);
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
            Span::Text("hello ".to_string(), markset(vec![("bold", true.into())])),
            Span::Text("middle".to_string(), None),
            Span::Text(" world".to_string(), markset(vec![("italic", true.into())])),
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
    assert_eq!(spans, vec![Span::Text("hello world".to_string(), None),]);
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
    assert_eq!(spans, vec![Span::Text("hello world".to_string(), None),]);
}
