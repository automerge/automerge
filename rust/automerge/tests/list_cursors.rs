//! Cursors on list objects.
//!
//! `Cursor` identifies a position in any sequence, not only in text. These
//! tests pin down the list behaviour independently of the bindings, mirroring
//! the text cursor tests in `src/automerge/tests.rs`.

use automerge::transaction::Transactable;
use automerge::{
    AutoCommit, AutomergeError, Cursor, CursorPosition, MoveCursor, ObjId, ObjType, ReadDoc,
    ScalarValue, Value, ROOT,
};
use proptest::prelude::*;

fn list_with(doc: &mut AutoCommit, values: &[&str]) -> ObjId {
    let list = doc.put_object(ROOT, "list", ObjType::List).unwrap();
    for (i, v) in values.iter().enumerate() {
        doc.insert(&list, i, *v).unwrap();
    }
    list
}

fn values(doc: &AutoCommit, list: &ObjId) -> Vec<String> {
    doc.list_range(list, ..)
        .map(|item| match Value::from(item.value) {
            Value::Scalar(s) => s.as_str().unwrap().to_string(),
            Value::Object(t) => format!("{:?}", t),
        })
        .collect()
}

fn pos(doc: &AutoCommit, list: &ObjId, cursor: &Cursor) -> usize {
    doc.get_cursor_position(list, cursor, None).unwrap()
}

#[test]
fn list_cursor_follows_element_through_inserts_before_it() {
    let mut doc = AutoCommit::new();
    let list = list_with(&mut doc, &["a", "b", "c"]);
    let cursor = doc.get_cursor(&list, 1, None).unwrap();
    assert!(matches!(cursor, Cursor::Op(_)));
    assert_eq!(pos(&doc, &list, &cursor), 1);

    doc.insert(&list, 0, "x").unwrap();
    doc.insert(&list, 0, "y").unwrap();
    assert_eq!(values(&doc, &list), ["y", "x", "a", "b", "c"]);
    assert_eq!(pos(&doc, &list, &cursor), 3);

    // inserts after the element don't move the cursor
    doc.insert(&list, 4, "z").unwrap();
    assert_eq!(pos(&doc, &list, &cursor), 3);

    // deletes before the element do
    doc.delete(&list, 0).unwrap();
    assert_eq!(pos(&doc, &list, &cursor), 2);
    assert_eq!(doc.get(&list, 2).unwrap().unwrap().0, Value::from("b"));
}

#[test]
fn list_cursor_follows_element_through_concurrent_inserts() {
    let mut doc1 = AutoCommit::new();
    let list = list_with(&mut doc1, &["a", "b", "c"]);
    let mut doc2 = doc1.fork();
    let cursor = doc1.get_cursor(&list, 2, None).unwrap();

    doc1.insert(&list, 0, "x").unwrap();
    doc2.insert(&list, 1, "y").unwrap();
    doc2.insert(&list, 3, "z").unwrap();
    doc1.merge(&mut doc2).unwrap();
    assert_eq!(values(&doc1, &list), ["x", "a", "y", "b", "z", "c"]);

    let p = pos(&doc1, &list, &cursor);
    assert_eq!(p, 5);
    assert_eq!(doc1.get(&list, p).unwrap().unwrap().0, Value::from("c"));

    // the cursor string round-trips and works in the other document too
    let parsed = Cursor::try_from(cursor.to_string()).unwrap();
    doc2.merge(&mut doc1).unwrap();
    assert_eq!(pos(&doc2, &list, &parsed), 5);
}

#[test]
fn list_cursor_after_deleted_element_resolves_like_text() {
    // Same scenario as the text test "aaa@bbb" -> "aaa~~~bbb": the element
    // at index 3 is deleted and three elements are inserted in its place.
    let mut doc = AutoCommit::new();
    let list = list_with(&mut doc, &["a", "a", "a", "@", "b", "b", "b"]);
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "aaa@bbb").unwrap();

    let list_before = doc
        .get_cursor_moving(&list, 3, None, MoveCursor::Before)
        .unwrap();
    let list_after = doc
        .get_cursor_moving(&list, 3, None, MoveCursor::After)
        .unwrap();
    let text_before = doc
        .get_cursor_moving(&text, 3, None, MoveCursor::Before)
        .unwrap();
    let text_after = doc
        .get_cursor_moving(&text, 3, None, MoveCursor::After)
        .unwrap();
    // the default is `MoveCursor::After`
    let list_default = doc.get_cursor(&list, 3, None).unwrap();

    doc.splice(&list, 3, 1, ["~", "~", "~"].map(ScalarValue::from))
        .unwrap();
    doc.splice_text(&text, 3, 1, "~~~").unwrap();
    assert_eq!(
        values(&doc, &list),
        ["a", "a", "a", "~", "~", "~", "b", "b", "b"]
    );

    assert_eq!(pos(&doc, &list, &list_before), 2);
    assert_eq!(pos(&doc, &list, &list_after), 6);
    assert_eq!(pos(&doc, &list, &list_default), 6);
    assert_eq!(pos(&doc, &text, &text_before), 2);
    assert_eq!(pos(&doc, &text, &text_after), 6);

    // deleting everything before the cursor
    doc.splice(&list, 0, 6, Vec::<ScalarValue>::new()).unwrap();
    assert_eq!(pos(&doc, &list, &list_before), 0);
    assert_eq!(pos(&doc, &list, &list_after), 0);

    // deleting everything after the cursor
    doc.splice(&list, 0, 3, Vec::<ScalarValue>::new()).unwrap();
    assert_eq!(doc.length(&list), 0);
    assert_eq!(pos(&doc, &list, &list_before), 0);
    assert_eq!(pos(&doc, &list, &list_after), 0);
}

#[test]
fn list_cursor_after_deleting_last_element_moves_to_end() {
    let mut doc = AutoCommit::new();
    let list = list_with(&mut doc, &["a", "b", "c"]);
    let before = doc
        .get_cursor_moving(&list, 2, None, MoveCursor::Before)
        .unwrap();
    let after = doc
        .get_cursor_moving(&list, 2, None, MoveCursor::After)
        .unwrap();
    doc.delete(&list, 2).unwrap();
    assert_eq!(pos(&doc, &list, &before), 1);
    assert_eq!(pos(&doc, &list, &after), 2);
}

#[test]
fn list_cursor_at_historical_heads() {
    let mut doc = AutoCommit::new();
    let list = list_with(&mut doc, &["a", "b", "c"]);
    let heads0 = doc.get_heads();
    let cursor = doc.get_cursor(&list, 1, None).unwrap();

    doc.insert(&list, 0, "x").unwrap();
    doc.delete(&list, 3).unwrap();
    let heads1 = doc.get_heads();
    assert_eq!(pos(&doc, &list, &cursor), 2);

    // position at old heads
    assert_eq!(
        doc.get_cursor_position(&list, &cursor, Some(&heads0)),
        Ok(1)
    );
    assert_eq!(
        doc.get_cursor_position(&list, &cursor, Some(&heads1)),
        Ok(2)
    );

    // a cursor created at old heads refers to the same element
    let at_heads0 = doc.get_cursor(&list, 1, Some(&heads0)).unwrap();
    assert_eq!(at_heads0, cursor);
    assert_eq!(doc.get_cursor(&list, 2, None).unwrap(), cursor);

    // index 2 at heads0 is "c", which is deleted now
    let c_at_heads0 = doc
        .get_cursor_moving(&list, 2, Some(&heads0), MoveCursor::Before)
        .unwrap();
    assert_eq!(pos(&doc, &list, &c_at_heads0), 2);
    assert_eq!(
        doc.get_cursor_position(&list, &c_at_heads0, Some(&heads0)),
        Ok(2)
    );

    // an index past the end at the old heads is invalid
    assert_eq!(
        doc.get_cursor(&list, 3, Some(&heads0)),
        Err(AutomergeError::InvalidIndex(3))
    );

    // a cursor to an element created after the heads is invalid at those heads
    let x = doc.get_cursor(&list, 0, None).unwrap();
    assert_eq!(
        doc.get_cursor_position(&list, &x, Some(&heads0)),
        Err(AutomergeError::InvalidCursor(x))
    );
}

#[test]
fn list_start_and_end_cursors() {
    let mut doc = AutoCommit::new();
    let list = list_with(&mut doc, &["a", "b", "c"]);
    let heads0 = doc.get_heads();
    let start = doc.get_cursor(&list, CursorPosition::Start, None).unwrap();
    let end = doc.get_cursor(&list, CursorPosition::End, None).unwrap();
    assert_eq!(start, Cursor::Start);
    assert_eq!(end, Cursor::End);
    assert_eq!(pos(&doc, &list, &start), 0);
    assert_eq!(pos(&doc, &list, &end), 3);

    doc.insert(&list, 0, "x").unwrap();
    doc.insert(&list, 4, "y").unwrap();
    assert_eq!(pos(&doc, &list, &start), 0);
    assert_eq!(pos(&doc, &list, &end), 5);
    assert_eq!(doc.get_cursor_position(&list, &end, Some(&heads0)), Ok(3));

    doc.splice(&list, 0, 5, Vec::<ScalarValue>::new()).unwrap();
    assert_eq!(pos(&doc, &list, &start), 0);
    assert_eq!(pos(&doc, &list, &end), 0);
}

#[test]
fn list_cursor_to_nested_object() {
    let mut doc = AutoCommit::new();
    let list = doc.put_object(ROOT, "list", ObjType::List).unwrap();
    doc.insert(&list, 0, "a").unwrap();
    let map = doc.insert_object(&list, 1, ObjType::Map).unwrap();
    doc.put(&map, "key", "value").unwrap();
    let inner = doc.insert_object(&list, 2, ObjType::List).unwrap();
    doc.insert(&inner, 0, 1).unwrap();
    doc.insert(&inner, 1, 2).unwrap();

    let to_map = doc.get_cursor(&list, 1, None).unwrap();
    doc.insert(&list, 0, "x").unwrap();
    doc.insert_object(&list, 0, ObjType::Text).unwrap();
    let p = pos(&doc, &list, &to_map);
    assert_eq!(p, 3);
    assert_eq!(
        doc.get(&list, p).unwrap().unwrap(),
        (Value::Object(ObjType::Map), map.clone())
    );

    // updating the nested object doesn't affect the cursor
    doc.put(&map, "key", "other").unwrap();
    assert_eq!(pos(&doc, &list, &to_map), 3);

    // cursors on a list nested in a list
    let in_inner = doc.get_cursor(&inner, 1, None).unwrap();
    doc.insert(&inner, 0, 0).unwrap();
    assert_eq!(pos(&doc, &inner, &in_inner), 2);

    // deleting the nested object resolves like any other deleted element
    let before = doc
        .get_cursor_moving(&list, 3, None, MoveCursor::Before)
        .unwrap();
    doc.delete(&list, 3).unwrap();
    assert_eq!(pos(&doc, &list, &to_map), 3);
    assert_eq!(pos(&doc, &list, &before), 2);
}

#[test]
fn cursors_are_rejected_on_maps() {
    let mut doc = AutoCommit::new();
    let list = list_with(&mut doc, &["a"]);
    let map = doc.put_object(ROOT, "map", ObjType::Map).unwrap();
    doc.put(&map, "a", 1).unwrap();
    assert_eq!(
        doc.get_cursor(&map, 0, None),
        Err(AutomergeError::InvalidOp(ObjType::Map))
    );
    assert_eq!(
        doc.get_cursor(&ROOT, 0, None),
        Err(AutomergeError::InvalidOp(ObjType::Map))
    );
    let cursor = doc.get_cursor(&list, 0, None).unwrap();
    assert_eq!(
        doc.get_cursor_position(&map, &cursor, None),
        Err(AutomergeError::InvalidCursor(cursor))
    );
}

#[derive(Debug, Clone)]
enum Action {
    Insert { index: usize, value: char },
    Delete { index: usize, len: usize },
}

fn arb_actions() -> impl Strategy<Value = (Vec<char>, usize, bool, Vec<Action>)> {
    (1usize..10).prop_flat_map(|initial_len| {
        (
            proptest::collection::vec(proptest::char::range('a', 'z'), initial_len),
            0..initial_len,
            any::<bool>(),
            proptest::collection::vec(
                prop_oneof![
                    (any::<usize>(), proptest::char::range('a', 'z'))
                        .prop_map(|(index, value)| Action::Insert { index, value }),
                    (any::<usize>(), 1usize..4)
                        .prop_map(|(index, len)| Action::Delete { index, len }),
                ],
                0..30,
            ),
        )
    })
}

proptest! {
    // The same sequence of edits applied to a list and to a text object with
    // the same (ASCII) content must move cursors identically.
    #[test]
    fn list_and_text_cursors_agree((initial, index, before, actions) in arb_actions()) {
        let mut doc = AutoCommit::new();
        let list = doc.put_object(ROOT, "list", ObjType::List).unwrap();
        let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
        for (i, c) in initial.iter().enumerate() {
            doc.insert(&list, i, c.to_string()).unwrap();
        }
        doc.splice_text(&text, 0, 0, &initial.iter().collect::<String>()).unwrap();

        let move_cursor = if before { MoveCursor::Before } else { MoveCursor::After };
        let list_cursor = doc.get_cursor_moving(&list, index, None, move_cursor.clone()).unwrap();
        let text_cursor = doc.get_cursor_moving(&text, index, None, move_cursor).unwrap();

        let mut len = initial.len();
        for action in actions {
            match action {
                Action::Insert { index, value } => {
                    let index = index % (len + 1);
                    doc.insert(&list, index, value.to_string()).unwrap();
                    doc.splice_text(&text, index, 0, &value.to_string()).unwrap();
                    len += 1;
                }
                Action::Delete { index, len: del } => {
                    if len == 0 {
                        continue;
                    }
                    let index = index % len;
                    let del = del.min(len - index);
                    doc.splice(&list, index, del as isize, Vec::<ScalarValue>::new()).unwrap();
                    doc.splice_text(&text, index, del as isize, "").unwrap();
                    len -= del;
                }
            }
            prop_assert_eq!(doc.length(&list), len);
            prop_assert_eq!(doc.length(&text), len);
            let list_pos = doc.get_cursor_position(&list, &list_cursor, None).unwrap();
            let text_pos = doc.get_cursor_position(&text, &text_cursor, None).unwrap();
            prop_assert_eq!(list_pos, text_pos);
            prop_assert!(list_pos <= len);
        }
    }
}
