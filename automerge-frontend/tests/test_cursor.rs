use std::convert::TryInto;

use amp::RootDiff;
use automerge_backend::Backend;
use automerge_frontend::{Frontend, InvalidChangeRequest, LocalChange, Path, Primitive, Value};
use automerge_protocol as amp;
use maplit::hashmap;
use unicode_segmentation::UnicodeSegmentation;

#[test]
fn test_allow_cursor_on_list_element() {
    let _ = env_logger::builder().is_test(true).try_init().unwrap();
    let mut frontend = Frontend::new();
    let change = frontend
        .change::<_, _, InvalidChangeRequest>(None, |d| {
            d.add_change(LocalChange::set(Path::root().key("list"), vec![1, 2, 3]))?;
            let cursor = d
                .cursor_to_path(&Path::root().key("list").index(1))
                .unwrap();
            d.add_change(LocalChange::set(Path::root().key("cursor"), cursor))?;
            Ok(())
        })
        .unwrap()
        .1
        .unwrap();
    let mut backend = Backend::new();
    backend
        .apply_changes(vec![change.try_into().unwrap()])
        .unwrap();

    let mut backend2 = Backend::new();
    backend2
        .apply_changes(backend.get_changes(&[]).into_iter().cloned().collect())
        .unwrap();
    let mut frontend2 = Frontend::new();
    frontend2
        .apply_patch(backend2.get_patch().unwrap())
        .unwrap();
    let index_value = frontend2.get_value(&Path::root().key("cursor")).unwrap();
    if let Value::Primitive(Primitive::Cursor(c)) = index_value {
        assert_eq!(c.index, 1)
    } else {
        panic!("value was not a cursor");
    }
}

#[test]
fn test_allow_cursor_on_text_element() {
    let mut frontend = Frontend::new();
    let change = frontend
        .change::<_, _, InvalidChangeRequest>(None, |d| {
            d.add_change(LocalChange::set(
                Path::root().key("list"),
                Value::Text("123".graphemes(true).map(|s| s.to_owned()).collect()),
            ))?;
            let cursor = d
                .cursor_to_path(&Path::root().key("list").index(1))
                .unwrap();
            d.add_change(LocalChange::set(Path::root().key("cursor"), cursor))?;
            Ok(())
        })
        .unwrap()
        .1
        .unwrap();
    let mut backend = Backend::new();
    backend
        .apply_changes(vec![change.try_into().unwrap()])
        .unwrap();

    let mut backend2 = Backend::new();
    backend2
        .apply_changes(backend.get_changes(&[]).into_iter().cloned().collect())
        .unwrap();
    let mut frontend2 = Frontend::new();
    frontend2
        .apply_patch(backend2.get_patch().unwrap())
        .unwrap();
    let index_value = frontend2.get_value(&Path::root().key("cursor")).unwrap();
    if let Value::Primitive(Primitive::Cursor(c)) = index_value {
        assert_eq!(c.index, 1)
    } else {
        panic!("value was not a cursor");
    }
}

#[test]
fn test_do_not_allow_index_past_end_of_list() {
    let mut frontend = Frontend::new();
    frontend
        .change::<_, _, InvalidChangeRequest>(None, |d| {
            d.add_change(LocalChange::set(
                Path::root().key("list"),
                Value::Text("123".graphemes(true).map(|s| s.to_owned()).collect()),
            ))?;
            let cursor = d.cursor_to_path(&Path::root().key("list").index(10));
            assert_eq!(cursor, None);
            Ok(())
        })
        .unwrap();
}

// #[test]
// fn test_updates_cursor_during_change_function() {
//     let mut frontend = Frontend::new();
//     frontend
//         .change::<_, _, InvalidChangeRequest>(None, |d| {
//             d.add_change(LocalChange::set(
//                 Path::root().key("list"),
//                 Value::Text("123".graphemes(true).map(|s| s.to_owned()).collect()),
//             ))?;
//             let cursor = d
//                 .cursor_to_path(&Path::root().key("list").index(1))
//                 .unwrap();
//             d.add_change(LocalChange::set(Path::root().key("cursor"), cursor))?;
//             let cursor_the_second = d.value_at_path(&Path::root().key("cursor"));
//             if let Some(Value::Primitive(Primitive::Cursor(c))) = cursor_the_second {
//                 assert_eq!(c.index, 1);
//             } else {
//                 panic!("Cursor the second not found");
//             }

//             d.add_change(LocalChange::insert(
//                 Path::root().key("list").index(0),
//                 Value::Primitive(Primitive::Str("0".to_string())),
//             ))?;
//             let cursor_the_third = d.value_at_path(&Path::root().key("cursor"));
//             if let Some(Value::Primitive(Primitive::Cursor(c))) = cursor_the_third {
//                 assert_eq!(c.index, 2);
//             } else {
//                 panic!("Cursor the third not found");
//             }
//             Ok(())
//         })
//         .unwrap();
// }

#[test]
fn test_set_cursor_to_new_element_in_diff() {
    let mut frontend = Frontend::new();
    let actor = frontend.actor_id.clone();
    let patch1 = amp::Patch {
        actor: Some(actor.clone()),
        deps: Vec::new(),
        seq: Some(1),
        clock: hashmap! {actor.clone() => 1},
        max_op: 3,
        pending_changes: 0,
        diffs: RootDiff {
            props: hashmap! {
                "list".to_string() => hashmap!{
                    actor.op_id_at(1) => amp::Diff::Seq(amp::SeqDiff{
                        object_id: actor.op_id_at(1).into(),
                        seq_type: amp::SequenceType::List,
                        edits: vec![
                            amp::DiffEdit::SingleElementInsert{
                                index: 0,
                                elem_id: actor.op_id_at(2).into(),
                                op_id: actor.op_id_at(2),
                                value: amp::Diff::Value("one".into()),
                            },
                            amp::DiffEdit::SingleElementInsert{
                                index: 1,
                                elem_id: actor.op_id_at(3).into(),
                                op_id: actor.op_id_at(3),
                                value: amp::Diff::Value("two".into()),
                            },
                        ],
                    }),
                },
                "cursor".to_string() => hashmap!{
                    actor.op_id_at(4) => amp::Diff::Cursor(amp::CursorDiff{
                        elem_id: actor.op_id_at(3),
                        index: 1,
                        object_id: actor.op_id_at(1).into(),
                    })
                },
            },
        },
    };

    frontend.apply_patch(patch1).unwrap();
    let patch2 = amp::Patch {
        actor: Some(actor.clone()),
        deps: Vec::new(),
        seq: Some(2),
        clock: hashmap! {actor.clone() => 2},
        max_op: 5,
        pending_changes: 0,
        diffs: RootDiff {
            props: hashmap! {
                "cursor".to_string() => hashmap!{
                    actor.op_id_at(4) => amp::Diff::Cursor(amp::CursorDiff{
                        elem_id: actor.op_id_at(2),
                        index: 0,
                        object_id: actor.op_id_at(1).into(),
                    })
                }
            },
        },
    };
    frontend.apply_patch(patch2).unwrap();

    frontend
        .change::<_, _, InvalidChangeRequest>(None, |doc| {
            doc.add_change(LocalChange::insert(
                Path::root().key("list").index(1),
                "three".into(),
            ))?;
            let cursor = doc.value_at_path(&Path::root().key("cursor")).unwrap();
            match cursor {
                Value::Primitive(Primitive::Cursor(c)) => assert_eq!(c.index, 0),
                _ => panic!("Cursor value was not a cursor"),
            }
            Ok(())
        })
        .unwrap();
}

// #[test]
// fn test_set_cursor_to_new_element_in_local_change() {
//     let mut frontend = Frontend::new();
//     frontend
//         .change::<_, _, InvalidChangeRequest>(None, |d| {
//             d.add_change(LocalChange::set(
//                 Path::root().key("list"),
//                 Value::Text("123".graphemes(true).map(|s| s.to_owned()).collect()),
//             ))?;
//             let cursor = d
//                 .cursor_to_path(&Path::root().key("list").index(1))
//                 .unwrap();
//             d.add_change(LocalChange::set(Path::root().key("cursor"), cursor))?;
//             let cursor_the_second = d.value_at_path(&Path::root().key("cursor"));
//             if let Some(Value::Primitive(Primitive::Cursor(c))) = cursor_the_second {
//                 assert_eq!(c.index, 1);
//             } else {
//                 panic!("Cursor the second not found");
//             }

//             d.add_change(LocalChange::insert(
//                 Path::root().key("list").index(0),
//                 Value::Primitive(Primitive::Str("0".to_string())),
//             ))?;
//             d.add_change(LocalChange::insert(
//                 Path::root().key("list").index(0),
//                 Value::Primitive(Primitive::Str("1".to_string())),
//             ))?;
//             let cursor = d
//                 .cursor_to_path(&Path::root().key("list").index(2))
//                 .unwrap();
//             d.add_change(LocalChange::set(Path::root().key("cursor"), cursor))?;
//             d.add_change(LocalChange::insert(
//                 Path::root().key("list").index(4),
//                 "2".into(),
//             ))?;
//             let cursor_the_third = d.value_at_path(&Path::root().key("cursor"));
//             if let Some(Value::Primitive(Primitive::Cursor(c))) = cursor_the_third {
//                 assert_eq!(c.index, 3);
//             } else {
//                 panic!("Cursor the third not found");
//             }
//             Ok(())
//         })
//         .unwrap();
// }
#[test]
fn test_delete_cursor_and_adding_again() {
    let mut frontend = Frontend::new();
    frontend
        .change::<_, _, InvalidChangeRequest>(None, |d| {
            d.add_change(LocalChange::set(
                Path::root().key("list"),
                Value::Text("123".graphemes(true).map(|s| s.to_owned()).collect()),
            ))?;
            let cursor = d
                .cursor_to_path(&Path::root().key("list").index(1))
                .unwrap();
            d.add_change(LocalChange::set(Path::root().key("cursor"), cursor.clone()))?;
            d.add_change(LocalChange::delete(Path::root().key("cursor")))?;
            d.add_change(LocalChange::set(Path::root().key("cursor"), cursor))?;

            let cursor_value = d.value_at_path(&Path::root().key("cursor"));
            if let Some(Value::Primitive(Primitive::Cursor(c))) = cursor_value {
                assert_eq!(c.index, 1);
            } else {
                panic!("Cursor the third not found");
            }
            Ok(())
        })
        .unwrap();
}

//TODO test removing a cursors
