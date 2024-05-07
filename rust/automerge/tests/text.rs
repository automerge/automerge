use std::str::FromStr;

use automerge::{
    hydrate_list, hydrate_map,
    iter::Span,
    marks::{ExpandMark, Mark},
    op_tree::B,
    transaction::Transactable,
    ActorId, AutoCommit, ObjType, Patch, PatchAction, ReadDoc, ScalarValue, ROOT,
};
use proptest::strategy::Strategy;
use test_log::test;

#[test]
fn simple_update_text() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "Hello, world!").unwrap();

    let mut doc2 = doc.fork();
    doc2.update_text(&text, "Goodbye, world!").unwrap();

    doc.update_text(&text, "Hello, friends!").unwrap();

    doc.merge(&mut doc2).unwrap();

    assert_eq!(doc.text(&text).unwrap(), "Goodbye, friends!");
}

#[test]
fn update_text_big_ole_graphemes() {
    let actor1 = ActorId::from_str("aaaaaa").unwrap();
    let actor2 = ActorId::from_str("bbbbbb").unwrap();
    let mut doc = AutoCommit::new().with_actor(actor1);
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();

    // <200d> is a "zero-width joiner" which is used to combine multiple graphemes into one.
    // combining man+woman+boy should render as a single emoji of a familry of three
    doc.splice_text(&text, 0, 0, "left👨‍👩‍👦right").unwrap();

    let mut doc2 = doc.fork().with_actor(actor2);
    // man, woman, girl - a different family of three
    doc2.update_text(&text, "left👨‍👩‍👧right").unwrap();

    // man, woman, boy, boy - a family of four
    doc.update_text(&text, "left👨‍👩‍👦‍👦right").unwrap();

    doc.merge(&mut doc2).unwrap();
    // should render as a family of three followed by a family of four
    assert_eq!(doc.text(&text).unwrap(), "left👨‍👩‍👧👨‍👩‍👦‍👦right");
}

macro_rules! assert_marks {
    ($marks:expr, $expected:expr) => {
        let marks = $marks
            .iter()
            .collect::<std::collections::HashMap<&str, &ScalarValue>>();
        let expected = $expected
            .into_iter()
            .map(|(name, value)| (name, ScalarValue::from(value)))
            .collect::<std::collections::HashMap<&str, _>>();
        assert_eq!(
            marks.len(),
            $expected.len(),
            "expected {} marks, got {}",
            $expected.len(),
            marks.len()
        );
        let mut marks_equal = true;
        for (mark_name, mark_value) in &expected {
            if marks.get(*mark_name) != Some(&&mark_value) {
                marks_equal = false;
                break;
            }
        }
        if !marks_equal {
            panic!("expected marks {:?}, got {:?}", expected, marks);
        }
    };
}

#[test]
fn incremental_splice_patches_include_marks() {
    // Test for a splice which triggers the can_shortcut_search optimization
    // failing to include marks in the incremental splice patch

    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "12345").unwrap();

    doc.mark(
        &text,
        Mark::new("strong".to_string(), true, 1, 2),
        automerge::marks::ExpandMark::Both,
    )
    .unwrap();
    doc.update_diff_cursor();

    // Do the first splice
    doc.splice_text(&text, 1, 0, "-").unwrap();
    let patches = doc.diff_incremental();
    assert_eq!(patches.len(), 1);

    let PatchAction::SpliceText {
        index,
        value,
        marks,
    } = patches[0].action.clone()
    else {
        panic!("expected a splice patch, got {:?}", patches[0].action);
    };
    assert_eq!(index, 1);
    assert_eq!(value.make_string(), "-");
    let Some(marks) = marks else {
        panic!("expected marks, got {:?}", patches[0].action);
    };
    assert_marks!(marks, [("strong", true)]);

    // Do the second splice
    doc.splice_text(&text, 2, 0, "-").unwrap();
    let patches = doc.diff_incremental();
    assert_eq!(patches.len(), 1);
    let patch = patches[0].clone();

    let PatchAction::SpliceText {
        index,
        value,
        marks,
    } = patch.action.clone()
    else {
        panic!("expected a splice patch, got {:?}", patch.action);
    };
    assert_eq!(index, 2);
    assert_eq!(value.make_string(), "-");
    let Some(marks) = marks else {
        panic!("expected patch with marks, got {:?}", patch.action);
    };
    assert_marks!(marks, [("strong", true)]);
}

#[test]
fn mark_created_after_insertion() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "12345").unwrap();

    doc.mark(
        &text,
        Mark::new("strong".to_string(), true, 1, 2),
        automerge::marks::ExpandMark::Both,
    )
    .unwrap();
    doc.mark(
        &text,
        Mark::new("strong".to_string(), true, 3, 4),
        automerge::marks::ExpandMark::Both,
    )
    .unwrap();

    let spans = doc.spans(&text).unwrap().collect::<Vec<_>>();
    println!("{:?}", spans);
}

#[test]
fn local_patches_created_for_marks() {
    let mut doc = AutoCommit::new();
    let text = doc
        .put_object(automerge::ROOT, "text", ObjType::Text)
        .unwrap();
    doc.splice_text(&text, 0, 0, "the quick fox jumps over the lazy dog")
        .unwrap();
    doc.mark(
        &text,
        Mark::new("bold".to_string(), true, 0, 37),
        ExpandMark::Both,
    )
    .unwrap();
    doc.mark(
        &text,
        Mark::new("italic".to_string(), true, 4, 19),
        ExpandMark::Both,
    )
    .unwrap();
    let id = "somerandomcommentid".to_string();
    doc.mark(
        &text,
        Mark::new(
            format!("comment:{}", id),
            "foxes are my favorite animal!".to_string(),
            10,
            13,
        ),
        ExpandMark::Both,
    )
    .unwrap();
    doc.commit().unwrap();
    let patches = doc.diff_incremental();

    let expected_patches = vec![
        Patch {
            obj: automerge::ROOT,
            path: vec![],
            action: PatchAction::PutMap {
                key: "text".to_string(),
                value: (
                    automerge::Value::Object(automerge::ObjType::Text),
                    text.clone(),
                ),
                conflict: false,
            },
        },
        Patch {
            obj: text.clone(),
            path: vec![(automerge::ROOT, "text".into())],
            action: PatchAction::SpliceText {
                index: 0,
                value: "the ".into(),
                marks: Some(
                    vec![("bold".to_string(), ScalarValue::from(true))]
                        .into_iter()
                        .collect(),
                ),
            },
        },
        Patch {
            obj: text.clone(),
            path: vec![(automerge::ROOT, "text".into())],
            action: PatchAction::SpliceText {
                index: 4,
                value: "quick ".into(),
                marks: Some(
                    vec![
                        ("bold".to_string(), ScalarValue::from(true)),
                        ("italic".to_string(), ScalarValue::from(true)),
                    ]
                    .into_iter()
                    .collect(),
                ),
            },
        },
        Patch {
            obj: text.clone(),
            path: vec![(automerge::ROOT, "text".into())],
            action: PatchAction::SpliceText {
                index: 10,
                value: "fox".into(),
                marks: Some(
                    vec![
                        ("bold".to_string(), ScalarValue::from(true)),
                        (
                            format!("comment:{}", id),
                            ScalarValue::from("foxes are my favorite animal!".to_string()),
                        ),
                        ("italic".to_string(), ScalarValue::from(true)),
                    ]
                    .into_iter()
                    .collect(),
                ),
            },
        },
        Patch {
            obj: text.clone(),
            path: vec![(automerge::ROOT, "text".into())],
            action: PatchAction::SpliceText {
                index: 13,
                value: " jumps".into(),
                marks: Some(
                    vec![
                        ("bold".to_string(), ScalarValue::from(true)),
                        ("italic".to_string(), ScalarValue::from(true)),
                    ]
                    .into_iter()
                    .collect(),
                ),
            },
        },
        Patch {
            obj: text.clone(),
            path: vec![(automerge::ROOT, "text".into())],
            action: PatchAction::SpliceText {
                index: 19,
                value: " over the lazy dog".into(),
                marks: Some(
                    vec![("bold".to_string(), ScalarValue::from(true))]
                        .into_iter()
                        .collect(),
                ),
            },
        },
    ];

    assert_eq!(patches, expected_patches);
}

#[test]
fn spans_are_consolidated_in_the_presence_of_zero_length_spans() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "1234").unwrap();

    doc.mark(
        &text,
        Mark::new("strong".to_string(), true, 1, 1),
        automerge::marks::ExpandMark::Both,
    )
    .unwrap();

    doc.mark(
        &text,
        Mark::new("strong".to_string(), true, 2, 2),
        automerge::marks::ExpandMark::Both,
    )
    .unwrap();

    let spans = doc.spans(&text).unwrap().collect::<Vec<_>>();
    println!("{:?}", spans);
    assert!(marks_are_consolidated(&spans));
}

#[test]
fn empty_marks_before_block_marker_dont_repeat_text() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.split_block(&text, 0).unwrap();
    doc.split_block(&text, 0).unwrap();
    doc.mark(
        &text,
        Mark::new("strong".to_string(), ScalarValue::from(true), 1, 1),
        automerge::marks::ExpandMark::Both,
    )
    .unwrap();
    doc.splice_text(&text, 2, 0, "a").unwrap();

    let spans = doc.spans(&text).unwrap().collect::<Vec<_>>();

    assert_eq!(
        spans,
        vec![
            Span::Block(hydrate_map! {}),
            Span::Block(hydrate_map! {}),
            Span::Text("a".to_string(), None),
        ]
    );
}

#[test]
fn insertions_after_noexpand_spans_are_not_marked() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    let block1 = doc.split_block(&text, 0).unwrap();
    doc.update_object(
        &block1,
        &hydrate_map! {
            "type" => "heading",
            "parents" => hydrate_list![],
            "attrs" => hydrate_map!{},
        }
        .into(),
    )
    .unwrap();
    doc.splice_text(&text, 1, 0, "Heading").unwrap();
    let block2 = doc.split_block(&text, 8).unwrap();
    doc.update_object(
        &block2,
        &hydrate_map! {
            "type" => "paragraph",
            "parents" => hydrate_list![],
            "attrs" => hydrate_map!{},
        }
        .into(),
    )
    .unwrap();
    doc.splice_text(&text, 9, 0, "a").unwrap();
    doc.mark(
        &text,
        Mark::new("strong".to_string(), ScalarValue::from(true), 9, 9),
        automerge::marks::ExpandMark::None,
    )
    .unwrap();

    let spans = doc.spans(&text).unwrap();
    let mut new_blocks = spans
        .map(|s| match s {
            Span::Text(s, _) => automerge::BlockOrText::Text(s.into()),
            Span::Block(m) => automerge::BlockOrText::Block(m),
        })
        .collect::<Vec<_>>();
    new_blocks.push(automerge::BlockOrText::Block(hydrate_map! {
        "type" => "paragraph",
        "parents" => hydrate_list![],
        "attrs" => hydrate_map!{},
    }));

    doc.update_spans(&text, new_blocks).unwrap();

    let marks = doc.marks(&text).unwrap();
    println!("marks: {:?}", marks);

    let heads_before = doc.get_heads();
    doc.splice_text(&text, 11, 0, "a").unwrap();
    let heads_after = doc.get_heads();

    let marks = doc.marks(&text).unwrap();
    println!("marks: {:?}", marks);

    let patches = doc.diff(&heads_before, &heads_after);
    assert_eq!(patches.len(), 1);

    let Patch {
        action: PatchAction::SpliceText { marks, .. },
        ..
    } = &patches[0]
    else {
        panic!("expected single splice patch, got: {:?}", patches);
    };
    assert_eq!(
        marks, &None,
        "expected marks to be none, got {:?}",
        patches[0]
    );
}

#[test]
fn marks_which_cross_optree_boundaries_are_not_double_counted_in_splice_patches() {
    // This test exposese an issue where marks suddenly appeared on characters in a document after
    // a mark which had ended much earlier in the document.The problem was caused by an interaction
    // between the indexes on optree pages and a bug in the way the insert query keeps track of
    // marks as it searches for its target.
    //
    // As the insert query traverses the op tree it keeps track of a HashMap<OpId, MarkData>. The
    // OpId is the ID of the op which created the mark and the MarkData is the data associated with
    // that mark. Whenever the query encounters a BeginMark operation it adds it to the map and
    // whenever it encounters an EndMark operation it removes it. Once the query has found its
    // target the map can be evaluated to determine the marks active at the insertion point, which
    // are then passed to the patch.
    //
    // The OpTree nodes have indexes on them which the insert query uses to skip sections of the
    // tree which definitely don't contain the target op. The primary useful thing the index
    // contains is the visible length - the number of visible ops - in the node and its children.
    // This allows the insert query to skip past the entire node if the target is beyond the
    // visible length of the index.
    //
    // In order to account for the fact that the skipped nodes may contain begin or end mark
    // operations the indexes also have a `mark_begin: HashMap<OpId, MarkData>` and a `mark_end:
    // Vec<OpId>` field which keep track of the begin and end operations in the node and its
    // children. When the insert query skips a node it also updates its map of active marks by
    // removing the marks which have an end operation in the node and adding the marks which have a
    // begin operation.
    //
    // The bug was that the removal code was doing this:
    //
    //     for id in index.mark_end.iter() {
    //         marks.remove(&id);
    //     }
    //
    // Recall that `marks` is a HashMap<OpId, MarkData> which maps the ID of the op which created
    // the mark, but the `mark_end` field is a Vec<OpId> which contains the ID of the end mark. End
    // mark operations are always the operation immediately following the begin mark operation, so
    // we can calculate the op we actually need to remove by calling `id.prev()`. Like so:
    //
    //    for id in index.mark_end.iter() {
    //        marks.remove(&id.prev());
    //    }
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    // insert enough text that we cover two pages of the op tree
    let one_page = "a".repeat(B * 2);
    doc.splice_text(&text, doc.length(&text), 0, &one_page)
        .unwrap();
    // Add a mark that starts in one page and ends in the next
    doc.mark(
        &text,
        Mark::new("strong".to_string(), ScalarValue::from(true), B - 1, B + 1),
        automerge::marks::ExpandMark::None,
    )
    .unwrap();

    // Now add characters. Eventually the end mark will be in an op tree page which is skipped by
    // the insert query, exposing the faulty logic.
    for _ in 0..100 {
        // This is necessary to clear the `Index::last_insert cache, which otherwise just uses the
        // marks of the last inserted character when generating the patch.
        doc.split_block(&text, doc.length(&text)).unwrap();
        // This is necessary because otherwise the patches can be generated from scratch, which
        // will mean we don't use the logic in the insert query
        doc.update_diff_cursor();
        let heads_before = doc.get_heads();
        doc.splice_text(&text, doc.length(&text), 0, "a").unwrap();
        let heads_after = doc.get_heads();

        let patches = doc.diff(&heads_before, &heads_after);
        assert_eq!(patches.len(), 1);

        let Patch {
            action: PatchAction::SpliceText { marks, .. },
            ..
        } = &patches[0]
        else {
            panic!("expected single splice patch, got: {:?}", patches);
        };
        // If the end mark is not removed correctly then we will see unexpected marks
        assert_eq!(
            marks, &None,
            "expected marks to be none, got {:?}",
            patches[0]
        );
    }
}

#[test]
fn noexpand_marks_at_the_end_of_text_should_not_emit_marked_patches_on_following_insertions() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, doc.length(&text), 0, "Hello world")
        .unwrap();
    let mark_start = doc.length(&text) - 1;
    let mark_end = doc.length(&text);
    doc.mark(
        &text,
        Mark::new(
            "strong".to_string(),
            ScalarValue::from(true),
            mark_start,
            mark_end,
        ),
        automerge::marks::ExpandMark::None,
    )
    .unwrap();

    doc.update_diff_cursor();
    let heads_before = doc.get_heads();
    println!("doing splice");
    doc.splice_text(&text, doc.length(&text), 0, "a").unwrap();
    println!("done splice");
    let heads_after = doc.get_heads();

    let patches = doc.diff(&heads_before, &heads_after);
    assert_eq!(patches.len(), 1);

    let Patch {
        action: PatchAction::SpliceText { marks, .. },
        ..
    } = &patches[0]
    else {
        panic!("expected single splice patch, got: {:?}", patches);
    };
    assert_eq!(marks, &None,);
}

#[test]
fn expand_marks_are_reported_in_patches() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "aaabbbccc").unwrap();
    doc.mark(
        &text,
        Mark::new("strong".to_string(), ScalarValue::from(true), 3, 6),
        automerge::marks::ExpandMark::Both,
    )
    .unwrap();

    doc.update_diff_cursor();
    let mut patches = Vec::new();

    doc.splice_text(&text, 6, 0, "<").unwrap();
    patches.extend(doc.diff_incremental());

    doc.splice_text(&text, 3, 0, ">").unwrap();
    patches.extend(doc.diff_incremental());

    assert_eq!(patches.len(), 2);

    let Patch {
        action:
            PatchAction::SpliceText {
                marks,
                index,
                value,
                ..
            },
        ..
    } = &patches[0]
    else {
        panic!("expected a patch, got: {:?}", patches);
    };
    assert_eq!(*index, 6);
    assert_eq!(value.make_string(), "<".to_string());
    assert_eq!(
        marks,
        &Some(
            vec![("strong".to_string(), ScalarValue::from(true))]
                .into_iter()
                .collect()
        )
    );

    let Patch {
        action:
            PatchAction::SpliceText {
                marks,
                index,
                value,
                ..
            },
        ..
    } = &patches[1]
    else {
        panic!("expected a patch, got: {:?}", patches);
    };
    assert_eq!(*index, 3);
    assert_eq!(value.make_string(), ">".to_string());
    assert_eq!(
        marks,
        &Some(
            vec![("strong".to_string(), ScalarValue::from(true))]
                .into_iter()
                .collect()
        )
    );
}

#[test]
fn test_remote_patches_for_marks_with_expand_after() {
    let mut doc_a = AutoCommit::new();
    let text = doc_a.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc_a.splice_text(&text, 0, 0, "fox").unwrap();
    doc_a
        .mark(
            &text,
            Mark::new("strong".to_string(), ScalarValue::from(true), 0, 3),
            automerge::marks::ExpandMark::After,
        )
        .unwrap();

    let mut doc_b = doc_a.fork();

    let heads_before_a = doc_a.get_heads();
    doc_a.splice_text(&text, 3, 0, "a").unwrap();
    let heads_after_a = doc_a.get_heads();

    doc_b.update_diff_cursor();
    let heads_before_b = doc_b.get_heads();
    println!("doing merge");
    doc_b.merge(&mut doc_a).unwrap();
    println!("done merge");
    let heads_after_b = doc_b.get_heads();

    let patches_a = doc_a.diff(&heads_before_a, &heads_after_a);
    let patches_b = doc_b.diff(&heads_before_b, &heads_after_b);

    #[cfg(feature = "optree-visualisation")]
    {
        println!("--------------------------------");
        println!("Doc A");
        println!("{}", doc_a.visualise_optree(None));
        println!("--------------------------------");
        println!("Doc B");
        println!("{}", doc_b.visualise_optree(None));
    }

    assert_eq!(patches_a, patches_b);
}

proptest::proptest! {
    #[test]
    fn marks_are_okay(scenario in arb_scenario()) {
        let mut doc = AutoCommit::new();
        let text = doc.put_object(ROOT, "text", ObjType::Text).unwrap();
        let mut expected_chars = String::new();
        for action in &scenario {
            match action {
                Action::Insert(index, value) => {
                    doc.splice_text(&text, *index, 0, value).unwrap();
                    expected_chars.insert_str(*index, value);
                }
                Action::Delete(index, len) => {
                    doc.splice_text(&text, *index, *len as isize, "").unwrap();
                    expected_chars.drain(*index..(*index + *len));
                }
                Action::SplitBlock(index) => {
                    doc.split_block(&text, *index).unwrap();
                    expected_chars.insert(*index, '\n');
                }
                Action::AddMark(index, len, name, value) => {
                    doc.mark(&text, Mark::new(name.clone(), value.clone(), *index, index + len), automerge::marks::ExpandMark::Both).unwrap();
                }
            }
        }
        let spans = doc.spans(&text).unwrap().collect::<Vec<_>>();
        if !marks_are_consolidated(&spans) {
            println!("scenario: {:?}", scenario);
            println!("spans: {:?}", spans);
            panic!("marks are not consolidated");
        }

        let span_chars = spans.iter().map(|span| match span {
            Span::Text(text, _) => text.clone(),
            Span::Block(_) => "\n".to_string(),
        }).collect::<String>();
        if !span_chars.chars().eq(expected_chars.chars()) {
            println!("scenario: {:?}", scenario);
            println!("expected: {:?}", expected_chars);
            println!("actual: {:?}", span_chars);
            panic!("expected text did not match span text");
        }

        // replace unicode object replacement which automerge inserts for block markers wwith a newline
        let actual_chars = doc.text(&text).unwrap().replace('\u{fffc}', "\n");
        if !actual_chars.chars().eq(expected_chars.chars()) {
            println!("scenario: {:?}", scenario);
            println!("expected: {:?}", expected_chars);
            println!("actual: {:?}", actual_chars);
            panic!("expected text did not match actual text");
        }
    }
}

fn marks_are_consolidated(spans: &Vec<Span>) -> bool {
    let mut last_marks = None;
    for span in spans {
        match span {
            Span::Text(_, marks) => {
                if let Some(last_marks) = last_marks {
                    if marks == last_marks {
                        return false;
                    }
                }
                last_marks = Some(marks);
            }
            _ => {
                last_marks = None;
            }
        }
    }
    true
}

#[derive(Debug, Clone)]
enum Action {
    Insert(usize, String),
    Delete(usize, usize),
    SplitBlock(usize),
    AddMark(usize, usize, String, ScalarValue),
}

fn arb_insert(text: &str) -> impl proptest::strategy::Strategy<Value = Action> {
    (0..=text.len(), "[a-zA-Z]{1,10}").prop_map(|(index, value)| Action::Insert(index, value))
}

fn arb_delete(text: &str) -> impl proptest::strategy::Strategy<Value = Action> {
    let len = text.len();
    if len == 1 {
        return proptest::strategy::Just(Action::Delete(0, 1)).boxed();
    }
    (1..len)
        .prop_flat_map(move |delete_len| {
            (0..(len - delete_len)).prop_map(move |index| Action::Delete(index, delete_len))
        })
        .boxed()
}

fn arb_split_block(text: &str) -> impl proptest::strategy::Strategy<Value = Action> {
    (0..=text.len()).prop_map(Action::SplitBlock)
}

fn arb_add_mark(text: &str) -> impl proptest::strategy::Strategy<Value = Action> {
    let text_len = text.len();
    (0..text_len).prop_flat_map(move |index| {
        (0..(text_len - index)).prop_flat_map(move |len| {
            ("[a-zA-Z]{1,10}", "[a-zA-Z]{1,10}").prop_map(move |(name, value)| {
                Action::AddMark(index, len, name, ScalarValue::from(value))
            })
        })
    })
}

fn arb_action(text: &str) -> impl proptest::strategy::Strategy<Value = Action> {
    if text.is_empty() {
        return arb_insert(text).boxed();
    }
    proptest::prop_oneof![
        arb_insert(text),
        arb_delete(text),
        arb_split_block(text),
        arb_add_mark(text),
    ]
    .boxed()
}

fn arb_scenario() -> impl proptest::strategy::Strategy<Value = Vec<Action>> {
    fn pump(
        state: String,
        actions_so_far: Vec<Action>,
        max_actions: usize,
    ) -> impl proptest::strategy::Strategy<Value = Vec<Action>> {
        if actions_so_far.len() >= max_actions {
            return proptest::strategy::Just(actions_so_far).boxed();
        }
        arb_action(&state)
            .prop_flat_map(move |action| {
                let mut state = state.clone();
                let mut actions_so_far = actions_so_far.clone();
                actions_so_far.push(action.clone());
                match action {
                    Action::Insert(index, value) => {
                        state.insert_str(index, &value);
                    }
                    Action::Delete(index, len) => {
                        state.drain(index..index + len);
                    }
                    Action::SplitBlock(index) => {
                        state.insert(index, '\n');
                    }
                    Action::AddMark(..) => {}
                }
                pump(state, actions_so_far, max_actions)
            })
            .boxed()
    }
    (0_usize..10)
        .prop_flat_map(move |max_actions| pump(String::new(), Vec::new(), max_actions).boxed())
}
