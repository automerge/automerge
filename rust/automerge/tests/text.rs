use std::str::FromStr;

use automerge::{
    marks::Mark, transaction::Transactable, ActorId, AutoCommit, ObjType, PatchAction, ReadDoc,
    ScalarValue, ROOT,
};
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
