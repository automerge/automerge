use automerge::{
    marks::{ExpandMark, Mark},
    transaction::Transactable,
    AutoCommit, Automerge, ObjType, ReadDoc, ScalarValue, ROOT,
};
// set up logging for all the tests
use test_log::test;

#[test]
fn marks() {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();

    let text_id = tx.put_object(&ROOT, "text", ObjType::Text).unwrap();

    tx.splice_text(&text_id, 0, 0, "hello world").unwrap();

    let mark = Mark::new("bold".to_string(), true, 0, "hello".len());
    tx.mark(&text_id, mark, ExpandMark::Both).unwrap();

    // add " cool" (it will be bold because ExpandMark::Both)
    tx.splice_text(&text_id, "hello".len(), 0, " cool").unwrap();

    // unbold "hello"
    tx.unmark(&text_id, "bold", 0, "hello".len(), ExpandMark::Before)
        .unwrap();

    // insert "why " before hello.
    tx.splice_text(&text_id, 0, 0, "why ").unwrap();

    let marks = tx.marks(&text_id).unwrap();

    assert_eq!(marks.len(), 1);
    assert_eq!(marks[0].start, 9);
    assert_eq!(marks[0].end, 14);
    assert_eq!(marks[0].name(), "bold");
    assert_eq!(marks[0].value(), &ScalarValue::from(true));
}

#[test]
fn deleted_mark_added_to_new_text() {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    let text_id = tx.put_object(&ROOT, "text", ObjType::Text).unwrap();
    tx.splice_text(&text_id, 0, 0, "hello world").unwrap();
    let mark = Mark::new("bold".to_string(), true, 2, 8);
    tx.mark(&text_id, mark, ExpandMark::After).unwrap();
    let mark = Mark::new("link".to_string(), true, 3, 6);
    tx.mark(&text_id, mark, ExpandMark::None).unwrap();

    tx.splice_text(&text_id, 1, 10, "").unwrap(); // 'h'
    tx.splice_text(&text_id, 0, 0, "a").unwrap(); // 'ah'
    tx.splice_text(&text_id, 2, 0, "a").unwrap(); // 'ah<bold>a</bold>'
    tx.commit();

    assert_eq!(doc.marks(&text_id).unwrap().len(), 0);
}

#[test]
fn marks_at_beginning_of_a_string() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(&ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "aaabbbccc").unwrap();
    let mark = Mark::new("bold".to_string(), true, 0, 3);
    doc.mark(&text, mark, ExpandMark::None).unwrap();
    let marks = doc.marks(&text).unwrap();
    assert_eq!(marks.len(), 1);
    assert_eq!(marks[0].start, 0);
    assert_eq!(marks[0].end, 3);
    assert_eq!(marks[0].name(), "bold");
    assert_eq!(marks[0].value(), &ScalarValue::from(true));

    let mut doc2 = doc.fork();
    doc2.splice_text(&text, 0, 0, "A").unwrap();
    doc2.splice_text(&text, 4, 0, "B").unwrap();
    doc.merge(&mut doc2).unwrap();
    let marks = doc.marks(&text).unwrap();
    assert_eq!(marks.len(), 1);
    assert_eq!(marks[0].start, 1);
    assert_eq!(marks[0].end, 4);
    assert_eq!(marks[0].name(), "bold");
    assert_eq!(marks[0].value(), &ScalarValue::from(true));
}

#[test]
fn expand_marks_with_deleted_ends() {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(&ROOT, "text", ObjType::Text).unwrap();
    doc.splice_text(&text, 0, 0, "aaabbbccc").unwrap();
    let mark = Mark::new("bold".to_string(), true, 3, 6);
    doc.mark(&text, mark, ExpandMark::Both).unwrap();
    println!("initial marks");
    #[cfg(feature = "optree-visualisation")]
    {
        doc.document().visualise_marks(&text);
    }
    let marks = doc.marks(&text).unwrap();
    assert_eq!(marks.len(), 1);
    assert_eq!(marks[0].start, 3);
    assert_eq!(marks[0].end, 6);
    assert_eq!(marks[0].name(), "bold");
    assert_eq!(marks[0].value(), &ScalarValue::from(true));

    doc.delete(&text, 5).unwrap();
    doc.delete(&text, 5).unwrap();
    doc.delete(&text, 2).unwrap();
    doc.delete(&text, 2).unwrap();
    let marks = doc.marks(&text).unwrap();
    assert_eq!(marks.len(), 1);
    assert_eq!(marks[0].start, 2);
    assert_eq!(marks[0].end, 3);
    assert_eq!(marks[0].name(), "bold");
    assert_eq!(marks[0].value(), &ScalarValue::from(true));

    println!("after deleting the ends of the mark");
    #[cfg(feature = "optree-visualisation")]
    {
        doc.document().visualise_marks(&text);
    }
    doc.splice_text(&text, 3, 0, "A").unwrap();
    println!("insert at index 3");
    #[cfg(feature = "optree-visualisation")]
    {
        doc.document().visualise_marks(&text);
    }
    doc.splice_text(&text, 2, 0, "A").unwrap();
    println!("insert at index 2");
    #[cfg(feature = "optree-visualisation")]
    {
        doc.document().visualise_marks(&text);
    }
    let marks = doc.marks(&text).unwrap();
    assert_eq!(marks.len(), 1);
    assert_eq!(marks[0].start, 2);
    assert_eq!(marks[0].end, 5);
    assert_eq!(marks[0].name(), "bold");
    assert_eq!(marks[0].value(), &ScalarValue::from(true));
}
