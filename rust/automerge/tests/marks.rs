use automerge::{Automerge, transaction::Transactable, ObjType, ROOT, marks::{Mark, ExpandMark}, ReadDoc, ScalarValue};
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
