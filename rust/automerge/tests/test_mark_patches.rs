use automerge::{
    marks::{ExpandMark, Mark},
    transaction::Transactable,
    ObjType, PatchAction, ROOT,
};
use test_log::test;

#[test]
fn mark_patches_at_end_of_text() {
    let mut doc1 = automerge::AutoCommit::new();
    let text = doc1.put_object(ROOT, "text", ObjType::Text).unwrap();
    doc1.splice_text(&text, 0, 0, "sample").unwrap();
    let heads_before_mark = doc1.get_heads();
    let mut doc2 = automerge::AutoCommit::load(&doc1.save()).unwrap();

    doc1.mark(
        &text,
        Mark::new("bold".to_string(), true, 5, 6),
        ExpandMark::After,
    )
    .unwrap();

    // Pop the patches
    doc2.diff_incremental();
    let changes_after_mark = doc1.save_after(&heads_before_mark);
    doc2.load_incremental(&changes_after_mark).unwrap();

    let mut patches = doc2.diff_incremental();
    assert_eq!(patches.len(), 1, "no patches generated");
    let patch = patches.pop().unwrap();
    assert_eq!(patch.obj, text);
    assert_eq!(patch.path, vec![(ROOT, "text".into())]);
    let PatchAction::Mark { mut marks } = patch.action else {
        panic!("Expected a mark patch, got {:?}", patch.action);
    };
    assert_eq!(marks.len(), 1);
    let mark = marks.pop().unwrap();
    assert_eq!(mark.name(), "bold");
}
