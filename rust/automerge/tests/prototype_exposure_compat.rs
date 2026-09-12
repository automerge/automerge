//! Control-free regressions for the exposure machinery shared with the prototype.
use automerge::{
    marks::{ExpandMark, Mark},
    transaction::Transactable,
    Automerge, ObjType, PatchAction, ROOT,
};

#[test]
fn reverse_deletion_diff_splits_restored_text_at_unchanged_mark() {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    let text = tx.put_object(ROOT, "text", ObjType::Text).unwrap();
    tx.splice_text(&text, 0, 0, "aXXXbcd").unwrap();
    tx.mark(
        &text,
        Mark::new("bold".into(), true, 2, 3),
        ExpandMark::Both,
    )
    .unwrap();
    tx.splice_text(&text, 3, 0, "Q").unwrap();
    tx.commit();
    let before = doc.get_heads();
    let mut tx = doc.transaction();
    tx.splice_text(&text, 4, 1, "").unwrap();
    tx.splice_text(&text, 1, 2, "").unwrap();
    tx.commit();
    let hidden = doc.get_heads();
    let patches = doc.diff(&hidden, &before);
    let marked: String = patches
        .iter()
        .filter_map(|p| match &p.action {
            PatchAction::SpliceText {
                value,
                marks: Some(m),
                ..
            } if m
                .iter()
                .any(|(n, v)| n == "bold" && v.as_bool() == Some(true)) =>
            {
                Some(value.make_string())
            }
            _ => None,
        })
        .collect();
    assert_eq!(marked, "X", "{patches:?}");
}

#[test]
fn restored_text_exposes_marks_and_block_contents_without_controls() {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    let text = tx.put_object(ROOT, "text", ObjType::Text).unwrap();
    tx.splice_text(&text, 0, 0, "abcd").unwrap();
    tx.mark(
        &text,
        Mark::new("bold".into(), true, 1, 3),
        ExpandMark::Both,
    )
    .unwrap();
    let block = tx.split_block(&text, 2).unwrap();
    tx.put(&block, "kind", "paragraph").unwrap();
    tx.commit();
    let visible = doc.get_heads();
    let mut tx = doc.transaction();
    tx.delete(ROOT, "text").unwrap();
    tx.commit();
    let hidden = doc.get_heads();
    let patches = doc.diff(&hidden, &visible);
    let marked: String = patches
        .iter()
        .filter_map(|p| match &p.action {
            PatchAction::SpliceText {
                value,
                marks: Some(m),
                ..
            } if m
                .iter()
                .any(|(n, v)| n == "bold" && v.as_bool() == Some(true)) =>
            {
                Some(value.make_string())
            }
            _ => None,
        })
        .collect();
    assert_eq!(marked, "bc");
    assert!(patches.iter().any(|p|p.obj==text&&matches!(&p.action,PatchAction::Insert{index:2,values} if values.len()==1&&values.get(0).unwrap().1==block)));
    assert!(patches.iter().any(|p|p.obj==block&&matches!(&p.action,PatchAction::PutMap{key,value,..} if key=="kind"&&value.0.as_str()==Some("paragraph"))));
}
