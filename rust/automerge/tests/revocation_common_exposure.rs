//! Control-free regressions for the exposure machinery shared with the
//! prototype (supplied by the coordinator; kept independent of the session).
use automerge::{
    marks::{ExpandMark, Mark},
    transaction::Transactable,
    Automerge, ObjType, PatchAction, ROOT,
};

fn marked_bold(patches: &[automerge::Patch]) -> String {
    patches
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
        .collect()
}

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
    assert_eq!(marked_bold(&patches), "X", "{patches:?}");
    // Replay of the formatted state through spans.
    let mut replay = doc.hydrate(Some(&hidden));
    replay
        .apply_patches(automerge::TextEncoding::platform_default(), patches.clone())
        .unwrap();
    assert_eq!(replay, doc.hydrate(Some(&before)));
}
