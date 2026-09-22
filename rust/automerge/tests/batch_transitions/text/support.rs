use super::super::support::actor;
use super::observer::units;
use automerge::{
    marks::{ExpandMark, Mark},
    transaction::Transactable,
    Automerge, Change, ObjId, ObjType, TextEncoding, ROOT,
};

pub(super) const ENCODINGS: [TextEncoding; 3] = [
    TextEncoding::UnicodeCodePoint,
    TextEncoding::Utf8CodeUnit,
    TextEncoding::Utf16CodeUnit,
];

pub(super) fn initial(encoding: TextEncoding) -> (Automerge, ObjId, usize) {
    let mut doc = Automerge::new_with_encoding(encoding).with_actor(actor(9));
    let start = units("é", encoding).len();
    let mut tx = doc.transaction();
    let text = tx.put_object(ROOT, "text", ObjType::Text).unwrap();
    tx.insert(&text, 0, "é").unwrap();
    tx.insert(&text, start, "x").unwrap();
    tx.insert(&text, start + 1, "!").unwrap();
    tx.mark(
        &text,
        Mark::new("bold".into(), true, start, start + 1),
        ExpandMark::Both,
    )
    .unwrap();
    tx.mark(
        &text,
        Mark::new("color".into(), "red", start, start + 1),
        ExpandMark::Both,
    )
    .unwrap();
    tx.commit();
    (doc, text, start)
}

pub(super) fn change_marks(
    doc: &mut Automerge,
    text: &ObjId,
    start: usize,
    width: usize,
) -> Change {
    let mut tx = doc.transaction();
    tx.mark(
        text,
        Mark::new("italic".into(), true, start, start + width),
        ExpandMark::Both,
    )
    .unwrap();
    tx.unmark(text, "color", start, start + width, ExpandMark::Both)
        .unwrap();
    tx.commit();
    doc.get_last_local_change().unwrap()
}
