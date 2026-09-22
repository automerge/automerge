use super::super::support::{actor, delete, put};
use super::observer::{formatted, observe, replay, units};
use super::support::ENCODINGS;
use automerge::{
    marks::{ExpandMark, Mark},
    transaction::Transactable,
    Automerge, ObjType, Prop, ReadDoc, ScalarValue, ROOT,
};
use std::collections::BTreeMap;

#[test]
fn appeared_text_keeps_unchanged_marks_after_concurrent_delete_and_put() {
    for encoding in ENCODINGS {
        for (prefix, replacement) in [("a", "X"), ("é", "🦀")] {
            let mut base = Automerge::new_with_encoding(encoding).with_actor(actor(9));
            let initial = format!("{prefix}bc");
            let index = units(prefix, encoding).len();
            let mut tx = base.transaction();
            let text = tx.put_object(ROOT, "text", ObjType::Text).unwrap();
            tx.splice_text(&text, 0, 0, &initial).unwrap();
            tx.mark(
                &text,
                Mark::new("bold".into(), true, 0, units(&initial, encoding).len()),
                ExpandMark::Both,
            )
            .unwrap();
            tx.commit();

            // The put sees the original element, not the concurrent deletion.
            let mut deleting = base.clone().with_actor(actor(1));
            let deletion = delete(&mut deleting, &text, &Prop::Seq(index));
            let mut assigning = base.clone().with_actor(actor(2));
            let assignment = put(&mut assigning, &text, &Prop::Seq(index), replacement.into());
            let winner = assigning.get(&text, index).unwrap().unwrap().1;

            // At receipt of the put, the element has no visible candidate.
            let mut doc = base;
            doc.apply_changes([deletion]).unwrap();
            let bold = BTreeMap::from([("bold".to_owned(), ScalarValue::Boolean(true))]);
            assert_eq!(
                observe(&doc, &text),
                formatted(&format!("{prefix}c"), bold.clone(), encoding)
            );
            let after = replay(doc, &text, vec![assignment]);
            assert_eq!(after.get(&text, index).unwrap().unwrap().1, winner);
            assert_eq!(
                observe(&after, &text),
                formatted(&format!("{prefix}{replacement}c"), bold, encoding)
            );
        }
    }
}
