use super::super::support::{actor, batch_orders, delete, put};
use super::observer::replay;
use super::support::{initial, ENCODINGS};
use automerge::{Prop, ReadDoc};

#[test]
fn replacement_removes_actual_before_width_and_keeps_formatting() {
    for encoding in ENCODINGS {
        let (base, text, start) = initial(encoding);
        let mut doc = base.clone().with_actor(actor(4));
        put(&mut doc, &text, &Prop::Seq(start), "a🦀é".into());
        let mut deleting = doc.clone();
        let deletion = delete(&mut deleting, &text, &Prop::Seq(start));
        let mut survivor = base.clone().with_actor(actor(2));
        let existing = put(&mut survivor, &text, &Prop::Seq(start), "ø".into());
        let winner = survivor.get(&text, start).unwrap().unwrap().1;
        doc.apply_changes([existing]).unwrap();
        let mut loser = base.with_actor(actor(1));
        let incoming = put(&mut loser, &text, &Prop::Seq(start), "q".into());
        for changes in batch_orders(vec![deletion, incoming]) {
            let after = replay(doc.clone(), &text, changes);
            assert_eq!(after.get(&text, start).unwrap().unwrap().1, winner);
            assert_eq!(after.get_all(&text, start).unwrap().len(), 2);
            assert_eq!(after.text(&text).unwrap(), "éø!");
        }
    }
}
