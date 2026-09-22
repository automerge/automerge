use super::super::support::{actor, batch_orders, delete, increment, put};
use super::observer::{observe, replay, units};
use super::support::{change_marks, initial, ENCODINGS};
use automerge::{transaction::Transactable, Prop, ReadDoc, ScalarValue, ROOT};

fn conflict_clear(change_format: bool) {
    for encoding in ENCODINGS {
        let (base, text, start) = initial(encoding);
        let mut doc = base.clone().with_actor(actor(4));
        put(&mut doc, &text, &Prop::Seq(start), "a🦀é".into());
        let winner = doc.get(&text, start).unwrap().unwrap().1;
        let mut loser = base.with_actor(actor(1));
        let losing = put(&mut loser, &text, &Prop::Seq(start), "Z".into());
        let deletion = delete(&mut loser, &text, &Prop::Seq(start));
        doc.apply_changes([losing]).unwrap();
        assert_eq!(doc.get_all(&text, start).unwrap().len(), 2);
        assert_eq!(doc.get(&text, start).unwrap().unwrap().1, winner);
        assert_eq!(
            observe(&doc, &text)[start].1.get("bold"),
            Some(&true.into())
        );
        let mut changes = vec![deletion];
        if change_format {
            let mut marks = doc.clone().with_actor(actor(6));
            changes.push(change_marks(
                &mut marks,
                &text,
                start,
                units("a🦀é", encoding).len(),
            ));
        }
        for changes in batch_orders(changes) {
            let after = replay(doc.clone(), &text, changes);
            assert_eq!(after.get(&text, start).unwrap().unwrap().1, winner);
            assert_eq!(after.get_all(&text, start).unwrap().len(), 1);
            assert_eq!(after.text(&text).unwrap(), "éa🦀é!");
            let observed = observe(&after, &text);
            assert_eq!(observed[start].1.get("bold"), Some(&true.into()));
            if change_format {
                assert_eq!(observed[start].1.get("italic"), Some(&true.into()));
                assert!(!observed[start].1.contains_key("color"));
            }
        }
    }
}

#[test]
fn retained_text_winner_keeps_unchanged_marks_when_conflict_clears() {
    conflict_clear(false);
}

#[test]
fn retained_text_winner_combines_unchanged_and_changed_marks_when_conflict_clears() {
    conflict_clear(true);
}

#[test]
fn retained_marked_text_with_losing_candidate_churn_preserves_observation() {
    for encoding in ENCODINGS {
        for change_format in [false, true] {
            let (base, text, start) = initial(encoding);
            let mut doc = base.clone().with_actor(actor(4));
            put(&mut doc, &text, &Prop::Seq(start), "a🦀é".into());
            let winner = doc.get(&text, start).unwrap().unwrap().1;
            let mut loser = base.clone().with_actor(actor(1));
            let losing = put(&mut loser, &text, &Prop::Seq(start), "Z".into());
            let deletion = delete(&mut loser, &text, &Prop::Seq(start));
            doc.apply_changes([losing]).unwrap();
            assert_eq!(doc.get_all(&text, start).unwrap().len(), 2);
            let before = observe(&doc, &text);
            let mut incoming = base.with_actor(actor(2));
            let mut changes = vec![
                deletion,
                put(&mut incoming, &text, &Prop::Seq(start), "q".into()),
            ];
            let width = units("a🦀é", encoding).len();
            if change_format {
                let mut marks = doc.clone().with_actor(actor(6));
                changes.push(change_marks(&mut marks, &text, start, width));
            }
            for changes in batch_orders(changes) {
                let after = replay(doc.clone(), &text, changes);
                assert_eq!(after.get(&text, start).unwrap().unwrap().1, winner);
                assert_eq!(after.get_all(&text, start).unwrap().len(), 2);
                assert_eq!(after.text(&text).unwrap(), "éa🦀é!");
                let observed = observe(&after, &text);
                let mut expected = before.clone();
                if change_format {
                    for (_, marks) in &mut expected[start..start + width] {
                        marks.insert("italic".into(), true.into());
                        marks.remove("color");
                    }
                }
                assert_eq!(observed, expected);
                for (_, marks) in &observed[start..start + width] {
                    assert_eq!(marks.get("bold"), Some(&true.into()));
                }
            }
        }
    }
}

#[test]
fn retained_text_counter_marks_change_alongside_increment_and_conflict() {
    for encoding in ENCODINGS {
        let (base, text, start) = initial(encoding);
        let mut doc = base.clone().with_actor(actor(4));
        put(&mut doc, &text, &Prop::Seq(start), ScalarValue::counter(10));
        let winner = doc.get(&text, start).unwrap().unwrap().1;
        let mut changes = Vec::new();
        for (actor_id, n) in [(5, 7), (6, -11), (7, 3)] {
            let mut branch = doc.clone().with_actor(actor(actor_id));
            changes.push(increment(&mut branch, &text, &Prop::Seq(start), n));
        }
        let mut loser = base.with_actor(actor(1));
        changes.push(put(&mut loser, &text, &Prop::Seq(start), "z".into()));
        let mut marks = doc.clone().with_actor(actor(8));
        changes.push(change_marks(
            &mut marks,
            &text,
            start,
            units("\u{fffc}", encoding).len(),
        ));
        for changes in batch_orders(changes) {
            let after = replay(doc.clone(), &text, changes);
            // Fast text get/get_all includes increment ops and panics. A
            // genuinely historical query uses the counter-aware path. Advance
            // a clone at an unrelated key (current heads otherwise optimize
            // back to the fast path); the replayed document remains unchanged.
            let heads = after.get_heads();
            let mut historical = after.clone();
            let mut tx = historical.transaction();
            tx.put(ROOT, "unrelated", true).unwrap();
            tx.commit();
            let (value, id) = historical.get_at(&text, start, &heads).unwrap().unwrap();
            assert_eq!(id, winner);
            assert_eq!(value.as_i64(), Some(9));
            assert_eq!(
                historical.get_all_at(&text, start, &heads).unwrap().len(),
                2
            );
            let observed = observe(&after, &text);
            assert_eq!(observed[start].1.get("bold"), Some(&true.into()));
            assert_eq!(observed[start].1.get("italic"), Some(&true.into()));
            assert!(!observed[start].1.contains_key("color"));
        }
    }
}
