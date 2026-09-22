//! Counter totals, identity, conflict metadata and duplicate delivery.
use super::support::{
    actor, batch_orders, candidates, delete, increment, put, replay, Replay, Site, SCHEDULES, SITES,
};
use automerge::{PatchAction, ReadDoc, ScalarValue};

#[test]
fn retained_counter_increment_and_conflict_clear_replays_once() {
    for site in SITES {
        let (base, obj, prop) = site.initial("key");
        let mut loser = base.clone().with_actor(actor(1));
        let losing = put(&mut loser, &obj, &prop, false.into());
        let deletion = delete(&mut loser, &obj, &prop);
        let mut doc = base.with_actor(actor(4));
        put(&mut doc, &obj, &prop, ScalarValue::counter(10));
        let counter_id = doc.get(&obj, prop.clone()).unwrap().unwrap().1;
        let mut updater = doc.clone();
        let increment = increment(&mut updater, &obj, &prop, 5);
        doc.apply_changes([losing]).unwrap();
        assert_eq!(doc.get_all(&obj, prop.clone()).unwrap().len(), 2);
        let (value, winner) = doc.get(&obj, prop.clone()).unwrap().unwrap();
        assert_eq!(winner, counter_id);
        assert!(value.is_counter());
        assert_eq!(value.as_i64(), Some(10));

        for schedule in SCHEDULES {
            let mut replay = Replay::new(doc.clone());
            for (step, changes) in schedule
                .groups(&deletion, &increment)
                .into_iter()
                .enumerate()
            {
                let patches =
                    replay.apply(changes, &format!("{site:?}, {schedule:?}, step {step}"));
                if schedule.is_grouped() {
                    assert!(!patches
                        .iter()
                        .any(|p| matches!(&p.action, PatchAction::Increment { .. })));
                    assert!(
                        patches.iter().any(|p| match &p.action {
                            PatchAction::PutMap {
                                key,
                                value,
                                conflict,
                            } if p.obj == obj && key == "key" =>
                                value.0.is_counter()
                                    && value.0.as_i64() == Some(15)
                                    && value.1 == counter_id
                                    && !conflict,
                            PatchAction::PutSeq {
                                index,
                                value,
                                conflict,
                            } if p.obj == obj && *index == 0 =>
                                value.0.is_counter()
                                    && value.0.as_i64() == Some(15)
                                    && value.1 == counter_id
                                    && !conflict,
                            _ => false,
                        }),
                        "patches={patches:?}"
                    );
                }
            }
            let candidates = replay.doc.get_all(&obj, prop.clone()).unwrap();
            assert_eq!(candidates.len(), 1);
            assert_eq!(candidates[0].1, counter_id);
            assert!(candidates[0].0.is_counter());
            assert_eq!(candidates[0].0.as_i64(), Some(15));

            // Check metadata on the replayed view, not just candidate count.
            let (value, conflict) = site.observed_value(&mut replay.view, "key");
            assert!(!conflict);
            assert!(matches!(
                value,
                automerge::hydrate::Value::Scalar(ScalarValue::Counter(_))
            ));
            assert_eq!(value.as_i64(), 15);
        }
    }
}

fn exposed_counter_with_increment(site: Site) {
    let (base, obj, prop) = site.initial("b");
    let mut old = base.clone().with_actor(actor(4));
    put(&mut old, &obj, &prop, ScalarValue::Boolean(false));
    let mut deleting = old.clone();
    let deletion = delete(&mut deleting, &obj, &prop);
    let mut counter = base.with_actor(actor(2));
    let base_change = put(&mut counter, &obj, &prop, ScalarValue::counter(10));
    let base_id = candidates(&counter, &obj, &prop)[0].1.clone();
    let increment = increment(&mut counter, &obj, &prop, 5);
    old.apply_changes([base_change]).unwrap();
    for schedule in SCHEDULES {
        let doc = replay(
            old.clone(),
            schedule.groups(&increment, &deletion),
            &format!("counter {site:?} {schedule:?}"),
        );
        let cs = candidates(&doc, &obj, &prop);
        assert_eq!(cs.len(), 1);
        assert_eq!(cs[0].0.as_i64(), Some(15));
        assert_eq!(cs[0].1, base_id);
    }
}

#[test]
fn map_exposed_counter_with_increment() {
    exposed_counter_with_increment(Site::Map);
}

#[test]
fn list_exposed_counter_with_increment() {
    exposed_counter_with_increment(Site::List);
}

#[test]
fn stable_counter_increment_and_duplicate_replay() {
    for site in SITES {
        let (mut doc, obj, prop) = site.initial("b");
        put(&mut doc, &obj, &prop, ScalarValue::counter(10));
        let mut branch = doc.clone();
        let inc = increment(&mut branch, &obj, &prop, 5);
        let after = replay(
            doc,
            vec![vec![inc.clone()], vec![inc], vec![]],
            "stable counter",
        );
        assert_eq!(candidates(&after, &obj, &prop)[0].0.as_i64(), Some(15));
    }
}

#[test]
fn stable_counter_increment_can_also_gain_a_conflict() {
    for site in SITES {
        let (base, obj, prop) = site.initial("key");
        let mut doc = base.clone().with_actor(actor(4));
        put(&mut doc, &obj, &prop, ScalarValue::counter(10));
        let id = doc.get(&obj, prop.clone()).unwrap().unwrap().1;
        let mut branch = doc.clone();
        let inc = increment(&mut branch, &obj, &prop, 5);
        let mut loser = base.with_actor(actor(1));
        let losing = put(&mut loser, &obj, &prop, true.into());
        for changes in batch_orders(vec![inc, losing]) {
            let after = replay(doc.clone(), vec![changes], &format!("{site:?}"));
            let (value, after_id) = after.get(&obj, prop.clone()).unwrap().unwrap();
            assert_eq!(value.as_i64(), Some(15));
            assert_eq!(after_id, id);
            assert_eq!(after.get_all(&obj, prop.clone()).unwrap().len(), 2);
        }
    }
}

#[test]
fn stable_counter_multiple_signed_increments_with_incoming_loser() {
    for site in SITES {
        let (base, obj, prop) = site.initial("key");
        let mut doc = base.clone().with_actor(actor(4));
        put(&mut doc, &obj, &prop, ScalarValue::counter(10));
        let id = doc.get(&obj, prop.clone()).unwrap().unwrap().1;
        let mut changes = Vec::new();
        for (actor_id, n) in [(5, 7), (6, -11), (7, 3)] {
            let mut branch = doc.clone().with_actor(actor(actor_id));
            changes.push(increment(&mut branch, &obj, &prop, n));
        }
        let mut loser = base.with_actor(actor(1));
        changes.push(put(&mut loser, &obj, &prop, true.into()));
        for changes in batch_orders(changes) {
            let after = replay(doc.clone(), vec![changes], &format!("{site:?}"));
            let (value, after_id) = after.get(&obj, prop.clone()).unwrap().unwrap();
            assert_eq!(value.as_i64(), Some(9));
            assert_eq!(after_id, id);
            assert_eq!(after.get_all(&obj, prop.clone()).unwrap().len(), 2);
        }
    }
}

#[test]
fn incoming_counter_total_includes_all_batch_increments() {
    for site in SITES {
        let (base, obj, prop) = site.initial("key");
        let mut doc = base.clone().with_actor(actor(4));
        put(&mut doc, &obj, &prop, false.into());
        let mut deleting = doc.clone();
        let deletion = delete(&mut deleting, &obj, &prop);
        let mut counter = base.with_actor(actor(1));
        let creation = put(&mut counter, &obj, &prop, ScalarValue::counter(10));
        let id = counter.get(&obj, prop.clone()).unwrap().unwrap().1;
        let mut changes = vec![creation, deletion];
        for n in [5, -2] {
            changes.push(increment(&mut counter, &obj, &prop, n));
        }
        for changes in batch_orders(changes) {
            let after = replay(doc.clone(), vec![changes], &format!("{site:?}"));
            let (value, after_id) = after.get(&obj, prop.clone()).unwrap().unwrap();
            assert_eq!(value.as_i64(), Some(13));
            assert_eq!(after_id, id);
        }
    }
}
