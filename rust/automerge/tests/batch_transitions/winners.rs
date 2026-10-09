//! Winner replacement and conflict-only histories for map properties and list elements.
use super::support::{actor, candidates, delete, put, replay, Site, SCHEDULES, SITES};
use automerge::{
    transaction::Transactable, Automerge, Change, ObjId, ObjType, Prop, ReadDoc, ScalarValue,
};

#[derive(Clone, Copy, Debug)]
enum Payload {
    Scalar,
    EmptyMap,
    PopulatedMap,
}

fn incoming(doc: &mut Automerge, obj: &ObjId, prop: &Prop, payload: Payload) -> Change {
    if matches!(payload, Payload::Scalar) {
        return put(doc, obj, prop, ScalarValue::Boolean(true));
    }
    let mut tx = doc.transaction();
    let child = tx.put_object(obj, prop.clone(), ObjType::Map).unwrap();
    if matches!(payload, Payload::PopulatedMap) {
        tx.put(&child, "child", 42).unwrap();
    }
    tx.commit();
    doc.get_last_local_change().unwrap()
}

fn incoming_replaces_deleted_winner(site: Site) {
    for n in [1, 5] {
        for payload in [Payload::Scalar, Payload::EmptyMap, Payload::PopulatedMap] {
            let (base, obj, prop) = site.initial("b");
            let mut old = base.clone().with_actor(actor(4));
            put(&mut old, &obj, &prop, ScalarValue::Boolean(false));
            let mut branch = base.with_actor(actor(n));
            let assignment = incoming(&mut branch, &obj, &prop, payload);
            let expected_id = candidates(&branch, &obj, &prop)[0].1.clone();
            let expected = branch.hydrate(None);
            let mut deleting = old.clone();
            let deletion = delete(&mut deleting, &obj, &prop);
            for schedule in SCHEDULES {
                let label = format!("{site:?} actor={n} {payload:?} {schedule:?}");
                let doc = replay(old.clone(), schedule.groups(&assignment, &deletion), &label);
                assert_eq!(doc.hydrate(None), expected, "{label}");
                let cs = candidates(&doc, &obj, &prop);
                assert_eq!(cs.len(), 1, "{label}");
                assert_eq!(cs[0].1, expected_id, "{label}");
            }
        }
    }
}

#[test]
fn map_incoming_replaces_deleted_winner() {
    incoming_replaces_deleted_winner(Site::Map);
}

#[test]
fn list_incoming_replaces_deleted_winner() {
    incoming_replaces_deleted_winner(Site::List);
}

fn exposed_survivor_with_incoming(site: Site) {
    for incoming_actor in [1, 3] {
        for survivor_map in [false, true] {
            let (base, obj, prop) = site.initial("b");
            let mut old = base.clone().with_actor(actor(4));
            put(&mut old, &obj, &prop, ScalarValue::Boolean(false));
            let mut deleting = old.clone();
            let deletion = delete(&mut deleting, &obj, &prop);
            let mut lower = base.clone().with_actor(actor(2));
            let lower_change = if survivor_map {
                incoming(&mut lower, &obj, &prop, Payload::PopulatedMap)
            } else {
                put(&mut lower, &obj, &prop, ScalarValue::Int(42))
            };
            let mut new = base.with_actor(actor(incoming_actor));
            let assignment = put(&mut new, &obj, &prop, ScalarValue::Boolean(true));
            let expected_id = if incoming_actor < 2 {
                candidates(&lower, &obj, &prop)[0].1.clone()
            } else {
                candidates(&new, &obj, &prop)[0].1.clone()
            };
            old.apply_changes([lower_change]).unwrap();
            for schedule in SCHEDULES {
                let label = format!(
                    "survivor {site:?} incoming={incoming_actor} map={survivor_map} {schedule:?}"
                );
                let doc = replay(old.clone(), schedule.groups(&assignment, &deletion), &label);
                let cs = candidates(&doc, &obj, &prop);
                assert_eq!(cs.len(), 2, "{label}");
                let chosen = doc.get(&obj, prop.clone()).unwrap().unwrap();
                assert_eq!(chosen.1, expected_id, "{label}");
            }
        }
    }
}

#[test]
fn map_exposed_survivor_with_incoming() {
    exposed_survivor_with_incoming(Site::Map);
}

#[test]
fn list_exposed_survivor_with_incoming() {
    exposed_survivor_with_incoming(Site::List);
}

#[test]
fn incoming_loser_without_delete_only_adds_conflict() {
    for site in SITES {
        let (base, obj, prop) = site.initial("b");
        let mut old = base.clone().with_actor(actor(4));
        put(&mut old, &obj, &prop, ScalarValue::Boolean(false));
        let before_id = candidates(&old, &obj, &prop)[0].1.clone();
        let mut lower = base.with_actor(actor(1));
        let assignment = put(&mut lower, &obj, &prop, ScalarValue::Boolean(true));
        let after = replay(old, vec![vec![assignment]], "retained winner");
        let cs = candidates(&after, &obj, &prop);
        assert_eq!(cs.len(), 2);
        let value = after.get(&obj, prop.clone()).unwrap().unwrap();
        assert_eq!(value.1, before_id);
        assert_eq!(value.0.as_bool(), Some(false));
    }
}
