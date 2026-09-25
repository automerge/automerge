//! Existing-object reconstruction and exactly-once nested updates.
use super::support::{actor, batch_orders, delete, put, replay, SITES};
use automerge::{transaction::Transactable, ObjType, ReadDoc, ScalarValue};

#[test]
fn clearing_conflict_keeps_existing_object_contents_and_identity() {
    for site in SITES {
        let (base, obj, prop) = site.initial("key");
        let mut loser = base.clone().with_actor(actor(1));
        let losing = put(&mut loser, &obj, &prop, false.into());
        let deletion = delete(&mut loser, &obj, &prop);
        let mut doc = base.with_actor(actor(4));
        let mut tx = doc.transaction();
        let child = tx.put_object(&obj, prop.clone(), ObjType::Map).unwrap();
        let nested = tx.put_object(&child, "nested", ObjType::List).unwrap();
        tx.insert(&nested, 0, ScalarValue::counter(10)).unwrap();
        tx.commit();
        doc.apply_changes([losing]).unwrap();
        let mut update = doc.clone();
        let mut tx = update.transaction();
        tx.increment(&nested, 0, 5).unwrap();
        tx.insert(&nested, 1, "new").unwrap();
        tx.commit();
        let edit = update.get_last_local_change().unwrap();
        for changes in batch_orders(vec![deletion, edit]) {
            let after = replay(doc.clone(), vec![changes], &format!("{site:?}"));
            assert_eq!(after.get(&obj, prop.clone()).unwrap().unwrap().1, child);
            assert_eq!(after.get_all(&obj, prop.clone()).unwrap().len(), 1);
        }
    }
}

#[test]
fn exposed_object_includes_simultaneous_nested_updates_once() {
    for site in SITES {
        let (base, obj, prop) = site.initial("key");
        let mut doc = base.clone().with_actor(actor(4));
        put(&mut doc, &obj, &prop, false.into());
        let mut deleting = doc.clone();
        let deletion = delete(&mut deleting, &obj, &prop);
        let mut survivor = base.clone().with_actor(actor(2));
        let mut tx = survivor.transaction();
        let child = tx.put_object(&obj, prop.clone(), ObjType::Map).unwrap();
        let nested = tx.put_object(&child, "nested", ObjType::List).unwrap();
        tx.insert(&nested, 0, ScalarValue::counter(10)).unwrap();
        tx.commit();
        doc.apply_changes([survivor.get_last_local_change().unwrap()])
            .unwrap();
        let mut tx = survivor.transaction();
        tx.increment(&nested, 0, 5).unwrap();
        tx.insert(&nested, 1, "new").unwrap();
        tx.commit();
        let edit = survivor.get_last_local_change().unwrap();
        let mut loser = base.with_actor(actor(1));
        let losing = put(&mut loser, &obj, &prop, true.into());
        let changes = vec![deletion, edit, losing];
        for changes in batch_orders(changes) {
            let after = replay(doc.clone(), vec![changes], &format!("{site:?}"));
            assert_eq!(after.get(&obj, prop.clone()).unwrap().unwrap().1, child);
            assert_eq!(after.get_all(&obj, prop.clone()).unwrap().len(), 2);
        }
    }
}
