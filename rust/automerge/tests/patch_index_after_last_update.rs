//! When a batch of changes updates the last element of a list (a counter
//! increment, or an update the write frontier hides) and also inserts
//! after it, the insert's patch counted the element as gone: the patch
//! inserted at its index instead of after it, so a model kept from the
//! patches disagreed with the document. `patches_replay_to_hydrate`
//! (tests/write_frontier_parity.rs) found the masked case now and then.

use automerge::{
    transaction::Transactable, ActorId, Author, AutoCommit, ObjType, PatchAction, ReadDoc,
    ScalarValue, ROOT,
};

fn insert_indexes(patches: &[automerge::Patch]) -> Vec<usize> {
    patches
        .iter()
        .filter_map(|p| match &p.action {
            PatchAction::Insert { index, .. } => Some(*index),
            _ => None,
        })
        .collect()
}

#[test]
fn an_increment_of_the_last_element_then_an_insert_after_it() {
    let mut main = AutoCommit::new().with_actor(ActorId::from(vec![0x01]));
    let list = main.put_object(ROOT, "list", ObjType::List).unwrap();
    main.insert(&list, 0, ScalarValue::counter(0)).unwrap();
    main.commit();
    let mut a = main.fork().with_actor(ActorId::from(vec![0xA0]));
    let mut b = main.fork().with_actor(ActorId::from(vec![0xB0]));
    main.update_diff_cursor();
    a.increment(&list, 0, 5).unwrap();
    a.commit();
    b.insert(&list, 1, "x").unwrap();
    b.commit();
    let heads = main.get_heads();
    let changes: Vec<_> = a
        .get_changes(&heads)
        .into_iter()
        .chain(b.get_changes(&heads))
        .collect();
    main.apply_changes(changes).unwrap();
    let patches = main.diff_incremental();
    assert_eq!(insert_indexes(&patches), vec![1]);
    assert_eq!(main.length(&list), 2);
}

#[test]
fn a_hidden_delete_of_the_last_element_then_an_insert_after_it() {
    let bob = Author::try_from("bbbb").unwrap();
    let mut main = AutoCommit::new().with_actor(ActorId::from(vec![0x01]));
    let list = main.put_object(ROOT, "list", ObjType::List).unwrap();
    main.commit();
    let mut a = main.fork().with_actor(ActorId::from(vec![0xA0]));
    let mut b = main
        .fork()
        .with_author(Some(bob.clone()))
        .with_actor(ActorId::from(vec![0xB0]));
    main.update_diff_cursor();
    a.insert(&list, 0, 0).unwrap();
    a.commit();
    b.insert(&list, 0, -1).unwrap();
    b.commit();
    main.merge(&mut b).unwrap();
    // hide bob's changes after this point
    main.mask_author(bob.clone(), &b.get_heads());
    b.merge(&mut a).unwrap();
    // bob deletes -1 (hidden from main), alice's 0 arrives with it
    b.delete(&list, 0).unwrap();
    b.commit();
    main.diff_incremental();
    main.merge(&mut b).unwrap();
    let patches = main.diff_incremental();
    let values: Vec<String> = main.values(&list).map(|(v, _)| v.to_string()).collect();
    assert_eq!(values, vec!["-1", "0"]);
    assert_eq!(insert_indexes(&patches), vec![1]);
}
