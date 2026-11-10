use crate::admission::CandidateHistory;
use crate::transaction::Transactable;
use crate::{ActorId, ReadDoc, ROOT};

use super::{fixture, retain_signature, value};

#[test]
fn continued_editing_preserves_acceptance_without_accepting_the_new_change() {
    let (mut document, changes, key) = fixture(&[0]);
    let first = &changes[0];
    {
        let mut signing = document.try_signing().unwrap();
        retain_signature(&mut signing, first, &key);
        signing
            .accept_applied(&[CandidateHistory::Loose(first)])
            .unwrap();
    }

    // Edit the same document after releasing the handle, without cloning or reloading.
    let mut tx = document.transaction();
    tx.put(ROOT, "value", 2).unwrap();
    let (hash, _) = tx.commit();
    let second = document.get_change_by_hash(&hash.unwrap()).unwrap();
    assert_eq!(second.actor_id(), first.actor_id());
    assert_eq!(second.seq(), first.seq() + 1);
    assert_eq!(value(&document), 2.into());
    {
        let mut signing = document.try_signing().unwrap();
        assert!(signing.is_accepted(&first.hash()));
        assert!(!signing.is_accepted(&second.hash()));
        let candidates = [CandidateHistory::Loose(&second)];
        signing.accept_applied(&candidates).unwrap();
        assert!(!signing.is_accepted(&second.hash()));

        retain_signature(&mut signing, &second, &key);
        signing.accept_applied(&candidates).unwrap();
    }
    let signing = document.try_signing().unwrap();
    assert!(signing.is_accepted(&first.hash()));
    assert!(signing.is_accepted(&second.hash()));
}

#[test]
fn inserting_an_earlier_actor_preserves_the_existing_accepted_prefix() {
    let (mut document, changes, key) = fixture(&[0]);
    let first = &changes[0];
    let earlier_actor = ActorId::from([0]);
    assert!(earlier_actor < *first.actor_id());
    {
        let mut signing = document.try_signing().unwrap();
        assert_eq!(signing.ops.actors.lookup(first.actor_id()), Some(0));
        retain_signature(&mut signing, first, &key);
        signing
            .accept_applied(&[CandidateHistory::Loose(first)])
            .unwrap();
    }

    document.set_actor(earlier_actor.clone());
    let mut tx = document.transaction();
    tx.put(ROOT, "value", 2).unwrap();
    let (hash, _) = tx.commit();
    let second = document.get_change_by_hash(&hash.unwrap()).unwrap();
    assert_eq!(second.actor_id(), &earlier_actor);
    assert_eq!(second.seq(), 1);
    {
        let mut signing = document.try_signing().unwrap();
        assert_eq!(signing.ops.actors.lookup(&earlier_actor), Some(0));
        assert_eq!(signing.ops.actors.lookup(first.actor_id()), Some(1));
        assert!(signing.is_accepted(&first.hash()));
        assert!(!signing.is_accepted(&second.hash()));

        retain_signature(&mut signing, &second, &key);
        signing
            .accept_applied(&[CandidateHistory::Loose(&second)])
            .unwrap();
    }
    let signing = document.try_signing().unwrap();
    assert!(signing.is_accepted(&first.hash()));
    assert!(signing.is_accepted(&second.hash()));
}

#[test]
fn rolling_back_an_unused_actor_preserves_the_existing_accepted_prefix() {
    let (mut document, changes, key) = fixture(&[0]);
    let first = &changes[0];
    let temporary_actor = ActorId::from([0]);
    assert!(temporary_actor < *first.actor_id());
    let heads_before = document.get_heads();
    let value_before = value(&document);
    {
        let mut signing = document.try_signing().unwrap();
        retain_signature(&mut signing, first, &key);
        signing
            .accept_applied(&[CandidateHistory::Loose(first)])
            .unwrap();
    }

    document.set_actor(temporary_actor.clone());
    let mut tx = document.transaction();
    tx.put(ROOT, "value", 2).unwrap();
    assert_eq!(tx.rollback(), 1);

    assert_eq!(document.get_heads(), heads_before);
    assert_eq!(value(&document), value_before);
    {
        let signing = document.try_signing().unwrap();
        assert_eq!(signing.ops.actors.lookup(&temporary_actor), None);
        assert_eq!(signing.ops.actors.lookup(first.actor_id()), Some(0));
        assert_eq!(signing.ops.actors.iter().len(), 1);
        assert!(signing.is_accepted(&first.hash()));
    }

    // Re-inserting the rolled-back actor must start with an empty accepted slot.
    let mut tx = document.transaction();
    tx.put(ROOT, "value", 3).unwrap();
    let (hash, _) = tx.commit();
    let second = document.get_change_by_hash(&hash.unwrap()).unwrap();
    assert_eq!(second.actor_id(), &temporary_actor);
    let mut signing = document.try_signing().unwrap();
    assert!(signing.is_accepted(&first.hash()));
    assert!(!signing.is_accepted(&second.hash()));
    retain_signature(&mut signing, &second, &key);
    signing
        .accept_applied(&[CandidateHistory::Loose(&second)])
        .unwrap();
    assert!(signing.is_accepted(&first.hash()));
    assert!(signing.is_accepted(&second.hash()));
}

#[test]
fn clone_copies_a_populated_clock_without_sharing_later_acceptance() {
    let (mut original, changes, key) = fixture(&[0, 0]);
    {
        let mut signing = original.try_signing().unwrap();
        for change in &changes {
            retain_signature(&mut signing, change, &key);
        }
        signing
            .accept_applied(&[CandidateHistory::Loose(&changes[0])])
            .unwrap();
    }
    let mut cloned = original.clone();
    {
        let mut signing = cloned.try_signing().unwrap();
        assert!(signing.is_accepted(&changes[0].hash()));
        assert!(!signing.is_accepted(&changes[1].hash()));
        // The copied store already contains the second change's evidence.
        signing
            .accept_applied(&[CandidateHistory::Loose(&changes[1])])
            .unwrap();
        assert!(signing.is_accepted(&changes[0].hash()));
        assert!(signing.is_accepted(&changes[1].hash()));
    }
    {
        let signing = original.try_signing().unwrap();
        assert!(signing.is_accepted(&changes[0].hash()));
        assert!(!signing.is_accepted(&changes[1].hash()));
    }
    assert_eq!(original.get_heads(), cloned.get_heads());
    assert_eq!(value(&original), value(&cloned));
}
