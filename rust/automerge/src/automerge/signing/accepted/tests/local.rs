//! Exercise document-owned candidate assembly without caller-supplied candidates.

use ed25519_dalek::SigningKey;

use crate::automerge::signing::error;
use crate::{Automerge, ChangeHash, ReadDoc};

use super::{append_at_level, fixture, inject_evidence, retain_signature, value};

#[test]
fn empty_local_acceptance_is_a_repeatable_no_op() {
    let mut document = Automerge::signing();
    {
        let mut signing = document.try_signing().unwrap();
        signing.accept_local().unwrap();
        signing.accept_local().unwrap();
        assert!(!signing.is_accepted(&ChangeHash([0xff; 32])));
    }
    assert!(document.get_heads().is_empty());
}

#[test]
fn loose_candidates_wait_for_signature_gaps_and_retry_without_reapplication() {
    let (mut document, changes, key) = fixture(&[0, 0, 0]);
    let heads = document.get_heads();
    let before = value(&document);
    {
        let mut signing = document.try_signing().unwrap();
        retain_signature(&mut signing, &changes[0], &key);
        retain_signature(&mut signing, &changes[2], &key);
        assert!(changes
            .iter()
            .all(|change| !signing.is_accepted(&change.hash())));

        signing.accept_local().unwrap();
        assert!(signing.is_accepted(&changes[0].hash()));
        assert!(!signing.is_accepted(&changes[1].hash()));
        assert!(!signing.is_accepted(&changes[2].hash()));
    }
    assert_eq!(document.get_heads(), heads);
    assert_eq!(value(&document), before);
    {
        let mut signing = document.try_signing().unwrap();
        retain_signature(&mut signing, &changes[1], &key);
        assert!(!signing.is_accepted(&changes[1].hash()));
        signing.accept_local().unwrap();
        assert!(changes
            .iter()
            .all(|change| signing.is_accepted(&change.hash())));
        signing.accept_local().unwrap();
        assert!(changes
            .iter()
            .all(|change| signing.is_accepted(&change.hash())));
    }
    assert_eq!(document.get_heads(), heads);
    assert_eq!(value(&document), before);
}

#[test]
fn fragment_members_are_not_also_offered_as_loose_candidates() {
    let (mut document, changes, key) = fixture(&[0, 0, 1]);
    let fragments = document.fragments(..);
    assert_eq!(fragments.len(), 1);
    assert_eq!(fragments[0].members.len(), 3);
    let heads = document.get_heads();
    let before = value(&document);
    {
        let mut signing = document.try_signing().unwrap();
        retain_signature(&mut signing, &changes[0], &key);
        signing.accept_local().unwrap();
        // The signed interior cannot escape the whole-fragment obligation.
        assert!(changes
            .iter()
            .all(|change| !signing.is_accepted(&change.hash())));

        retain_signature(&mut signing, &changes[2], &key);
        signing.accept_local().unwrap();
        assert!(changes
            .iter()
            .all(|change| signing.is_accepted(&change.hash())));
        assert_eq!(
            signing
                .state
                .signatures
                .ed25519_entries(&changes[1].hash())
                .count(),
            0
        );
    }
    assert_eq!(document.get_heads(), heads);
    assert_eq!(value(&document), before);
}

#[test]
fn a_partly_accepted_fragment_is_retained_with_its_full_membership() {
    let (mut document, changes, key) = fixture(&[0]);
    let first = &changes[0];
    {
        let mut signing = document.try_signing().unwrap();
        retain_signature(&mut signing, first, &key);
        signing.accept_local().unwrap();
        assert!(signing.is_accepted(&first.hash()));
    }

    let middle = append_at_level(&mut document, 2, 0);
    let tip = append_at_level(&mut document, 3, 1);
    let fragment = document.get_fragment(tip).unwrap();
    assert_eq!(fragment.members.len(), 3);
    assert!(fragment.members.contains(&first.hash()));
    let tip_change = document.get_change_by_hash(&tip).unwrap();
    let heads = document.get_heads();
    let before = value(&document);
    {
        let mut signing = document.try_signing().unwrap();
        signing.accept_local().unwrap();
        assert!(signing.is_accepted(&first.hash()));
        assert!(!signing.is_accepted(&middle));
        assert!(!signing.is_accepted(&tip));

        retain_signature(&mut signing, &tip_change, &key);
        signing.accept_local().unwrap();
        assert!(fragment
            .members
            .iter()
            .all(|hash| signing.is_accepted(hash)));
        signing.accept_local().unwrap();
    }
    assert_eq!(document.get_heads(), heads);
    assert_eq!(value(&document), before);
}

#[test]
fn a_single_member_nonzero_level_fragment_can_complete() {
    let (mut document, changes, key) = fixture(&[1]);
    let fragment = document.get_fragment(changes[0].hash()).unwrap();
    assert_eq!(fragment.level, 1);
    assert_eq!(fragment.members, [changes[0].hash()]);
    let heads = document.get_heads();
    let before = value(&document);
    {
        let mut signing = document.try_signing().unwrap();
        signing.accept_local().unwrap();
        assert!(!signing.is_accepted(&changes[0].hash()));
        retain_signature(&mut signing, &changes[0], &key);
        signing.accept_local().unwrap();
        assert!(signing.is_accepted(&changes[0].hash()));
    }
    assert_eq!(document.get_heads(), heads);
    assert_eq!(value(&document), before);
}

#[test]
fn accepted_fragment_supports_an_outstanding_loose_suffix() {
    let (mut document, changes, key) = fixture(&[0, 0, 1]);
    {
        let mut signing = document.try_signing().unwrap();
        retain_signature(&mut signing, &changes[2], &key);
        signing.accept_local().unwrap();
        assert!(changes
            .iter()
            .all(|change| signing.is_accepted(&change.hash())));
    }
    let suffix = append_at_level(&mut document, 4, 0);
    let suffix_change = document.get_change_by_hash(&suffix).unwrap();
    let heads = document.get_heads();
    let before = value(&document);
    {
        let mut signing = document.try_signing().unwrap();
        signing.accept_local().unwrap();
        assert!(!signing.is_accepted(&suffix));
        retain_signature(&mut signing, &suffix_change, &key);
        signing.accept_local().unwrap();
        assert!(signing.is_accepted(&suffix));
        assert!(changes
            .iter()
            .all(|change| signing.is_accepted(&change.hash())));
    }
    assert_eq!(document.get_heads(), heads);
    assert_eq!(value(&document), before);
}

#[test]
fn acceptance_error_preserves_its_cause_without_partial_advancement() {
    let (mut document, changes, key) = fixture(&[0, 0]);
    let heads = document.get_heads();
    let before = value(&document);
    {
        let mut signing = document.try_signing().unwrap();
        retain_signature(&mut signing, &changes[0], &key);
        // Defensive corruption fixture: this association cannot be inserted
        // through the document's checked signature insertion method.
        inject_evidence(&mut signing, &changes[1], &SigningKey::from_bytes(&[8; 32]));
        let error = signing.accept_local().unwrap_err();
        assert!(matches!(
            &error,
            error::AcceptLocal::Acceptance(error::AcceptApplied::AuthorMismatch { change })
                if *change == changes[1].hash()
        ));
        assert!(std::error::Error::source(&error).is_some());
        assert!(changes
            .iter()
            .all(|change| !signing.is_accepted(&change.hash())));
    }
    assert_eq!(document.get_heads(), heads);
    assert_eq!(value(&document), before);
}
