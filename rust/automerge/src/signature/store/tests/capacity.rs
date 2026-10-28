//! Private collection-policy tests use synthetic signatures, not forged witnesses.

use std::collections::{BTreeMap, BTreeSet};

use proptest::prelude::*;

use crate::signature::store::{AuthorContext, ChangeSignatures, Insert, ResolvedChangeSignatures};
use crate::signature::{ed25519, Signature, SignatureBytes, SignatureTag};
use crate::Author;

fn signature(byte: u8) -> ed25519::Signature {
    ed25519::Signature::from_bytes([byte; 64])
}
fn author(byte: u8) -> Author<'static> {
    Author::from(vec![byte; 32])
}
fn group(initial: u8) -> ChangeSignatures {
    ChangeSignatures::Resolved(Box::new(ResolvedChangeSignatures {
        author: AuthorContext::IdentityOnly(author(7)),
        unchecked: Vec::new(),
        ed25519_evidence: vec![signature(initial)],
    }))
}
fn resolved(group: &ChangeSignatures) -> &ResolvedChangeSignatures {
    let ChangeSignatures::Resolved(group) = group else {
        panic!("expected resolved group")
    };
    group
}
// Mirrors the outer store's author check, without constructing fake witnesses.
fn insert(
    group: &mut ChangeSignatures,
    author: &Author<'_>,
    signature: ed25519::Signature,
) -> Option<Insert> {
    if !group.associate(AuthorContext::IdentityOnly(author.clone())) {
        return None;
    }
    match group {
        ChangeSignatures::Resolved(group) => Some(group.insert_ed25519(signature)),
        ChangeSignatures::Unresolved { .. } => panic!("successful association resolves the group"),
    }
}
fn full_group() -> ChangeSignatures {
    let mut group = group(0);
    for value in 1..16 {
        assert_eq!(
            insert(&mut group, &author(7), signature(value)),
            Some(Insert::Stored)
        );
    }
    group
}

#[test]
fn accepts_sixteen_distinct_entries_including_the_initial_entry() {
    let group = full_group();
    assert_eq!(resolved(&group).ed25519_evidence.len(), 16);
    for value in 0..16 {
        assert!(resolved(&group)
            .ed25519_evidence
            .contains(&signature(value)));
    }
}

#[test]
fn seventeenth_distinct_entry_is_dropped_without_mutation() {
    let mut group = full_group();
    let before = resolved(&group).ed25519_evidence.clone();
    assert_eq!(
        insert(&mut group, &author(7), signature(16)),
        Some(Insert::DroppedOverflow)
    );
    assert_eq!(resolved(&group).ed25519_evidence, before);
    assert_eq!(resolved(&group).author.author(), &author(7));
}

#[test]
fn repeated_duplicates_at_capacity_are_still_duplicates() {
    let mut group = full_group();
    let before = resolved(&group).ed25519_evidence.clone();
    for _ in 0..3 {
        for value in 0..16 {
            assert_eq!(
                insert(&mut group, &author(7), signature(value)),
                Some(Insert::Duplicate)
            );
        }
    }
    assert_eq!(resolved(&group).ed25519_evidence, before);
}

#[test]
fn author_conflict_takes_precedence_over_duplicate_and_capacity() {
    let mut group = full_group();
    let before = resolved(&group).ed25519_evidence.clone();
    assert_eq!(insert(&mut group, &author(8), signature(0)), None);
    assert_eq!(insert(&mut group, &author(8), signature(16)), None);
    assert_eq!(resolved(&group).ed25519_evidence, before);
    assert_eq!(resolved(&group).author.author(), &author(7));
}

#[test]
fn repeated_overflow_never_replaces_an_existing_signature() {
    let mut group = full_group();
    let before = resolved(&group).ed25519_evidence.clone();
    for value in 16..=u8::MAX {
        assert_eq!(
            insert(&mut group, &author(7), signature(value)),
            Some(Insert::DroppedOverflow)
        );
    }
    assert_eq!(resolved(&group).ed25519_evidence, before);
    assert_eq!(
        insert(&mut group, &author(7), signature(0)),
        Some(Insert::Duplicate)
    );
}

#[test]
fn capacity_is_per_group_not_shared_between_hashes() {
    let mut full = full_group();
    let mut other = group(100);
    assert_eq!(
        insert(&mut full, &author(7), signature(16)),
        Some(Insert::DroppedOverflow)
    );
    assert_eq!(
        insert(&mut other, &author(7), signature(101)),
        Some(Insert::Stored)
    );
    assert_eq!(resolved(&other).ed25519_evidence.len(), 2);
    assert_eq!(resolved(&full).ed25519_evidence.len(), 16);
}

#[test]
fn further_proofs_do_not_displace_entries_but_can_establish_evidence_at_capacity() {
    let mut group = group(0);
    for value in 1..16 {
        assert_eq!(
            group.insert_unchecked(Signature::ed25519(signature(value))),
            Insert::Stored
        );
    }
    assert_eq!(
        insert(&mut group, &author(7), signature(16)),
        Some(Insert::DroppedOverflow)
    );
    assert_eq!(
        insert(&mut group, &author(7), signature(15)),
        Some(Insert::EstablishedEd25519Evidence)
    );
    assert_eq!(
        resolved(&group).ed25519_evidence,
        vec![signature(0), signature(15)]
    );
    assert_eq!(resolved(&group).unchecked.len(), 14);
    assert!(resolved(&group)
        .unchecked
        .contains(&Signature::ed25519(signature(1))));
    assert_eq!(
        group.insert_unchecked(Signature::ed25519(signature(15))),
        Insert::Duplicate
    );
}

#[test]
fn a_known_author_without_evidence_does_not_block_first_proof_replacement() {
    let mut group = ChangeSignatures::Unresolved {
        unchecked: (0..16).map(|b| Signature::ed25519(signature(b))).collect(),
    };
    assert!(group.associate(AuthorContext::IdentityOnly(author(7))));
    assert!(resolved(&group).ed25519_evidence.is_empty());
    assert_eq!(
        insert(&mut group, &author(7), signature(16)),
        Some(Insert::ReplacedUnchecked)
    );
    assert_eq!(resolved(&group).unchecked.len(), 15);
    assert!(!resolved(&group)
        .unchecked
        .contains(&Signature::ed25519(signature(15))));
    assert_eq!(resolved(&group).ed25519_evidence, vec![signature(16)]);
}

// Independent oracle: raw tag/byte keys and evidence flags, with author binding
// recorded separately. Resolution alone must not mark signatures as checked.
type Raw = (u8, Vec<u8>);
fn observed(group: &ChangeSignatures) -> (Option<Author<'static>>, BTreeMap<Raw, bool>) {
    let mut result = BTreeMap::new();
    let owner = match group {
        ChangeSignatures::Unresolved { .. } => None,
        ChangeSignatures::Resolved(group) => {
            for signature in &group.ed25519_evidence {
                assert!(result
                    .insert((0, signature.to_bytes().to_vec()), true)
                    .is_none());
            }
            Some(group.author.author().clone())
        }
    };
    for signature in group.unchecked() {
        assert!(result
            .insert(
                (
                    signature.tag().inner_tag(),
                    signature.to_bytes().as_bytes().to_vec()
                ),
                false
            )
            .is_none());
    }
    (owner, result)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn insertions_follow_a_bounded_set_model(initial in any::<u8>(), operations in proptest::collection::vec((0u8..32, any::<bool>()), 0..128)) {
        let expected_author = author(7);
        let mut group = group(initial);
        let mut retained = BTreeSet::from([initial]);
        for (value, same_author) in operations {
            let expected = if !same_author { None }
                else if retained.contains(&value) { Some(Insert::Duplicate) }
                else if retained.len() == 16 { Some(Insert::DroppedOverflow) }
                else { retained.insert(value); Some(Insert::Stored) };
            let supplied = if same_author { author(7) } else { author(8) };
            prop_assert_eq!(insert(&mut group, &supplied, signature(value)), expected);
            let actual = resolved(&group);
            prop_assert!(actual.ed25519_evidence.len() <= 16);
            prop_assert_eq!(actual.author.author(), &expected_author);
            let actual: BTreeSet<_> = actual.ed25519_evidence.iter().map(|s| s.to_bytes()).collect();
            let expected: BTreeSet<_> = retained.iter().map(|value| [*value; 64]).collect();
            prop_assert_eq!(actual, expected);
        }
    }

    #[test]
    fn mixed_resolution_and_insertions_preserve_identity_and_evidence(
        operations in proptest::collection::vec((0u8..10, 0u8..24, 0u8..3, any::<bool>()), 0..160),
    ) {
        let mut group = ChangeSignatures::Unresolved { unchecked: Vec::new() };
        let mut model: BTreeMap<Raw, bool> = BTreeMap::new();
        let mut owner: Option<Author<'static>> = None;
        for (operation, byte, tag, length64) in operations {
            let incoming = if operation % 2 == 0 { author(7) } else { author(8) };
            if operation >= 8 {
                let expected = if owner.as_ref().is_some_and(|old| old != &incoming) { false }
                    else { owner = Some(incoming.clone()); true };
                prop_assert_eq!(group.associate(AuthorContext::IdentityOnly(incoming)), expected);
                let (actual_owner, actual_entries) = observed(&group);
                prop_assert_eq!(&actual_owner, &owner);
                prop_assert_eq!(&actual_entries, &model);
                continue;
            }
            let with_evidence = operation >= 6;
            let raw = if with_evidence { (0, vec![byte; 64]) } else { (tag, vec![byte; if length64 {64} else {1}]) };
            let protected: BTreeSet<_> = model.iter().filter(|(_, v)| **v).map(|(raw, _)| raw.clone()).collect();
            let expected = if with_evidence && owner.as_ref().is_some_and(|old| old != &incoming) { None }
            else {
                if with_evidence { owner = Some(incoming.clone()); }
                if let Some(has_evidence) = model.get_mut(&raw) {
                    if with_evidence && !*has_evidence { *has_evidence = true; Some(Insert::EstablishedEd25519Evidence) }
                    else { Some(Insert::Duplicate) }
                } else if model.len() < 16 {
                    model.insert(raw.clone(), with_evidence); Some(Insert::Stored)
                } else if with_evidence && !model.values().any(|v| *v) {
                    model.pop_last(); model.insert(raw.clone(), true); Some(Insert::ReplacedUnchecked)
                } else { Some(Insert::DroppedOverflow) }
            };
            let actual = if with_evidence { insert(&mut group, &incoming, signature(byte)) }
                else { Some(group.insert_unchecked(Signature::new(SignatureTag::new(raw.0), SignatureBytes::new(raw.1).unwrap()))) };
            prop_assert_eq!(actual, expected);
            let (actual_owner, actual_entries) = observed(&group);
            prop_assert_eq!(&actual_owner, &owner);
            prop_assert_eq!(&actual_entries, &model);
            prop_assert!(actual_entries.len() <= 16);
            for raw in protected { prop_assert_eq!(actual_entries.get(&raw), Some(&true)); }
        }
    }
}
