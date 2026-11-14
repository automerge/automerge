use ed25519_dalek::{Signer, SigningKey};
use proptest::prelude::*;

use super::*;
use crate::signature::{signing_message, Signature, SignatureBytes, SignatureTag};

fn hash(value: u8) -> ChangeHash {
    ChangeHash([value; 32])
}

fn signature(hash: ChangeHash, key: &SigningKey) -> Signature {
    Signature::ed25519(ed25519::Signature::from_bytes(
        key.sign(&signing_message(&hash)).to_bytes(),
    ))
}

fn entries(items: impl IntoIterator<Item = (ChangeHash, Signature)>) -> HashEntries {
    HashEntries::canonicalize(items).unwrap()
}

fn author(key: &SigningKey) -> Author<'static> {
    Author::from(key.verifying_key().to_bytes().to_vec())
}

#[test]
fn requirements_are_fixed_actor_frontiers_not_all_members() {
    let requirements =
        FragmentRequirements::for_members([(0, 2, hash(2)), (1, 1, hash(3)), (0, 1, hash(1))]);
    assert_eq!(
        requirements.hashes().copied().collect::<Vec<_>>(),
        [hash(2), hash(3)]
    );
    assert!(requirements.is_satisfied_by(|h| *h == hash(2) || *h == hash(3)));
    assert!(!requirements.is_satisfied_by(|h| *h == hash(1) || *h == hash(3)));
    assert_eq!(requirements.hashes().len(), 2);
}

#[test]
fn one_actor_singleton_has_the_same_obligation_at_any_fragment_level() {
    for h in [hash(1), ChangeHash([0; 32])] {
        let requirements = FragmentRequirements::for_members([(0, 5, h)]);
        assert_eq!(requirements.hashes().copied().collect::<Vec<_>>(), [h]);
    }
}

#[test]
fn independent_proof_verifies_and_retains_only_required_evidence() {
    let key_a = SigningKey::from_bytes(&[7; 32]);
    let key_b = SigningKey::from_bytes(&[8; 32]);
    let requirements =
        FragmentRequirements::for_members([(0, 1, hash(1)), (0, 2, hash(2)), (1, 1, hash(3))]);
    let attached = entries([
        (hash(1), signature(hash(1), &key_a)),
        (hash(2), signature(hash(2), &key_a)),
        (hash(3), signature(hash(3), &key_b)),
        (hash(4), signature(hash(4), &key_a)),
    ]);
    let proof = requirements
        .verify_entries(&attached, |h| {
            Some(author(if *h == hash(3) { &key_b } else { &key_a }))
        })
        .unwrap();
    assert!(requirements.is_satisfied_by(|h| proof.ed25519_entries(h).next().is_some()));
    assert_eq!(proof.ed25519_entries(&hash(1)).count(), 0);
    assert_eq!(proof.ed25519_entries(&hash(4)).count(), 0);
}

#[test]
fn another_groups_descendant_or_empty_entries_does_not_satisfy_frontier() {
    let key_a = SigningKey::from_bytes(&[7; 32]);
    let key_b = SigningKey::from_bytes(&[8; 32]);
    let a = hash(1);
    let b = hash(2);
    let c = hash(3);
    let f = FragmentRequirements::for_members([(0, 1, a), (1, 1, b)]);
    let g = FragmentRequirements::for_members([(0, 2, c)]);
    let attached_f = entries([(b, signature(b, &key_b)), (c, signature(c, &key_a))]);
    assert!(matches!(
        f.verify_entries(&attached_f, |h| Some(author(if *h == b { &key_b } else { &key_a }))),
        Err(ProofError::MissingSignature(h)) if h == a
    ));
    let attached_g = entries([(c, signature(c, &key_a))]);
    assert!(g
        .verify_entries(&attached_g, |_| Some(author(&key_a)))
        .is_ok());

    let empty = entries([]);
    assert!(matches!(
        g.verify_entries(&empty, |_| Some(author(&key_a))),
        Err(ProofError::MissingSignature(h)) if h == c
    ));
}

#[test]
fn actors_sharing_an_author_still_require_separate_frontiers() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let requirements = FragmentRequirements::for_members([(0, 1, hash(1)), (1, 2, hash(2))]);
    let partial = entries([(hash(2), signature(hash(2), &key))]);
    assert!(matches!(
        requirements.verify_entries(&partial, |_| Some(author(&key))),
        Err(ProofError::MissingSignature(h)) if h == hash(1)
    ));
    let complete = entries([
        (hash(1), signature(hash(1), &key)),
        (hash(2), signature(hash(2), &key)),
    ]);
    assert!(requirements
        .verify_entries(&complete, |_| Some(author(&key)))
        .is_ok());
}

#[test]
fn invalid_unsupported_wrong_author_and_missing_author_cases_are_rejected() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let other_key = SigningKey::from_bytes(&[8; 32]);
    let requirements = FragmentRequirements::for_members([(0, 1, hash(1))]);
    for invalid in [
        signature(hash(1), &other_key),
        signature(hash(2), &key),
        Signature::new(SignatureTag::ED25519, SignatureBytes::new(vec![1]).unwrap()),
        Signature::new(SignatureTag::new(42), SignatureBytes::new(vec![1]).unwrap()),
    ] {
        let attached = entries([(hash(1), invalid.clone())]);
        assert!(matches!(
            requirements.verify_entries(&attached, |_| Some(author(&key))),
            Err(ProofError::MissingSignature(h)) if h == hash(1)
        ));
        let both = entries([(hash(1), invalid), (hash(1), signature(hash(1), &key))]);
        assert!(requirements
            .verify_entries(&both, |_| Some(author(&key)))
            .is_ok());
    }

    let valid = entries([(hash(1), signature(hash(1), &key))]);
    assert!(matches!(
        requirements.verify_entries(&valid, |_| None),
        Err(ProofError::MissingAuthor(_))
    ));
    assert!(matches!(
        requirements.verify_entries(&valid, |_| Some(Author::from(vec![0; 3]))),
        Err(ProofError::InvalidAuthorKey { .. })
    ));
}

proptest! {
    #[test]
    fn frontier_selection_matches_a_member_scan(sequences in prop::collection::vec((0u8..8, 1u8..32), 0..100)) {
        let members: Vec<_> = sequences.iter().map(|(actor, seq)| {
            let mut hash = [0; 32];
            hash[0] = *actor;
            hash[1] = *seq;
            (*actor, u64::from(*seq), ChangeHash(hash))
        }).collect();
        let requirements = FragmentRequirements::for_members(members.iter().copied());
        let expected: std::collections::BTreeSet<_> = members.iter()
            .filter(|(actor, seq, _)| !members.iter().any(|(a, s, _)| a == actor && s > seq))
            .map(|(_, _, hash)| *hash).collect();
        prop_assert_eq!(requirements.hashes().copied().collect::<std::collections::BTreeSet<_>>(), expected);
        prop_assert_eq!(FragmentRequirements::for_members(members.into_iter().rev()), requirements);
    }
}
