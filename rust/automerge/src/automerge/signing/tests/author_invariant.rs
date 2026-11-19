//! Current-author invariants: construction and mutation are distinct from history.

use std::error::Error;

use ed25519_dalek::{Signer, SigningKey};

use crate::signature::{ed25519, signing_message};
use crate::transaction::Transactable;
use crate::{ActorId, Author, Automerge, ChangeHash, ReadDoc, SigningAuthorError, ROOT};

use super::super::{error, prepare_author, RequiredSigning, SigningState};

fn author(seed: u8) -> Author<'static> {
    Author::from(
        SigningKey::from_bytes(&[seed; 32])
            .verifying_key()
            .to_bytes()
            .to_vec(),
    )
}

fn invalid_authors() -> Vec<Author<'static>> {
    let mut weak = vec![0; 32];
    weak[0] = 1;
    vec![Author::from(vec![1, 2, 3]), Author::from(weak)]
}

#[test]
fn required_author_validation_is_scheme_aware_and_unsigned_validation_is_permissive() {
    let doc = Automerge::new();
    let required = SigningState::Required(RequiredSigning::new(doc.actors()));
    let mut proposed = vec![None, Some(author(7))];
    proposed.extend(invalid_authors().into_iter().map(Some));
    for candidate in &proposed {
        SigningState::Unsigned
            .validate_author(candidate.as_ref())
            .unwrap();
    }
    assert!(required.validate_author(None).is_err());
    required.validate_author(proposed[1].as_ref()).unwrap();
    for candidate in &proposed[2..] {
        let failure = required.validate_author(candidate.as_ref()).unwrap_err();
        assert!(failure.source().is_some());
    }
}

#[test]
fn author_preparation_distinguishes_missing_invalid_and_weak_keys_and_retains_identity() {
    use crate::error::SigningAuthorErrorInner;
    use ed25519::error::VerifyingAuthorKind;

    let missing = prepare_author(None).unwrap_err();
    assert!(matches!(missing.0, SigningAuthorErrorInner::MissingAuthor));
    for (invalid, expected) in invalid_authors().iter().zip([
        VerifyingAuthorKind::InvalidKey,
        VerifyingAuthorKind::WeakKey,
    ]) {
        let failure = prepare_author(Some(invalid)).unwrap_err();
        let SigningAuthorErrorInner::InvalidKey(source) = failure.0 else {
            panic!("expected an invalid-key diagnostic");
        };
        assert_eq!(source.kind(), expected);
    }
    let valid = author(7);
    let prepared = prepare_author(Some(&valid)).unwrap();
    assert_eq!(prepared.author(), &valid);
    let hash = ChangeHash([4; 32]);
    let signature = ed25519::Signature::from_bytes(
        SigningKey::from_bytes(&[7; 32])
            .sign(&signing_message(&hash))
            .to_bytes(),
    );
    assert!(prepared.verify_hash(&hash, &signature).is_ok());
}

#[test]
fn required_construction_requires_a_usable_author_without_creating_history() {
    let expected = author(7);
    let mut doc = Automerge::signing(expected.clone()).unwrap();
    assert_eq!(doc.get_author(), Some(&expected));
    assert!(doc.get_heads().is_empty());
    assert!(doc.staged.is_empty());
    let before = format!("{doc:?}");
    {
        let signing = doc.try_signing().unwrap();
        assert_eq!(signing.author.author(), &expected);
        assert!(signing.graph.is_empty());
        assert_eq!(signing.ops.actors.len(), 0);
        assert!(!signing.is_accepted(&ChangeHash([0xff; 32])));
    }
    assert_eq!(format!("{doc:?}"), before);
    for invalid in invalid_authors() {
        let failure: SigningAuthorError = Automerge::signing(invalid).unwrap_err();
        assert!(failure.source().is_some());
    }
}

#[test]
fn rejected_required_author_forks_preserve_actor_history_and_evidence() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let mut doc = Automerge::signing(author(7))
        .unwrap()
        .with_actor(ActorId::from([1]));
    let mut tx = doc.transaction();
    tx.put(ROOT, "value", 1).unwrap();
    let (hash, _) = tx.commit();
    let hash = hash.unwrap();
    let sig = ed25519::Signature::from_bytes(key.sign(&signing_message(&hash)).to_bytes());
    doc.try_signing()
        .unwrap()
        .insert_signature(&hash, &sig)
        .unwrap();
    assert!(doc.try_signing().unwrap().is_accepted(&hash));
    let mut candidates = vec![None];
    candidates.extend(invalid_authors().into_iter().map(Some));
    for candidate in candidates {
        let before = format!("{doc:?}");
        assert!(matches!(
            doc.fork_with_author(candidate),
            Err(crate::AutomergeError::SigningAuthor(_))
        ));
        assert_eq!(format!("{doc:?}"), before);
    }
}

#[test]
fn same_author_forks_keep_identity_and_later_commits_omit_the_author_footer() {
    let alice = author(7);
    let bob = author(8);
    let mut doc = Automerge::signing(alice.clone()).unwrap();
    let actor = doc.get_actor().clone();
    let mut tx = doc.transaction();
    tx.put(ROOT, "first", 1).unwrap();
    let (first, _) = tx.commit();
    let first = doc.get_change_by_hash(&first.unwrap()).unwrap();
    let mut tx = doc.transaction();
    tx.put(ROOT, "second", 2).unwrap();
    let (second, _) = tx.commit();
    let second = doc.get_change_by_hash(&second.unwrap()).unwrap();
    assert_eq!(first.author().as_ref(), Some(&alice));
    assert!(second.author().is_none());
    assert_eq!(doc.get_author(), Some(&alice));
    assert_eq!(doc.try_signing().unwrap().author.author(), &alice);

    // A same-author fork is a normal fork: fresh actor, same author and the
    // old actor's binding intact; the source is untouched.
    let before = format!("{doc:?}");
    let mut fork = doc.fork_with_author(Some(alice.clone())).unwrap();
    assert_eq!(format!("{doc:?}"), before);
    assert_ne!(fork.get_actor(), &actor);
    assert_eq!(fork.try_signing().unwrap().author.author(), &alice);
    assert_eq!(fork.get_author_for_actor(&actor).as_ref(), Some(&alice));
    let old_signature = ed25519::Signature::from_bytes(
        SigningKey::from_bytes(&[7; 32])
            .sign(&signing_message(&first.hash()))
            .to_bytes(),
    );
    fork.try_signing()
        .unwrap()
        .insert_signature(&first.hash(), &old_signature)
        .unwrap();
    // Drafts remain, so Bob cannot take over this history yet.
    assert!(matches!(
        fork.fork_with_author(Some(bob)),
        Err(crate::AutomergeError::Signing(
            crate::signature::error::SigningError::UnpublishedLocalChanges
        ))
    ));
}

#[test]
fn constructors_use_the_same_mode_specific_author_rules() {
    for candidate in [None, Some(Author::from(vec![1, 2, 3]))] {
        let unsigned = Automerge::new_with_author(candidate.clone());
        assert_eq!(unsigned.get_author(), candidate.as_ref());
        assert!(Automerge::signing(author(7))
            .unwrap()
            .fork_with_author(candidate)
            .is_err());
    }
    let doc = Automerge::signing(author(7))
        .unwrap()
        .fork_with_author(Some(author(8)))
        .unwrap();
    assert_eq!(doc.get_author(), Some(&author(8)));
    let arbitrary = Author::from(vec![1, 2, 3]);
    let unsigned = Automerge::new_with_author(Some(arbitrary.clone()));
    assert_eq!(unsigned.get_author(), Some(&arbitrary));
    let unsigned = unsigned.fork_with_author(None).unwrap();
    assert!(unsigned.get_author().is_none());
}

#[test]
fn handle_prepares_the_current_key_without_mutating_or_owning_the_document() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let mut doc = Automerge::signing(author(7)).unwrap();
    let before = format!("{doc:?}");
    let hash = ChangeHash([3; 32]);
    let signature = ed25519::Signature::from_bytes(key.sign(&signing_message(&hash)).to_bytes());
    for _ in 0..2 {
        let signing = doc.try_signing().unwrap();
        let witness = signing.author.verify_hash(&hash, &signature).unwrap();
        assert_eq!(*witness.hash(), hash);
        assert_eq!(witness.author(), &author(7));
    }
    assert_eq!(format!("{doc:?}"), before);
}

#[test]
fn checked_handle_reports_wrong_mode_and_broken_author_invariants_without_panicking() {
    let mut unsigned = Automerge::new();
    let before = format!("{unsigned:?}");
    assert!(matches!(
        unsigned.try_signing(),
        Err(error::SigningAccess::Mode(error::SigningMode::Required))
    ));
    assert_eq!(format!("{unsigned:?}"), before);
    let mut candidates = vec![None];
    candidates.extend(invalid_authors().into_iter().map(Some));
    for invalid in candidates {
        let mut doc = Automerge::signing(author(7)).unwrap();
        // Deliberately violate the private invariant to test checked handle
        // construction. Normal constructors and setters must not produce this.
        doc.author = invalid;
        let before = format!("{doc:?}");
        assert!(matches!(
            doc.try_signing(),
            Err(error::SigningAccess::Author(_))
        ));
        assert_eq!(format!("{doc:?}"), before);
    }
}
