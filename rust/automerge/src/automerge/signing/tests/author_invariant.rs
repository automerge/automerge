//! Configured identity is retained separately from historical author bindings.

use ed25519_dalek::{Signer, SigningKey};

use crate::signature::{ed25519, signing_message};
use crate::transaction::Transactable;
use crate::{ActorId, Author, Automerge, ChangeHash, ReadDoc, ROOT};

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
fn signing_configuration_reports_the_invalid_or_weak_key_diagnostic_kind() {
    use crate::error::SigningAuthorErrorInner;
    use ed25519::error::VerifyingAuthorKind;

    for (invalid, expected) in invalid_authors().iter().zip([
        VerifyingAuthorKind::InvalidKey,
        VerifyingAuthorKind::WeakKey,
    ]) {
        let failure = crate::Config::default()
            .signing(invalid.clone())
            .unwrap_err();
        assert!(std::error::Error::source(&failure).is_some());
        let SigningAuthorErrorInner::InvalidKey(source) = failure.0 else {
            panic!("expected an invalid-key diagnostic");
        };
        assert_eq!(source.kind(), expected);
    }
}

#[test]
fn required_construction_requires_a_usable_author_without_creating_history() {
    let expected = author(7);
    let mut doc =
        Automerge::from_config(crate::Config::default().signing(expected.clone()).unwrap());
    assert_eq!(doc.get_author(), Some(&expected));
    assert!(doc.get_heads().is_empty());
    assert!(doc.staged.is_empty());
    let before = format!("{doc:?}");
    {
        let signing = doc.try_signing().unwrap();
        assert_eq!(signing.state.author.author(), &expected);
        assert!(signing.graph.is_empty());
        assert_eq!(signing.ops.actors.len(), 0);
        assert!(!signing.is_published(&ChangeHash([0xff; 32])));
    }
    assert_eq!(format!("{doc:?}"), before);
}

#[test]
fn published_required_fork_still_rejects_a_missing_or_invalid_author() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let mut doc = Automerge::from_config(crate::Config::default().signing(author(7)).unwrap())
        .with_actor(ActorId::from([1]))
        .unwrap();
    let mut tx = doc.transaction();
    tx.put(ROOT, "value", 1).unwrap();
    let (hash, _) = tx.commit();
    let hash = hash.unwrap();
    let sig = ed25519::Signature::from_bytes(key.sign(&signing_message(&hash)).to_bytes());
    super::insert_one(&mut doc, hash, &sig).unwrap();
    assert!(doc.try_signing().unwrap().is_published(&hash));
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
fn only_the_first_commit_of_an_actor_carries_the_author_footer() {
    let alice = author(7);
    let mut doc = Automerge::from_config(crate::Config::default().signing(alice.clone()).unwrap());
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
    assert_eq!(doc.try_signing().unwrap().state.author.author(), &alice);
}

#[test]
fn same_author_fork_keeps_the_identity_and_the_old_actor_binding() {
    let alice = author(7);
    let mut doc = Automerge::from_config(crate::Config::default().signing(alice.clone()).unwrap());
    let actor = doc.get_actor().clone();
    let mut tx = doc.transaction();
    tx.put(ROOT, "first", 1).unwrap();
    let (first, _) = tx.commit();
    let first = doc.get_change_by_hash(&first.unwrap()).unwrap().hash();

    // A same-author fork is a normal fork: fresh actor, same author and the
    // old actor's binding intact; the source is untouched.
    let before = format!("{doc:?}");
    let mut fork = doc.fork_with_author(Some(alice.clone())).unwrap();
    assert_eq!(format!("{doc:?}"), before);
    assert_ne!(fork.get_actor(), &actor);
    assert_eq!(fork.try_signing().unwrap().state.author.author(), &alice);
    assert_eq!(fork.get_author_for_actor(&actor).as_ref(), Some(&alice));
    // The inherited draft is still signable with the retained verifier.
    let old_signature = ed25519::Signature::from_bytes(
        SigningKey::from_bytes(&[7; 32])
            .sign(&signing_message(&first))
            .to_bytes(),
    );
    super::insert_one(&mut fork, first, &old_signature).unwrap();
}

#[test]
fn author_rules_per_mode_across_construction_and_fork() {
    for candidate in [None, Some(Author::from(vec![1, 2, 3]))] {
        let unsigned = Automerge::from_config(crate::Config::default().author(candidate.clone()));
        assert_eq!(unsigned.get_author(), candidate.as_ref());
        assert!(
            Automerge::from_config(crate::Config::default().signing(author(7)).unwrap())
                .fork_with_author(candidate)
                .is_err()
        );
    }
    let doc = Automerge::from_config(crate::Config::default().signing(author(7)).unwrap())
        .fork_with_author(Some(author(8)))
        .unwrap();
    assert_eq!(doc.get_author(), Some(&author(8)));
    let arbitrary = Author::from(vec![1, 2, 3]);
    let unsigned = Automerge::from_config(crate::Config::default().author(Some(arbitrary.clone())));
    assert_eq!(unsigned.get_author(), Some(&arbitrary));
    let unsigned = unsigned.fork_with_author(None).unwrap();
    assert!(unsigned.get_author().is_none());
}

#[test]
fn handle_borrows_the_retained_verifier_without_changing_the_document() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let mut doc = Automerge::from_config(crate::Config::default().signing(author(7)).unwrap());
    let before = format!("{doc:?}");
    let hash = ChangeHash([3; 32]);
    let signature = ed25519::Signature::from_bytes(key.sign(&signing_message(&hash)).to_bytes());
    let retained = {
        let signing = doc.try_signing().unwrap();
        std::ptr::from_ref(&signing.state.author)
    };
    for _ in 0..2 {
        let signing = doc.try_signing().unwrap();
        assert_eq!(std::ptr::from_ref(&signing.state.author), retained);
        let witness = signing.state.author.verify_hash(&hash, &signature).unwrap();
        assert_eq!(witness.hash(), &hash);
        assert_eq!(witness.signature(), &signature);
    }
    assert_eq!(format!("{doc:?}"), before);
}
