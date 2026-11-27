//! AutoCommit authors are fixed at construction; forks decide before closing edits.

use ed25519_dalek::SigningKey;

use crate::signature::error::AttestationError;
use crate::signature::{ed25519, signing_message};
use crate::transaction::Transactable;
use crate::{Author, AutomergeError, ChangeHash, ReadDoc, ROOT};

use super::AutoCommit;

fn author(seed: u8) -> Author<'static> {
    Author::from(
        SigningKey::from_bytes(&[seed; 32])
            .verifying_key()
            .to_bytes()
            .to_vec(),
    )
}

fn signature(seed: u8, hash: &ChangeHash) -> ed25519::Signature {
    use ed25519_dalek::Signer;
    ed25519::Signature::from_bytes(
        SigningKey::from_bytes(&[seed; 32])
            .sign(&signing_message(hash))
            .to_bytes(),
    )
}

#[test]
fn rejected_author_fork_does_not_close_an_open_transaction() {
    let mut weak = vec![0; 32];
    weak[0] = 1;
    for proposed in [
        None,
        Some(Author::from(vec![1, 2, 3])),
        Some(Author::from(weak)),
        Some(author(8)),
    ] {
        let mut doc =
            AutoCommit::from_config(crate::Config::default().attested(author(7)).unwrap());
        doc.put(ROOT, "pending", 1).unwrap();
        assert!(doc.transaction.is_some());
        assert!(doc.doc.get_heads().is_empty());
        let before = format!("{doc:?}");
        let error = doc.fork_with_author(proposed.clone()).unwrap_err();
        match proposed {
            Some(candidate) if candidate == author(8) => assert!(matches!(
                error,
                AutomergeError::Signing(AttestationError::UnpublishedLocalChanges)
            )),
            _ => assert!(matches!(error, AutomergeError::SigningAuthor(_))),
        }
        assert_eq!(format!("{doc:?}"), before);
        assert_eq!(doc.rollback(), 1);
        assert!(doc.doc.get_heads().is_empty());
        assert!(doc.get(ROOT, "pending").unwrap().is_none());
    }
}

#[test]
fn rolled_back_transaction_permits_a_different_author_fork_of_published_work() {
    let mut doc = AutoCommit::from_config(crate::Config::default().attested(author(7)).unwrap());
    doc.put(ROOT, "x", 1).unwrap();
    let hash = doc.commit().unwrap();
    doc.insert_signatures([(hash, signature(7, &hash))]);
    assert!(!doc.has_unpublished_changes());

    doc.put(ROOT, "y", 2).unwrap();
    assert!(doc.fork_with_author(Some(author(8))).is_err());
    assert_eq!(doc.pending_ops(), 1);
    doc.rollback();

    let fork = doc.fork_with_author(Some(author(8))).unwrap();
    assert_eq!(fork.get_author(), Some(&author(8)));
    assert_ne!(fork.get_actor(), doc.get_actor());
}

#[test]
fn same_author_fork_commits_pending_work_and_rotates_only_the_fork_actor() {
    let mut doc = AutoCommit::from_config(crate::Config::default().attested(author(7)).unwrap());
    doc.put(ROOT, "pending", 1).unwrap();
    let actor = doc.doc.get_actor().clone();
    let mut fork = doc.fork_with_author(Some(author(7))).unwrap();
    assert!(doc.transaction.is_none());
    assert_eq!(doc.pending_ops(), 0, "pending work was drained");
    assert_eq!(doc.doc.get_actor(), &actor);
    assert_ne!(fork.doc.get_actor(), &actor);
    assert_eq!(fork.get_heads(), doc.get_heads());
    assert_eq!(fork.get_author(), Some(&author(7)));
    assert_eq!(fork.signing_requests().unwrap().len(), 1);
    let changes = doc.doc.get_changes(&[]);
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].actor_id(), &actor);
    assert_eq!(changes[0].author(), Some(author(7)));
    assert_eq!(fork.doc.get_changes(&[]).len(), 1);
}

#[test]
fn unattested_constructors_and_forks_remain_permissive() {
    let arbitrary = Author::from(vec![1, 2, 3]);
    let mut doc = AutoCommit::from_config(crate::Config::default().author(Some(arbitrary.clone())));
    doc.put(ROOT, "pending", 1).unwrap();
    let mut fork = doc.fork_with_author(None).unwrap();
    assert!(doc.transaction.is_none());
    assert_eq!(fork.get_heads(), doc.get_heads(), "pending edits committed");
    assert_eq!(
        doc.doc.get_changes(&[])[0].author(),
        Some(arbitrary.clone())
    );
    assert_eq!(doc.get_author(), Some(&arbitrary));
    assert!(fork.get_author().is_none());
    let replaced = doc.fork_with_author(Some(Author::from(vec![2]))).unwrap();
    assert_eq!(replaced.get_author(), Some(&Author::from(vec![2])));
    let doc = AutoCommit::from_config(crate::Config::default().author(None));
    assert!(doc.get_author().is_none());
}
