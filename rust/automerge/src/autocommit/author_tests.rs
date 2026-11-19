//! AutoCommit authors are fixed at construction; forks decide before closing edits.

use ed25519_dalek::SigningKey;

use crate::signature::error::SigningError;
use crate::transaction::Transactable;
use crate::{Author, AutomergeError, ReadDoc, ROOT};

use super::AutoCommit;

fn author(seed: u8) -> Author<'static> {
    Author::from(
        SigningKey::from_bytes(&[seed; 32])
            .verifying_key()
            .to_bytes()
            .to_vec(),
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
        let mut doc = AutoCommit::signing(author(7)).unwrap();
        doc.put(ROOT, "pending", 1).unwrap();
        assert!(doc.transaction.is_some());
        assert!(doc.doc.get_heads().is_empty());
        let before = format!("{doc:?}");
        let error = doc.fork_with_author(proposed.clone()).unwrap_err();
        match proposed {
            Some(candidate) if candidate == author(8) => assert!(matches!(
                error,
                AutomergeError::Signing(SigningError::UnpublishedLocalChanges)
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
fn same_author_fork_commits_pending_work_and_rotates_only_the_fork_actor() {
    let mut doc = AutoCommit::signing(author(7)).unwrap();
    doc.put(ROOT, "pending", 1).unwrap();
    let actor = doc.doc.get_actor().clone();
    let fork = doc.fork_with_author(Some(author(7))).unwrap();
    assert!(doc.transaction.is_none());
    assert_eq!(doc.doc.get_actor(), &actor);
    assert_ne!(fork.doc.get_actor(), &actor);
    let changes = doc.doc.get_changes(&[]);
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].actor_id(), &actor);
    assert_eq!(changes[0].author(), Some(author(7)));
    assert_eq!(fork.doc.get_changes(&[]).len(), 1);
}

#[test]
fn unsigned_constructors_and_forks_remain_permissive() {
    let arbitrary = Author::from(vec![1, 2, 3]);
    let mut doc = AutoCommit::new_with_author(Some(arbitrary.clone()));
    doc.put(ROOT, "pending", 1).unwrap();
    let fork = doc.fork_with_author(None).unwrap();
    assert!(doc.transaction.is_none());
    assert_eq!(
        doc.doc.get_changes(&[])[0].author(),
        Some(arbitrary.clone())
    );
    assert_eq!(doc.get_author(), Some(&arbitrary));
    assert!(fork.get_author().is_none());
    let doc = AutoCommit::new_with_author(None);
    assert!(doc.get_author().is_none());
}

#[test]
fn required_constructor_rejects_missing_or_unusable_authors() {
    assert!(AutoCommit::signing(Author::from(vec![1, 2, 3])).is_err());
    let doc = AutoCommit::signing(author(8)).unwrap();
    assert_eq!(doc.get_author(), Some(&author(8)));
}
