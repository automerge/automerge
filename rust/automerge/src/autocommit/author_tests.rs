//! Author validation must precede AutoCommit's implicit transaction closure.

use ed25519_dalek::SigningKey;

use crate::transaction::Transactable;
use crate::{Author, Automerge, ReadDoc, ROOT};

use super::AutoCommit;

fn author(seed: u8) -> Author<'static> {
    Author::from(
        SigningKey::from_bytes(&[seed; 32])
            .verifying_key()
            .to_bytes()
            .to_vec(),
    )
}

fn required() -> AutoCommit {
    // No new constructor/public testing hook: use the existing private fields
    // to place a checked signing document inside an otherwise fresh AutoCommit.
    AutoCommit {
        doc: Automerge::signing(author(7)).unwrap(),
        ..AutoCommit::new()
    }
}

#[test]
fn rejected_author_update_does_not_close_an_open_transaction() {
    let mut weak = vec![0; 32];
    weak[0] = 1;
    for proposed in [
        None,
        Some(Author::from(vec![1, 2, 3])),
        Some(Author::from(weak)),
    ] {
        let mut doc = required();
        doc.put(ROOT, "pending", 1).unwrap();
        assert!(doc.transaction.is_some());
        assert!(doc.doc.get_heads().is_empty());
        let before = format!("{doc:?}");
        assert!(doc.set_author(proposed).is_err());
        assert_eq!(format!("{doc:?}"), before);
        assert_eq!(doc.rollback(), 1);
        assert!(doc.doc.get_heads().is_empty());
        assert!(doc.get(ROOT, "pending").unwrap().is_none());
    }
}

#[test]
fn valid_switch_commits_pending_work_under_the_old_author_then_rotates_actor() {
    let mut doc = required();
    doc.put(ROOT, "old", 1).unwrap();
    let actor = doc.doc.get_actor().clone();
    assert!(doc.doc.get_heads().is_empty());
    doc.set_author(Some(author(8))).unwrap();
    assert!(doc.transaction.is_none());
    assert_ne!(doc.doc.get_actor(), &actor);
    let changes = doc.doc.get_changes(&[]);
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].actor_id(), &actor);
    assert_eq!(changes[0].author(), Some(author(7)));
    assert_eq!(doc.get_author(), Some(&author(8)));
    doc.put(ROOT, "new", 2).unwrap();
    let hash = doc.commit().unwrap();
    assert_eq!(
        doc.doc.get_change_by_hash(&hash).unwrap().author(),
        Some(author(8))
    );
}

#[test]
fn same_author_closes_pending_transaction_without_rotating_actor() {
    let mut doc = required();
    doc.put(ROOT, "pending", 1).unwrap();
    let actor = doc.doc.get_actor().clone();
    doc.set_author(Some(author(7))).unwrap();
    assert!(doc.transaction.is_none());
    assert_eq!(doc.doc.get_actor(), &actor);
    assert_eq!(doc.doc.get_changes(&[]).len(), 1);
}

#[test]
fn unsigned_setters_and_builders_remain_permissive() {
    let arbitrary = Author::from(vec![1, 2, 3]);
    let mut doc = AutoCommit::new()
        .with_author(Some(arbitrary.clone()))
        .unwrap();
    doc.put(ROOT, "pending", 1).unwrap();
    doc.set_author(None).unwrap();
    assert!(doc.transaction.is_none());
    assert_eq!(doc.doc.get_changes(&[])[0].author(), Some(arbitrary));
    assert!(doc.get_author().is_none());
    let doc = AutoCommit::new().with_author(None).unwrap();
    assert!(doc.get_author().is_none());
}

#[test]
fn required_builder_rejects_missing_or_unusable_authors() {
    assert!(required().with_author(None).is_err());
    assert!(required()
        .with_author(Some(Author::from(vec![1, 2, 3])))
        .is_err());
    let doc = required().with_author(Some(author(8))).unwrap();
    assert_eq!(doc.get_author(), Some(&author(8)));
}
