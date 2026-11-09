use crate::transaction::Transactable;
use crate::{ActorId, Automerge, Change, ChangeHash, ReadDoc, ROOT};

use super::{ChangeBatch, StagedChanges};

mod branching;
mod properties;

fn append(document: &mut Automerge, value: i64) -> Change {
    let mut tx = document.transaction();
    tx.put(ROOT, "value", value).unwrap();
    let (hash, _) = tx.commit();
    document.get_change_by_hash(&hash.unwrap()).unwrap()
}

// Two dependent changes and an independent root, all with real content hashes.
fn history() -> (Change, Change, Change) {
    let mut chain = Automerge::new().with_actor(ActorId::from([1]));
    let a = append(&mut chain, 1);
    let b = append(&mut chain, 2);
    let mut other = Automerge::new().with_actor(ActorId::from([2]));
    let c = append(&mut other, 3);
    assert_eq!(b.deps(), &[a.hash()]);
    assert!(a.deps().is_empty());
    assert!(c.deps().is_empty());
    (a, b, c)
}

fn stage(changes: &[&Change]) -> StagedChanges {
    let mut batch = ChangeBatch::new();
    for change in changes {
        batch.push((*change).clone()).unwrap();
    }
    let mut staged = StagedChanges::new();
    staged.extend(batch);
    staged
}

fn hashes(changes: &[&Change]) -> Vec<ChangeHash> {
    changes.iter().map(|change| change.hash()).collect()
}

#[test]
fn empty_staging_prepares_an_empty_batch() {
    let staged = StagedChanges::new();
    let applied = Automerge::new();
    let prepared = staged.prepare(&applied.change_graph);
    assert!(prepared.changes().is_empty());
    assert!(staged.is_empty());
}

#[test]
fn preparation_borrows_all_ready_changes_without_consuming_them() {
    let (a, b, c) = history();
    let staged = stage(&[&b, &c, &a]);
    let before: Vec<_> = staged.iter().cloned().collect();
    let applied = Automerge::new();
    {
        let prepared = staged.prepare(&applied.change_graph);
        assert_eq!(hashes(prepared.changes()), [c.hash(), a.hash(), b.hash()]);
        for change in prepared.changes() {
            let stored = staged
                .iter()
                .find(|stored| stored.hash() == change.hash())
                .unwrap();
            assert!(std::ptr::eq(*change, stored));
        }
    }
    assert_eq!(staged.iter().cloned().collect::<Vec<_>>(), before);
    let retried = staged.prepare(&applied.change_graph);
    assert_eq!(hashes(retried.changes()), [c.hash(), a.hash(), b.hash()]);
}

#[test]
fn prepared_contents_do_not_borrow_the_applied_graph() {
    let (a, b, c) = history();
    let staged = stage(&[&a, &b, &c]);
    let prepared = {
        let applied = Automerge::new();
        staged.prepare(&applied.change_graph)
    };
    assert_eq!(hashes(prepared.changes()), [a.hash(), c.hash(), b.hash()]);
}

#[test]
fn repeated_change_delivery_emits_each_change_once() {
    let (a, b, c) = history();
    let unique = stage(&[&c, &b, &a]);
    let repeated = stage(&[&c, &b, &a, &b, &a, &c]);
    let applied = Automerge::new();
    assert_eq!(
        hashes(repeated.prepare(&applied.change_graph).changes()),
        hashes(unique.prepare(&applied.change_graph).changes())
    );
    assert_eq!(repeated.iter().count(), 3);
}

#[test]
fn missing_dependency_returns_an_empty_batch_without_consuming_staging() {
    let (_, b, _) = history();
    let staged = stage(&[&b]);
    let before: Vec<_> = staged.iter().cloned().collect();
    let applied = Automerge::new();
    assert!(staged.prepare(&applied.change_graph).changes().is_empty());
    assert_eq!(staged.iter().cloned().collect::<Vec<_>>(), before);
}

#[test]
fn applied_dependency_need_not_be_staged() {
    let (a, b, _) = history();
    let staged = stage(&[&b]);
    let before: Vec<_> = staged.iter().cloned().collect();
    let mut applied = Automerge::new();
    applied.apply_changes([a.clone()]).unwrap();
    let prepared = staged.prepare(&applied.change_graph);
    assert_eq!(hashes(prepared.changes()), [b.hash()]);
    assert_eq!(staged.iter().cloned().collect::<Vec<_>>(), before);
    assert_eq!(applied.get_heads(), [a.hash()]);
}

#[test]
fn staging_a_missing_dependency_makes_its_dependents_ready_on_retry() {
    let (a, b, _) = history();
    let mut staged = stage(&[&b]);
    let applied = Automerge::new();
    assert!(staged.prepare(&applied.change_graph).changes().is_empty());
    let mut incoming = ChangeBatch::new();
    incoming.push(a.clone()).unwrap();
    staged.extend(incoming);
    assert_eq!(
        hashes(staged.prepare(&applied.change_graph).changes()),
        [a.hash(), b.hash()]
    );
    assert_eq!(
        hashes(&staged.iter().collect::<Vec<_>>()),
        [b.hash(), a.hash()]
    );
}

#[test]
fn applying_a_missing_dependency_makes_staged_history_ready_on_retry() {
    let (a, b, _) = history();
    let staged = stage(&[&b]);
    let before: Vec<_> = staged.iter().cloned().collect();
    let mut applied = Automerge::new();
    assert!(staged.prepare(&applied.change_graph).changes().is_empty());
    applied.apply_changes([a]).unwrap();
    assert_eq!(
        hashes(staged.prepare(&applied.change_graph).changes()),
        [b.hash()]
    );
    assert_eq!(staged.iter().cloned().collect::<Vec<_>>(), before);
}
