use crate::{ActorId, Automerge, Change};

use super::{append, hashes, stage};

// Each node is a real local commit, made after applying exactly its parents.
fn node(actor: u8, parents: &[&Change]) -> Change {
    let mut document = Automerge::new().with_actor(ActorId::from([actor]));
    document
        .apply_changes(parents.iter().map(|c| (*c).clone()).collect::<Vec<_>>())
        .unwrap();
    append(&mut document, i64::from(actor))
}

#[test]
fn join_waits_for_both_independent_dependencies() {
    let a = node(1, &[]);
    let b = node(2, &[]);
    let c = node(3, &[&a, &b]);
    assert_eq!(c.deps().len(), 2);
    let staged = stage(&[&c, &a, &b]);
    let before: Vec<_> = staged.iter().cloned().collect();
    let applied = Automerge::new();
    let prepared = staged.prepare(&applied.change_graph);
    assert_eq!(hashes(prepared.changes()), [a.hash(), b.hash(), c.hash()]);
    assert_eq!(staged.iter().cloned().collect::<Vec<_>>(), before);
}

#[test]
fn join_waits_only_for_the_dependency_not_already_applied() {
    let a = node(1, &[]);
    let b = node(2, &[]);
    let c = node(3, &[&a, &b]);
    let staged = stage(&[&c, &b]);
    let mut applied = Automerge::new();
    applied.apply_changes([a.clone()]).unwrap();
    let prepared = staged.prepare(&applied.change_graph);
    assert_eq!(hashes(prepared.changes()), [b.hash(), c.hash()]);
    assert_eq!(applied.get_heads(), [a.hash()]);
}

#[test]
fn missing_join_dependency_blocks_the_join_and_its_descendants() {
    let a = node(1, &[]);
    let b = node(2, &[]);
    let c = node(3, &[&a, &b]);
    let d = node(4, &[&a, &b, &c]);
    let staged = stage(&[&d, &c, &a]);
    let before: Vec<_> = staged.iter().cloned().collect();
    let mut applied = Automerge::new();
    assert_eq!(
        hashes(staged.prepare(&applied.change_graph).changes()),
        [a.hash()]
    );
    assert_eq!(staged.iter().cloned().collect::<Vec<_>>(), before);
    // Applying the absent parent makes the join and its descendant available.
    applied.apply_changes([b]).unwrap();
    let prepared = staged.prepare(&applied.change_graph);
    assert_eq!(hashes(prepared.changes()), [a.hash(), c.hash(), d.hash()]);
    assert_eq!(staged.iter().cloned().collect::<Vec<_>>(), before);
}

#[test]
fn diamond_releases_all_dependents_and_emits_the_join_once() {
    let a = node(1, &[]);
    let b = node(2, &[&a]);
    let c = node(3, &[&a]);
    let d = node(4, &[&a, &b, &c]);
    assert_eq!(d.deps().len(), 2);
    assert!(d.deps().contains(&b.hash()));
    assert!(d.deps().contains(&c.hash()));
    let staged = stage(&[&d, &c, &b, &a]);
    let applied = Automerge::new();
    let prepared = staged.prepare(&applied.change_graph);
    assert_eq!(
        hashes(prepared.changes()),
        [a.hash(), c.hash(), b.hash(), d.hash()]
    );
}

#[test]
fn newly_ready_changes_follow_roots_already_waiting_in_the_fifo() {
    let a = node(1, &[]);
    let b = node(2, &[&a]);
    let c = node(3, &[]);
    let staged = stage(&[&b, &a, &c]);
    let applied = Automerge::new();
    let prepared = staged.prepare(&applied.change_graph);
    // a releases b, but c was already in the ready queue.
    assert_eq!(hashes(prepared.changes()), [a.hash(), c.hash(), b.hash()]);
}

#[test]
fn unrelated_blocked_history_does_not_prevent_preparing_ready_history() {
    let a = node(1, &[]);
    let blocked = node(2, &[&a]);
    let c = node(3, &[]);
    let staged = stage(&[&blocked, &c]);
    let applied = Automerge::new();
    let prepared = staged.prepare(&applied.change_graph);
    assert_eq!(hashes(prepared.changes()), [c.hash()]);
    assert_eq!(
        hashes(&staged.iter().collect::<Vec<_>>()),
        [blocked.hash(), c.hash()]
    );
}
