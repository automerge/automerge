use std::collections::HashSet;

use crate::{ActorId, Automerge, Change, ChangeHash};

use super::{append, hashes, history, stage, ChangeBatch, StagedChanges};

// Internal indexes are part of removal's invariant. Check them without adding
// production accessors solely for tests. Membership alone would miss stale entries.
fn assert_contents(staged: &StagedChanges, expected: &[&Change]) {
    assert_eq!(staged.iter().collect::<Vec<_>>(), expected);
    assert_eq!(staged.is_empty(), expected.is_empty());
    assert_eq!(
        staged.hashes,
        expected
            .iter()
            .map(|change| change.hash())
            .collect::<HashSet<_>>()
    );
    assert_eq!(
        staged.incoming_actor_seqs,
        expected
            .iter()
            .map(|change| (change.actor_id().clone(), change.seq()))
            .collect::<HashSet<_>>()
    );
}

#[test]
fn completion_preserves_dependent_changes_and_remaining_order() {
    let (a, b, c) = history();
    let mut staged = stage(&[&b, &a, &c]);
    // a is completed, but b has not been. Completion is not branch eviction.
    let mut applied = Automerge::new();
    applied.apply_changes([a.clone()]).unwrap();
    staged.remove_completed(&HashSet::from([a.hash()]));
    assert_contents(&staged, &[&b, &c]);
    assert_eq!(
        hashes(staged.prepare(&applied.change_graph).changes()),
        [b.hash(), c.hash()]
    );
}

#[test]
fn empty_unknown_and_repeated_completion_are_harmless() {
    let (a, b, c) = history();
    let mut staged = stage(&[&b, &a]);
    staged.remove_completed(&HashSet::new());
    assert_contents(&staged, &[&b, &a]);
    // Use a real, unstaged change rather than a fabricated hash.
    staged.remove_completed(&HashSet::from([c.hash()]));
    assert_contents(&staged, &[&b, &a]);
    let completed = HashSet::from([a.hash(), c.hash()]);
    staged.remove_completed(&completed);
    assert_contents(&staged, &[&b]);
    staged.remove_completed(&completed);
    assert_contents(&staged, &[&b]);
    staged.remove_completed(&HashSet::from([b.hash()]));
    assert_contents(&staged, &[]);
    staged.remove_completed(&completed);
    assert_contents(&staged, &[]);
}

#[test]
fn completion_releases_indexes_for_subsequent_staging() {
    let (a, b, c) = history();
    let mut staged = stage(&[&a, &c]);
    staged.remove_completed(&HashSet::from([a.hash(), c.hash()]));
    assert_contents(&staged, &[]);

    // Exercise the staging primitive independently of the importer's applied-hash
    // filter. Stale hash or actor/sequence entries would prevent reusing the pool.
    let mut incoming = ChangeBatch::new();
    incoming.push(a.clone()).unwrap();
    incoming.push(b.clone()).unwrap();
    incoming.push(c.clone()).unwrap();
    staged.extend(incoming);
    assert_contents(&staged, &[&a, &b, &c]);
    let applied = Automerge::new();
    assert_eq!(
        hashes(staged.prepare(&applied.change_graph).changes()),
        [a.hash(), c.hash(), b.hash()]
    );
}

#[test]
fn prepare_apply_complete_keeps_blocked_history_for_a_later_attempt() {
    let (a, b, c) = history();
    let mut blocked_source = Automerge::new().with_actor(ActorId::from([3]));
    let missing = append(&mut blocked_source, 4);
    let blocked = append(&mut blocked_source, 5);
    let mut staged = stage(&[&blocked, &b, &c, &a]);
    let mut applied = Automerge::new();

    // A dropped preparation does not imply completion.
    {
        let prepared = staged.prepare(&applied.change_graph);
        assert_eq!(hashes(prepared.changes()), [c.hash(), a.hash(), b.hash()]);
    }
    assert_contents(&staged, &[&blocked, &b, &c, &a]);

    let completed: HashSet<ChangeHash> = {
        let prepared = staged.prepare(&applied.change_graph);
        // Fixture application owns copies because borrowed production application
        // is a separate integration step. Collect completion hashes only on success.
        applied
            .apply_changes(
                prepared
                    .changes()
                    .iter()
                    .map(|c| (*c).clone())
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        hashes(prepared.changes()).into_iter().collect()
    };
    staged.remove_completed(&completed);
    assert_contents(&staged, &[&blocked]);
    assert!(staged.prepare(&applied.change_graph).changes().is_empty());

    applied.apply_changes([missing]).unwrap();
    let completed = {
        let prepared = staged.prepare(&applied.change_graph);
        assert_eq!(hashes(prepared.changes()), [blocked.hash()]);
        applied
            .apply_changes(
                prepared
                    .changes()
                    .iter()
                    .map(|c| (*c).clone())
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        hashes(prepared.changes()).into_iter().collect()
    };
    staged.remove_completed(&completed);
    assert_contents(&staged, &[]);
    assert!(applied.change_graph.has_change(&blocked.hash()));
}
