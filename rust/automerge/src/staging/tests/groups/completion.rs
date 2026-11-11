//! Completion retires grouping only after its complete membership is applied.

use std::collections::HashSet;

use crate::staging::StagedGroup;
use crate::{ActorId, Automerge, Change};

use super::{
    assert_contents, assert_unchanged, at_level, batch, history, overlap, stored_fragment, Overlap,
    StagedChanges,
};

#[test]
fn partial_completion_preserves_full_descriptor_until_every_member_is_applied() {
    let (source, changes) = history(&[0, 0, 1]);
    let fragment = source.get_fragment(changes[2].hash()).unwrap();
    let mut applied = Automerge::new();
    let mut staged = StagedChanges::new();
    staged.extend_with_fragments(batch(&changes), [fragment.clone()], &applied.change_graph);

    applied.apply_changes([changes[0].clone()]).unwrap();
    staged.remove_completed(&HashSet::from([changes[0].hash()]), &applied.change_graph);
    assert_eq!(stored_fragment(&staged), &fragment);
    assert_contents(&staged, &[&changes[1], &changes[2]]);

    applied.apply_changes([changes[1].clone()]).unwrap();
    staged.remove_completed(&HashSet::from([changes[1].hash()]), &applied.change_graph);
    assert_eq!(stored_fragment(&staged), &fragment);
    assert_contents(&staged, &[&changes[2]]);

    applied.apply_changes([changes[2].clone()]).unwrap();
    let completed = HashSet::from([changes[2].hash()]);
    staged.remove_completed(&completed, &applied.change_graph);
    assert_contents(&staged, &[]);
    assert!(staged.groups().is_empty());
    staged.remove_completed(&completed, &applied.change_graph);
    assert!(staged.groups().is_empty());
}

#[test]
fn completion_retires_a_fragment_with_members_applied_before_staging() {
    let (source, changes) = history(&[0, 0, 1]);
    let fragment = source.get_fragment(changes[2].hash()).unwrap();
    let mut applied = Automerge::new();
    applied.apply_changes([changes[0].clone()]).unwrap();
    let mut staged = StagedChanges::new();
    staged.extend_with_fragments(
        batch(&changes[1..]),
        [fragment.clone()],
        &applied.change_graph,
    );
    assert_eq!(stored_fragment(&staged), &fragment);
    assert_contents(&staged, &[&changes[1], &changes[2]]);
    assert_eq!(
        staged.prepare(&applied.change_graph).changes(),
        &[&changes[1], &changes[2]]
    );

    applied.apply_changes(changes[1..].to_vec()).unwrap();
    staged.remove_completed(
        &changes[1..].iter().map(Change::hash).collect(),
        &applied.change_graph,
    );
    assert_contents(&staged, &[]);
    assert!(staged.groups().is_empty());
}

#[test]
fn completion_retires_loose_and_single_member_fragment_groups() {
    for level in [0, 1] {
        let (source, changes) = history(&[level]);
        let mut applied = Automerge::new();
        let mut staged = StagedChanges::new();
        staged.extend_with_fragments(batch(&changes), source.fragments(..), &applied.change_graph);
        match (level, staged.groups()) {
            (0, [StagedGroup::Loose(hash)]) => assert_eq!(*hash, changes[0].hash()),
            (1, [StagedGroup::Fragment(fragment)]) => {
                assert_eq!(fragment.members, [changes[0].hash()]);
            }
            _ => panic!("unexpected fixture grouping"),
        }

        applied.apply_changes(changes.clone()).unwrap();
        staged.remove_completed(&HashSet::from([changes[0].hash()]), &applied.change_graph);
        assert_contents(&staged, &[]);
        assert!(staged.groups().is_empty());
    }
}

#[test]
fn completing_one_overlap_preserves_the_other_full_descriptor_and_contents() {
    let Overlap {
        a,
        b,
        c,
        d,
        e,
        f,
        g,
    } = overlap();

    let mut applied = Automerge::new();
    let mut staged = StagedChanges::new();
    // Deliberately interleave the two groups' contents to pin survivor order.
    let contents = [d.clone(), a.clone(), b.clone(), e.clone(), c.clone()];
    staged.extend_with_fragments(batch(&contents), [f, g.clone()], &applied.change_graph);
    applied
        .apply_changes([a.clone(), b.clone(), c.clone()])
        .unwrap();
    staged.remove_completed(
        &HashSet::from([a.hash(), b.hash(), c.hash()]),
        &applied.change_graph,
    );

    assert_contents(&staged, &[&d, &e]);
    assert_eq!(stored_fragment(&staged), &g);
    let prepared = staged.prepare(&applied.change_graph);
    assert_eq!(prepared.changes(), &[&d, &e]);

    applied.apply_changes([d.clone(), e.clone()]).unwrap();
    staged.remove_completed(&HashSet::from([d.hash(), e.hash()]), &applied.change_graph);
    assert_contents(&staged, &[]);
    assert!(staged.groups().is_empty());
}

#[test]
fn completion_of_a_clone_does_not_retire_the_originals_group() {
    let (source, changes) = history(&[0, 0, 1]);
    let fragment = source.get_fragment(changes[2].hash()).unwrap();
    let mut applied = Automerge::new();
    let mut original = StagedChanges::new();
    original.extend_with_fragments(batch(&changes), [fragment.clone()], &applied.change_graph);
    let mut copy = original.clone();

    applied.apply_changes(changes.clone()).unwrap();
    copy.remove_completed(
        &changes.iter().map(Change::hash).collect(),
        &applied.change_graph,
    );
    assert_contents(&copy, &[]);
    assert!(copy.groups().is_empty());
    assert_contents(&original, &changes.iter().collect::<Vec<_>>());
    assert_eq!(stored_fragment(&original), &fragment);
}

#[test]
fn empty_and_unknown_completion_do_not_retire_unapplied_groups() {
    let (source, changes) = history(&[0, 0, 1]);
    let applied = Automerge::new();
    let mut staged = StagedChanges::new();
    staged.extend_with_fragments(batch(&changes), source.fragments(..), &applied.change_graph);
    let before = staged.clone();

    staged.remove_completed(&HashSet::new(), &applied.change_graph);
    assert_unchanged(&staged, &before);
    let mut other = Automerge::new().with_actor(ActorId::from([2]));
    let unknown = at_level(&mut other, 9, 0);
    staged.remove_completed(&HashSet::from([unknown.hash()]), &applied.change_graph);
    assert_unchanged(&staged, &before);
}

#[test]
fn empty_completion_still_retires_groups_when_the_applied_graph_has_advanced() {
    let (source, changes) = history(&[0, 0, 1]);
    let mut applied = Automerge::new();
    let mut staged = StagedChanges::new();
    staged.extend_with_fragments(batch(&changes), source.fragments(..), &applied.change_graph);
    applied.apply_changes(changes.clone()).unwrap();

    // Model already-released contents with stale grouping. The cleanup must
    // consult the applied graph even when this call reports no new byte removal.
    staged.changes.clear();
    staged.hashes.clear();
    staged.incoming_actor_seqs.clear();
    assert!(!staged.groups().is_empty());
    staged.remove_completed(&HashSet::new(), &applied.change_graph);
    assert_contents(&staged, &[]);
    assert!(staged.groups().is_empty());
}
