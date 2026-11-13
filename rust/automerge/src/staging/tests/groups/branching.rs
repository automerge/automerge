//! Discarding a branch invalidates affected grouping, not unrelated contents.

use std::collections::HashSet;

use crate::staging::StagedGroup;
use crate::{ActorId, Automerge};

use super::{
    assert_contents, assert_unchanged, at_level, batch, history, overlap, stored_fragment, Overlap,
    StagedChanges,
};

#[test]
fn discarded_suffix_invalidates_fragment_without_discarding_its_surviving_prefix() {
    let (source, changes) = history(&[0, 0, 1]);
    let fragment = source.get_fragment(changes[2].hash()).unwrap();
    assert_eq!(fragment.members.len(), 3);
    let applied = Automerge::new();
    let mut staged = StagedChanges::new();
    staged.extend_with_fragments(batch(&changes), [fragment], &applied.change_graph);

    staged.remove_actor_branch_from(changes[1].actor_id(), changes[1].seq());

    assert_contents(&staged, &[&changes[0]]);
    assert_eq!(
        staged.prepare(&applied.change_graph).changes(),
        &[&changes[0]]
    );
    assert!(
        staged.groups().is_empty(),
        "the surviving prefix must be unclassified, not a truncated fragment or loose group"
    );

    // Repeating a discard must not affect its surviving prefix.
    let after = staged.clone();
    staged.remove_actor_branch_from(changes[1].actor_id(), changes[1].seq());
    assert_unchanged(&staged, &after);
}

#[test]
fn discarding_one_overlap_preserves_the_other_full_descriptor_and_shared_contents() {
    let Overlap {
        a,
        b,
        c,
        d,
        e,
        f,
        g,
    } = overlap();
    let applied = Automerge::new();
    let mut staged = StagedChanges::new();
    // Interleave contents so retaining a group cannot justify rebuilding their order.
    let contents = [d.clone(), b.clone(), a.clone(), c, e.clone()];
    staged.extend_with_fragments(batch(&contents), [f, g.clone()], &applied.change_graph);

    staged.remove_actor_branch_from(b.actor_id(), b.seq());

    assert_contents(&staged, &[&d, &a, &e]);
    let prepared = staged.prepare(&applied.change_graph);
    assert_eq!(prepared.changes(), &[&a, &d, &e]);
    for change in prepared.changes() {
        let held = staged
            .iter()
            .find(|held| held.hash() == change.hash())
            .unwrap();
        assert!(std::ptr::eq(*change, held));
    }
    assert_eq!(stored_fragment(&staged), &g);
}

#[test]
fn discarding_a_loose_group_preserves_unrelated_groups_and_their_order() {
    let (removed_source, removed_changes) = history(&[0]);
    let removed = &removed_changes[0];
    let mut first_doc = Automerge::new().with_actor(ActorId::from([2]));
    let first = at_level(&mut first_doc, 2, 0);
    let mut fragment_doc = Automerge::new().with_actor(ActorId::from([3]));
    let member = at_level(&mut fragment_doc, 3, 1);
    let fragment = fragment_doc.get_fragment(member.hash()).unwrap();
    let mut last_doc = Automerge::new().with_actor(ActorId::from([4]));
    let last = at_level(&mut last_doc, 4, 0);
    let applied = Automerge::new();
    let mut staged = StagedChanges::new();
    staged.extend_with_fragments(
        batch(&[last.clone(), removed.clone(), member.clone(), first.clone()]),
        [
            first_doc.get_fragment(first.hash()).unwrap(),
            removed_source.get_fragment(removed.hash()).unwrap(),
            fragment.clone(),
            last_doc.get_fragment(last.hash()).unwrap(),
        ],
        &applied.change_graph,
    );

    staged.remove_actor_branch_from(removed.actor_id(), removed.seq());

    assert_contents(&staged, &[&last, &member, &first]);
    assert_eq!(
        staged.prepare(&applied.change_graph).changes(),
        &[&last, &member, &first]
    );
    let [StagedGroup::Loose(first_hash), StagedGroup::Fragment(kept), StagedGroup::Loose(last_hash)] =
        staged.groups()
    else {
        panic!("expected the three unaffected groups in their original order");
    };
    assert_eq!(*first_hash, first.hash());
    assert_eq!(kept, &fragment);
    assert_eq!(*last_hash, last.hash());
}

#[test]
fn transitive_discard_invalidates_a_fragment_on_another_actor() {
    let (base, prefix) = history(&[1]);
    let mut descendant = base.with_actor(ActorId::from([2]));
    let b = at_level(&mut descendant, 2, 0);
    let c = at_level(&mut descendant, 3, 1);
    let fragment = descendant.get_fragment(c.hash()).unwrap();
    assert_eq!(
        fragment.members.iter().copied().collect::<HashSet<_>>(),
        HashSet::from([b.hash(), c.hash()])
    );
    assert!(fragment.boundary.contains(&prefix[0].hash()));
    let mut other = Automerge::new().with_actor(ActorId::from([3]));
    let independent = at_level(&mut other, 4, 0);
    let applied = Automerge::new();
    let mut staged = StagedChanges::new();
    staged.extend_with_fragments(
        batch(&[c, independent.clone(), b, prefix[0].clone()]),
        [fragment, other.get_fragment(independent.hash()).unwrap()],
        &applied.change_graph,
    );

    // The directly discarded root is outside the fragment; its members are
    // removed through dependencies, across the actor boundary.
    staged.remove_actor_branch_from(prefix[0].actor_id(), prefix[0].seq());

    assert_contents(&staged, &[&independent]);
    assert_eq!(
        staged.prepare(&applied.change_graph).changes(),
        &[&independent]
    );
    assert!(
        matches!(staged.groups(), [StagedGroup::Loose(hash)] if *hash == independent.hash()),
        "transitively discarded members must invalidate their fragment"
    );
}

#[test]
fn discarding_a_partly_completed_clone_does_not_change_the_original() {
    let (source, changes) = history(&[0, 0, 1]);
    let fragment = source.get_fragment(changes[2].hash()).unwrap();
    let mut applied = Automerge::new();
    let mut original = StagedChanges::new();
    original.extend_with_fragments(batch(&changes), [fragment.clone()], &applied.change_graph);
    applied.apply_changes([changes[0].clone()]).unwrap();
    original.remove_completed(&HashSet::from([changes[0].hash()]), &applied.change_graph);
    let mut copy = original.clone();

    copy.remove_actor_branch_from(changes[1].actor_id(), changes[1].seq());

    assert_contents(&copy, &[]);
    assert_contents(&original, &[&changes[1], &changes[2]]);
    assert_eq!(stored_fragment(&original), &fragment);
    assert_eq!(
        original.prepare(&applied.change_graph).changes(),
        &[&changes[1], &changes[2]]
    );
    assert!(
        copy.groups().is_empty(),
        "discarding the remaining members must invalidate the partly completed group"
    );
}

#[test]
fn absent_actor_or_later_sequence_leaves_grouping_and_contents_unchanged() {
    let (source, changes) = history(&[0, 0, 1]);
    let fragment = source.get_fragment(changes[2].hash()).unwrap();
    let applied = Automerge::new();
    let mut staged = StagedChanges::new();
    staged.extend_with_fragments(batch(&changes), [fragment], &applied.change_graph);
    let before = staged.clone();

    staged.remove_actor_branch_from(&ActorId::from([99]), 1);
    assert_unchanged(&staged, &before);
    staged.remove_actor_branch_from(changes[2].actor_id(), changes[2].seq() + 1);
    assert_unchanged(&staged, &before);
}
