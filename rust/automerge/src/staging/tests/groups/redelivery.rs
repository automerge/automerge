//! Contents can return without reviving discarded grouping knowledge.

use std::collections::HashSet;

use crate::staging::StagedGroup;
use crate::{Automerge, Fragment};

use super::{assert_contents, assert_unchanged, batch, overlap, Overlap, StagedChanges};

fn assert_fragments(staged: &StagedChanges, expected: &[&Fragment]) {
    assert_eq!(staged.groups().len(), expected.len());
    for (actual, expected) in staged.groups().iter().zip(expected) {
        let StagedGroup::Fragment(actual) = actual else {
            panic!("expected an unchanged canonical descriptor");
        };
        assert_eq!(actual, *expected);
    }
}

#[test]
fn descriptor_redelivery_after_partial_completion_keeps_full_membership_and_order() {
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
    staged.extend_with_fragments(
        batch(&[a.clone(), b.clone(), c.clone(), d.clone(), e.clone()]),
        [f.clone(), g.clone()],
        &applied.change_graph,
    );
    applied.apply_changes([a.clone()]).unwrap();
    staged.remove_completed(&HashSet::from([a.hash()]), &applied.change_graph);
    let before = staged.clone();
    let pointers: Vec<_> = staged.iter().map(std::ptr::from_ref).collect();

    // The importer deduplicates already-applied/staged contents before insertion.
    // At this seam redelivery therefore supplies only the full descriptor.
    let mut reordered = f.clone();
    reordered.members.reverse();
    staged.extend_with_fragments(batch(&[]), [reordered], &applied.change_graph);

    assert_contents(&staged, &[&b, &c, &d, &e]);
    assert_fragments(&staged, &[&f, &g]);
    assert_unchanged(&staged, &before);
    assert_eq!(
        staged.iter().map(std::ptr::from_ref).collect::<Vec<_>>(),
        pointers
    );
}

#[test]
fn redelivery_after_discard_appends_restored_contents_and_descriptor_after_survivors() {
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
    let mut original = StagedChanges::new();
    original.extend_with_fragments(
        batch(&[a.clone(), b.clone(), c.clone(), d.clone(), e.clone()]),
        [f.clone(), g.clone()],
        &applied.change_graph,
    );
    let mut staged = original.clone();
    staged.remove_actor_branch_from(b.actor_id(), b.seq());
    assert_contents(&staged, &[&a, &d, &e]);
    assert_fragments(&staged, &[&g]);

    // Missing b/c return in c,b order, while shared a is already held.
    // The restored F follows G: its old descriptor position is not resurrected.
    let mut restored = f.clone();
    restored.members.reverse();
    staged.extend_with_fragments(
        batch(&[c.clone(), b.clone()]),
        [restored.clone(), restored.clone()],
        &applied.change_graph,
    );
    assert_contents(&staged, &[&a, &d, &e, &c, &b]);
    assert_fragments(&staged, &[&g, &restored]);
    assert_eq!(staged.prepare(&applied.change_graph).changes().len(), 5);
    let before_retry = staged.clone();
    staged.extend_with_fragments(batch(&[]), [f.clone()], &applied.change_graph);
    assert_unchanged(&staged, &before_retry);
    assert_contents(&original, &[&a, &b, &c, &d, &e]);
    assert_fragments(&original, &[&f, &g]);
}

#[test]
fn contents_only_redelivery_does_not_restore_grouping_until_its_descriptor_arrives() {
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
    staged.extend_with_fragments(
        batch(&[a.clone(), b.clone(), c.clone(), d.clone(), e.clone()]),
        [f.clone(), g.clone()],
        &applied.change_graph,
    );
    staged.remove_actor_branch_from(b.actor_id(), b.seq());

    staged.extend(batch(&[c.clone(), b.clone()]));
    assert_contents(&staged, &[&a, &d, &e, &c, &b]);
    assert_fragments(&staged, &[&g]);
    let pointers: Vec<_> = staged.iter().map(std::ptr::from_ref).collect();

    staged.extend_with_fragments(batch(&[]), [f.clone()], &applied.change_graph);
    assert_fragments(&staged, &[&g, &f]);
    assert_eq!(
        staged.iter().map(std::ptr::from_ref).collect::<Vec<_>>(),
        pointers
    );
}
