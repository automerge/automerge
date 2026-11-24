//! Known grouping is separate from staged change contents and causal readiness.

use std::collections::BTreeSet;
use std::panic::{catch_unwind, AssertUnwindSafe};

use crate::staging::StagedGroup;
use crate::transaction::{CommitOptions, Transactable};
use crate::{ActorId, Automerge, Change, Fragment, ReadDoc, ROOT};

use super::{assert_contents, hashes, stage, ChangeBatch, StagedChanges};

mod branching;
mod completion;
mod properties;
mod redelivery;

fn at_level(doc: &mut Automerge, value: i64, level: usize) -> Change {
    for time in 0..10_000 {
        let mut next = doc.clone();
        let mut tx = next.transaction();
        tx.put(ROOT, "value", value).unwrap();
        let (hash, _) = tx.commit_with(CommitOptions::default().with_time(time));
        let hash = hash.unwrap();
        if hash.fragment_level() == level {
            let change = next.get_change_by_hash(&hash).unwrap();
            *doc = next;
            return change;
        }
    }
    panic!("fixture level search exhausted");
}

fn history(levels: &[usize]) -> (Automerge, Vec<Change>) {
    let mut doc = Automerge::new().with_actor(ActorId::from([1]));
    let changes = levels
        .iter()
        .enumerate()
        .map(|(i, level)| at_level(&mut doc, i as i64 + 1, *level))
        .collect();
    (doc, changes)
}

fn batch(changes: &[Change]) -> ChangeBatch {
    let mut batch = ChangeBatch::new();
    for change in changes {
        batch.push(change.clone()).unwrap();
    }
    batch
}

#[test]
fn raw_contents_do_not_claim_loose_grouping() {
    let (_, changes) = history(&[0, 0, 1]);
    let mut staged = StagedChanges::new();
    staged.extend(batch(&changes));
    assert!(staged.groups().is_empty());
    assert_eq!(staged.iter().count(), 3);
}

/// F={a,b,c} and G={a,d,e}, with independent actors on the two branches.
struct Overlap {
    a: Change,
    b: Change,
    c: Change,
    d: Change,
    e: Change,
    f: Fragment,
    g: Fragment,
}

fn overlap() -> Overlap {
    let (base, prefix) = history(&[0]);
    let a = prefix.into_iter().next().unwrap();
    let mut left = base.clone().with_actor(ActorId::from([2]));
    let b = at_level(&mut left, 2, 0);
    let c = at_level(&mut left, 3, 1);
    let f = left.get_fragment(c.hash()).unwrap();
    let mut right = base.with_actor(ActorId::from([3]));
    let d = at_level(&mut right, 4, 0);
    let e = at_level(&mut right, 5, 1);
    let g = right.get_fragment(e.hash()).unwrap();
    assert_eq!(
        f.members.iter().copied().collect::<BTreeSet<_>>(),
        BTreeSet::from([a.hash(), b.hash(), c.hash()])
    );
    assert_eq!(
        g.members.iter().copied().collect::<BTreeSet<_>>(),
        BTreeSet::from([a.hash(), d.hash(), e.hash()])
    );
    Overlap {
        a,
        b,
        c,
        d,
        e,
        f,
        g,
    }
}

fn stored_fragment(staged: &StagedChanges) -> &Fragment {
    let [StagedGroup::Fragment(fragment)] = staged.groups() else {
        panic!("expected exactly one known fragment");
    };
    fragment
}

fn assert_unchanged(staged: &StagedChanges, before: &StagedChanges) {
    assert_eq!(staged.changes, before.changes);
    assert_eq!(staged.hashes, before.hashes);
    assert_eq!(staged.incoming_actor_seqs, before.incoming_actor_seqs);
    assert_eq!(staged.groups().len(), before.groups().len());
    for (actual, expected) in staged.groups().iter().zip(before.groups()) {
        match (actual, expected) {
            (StagedGroup::Loose(a), StagedGroup::Loose(b)) => assert_eq!(a, b),
            (StagedGroup::Fragment(a), StagedGroup::Fragment(b)) => assert_eq!(a, b),
            _ => panic!("group classification changed"),
        }
    }
}

fn assert_invariant_panic(failure: Box<dyn std::any::Any + Send>, head: crate::ChangeHash) {
    let message = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains(&head.to_string()),
        "missing head context: {message}"
    );
}

#[test]
fn grouping_does_not_change_ready_order_or_copy_contents() {
    let (source, changes) = history(&[0, 0, 1]);
    let fragment = source.get_fragment(changes[2].hash()).unwrap();
    let applied = Automerge::new();
    // Preserve delivery order rather than sorting by descriptor membership.
    let delivery = [changes[2].clone(), changes[0].clone(), changes[1].clone()];
    let mut staged = StagedChanges::new();
    staged.extend_with_fragments(batch(&delivery), [fragment.clone()], &applied.change_graph);
    assert_eq!(stored_fragment(&staged), &fragment);
    assert_eq!(
        staged.iter().collect::<Vec<_>>(),
        delivery.iter().collect::<Vec<_>>()
    );

    let raw = stage(&delivery.iter().collect::<Vec<_>>());
    let prepared = staged.prepare(&applied.change_graph);
    assert_eq!(
        hashes(prepared.changes()),
        hashes(raw.prepare(&applied.change_graph).changes())
    );
    for change in prepared.changes() {
        let held = staged
            .iter()
            .find(|held| held.hash() == change.hash())
            .unwrap();
        assert!(std::ptr::eq(*change, held));
    }
}

#[test]
fn known_loose_and_single_member_fragment_remain_distinct() {
    let applied = Automerge::new();
    for level in [0, 1] {
        let (source, changes) = history(&[level]);
        let mut staged = StagedChanges::new();
        staged.extend_with_fragments(batch(&changes), source.fragments(..), &applied.change_graph);
        match (level, staged.groups()) {
            (0, [StagedGroup::Loose(hash)]) => assert_eq!(*hash, changes[0].hash()),
            (1, [StagedGroup::Fragment(fragment)]) => {
                assert_eq!(fragment.members, [changes[0].hash()]);
                assert_eq!(fragment.level, 1);
            }
            _ => panic!("wrong classification for level {level}"),
        }
    }
}

#[test]
fn empty_known_input_keeps_contents_unclassified() {
    let (_, changes) = history(&[0, 0, 1]);
    let applied = Automerge::new();
    let mut staged = StagedChanges::new();
    staged.extend_with_fragments(batch(&changes), [], &applied.change_graph);
    assert!(staged.groups().is_empty());
    assert_eq!(
        staged.iter().collect::<Vec<_>>(),
        changes.iter().collect::<Vec<_>>()
    );
}

#[test]
fn repeated_and_reordered_descriptors_preserve_first_descriptor_and_contents() {
    let (source, changes) = history(&[0, 0, 1]);
    let fragment = source.get_fragment(changes[2].hash()).unwrap();
    let applied = Automerge::new();
    let mut staged = StagedChanges::new();
    staged.extend_with_fragments(
        batch(&changes),
        [fragment.clone(), fragment.clone()],
        &applied.change_graph,
    );
    assert_eq!(stored_fragment(&staged), &fragment);
    assert_contents(&staged, &changes.iter().collect::<Vec<_>>());
    let mut reordered = fragment.clone();
    reordered.members.reverse();
    assert_ne!(fragment.members, reordered.members);
    let before = staged.clone();
    for descriptor in [fragment.clone(), reordered] {
        staged.extend_with_fragments(ChangeBatch::new(), [descriptor], &applied.change_graph);
        assert_unchanged(&staged, &before);
        assert_eq!(stored_fragment(&staged), &fragment);
    }
}

#[test]
fn grouping_can_arrive_after_contents_without_copying_them() {
    let (source, changes) = history(&[0, 0, 1]);
    let fragment = source.get_fragment(changes[2].hash()).unwrap();
    let applied = Automerge::new();
    let mut staged = stage(&changes.iter().collect::<Vec<_>>());
    let pointers = staged.iter().map(std::ptr::from_ref).collect::<Vec<_>>();
    staged.extend_with_fragments(
        ChangeBatch::new(),
        [fragment.clone()],
        &applied.change_graph,
    );
    assert_eq!(stored_fragment(&staged), &fragment);
    assert_eq!(
        staged.iter().map(std::ptr::from_ref).collect::<Vec<_>>(),
        pointers
    );
}

#[test]
fn wholly_applied_groups_are_not_retained() {
    let (applied, changes) = history(&[0, 0, 1]);
    let mut staged = StagedChanges::new();
    staged.extend_with_fragments(
        ChangeBatch::new(),
        applied.fragments(..),
        &applied.change_graph,
    );
    assert!(staged.groups().is_empty());
    assert!(staged.is_empty());
    assert_eq!(applied.get_heads(), [changes[2].hash()]);
}

#[test]
fn complete_contents_can_wait_on_an_external_dependency() {
    let (source, changes) = history(&[1, 0, 1]);
    let fragment = source.get_fragment(changes[2].hash()).unwrap();
    assert_eq!(
        fragment.members.iter().copied().collect::<BTreeSet<_>>(),
        BTreeSet::from([changes[1].hash(), changes[2].hash()])
    );
    let mut applied = Automerge::new();
    let mut staged = StagedChanges::new();
    staged.extend_with_fragments(
        batch(&changes[1..]),
        [fragment.clone()],
        &applied.change_graph,
    );
    assert_eq!(stored_fragment(&staged), &fragment);
    assert!(staged.prepare(&applied.change_graph).changes().is_empty());
    applied.apply_changes([changes[0].clone()]).unwrap();
    assert_eq!(
        hashes(staged.prepare(&applied.change_graph).changes()),
        [changes[1].hash(), changes[2].hash()]
    );
}

#[test]
fn overlapping_fragments_share_staged_contents() {
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
    staged.extend_with_fragments(batch(&[a.clone(), b, c]), [f], &applied.change_graph);
    staged.extend_with_fragments(batch(&[d, e]), [g], &applied.change_graph);
    assert_eq!(staged.groups().len(), 2);
    assert_eq!(staged.iter().count(), 5);
    assert_eq!(
        staged
            .iter()
            .filter(|change| change.hash() == a.hash())
            .count(),
        1
    );
    let prepared = staged.prepare(&applied.change_graph);
    assert_eq!(prepared.changes().len(), 5);
    assert_eq!(
        hashes(prepared.changes())
            .into_iter()
            .collect::<BTreeSet<_>>()
            .len(),
        5
    );
}

#[test]
fn inconsistent_descriptor_asserts_before_inserting_new_contents() {
    let (source, changes) = history(&[0, 0, 1]);
    let fragment = source.get_fragment(changes[2].hash()).unwrap();
    let applied = Automerge::new();
    let mut staged = StagedChanges::new();
    staged.extend_with_fragments(batch(&changes), [fragment.clone()], &applied.change_graph);
    let before = staged.clone();
    let mut other = Automerge::new().with_actor(ActorId::from([2]));
    let extra = at_level(&mut other, 9, 0);
    let mut inconsistent = fragment.clone();
    inconsistent
        .members
        .retain(|hash| *hash != changes[0].hash());
    let failure = catch_unwind(AssertUnwindSafe(|| {
        staged.extend_with_fragments(batch(&[extra]), [inconsistent], &applied.change_graph);
    }))
    .expect_err("descriptor disagreement must assert");
    assert_invariant_panic(failure, fragment.head);
    assert_unchanged(&staged, &before);
}

#[test]
fn same_call_disagreement_is_checked_before_any_mutation_even_if_applied() {
    let (source, changes) = history(&[0, 0, 1]);
    let fragment = source.get_fragment(changes[2].hash()).unwrap();
    let mut inconsistent = fragment.clone();
    inconsistent
        .members
        .retain(|hash| *hash != changes[0].hash());
    for already_applied in [false, true] {
        let applied = if already_applied {
            source.clone()
        } else {
            Automerge::new()
        };
        let incoming = if already_applied {
            ChangeBatch::new()
        } else {
            batch(&changes)
        };
        let mut staged = StagedChanges::new();
        let before = staged.clone();
        let failure = catch_unwind(AssertUnwindSafe(|| {
            staged.extend_with_fragments(
                incoming,
                [fragment.clone(), inconsistent.clone()],
                &applied.change_graph,
            );
        }))
        .expect_err("same-call disagreement must assert");
        assert_invariant_panic(failure, fragment.head);
        assert_unchanged(&staged, &before);
    }
}

#[test]
fn missing_member_is_not_treated_as_an_external_dependency() {
    let (source, changes) = history(&[0, 0, 1]);
    let fragment = source.get_fragment(changes[2].hash()).unwrap();
    let applied = Automerge::new();
    let mut staged = StagedChanges::new();
    let before = staged.clone();
    let failure = catch_unwind(AssertUnwindSafe(|| {
        staged.extend_with_fragments(
            batch(&changes[1..]),
            [fragment.clone()],
            &applied.change_graph,
        );
    }))
    .expect_err("an advertised member must be available");
    assert_invariant_panic(failure, fragment.head);
    assert_unchanged(&staged, &before);
}
