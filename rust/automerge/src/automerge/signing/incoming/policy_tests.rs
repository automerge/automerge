use super::{
    evaluation, evaluation_batch, intrinsic_chain, intrinsic_index, intrinsic_shape, topology_input,
};
use std::collections::HashMap;

#[test]
fn admits_repeated_hashes_across_occurrences_but_not_inside_a_group_or_slot() {
    let member = |hash: &u8| intrinsic_index::Member {
        actor: 1,
        seq: 2,
        hash: *hash,
        has_author: false,
        conflicts_with_applied: false,
    };
    let mut batch = HashMap::new();
    assert!(intrinsic_index::index(&[5], member, &mut batch).is_ok());
    assert!(intrinsic_index::index(&[5], member, &mut batch).is_ok());
    assert_eq!(
        intrinsic_index::index(&[5, 5], member, &mut batch),
        Err((1, intrinsic_index::Fault::DuplicateHash))
    );
    assert_eq!(
        intrinsic_index::index(&[6], member, &mut batch),
        Err((0, intrinsic_index::Fault::ConflictingSlot))
    );
    assert_eq!(batch, HashMap::from([((1, 2), 5)]));
}

#[test]
fn unique_head_alone_is_not_sufficient_for_a_group_to_be_wholly_reachable() {
    let deps: [&[usize]; 4] = [&[1], &[], &[3], &[2]];
    let members = [0, 1, 2, 3];
    let head = intrinsic_shape::unique_head(&members, |node| deps[node], |dep| dep < members.len());
    // The disconnected cycle has no head of its own.
    assert_eq!(head, Some(0));
    assert!(!intrinsic_shape::spans(&members, &[0, 1]));
}

#[test]
fn checks_exact_edges_in_sorted_actor_runs_and_reports_original_position() {
    let mut members = [(1, 3, 0), (2, 10, 1), (1, 2, 2)];
    assert!(intrinsic_chain::check(&mut members, |pred, child| (pred, child) == (2, 0)).is_ok());
    assert_eq!(intrinsic_chain::check(&mut members, |_, _| false), Err(0));
}

#[test]
fn broken_ancestry_wins_over_waiting_and_does_not_trigger_proof_checks() {
    let status = evaluation::ancestry(&[Ok(false), Err("broken")], |member| *member);
    let (_, result) = evaluation::decide(
        status,
        (),
        || panic!("equivocation"),
        |_| -> ((), Result<(), &str>) { panic!("canonicality") },
        || -> Result<(), &str> { panic!("proof") },
    );
    assert_eq!(result, Err("broken"));
}

#[test]
fn batch_partition_preserves_waiting_and_verified_occurrence_indices() {
    use evaluation_batch::Outcome::*;
    let results = evaluation_batch::assess(&[0, 1, 0, 2], (), |state, value| {
        (
            state,
            Ok::<_, ()>(match value {
                0 => Verified(value * 10),
                1 => Waiting,
                _ => Dropped,
            }),
        )
    });
    assert_eq!(results, Ok((vec![1], vec![(0, 0), (2, 0)])));
}

#[test]
fn topology_counts_missing_and_repeated_parents_without_reversing_applied_or_missing_edges() {
    use topology_input::Parent::*;
    let parents: [&[usize]; 2] = [&[], &[0, 0, 2, 3]];
    let (waiting, reverse) = topology_input::build(
        &[0, 1],
        |node| parents[node],
        |parent| match parent {
            0 => Candidate(0),
            2 => Applied,
            _ => Missing,
        },
    );
    assert_eq!(waiting, [0, 3]);
    assert_eq!(reverse, [vec![1, 1], vec![]]);
}

#[test]
fn indexes_keep_the_requested_occurrence_even_when_the_first_payload_is_none() {
    use super::snapshot_index::{build, Keep};
    let groups = [
        vec![(1, None), (2, Some(20))],
        vec![(1, Some(10)), (3, Some(30))],
    ];
    let first = build(&groups, Vec::as_slice, |row| Some(*row), Keep::First);
    let last = build(&groups, Vec::as_slice, |row| Some(*row), Keep::Last);
    assert_eq!(
        first,
        HashMap::from([(1, None), (2, Some(20)), (3, Some(30))])
    );
    assert_eq!(
        last,
        HashMap::from([(1, Some(10)), (2, Some(20)), (3, Some(30))])
    );
}

#[test]
fn a_resolved_authorless_change_is_neither_missing_nor_broken() {
    use super::actor_chains::{AuthorSource, Resolution};
    use super::author_projection;
    let hashes = [10, 20, 30];
    let (authors, broken) = author_projection::materialize(
        &hashes,
        &[
            Resolution::Linked(AuthorSource::Initial(0)),
            Resolution::Broken(1),
            Resolution::Linked(AuthorSource::Applied(5)),
        ],
        |root| match root {
            0 => None,
            _ => unreachable!(),
        },
        |slot| match slot {
            5 => Some(42),
            _ => unreachable!(),
        },
    );
    assert_eq!(authors, HashMap::from([(10, None), (30, Some(42))]));
    assert_eq!(broken, HashMap::from([(20, 20)]));
}

#[test]
fn prepared_payloads_are_selected_by_verified_occurrence_order() {
    let selected = super::plan_contents::take_selected(
        vec!["unselected", "first", "second", "discard"],
        &[1, 2],
        |value| value,
    );
    assert_eq!(selected, ["first", "second"]);
}

#[test]
fn delivery_members_are_compared_as_sets_and_candidate_origin_is_preserved() {
    use super::{candidate_origin, delivery, pending::Origin};
    assert!(delivery::same_members(&[1, 2], &[2, 1], |x| *x));
    assert!(!delivery::same_members(&[1, 1], &[1, 2], |x| *x));
    assert!(!delivery::same_members(&[1, 2], &[1, 3], |x| *x));
    let tagged = candidate_origin::candidates(vec!["old"], vec!["new"]);
    assert!(matches!(tagged[0].origin, Origin::ExistingPending));
    assert_eq!(tagged[0].group, "old");
    assert!(matches!(tagged[1].origin, Origin::IncomingBatch));
    assert_eq!(tagged[1].group, "new");
}
