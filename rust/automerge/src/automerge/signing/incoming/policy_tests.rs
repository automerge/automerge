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
