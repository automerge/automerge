//! Grouped input shares the ordinary import preflight and causal application.

use std::collections::HashSet;
use std::panic::{catch_unwind, AssertUnwindSafe};

use crate::staging::StagedGroup;
use crate::transaction::{CommitOptions, Transactable};
use crate::{ActorId, Automerge, AutomergeError, Change, Fragment, PatchLog, ReadDoc, ROOT};

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
    let mut source = Automerge::new().with_actor(ActorId::from([1]));
    let changes = levels
        .iter()
        .enumerate()
        .map(|(i, level)| at_level(&mut source, i as i64 + 1, *level))
        .collect();
    (source, changes)
}

fn assert_fragment(doc: &Automerge, expected: &Fragment) {
    let [StagedGroup::Fragment(actual)] = doc.staged.groups() else {
        panic!("expected exactly one retained fragment");
    };
    assert_eq!(actual, expected);
}

#[test]
fn grouped_import_matches_raw_order_and_patches_with_an_applied_prefix() {
    let (source, changes) = history(&[0, 0, 1]);
    let fragment = source.get_fragment(changes[2].hash()).unwrap();
    let mut grouped = Automerge::new();
    grouped.apply_changes([changes[0].clone()]).unwrap();
    let mut raw = grouped.clone();
    let incoming = grouped.get_changes_added(&source);
    assert_eq!(incoming.len(), 2);
    let mut grouped_log = PatchLog::active();
    let mut raw_log = PatchLog::active();

    // Repeat the original delivery: even fully applied descriptors are checked,
    // but must not leave grouping behind or emit duplicate patches.
    for _ in 0..2 {
        grouped
            .apply_changes_with_fragments_log_patches(
                incoming.clone(),
                vec![fragment.clone()],
                &mut grouped_log,
            )
            .unwrap();
        raw.apply_changes_batch_log_patches(incoming.clone(), &mut raw_log)
            .unwrap();
        assert_eq!(grouped.get_heads(), source.get_heads());
        assert_eq!(grouped.get_heads(), raw.get_heads());
        assert_eq!(grouped.hydrate(None), raw.hydrate(None));
        assert_eq!(grouped.save_nocompress(), raw.save_nocompress());
        assert_eq!(
            grouped.make_patches(&mut grouped_log),
            raw.make_patches(&mut raw_log)
        );
        assert!(grouped.staged.is_empty());
        assert!(grouped.staged.groups().is_empty());
        assert!(grouped.get_missing_deps(&[]).is_empty());
    }
}

#[test]
fn grouped_import_keeps_blocked_fragment_while_applying_ready_loose_history() {
    let (mut source, changes) = history(&[1, 0, 1]);
    let mut independent = Automerge::new().with_actor(ActorId::from([2]));
    let ready = at_level(&mut independent, 4, 0);
    source.apply_changes([ready.clone()]).unwrap();
    let fragment = source.get_fragment(changes[2].hash()).unwrap();
    assert_eq!(
        fragment.members.iter().copied().collect::<HashSet<_>>(),
        HashSet::from([changes[1].hash(), changes[2].hash()])
    );
    assert!(fragment.boundary.contains(&changes[0].hash()));
    let loose = source.get_fragment(ready.hash()).unwrap();
    assert_eq!(loose.level, 0);
    assert_eq!(loose.members, [ready.hash()]);
    let cover = source.fragments(..);
    assert!(cover.contains(&fragment));
    assert!(
        cover.contains(&loose),
        "ready loose descriptor must belong to the source cover"
    );
    let mut receiver = Automerge::new();
    // Deliberately not causal order; missing a blocks b/c, not the loose root.
    receiver
        .apply_changes_with_fragments_log_patches(
            [changes[2].clone(), ready.clone(), changes[1].clone()],
            vec![loose, fragment.clone()],
            &mut PatchLog::inactive(),
        )
        .unwrap();
    assert_eq!(receiver.get_heads(), [ready.hash()]);
    assert_eq!(receiver.get_missing_deps(&[]), [changes[0].hash()]);
    assert_eq!(
        receiver.staged.iter().collect::<Vec<_>>(),
        [&changes[2], &changes[1]]
    );
    assert_fragment(&receiver, &fragment);

    // The unclassified route must complete previously grouped work as well.
    receiver.apply_changes([changes[0].clone()]).unwrap();
    assert_eq!(receiver.get_heads(), source.get_heads());
    assert_eq!(receiver.hydrate(None), source.hydrate(None));
    assert!(receiver.staged.is_empty());
    assert!(receiver.staged.groups().is_empty());
}

#[test]
fn grouping_is_processed_when_all_supplied_contents_are_already_staged() {
    let (source, changes) = history(&[1, 0, 1]);
    let fragment = source.get_fragment(changes[2].hash()).unwrap();
    let contents = vec![changes[2].clone(), changes[1].clone()];
    let mut receiver = Automerge::new();
    receiver.apply_changes(contents.clone()).unwrap();
    assert!(receiver.staged.groups().is_empty());

    // First send duplicate contents, then just the descriptor. Both yield an
    // empty validated batch, but that must not bypass processing grouping.
    for delivery in [contents.clone(), Vec::new()] {
        receiver
            .apply_changes_with_fragments_log_patches(
                delivery,
                vec![fragment.clone()],
                &mut PatchLog::inactive(),
            )
            .unwrap();
        assert_eq!(
            receiver.staged.iter().collect::<Vec<_>>(),
            contents.iter().collect::<Vec<_>>()
        );
        assert_fragment(&receiver, &fragment);
        assert!(receiver.get_heads().is_empty());
        assert_eq!(receiver.get_missing_deps(&[]), [changes[0].hash()]);
    }
}

#[test]
fn grouped_import_rejects_a_foreign_patch_log_before_mutating_staging() {
    let (source, changes) = history(&[0, 0, 1]);
    let fragment = source.get_fragment(changes[2].hash()).unwrap();
    let mut receiver = Automerge::new();
    receiver.apply_changes([changes[2].clone()]).unwrap();
    let before = receiver.save_nocompress();

    let mut foreign = Automerge::new().with_actor(ActorId::from([99]));
    let mut log = PatchLog::active();
    let mut tx = foreign.transaction_log_patches(log).unwrap();
    tx.put(ROOT, "foreign", 9).unwrap();
    let (_, returned_log) = tx.commit();
    log = returned_log;
    let expected_patches = foreign.make_patches(&mut log.clone());
    assert!(!expected_patches.is_empty());

    let error = receiver
        .apply_changes_with_fragments_log_patches(changes[..2].to_vec(), vec![fragment], &mut log)
        .unwrap_err();
    assert!(matches!(error, AutomergeError::PatchLogMismatch(_)));
    assert_eq!(receiver.save_nocompress(), before);
    assert!(receiver.get_heads().is_empty());
    assert_eq!(receiver.staged.iter().collect::<Vec<_>>(), [&changes[2]]);
    assert!(receiver.staged.groups().is_empty());
    assert_eq!(foreign.make_patches(&mut log), expected_patches);

    receiver.apply_changes(changes[..2].to_vec()).unwrap();
    assert_eq!(receiver.get_heads(), source.get_heads());
}

#[test]
fn inconsistent_group_descriptors_assert_before_insertion_or_application() {
    let (source, changes) = history(&[0, 0, 1]);
    let fragment = source.get_fragment(changes[2].hash()).unwrap();
    let mut inconsistent = fragment.clone();
    inconsistent
        .members
        .retain(|hash| *hash != changes[0].hash());
    let mut receiver = Automerge::new();
    receiver.apply_changes([changes[2].clone()]).unwrap();
    let before = receiver.save_nocompress();
    let before_view = receiver.hydrate(None);
    let before_ops = receiver.ops.len();
    let mut log = PatchLog::active();

    let failure = catch_unwind(AssertUnwindSafe(|| {
        receiver
            .apply_changes_with_fragments_log_patches(
                changes[..2].to_vec(),
                vec![fragment.clone(), inconsistent],
                &mut log,
            )
            .unwrap();
    }))
    .expect_err("inconsistent internal descriptors must assert");
    let message = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains("inconsistent fragment descriptor")
            && message.contains(&fragment.head.to_string()),
        "expected descriptor disagreement identifying the head, got: {message}"
    );
    assert_eq!(receiver.save_nocompress(), before);
    assert_eq!(receiver.hydrate(None), before_view);
    assert_eq!(receiver.ops.len(), before_ops);
    assert!(receiver.get_heads().is_empty());
    assert_eq!(receiver.staged.iter().collect::<Vec<_>>(), [&changes[2]]);
    assert!(receiver.staged.groups().is_empty());
    assert!(receiver.make_patches(&mut log).is_empty());
}

#[test]
fn queued_sequence_conflict_rejects_the_whole_grouped_batch() {
    let (base, prefix) = history(&[0]);
    let mut left = base.clone();
    let pending = at_level(&mut left, 2, 1);
    let mut right = base;
    let alternative = at_level(&mut right, 3, 1);
    assert_eq!(pending.actor_id(), alternative.actor_id());
    assert_eq!(pending.seq(), alternative.seq());
    assert_ne!(pending.hash(), alternative.hash());
    let fragment = right.get_fragment(alternative.hash()).unwrap();
    let mut receiver = Automerge::new();
    receiver.apply_changes([pending.clone()]).unwrap();
    let before = receiver.save_nocompress();
    let mut log = PatchLog::active();

    let error = receiver
        .apply_changes_with_fragments_log_patches(
            [prefix[0].clone(), alternative.clone()],
            vec![fragment],
            &mut log,
        )
        .unwrap_err();
    assert!(
        matches!(error, AutomergeError::DuplicateSeqNumber(seq, actor)
        if seq == alternative.seq() && &actor == alternative.actor_id())
    );
    assert_eq!(receiver.save_nocompress(), before);
    assert!(receiver.get_heads().is_empty());
    assert_eq!(receiver.staged.iter().collect::<Vec<_>>(), [&pending]);
    assert!(receiver.staged.groups().is_empty());
    assert!(receiver.make_patches(&mut log).is_empty());
}
