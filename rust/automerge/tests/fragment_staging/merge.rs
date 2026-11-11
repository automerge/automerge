//! Merge must preserve ordinary import order, patches and retry behavior.

use std::collections::HashSet;

use automerge::transaction::{CommitOptions, Transactable};
use automerge::{ActorId, Automerge, AutomergeError, Change, Patch, PatchLog, ReadDoc, ROOT};

fn at_level(doc: &mut Automerge, value: i64, level: usize) -> Change {
    for time in 0..10_000 {
        let mut next = doc.clone();
        let mut tx = next.transaction();
        tx.put(ROOT, "value", value).unwrap();
        let (hash, _) = tx.commit_with(CommitOptions::default().with_time(time));
        let hash = hash.unwrap();
        if next.get_fragment(hash).unwrap().level == level {
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

fn assert_merge_matches_raw(
    merged: &mut Automerge,
    raw: &mut Automerge,
    source: &mut Automerge,
    merged_log: &mut PatchLog,
    raw_log: &mut PatchLog,
) -> Vec<Patch> {
    let incoming = raw.get_changes_added(source);
    assert_eq!(merged.get_changes_added(source), incoming);
    let source_before = source.save_nocompress();
    let mut view = merged.hydrate(None);
    let heads = merged.merge_and_log_patches(source, merged_log).unwrap();
    raw.apply_changes_log_patches(incoming, raw_log).unwrap();

    assert_eq!(heads, merged.get_heads());
    assert_eq!(heads, raw.get_heads());
    assert_eq!(merged.hydrate(None), raw.hydrate(None));
    assert_eq!(merged.get_missing_deps(&[]), raw.get_missing_deps(&[]));
    assert_eq!(merged.save_nocompress(), raw.save_nocompress());
    assert_eq!(source.save_nocompress(), source_before);
    let patches = merged.make_patches(merged_log);
    assert_eq!(patches, raw.make_patches(raw_log));
    view.apply_patches(merged.text_encoding(), patches.iter().cloned())
        .unwrap();
    assert_eq!(view, merged.hydrate(None));
    patches
}

#[test]
fn merge_matches_raw_with_an_applied_fragment_prefix_and_repeated_delivery() {
    let (mut source, changes) = history(&[1, 0, 0, 1]);
    let fragment = source.get_fragment(changes[3].hash()).unwrap();
    assert_eq!(
        fragment.members.iter().copied().collect::<HashSet<_>>(),
        changes[1..].iter().map(Change::hash).collect()
    );
    assert!(source.fragments(..).contains(&fragment));
    let mut merged = Automerge::new();
    // The first fragment is wholly applied, and the next fragment's first
    // member is applied too. Its descriptor must nevertheless remain whole.
    merged.apply_changes(changes[..2].to_vec()).unwrap();
    let mut raw = merged.clone();
    let incoming = merged.get_changes_added(&source);
    assert_eq!(incoming.len(), 2);
    assert!(!incoming
        .iter()
        .any(|change| change.hash() == changes[1].hash()));
    let mut merged_log = PatchLog::active();
    let mut raw_log = PatchLog::active();

    let patches = assert_merge_matches_raw(
        &mut merged,
        &mut raw,
        &mut source,
        &mut merged_log,
        &mut raw_log,
    );
    assert!(!patches.is_empty());
    assert_eq!(merged.get_heads(), source.get_heads());
    assert_eq!(merged.hydrate(None), source.hydrate(None));
    assert!(merged.get_missing_deps(&[]).is_empty());

    assert!(merged.get_changes_added(&source).is_empty());
    // make_patches does not drain the log; isolate the retry's new events.
    merged_log = PatchLog::active();
    raw_log = PatchLog::active();
    assert!(assert_merge_matches_raw(
        &mut merged,
        &mut raw,
        &mut source,
        &mut merged_log,
        &mut raw_log,
    )
    .is_empty());
}

#[test]
fn merge_matches_raw_with_concurrent_fragments_and_already_staged_contents() {
    let (base, prefix) = history(&[0]);
    let mut source = base.clone().with_actor(ActorId::from([2]));
    let b = at_level(&mut source, 2, 0);
    let c = at_level(&mut source, 3, 1);
    let mut right = base.clone().with_actor(ActorId::from([3]));
    let d = at_level(&mut right, 4, 0);
    let e = at_level(&mut right, 5, 1);
    source.apply_changes([d.clone(), e.clone()]).unwrap();
    assert!(source
        .fragments(..)
        .iter()
        .any(|fragment| fragment.level > 0));

    let mut merged = base;
    // These contents predate the merge and wait on different missing parents.
    // Their first-seen order is independent of the source's fragment order.
    merged.apply_changes([e.clone(), c.clone()]).unwrap();
    assert_eq!(merged.get_heads(), [prefix[0].hash()]);
    assert_eq!(
        merged
            .get_missing_deps(&[])
            .into_iter()
            .collect::<HashSet<_>>(),
        HashSet::from([b.hash(), d.hash()])
    );
    assert!(merged.get_change_by_hash(&c.hash()).is_none());
    assert!(merged.get_change_by_hash(&e.hash()).is_none());
    let incoming_hashes: HashSet<_> = merged
        .get_changes_added(&source)
        .iter()
        .map(Change::hash)
        .collect();
    assert_eq!(
        incoming_hashes,
        HashSet::from([b.hash(), c.hash(), d.hash(), e.hash()])
    );
    let mut raw = merged.clone();
    let mut merged_log = PatchLog::active();
    let mut raw_log = PatchLog::active();

    assert!(!assert_merge_matches_raw(
        &mut merged,
        &mut raw,
        &mut source,
        &mut merged_log,
        &mut raw_log,
    )
    .is_empty());
    assert_eq!(merged.get_heads(), source.get_heads());
    assert_eq!(merged.hydrate(None), source.hydrate(None));
    assert!(merged.get_missing_deps(&[]).is_empty());
    merged_log = PatchLog::active();
    raw_log = PatchLog::active();
    assert!(assert_merge_matches_raw(
        &mut merged,
        &mut raw,
        &mut source,
        &mut merged_log,
        &mut raw_log,
    )
    .is_empty());
}

#[test]
fn empty_merge_preserves_unrelated_orphan_contents_for_later_delivery() {
    let (mut source, _) = history(&[0, 0, 1]);
    let mut other = Automerge::new().with_actor(ActorId::from([9]));
    let parent = at_level(&mut other, 8, 0);
    let child = at_level(&mut other, 9, 0);
    let mut merged = source.clone();
    merged.apply_changes([child.clone()]).unwrap();
    assert!(merged.get_changes_added(&source).is_empty());
    let mut raw = merged.clone();
    let before = merged.save_nocompress();

    assert!(assert_merge_matches_raw(
        &mut merged,
        &mut raw,
        &mut source,
        &mut PatchLog::active(),
        &mut PatchLog::active(),
    )
    .is_empty());
    assert_eq!(merged.save_nocompress(), before);
    assert_eq!(merged.get_missing_deps(&[]), [parent.hash()]);

    // Saving after the empty merge must still preserve that unrelated orphan.
    let mut restored = Automerge::load(&merged.save_nocompress()).unwrap();
    restored.apply_changes([parent.clone()]).unwrap();
    merged.apply_changes([parent.clone()]).unwrap();
    raw.apply_changes([parent]).unwrap();
    assert!(merged.get_change_by_hash(&child.hash()).is_some());
    assert!(merged.get_missing_deps(&[]).is_empty());
    assert_eq!(merged.save_nocompress(), raw.save_nocompress());
    assert_eq!(restored.hydrate(None), merged.hydrate(None));
    assert_eq!(restored.get_heads(), merged.get_heads());
}

#[test]
fn empty_merge_still_rejects_a_foreign_patch_log_without_mutation() {
    let (mut source, _) = history(&[0, 0, 1]);
    let mut merged = source.clone();
    let mut raw = merged.clone();
    assert!(merged.get_changes_added(&source).is_empty());
    let before = merged.save_nocompress();
    let source_before = source.save_nocompress();

    let mut foreign = Automerge::new().with_actor(ActorId::from([99]));
    let mut tx = foreign.transaction_log_patches(PatchLog::active()).unwrap();
    tx.put(ROOT, "foreign", 9).unwrap();
    let (_, mut merged_log) = tx.commit();
    let mut raw_log = merged_log.clone();
    let expected_patches = foreign.make_patches(&mut merged_log.clone());
    assert!(!expected_patches.is_empty());

    let error = merged
        .merge_and_log_patches(&mut source, &mut merged_log)
        .unwrap_err();
    assert!(matches!(error, AutomergeError::PatchLogMismatch(_)));
    let error = raw
        .apply_changes_log_patches(Vec::<Change>::new(), &mut raw_log)
        .unwrap_err();
    assert!(matches!(error, AutomergeError::PatchLogMismatch(_)));
    assert_eq!(merged.save_nocompress(), before);
    assert_eq!(raw.save_nocompress(), before);
    assert_eq!(source.save_nocompress(), source_before);
    assert_eq!(foreign.make_patches(&mut merged_log), expected_patches);
    assert_eq!(foreign.make_patches(&mut raw_log), expected_patches);
}
