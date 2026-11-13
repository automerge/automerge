//! Compatibility checks for unclassified delivery and fragment-aware merge.

use automerge::transaction::Transactable;
use automerge::{ActorId, Automerge, Change, PatchLog, ReadDoc, ROOT};

#[path = "fragment_staging/merge.rs"]
mod merge;

fn put(doc: &mut Automerge, value: i64) -> Change {
    let mut tx = doc.transaction();
    tx.put(ROOT, "value", value).unwrap();
    let (hash, _) = tx.commit();
    doc.get_change_by_hash(&hash.unwrap()).unwrap()
}

fn history() -> (Automerge, [Change; 3]) {
    let mut source = Automerge::new().with_actor(ActorId::from([1]));
    let a = put(&mut source, 1);
    let b = put(&mut source, 2);
    let c = put(&mut source, 3);
    (source, [a, b, c])
}

#[test]
fn subset_bundle_keeps_partial_progress_and_missing_history() {
    let (source, [a, b, c]) = history();
    let bundle = source.bundle([a.hash(), c.hash()]).unwrap();
    let mut receiver = Automerge::new();
    receiver.load_incremental(bundle.bytes()).unwrap();
    assert!(receiver.get_change_by_hash(&a.hash()).is_some());
    assert!(receiver.get_change_by_hash(&c.hash()).is_none());
    assert_eq!(receiver.get_missing_deps(&[]), [b.hash()]);

    let mut reloaded = Automerge::load(&receiver.save()).unwrap();
    assert_eq!(reloaded.get_missing_deps(&[]), [b.hash()]);
    reloaded.load_incremental(b.raw_bytes()).unwrap();
    assert_eq!(reloaded.get_heads(), source.get_heads());
    assert_eq!(reloaded.hydrate(None), source.hydrate(None));
    assert!(reloaded.get_missing_deps(&[]).is_empty());
}

#[test]
fn subset_bundle_matches_raw_delivery_patches_and_duplicate_retry() {
    let (source, [a, b, c]) = history();
    let bundle = source.bundle([a.hash(), c.hash()]).unwrap();
    // Bundle reconstruction, not the requested hash order, defines the
    // equivalent change delivery order.
    let decoded = bundle.to_changes().unwrap();
    let mut bundled = Automerge::new();
    let mut raw = Automerge::new();
    let mut bundled_log = PatchLog::active();
    let mut raw_log = PatchLog::active();

    for _ in 0..2 {
        bundled
            .load_incremental_log_patches(bundle.bytes(), &mut bundled_log)
            .unwrap();
        raw.apply_changes_log_patches(decoded.clone(), &mut raw_log)
            .unwrap();
        assert_eq!(bundled.get_heads(), raw.get_heads());
        assert_eq!(bundled.hydrate(None), raw.hydrate(None));
        assert_eq!(bundled.get_missing_deps(&[]), [b.hash()]);
        assert_eq!(raw.get_missing_deps(&[]), [b.hash()]);
        assert_eq!(bundled.save_nocompress(), raw.save_nocompress());
        assert_eq!(
            bundled.make_patches(&mut bundled_log),
            raw.make_patches(&mut raw_log)
        );
    }

    bundled
        .load_incremental_log_patches(b.raw_bytes(), &mut bundled_log)
        .unwrap();
    raw.apply_changes_log_patches([b], &mut raw_log).unwrap();
    assert_eq!(bundled.get_heads(), source.get_heads());
    assert_eq!(raw.get_heads(), source.get_heads());
    assert_eq!(bundled.hydrate(None), source.hydrate(None));
    assert_eq!(raw.hydrate(None), source.hydrate(None));
    assert_eq!(bundled.save_nocompress(), raw.save_nocompress());
    assert_eq!(
        bundled.make_patches(&mut bundled_log),
        raw.make_patches(&mut raw_log)
    );
    assert!(bundled.get_missing_deps(&[]).is_empty());
    assert!(raw.get_missing_deps(&[]).is_empty());
}
