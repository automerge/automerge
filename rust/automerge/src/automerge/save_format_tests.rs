//! A small save leaves out the hashes of its tiny changes and the load
//! rehashes them; nothing else is ever left out.

use super::{Automerge, LoadOptions, NameHashes, SaveOptions};
use crate::autocommit::AutoCommit;
use crate::hash_retention::Retained;
use crate::storage::bundle::{REHASHABLE_CHANGES, REHASHABLE_OPS};
use crate::tx::{CommitOptions, Transactable};
use crate::{ActorId, ROOT};

/// One change per entry of `sizes`, with that many ops.
fn build(sizes: &[usize]) -> AutoCommit {
    let mut doc = AutoCommit::new().with_actor(ActorId::from(&b"save"[..]));
    for (c, n) in sizes.iter().enumerate() {
        for o in 0..*n {
            doc.put(&ROOT, format!("k{}", o % 64), (c * 100_000 + o) as i64)
                .unwrap();
        }
        doc.commit_with(CommitOptions::default().with_time(0));
    }
    doc
}

fn reload(bytes: &[u8]) -> Automerge<Retained> {
    Automerge::load_with_options(bytes, LoadOptions::new().with_manual_gc()).unwrap()
}

#[test]
fn a_small_save_leaves_out_tiny_hashes_and_the_load_puts_them_back() {
    let mut doc = build(&[1; 200]);
    let doc = doc.document();
    let small = doc.save();
    let full = doc.save_with(SaveOptions::default(), NameHashes::All);
    assert!(
        small.len() + 190 * 32 < full.len(),
        "{} vs {}",
        small.len(),
        full.len()
    );
    let held = doc.change_graph.held_hashes();
    assert_eq!(reload(&small).change_graph.held_hashes(), held);

    let mut other = Automerge::<Retained>::new().with_manual_gc();
    other.load_incremental(&small).unwrap();
    assert_eq!(other.change_graph.held_hashes(), held);
}

#[test]
fn a_big_change_keeps_its_hash() {
    let mut sizes = vec![1; 100];
    sizes.push(1000);
    sizes.extend([1; 100]);
    let mut doc = build(&sizes);
    let doc = doc.document();
    let big = doc.change_graph.all_nodes()[100];
    let big_hash = doc.change_graph.hash_for_node(big).unwrap();
    let bundle = doc.bundle_document_with(NameHashes::UntagTiny).unwrap();
    assert!(bundle.retained.iter().any(|(_, h)| *h == big_hash));
    assert!(bundle.retained.len() < 10, "tiny hashes left out");
    assert_eq!(
        reload(&doc.save()).change_graph.held_hashes(),
        doc.change_graph.held_hashes()
    );
}

#[test]
fn a_save_over_either_limit_names_every_hash() {
    let mut too_many_ops = vec![1; 10];
    too_many_ops.push(REHASHABLE_OPS as usize - 9);
    for sizes in [vec![1; REHASHABLE_CHANGES + 1], too_many_ops] {
        let mut doc = build(&sizes);
        let doc = doc.document();
        assert_eq!(
            doc.save(),
            doc.save_with(SaveOptions::default(), NameHashes::All)
        );
    }
}

#[test]
fn a_save_at_the_limits_leaves_out_tiny_hashes() {
    let mut sizes = vec![1; REHASHABLE_CHANGES - 1];
    sizes.push(REHASHABLE_OPS as usize - (REHASHABLE_CHANGES - 1));
    let mut doc = build(&sizes);
    let doc = doc.document();
    let small = doc.save();
    assert!(small.len() < doc.save_with(SaveOptions::default(), NameHashes::All).len());
    assert_eq!(
        reload(&small).change_graph.held_hashes(),
        doc.change_graph.held_hashes()
    );
}

#[test]
fn a_small_bundle_missing_any_hash_is_rehashed() {
    let mut sizes = vec![1; 50];
    sizes.push(1000);
    let mut doc = build(&sizes);
    let doc = doc.document();
    let mut bundle = doc.bundle_document_with(NameHashes::All).unwrap();
    bundle.retained.clear();
    let mut other = Automerge::<Retained>::new().with_manual_gc();
    other.apply_bundle(bundle).unwrap();
    assert_eq!(
        other.change_graph.held_hashes(),
        doc.change_graph.held_hashes()
    );
}

fn big_bundle_missing_hashes() -> (Automerge<Retained>, crate::storage::Bundle) {
    let mut doc = build(&[1; REHASHABLE_CHANGES + 100]);
    let doc = doc.document().clone();
    let mut bundle = doc.bundle_document_with(NameHashes::All).unwrap();
    assert!(!bundle.retained.is_empty());
    bundle.retained.clear();
    (doc, bundle)
}

#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "must retain")]
fn a_bundle_missing_retained_hashes_panics_in_debug() {
    let (_, bundle) = big_bundle_missing_hashes();
    let _ = Automerge::<Retained>::new().apply_bundle(bundle);
}

#[test]
#[cfg(not(debug_assertions))]
fn a_bundle_missing_retained_hashes_is_rebuilt_in_release() {
    let (doc, bundle) = big_bundle_missing_hashes();
    let mut other = Automerge::<Retained>::new();
    other.apply_bundle(bundle).unwrap();
    assert_eq!(
        other.change_graph.held_hashes(),
        doc.change_graph.held_hashes()
    );
}
