//! Puts into a document loaded from a save whose value column is large.
//!
//! A loaded document's value column is read whole. A put into it used to
//! copy every value after the put's point and keep a buffer the size of
//! that tail alive, so a few hundred puts into a document with a large
//! value column took seconds and gigabytes, whether the puts were made
//! locally or received. These tests pin the bytes hexane copies with its
//! `raw-counters` feature, not the time taken.

use automerge::{transaction::Transactable, AutoCommit, ReadDoc, ROOT};
use hexane::raw::counters;

/// 2,000 values of 500 bytes: a value column of about a megabyte.
const KEYS: usize = 2_000;
const LEN: usize = 500;
/// Puts spread evenly through the values.
const PUTS: usize = 100;
/// The most bytes the puts may copy. Each used to copy the values past
/// its point, 50,459,500 bytes in all, against 1,000 now: the gate is a
/// tenth of the way from the new count to the old.
const MOST_COPIED: usize = 5_046_850;

fn key(k: usize) -> String {
    format!("k{k:05}")
}

/// A document of `KEYS` string values, and its save.
fn saved() -> (AutoCommit, Vec<u8>) {
    let mut doc = AutoCommit::new();
    for k in 0..KEYS {
        doc.put(ROOT, key(k), "x".repeat(LEN)).unwrap();
    }
    doc.commit();
    let saved = doc.save();
    (doc, saved)
}

/// `PUTS` new values, spread through the document.
fn put_spread(doc: &mut AutoCommit) {
    for p in 0..PUTS {
        doc.put(ROOT, key(p * (KEYS / PUTS)), "y".repeat(LEN))
            .unwrap();
    }
    doc.commit();
}

fn assert_put(doc: &AutoCommit) {
    assert_eq!(doc.length(ROOT), KEYS);
    for p in 0..PUTS {
        let (value, _) = doc.get(ROOT, key(p * (KEYS / PUTS))).unwrap().unwrap();
        assert_eq!(value.as_str(), Some("y".repeat(LEN).as_str()));
    }
}

#[test]
fn local_puts_into_a_loaded_value_column_copy_only_near_them() {
    let (_, saved) = saved();
    let mut loaded = AutoCommit::load(&saved).unwrap();
    counters::take_copied();
    put_spread(&mut loaded);
    let copied = counters::take_copied();
    assert_put(&loaded);
    assert!(copied <= MOST_COPIED, "copied {copied} bytes");
}

#[test]
fn received_puts_into_a_loaded_value_column_copy_only_near_them() {
    let (mut doc, saved) = saved();
    let mut loaded = AutoCommit::load(&saved).unwrap();
    let heads = doc.get_heads();
    put_spread(&mut doc);
    let change = doc.save_after(&heads);
    counters::take_copied();
    loaded.load_incremental(&change).unwrap();
    let copied = counters::take_copied();
    assert_put(&loaded);
    assert!(copied <= MOST_COPIED, "copied {copied} bytes");
}

/// `puts` new values spread through the document, in ascending key
/// order, or in descending order when `backward`.
fn put_walk(doc: &mut AutoCommit, puts: usize, backward: bool) {
    let step = KEYS / puts;
    for p in 0..puts {
        let p = if backward { puts - 1 - p } else { p };
        doc.put(ROOT, key(p * step), "z".repeat(LEN)).unwrap();
    }
    doc.commit();
}

/// The most slabs 1,000 received puts may visit. Rebuilding the slab
/// index whole as each split the loaded values visited 999,001; splicing
/// it where they land visits 3,996. The gate is a tenth of the way from
/// the new count to the old.
const MOST_VISITED: usize = 103_496;

/// The slabs visited and moved as a loaded copy of the document
/// receives 1,000 puts made in another, walking forward or backward.
fn received_walk_counts(backward: bool) -> (usize, usize) {
    let (mut doc, saved) = saved();
    let mut loaded = AutoCommit::load(&saved).unwrap();
    let heads = doc.get_heads();
    put_walk(&mut doc, 1_000, backward);
    let change = doc.save_after(&heads);
    counters::take_visited();
    counters::take_moved();
    loaded.load_incremental(&change).unwrap();
    let counts = (counters::take_visited(), counters::take_moved());
    assert_eq!(loaded.save(), doc.save());
    counts
}

#[test]
fn received_puts_walking_forward_visit_and_move_few_slabs() {
    let (visited, _) = received_walk_counts(false);
    assert!(visited <= MOST_VISITED, "visited {visited} slabs");
}

#[test]
fn received_puts_walking_backward_visit_and_move_few_slabs() {
    let (visited, _) = received_walk_counts(true);
    assert!(visited <= MOST_VISITED, "visited {visited} slabs");
}
