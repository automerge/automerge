//! Guards against superlinear cost in the number of operations in a single transaction.
//!
//! `TransactionInner::exid_to_obj` validates that the object being edited is visible to the
//! transaction's isolation scope, allowing an exception for objects created inside the
//! transaction itself. If that exception is implemented by scanning the pending operation list,
//! the cost of a transaction becomes quadratic in its own length: every mutating call walks every
//! operation recorded so far.
//!
//! The timing test is `#[ignore]`d because wall-clock assertions do not belong in the default
//! test run. Run it deliberately, in release mode:
//!
//! ```sh
//! cargo test --release --test transaction_scaling -- --ignored --nocapture
//! ```
//!
//! The semantic tests are cheap and always run. They pin the behaviour that any optimisation of
//! the scope check must preserve, so a fix cannot silently widen or narrow what a transaction is
//! allowed to edit.

use std::time::{Duration, Instant};

use automerge::{
    transaction::Transactable, AutoCommit, Automerge, AutomergeError, ObjType, ReadDoc, ROOT,
};

/// Number of operations recorded per simulated file entry in [`build_map_of_maps`].
const OPS_PER_ENTRY: usize = 3;

/// Sizes for the scaling check. The larger is 4x the smaller, so linear growth predicts a ratio
/// near 4 and quadratic growth predicts a ratio near 16.
const SMALL: usize = 8_000;
const LARGE: usize = 32_000;

/// Maximum tolerated ratio between the two sample times. Chosen to sit well clear of both the
/// linear expectation (~4) and the quadratic one (~16); measured ratios are roughly 4.1 with an
/// O(1) scope check and 7.8 with the scanning one.
const MAX_RATIO: f64 = 6.0;

/// Build a document whose shape mirrors a real bulk-commit workload: a `files` map holding many
/// small map entries, each entry written immediately after it is created.
///
/// The "create, then immediately write into it" ordering matters. A scan of pending operations
/// which starts at the front finds the freshly created object last, so nearly every lookup walks
/// the entire list.
fn build_map_of_maps(entries: usize) -> Automerge {
    let mut doc = Automerge::new();
    let mut tx = doc.transaction();
    let files = tx.put_object(ROOT, "files", ObjType::Map).unwrap();

    for i in 0..entries {
        let entry = tx
            .put_object(&files, format!("file{i}"), ObjType::Map)
            .unwrap();
        tx.put(&entry, "hash", i as i64).unwrap();
        tx.put(&entry, "url", "automerge:placeholder").unwrap();
    }

    tx.commit();
    doc
}

fn time_build(entries: usize) -> Duration {
    let start = Instant::now();
    let doc = build_map_of_maps(entries);
    let elapsed = start.elapsed();
    std::hint::black_box(doc);
    elapsed
}

fn nanos_per_op(elapsed: Duration, entries: usize) -> f64 {
    elapsed.as_secs_f64() * 1e9 / (entries * OPS_PER_ENTRY) as f64
}

#[test]
#[ignore = "wall-clock measurement; run explicitly in release mode"]
fn transaction_cost_is_linear_in_pending_ops() {
    // Warm the allocator and any lazily built indexes so the first sample is not penalised.
    std::hint::black_box(build_map_of_maps(SMALL / 8));

    let small = time_build(SMALL);
    let large = time_build(LARGE);
    let ratio = large.as_secs_f64() / small.as_secs_f64();

    println!(
        "{SMALL:>7} entries: {small:>10.3?}  ({:>7.1} ns/op)",
        nanos_per_op(small, SMALL)
    );
    println!(
        "{LARGE:>7} entries: {large:>10.3?}  ({:>7.1} ns/op)",
        nanos_per_op(large, LARGE)
    );
    println!("ratio for a 4x size increase: {ratio:.2} (linear ~4, quadratic ~16)");

    assert!(
        ratio < MAX_RATIO,
        "transaction cost grew {ratio:.2}x for a 4x increase in operation count \
         (expected under {MAX_RATIO:.1}x); the per-operation work is superlinear in the \
         number of pending ops"
    );
}

/// A non-isolated transaction has no scope, so the scope check can never reject anything. This is
/// the overwhelmingly common case and it must not pay for the check.
#[test]
fn unisolated_transaction_may_edit_any_object() {
    let mut doc = Automerge::new();

    let mut tx = doc.transaction();
    let map = tx.put_object(ROOT, "map", ObjType::Map).unwrap();
    tx.commit();

    // A later, separate transaction edits an object created before it began.
    let mut tx = doc.transaction();
    tx.put(&map, "key", "value").unwrap();
    tx.commit();

    assert!(doc.get(&map, "key").unwrap().is_some());
}

/// An isolated transaction must still reject objects which do not exist at its heads.
/// This is the behaviour the scan was added to provide; any faster implementation must keep it.
#[test]
fn isolated_transaction_rejects_object_outside_scope() {
    let mut doc = AutoCommit::new();
    let list = doc.put_object(ROOT, "list", ObjType::List).unwrap();
    doc.commit();

    doc.isolate(&[]);
    let result = doc.insert(&list, 0, 1);

    assert!(
        matches!(result, Err(AutomergeError::InvalidObjId(_))),
        "expected InvalidObjId, got {result:?}"
    );
}

/// An isolated transaction may edit objects it created itself, even though those objects are not
/// covered by its isolation clock. This is the exception the pending-op scan exists to implement.
#[test]
fn isolated_transaction_may_edit_objects_it_created() {
    let mut doc = AutoCommit::new();
    doc.put(ROOT, "existing", "value").unwrap();
    doc.commit();

    doc.isolate(&[]);
    let fresh = doc.put_object(ROOT, "fresh", ObjType::Map).unwrap();
    doc.put(&fresh, "key", "value").unwrap();
    let nested = doc.put_object(&fresh, "nested", ObjType::Map).unwrap();
    doc.put(&nested, "key", "value").unwrap();
    doc.commit();

    assert_eq!(
        doc.get(&nested, "key").unwrap().map(|(value, _)| value),
        Some("value".into())
    );
}

/// The exception must survive many intervening operations. A correct implementation recognises an
/// object created at the very start of a long transaction just as readily as one created last.
#[test]
fn isolated_transaction_may_edit_object_created_before_many_others() {
    let mut doc = AutoCommit::new();
    doc.put(ROOT, "existing", "value").unwrap();
    doc.commit();

    doc.isolate(&[]);
    let first = doc.put_object(ROOT, "first", ObjType::Map).unwrap();
    for i in 0..1_000 {
        doc.put(ROOT, format!("filler{i}"), i).unwrap();
    }
    doc.put(&first, "late", "write").unwrap();
    doc.commit();

    assert_eq!(
        doc.get(&first, "late").unwrap().map(|(value, _)| value),
        Some("write".into())
    );
}
