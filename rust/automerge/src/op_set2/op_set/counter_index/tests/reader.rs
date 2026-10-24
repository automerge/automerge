//! Tests for reading counter totals from the index instead of summing
//! successors.
//!
//! The tests ensure parity in reading counter values using the index, and
//! specifically comparing to where `Op::fix_counter` was always used
//! previously. The use of `Op::fix_counter` now only occurs when a historical
//! clock is used for reading.

use crate::op_set2::op_set::OpSet;
use crate::transaction::Transactable;
use crate::{ActorId, AutoCommit, ChangeHash, ReadDoc, ScalarValue, Value, ROOT};
use proptest::prelude::*;

fn actor(byte: u8) -> ActorId {
    ActorId::from(vec![byte])
}

fn doc_a() -> AutoCommit {
    AutoCommit::new().with_actor(actor(0x10))
}

/// Every counter op in `op_set`, as `(pos, indexed total, summed total)`.
///
/// `summed` is the unmasked sum — the oracle for an unmasked current read.
fn counter_totals(op_set: &OpSet) -> Vec<(usize, Option<i64>, Option<i64>)> {
    use crate::op_set2::types::ScalarValue as Scalar;

    op_set
        .iter()
        .filter_map(|op| match op.value {
            Scalar::Counter(base) => {
                let summed = op
                    .succ_inc()
                    .filter_map(|(_, inc)| inc)
                    .fold(base, i64::wrapping_add);
                Some((op.pos, op_set.counter_total_at(op.pos), Some(summed)))
            }
            _ => None,
        })
        .collect()
}

/// Assert the index agrees with the sum for every counter op.
///
/// Reads `doc.doc` rather than [`AutoCommit::document`], which would close an
/// open transaction and so flatten the transaction grouping these tests vary.
fn assert_parity(doc: &AutoCommit) {
    let op_set = doc.doc.ops();
    for (pos, indexed, summed) in counter_totals(op_set) {
        assert_eq!(indexed, summed, "index != sum at pos {pos}");
    }
}

fn counter_value(doc: &AutoCommit, key: &str) -> Option<i64> {
    match doc.get(ROOT, key).unwrap()? {
        (Value::Scalar(s), _) => match s.as_ref() {
            ScalarValue::Counter(c) => Some(c.into()),
            _ => None,
        },
        _ => None,
    }
}

fn counter_value_at(doc: &AutoCommit, key: &str, heads: &[ChangeHash]) -> Option<i64> {
    match doc.get_at(ROOT, key, heads).unwrap()? {
        (Value::Scalar(s), _) => match s.as_ref() {
            ScalarValue::Counter(c) => Some(c.into()),
            _ => None,
        },
        _ => None,
    }
}

/// Run `f` over every op in `doc`'s op set.
///
/// Local ops reach the op set as they are made, so an open transaction is no
/// obstacle — and is left open, per [`assert_parity`].
fn with_ops<F: FnMut(crate::op_set2::op::Op<'_>, &OpSet)>(doc: &AutoCommit, mut f: F) {
    let op_set = doc.doc.ops();
    for op in op_set.iter() {
        f(op, op_set);
    }
}

#[test]
fn set_counter_from_index_replaces_the_base_with_the_total() {
    use crate::op_set2::types::ScalarValue as Scalar;

    let mut doc = doc_a();
    doc.put(ROOT, "c", ScalarValue::counter(10)).unwrap();
    doc.increment(ROOT, "c", 5).unwrap();
    doc.increment(ROOT, "c", 7).unwrap();

    let mut seen = 0;
    with_ops(&doc, |op, op_set| {
        if !matches!(op.value, Scalar::Counter(_)) {
            return;
        }
        seen += 1;
        // The op as stored carries the base, not the total.
        assert_eq!(op.value, Scalar::Counter(10));
        let mut op = op;
        op.set_counter_from_index(op_set);
        assert_eq!(op.value, Scalar::Counter(22));
    });
    assert_eq!(seen, 1, "expected exactly one counter op");
}

/// Unlike its siblings, this test passes against a do-nothing
/// `set_counter_from_index`: it exists to catch the method growing reach over
/// non-counter values, not to pin the fix-up itself.
#[test]
fn set_counter_from_index_leaves_non_counters_alone() {
    use crate::op_set2::types::ScalarValue as Scalar;

    let mut doc = doc_a();
    doc.put(ROOT, "s", "a string").unwrap();
    doc.put(ROOT, "n", 42).unwrap();

    with_ops(&doc, |op, op_set| {
        let before = op.value.clone();
        assert!(!matches!(before, Scalar::Counter(_)));
        let mut op = op;
        op.set_counter_from_index(op_set);
        assert_eq!(op.value, before);
    });
}

#[test]
fn set_counter_from_index_matches_fix_counter_on_a_current_read() {
    use crate::op_set2::types::ScalarValue as Scalar;

    let mut a = doc_a();
    a.put(ROOT, "c", ScalarValue::counter(1)).unwrap();
    a.commit();
    let mut b = a.fork().with_actor(actor(0x20));
    a.increment(ROOT, "c", 2).unwrap();
    b.increment(ROOT, "c", 3).unwrap();
    a.merge(&mut b).unwrap();

    with_ops(&a, |op, op_set| {
        if !matches!(op.value, Scalar::Counter(_)) {
            return;
        }
        let mut indexed = op.clone();
        indexed.set_counter_from_index(op_set);
        let mut summed = op;
        summed.fix_counter(None);
        assert_eq!(indexed.value, summed.value);
    });
}

#[test]
fn map_get_reads_the_current_total() {
    let mut doc = doc_a();
    doc.put(ROOT, "c", ScalarValue::counter(10)).unwrap();
    for _ in 0..5 {
        doc.increment(ROOT, "c", 2).unwrap();
    }

    assert_eq!(counter_value(&doc, "c"), Some(20));
    assert_parity(&doc);
}

#[test]
fn map_get_after_reload_reads_the_rebuilt_total() {
    let mut doc = doc_a();
    doc.put(ROOT, "c", ScalarValue::counter(1)).unwrap();
    doc.increment(ROOT, "c", 41).unwrap();
    doc.commit();

    let loaded = AutoCommit::load(&doc.save()).unwrap();

    assert_eq!(counter_value(&loaded, "c"), Some(42));
    assert_parity(&loaded);
}

#[test]
fn list_get_reads_the_current_total() {
    use crate::ObjType;

    let mut doc = doc_a();
    let list = doc.put_object(ROOT, "l", ObjType::List).unwrap();
    doc.insert(&list, 0, ScalarValue::counter(100)).unwrap();
    doc.increment(&list, 0, 5).unwrap();

    let value = match doc.get(&list, 0).unwrap().unwrap() {
        (Value::Scalar(s), _) => match s.as_ref() {
            ScalarValue::Counter(c) => Some(i64::from(c)),
            _ => None,
        },
        _ => None,
    };
    assert_eq!(value, Some(105));
    assert_parity(&doc);
}

#[test]
fn get_all_reads_every_conflicting_counter_total() {
    let mut a = doc_a();
    a.put(ROOT, "x", 1).unwrap();
    a.commit();
    let mut b = a.fork().with_actor(actor(0x20));

    a.put(ROOT, "c", ScalarValue::counter(100)).unwrap();
    a.increment(ROOT, "c", 1).unwrap();
    b.put(ROOT, "c", ScalarValue::counter(200)).unwrap();
    b.increment(ROOT, "c", 2).unwrap();
    a.merge(&mut b).unwrap();

    let totals: Vec<i64> = a
        .get_all(ROOT, "c")
        .unwrap()
        .into_iter()
        .filter_map(|(v, _)| match v {
            Value::Scalar(s) => match s.as_ref() {
                ScalarValue::Counter(c) => Some(i64::from(c)),
                _ => None,
            },
            _ => None,
        })
        .collect();

    assert_eq!(totals, vec![101, 202]);
    assert_parity(&a);
}

#[test]
fn historical_get_sees_only_increments_up_to_those_heads() {
    let mut doc = doc_a();
    doc.put(ROOT, "c", ScalarValue::counter(0)).unwrap();
    doc.increment(ROOT, "c", 1).unwrap();
    doc.commit();
    let early = doc.get_heads();

    doc.increment(ROOT, "c", 10).unwrap();
    doc.commit();

    assert_eq!(counter_value(&doc, "c"), Some(11));
    assert_eq!(counter_value_at(&doc, "c", &early), Some(1));
    assert_parity(&doc);
}

#[test]
fn historical_get_before_the_counter_existed() {
    let mut doc = doc_a();
    doc.put(ROOT, "x", 1).unwrap();
    doc.commit();
    let before = doc.get_heads();

    doc.put(ROOT, "c", ScalarValue::counter(5)).unwrap();
    doc.increment(ROOT, "c", 5).unwrap();
    doc.commit();

    assert_eq!(counter_value_at(&doc, "c", &before), None);
    assert_eq!(counter_value(&doc, "c"), Some(10));
}

#[test]
fn historical_read_of_a_concurrent_increment() {
    let mut a = doc_a();
    a.put(ROOT, "c", ScalarValue::counter(0)).unwrap();
    a.commit();
    let mut b = a.fork().with_actor(actor(0x20));
    let forked = a.get_heads();

    a.increment(ROOT, "c", 3).unwrap();
    a.commit();
    b.increment(ROOT, "c", 4).unwrap();
    b.commit();
    a.merge(&mut b).unwrap();

    assert_eq!(counter_value(&a, "c"), Some(7));
    assert_eq!(counter_value_at(&a, "c", &forked), Some(0));
    assert_parity(&a);
}

#[test]
fn apply_onto_a_counter_with_existing_increments() {
    let mut a = doc_a();
    a.put(ROOT, "c", ScalarValue::counter(0)).unwrap();
    a.commit();
    let mut b = a.fork().with_actor(actor(0x20));

    for _ in 0..5 {
        b.increment(ROOT, "c", 1).unwrap();
    }
    b.commit();

    for _ in 0..3 {
        a.increment(ROOT, "c", 10).unwrap();
    }
    a.commit();

    let heads = b.get_heads();
    b.apply_changes(a.get_changes(&heads)).unwrap();

    assert_eq!(counter_value(&b, "c"), Some(35));
    assert_parity(&b);
}

/// Random increments, in random batch sizes, applied locally and remotely.
#[derive(Debug, Clone)]
enum Step {
    LocalIncrement(i64),
    RemoteIncrement(i64),
    Commit,
    Merge,
}

fn any_step() -> impl Strategy<Value = Step> {
    prop_oneof![
        5 => (-20i64..20).prop_map(Step::LocalIncrement),
        5 => (-20i64..20).prop_map(Step::RemoteIncrement),
        2 => Just(Step::Commit),
        2 => Just(Step::Merge),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// The value a current read reports equals the base plus every
    /// increment applied, however the increments arrived.
    #[test]
    fn current_read_equals_the_sum_of_all_increments(
        base in -100i64..100,
        steps in proptest::collection::vec(any_step(), 1..30),
    ) {
        let mut local = doc_a();
        local.put(ROOT, "c", ScalarValue::counter(base)).unwrap();
        local.commit();
        let mut remote = local.fork().with_actor(actor(0x20));

        let mut expected = base;
        for step in steps {
            match step {
                Step::LocalIncrement(n) => {
                    local.increment(ROOT, "c", n).unwrap();
                    expected += n;
                }
                Step::RemoteIncrement(n) => {
                    remote.increment(ROOT, "c", n).unwrap();
                    expected += n;
                }
                Step::Commit => {
                    local.commit();
                    remote.commit();
                }
                Step::Merge => {
                    local.merge(&mut remote).unwrap();
                    remote.merge(&mut local).unwrap();
                }
            }
            assert_parity(&local);
            assert_parity(&remote);
        }

        local.merge(&mut remote).unwrap();
        remote.merge(&mut local).unwrap();

        prop_assert_eq!(counter_value(&local, "c"), Some(expected));
        prop_assert_eq!(counter_value(&remote, "c"), Some(expected));
    }

    /// A historical read at the heads captured after each step reports the
    /// total as of that step, not the latest total.
    #[test]
    fn historical_reads_track_their_heads(
        base in -100i64..100,
        increments in proptest::collection::vec(-20i64..20, 1..12),
    ) {
        let mut doc = doc_a();
        doc.put(ROOT, "c", ScalarValue::counter(base)).unwrap();
        doc.commit();

        let mut checkpoints = vec![(doc.get_heads(), base)];
        let mut running = base;
        for n in increments {
            doc.increment(ROOT, "c", n).unwrap();
            doc.commit();
            running += n;
            checkpoints.push((doc.get_heads(), running));
        }

        prop_assert_eq!(counter_value(&doc, "c"), Some(running));
        for (heads, expected) in checkpoints {
            prop_assert_eq!(counter_value_at(&doc, "c", &heads), Some(expected));
        }
    }
}
