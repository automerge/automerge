//! Test [`CounterIndex`] through the Automerge document API.

use crate::op_set2::op_set::OpSet;
use crate::transaction::Transactable;
use crate::{ActorId, AutoCommit, ObjType, ReadDoc, ScalarValue, Value, ROOT};
use proptest::prelude::*;
use std::collections::BTreeMap;

fn actor(byte: u8) -> ActorId {
    ActorId::from(vec![byte])
}

/// A document whose actor sorts first.
fn doc_a() -> AutoCommit {
    AutoCommit::new().with_actor(actor(0x10))
}

/// Fork `doc` onto an actor that sorts after [`doc_a`]'s.
fn fork_b(doc: &mut AutoCommit) -> AutoCommit {
    doc.fork().with_actor(actor(0x20))
}

/// The op set, without closing an open transaction.
///
/// [`AutoCommit::document`] calls `ensure_transaction_closed`, so reaching the
/// op set through it would commit after every step and leave
/// [`Step::Rollback`] with nothing to roll back.
fn ops(doc: &AutoCommit) -> &OpSet {
    doc.doc.ops()
}

fn check(doc: &AutoCommit) {
    let mask = doc.doc.mask().map(|m| m.clock().clone());
    ops(doc).assert_counter_index_matches(mask.as_ref());
}

/// [`check`] must not commit, or the generated `Step::Rollback` steps in
/// `index_matches_resum_after_every_step` would have nothing left to roll back
/// and the property would silently stop covering rollback. Reaching the op set
/// through [`AutoCommit::document`] is what would break this.
#[test]
fn check_leaves_the_transaction_open() {
    let mut doc = doc_a();
    doc.put(ROOT, "c", ScalarValue::counter(1)).unwrap();

    check(&doc);

    assert_eq!(doc.pending_ops(), 1);
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

#[test]
fn fresh_counter_has_its_base_in_the_index() {
    let mut doc = AutoCommit::new();
    doc.put(ROOT, "c", ScalarValue::counter(10)).unwrap();

    check(&doc);
    assert_eq!(counter_value(&doc, "c"), Some(10));
}

#[test]
fn local_increments_accumulate() {
    let mut doc = AutoCommit::new();
    doc.put(ROOT, "c", ScalarValue::counter(0)).unwrap();
    for n in 1..=5 {
        doc.increment(ROOT, "c", n).unwrap();
        check(&doc);
    }

    assert_eq!(counter_value(&doc, "c"), Some(15));
}

#[test]
fn increments_across_commits_accumulate() {
    let mut doc = AutoCommit::new();
    doc.put(ROOT, "c", ScalarValue::counter(0)).unwrap();
    doc.commit();
    doc.increment(ROOT, "c", 3).unwrap();
    doc.commit();
    doc.increment(ROOT, "c", 4).unwrap();
    doc.commit();

    check(&doc);
    assert_eq!(counter_value(&doc, "c"), Some(7));
}

#[test]
fn rollback_undoes_increments() {
    let mut doc = AutoCommit::new();
    doc.put(ROOT, "c", ScalarValue::counter(10)).unwrap();
    doc.commit();

    doc.increment(ROOT, "c", 5).unwrap();
    doc.increment(ROOT, "c", 6).unwrap();
    doc.rollback();

    check(&doc);
    assert_eq!(counter_value(&doc, "c"), Some(10));
}

#[test]
fn rollback_undoes_a_new_counter() {
    let mut doc = AutoCommit::new();
    doc.put(ROOT, "a", 1).unwrap();
    doc.commit();

    doc.put(ROOT, "c", ScalarValue::counter(10)).unwrap();
    doc.increment(ROOT, "c", 5).unwrap();
    doc.rollback();

    check(&doc);
    assert_eq!(counter_value(&doc, "c"), None);
}

#[test]
fn overwriting_a_counter_leaves_its_total_intact() {
    // The overwritten counter is hidden by `visible`; its own total must not
    // be disturbed so historical/conflict reads of it still work.
    let mut doc = AutoCommit::new();
    doc.put(ROOT, "c", ScalarValue::counter(10)).unwrap();
    doc.increment(ROOT, "c", 5).unwrap();
    doc.put(ROOT, "c", "now a string").unwrap();

    check(&doc);
    assert_eq!(counter_value(&doc, "c"), None);
}

#[test]
fn deleting_a_counter() {
    let mut doc = AutoCommit::new();
    doc.put(ROOT, "c", ScalarValue::counter(10)).unwrap();
    doc.increment(ROOT, "c", 5).unwrap();
    doc.delete(ROOT, "c").unwrap();

    check(&doc);
    assert_eq!(counter_value(&doc, "c"), None);
}

/// An incoming delete suppresses the counter without disturbing its total,
/// so an undo or historical read of the op still sees the right value.
#[test]
fn apply_a_delete_over_an_incremented_counter() {
    let mut a = doc_a();
    a.put(ROOT, "c", ScalarValue::counter(0)).unwrap();
    a.commit();
    let mut b = fork_b(&mut a);

    b.increment(ROOT, "c", 7).unwrap();
    b.commit();
    a.delete(ROOT, "c").unwrap();
    a.commit();

    let heads = b.get_heads();
    b.apply_changes(a.get_changes(&heads)).unwrap();

    check(&b);
    assert_eq!(counter_value(&b, "c"), None);
}

#[test]
fn remote_increments_arrive_via_apply() {
    let mut a = doc_a();
    a.put(ROOT, "c", ScalarValue::counter(0)).unwrap();
    a.commit();
    let mut b = fork_b(&mut a);

    for _ in 0..10 {
        a.increment(ROOT, "c", 1).unwrap();
    }
    a.commit();
    let heads = b.get_heads();
    b.apply_changes(a.get_changes(&heads)).unwrap();

    check(&b);
    assert_eq!(counter_value(&b, "c"), Some(10));
}

#[test]
fn concurrent_increments_from_two_actors_sum() {
    let mut a = doc_a();
    a.put(ROOT, "c", ScalarValue::counter(0)).unwrap();
    a.commit();
    let mut b = fork_b(&mut a);

    a.increment(ROOT, "c", 3).unwrap();
    b.increment(ROOT, "c", 4).unwrap();
    a.merge(&mut b).unwrap();
    b.merge(&mut a).unwrap();

    check(&a);
    check(&b);
    assert_eq!(counter_value(&a, "c"), Some(7));
    assert_eq!(counter_value(&b, "c"), Some(7));
}

#[test]
fn conflicting_counters_each_carry_their_own_total() {
    // `a`'s actor sorts before `b`'s, so `a`'s counter is the first conflict.
    let mut a = doc_a();
    a.put(ROOT, "x", 1).unwrap();
    a.commit();
    let mut b = fork_b(&mut a);

    a.put(ROOT, "c", ScalarValue::counter(100)).unwrap();
    a.increment(ROOT, "c", 1).unwrap();
    b.put(ROOT, "c", ScalarValue::counter(200)).unwrap();
    b.increment(ROOT, "c", 2).unwrap();
    a.merge(&mut b).unwrap();

    check(&a);
    let totals: Vec<i64> = a
        .get_all(ROOT, "c")
        .unwrap()
        .into_iter()
        .filter_map(|(v, _)| match v {
            Value::Scalar(s) => match s.as_ref() {
                ScalarValue::Counter(c) => Some(c.into()),
                _ => None,
            },
            _ => None,
        })
        .collect();
    assert_eq!(totals, vec![101, 202]);
}

#[test]
fn increment_onto_conflicted_register_increments_the_counter() {
    let mut a = doc_a();
    a.put(ROOT, "x", 1).unwrap();
    a.commit();
    let mut b = fork_b(&mut a);

    a.put(ROOT, "c", ScalarValue::counter(100)).unwrap();
    b.put(ROOT, "c", "str").unwrap();
    a.merge(&mut b).unwrap();
    a.increment(ROOT, "c", 5).unwrap();

    check(&a);
    // The increment supersedes the conflicting string.
    assert_eq!(counter_value(&a, "c"), Some(105));
}

#[test]
fn counters_in_lists() {
    let mut doc = AutoCommit::new();
    let list = doc.put_object(ROOT, "l", ObjType::List).unwrap();
    doc.insert(&list, 0, ScalarValue::counter(0)).unwrap();
    doc.insert(&list, 1, "between").unwrap();
    doc.insert(&list, 2, ScalarValue::counter(10)).unwrap();
    doc.increment(&list, 0, 1).unwrap();
    doc.increment(&list, 2, 2).unwrap();
    doc.delete(&list, 1).unwrap();

    check(&doc);
}

#[test]
fn save_and_load_rebuilds_the_index() {
    let mut doc = AutoCommit::new();
    doc.put(ROOT, "c", ScalarValue::counter(10)).unwrap();
    for _ in 0..7 {
        doc.increment(ROOT, "c", 1).unwrap();
    }
    doc.put(ROOT, "s", "string").unwrap();
    doc.commit();

    let bytes = doc.save();
    let loaded = AutoCommit::load(&bytes).unwrap();

    check(&loaded);
    assert_eq!(counter_value(&loaded, "c"), Some(17));
}

#[test]
fn save_and_load_then_increment() {
    let mut doc = AutoCommit::new();
    doc.put(ROOT, "c", ScalarValue::counter(10)).unwrap();
    doc.increment(ROOT, "c", 1).unwrap();
    doc.commit();

    let bytes = doc.save();
    let mut loaded = AutoCommit::load(&bytes).unwrap();
    loaded.increment(ROOT, "c", 1).unwrap();

    check(&loaded);
    assert_eq!(counter_value(&loaded, "c"), Some(12));
}

#[derive(Debug, Clone)]
enum Step {
    /// `put(key, counter(base))` on `actor`.
    PutCounter { actor: usize, key: u8, base: i64 },
    /// `put(key, int)` on `actor` — overwrites whatever is there.
    PutInt { actor: usize, key: u8 },
    /// `increment(key, n)` on `actor`; skipped if the key holds no counter.
    Increment { actor: usize, key: u8, n: i64 },
    /// `delete(key)` on `actor`; skipped if the key is absent.
    Delete { actor: usize, key: u8 },
    /// Commit on `actor` (exercises the per-commit path).
    Commit { actor: usize },
    /// Merge `from` into `to` — exercises batch apply with `n` existing
    /// successors on the receiver.
    Merge { from: usize, to: usize },
    /// Roll back the open transaction on `actor`.
    Rollback { actor: usize },
}

fn any_step() -> impl Strategy<Value = Step> {
    let actor = 0usize..2;
    let key = 0u8..3;
    prop_oneof![
        3 => (actor.clone(), key.clone(), -20i64..20)
            .prop_map(|(actor, key, base)| Step::PutCounter { actor, key, base }),
        1 => (actor.clone(), key.clone()).prop_map(|(actor, key)| Step::PutInt { actor, key }),
        8 => (actor.clone(), key.clone(), -5i64..5)
            .prop_map(|(actor, key, n)| Step::Increment { actor, key, n }),
        1 => (actor.clone(), key.clone()).prop_map(|(actor, key)| Step::Delete { actor, key }),
        2 => actor.clone().prop_map(|actor| Step::Commit { actor }),
        2 => (actor.clone(), actor.clone())
            .prop_filter("distinct", |(a, b)| a != b)
            .prop_map(|(from, to)| Step::Merge { from, to }),
        1 => actor.prop_map(|actor| Step::Rollback { actor }),
    ]
}

fn key_name(key: u8) -> String {
    format!("k{key}")
}

/// Which keys in `doc` currently hold a counter, as seen through the public
/// API. Used only to decide whether an `Increment` step is legal.
fn counter_keys(doc: &AutoCommit) -> BTreeMap<u8, i64> {
    (0..3)
        .filter_map(|k| counter_value(doc, &key_name(k)).map(|v| (k, v)))
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// After every step, on every actor, the index equals the slow re-sum.
    #[test]
    fn index_matches_resum_after_every_step(
        steps in proptest::collection::vec(any_step(), 1..40),
    ) {
        let mut root = doc_a();
        root.put(ROOT, "init", 0).unwrap();
        root.commit();
        let mut docs = vec![
            root.fork().with_actor(actor(0x20)),
            root.fork().with_actor(actor(0x30)),
        ];

        for step in steps {
            match step {
                Step::PutCounter { actor, key, base } => {
                    docs[actor].put(ROOT, key_name(key), ScalarValue::counter(base)).unwrap();
                }
                Step::PutInt { actor, key } => {
                    docs[actor].put(ROOT, key_name(key), 1).unwrap();
                }
                Step::Increment { actor, key, n } => {
                    if counter_keys(&docs[actor]).contains_key(&key) {
                        docs[actor].increment(ROOT, key_name(key), n).unwrap();
                    }
                }
                Step::Delete { actor, key } => {
                    if docs[actor].get(ROOT, key_name(key)).unwrap().is_some() {
                        docs[actor].delete(ROOT, key_name(key)).unwrap();
                    }
                }
                Step::Commit { actor } => {
                    docs[actor].commit();
                }
                Step::Merge { from, to } => {
                    let (a, b) = if from < to {
                        let (l, r) = docs.split_at_mut(to);
                        (&mut l[from], &mut r[0])
                    } else {
                        let (l, r) = docs.split_at_mut(from);
                        (&mut r[0], &mut l[to])
                    };
                    b.merge(a).unwrap();
                }
                Step::Rollback { actor } => {
                    docs[actor].rollback();
                }
            }
            for doc in docs.iter() {
                check(doc);
            }
        }
    }

    /// Fully merged documents agree on every counter, and the value the
    /// index reports equals the value from a fresh load of the saved bytes
    /// (which rebuilds the index from scratch).
    #[test]
    fn merged_docs_agree_and_survive_reload(
        steps in proptest::collection::vec(any_step(), 1..40),
    ) {
        let mut root = doc_a();
        root.put(ROOT, "init", 0).unwrap();
        root.commit();
        let mut docs = vec![
            root.fork().with_actor(actor(0x20)),
            root.fork().with_actor(actor(0x30)),
        ];

        for step in steps {
            match step {
                Step::PutCounter { actor, key, base } => {
                    docs[actor].put(ROOT, key_name(key), ScalarValue::counter(base)).unwrap();
                }
                Step::PutInt { actor, key } => {
                    docs[actor].put(ROOT, key_name(key), 1).unwrap();
                }
                Step::Increment { actor, key, n } => {
                    if counter_keys(&docs[actor]).contains_key(&key) {
                        docs[actor].increment(ROOT, key_name(key), n).unwrap();
                    }
                }
                Step::Delete { actor, key } => {
                    if docs[actor].get(ROOT, key_name(key)).unwrap().is_some() {
                        docs[actor].delete(ROOT, key_name(key)).unwrap();
                    }
                }
                Step::Commit { actor } => {
                    docs[actor].commit();
                }
                Step::Merge { .. } | Step::Rollback { .. } => {}
            }
        }

        let (a, b) = docs.split_at_mut(1);
        let (a, b) = (&mut a[0], &mut b[0]);
        a.merge(b).unwrap();
        b.merge(a).unwrap();
        check(a);
        check(b);
        prop_assert_eq!(counter_keys(a), counter_keys(b));

        let reloaded = AutoCommit::load(&a.save()).unwrap();
        prop_assert_eq!(counter_keys(&reloaded), counter_keys(a));
    }
}

/// Rolling back an extreme increment restores the counter.
///
/// `undo_succ` negates the increment, and `i64::MIN` is its own negation
/// under two's complement, so this only holds with wrapping arithmetic.
#[test]
fn rollback_of_an_extreme_increment() {
    let mut doc = doc_a();
    doc.put(ROOT, "c", ScalarValue::counter(0)).unwrap();
    doc.commit();

    doc.increment(ROOT, "c", i64::MIN).unwrap();
    doc.rollback();

    check(&doc);
    assert_eq!(counter_value(&doc, "c"), Some(0));
}
