//! Unit tests for [`CounterIndex`]'s set of methods.

use crate::op_set2::op::{ChangeOp, OpBuilder, SuccInsert, SuccessorValue};
use crate::op_set2::op_set::counter_index::CounterIndex;
use crate::op_set2::types::{Action, KeyRef, ScalarValue};
use crate::types::{ObjId, OpId};
use proptest::prelude::*;
use std::borrow::Cow;

fn id(counter: u64, actor: usize) -> OpId {
    OpId::new(counter, actor)
}

/// A `Set` op at `ROOT["k"]` with the given value and successors.
fn op(id: OpId, value: ScalarValue<'static>, succ: Vec<SuccessorValue>) -> ChangeOp {
    ChangeOp {
        succ,
        pos: None,
        subsort: 0,
        conflicted: false,
        masked: false,
        bld: OpBuilder {
            id,
            obj: ObjId::root(),
            action: Action::Set,
            key: KeyRef::Map(Cow::Borrowed("k")),
            value,
            insert: false,
            expand: false,
            mark_name: None,
            pred: vec![],
        },
    }
}

fn counter(id: OpId, base: i64, succ: Vec<SuccessorValue>) -> ChangeOp {
    op(id, ScalarValue::Counter(base), succ)
}

fn string(id: OpId) -> ChangeOp {
    op(id, ScalarValue::str("s"), vec![])
}

fn inc(id: OpId, n: i64) -> SuccessorValue {
    SuccessorValue::new(id, Some(n), false)
}

fn masked_inc(id: OpId, n: i64) -> SuccessorValue {
    SuccessorValue::new(id, Some(n), true)
}

fn delete(id: OpId) -> SuccessorValue {
    SuccessorValue::new(id, None, false)
}

/// A `SuccInsert` targeting the op at `pos`; only `pos`, `inc` and `masked`
/// matter to the index.
fn insert(pos: usize, inc: Option<i64>, masked: bool) -> SuccInsert {
    SuccInsert {
        id: id(99, 0),
        pos,
        inc,
        len: 0,
        sub_pos: 0,
        masked,
    }
}

fn totals(index: &CounterIndex) -> Vec<Option<i64>> {
    (0..index.len()).map(|pos| index.get(pos)).collect()
}

#[test]
fn from_values_round_trips() {
    let index = CounterIndex::from_values(vec![None, Some(3), None, Some(-7)]);

    assert_eq!(index.len(), 4);
    assert_eq!(totals(&index), vec![None, Some(3), None, Some(-7)]);
}

#[test]
fn empty_index_has_no_slots() {
    let index = CounterIndex::new();

    assert_eq!(index.len(), 0);
}

#[test]
fn splice_counter_with_no_successors_is_its_base() {
    let mut index = CounterIndex::new();
    let ops = [counter(id(1, 0), 10, vec![])];

    index.splice(0, 0, ops.iter());

    assert_eq!(totals(&index), vec![Some(10)]);
}

#[test]
fn splice_counter_sums_its_increment_successors() {
    let mut index = CounterIndex::new();
    let ops = [counter(
        id(1, 0),
        10,
        vec![inc(id(2, 0), 5), inc(id(3, 1), -2)],
    )];

    index.splice(0, 0, ops.iter());

    assert_eq!(totals(&index), vec![Some(13)]);
}

#[test]
fn splice_counter_ignores_delete_successors() {
    let mut index = CounterIndex::new();
    let ops = [counter(
        id(1, 0),
        10,
        vec![inc(id(2, 0), 5), delete(id(3, 0))],
    )];

    index.splice(0, 0, ops.iter());

    assert_eq!(totals(&index), vec![Some(15)]);
}

#[test]
fn splice_counter_ignores_masked_increment_successors() {
    // A masked increment is still linked (so its slot in `inc` exists) but
    // must not contribute to the current total, matching `visible`.
    let mut index = CounterIndex::new();
    let ops = [counter(
        id(1, 0),
        10,
        vec![inc(id(2, 0), 5), masked_inc(id(3, 1), 100)],
    )];

    index.splice(0, 0, ops.iter());

    assert_eq!(totals(&index), vec![Some(15)]);
}

#[test]
fn splice_non_counter_is_none() {
    let mut index = CounterIndex::new();
    let ops = [string(id(1, 0))];

    index.splice(0, 0, ops.iter());

    assert_eq!(totals(&index), vec![None]);
}

#[test]
fn splice_inserts_at_position_and_shifts_later_slots() {
    let mut index = CounterIndex::from_values(vec![Some(1), Some(3)]);
    let ops = [string(id(5, 0)), counter(id(6, 0), 2, vec![])];

    index.splice(1, 0, ops.iter());

    assert_eq!(totals(&index), vec![Some(1), None, Some(2), Some(3)]);
}

#[test]
fn splice_with_del_removes_slots() {
    let mut index = CounterIndex::from_values(vec![Some(1), None, Some(3), Some(4)]);

    index.splice(1, 2, std::iter::empty::<&ChangeOp>());

    assert_eq!(totals(&index), vec![Some(1), Some(4)]);
}

#[test]
fn splice_replaces_slots_when_both_deleting_and_inserting() {
    let mut index = CounterIndex::from_values(vec![Some(1), Some(2), Some(3)]);
    let ops = [counter(id(7, 0), 20, vec![])];

    index.splice(1, 1, ops.iter());

    assert_eq!(totals(&index), vec![Some(1), Some(20), Some(3)]);
}

#[test]
fn add_succ_unmasked_increment_adds_to_total() {
    let mut index = CounterIndex::from_values(vec![None, Some(10)]);

    index.add_succ(&insert(1, Some(5), false));

    assert_eq!(totals(&index), vec![None, Some(15)]);
}

#[test]
fn add_succ_negative_increment_subtracts() {
    let mut index = CounterIndex::from_values(vec![Some(10)]);

    index.add_succ(&insert(0, Some(-3), false));

    assert_eq!(totals(&index), vec![Some(7)]);
}

#[test]
fn add_succ_masked_increment_does_not_change_total() {
    let mut index = CounterIndex::from_values(vec![Some(10)]);

    index.add_succ(&insert(0, Some(5), true));

    assert_eq!(totals(&index), vec![Some(10)]);
}

#[test]
fn add_succ_delete_does_not_change_total() {
    // A deleting successor hides the counter via `visible`; its total is
    // unaffected, so a historical/undo view of the op still reads correctly.
    let mut index = CounterIndex::from_values(vec![Some(10)]);

    index.add_succ(&insert(0, None, false));

    assert_eq!(totals(&index), vec![Some(10)]);
}

#[test]
fn add_succ_on_non_counter_slot_leaves_none() {
    // `add_succ_with_mask` already drops `inc` for non-counters, so this is
    // defensive: a stray increment must not conjure a total.
    let mut index = CounterIndex::from_values(vec![None]);

    index.add_succ(&insert(0, Some(5), false));

    assert_eq!(totals(&index), vec![None]);
}

#[test]
fn add_succ_only_touches_the_target_slot() {
    let mut index = CounterIndex::from_values(vec![Some(1), Some(2), Some(3)]);

    index.add_succ(&insert(1, Some(10), false));

    assert_eq!(totals(&index), vec![Some(1), Some(12), Some(3)]);
}

#[test]
fn undo_succ_reverses_add_succ() {
    let mut index = CounterIndex::from_values(vec![Some(10)]);
    let i = insert(0, Some(5), false);

    index.add_succ(&i);
    index.undo_succ(&i);

    assert_eq!(totals(&index), vec![Some(10)]);
}

#[test]
fn undo_succ_of_masked_increment_is_a_noop() {
    let mut index = CounterIndex::from_values(vec![Some(10)]);
    let i = insert(0, Some(5), true);

    index.add_succ(&i);
    index.undo_succ(&i);

    assert_eq!(totals(&index), vec![Some(10)]);
}

#[test]
fn undo_succ_of_delete_is_a_noop() {
    let mut index = CounterIndex::from_values(vec![Some(10)]);
    let i = insert(0, None, false);

    index.add_succ(&i);
    index.undo_succ(&i);

    assert_eq!(totals(&index), vec![Some(10)]);
}

/// `(inc, masked)` for a successor insert, where a `None` inc is a deletion.
///
/// The increment covers the whole `i64` range to cover overflow boundaries.
fn any_insert_shape() -> impl Strategy<Value = (Option<i64>, bool)> {
    (proptest::option::of(any::<i64>()), any::<bool>())
}

proptest! {
    /// Applying a sequence of `add_succ` to one slot, then undoing them in
    /// reverse, restores the original total.
    #[test]
    fn add_then_undo_in_reverse_restores(
        base in any::<i64>(),
        shapes in proptest::collection::vec(any_insert_shape(), 0..16),
    ) {
        let mut index = CounterIndex::from_values(vec![Some(base)]);
        let inserts: Vec<_> = shapes.iter().map(|&(inc, m)| insert(0, inc, m)).collect();

        for i in &inserts {
            index.add_succ(i);
        }
        for i in inserts.iter().rev() {
            index.undo_succ(i);
        }

        prop_assert_eq!(totals(&index), vec![Some(base)]);
    }

    /// After any sequence of `add_succ`, the total is the base plus the sum
    /// of the unmasked increments.
    #[test]
    fn total_is_base_plus_unmasked_increments(
        base in any::<i64>(),
        shapes in proptest::collection::vec(any_insert_shape(), 0..16),
    ) {
        let mut index = CounterIndex::from_values(vec![Some(base)]);
        for &(inc, m) in &shapes {
            index.add_succ(&insert(0, inc, m));
        }

        let expected = shapes
            .iter()
            .filter(|(_, masked)| !masked)
            .filter_map(|(inc, _)| *inc)
            .fold(base, i64::wrapping_add);

        prop_assert_eq!(index.get(0), Some(expected));
    }

    /// `splice` of a counter carrying successors equals building the empty
    /// counter and then `add_succ`-ing each successor.
    #[test]
    fn splice_equals_incremental_add_succ(
        base in any::<i64>(),
        shapes in proptest::collection::vec(any_insert_shape(), 0..16),
    ) {
        let succs: Vec<_> = shapes
            .iter()
            .enumerate()
            .map(|(i, &(inc, m))| SuccessorValue::new(id(i as u64 + 2, 0), inc, m))
            .collect();

        let mut spliced = CounterIndex::new();
        let ops = [counter(id(1, 0), base, succs)];
        spliced.splice(0, 0, ops.iter());

        let mut incremental = CounterIndex::new();
        let bare = [counter(id(1, 0), base, vec![])];
        incremental.splice(0, 0, bare.iter());
        for &(inc, m) in &shapes {
            incremental.add_succ(&insert(0, inc, m));
        }

        prop_assert_eq!(totals(&spliced), totals(&incremental));
    }

    /// Splicing is positional: the slots outside `pos..pos+del` are
    /// untouched and the inserted ones land in order.
    #[test]
    fn splice_is_positional(
        before in proptest::collection::vec(proptest::option::of(-50i64..50), 0..8),
        after in proptest::collection::vec(proptest::option::of(-50i64..50), 0..8),
        removed in proptest::collection::vec(proptest::option::of(-50i64..50), 0..8),
        added in proptest::collection::vec(proptest::option::of(-50i64..50), 0..8),
    ) {
        let initial: Vec<_> = before.iter().chain(&removed).chain(&after).copied().collect();
        let mut index = CounterIndex::from_values(initial);

        let ops: Vec<ChangeOp> = added
            .iter()
            .enumerate()
            .map(|(i, v)| match v {
                Some(b) => counter(id(i as u64 + 1, 0), *b, vec![]),
                None => string(id(i as u64 + 1, 0)),
            })
            .collect();
        index.splice(before.len(), removed.len(), ops.iter());

        let expected: Vec<_> = before.iter().chain(&added).chain(&after).copied().collect();
        prop_assert_eq!(totals(&index), expected);
    }
}

#[test]
fn undo_succ_inverts_i64_min() {
    // `i64::MIN.wrapping_neg()` is still `i64::MIN`, so only wrapping addition
    // makes the undo a true inverse.
    let mut index = CounterIndex::from_values(vec![Some(0)]);
    let i = insert(0, Some(i64::MIN), false);

    index.add_succ(&i);
    index.undo_succ(&i);

    assert_eq!(index.get(0), Some(0));
}

#[test]
fn a_transient_overflow_does_not_lose_the_total() {
    // `OpSet::add_succ` applies inserts in reverse, so a batch whose net
    // effect is representable can still pass through an unrepresentable
    // intermediate. Wrapping arithmetic is a group operation, so the order
    // cannot change the result.
    let mut index = CounterIndex::from_values(vec![Some(i64::MAX)]);

    index.add_succ(&insert(0, Some(1), false));
    assert_eq!(index.get(0), Some(i64::MIN), "wrapped at the boundary");
    index.add_succ(&insert(0, Some(-1), false));

    assert_eq!(index.get(0), Some(i64::MAX));
}

proptest! {
    /// The total does not depend on the order successors are applied.
    ///
    /// This matters because `OpSet::add_succ` applies inserts in reverse and
    /// merged increments arrive in arbitrary order. Wrapping addition is a
    /// group operation, so it is associative and commutative even at the
    /// `i64` boundary; saturating addition would not be.
    #[test]
    fn total_is_independent_of_application_order(
        base in any::<i64>(),
        shapes in proptest::collection::vec((proptest::option::of(any::<i64>()), any::<bool>()), 0..16),
    ) {
        let total_applying = |order: &mut dyn Iterator<Item = &(Option<i64>, bool)>| {
            let mut index = CounterIndex::from_values(vec![Some(base)]);
            for &(inc, masked) in order {
                index.add_succ(&insert(0, inc, masked));
            }
            index.get(0)
        };

        let forward = total_applying(&mut shapes.iter());
        let reversed = total_applying(&mut shapes.iter().rev());

        prop_assert_eq!(forward, reversed);
    }
}
