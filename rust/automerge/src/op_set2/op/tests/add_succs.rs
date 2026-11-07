use crate::op_set2::op::{Op, SuccCursors, SuccInsert, SuccessorValue};
use crate::op_set2::types::{Action, ActorIdx, KeyRef, ScalarValue};
use crate::types::{ObjId, OpId};
use proptest::prelude::*;
use std::borrow::Cow;

/// Column backing for one op's successor slice, kept alive so the op's
/// cursors can borrow it.
struct Fixture {
    actors: hexane::Column<ActorIdx>,
    counters: hexane::DeltaColumn<u32>,
    incs: hexane::Column<Option<i64>>,
    len: usize,
}

impl Fixture {
    fn new(succs: &[OpId]) -> Self {
        Self {
            actors: hexane::Column::from_values(succs.iter().map(|s| s.actoridx()).collect()),
            counters: hexane::DeltaColumn::from_values(
                succs.iter().map(|s| s.counter() as u32).collect(),
            ),
            incs: hexane::Column::fill(succs.len(), None),
            len: succs.len(),
        }
    }

    fn op(&self, value: ScalarValue<'static>) -> Op<'_> {
        Op {
            pos: 7,
            conflict: false,
            id: OpId::new(1, 0),
            action: Action::Set,
            obj: ObjId::root(),
            key: KeyRef::Map(Cow::Borrowed("k")),
            insert: false,
            value,
            expand: false,
            mark_name: None,
            succ_cursors: SuccCursors {
                len: self.len,
                succ_actor: self.actors.iter(),
                succ_counter: self.counters.iter(),
                inc_values: self.incs.iter(),
            },
        }
    }
}

fn id(counter: u64, actor: usize) -> OpId {
    OpId::new(counter, actor)
}

fn ids(pairs: &[(u64, usize)]) -> Vec<OpId> {
    pairs.iter().map(|&(c, a)| id(c, a)).collect()
}

fn succ(id: OpId, inc: Option<i64>, masked: bool) -> SuccessorValue {
    SuccessorValue::new(id, inc, masked)
}

/// Unmasked `+1` increments, one per id.
fn incoming(ids: &[OpId]) -> Vec<SuccessorValue> {
    ids.iter().map(|&i| succ(i, Some(1), false)).collect()
}

fn sub_positions(inserts: &[SuccInsert]) -> Vec<usize> {
    inserts.iter().map(|i| i.sub_pos).collect()
}

fn one_by_one(op: &Op<'_>, incoming: &[SuccessorValue]) -> Vec<SuccInsert> {
    incoming
        .iter()
        .map(|v| op.add_succ_with_mask(v.id, v.increment, v.masked))
        .collect()
}

#[test]
fn interleaved_incoming_lands_between_existing() {
    let fx = Fixture::new(&ids(&[(1, 0), (3, 0), (5, 0)]));
    let op = fx.op(ScalarValue::Counter(0));
    let incoming = incoming(&ids(&[(0, 1), (2, 1), (4, 1), (6, 1)]));

    let got: Vec<_> = op.add_succs(incoming.clone()).collect();

    assert_eq!(sub_positions(&got), vec![0, 1, 2, 3]);
    assert_eq!(got, one_by_one(&op, &incoming));
}

#[test]
fn incoming_after_all_existing_appends() {
    let fx = Fixture::new(&ids(&[(1, 0), (3, 0), (5, 0)]));
    let op = fx.op(ScalarValue::Counter(0));
    let incoming = incoming(&ids(&[(6, 0), (7, 0), (8, 1)]));

    let got: Vec<_> = op.add_succs(incoming.clone()).collect();

    assert_eq!(sub_positions(&got), vec![3, 3, 3]);
    assert!(got.iter().all(|i| i.len == 3 && i.pos == 7));
    assert_eq!(got, one_by_one(&op, &incoming));
}

#[test]
fn empty_existing_puts_everything_at_zero() {
    let fx = Fixture::new(&[]);
    let op = fx.op(ScalarValue::Counter(0));
    let incoming = incoming(&ids(&[(2, 0), (4, 0)]));

    let got: Vec<_> = op.add_succs(incoming.clone()).collect();

    assert_eq!(sub_positions(&got), vec![0, 0]);
    assert!(got.iter().all(|i| i.len == 0));
    assert_eq!(got, one_by_one(&op, &incoming));
}

#[test]
fn empty_incoming_yields_nothing() {
    let fx = Fixture::new(&ids(&[(1, 0), (3, 0)]));
    let op = fx.op(ScalarValue::Counter(0));

    assert_eq!(op.add_succs(Vec::new()).count(), 0);
}

#[test]
fn same_counter_orders_by_actor() {
    // Existing 4@1; incoming 4@0 sorts before it, 4@2 after it.
    let fx = Fixture::new(&ids(&[(4, 1)]));
    let op = fx.op(ScalarValue::Counter(0));
    let incoming = incoming(&ids(&[(4, 0), (4, 2)]));

    let got: Vec<_> = op.add_succs(incoming.clone()).collect();

    assert_eq!(sub_positions(&got), vec![0, 1]);
    assert_eq!(got, one_by_one(&op, &incoming));
}

#[test]
fn increments_are_dropped_for_non_counter_preds() {
    let fx = Fixture::new(&ids(&[(1, 0)]));
    let op = fx.op(ScalarValue::str("not a counter"));
    let incoming = vec![
        succ(id(2, 0), Some(5), false),
        succ(id(3, 0), Some(7), true),
    ];

    let got: Vec<_> = op.add_succs(incoming.clone()).collect();

    assert!(got.iter().all(|i| i.inc.is_none()));
    assert_eq!(got, one_by_one(&op, &incoming));
}

#[test]
fn increments_and_mask_flags_are_preserved_for_counters() {
    let fx = Fixture::new(&ids(&[(1, 0)]));
    let op = fx.op(ScalarValue::Counter(0));
    let incoming = vec![
        succ(id(2, 0), Some(5), false),
        succ(id(3, 0), None, false),
        succ(id(4, 0), Some(7), true),
    ];

    let got: Vec<_> = op.add_succs(incoming.clone()).collect();

    let flags: Vec<_> = got.iter().map(|i| (i.id, i.inc, i.masked)).collect();
    let expected: Vec<_> = incoming
        .iter()
        .map(|v| (v.id, v.increment, v.masked))
        .collect();
    assert_eq!(flags, expected);
    assert_eq!(got, one_by_one(&op, &incoming));
}

/// Strictly ascending, de-duplicated op ids drawn from a small space so
/// collisions and interleavings with the existing slice are common.
fn sorted_ids() -> impl Strategy<Value = Vec<OpId>> {
    proptest::collection::btree_set((0u64..40, 0usize..3), 0..24)
        .prop_map(|set| set.into_iter().map(|(c, a)| id(c, a)).collect())
}

proptest! {
    #[test]
    fn matches_add_succ_with_mask_per_item(
        existing in sorted_ids(),
        new in sorted_ids(),
        is_counter in any::<bool>(),
    ) {
        // Incoming ids are new ops, so they never collide with existing successors.
        let new: Vec<OpId> = new.into_iter().filter(|i| !existing.contains(i)).collect();
        let fx = Fixture::new(&existing);
        let value = if is_counter { ScalarValue::Counter(0) } else { ScalarValue::Null };
        let op = fx.op(value);
        let incoming: Vec<_> = new
            .iter()
            .enumerate()
            .map(|(i, &id)| succ(id, (i % 3 != 0).then_some(i as i64), i % 4 == 0))
            .collect();

        let got: Vec<_> = op.add_succs(incoming.clone()).collect();

        prop_assert_eq!(got, one_by_one(&op, &incoming));
    }
}
