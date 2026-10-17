//! Clocks and clock ranges produced by [`Automerge`] documents, which allow
//! callers to check the visibility of operations.
//!
//! [`Automerge`]: super::Automerge

use std::collections::HashMap;

use crate::actor::{ActorRemoval, ActorShift};
use crate::clock::Clock;
use crate::types::OpId;
use crate::write_frontier::{Boundary, WriteFrontier};
use crate::{Author, ChangeId};

use std::borrow::Cow;

/// A point to diff from or to: some heads, read either under the document's
/// current write-frontier or under one of their own.
///
/// Heads convert into a viewpoint under the current write-frontier. Take one
/// with `viewpoint()` before a change and pass it to `diff` after to get
/// patches that include write-frontier changes, which leave the heads alone.
/// A boundary its document had not yet received hides its author entirely; for
/// a viewpoint built with [`Viewpoint::new`] that is any boundary outside the
/// history of `heads`.
#[derive(Debug, Clone, Default)]
pub struct Viewpoint {
    pub(crate) heads: Vec<ChangeId>,
    /// `None`: whatever write-frontier the document has when the viewpoint is
    /// read.
    pub(crate) write_frontier: Option<WriteFrontier>,
    /// The visible clock, valid while the document's actor generation matches.
    pub(crate) cache: Option<(u64, VisibleClock)>,
    /// The visibility epoch, for a viewpoint of a document's current state.
    pub(crate) epoch: Option<u64>,
    /// The boundaries still pending when a document made this viewpoint;
    /// `None` counts those outside the history of `heads`.
    pub(crate) pending: Option<Vec<Boundary>>,
}

impl PartialEq for Viewpoint {
    fn eq(&self, other: &Self) -> bool {
        self.heads == other.heads && self.write_frontier == other.write_frontier
    }
}

impl Eq for Viewpoint {}

impl Viewpoint {
    /// `heads` under `write_frontier`, as from `get_heads()` and
    /// `get_write_frontier()`.
    pub fn new(
        heads: Vec<ChangeId>,
        write_frontier: HashMap<Author<'static>, Vec<ChangeId>>,
    ) -> Self {
        Viewpoint {
            write_frontier: Some(WriteFrontier::from_ids(write_frontier)),
            ..Viewpoint::at_heads(heads)
        }
    }

    /// `heads` under whatever write-frontier the document has when read.
    pub(crate) fn at_heads(heads: Vec<ChangeId>) -> Self {
        Viewpoint {
            heads,
            write_frontier: None,
            cache: None,
            epoch: None,
            pending: None,
        }
    }

    pub fn heads(&self) -> &[ChangeId] {
        &self.heads
    }

    /// `None` for heads read under the document's current write-frontier.
    /// Boundaries named by a hash the document never held are left out.
    pub fn write_frontier(&self) -> Option<HashMap<Author<'static>, Vec<ChangeId>>> {
        self.write_frontier.as_ref().map(WriteFrontier::ids)
    }
}

impl From<&[ChangeId]> for Viewpoint {
    fn from(heads: &[ChangeId]) -> Self {
        Viewpoint::at_heads(heads.to_vec())
    }
}

impl From<&Vec<ChangeId>> for Viewpoint {
    fn from(heads: &Vec<ChangeId>) -> Self {
        Viewpoint::at_heads(heads.clone())
    }
}

impl<const N: usize> From<&[ChangeId; N]> for Viewpoint {
    fn from(heads: &[ChangeId; N]) -> Self {
        Viewpoint::at_heads(heads.to_vec())
    }
}

impl From<&Viewpoint> for Viewpoint {
    fn from(viewpoint: &Viewpoint) -> Self {
        viewpoint.clone()
    }
}

/// Changes whenever the actor table does, and on every clone, so a cached
/// clock is only reused by the document instance that made it.
#[derive(Debug)]
pub(crate) struct ActorGeneration(u64);

impl ActorGeneration {
    pub(crate) fn new() -> Self {
        let mut buf = [0u8; 8];
        getrandom::fill(&mut buf).expect("random number generator failed");
        ActorGeneration(u64::from_le_bytes(buf))
    }

    pub(crate) fn get(&self) -> u64 {
        self.0
    }

    pub(crate) fn bump(&mut self) {
        self.0 = self.0.wrapping_add(1);
    }
}

impl Clone for ActorGeneration {
    fn clone(&self) -> Self {
        ActorGeneration::new()
    }
}

/// A [`Clock`] that is used for reads.
///
/// It consists of the causal clock, taken at some heads of the change graph,
/// and can optionally be intersected with a visibility [`Mask`].
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct VisibleClock(Clock, Option<Clock>);

impl VisibleClock {
    /// Construct a new [`VisibleClock`] using the `causal` clock and the optional `mask`.
    ///
    /// If the mask is supplied then the two clocks are intersected to get the
    /// final [`Clock`].
    pub(super) fn new(causal: Clock, mask: Option<&Mask>) -> Self {
        match mask {
            Some(mask) => {
                let mut visible = causal.clone();
                visible.intersect(&mask.0);
                Self(visible, Some(causal))
            }
            None => Self(causal, None),
        }
    }

    /// Whether `id` is in the causal history, visible or not.
    fn exists(&self, id: &OpId) -> bool {
        self.1.as_ref().unwrap_or(&self.0).covers(id)
    }

    /// Return a reference to the underlying [`Clock`].
    pub(crate) fn clock(&self) -> &Clock {
        &self.0
    }

    /// Returns `true` if the clock covers [`OpId`] (see [`Clock::covers`]).
    pub(crate) fn covers(&self, id: &OpId) -> bool {
        self.0.covers(id)
    }

    /// Isolate the given `actor` by setting their counter to the max value.
    pub(crate) fn isolate(&mut self, actor: usize) {
        self.0.isolate(actor)
    }

    /// Create new, empty [`Clock`] which is the same size as this clock.
    pub(crate) fn empty_like(&self) -> Clock {
        self.0.empty_like()
    }

    /// Returns `true` if any operation is covered.
    pub(crate) fn covers_something(&self) -> bool {
        self.0.covers_something()
    }
}

/// A [`Clock`] that restricts an actors operations from being visible.
///
/// A `u32::MAX` means the actor is unrestricted.
///
/// See [`VisibleClock::new`].
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Mask(Clock);

impl Mask {
    /// Construct a new [`Mask`].
    pub(super) fn new(clock: Clock) -> Self {
        Self(clock)
    }

    /// Return a reference to the underlying [`Clock`].
    pub(crate) fn clock(&self) -> &Clock {
        &self.0
    }

    /// Returns `true` if the mask hides the given [`OpId`].
    ///
    /// This is the inverse of [`Clock::covers`].
    pub(crate) fn hides(&self, id: &OpId) -> bool {
        !self.0.covers(id)
    }

    /// Whether the mask hides any of `actor`'s ops.
    pub(crate) fn restricts(&self, actor: usize) -> bool {
        self.0.max_op(actor) < u64::from(u32::MAX)
    }

    /// Add an unrestricted slot for an actor just inserted into the actor table.
    pub(crate) fn insert_actor(&mut self, shift: &ActorShift) {
        self.0.insert_actor(shift, u32::MAX)
    }

    /// Drop the mask slot for an actor just removed from the actor table.
    pub(crate) fn remove_actor(&mut self, removal: &ActorRemoval) {
        self.0.remove_actor(removal);
    }
}

/// A read position: the document's current state, where the indexes are
/// authoritative, or its state at a clock, under the write-frontier.
#[derive(Debug, Clone, PartialEq)]
pub struct ReadAt<'a>(Inner<'a>);

#[derive(Debug, Clone, PartialEq)]
enum Inner<'a> {
    /// The snapshot is given for the current heads, and thus no causal clock is
    /// required. However, a [`Mask`] may be provided to hide operations.
    Current { mask: Option<Cow<'a, Mask>> },
    /// The snapshot is historical for some given heads, and it may be combined
    /// with a [`Mask`] to also hide operations (see [`VisibleClock::new`]).
    At(Cow<'a, VisibleClock>),
}

impl<'a> ReadAt<'a> {
    /// A current read carrying the document's write-frontier `mask`, if any.
    pub(super) fn current(mask: Option<Cow<'a, Mask>>) -> Self {
        Self(Inner::Current { mask })
    }

    /// Construct a historical snapshot.
    pub(super) fn at(v: Cow<'a, VisibleClock>) -> Self {
        Self(Inner::At(v))
    }

    /// Return a historical clock, if this snapshot is configured to do so.
    ///
    /// `None` implies that indexes are authoritative, and is used for the fast path.
    pub(crate) fn historical(&self) -> Option<&Clock> {
        match &self.0 {
            Inner::At(v) => Some(v.clock()),
            Inner::Current { .. } => None,
        }
    }

    /// Return the [`Clock`] for op comparison on slow paths.
    ///
    /// `None` implies that there is no filtering required.
    pub(crate) fn historical_slow(&self) -> Option<&Clock> {
        match &self.0 {
            Inner::At(v) => Some(v.clock()),
            Inner::Current { mask } => mask.as_deref().map(Mask::clock),
        }
    }

    /// Wrap this read position into a single-endpoint [`ClockRange`].
    pub(crate) fn into_range(self) -> ClockRange<'a> {
        ClockRange(RangeInner::To(self))
    }

    /// Return a borrowed reference of this [`ReadAt`].
    pub(crate) fn borrow(&self) -> ReadAt<'_> {
        match &self.0 {
            Inner::Current { mask } => ReadAt(Inner::Current {
                mask: mask.as_deref().map(Cow::Borrowed),
            }),
            Inner::At(v) => ReadAt(Inner::At(Cow::Borrowed(v.as_ref()))),
        }
    }
}

/// The clocks a diff or range read runs under: a single read position, or a
/// pair of endpoints to diff between.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ClockRange<'a>(RangeInner<'a>);

#[derive(Debug, Clone, PartialEq)]
enum RangeInner<'a> {
    To(ReadAt<'a>),
    /// `after` may be a current read, which diffs against the indexes.
    Between(VisibleClock, ReadAt<'a>),
}

/// A borrowed view of a [`ClockRange`].
#[derive(Debug)]
pub(crate) enum RangeView<'r, 'a> {
    To(&'r ReadAt<'a>),
    Between {
        before: &'r VisibleClock,
        after: &'r ReadAt<'a>,
    },
}

/// For iterators over no op set, which never consult their clock.
impl Default for ClockRange<'_> {
    fn default() -> Self {
        Self(RangeInner::To(ReadAt(Inner::Current { mask: None })))
    }
}

impl<'a> ClockRange<'a> {
    /// A range between two [`VisibleClock`] points.
    pub(crate) fn between(before: VisibleClock, after: VisibleClock) -> Self {
        Self(RangeInner::Between(before, ReadAt::at(Cow::Owned(after))))
    }

    /// A range from `before` to the document's current state.
    pub(super) fn between_current(before: VisibleClock, current: ReadAt<'a>) -> Self {
        Self(RangeInner::Between(before, current))
    }

    /// A range from the start of the document up to this point.
    pub(super) fn to(read: ReadAt<'a>) -> Self {
        Self(RangeInner::To(read))
    }

    /// Borrow the range's endpoints.
    pub(crate) fn view(&self) -> RangeView<'_, 'a> {
        match &self.0 {
            RangeInner::To(read) => RangeView::To(read),
            RangeInner::Between(before, after) => RangeView::Between { before, after },
        }
    }

    /// The read position at this range's `after` endpoint.
    pub(crate) fn read_after(&self) -> ReadAt<'_> {
        match &self.0 {
            RangeInner::To(read) => read.borrow(),
            RangeInner::Between(_, after) => after.borrow(),
        }
    }

    pub(crate) fn visible_after(&self, id: &OpId) -> bool {
        match &self.0 {
            RangeInner::Between(_, after) | RangeInner::To(after) => {
                after.historical_slow().is_none_or(|c| c.covers(id))
            }
        }
    }

    pub(crate) fn visible_before(&self, id: &OpId) -> bool {
        match &self.0 {
            RangeInner::Between(before, _) => before.covers(id),
            RangeInner::To(_) => false,
        }
    }

    /// Whether `id` existed at `before`, even if hidden there: an op that
    /// becomes visible without being new is exposed.
    pub(crate) fn predates(&self, id: &OpId) -> bool {
        match &self.0 {
            RangeInner::Between(before, _) => before.exists(id),
            RangeInner::To(_) => false,
        }
    }

    /// The range for children of the object created by `parent`. `None`
    /// when the children can keep the parent's range.
    ///
    /// When diffing, an object whose creation op is absent from `before` was
    /// not visible in the before view, so its children must all emit as
    /// inserts: they are walked with an empty `before`.
    ///
    /// Without a write-frontier, the visible set is causally closed: covering a
    /// child operation implies covering the object's creation operation.
    /// Emptying `before` does not change the resulting child diff.
    /// A write-frontier can hide the creation operation without hiding its
    /// children, making this adjustment necessary.
    ///
    /// `after` never needs narrowing: [`DiffIter::process_item`]'s `make_obj`
    /// only queues objects that are visible in `after`, so a parent absent
    /// from `after` is never descended into.
    ///
    /// [`DiffIter::process_item`]: crate::iter::DiffIter
    pub(crate) fn descend(&self, parent: &OpId) -> Option<ClockRange<'a>> {
        match &self.0 {
            // An already-empty `before` cannot be narrowed further, so keep
            // the cheap shifting path for diffs from the empty document.
            RangeInner::Between(before, after)
                if before.covers_something() && !before.covers(parent) =>
            {
                Some(Self(RangeInner::Between(
                    VisibleClock::new(before.empty_like(), None),
                    after.clone(),
                )))
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::op_set2::op_set::tests::{with_test_ops, TestOp};
    use crate::op_set2::types::{Action, ScalarValue};
    use crate::op_set2::{KeyRef, OpQuery};
    use crate::types::{ElemId, ObjId};

    #[test]
    fn mask_actor_migration() {
        let mut m = Mask::new(Clock::from_counters([Some(3)]));
        m.insert_actor(&ActorShift::for_test(0));
        assert_eq!(m.clock(), &Clock::from_counters([Some(u32::MAX), Some(3)]));
        assert!(m.hides(&OpId::new(4, 1)));
        assert!(!m.hides(&OpId::new(3, 1)));
    }

    #[test]
    fn descend_narrows_before_only_when_nonempty_before_lacks_parent() {
        let before = VisibleClock::new(Clock::from_counters([Some(2), Some(5)]), None);
        let after = VisibleClock::new(Clock::from_counters([Some(9), Some(9)]), None);
        let range = ClockRange::between(before.clone(), after.clone());
        // parent covered by before: identity
        assert!(range.descend(&OpId::new(2, 0)).is_none());
        // parent not covered by before: the children's before empties out
        // (everything emits as an insert) while after stays untouched
        let narrowed = range.descend(&OpId::new(3, 0)).unwrap();
        assert!(!narrowed.predates(&OpId::new(1, 1)));
        assert!(narrowed.visible_after(&OpId::new(9, 1)));
        assert!(!narrowed.visible_after(&OpId::new(10, 1)));
        // an already-empty before cannot be narrowed further
        let empty = ClockRange::between(
            VisibleClock::new(Clock::from_counters([None, None]), None),
            after,
        );
        assert!(empty.descend(&OpId::new(3, 0)).is_none());
        // A single-endpoint range never narrows. A parent absent from `after` needs no case
        // of its own: DiffIter::process_item's make_obj never descends into
        // it (its diff item is a delete), so descend never sees one.
        let current = ClockRange::to(ReadAt::current(None));
        assert!(current.descend(&OpId::new(3, 0)).is_none());
    }

    #[test]
    fn column_data_op_iterators() {
        let mut actors = vec![crate::ActorId::random(), crate::ActorId::random()];
        actors.sort();

        let test_ops = vec![
            TestOp {
                id: OpId::new(1, 1),
                obj: ObjId::root(),
                action: Action::MakeMap,
                value: ScalarValue::Null,
                key: KeyRef::Map("map".into()),
                insert: false,
                succs: vec![],
                expand: false,
                mark_name: None,
            },
            TestOp {
                id: OpId::new(2, 1),
                obj: ObjId::root(),
                action: Action::MakeMap,
                value: ScalarValue::Null,
                key: KeyRef::Map("list".into()),
                insert: false,
                succs: vec![],
                expand: false,
                mark_name: None,
            },
            TestOp {
                id: OpId::new(3, 1),
                obj: ObjId(OpId::new(1, 1)),
                action: Action::Set,
                value: ScalarValue::str("value1"),
                key: KeyRef::Map("key1".into()),
                insert: false,
                succs: vec![],
                expand: false,
                mark_name: None,
            },
            TestOp {
                id: OpId::new(4, 1),
                obj: ObjId(OpId::new(1, 1)),
                action: Action::Set,
                value: ScalarValue::str("value2a"),
                key: KeyRef::Map("key2".into()),
                insert: false,
                succs: vec![],
                expand: false,
                mark_name: None,
            },
            TestOp {
                id: OpId::new(4, 2),
                obj: ObjId(OpId::new(1, 1)),
                action: Action::Set,
                value: ScalarValue::str("value2b"),
                key: KeyRef::Map("key2".into()),
                insert: false,
                succs: vec![OpId::new(5, 2)],
                expand: false,
                mark_name: None,
            },
            TestOp {
                id: OpId::new(5, 2),
                obj: ObjId(OpId::new(1, 1)),
                action: Action::Set,
                value: ScalarValue::str("value2c"),
                key: KeyRef::Map("key2".into()),
                insert: false,
                succs: vec![],
                expand: false,
                mark_name: None,
            },
            TestOp {
                id: OpId::new(6, 1),
                obj: ObjId(OpId::new(1, 1)),
                action: Action::Set,
                value: ScalarValue::str("value3a"),
                key: KeyRef::Map("key3".into()),
                insert: false,
                succs: vec![OpId::new(7, 2)],
                expand: false,
                mark_name: None,
            },
            TestOp {
                id: OpId::new(7, 2),
                obj: ObjId(OpId::new(1, 1)),
                action: Action::Set,
                value: ScalarValue::str("value3b"),
                key: KeyRef::Map("key3".into()),
                insert: false,
                succs: vec![],
                expand: false,
                mark_name: None,
            },
            TestOp {
                id: OpId::new(8, 1),
                obj: ObjId(OpId::new(2, 1)),
                action: Action::Set,
                value: ScalarValue::str("a"),
                key: KeyRef::Seq(ElemId::head()),
                insert: true,
                succs: vec![],
                expand: false,
                mark_name: None,
            },
            TestOp {
                id: OpId::new(9, 1),
                obj: ObjId(OpId::new(2, 1)),
                action: Action::Set,
                value: ScalarValue::str("b"),
                key: KeyRef::Seq(ElemId(OpId::new(8, 1))),
                insert: true,
                succs: vec![],
                expand: false,
                mark_name: None,
            },
        ];

        with_test_ops(actors, &test_ops, |opset| {
            let iter = opset.iter_obj(&ObjId(OpId::new(1, 1)));
            let ops = iter.collect::<Vec<_>>();
            assert_eq!(&test_ops[2..8], ops.as_slice());

            let range = opset.prop_range(&ObjId(OpId::new(1, 1)), "key2");
            let iter = opset.iter_range(&range);
            let ops = iter.collect::<Vec<_>>();
            assert_eq!(&test_ops[3..6], ops.as_slice());

            let clock = Clock::from_counters([None, Some(9), Some(9)]);
            let read = ReadAt::at(Cow::Owned(VisibleClock::new(clock.clone(), None)));
            let ops = opset
                .top_ops(&ObjId(OpId::new(1, 1)), read.borrow())
                .collect::<Vec<_>>();
            assert_eq!(&test_ops[2], &ops[0]);
            assert_eq!(&test_ops[5], &ops[1]);
            assert_eq!(&test_ops[7], &ops[2]);
            assert_eq!(3, ops.len());

            let iter = opset.iter_obj(&ObjId(OpId::new(1, 1)));
            let ops = iter
                .key_ops()
                .map(|n| n.collect::<Vec<_>>())
                .collect::<Vec<_>>();
            let key1 = ops.first().unwrap().as_slice();
            let key2 = ops.get(1).unwrap().as_slice();
            let key3 = ops.get(2).unwrap().as_slice();
            let key4 = ops.get(3);
            assert_eq!(&test_ops[2..3], key1);
            assert_eq!(&test_ops[3..6], key2);
            assert_eq!(&test_ops[6..8], key3);
            assert!(key4.is_none());

            let iter = opset.iter_obj(&ObjId(OpId::new(1, 1)));
            let ops = iter
                .visible_slow(None)
                .key_ops()
                .map(|n| n.collect::<Vec<_>>())
                .collect::<Vec<_>>();
            let key1 = ops.first().unwrap().as_slice();
            let key2 = ops.get(1).unwrap().as_slice();
            let key3 = ops.get(2).unwrap().as_slice();
            let key4 = ops.get(3);
            let key2test = vec![test_ops[3].clone(), test_ops[5].clone()];
            assert_eq!(&test_ops[2..3], key1);
            assert_eq!(&key2test, key2);
            assert_eq!(&test_ops[7..8], key3);
            assert!(key4.is_none());

            let ops = opset
                .top_ops(&ObjId(OpId::new(1, 1)), read)
                .collect::<Vec<_>>();
            assert_eq!(&test_ops[2], &ops[0]);
            assert_eq!(&test_ops[5], &ops[1]);
            assert_eq!(&test_ops[7], &ops[2]);
            assert_eq!(3, ops.len());
        });
    }
}
