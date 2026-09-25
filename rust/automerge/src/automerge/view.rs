//! Read contexts: the clocks and masks a read is performed under.
//!
//! Everything here is *produced* only by [`Automerge`](super::Automerge)
//! (see `read_current` / `read_at` / `read_visible` / `read_scoped`) or,
//! for [`ClockRange::diff`], from two already-legitimate [`VisibleClock`]s.
//! The rest of the crate only consumes these values, so a read can never
//! forget the write-frontier mask or observe uncommitted transaction ops.

use crate::clock::Clock;
use crate::types::OpId;

use std::borrow::Cow;

/// A [`Clock`] that is used for reads.
///
/// It consists of the causal clock, taken at some heads of the change graph,
/// and can optionally be intersected with a visibility [`Mask`].
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct VisibleClock(Clock);

impl VisibleClock {
    /// Construct a new [`VisibleClock`] using the `causal` clock and the optional `mask`.
    ///
    /// If the mask is supplied then the two clocks are intersected to get the
    /// final [`Clock`].
    pub(super) fn new(mut causal: Clock, mask: Option<&Mask>) -> Self {
        if let Some(mask) = mask {
            causal.intersect(&mask.0);
        }
        Self(causal)
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

    /// Insert a new actor at `index` into the clock.
    pub(crate) fn insert_actor(&mut self, index: usize) {
        self.0 .0.insert(index, 0)
    }

    /// Remove an existing actor at `index` from the clock.
    pub(crate) fn remove_actor(&mut self, index: usize) {
        self.0 .0.remove(index);
    }

    /// Create new, empty [`Clock`] which is the same size as this clock.
    pub(crate) fn empty_like(&self) -> Clock {
        Clock(vec![0; self.0 .0.len()])
    }

    /// Returns `true` if no operation is covered, i.e. all values are `0`.
    pub(crate) fn covers_nothing(&self) -> bool {
        self.0 .0.iter().all(|c| *c == 0)
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

    /// Returns `true` is the mask hides the given [`OpId`].
    ///
    /// This is the inverse of [`Clock::covers`].
    pub(crate) fn hides(&self, id: &OpId) -> bool {
        !self.0.covers(id)
    }

    /// Insert a new actor at `index` into the mask.
    pub(crate) fn insert_actor(&mut self, index: usize) {
        self.0 .0.insert(index, u32::MAX)
    }

    /// Remove an existing actor at `index` from the mask.
    pub(crate) fn remove_actor(&mut self, index: usize) {
        self.0 .0.remove(index);
    }
}

/// The snapshot of a clock for a given document, either current or historical.
///
/// A `Current` read is constructed only from `&Automerge` and borrows its
/// mask, so it cannot outlive a mutation; it means "indexes authoritative".
/// An `At` read never consults indexes, so its lifetime is unconstrained.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ReadAt<'a>(Inner<'a>);

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
    pub(crate) fn filter(&self) -> Option<&Clock> {
        match &self.0 {
            Inner::At(v) => Some(v.clock()),
            Inner::Current { mask } => mask.as_deref().map(Mask::clock),
        }
    }

    /// Wrap this read position into a single-endpoint [`ClockRange`].
    ///
    /// Stays `pub(crate)` for the same reason as [`ClockRange::diff`]:
    /// re-wrapping an already-legitimate read manufactures nothing.
    pub(crate) fn into_range(self) -> ClockRange<'a> {
        ClockRange(RangeInner::Current(self))
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
    Current(ReadAt<'a>),
    Diff(VisibleClock, VisibleClock),
}

/// A borrowed view of a [`ClockRange`] for consumers that walk both
/// endpoints. Holding one manufactures nothing: every clock it exposes was
/// produced by `Automerge`.
#[derive(Debug)]
pub(crate) enum RangeView<'r, 'a> {
    Current(&'r ReadAt<'a>),
    Diff {
        before: &'r VisibleClock,
        after: &'r VisibleClock,
    },
}

impl<'a> ClockRange<'a> {
    /// Stays `pub(crate)`: a diff over two already-legitimate
    /// [`VisibleClock`]s manufactures nothing, and
    /// `patch_log::transition_to` builds one.
    pub(crate) fn diff(before: VisibleClock, after: VisibleClock) -> Self {
        Self(RangeInner::Diff(before, after))
    }

    pub(super) fn current(read: ReadAt<'a>) -> Self {
        Self(RangeInner::Current(read))
    }

    /// Borrow the range's endpoints.
    pub(crate) fn view(&self) -> RangeView<'_, 'a> {
        match &self.0 {
            RangeInner::Current(read) => RangeView::Current(read),
            RangeInner::Diff(before, after) => RangeView::Diff { before, after },
        }
    }

    /// The read position at this range's `after` endpoint.
    pub(crate) fn read_after(&self) -> ReadAt<'_> {
        match &self.0 {
            RangeInner::Current(read) => read.borrow(),
            RangeInner::Diff(_, after) => ReadAt(Inner::At(Cow::Borrowed(after))),
        }
    }

    pub(crate) fn visible_after(&self, id: &OpId) -> bool {
        match &self.0 {
            RangeInner::Diff(_, after) => after.covers(id),
            RangeInner::Current(r) => r.filter().is_none_or(|c| c.covers(id)),
        }
    }

    pub(crate) fn visible_before(&self, id: &OpId) -> bool {
        self.predates(id)
    }

    pub(crate) fn predates(&self, id: &OpId) -> bool {
        match &self.0 {
            RangeInner::Diff(before, _) => before.covers(id),
            RangeInner::Current(_) => false,
        }
    }

    /// The range for children of the object created by `parent`. `None`
    /// when the children can keep the parent's range.
    ///
    /// When diffing, an object whose creation op is absent from `before`
    /// did not exist before, so its children must all emit as inserts:
    /// they are walked with an empty `before`. Without a mask the visible
    /// set is causally closed and this is the identity.
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
            RangeInner::Diff(before, after)
                if !before.covers_nothing() && !before.covers(parent) =>
            {
                Some(Self(RangeInner::Diff(
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
    fn visible_clock_actor_migration() {
        let mut v = VisibleClock::new(Clock(vec![5, 7]), None);
        v.insert_actor(1);
        assert_eq!(v.clock(), &Clock(vec![5, 0, 7]));
        v.remove_actor(0);
        assert_eq!(v.clock(), &Clock(vec![0, 7]));
        let mut m = Mask::new(Clock(vec![3]));
        m.insert_actor(0);
        assert_eq!(m.clock(), &Clock(vec![u32::MAX, 3]));
        assert!(m.hides(&OpId::new(4, 1)));
        assert!(!m.hides(&OpId::new(3, 1)));
    }

    #[test]
    fn descend_narrows_before_only_when_nonempty_before_lacks_parent() {
        let before = VisibleClock::new(Clock(vec![2, 5]), None);
        let after = VisibleClock::new(Clock(vec![9, 9]), None);
        let range = ClockRange::diff(before.clone(), after.clone());
        // parent covered by before: identity
        assert!(range.descend(&OpId::new(2, 0)).is_none());
        // parent not covered by before: the children's before empties out
        // (everything emits as an insert) while after stays untouched
        let narrowed = range.descend(&OpId::new(3, 0)).unwrap();
        assert!(!narrowed.predates(&OpId::new(1, 1)));
        assert!(narrowed.visible_after(&OpId::new(9, 1)));
        assert!(!narrowed.visible_after(&OpId::new(10, 1)));
        // an already-empty before cannot be narrowed further
        let empty = ClockRange::diff(VisibleClock::new(Clock(vec![0, 0]), None), after);
        assert!(empty.descend(&OpId::new(3, 0)).is_none());
        // Current never narrows. A parent absent from `after` needs no case
        // of its own: DiffIter::process_item's make_obj never descends into
        // it (its diff item is a delete), so descend never sees one.
        let current = ClockRange::current(ReadAt::current(None));
        assert!(current.descend(&OpId::new(3, 0)).is_none());
    }

    #[test]
    fn column_data_op_iterators() {
        let actors = vec![crate::ActorId::random(), crate::ActorId::random()];

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

            let clock = [None, Some(9), Some(9)].into_iter().collect::<Clock>();
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
