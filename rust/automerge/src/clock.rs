use crate::actor::{ActorIndexed, ActorRemoval, ActorShift, ActorTable};
use crate::types::OpId;

use std::borrow::Cow;
use std::num::NonZeroU32;

/// A [`Clock`] is a vector clock for a set of actors.
///
/// Each slot holds the greatest op counter seen for that actor.
///
/// For example, given an [`OpId`], one can use [`OpId::actor`] to find the
/// currently stored counter of the actor in the [`Clock`].
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Clock(ActorIndexed<u32>);

/// A vector clock over change *sequence numbers*: each slot holds the
/// greatest `seq` seen for that actor, or `None` if none has.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SeqClock(ActorIndexed<Option<NonZeroU32>>);

impl SeqClock {
    pub(crate) fn remove_actor(&mut self, removal: &ActorRemoval) {
        self.0.remove(removal);
    }

    pub(crate) fn shift_actor(&mut self, shift: &ActorShift) {
        self.0.insert(shift, None)
    }

    pub(crate) fn get_for_actor(&self, actor_index: &usize) -> Option<NonZeroU32> {
        self.0.get(*actor_index).copied().flatten()
    }

    /// An empty clock with a slot for every actor in `actors`.
    pub(crate) fn new(actors: &ActorTable) -> Self {
        Self(ActorIndexed::new(actors))
    }

    /// An empty clock covering the same actors as `self`.
    pub(crate) fn empty_like(&self) -> Self {
        Self(ActorIndexed::new_like(&self.0))
    }

    /// Derive a [`Clock`] over the same actors, mapping each actor's `seq`
    /// to an op counter with `f`.
    pub(crate) fn map_to_clock(
        &self,
        mut f: impl FnMut(usize, NonZeroU32) -> Option<u32>,
    ) -> Clock {
        Clock(ActorIndexed::from_slots_of(&self.0, |actor, seq| {
            seq.and_then(|seq| f(actor, seq)).unwrap_or(0)
        }))
    }

    pub(crate) fn include(&mut self, actor_idx: usize, data: Option<u32>) -> bool {
        if let Some(data) = data {
            let slots = self.0.as_mut_slice();
            match slots[actor_idx] {
                None => {
                    slots[actor_idx] = NonZeroU32::try_from(data).ok();
                    true
                }
                Some(old_data) if old_data.get() < data => {
                    slots[actor_idx] = NonZeroU32::try_from(data).ok();
                    true
                }
                _ => false,
            }
        } else {
            false
        }
    }

    pub(crate) fn merge(a: &mut Self, b: &Self) {
        for (a, b) in std::iter::zip(a.0.as_mut_slice().iter_mut(), b.0.iter()) {
            if *a < *b {
                *a = *b;
            }
        }
    }

    pub(crate) fn covers(&self, other: &SeqClock) -> bool {
        assert_eq!(self.0.len(), other.0.len());
        std::iter::zip(self.0.iter(), other.0.iter()).all(|(a, b)| match (a, b) {
            (_, None) => true,
            (Some(s1), Some(s2)) => s1 >= s2,
            _ => false,
        })
    }
}

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
    pub(crate) fn new(mut causal: Clock, mask: Option<&Mask>) -> Self {
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

    /// Add the clock slot for an actor just inserted into the actor table.
    pub(crate) fn insert_actor(&mut self, shift: &ActorShift) {
        self.0 .0.insert(shift, 0)
    }

    /// Drop the clock slot for an actor just removed from the actor table.
    pub(crate) fn remove_actor(&mut self, removal: &ActorRemoval) {
        self.0 .0.remove(removal);
    }

    /// Number of actors this clock covers.
    pub(crate) fn len(&self) -> usize {
        self.0 .0.len()
    }

    /// Create new, empty [`Clock`] which is the same size as this clock.
    fn empty_like(&self) -> Clock {
        Clock(ActorIndexed::new_like(&self.0 .0))
    }

    /// Returns `true` if some operation is potentially covered, i.e. all values are `> 0`.
    fn covers_something(&self) -> bool {
        self.0 .0.iter().any(|c| *c > 0)
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
    pub(crate) fn new(clock: Clock) -> Self {
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
    pub(crate) fn insert_actor(&mut self, actor: &ActorShift) {
        self.0 .0.insert(actor, u32::MAX)
    }

    /// Remove an existing actor at `index` from the mask.
    pub(crate) fn remove_actor(&mut self, actor: &ActorRemoval) {
        self.0 .0.remove(actor);
    }
}

/// The snapshot of a clock for a given document, either current or historical.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ReadAt<'a> {
    /// The snapshot is given for the current heads, and thus no causal clock is
    /// required. However, a [`Mask`] may be provided to hide operations.
    Current { mask: Option<Cow<'a, Mask>> },
    /// The snapshot is historical for some given heads, and it may be combined
    /// with a [`Mask`] to also hide operations (see [`VisibleClock::new`]).
    At(Cow<'a, VisibleClock>),
}

impl<'a> ReadAt<'a> {
    /// An unmasked current read. Only valid where no document is
    /// available (empty iterators, unbound patch logs); every read that
    /// has an `&Automerge` must go through `Automerge::read_at` so the
    /// write-frontier mask cannot be forgotten.
    pub(crate) fn unmasked() -> Self {
        Self::Current { mask: None }
    }

    /// Construct a historical snapshot.
    pub(crate) fn at(v: VisibleClock) -> Self {
        Self::At(Cow::Owned(v))
    }

    /// Return a historical clock, if this snapshot is configured to do so.
    ///
    /// `None` implies that indexes are authoritative, and is used for the fast path.
    pub(crate) fn historical(&self) -> Option<&Clock> {
        match self {
            Self::At(v) => Some(v.clock()),
            Self::Current { .. } => None,
        }
    }

    /// Return the [`Clock`] for op comparison on slow paths.
    ///
    /// `None` implies that there is no filtering required.
    pub(crate) fn filter(&self) -> Option<&Clock> {
        match self {
            Self::At(v) => Some(v.clock()),
            Self::Current { mask } => mask.as_deref().map(Mask::clock),
        }
    }

    /// Return a borrowed reference of this [`ReadAt`].
    pub(crate) fn borrow(&self) -> ReadAt<'_> {
        match self {
            Self::Current { mask } => ReadAt::Current {
                mask: mask.as_deref().map(Cow::Borrowed),
            },
            Self::At(v) => ReadAt::At(Cow::Borrowed(v.as_ref())),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ClockRange<'a> {
    Current(ReadAt<'a>),
    Diff(VisibleClock, VisibleClock),
}

/// The default is an unmasked current read; only valid for empty/doc-less
/// iterator states (see [`ReadAt::unmasked`]). Any range built with a
/// document in hand must go through `Automerge::read_at`.
impl Default for ClockRange<'_> {
    fn default() -> Self {
        Self::Current(ReadAt::unmasked())
    }
}

impl<'a> ClockRange<'a> {
    pub(crate) fn current(read: ReadAt<'a>) -> Self {
        Self::Current(read)
    }

    pub(crate) fn visible_after(&self, id: &OpId) -> bool {
        match self {
            Self::Diff(_, after) => after.covers(id),
            Self::Current(r) => r.filter().is_none_or(|c| c.covers(id)),
        }
    }

    pub(crate) fn visible_before(&self, id: &OpId) -> bool {
        self.predates(id)
    }

    pub(crate) fn predates(&self, id: &OpId) -> bool {
        match self {
            Self::Diff(before, _) => before.covers(id),
            _ => false,
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
        match self {
            // An already-empty `before` cannot be narrowed further, so keep
            // the cheap shifting path for diffs from the empty document.
            Self::Diff(before, after) if before.covers_something() && !before.covers(parent) => {
                Some(Self::Diff(
                    VisibleClock::new(before.empty_like(), None),
                    after.clone(),
                ))
            }
            _ => None,
        }
    }
}

impl Clock {
    /// Construct a clock with one counter per actor in `actors`.
    pub(crate) fn from_actor_fn(actors: &ActorTable, counter: impl FnMut(usize) -> u32) -> Self {
        Self(ActorIndexed::build_from(actors, counter))
    }

    pub(crate) fn isolate(&mut self, actor_index: usize) {
        self.set_counter_of(actor_index, u32::MAX);
    }

    /// Get the minimum bound intersection of two [`Clock`]s.
    ///
    /// # Panics
    ///
    /// If the size of both [`Clock`]s are not equal.
    pub(crate) fn intersect(&mut self, other: &Self) {
        for (counter, bound) in self.0.zip_mut(&other.0) {
            *counter = (*counter).min(*bound);
        }
    }

    /// An [`OpId`] is covered by a [`Clock`] if the operation happened within
    /// the timeframe of this [`Clock`].
    ///
    /// The [`OpId::actor`] is looked up in the vector clock, and checks if it
    /// is greater than or equal to the [`OpId::counter`].
    ///
    /// # Panics
    ///
    /// If the [`OpId::actor`] is an index that is greater than the length of
    /// the vector, i.e. the actor does not exist in the [`Clock`].
    pub(crate) fn covers(&self, id: &OpId) -> bool {
        self.counter_of(id.actor()) as u64 >= id.counter()
    }

    /// Get the `u32` counter value for the given `actor`.
    ///
    /// # Panics
    ///
    /// If the `actor` index is out of bounds of the internal vector.
    #[inline]
    fn counter_of(&self, actor: usize) -> u32 {
        self.0[actor]
    }

    /// Set the `actor`'s counter to the given `counter` value.
    ///
    /// # Panics
    ///
    /// If the `actor` index is out of bounds of the internal vector.
    #[inline]
    fn set_counter_of(&mut self, actor: usize, counter: u32) {
        self.0.as_mut_slice()[actor] = counter;
    }

    /// A clock from explicit per-actor counters, for tests which model the
    /// actor table implicitly.
    #[cfg(test)]
    pub(crate) fn from_counters(counters: impl IntoIterator<Item = Option<u32>>) -> Self {
        Clock(ActorIndexed::from_slots_for_test(
            counters.into_iter().map(|c| c.unwrap_or(0)).collect(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;

    impl Clock {
        pub(crate) fn new(size: usize) -> Self {
            Self::from_counters(std::iter::repeat_n(None, size))
        }

        pub(crate) fn include(&mut self, actor_idx: usize, data: u32) -> bool {
            if data > self.0[actor_idx] {
                self.0.as_mut_slice()[actor_idx] = data;
                true
            } else {
                false
            }
        }

        fn is_greater(&self, other: &Self) -> bool {
            !std::iter::zip(self.0.iter(), other.0.iter()).any(|(a, b)| a < b)
        }
    }

    impl PartialOrd for Clock {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            if self.0 == other.0 {
                Some(Ordering::Equal)
            } else if self.is_greater(other) {
                Some(Ordering::Greater)
            } else if other.is_greater(self) {
                Some(Ordering::Less)
            } else {
                None
            }
        }
    }

    #[test]
    fn covers() {
        let mut clock = Clock::new(4);

        clock.include(1, 20);
        clock.include(2, 10);

        assert!(clock.covers(&OpId::new(10, 1)));
        assert!(clock.covers(&OpId::new(20, 1)));
        assert!(!clock.covers(&OpId::new(30, 1)));

        assert!(clock.covers(&OpId::new(5, 2)));
        assert!(clock.covers(&OpId::new(10, 2)));
        assert!(!clock.covers(&OpId::new(15, 2)));

        assert!(!clock.covers(&OpId::new(1, 3)));
        assert!(!clock.covers(&OpId::new(100, 3)));
    }

    #[test]
    fn comparison() {
        let mut base_clock = Clock::new(4);
        base_clock.include(1, 1);
        base_clock.include(2, 1);

        let mut after_clock = base_clock.clone();
        after_clock.include(1, 2);

        assert!(after_clock > base_clock);
        assert!(base_clock < after_clock);

        assert!(base_clock == base_clock);

        let mut new_actor_clock = base_clock.clone();
        new_actor_clock.include(3, 1);

        assert_eq!(
            base_clock.partial_cmp(&new_actor_clock),
            Some(Ordering::Less)
        );
        assert_eq!(
            new_actor_clock.partial_cmp(&base_clock),
            Some(Ordering::Greater)
        );

        assert_eq!(after_clock.partial_cmp(&new_actor_clock), None);
        assert_eq!(new_actor_clock.partial_cmp(&after_clock), None);
    }

    #[test]
    fn visible_clock_actor_migration() {
        let mut v = VisibleClock::new(Clock::from_counters([Some(5), Some(7)]), None);
        v.insert_actor(&ActorShift::for_test(1));
        assert_eq!(
            v.clock(),
            &Clock::from_counters([Some(5), Some(0), Some(7)])
        );
        v.remove_actor(&ActorRemoval::for_test(0));
        assert_eq!(v.clock(), &Clock::from_counters([Some(0), Some(7)]));
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
        let range = ClockRange::Diff(before.clone(), after.clone());
        // parent covered by before: identity
        assert!(range.descend(&OpId::new(2, 0)).is_none());
        // parent not covered by before: the children's before empties out
        // (everything emits as an insert) while after stays untouched
        let narrowed = range.descend(&OpId::new(3, 0)).unwrap();
        assert!(!narrowed.predates(&OpId::new(1, 1)));
        assert!(narrowed.visible_after(&OpId::new(9, 1)));
        assert!(!narrowed.visible_after(&OpId::new(10, 1)));
        // an already-empty before cannot be narrowed further
        let empty = ClockRange::Diff(
            VisibleClock::new(Clock::from_counters([Some(0), Some(0)]), None),
            after,
        );
        assert!(empty.descend(&OpId::new(3, 0)).is_none());
        // Current never narrows. A parent absent from `after` needs no case
        // of its own: DiffIter::process_item's make_obj never descends into
        // it (its diff item is a delete), so descend never sees one.
        assert!(ClockRange::default().descend(&OpId::new(3, 0)).is_none());
    }
}
