use crate::actor::{ActorIndexed, ActorRemoval, ActorShift, ActorTable};
use crate::types::OpId;

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

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ClockRange {
    Current(Option<Clock>),
    Diff(Clock, Clock),
}

impl Default for ClockRange {
    fn default() -> Self {
        Self::Current(None)
    }
}

impl ClockRange {
    pub(crate) fn current(clock: Option<Clock>) -> Self {
        Self::Current(clock)
    }

    pub(crate) fn after(&self) -> Option<&Clock> {
        match self {
            Self::Diff(_, after) => Some(after),
            Self::Current(Some(after)) => Some(after),
            _ => None,
        }
    }

    pub(crate) fn visible_after(&self, id: &OpId) -> bool {
        match self {
            Self::Current(Some(after)) => after.covers(id),
            Self::Diff(_, after) => after.covers(id),
            _ => true,
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
}

impl Clock {
    pub(crate) fn isolate(&mut self, actor_index: usize) {
        self.set_counter_of(actor_index, u32::MAX);
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
}
