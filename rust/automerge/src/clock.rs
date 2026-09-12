use crate::types::OpId;

use std::num::NonZeroU32;
use std::ops::RangeInclusive;
use std::sync::Arc;

/// A [`Clock`] is a vector clock for a set of actors.
///
/// Each index of the vector represents that actors counter.
///
/// For example, given an [`OpId`], one can use [`OpId::actor`] to find the
/// currently stored counter of the actor in the [`Clock`].
#[derive(Default, Debug, Clone, PartialEq)]
pub(crate) struct Clock {
    counters: Vec<u32>,
    /// Experimental (prototype A): operations structurally contained in the
    /// clock but not participating in interpretation. `None` is allow-all.
    mask: Option<Arc<OpMask>>,
}

/// Actor-indexed set of op counter ranges that are excluded/pending under an
/// eligibility selection. Bound to the actor table it was compiled against;
/// never persisted.
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub(crate) struct OpMask {
    excluded: Vec<Vec<RangeInclusive<u64>>>,
}

impl OpMask {
    pub(crate) fn new(num_actors: usize) -> Self {
        Self {
            excluded: vec![Vec::new(); num_actors],
        }
    }

    pub(crate) fn exclude(&mut self, actor: usize, range: RangeInclusive<u64>) {
        self.excluded[actor].push(range);
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.excluded.iter().all(|v| v.is_empty())
    }

    pub(crate) fn excludes(&self, id: &OpId) -> bool {
        self.excluded
            .get(id.actor())
            .is_some_and(|ranges| ranges.iter().any(|r| r.contains(&id.counter())))
    }
}

#[derive(Default, Debug, Clone, PartialEq)]
pub(crate) struct SeqClock(pub(crate) Vec<Option<NonZeroU32>>);

impl SeqClock {
    pub(crate) fn iter(&self) -> impl Iterator<Item = (usize, Option<NonZeroU32>)> + '_ {
        self.0.iter().copied().enumerate()
    }

    pub(crate) fn remove_actor(&mut self, idx: usize) {
        self.0.remove(idx);
    }

    pub(crate) fn rewrite_with_new_actor(&mut self, idx: usize) {
        self.0.insert(idx, None)
    }

    pub(crate) fn get_for_actor(&self, actor_index: &usize) -> Option<NonZeroU32> {
        self.0.get(*actor_index).copied().flatten()
    }

    pub(crate) fn new(size: usize) -> Self {
        Self(vec![None; size])
    }

    pub(crate) fn include(&mut self, actor_idx: usize, data: Option<u32>) -> bool {
        if let Some(data) = data {
            match self.0[actor_idx] {
                None => {
                    self.0[actor_idx] = NonZeroU32::try_from(data).ok();
                    true
                }
                Some(old_data) if old_data.get() < data => {
                    self.0[actor_idx] = NonZeroU32::try_from(data).ok();
                    true
                }
                _ => false,
            }
        } else {
            false
        }
    }

    pub(crate) fn merge(a: &mut Self, b: &Self) {
        for (a, b) in std::iter::zip(a.0.iter_mut(), b.0.iter()) {
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

    /// Structural containment in the `before` clock. Used for exposure
    /// decisions: an object that already structurally existed before must not
    /// be re-exposed wholesale even if it only now participates (its children
    /// emit their own deltas). Distinct from [`Self::visible_before`].
    pub(crate) fn predates(&self, id: &OpId) -> bool {
        match self {
            Self::Diff(before, _) => before.contains(id),
            _ => false,
        }
    }
}

impl Clock {
    pub(crate) fn isolate(&mut self, actor_index: usize) {
        self.set_counter_of(actor_index, u32::MAX);
    }

    /// Attach an eligibility mask. Masked operations are structurally
    /// `contains`-ed but do not `covers`/participate.
    pub(crate) fn with_mask(mut self, mask: Option<Arc<OpMask>>) -> Self {
        self.mask = mask.filter(|m| !m.is_empty());
        self
    }

    /// Structural containment: the operation is within the history described
    /// by this clock, regardless of eligibility.
    pub(crate) fn contains(&self, id: &OpId) -> bool {
        self.counter_of(id.actor()) as u64 >= id.counter()
    }

    /// An [`OpId`] is covered by a [`Clock`] if the operation happened within
    /// the timeframe of this [`Clock`] and participates under the clock's
    /// eligibility mask (allow-all when no mask is set).
    ///
    /// The [`OpId::actor`] is looked up in the vector clock, and checks if it
    /// is greater than or equal to the [`OpId::counter`].
    ///
    /// # Panics
    ///
    /// If the [`OpId::actor`] is an index that is greater than the length of
    /// the vector, i.e. the actor does not exist in the [`Clock`].
    pub(crate) fn covers(&self, id: &OpId) -> bool {
        self.contains(id) && !self.mask.as_ref().is_some_and(|m| m.excludes(id))
    }

    /// Get the `u32` counter value for the given `actor`.
    ///
    /// # Panics
    ///
    /// If the `actor` index is out of bounds of the internal vector.
    #[inline]
    fn counter_of(&self, actor: usize) -> u32 {
        self.counters[actor]
    }

    /// Set the `actor`'s counter to the given `counter` value.
    ///
    /// # Panics
    ///
    /// If the `actor` index is out of bounds of the internal vector.
    #[inline]
    fn set_counter_of(&mut self, actor: usize, counter: u32) {
        self.counters[actor] = counter;
    }
}

impl std::iter::FromIterator<Option<u32>> for Clock {
    fn from_iter<I: IntoIterator<Item = Option<u32>>>(iter: I) -> Self {
        Clock {
            counters: iter.into_iter().map(|i| i.unwrap_or(0)).collect(),
            mask: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;

    impl Clock {
        pub(crate) fn new(size: usize) -> Self {
            Self {
                counters: vec![0; size],
                mask: None,
            }
        }

        pub(crate) fn include(&mut self, actor_idx: usize, data: u32) -> bool {
            if data > self.counters[actor_idx] {
                self.counters[actor_idx] = data;
                true
            } else {
                false
            }
        }

        fn is_greater(&self, other: &Self) -> bool {
            !std::iter::zip(self.counters.iter(), other.counters.iter()).any(|(a, b)| a < b)
        }
    }

    impl PartialOrd for Clock {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            if self.counters == other.counters {
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
