use crate::types::OpId;

use std::num::NonZeroU32;

#[derive(Default, Debug, Clone, PartialEq)]
pub(crate) struct Clock(pub(crate) Vec<u32>);

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
        self.0[actor_index] = u32::MAX
    }

    pub(crate) fn covers(&self, id: &OpId) -> bool {
        self.0[id.actor()] as u64 >= id.counter()
    }
}

impl std::iter::FromIterator<Option<u32>> for Clock {
    fn from_iter<I: IntoIterator<Item = Option<u32>>>(iter: I) -> Self {
        Clock(iter.into_iter().map(|i| i.unwrap_or(0)).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;

    impl Clock {
        pub(crate) fn new(size: usize) -> Self {
            Self(vec![0; size])
        }

        pub(crate) fn include(&mut self, actor_idx: usize, data: u32) -> bool {
            if data > self.0[actor_idx] {
                self.0[actor_idx] = data;
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
