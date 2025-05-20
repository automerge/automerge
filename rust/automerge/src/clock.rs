use crate::types::OpId;

#[cfg(test)]
use std::cmp::Ordering;

#[derive(Default, Debug, Clone, Copy, PartialEq)]
pub(crate) struct ClockData {
    /// Maximum operation counter of the actor at the point in time.
    pub(crate) max_op: u32,
    /// Sequence number of the change from this actor.
    pub(crate) seq: u32,
}

impl PartialOrd for ClockData {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for ClockData {}

impl Ord for ClockData {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.max_op.cmp(&other.max_op)
    }
}

impl ClockData {
    pub(crate) fn new(max_op: u32, seq: u32) -> Self {
        ClockData { max_op, seq }
    }

    fn max() -> Self {
        ClockData {
            max_op: u32::MAX,
            seq: u32::MAX,
        }
    }
}

#[derive(Default, Debug, Clone, PartialEq)]
pub(crate) struct Clock(pub(crate) Vec<ClockData>);

#[cfg(test)]
impl PartialOrd for Clock {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.0 == other.0 {
            Some(Ordering::Equal)
        } else if self.is_greater(other) {
            Some(Ordering::Greater)
        } else if other.is_greater(self) {
            Some(Ordering::Less)
        } else {
            // concurrent
            None
        }
    }
}

impl Clock {
    pub(crate) fn new(size: usize) -> Self {
        //Self(vec![size; ClockData::new()])
        Self(vec![ClockData::new(0, 0); size])
    }

    pub(crate) fn remove_actor(&mut self, idx: usize) {
        self.0.remove(idx);
    }

    pub(crate) fn rewrite_with_new_actor(&mut self, idx: usize) {
        self.0.insert(idx, ClockData::new(0, 0))
    }

    pub(crate) fn merge(a: &mut Self, b: &Self) {
        for (a, b) in std::iter::zip(a.0.iter_mut(), b.0.iter()) {
            if a.max_op < b.max_op {
                *a = *b;
            }
        }
    }

    pub(crate) fn include(&mut self, actor_idx: usize, data: ClockData) -> bool {
        if data.max_op > self.0[actor_idx].max_op {
            self.0[actor_idx] = data;
            true
        } else {
            false
        }
    }

    pub(crate) fn isolate(&mut self, actor_index: usize) {
        self.0[actor_index] = ClockData::max()
    }

    pub(crate) fn covers(&self, id: &OpId) -> bool {
        self.0[id.actor()].max_op as u64 >= id.counter()
    }

    pub(crate) fn get_for_actor(&self, actor_index: &usize) -> Option<&ClockData> {
        self.0.get(*actor_index)
    }

    #[cfg(test)]
    fn is_greater(&self, other: &Self) -> bool {
        !std::iter::zip(self.0.iter(), other.0.iter()).any(|(a, b)| a.max_op < b.max_op)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn covers() {
        let mut clock = Clock::new(4);

        clock.include(1, ClockData { max_op: 20, seq: 1 });
        clock.include(2, ClockData { max_op: 10, seq: 2 });

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
        base_clock.include(1, ClockData { max_op: 1, seq: 1 });
        base_clock.include(2, ClockData { max_op: 1, seq: 1 });

        let mut after_clock = base_clock.clone();
        after_clock.include(1, ClockData { max_op: 2, seq: 2 });

        assert!(after_clock > base_clock);
        assert!(base_clock < after_clock);

        assert!(base_clock == base_clock);

        let mut new_actor_clock = base_clock.clone();
        new_actor_clock.include(3, ClockData { max_op: 1, seq: 1 });

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
