use crate::types::OpId;
use fxhash::FxBuildHasher;
use std::cmp::Ordering;

#[derive(Default, Debug, Clone, Copy, PartialEq)]
pub(crate) struct ClockData {
    /// Maximum operation counter of the actor at the point in time.
    pub(crate) max_op: u64,
    /// Sequence number of the change from this actor.
    pub(crate) seq: u64,
}

// a clock for the same actor is ahead of another if it has a higher max_op
impl PartialOrd for ClockData {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.max_op.partial_cmp(&other.max_op)
    }
}

/// Vector clock mapping actor indices to the max op counter of the changes created by that actor.
#[derive(Default, Debug, Clone, PartialEq)]
pub(crate) struct Clock(im::HashMap<usize, ClockData, FxBuildHasher>);

// A general clock is greater if it has one element the other does not or has a counter higher than
// the other for a given actor.
//
// It is equal with another clock if it has the same entries everywhere.
//
// It is less than another clock otherwise.
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
    pub(crate) fn new() -> Self {
        Clock(Default::default())
    }

    pub(crate) fn merge(a: &Clock, b: &Clock) -> Clock {
        if a.0.len() > b.0.len() {
            Self::merge(b, a)
        } else {
            let mut union = a.clone();
            for (key, b_value) in b.0.iter() {
                union
                    .0
                    .entry(*key)
                    .and_modify(|d| {
                        if b_value.max_op > d.max_op {
                            *d = *b_value;
                        }
                    })
                    .or_insert(*b_value);
            }
            union
        }
    }

    pub(crate) fn include(&mut self, actor_index: usize, data: ClockData) {
        self.0
            .entry(actor_index)
            .and_modify(|d| {
                if data.max_op > d.max_op {
                    *d = data;
                }
            })
            .or_insert(data);
    }

    pub(crate) fn isolate(&mut self, actor_index: usize) {
        self.include(
            actor_index,
            ClockData {
                max_op: u64::MAX,
                seq: u64::MAX,
            },
        )
    }

    pub(crate) fn covers(&self, id: &OpId) -> bool {
        if let Some(data) = self.0.get(&id.actor()) {
            data.max_op >= id.counter()
        } else {
            false
        }
    }

    /// Get the max_op counter recorded in this clock for the actor.
    pub(crate) fn get_for_actor(&self, actor_index: &usize) -> Option<&ClockData> {
        self.0.get(actor_index)
    }

    fn is_greater(&self, other: &Self) -> bool {
        let mut has_greater = false;

        let mut others_found = 0;

        for (actor, data) in &self.0 {
            if let Some(other_data) = other.0.get(actor) {
                if data < other_data {
                    // may be concurrent or less
                    return false;
                } else if data > other_data {
                    has_greater = true;
                }
                others_found += 1;
            } else {
                // other doesn't have this so effectively has a greater element
                has_greater = true;
            }
        }

        if has_greater {
            // if they are equal then we have seen every key in the other clock and have at least
            // one greater element so our clock is greater
            //
            // If they aren't the same then we haven't seen every key but have a greater element
            // anyway so are concurrent
            others_found == other.0.len()
        } else {
            // our clock doesn't have anything greater than the other clock so can't be greater but
            // could still be concurrent
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn covers() {
        let mut clock = Clock::new();

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
        let mut base_clock = Clock::new();
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
