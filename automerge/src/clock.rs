use crate::types::OpId;
use fxhash::FxBuildHasher;
use std::collections::HashMap;

#[derive(Default, Debug, Clone, Copy, PartialEq)]
pub(crate) struct ClockData {
    /// Maximum operation counter of the actor at the point in time.
    pub(crate) max_op: u64,
    /// Sequence number of the change from this actor.
    pub(crate) seq: u64,
}

/// Vector clock mapping actor indices to the max op counter of the changes created by that actor.
#[derive(Default, Debug, Clone, PartialEq)]
pub(crate) struct Clock(HashMap<usize, ClockData, FxBuildHasher>);

impl Clock {
    pub(crate) fn new() -> Self {
        Clock(Default::default())
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

    pub(crate) fn covers(&self, id: &OpId) -> bool {
        if let Some(data) = self.0.get(&id.1) {
            data.max_op >= id.0
        } else {
            false
        }
    }

    /// Get the max_op counter recorded in this clock for the actor.
    pub(crate) fn get_for_actor(&self, actor_index: &usize) -> Option<&ClockData> {
        self.0.get(actor_index)
    }

    pub(crate) fn merge(&mut self, other: &Self) {
        for (actor, data) in &other.0 {
            self.include(*actor, *data);
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

        assert!(clock.covers(&OpId(10, 1)));
        assert!(clock.covers(&OpId(20, 1)));
        assert!(!clock.covers(&OpId(30, 1)));

        assert!(clock.covers(&OpId(5, 2)));
        assert!(clock.covers(&OpId(10, 2)));
        assert!(!clock.covers(&OpId(15, 2)));

        assert!(!clock.covers(&OpId(1, 3)));
        assert!(!clock.covers(&OpId(100, 3)));
    }
}
