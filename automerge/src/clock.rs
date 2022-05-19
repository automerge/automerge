use crate::types::OpId;
use fxhash::FxBuildHasher;
use std::cmp;
use std::collections::HashMap;

/// Vector clock mapping actor indices to the max op counter of the changes created by that actor.
#[derive(Default, Debug, Clone, PartialEq)]
pub(crate) struct Clock(HashMap<usize, u64, FxBuildHasher>);

impl Clock {
    pub(crate) fn new() -> Self {
        Clock(Default::default())
    }

    pub(crate) fn include(&mut self, actor_index: usize, max_op: u64) {
        self.0
            .entry(actor_index)
            .and_modify(|m| *m = cmp::max(max_op, *m))
            .or_insert(max_op);
    }

    pub(crate) fn covers(&self, id: &OpId) -> bool {
        if let Some(max_op) = self.0.get(&id.1) {
            max_op >= &id.0
        } else {
            false
        }
    }

    /// Get the max_op counter recorded in this clock for the actor.
    pub(crate) fn get_for_actor(&self, actor_index: &usize) -> Option<&u64> {
        self.0.get(actor_index)
    }

    pub(crate) fn merge(&mut self, other: &Self) {
        for (actor, max_op) in &other.0 {
            self.include(*actor, *max_op);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn covers() {
        let mut clock = Clock::new();

        clock.include(1, 20);
        clock.include(2, 10);

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
