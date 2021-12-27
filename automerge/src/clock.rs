use crate::types::OpId;
use fxhash::FxBuildHasher;
use std::cmp;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Clock(HashMap<usize, u64, FxBuildHasher>);

impl Clock {
    pub fn new() -> Self {
        Clock(Default::default())
    }

    pub fn include(&mut self, key: usize, n: u64) {
        self.0
            .entry(key)
            .and_modify(|m| *m = cmp::max(n, *m))
            .or_insert(n);
    }

    pub fn covers(&self, id: &OpId) -> bool {
        if let Some(val) = self.0.get(&id.actor()) {
            val >= &id.counter()
        } else {
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
}
