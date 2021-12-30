use crate::{ActorId, OpId};
use std::cmp;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Clock(HashMap<ActorId, u64>);

impl Clock {
    pub fn new() -> Self {
        Clock(HashMap::new())
    }

    pub fn include(&mut self, key: &ActorId, n: u64) {
        self.0
            .entry(key.clone())
            .and_modify(|m| *m = cmp::max(n, *m))
            .or_insert(n);
    }

    pub fn covers(&self, id: &OpId) -> bool {
        if let Some(val) = self.0.get(&id.actor) {
            val >= &id.counter
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
        let a1 = ActorId::random();
        let a2 = ActorId::random();
        let a3 = ActorId::random();

        clock.include(&a1, 20);
        clock.include(&a2, 10);

        assert!(clock.covers(&OpId::at(10, &a1)));
        assert!(clock.covers(&OpId::at(20, &a1)));
        assert!(!clock.covers(&OpId::at(30, &a1)));

        assert!(clock.covers(&OpId::at(5, &a2)));
        assert!(clock.covers(&OpId::at(10, &a2)));
        assert!(!clock.covers(&OpId::at(15, &a2)));

        assert!(!clock.covers(&OpId::at(1, &a3)));
        assert!(!clock.covers(&OpId::at(100, &a3)));
    }
}
