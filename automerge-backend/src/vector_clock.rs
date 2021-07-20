use core::cmp::max;
use std::{collections::HashMap, ops::AddAssign};

use automerge_protocol as amp;

#[derive(Debug, Default, Clone)]
pub struct VectorClock {
    clock: HashMap<amp::ActorId, u64>,
}

impl VectorClock {
    pub fn update(&mut self, actor: &amp::ActorId, seq: u64) {
        if let Some(s) = self.clock.get_mut(actor) {
            *s = max(*s, seq);
        } else {
            self.clock.insert(actor.clone(), seq);
        }
    }

    pub fn len(&self) -> usize {
        self.clock.len()
    }

    pub fn get_seq(&self, actor: &amp::ActorId) -> Option<u64> {
        self.clock.get(actor).copied()
    }
}

impl AddAssign for VectorClock {
    fn add_assign(&mut self, rhs: Self) {
        for (a, s) in rhs.clock {
            if let Some(seq) = self.clock.get_mut(&a) {
                *seq = max(*seq, s);
            } else {
                self.clock.insert(a, s);
            }
        }
    }
}

impl AddAssign<&Self> for VectorClock {
    fn add_assign(&mut self, rhs: &Self) {
        for (a, s) in &rhs.clock {
            if let Some(seq) = self.clock.get_mut(a) {
                *seq = max(*seq, *s);
            } else {
                self.clock.insert(a.clone(), *s);
            }
        }
    }
}
