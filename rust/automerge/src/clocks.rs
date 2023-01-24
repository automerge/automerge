use crate::{
    clock::{Clock, ClockData},
    Change, ChangeHash,
};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub(crate) struct Clocks(HashMap<ChangeHash, Clock>);

#[derive(Debug, thiserror::Error)]
#[error("attempted to derive a clock for a change with dependencies we don't have")]
pub struct MissingDep(ChangeHash);

impl Clocks {
    pub(crate) fn new() -> Self {
        Self(HashMap::new())
    }

    pub(crate) fn add_change(
        &mut self,
        change: &Change,
        actor_index: usize,
    ) -> Result<(), MissingDep> {
        let mut clock = self.at(change.deps())?;
        clock.include(
            actor_index,
            ClockData {
                max_op: change.max_op(),
                seq: change.seq(),
            },
        );
        self.0.insert(change.hash(), clock);
        Ok(())
    }

    pub(crate) fn get(&self, hash: &ChangeHash) -> Option<&Clock> {
        self.0.get(hash)
    }

    pub(crate) fn insert(&mut self, hash: ChangeHash, clock: Clock) -> Option<Clock> {
        self.0.insert(hash, clock)
    }

    pub(crate) fn at(&self, heads: &[ChangeHash]) -> Result<Clock, MissingDep> {
        if let Some(first_hash) = heads.first() {
            let mut clock = self
                .0
                .get(first_hash)
                .ok_or(MissingDep(*first_hash))?
                .clone();

            for hash in &heads[1..] {
                let c = self.0.get(hash).ok_or(MissingDep(*hash))?;
                clock.merge(c);
            }

            Ok(clock)
        } else {
            Ok(Clock::new())
        }
    }
}

impl From<Clocks> for HashMap<ChangeHash, Clock> {
    fn from(c: Clocks) -> Self {
        c.0
    }
}
