use crate::{
    clock::{Clock, ClockData},
    Change, ChangeHash,
};
use std::collections::HashMap;

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
        let mut clock = Clock::new();
        for hash in change.deps() {
            let c = self.0.get(hash).ok_or(MissingDep(*hash))?;
            clock.merge(c);
        }
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
}

impl From<Clocks> for HashMap<ChangeHash, Clock> {
    fn from(c: Clocks) -> Self {
        c.0
    }
}
