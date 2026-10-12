use std::collections::BTreeMap;

use crate::change_queue::EntryId;

/// One actor's queued changes, keyed by sequence number.
#[derive(Debug, Clone, Default)]
pub(super) struct ActorQueue(BTreeMap<u64, EntryId>);

impl ActorQueue {
    /// The entry holding this actor's change at `seq`, if queued.
    pub(super) fn at(&self, seq: u64) -> Option<EntryId> {
        self.0.get(&seq).copied()
    }

    /// The entry holding this actor's first change, which is the only one
    /// allowed to carry an author.
    pub(super) fn first(&self) -> Option<EntryId> {
        self.at(1)
    }

    /// The entries holding this actor's changes at `seq` or later.
    pub(super) fn from_seq(&self, seq: u64) -> impl Iterator<Item = EntryId> + '_ {
        self.0.range(seq..).map(|(_, &id)| id)
    }

    pub(super) fn insert(&mut self, seq: u64, id: EntryId) {
        self.0.insert(seq, id);
    }

    pub(super) fn remove(&mut self, seq: u64) {
        self.0.remove(&seq);
    }

    pub(super) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}
