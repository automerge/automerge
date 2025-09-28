use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};
use std::num::NonZeroU64;

use crate::types::ActorId;
use crate::{Change, ChangeHash};

#[derive(Clone, Debug, PartialEq, Eq)]
struct ChangeOrder {
    start_op: NonZeroU64,
    hash: ChangeHash,
}

impl Ord for ChangeOrder {
    fn cmp(&self, other: &Self) -> Ordering {
        self.start_op
            .cmp(&other.start_op)
            .then_with(|| self.hash.cmp(&other.hash))
    }
}

impl PartialOrd for ChangeOrder {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Change {
    fn order(&self) -> ChangeOrder {
        ChangeOrder {
            start_op: self.start_op(),
            hash: self.hash(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ReadyQueue {
    changes: HashMap<ChangeHash, Change>,
    actor_seq: HashMap<ActorId, HashMap<u64, ChangeHash>>,
    order: BTreeSet<ChangeOrder>,
}

impl ReadyQueue {
    pub(crate) fn push(&mut self, change: Change) {
        self.actor_seq
            .entry(change.actor_id().clone())
            .or_default()
            .insert(change.seq(), change.hash());
        self.order.insert(change.order());
        self.changes.insert(change.hash(), change);
    }

    pub(crate) fn remove(&mut self, hash: &ChangeHash) -> Option<Change> {
        let change = self.changes.remove(hash)?;
        if let Some(sub) = self.actor_seq.get_mut(change.actor_id()) {
            sub.remove(&change.seq());
            if sub.is_empty() {
                self.actor_seq.remove(change.actor_id());
            }
        }
        self.order.remove(&change.order());
        Some(change)
    }

    pub(crate) fn len(&self) -> usize {
        self.changes.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        debug_assert_eq!(self.changes.is_empty(), self.actor_seq.is_empty());
        self.len() == 0
    }

    pub(crate) fn has_hash(&self, hash: &ChangeHash) -> bool {
        self.changes.contains_key(hash)
    }

    pub(crate) fn has_dupe(&self, change: &Change) -> bool {
        if let Some(hash) = self
            .actor_seq
            .get(change.actor_id())
            .and_then(|s| s.get(&change.seq()))
        {
            change.hash() != *hash
        } else {
            false
        }
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &Change> {
        self.order
            .iter()
            .filter_map(|ord| self.changes.get(&ord.hash))
    }
}

#[cfg(test)]
mod test {
    use crate::transaction::Transactable;
    use crate::types::{ActorId, ObjType};
    use crate::AutoCommit;
    use crate::AutomergeError::DuplicateSeqNumber;
    use crate::ROOT;

    #[test]
    fn test_duplicate_change() {
        let actor1 = ActorId::try_from("aaaaaa").unwrap();

        let mut doc1 = AutoCommit::new().with_actor(actor1.clone());
        let map1 = doc1.put_object(&ROOT, "map", ObjType::Map).unwrap();
        doc1.put(&map1, "key1", "val1").unwrap();
        doc1.put(&map1, "key2", "val2").unwrap();

        let mut doc2 = doc1.fork();

        doc1.put(&map1, "key3", "val3").unwrap();

        let mut doc1_b = doc1.fork().with_actor(actor1);

        doc1.put(&map1, "key4", "val4").unwrap();
        let good_change = doc1.get_last_local_change().unwrap();

        doc1_b.put(&map1, "key5", "val5").unwrap();
        let bad_change = doc1_b.get_last_local_change().unwrap();

        doc2.apply_changes(vec![good_change]).unwrap();
        let r = doc2.apply_changes(vec![bad_change]);
        assert!(matches!(r, Err(DuplicateSeqNumber(3, _))));
    }
}
