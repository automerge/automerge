use std::{
    collections::{HashMap, HashSet},
    num::NonZeroU32,
};

use crate::{author::Authors, clock::SeqClock, op_set2::ActorIdx, Author, ChangeHash};

#[derive(Debug, Default, Clone)]
pub(crate) struct Revocations {
    /// Revocations are a map from an [`Author`] to a [`ChangeHash`] heads
    /// boundary. These [`ChangeHash`]es, and any descendants will be made
    /// invisible to the materialized document.
    revocations: HashMap<Author<'static>, Vec<ChangeHash>>,
    /// Mapping from [`ActorIdx`] to maximum sequence number allowed by the
    /// revocations.
    ///
    /// See [`ChangeGraph::revocations`].
    revocations_mask: HashMap<ActorIdx, Option<NonZeroU32>>,
    pending_revoke: HashSet<ChangeHash>,
}

impl Revocations {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Returns `true` if no [`Author`]s have been revoked.
    pub(crate) fn is_empty(&self) -> bool {
        self.revocations.is_empty()
    }

    pub(crate) fn get_revocations_for_author(
        &self,
        author: &Author<'static>,
    ) -> Option<&Vec<ChangeHash>> {
        self.revocations.get(author)
    }

    pub(crate) fn get_revocations(&self) -> &HashMap<Author<'static>, Vec<ChangeHash>> {
        &self.revocations
    }

    pub(crate) fn revoke(
        &mut self,
        author: Author<'static>,
        heads: Vec<ChangeHash>,
        clock: &SeqClock,
        authors: &Authors,
    ) {
        for actor in authors.get_actors_for_author(&author) {
            self.revocations_mask
                .insert(actor.into(), clock.get_for_actor(&actor));
        }
        self.revocations.insert(author, heads);
    }

    pub(crate) fn unrevoke(&mut self, author: &Author<'static>, authors: &Authors) {
        for a in authors.get_actors_for_author(author) {
            self.revocations_mask.remove(&a.into());
        }
        self.revocations.remove(author);
    }

    pub(crate) fn is_revoked(&self, actor: ActorIdx, seq: u64) -> bool {
        match self.revocations_mask.get(&actor) {
            Some(Some(v)) if (v.get() as u64) < seq => true,
            Some(None) => true,
            _ => false,
        }
    }

    pub(crate) fn insert_mask_for(&mut self, actor: ActorIdx, mask: Option<NonZeroU32>) {
        self.revocations_mask.insert(actor, mask);
    }

    pub(crate) fn get_mask_for(&self, actor: &ActorIdx) -> Option<&Option<NonZeroU32>> {
        self.revocations_mask.get(actor)
    }

    pub(crate) fn get_revocation_mask(&self) -> &HashMap<ActorIdx, Option<NonZeroU32>> {
        &self.revocations_mask
    }

    pub(crate) fn insert_actor(&mut self, actor: usize) {
        self.revocations_mask = std::mem::take(&mut self.revocations_mask)
            .into_iter()
            .map(|(a, v)| {
                let shifted = if a.0 >= actor as u32 {
                    ActorIdx(a.0 + 1)
                } else {
                    a
                };
                (shifted, v)
            })
            .collect();
    }

    pub(crate) fn remove_actor(&mut self, actor: usize) {
        self.revocations_mask = std::mem::take(&mut self.revocations_mask)
            .into_iter()
            .filter_map(|(a, v)| match a.0.cmp(&(actor as u32)) {
                std::cmp::Ordering::Less => Some((a, v)),
                std::cmp::Ordering::Equal => None,
                std::cmp::Ordering::Greater => Some((ActorIdx(a.0 - 1), v)),
            })
            .collect();
    }

    pub(crate) fn clear(&mut self) {
        self.pending_revoke.clear();
        self.revocations_mask.clear();
        self.revocations.clear();
    }

    pub(crate) fn recompute_revocations(
        &mut self,
        authors: &Authors,
        get_clock: impl Fn(&Vec<ChangeHash>) -> SeqClock,
    ) {
        for (author, heads) in &self.revocations {
            let clock = get_clock(heads);
            for actor in authors.get_actors_for_author(author) {
                self.revocations_mask
                    .insert(actor.into(), clock.get_for_actor(&actor));
            }
        }
    }

    pub(crate) fn extend_pending_revocations(&mut self, heads: impl Iterator<Item = ChangeHash>) {
        self.pending_revoke.extend(heads)
    }

    pub(crate) fn pop_pending_revocation(&mut self, head: &ChangeHash) -> bool {
        self.pending_revoke.remove(head)
    }
}
