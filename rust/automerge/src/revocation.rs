use std::{
    collections::{HashMap, HashSet},
    num::NonZeroU32,
};

use crate::{author::Authors, clock::SeqClock, op_set2::ActorIdx, Author, ChangeHash};

/// Record revocations of [`Author`]s in relation to a set of changes.
///
/// Each author is uniquely revoked, and the heads are the inclusive bounds at
/// which the Automerge document will materialize for this author. Any changes
/// after these changes will be considered invisible.
///
/// # Revocation Mask
///
/// A revocation mask is a mapping from each actor to a sequence number. This
/// provides data to build a clock for marking operations as invisible when
/// materializing an Automerge document.
///
/// # Pending Changes
///
/// In some cases, the [`ChangeHash`] might not yet be recorded in the change
/// graph, these can be kept track of by using
/// [`Revocations::extend_pending_revocations`], and removed using
/// [`Revocations::pop_pending_revocation`].
#[derive(Debug, Default, Clone)]
pub(crate) struct Revocations {
    /// Revocations are a map from an [`Author`] to a [`ChangeHash`] heads
    /// boundary. These [`ChangeHash`]es, and any descendants will be made
    /// invisible to the materialized document.
    revocations: HashMap<Author<'static>, Vec<ChangeHash>>,
    /// Mapping from [`ActorIdx`] to maximum sequence number allowed by the
    /// revocations.
    revocations_mask: HashMap<ActorIdx, Option<NonZeroU32>>,
    /// [`ChangeHash`] values that are not witnessed in the graph, but are being
    /// revoked for an author.
    pending_revoke: HashSet<ChangeHash>,
}

impl Revocations {
    /// Initialize the [`Revocations`] beginning with no [`Author`]s being revoked.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Returns `true` if no [`Author`]s have been revoked.
    pub(crate) fn is_empty(&self) -> bool {
        self.revocations.is_empty()
    }

    /// Return at which changes the [`Author`] was revoked at, if any.
    pub(crate) fn get_revocations_for_author(
        &self,
        author: &Author<'static>,
    ) -> Option<&[ChangeHash]> {
        self.revocations.get(author).map(|heads| heads.as_slice())
    }

    /// Return the set of revocations, per [`Author`].
    pub(crate) fn get_revocations(&self) -> &HashMap<Author<'static>, Vec<ChangeHash>> {
        &self.revocations
    }

    /// Revoke the given [`Author`] at the given `heads`.
    ///
    /// [`Authors`] and [`SeqClock`] are used for building the [revocation
    /// mask](Revocations#revocation-mask).
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

    /// Unrevoke the given [`Author`], so that their changes can be materialized
    /// in the Automerge document again.
    ///
    /// [`Authors`] is used for removing the actors from the [revocation
    /// mask](Revocations#revocation-mask).
    pub(crate) fn unrevoke(&mut self, author: &Author<'static>, authors: &Authors) {
        for a in authors.get_actors_for_author(author) {
            self.revocations_mask.remove(&a.into());
        }
        self.revocations.remove(author);
    }

    /// Returns `true` if the `actor` is revoked at the given sequence number.
    ///
    /// If the recorded mask is `None`, then the actor is revoked regardless of
    /// the sequence number.
    pub(crate) fn is_revoked(&self, actor: ActorIdx, seq: u64) -> bool {
        match self.revocations_mask.get(&actor) {
            Some(Some(v)) if (v.get() as u64) < seq => true,
            Some(None) => true,
            _ => false,
        }
    }

    /// Insert a `mask` for the given `actor`.
    pub(crate) fn insert_mask_for(&mut self, actor: ActorIdx, mask: Option<NonZeroU32>) {
        self.revocations_mask.insert(actor, mask);
    }

    /// Return the mask for the given `actor`.
    pub(crate) fn get_mask_for(&self, actor: &ActorIdx) -> Option<&Option<NonZeroU32>> {
        self.revocations_mask.get(actor)
    }

    /// Return the [revocation mask](Revocations#revocation-mask).
    pub(crate) fn get_revocation_mask(&self) -> &HashMap<ActorIdx, Option<NonZeroU32>> {
        &self.revocations_mask
    }

    /// Insert an `actor` into the [revocation mask](Revocations#revocation-mask).
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

    /// Remove an `actor` from the [revocation mask](Revocations#revocation-mask).
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

    /// Clear all [`Revocations`] state.
    pub(crate) fn clear(&mut self) {
        self.pending_revoke.clear();
        self.revocations_mask.clear();
        self.revocations.clear();
    }

    /// Recompute the [revocation mask](Revocations#revocation-mask).
    ///
    /// [`Authors`] is used for finding the recorded actors for each author,
    /// while `get_clock` is a callback for calculating the new [`SeqClock`] for
    /// updating the mask entry for each actor.
    pub(crate) fn recompute_revocations(
        &mut self,
        authors: &Authors,
        get_clock: impl Fn(&[ChangeHash]) -> SeqClock,
    ) {
        for (author, heads) in &self.revocations {
            let clock = get_clock(heads);
            for actor in authors.get_actors_for_author(author) {
                self.revocations_mask
                    .insert(actor.into(), clock.get_for_actor(&actor));
            }
        }
    }

    /// Extend the set of pending revocation changes with the given `heads`.
    ///
    /// Once a change has been witnessed, it can be removed using
    /// [`Revocations::pop_pending_revocation`].
    pub(crate) fn extend_pending_revocations(&mut self, heads: impl Iterator<Item = ChangeHash>) {
        self.pending_revoke.extend(heads)
    }

    /// Remove the `head` from the pending revocations, returning `true` if it
    /// existed in the set.
    pub(crate) fn pop_pending_revocation(&mut self, head: &ChangeHash) -> bool {
        self.pending_revoke.remove(head)
    }
}
