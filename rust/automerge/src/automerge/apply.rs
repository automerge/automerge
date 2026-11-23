//! Coordinate incoming change validation, application, and patch recording.
//!
//! [`Automerge::apply_changes_batch_log_patches`] stages validated changes and
//! applies causally ready history. Operation conversion, walking, and installation
//! are handled by [`BatchApply`].

use std::collections::{HashMap, HashSet};

use crate::op_set2::change::batch::BatchApply;
use crate::signature::error::SigningError;
use crate::staging::{ChangeBatch, StagedChanges};
use crate::{ActorId, AutomergeError, Change, ChangeHash, Fragment, PatchLog, PatchLogMismatch};

use super::{ActorMaintenance, AuthorRegistration, Automerge, History, HistoryUpdate};

#[cfg(test)]
mod tests;

/// Grouping knowledge supplied separately from the ordered change contents.
///
/// Known descriptors come from an internal producer and may cover only part of
/// the input. Neither raw changes nor generic bundles establish this knowledge.
enum Grouping {
    Unclassified,
    Known(Vec<Fragment>),
}

impl Automerge {
    pub fn apply_changes_batch<I>(&mut self, changes: I) -> Result<(), AutomergeError>
    where
        I: IntoIterator,
        I::Item: TryInto<crate::LoadedGroup>,
        AutomergeError: From<<I::Item as TryInto<crate::LoadedGroup>>::Error>,
    {
        self.apply_changes_batch_log_patches(changes, &mut PatchLog::inactive())
    }

    pub fn apply_changes_batch_log_patches<I>(
        &mut self,
        changes: I,
        log: &mut PatchLog,
    ) -> Result<(), AutomergeError>
    where
        I: IntoIterator,
        I::Item: TryInto<crate::LoadedGroup>,
        AutomergeError: From<<I::Item as TryInto<crate::LoadedGroup>>::Error>,
    {
        use crate::storage::load::LoadedGroupBody;

        log.validate(&self.ops.actors)?;
        let groups: Vec<crate::LoadedGroup> = changes
            .into_iter()
            .map(|item| item.try_into().map_err(AutomergeError::from))
            .collect::<Result<_, _>>()?;
        if self.signing.is_required() {
            if groups.iter().any(|group| group.signatures.is_none()) {
                return Err(SigningError::UnauthenticatedChanges.into());
            }
            self.admit_loaded_groups(groups, log)
        } else {
            // Unsigned mode never strips a proof: decoded, parsed and encoded
            // inputs alike are rejected whole before any member is applied.
            if groups.iter().any(|group| group.signatures.is_some()) {
                return Err(crate::storage::load::Error::SignedContent.into());
            }
            let changes = groups.into_iter().flat_map(|group| match group.body {
                LoadedGroupBody::Changes(changes) => changes,
                LoadedGroupBody::Document(_) => {
                    unreachable!("apply_changes inputs exclude snapshots")
                }
            });
            self.apply_changes_with_grouping(changes, Grouping::Unclassified, log)
        }
    }

    /// Import ordered changes with grouping supplied by an internal producer.
    ///
    /// Descriptors keep their full membership, including already-applied or
    /// staged members. Content validation and causal application are shared with
    /// [`Self::apply_changes_batch_log_patches`]. Descriptor preconditions and
    /// invariant assertions are those of
    /// [`crate::staging::StagedChanges::extend_with_fragments`].
    pub(super) fn apply_changes_with_fragments_log_patches(
        &mut self,
        changes: impl IntoIterator<Item = Change>,
        fragments: Vec<Fragment>,
        log: &mut PatchLog,
    ) -> Result<(), AutomergeError> {
        self.apply_changes_with_grouping(changes, Grouping::Known(fragments), log)
    }

    fn apply_changes_with_grouping(
        &mut self,
        changes: impl IntoIterator<Item = Change>,
        grouping: Grouping,
        log: &mut PatchLog,
    ) -> Result<(), AutomergeError> {
        // Unclassified or bundle-grouped contents carry no proofs, so required
        // mode rejects them outright; unsigned mode skips the check.
        self.signing
            .with_required(|| Err(SigningError::UnauthenticatedChanges))?;
        // Validate the observer before staging or applying incoming changes.
        // A rejected log leaves existing staged history and recorded events
        // available for retry. Once validation passes, actor-table growth during
        // apply_ready preserves compatibility for later patch-log migrations.
        log.validate(&self.ops.actors)?;

        let mut seen: HashSet<ChangeHash> = self.staged.iter().map(Change::hash).collect();
        let mut actor_seqs: HashMap<ActorId, HashSet<u64>> = HashMap::new();
        let mut actor_author: HashSet<ActorId> = HashSet::new();

        for change in self.staged.iter() {
            actor_seqs
                .entry(change.actor_id().clone())
                .or_default()
                .insert(change.seq());
            if change.author().is_some() {
                actor_author.insert(change.actor_id().clone());
            }
        }

        let mut incoming = ChangeBatch::new();
        for change in changes {
            let hash = change.hash();
            if self.change_graph.has_change(&hash) || seen.contains(&hash) {
                continue;
            }
            if self.has_actor_seq(&change) {
                self.staged
                    .remove_actor_branch_from(change.actor_id(), change.seq().saturating_add(1));
                return Err(AutomergeError::duplicate_seq(&change));
            }
            if actor_seqs
                .get(change.actor_id())
                .is_some_and(|seqs| seqs.contains(&change.seq()))
            {
                return Err(AutomergeError::duplicate_seq(&change));
            }
            // TODO(finto): Refactor with BatchBuilder pattern
            if change.author().is_some()
                && (self.get_author_for_actor(change.actor_id()).is_some()
                    || actor_author.contains(change.actor_id()))
            {
                return Err(AutomergeError::duplicate_author(&change));
            }
            if change.author().is_some() && change.seq() != 1 {
                return Err(AutomergeError::author_on_non_initial_seq(&change));
            }

            seen.insert(hash);
            actor_seqs
                .entry(change.actor_id().clone())
                .or_default()
                .insert(change.seq());
            if change.author().is_some() {
                actor_author.insert(change.actor_id().clone());
            }
            incoming.push(change)?;
        }

        match grouping {
            Grouping::Unclassified => self.staged.extend(incoming),
            Grouping::Known(fragments) => {
                self.staged
                    .extend_with_fragments(incoming, fragments, &self.change_graph);
            }
        }
        self.apply_ready(log)?;
        Ok(())
    }

    fn apply_ready(&mut self, log: &mut PatchLog) -> Result<(), PatchLogMismatch> {
        let mut staged = std::mem::replace(&mut self.staged, StagedChanges::new());
        let result = (|| {
            let prepared = staged.prepare(&self.change_graph);
            let changes = prepared.changes().to_vec();
            let completed = changes.iter().map(|c| c.hash()).collect::<HashSet<_>>();
            self.apply_prepared_changes(changes, log)?;
            staged.remove_completed(&completed, &self.change_graph);
            Ok(())
        })();
        self.staged = staged;
        result
    }

    /// Apply causally ordered, validated changes to actors, authors, history,
    /// operations, visibility, and `log`.
    ///
    /// # Errors
    ///
    /// Fails only if `log` is incompatible with this document, and only before
    /// any actor, history, operation, or visibility mutation of the document.
    /// Once that preflight succeeds the log's actors equal the document's;
    /// every later log operation in this method sees a document whose actor
    /// table either grew (a superset migrates infallibly) or is unchanged.
    /// Cleanup cannot shrink it by construction: every actor inserted above
    /// gains a graph entry from `History::update` before `remove_unused`
    /// runs, and `remove_unused` only drops actors without one; the debug
    /// assertions around it check this, they do not enforce it in release
    /// builds. A log failure after preflight is therefore an internal
    /// invariant violation and panics after mutation, rather than returning an
    /// error that would imply the half-applied document is usable.
    pub(super) fn apply_prepared_changes(
        &mut self,
        changes: Vec<&Change>,
        log: &mut PatchLog,
    ) -> Result<(), PatchLogMismatch> {
        let mut batch = BatchApply::new(changes);

        // Preflight: align the batch and log with the incoming doc, taking a
        // snapshot of the mask before the op set mutation. This is the only
        // fallible log operation; the document has not been mutated yet (the
        // log itself may record a view transition on success).
        log.transition_to(self, |d| d.visible_current())?;
        const ALIGNED: &str = "patch log must remain compatible with the document's actors";

        {
            let changes: &[&Change] = batch.changes();
            let mut actors = ActorMaintenance::new(
                &mut self.ops,
                &mut self.change_graph,
                &mut self.actor,
                &mut self.authors,
                &mut self.visibility,
                &mut self.signing,
            );
            // Insert every new actor first: a later insertion would shift the
            // indices collected for the earlier ones. The second pass is pure
            // lookup. Registration is bulk: one visibility derivation per batch,
            // not one per new actor.
            for change in changes.iter().filter(|change| change.seq() == 1) {
                actors.insert(change.actor_id().clone());
            }
            let pairs: Vec<_> = changes
                .iter()
                .filter(|change| change.seq() == 1)
                .filter_map(|change| {
                    let author = change.author()?.into_owned();
                    let actor = self
                        .ops
                        .lookup_actor(change.actor_id())
                        .expect("batch actor was inserted before author registration");
                    Some((author, actor))
                })
                .collect();
            AuthorRegistration::new(
                &mut self.authors,
                &mut self.visibility,
                &self.change_graph,
                &self.ops.actors,
                &self.write_frontier,
            )
            .register(pairs);
        }

        self.assert_mask_derived();
        let mask0 = self.mask().cloned();

        // Mark whether a pending boundary has been resolved by any of the
        // incoming changes. If resolved is true, the mask is published after
        // the op set is consistent again.
        let resolved = {
            let mut history = History::new(
                &mut self.change_graph,
                &mut self.deps,
                &mut self.authors,
                &self.ops.actors,
                &self.visibility,
                &mut self.signing,
            );
            let mut resolved = false;
            for change in batch.changes() {
                resolved |= matches!(history.update(change), HistoryUpdate::BoundaryResolved);
            }
            resolved
        };

        let actors_before = self.ops().actors.len();
        batch.import_ops(self.actors(), mask0.as_ref());

        self.assert_no_unused_actors(cfg!(debug_assertions));
        ActorMaintenance::new(
            &mut self.ops,
            &mut self.change_graph,
            &mut self.actor,
            &mut self.authors,
            &mut self.visibility,
            &mut self.signing,
        )
        .remove_unused();

        // The mask snapshot and the imported ops use the actor indices established
        // above. Every inserted actor now has a graph entry, so cleanup must not
        // renumber actors between import and the operation walk.
        debug_assert_eq!(
            self.ops().actors.len(),
            actors_before,
            "no actor renumbering between the mask snapshot and the walk"
        );

        let updates = batch
            .walk_and_record(self, log, mask0.as_ref())
            .expect(ALIGNED);
        batch.install(&mut self.ops, updates);

        // If the batch witnessed a pending write-frontier boundary, publish the
        // mask change once, as its own visibility transition between two
        // internally consistent op-set states.
        if resolved {
            // Finish recording under the previous visibility.
            log.transition_to(self, |doc| doc.visible_current())
                .expect(ALIGNED);

            let changed = self.visibility.refresh(
                &self.write_frontier,
                &self.change_graph,
                &self.authors,
                &self.ops.actors,
            );

            if changed {
                let after = self.visible_current();
                self.ops.recompute_indexes(after.clock());
                log.transition_to(self, |_| after).expect(ALIGNED);
            }
        }

        debug_assert!(self.ops.validate_op_order());
        Ok(())
    }
}
