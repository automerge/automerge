//! Coordinate incoming change validation, application, and patch recording.
//!
//! [`Automerge::apply_changes_batch_log_patches`] stages validated changes and
//! applies causally ready history. Operation conversion, walking, and installation
//! are handled by [`BatchApply`].

use std::collections::{HashMap, HashSet};

use crate::op_set2::change::batch::BatchApply;
use crate::staging::ChangeBatch;
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
    pub fn apply_changes_batch(
        &mut self,
        changes: impl IntoIterator<Item = Change> + Clone,
    ) -> Result<(), AutomergeError> {
        self.apply_changes_batch_log_patches(changes, &mut PatchLog::inactive())
    }

    pub fn apply_changes_batch_log_patches<I: IntoIterator<Item = Change>>(
        &mut self,
        changes: I,
        log: &mut PatchLog,
    ) -> Result<(), AutomergeError> {
        self.apply_changes_with_grouping(changes, Grouping::Unclassified, log)
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
        let prepared = self.staged.prepare(&self.change_graph);
        let mut batch = BatchApply::new(prepared.changes().to_vec());

        // Align the batch and log with the incoming doc, taking a snapshot of
        // the mask before the op set mutation.
        log.transition_to(self, |d| d.visible_current())?;

        {
            let changes: &[&Change] = batch.changes();
            let mut actors = ActorMaintenance::new(
                &mut self.ops,
                &mut self.change_graph,
                &mut self.actor,
                &mut self.authors,
                &mut self.visibility,
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

        let updates = batch.walk_and_record(self, log, mask0.as_ref())?;
        batch.install(&mut self.ops, updates);

        // If the batch witnessed a pending write-frontier boundary, publish the
        // mask change once, as its own visibility transition between two
        // internally consistent op-set states.
        if resolved {
            // Finish recording under the previous visibility.
            log.transition_to(self, |doc| doc.visible_current())?;

            let changed = self.visibility.refresh(
                &self.write_frontier,
                &self.change_graph,
                &self.authors,
                &self.ops.actors,
            );

            if changed {
                let after = self.visible_current();
                self.ops.recompute_indexes(after.clock());
                log.transition_to(self, |_| after)?;
            }
        }

        debug_assert!(self.ops.validate_op_order());
        // Remove completed entries only after operations and visibility are
        // consistent. Collecting hashes ends the borrowed batch's last use.
        let completed = batch
            .changes()
            .iter()
            .map(|change| change.hash())
            .collect::<HashSet<_>>();
        self.staged.remove_completed(&completed, &self.change_graph);
        Ok(())
    }
}
