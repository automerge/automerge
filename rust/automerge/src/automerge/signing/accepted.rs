//! Record acceptance of history already applied in a signing document.
//!
//! [`SigningDocument::accept_applied`] prepares and records a private clock update
//! using the document's retained evidence. Document contents and heads stay
//! unchanged. Dropping a prepared update leaves acceptance unchanged too.
//!
//! The document retains the clock between handle borrows and forwards actor-table
//! insertion and removal to it. Incoming application, queue management, signature
//! verification, and fragment validation are separate work.

use super::{error, SigningDocument};
use crate::actor::{ActorRemoval, ActorShift, ActorTable};
use crate::admission;
use crate::admission::CandidateHistory;
use crate::author::Authors;
use crate::change_graph::ChangeGraph;
use crate::clock::SeqClock;
use crate::signature::SignatureStore;

/// Contiguous accepted change sequences, never merely the latest signed sequences.
///
/// No raw clock mutation or caller-supplied sequence setter is exposed. Each clock
/// starts with no accepted history and follows its owning document's actor table.
#[derive(Clone, Debug)]
pub(super) struct AcceptedClock {
    clock: SeqClock,
}

impl AcceptedClock {
    pub(super) fn new(actors: &ActorTable) -> Self {
        Self {
            clock: SeqClock::new(actors),
        }
    }

    /// Check whether a change's sequence lies within this actor's accepted prefix.
    pub(super) fn is_accepted_actor_seq(&self, actor: usize, seq: u32) -> bool {
        self.clock
            .get_for_actor(&actor)
            .is_some_and(|accepted| seq <= accepted.get())
    }

    pub(super) fn insert_actor(&mut self, actor: &ActorShift) {
        self.clock.shift_actor(actor);
    }

    pub(super) fn remove_actor(&mut self, actor: &ActorRemoval) {
        self.clock.remove_actor(actor);
    }

    fn snapshot_clock(&self) -> SeqClock {
        self.clock.clone()
    }
}

/// A checked update created by [`AppliedAdmission::prepare`].
///
/// Carries its target rather than being transferable to an arbitrary clock.
/// The private constructor path establishes matching document, actor-table and
/// evidence context. Borrowing alone would not establish those relationships.
pub(super) struct AppliedAdmission<'a> {
    target: &'a mut AcceptedClock,
    next: SeqClock,
    _graph: &'a ChangeGraph,
    _actors: &'a ActorTable,
    _authors: &'a Authors,
    _signatures: &'a SignatureStore,
}

impl<'a> AppliedAdmission<'a> {
    /// Prepare a clock update bound to this handle's document and evidence.
    ///
    /// All fallible work belongs here. Dropping the result has no effect until
    /// [`AppliedAdmission::record`] installs the proposed clock.
    pub(super) fn prepare(
        document: &'a mut SigningDocument<'_>,
        candidates: &[CandidateHistory<'_>],
    ) -> Result<Self, error::AcceptApplied> {
        // Validate all input before selecting against the document-owned store.
        // Build the proposed clock separately, then bind it to this clock and
        // the matching graph, actor table, authors, and evidence in the proof.
        document.validate_applied(candidates)?;

        let selection = admission::select(&*document, candidates, &document.state.signatures);
        // Preserve existing acceptance. Only recording the completed proof
        // installs this proposed clock, so dropping it has no effect.
        let mut next = document.state.accepted.snapshot_clock();

        // Resolve sequences and actor indices from this document, not from
        // caller-supplied limits. The stored sequences already fit SeqClock.
        for change in selection.selected().iter().flat_map(|c| c.changes()) {
            let hash = change.hash();
            let (actor, seq) = document
                .graph
                .actor_seq_for_hash(&hash)
                .ok_or(error::AcceptApplied::UnappliedChange { change: hash })?;
            next.include(actor, Some(seq));
        }
        let target = &mut document.state.accepted;

        Ok(Self {
            target,
            next,
            _graph: document.graph,
            _actors: document.actors,
            _authors: document.authors,
            _signatures: &document.state.signatures,
        })
    }
}

impl AppliedAdmission<'_> {
    /// Record the prepared update after all fallible checks have succeeded.
    pub(super) fn record(self) {
        self.target.clock = self.next;
    }
}

#[cfg(test)]
mod tests;
