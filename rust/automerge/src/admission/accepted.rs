//! Record acceptance of history already applied in one fixed document.
//!
//! Use [`AppliedAcceptance::accept_applied`] to advance a private sequence clock
//! after selecting admissible history that is already applied. Document contents
//! and heads stay unchanged.
//!
//! The context borrows a single document for its lifetime, so its graph and actor
//! table cannot change underneath this clock. Document-owned storage and actor
//! shift/removal integration remain separate work. Incoming application, queue
//! management, signature verification, and fragment validation are out of scope.

use crate::actor::ActorTable;
use crate::clock::SeqClock;
use crate::signature::SignatureStore;
use crate::{Automerge, ChangeHash};

use super::{AcceptedHistory, CandidateHistory};

/// Acceptance tracking for an already-applied, immutable document snapshot.
///
/// Construct with [`AppliedAcceptance::new`], query exact applied hashes with
/// [`AppliedAcceptance::is_accepted`], and evaluate candidates with
/// [`AppliedAcceptance::accept_applied`]. The document supplies its matching graph,
/// author associations, and actor table rather than accepting these separately.
///
/// The context is not persisted or integrated with normal document mutation yet.
/// A later document-owned design must forward actor-table changes to the clock.
#[derive(Debug)]
struct AppliedAcceptance<'doc> {
    document: &'doc Automerge,
    accepted: AcceptedClock,
}

impl AcceptedHistory for AppliedAcceptance<'_> {
    fn contains(&self, hash: &ChangeHash) -> bool {
        self.is_accepted(hash)
    }
}

/// Contiguous accepted change sequences, never merely the latest signed sequences.
///
/// No raw clock mutation or caller-supplied sequence setter is exposed. The clock
/// belongs to one [`AppliedAcceptance`] context and starts with no accepted history.
#[derive(Debug)]
struct AcceptedClock {
    clock: SeqClock,
}

impl AcceptedClock {
    fn new(actors: &ActorTable) -> Self {
        Self {
            clock: SeqClock::new(actors),
        }
    }

    /// Check whether a change's sequence lies within this actor's accepted prefix.
    fn is_accepted_actor_seq(&self, actor: usize, seq: u32) -> bool {
        self.clock
            .get_for_actor(&actor)
            .is_some_and(|accepted| seq <= accepted.get())
    }
}

impl<'doc> AppliedAcceptance<'doc> {
    /// Start with no accepted history, even if the document already has local edits.
    fn new(document: &'doc Automerge) -> Self {
        Self {
            document,
            accepted: AcceptedClock::new(document.actors()),
        }
    }

    /// Query acceptance of an exact hash already present in this document.
    ///
    /// Unknown hashes return false. Resolve actor and sequence from the document,
    /// not from an incoming change claiming an already-covered actor/sequence pair.
    fn is_accepted(&self, hash: &ChangeHash) -> bool {
        self.document
            .change_graph
            .actor_seq_for_hash(hash)
            .is_some_and(|(actor, seq)| self.accepted.is_accepted_actor_seq(actor, seq))
    }

    /// Evaluate applied candidates and record acceptance of the selected history.
    ///
    /// Insufficient evidence leaves candidates unaccepted and is not an error.
    /// Canonical membership, classification, and actor-chain validity remain input
    /// preconditions. Evidence comes from the store, without repeating verification.
    ///
    /// # Errors
    ///
    /// Preparation errors leave the accepted clock unchanged. Candidates must refer
    /// to applied changes, and stored author associations used by the calculation
    /// must agree with the document.
    fn accept_applied(
        &mut self,
        candidates: &[CandidateHistory<'_>],
        signatures: &SignatureStore,
    ) -> Result<(), error::AcceptApplied> {
        self.prepare(candidates, signatures)?.record();
        Ok(())
    }

    /// Prepare a proof for this clock using fixed document and evidence inputs.
    ///
    /// All fallible work belongs here. The completed value cannot be redirected to
    /// another clock, and it must be consumed before the borrowed context changes.
    fn prepare<'a>(
        &'a mut self,
        candidates: &[CandidateHistory<'_>],
        signatures: &'a SignatureStore,
    ) -> Result<AppliedAdmission<'a>, error::AcceptApplied> {
        // Check the entire input before preparing an update, so an invalid
        // candidate cannot leave earlier candidates partially accepted.
        for candidate in candidates.iter().flat_map(|c| c.changes()) {
            let hash = candidate.hash();
            if !self.document.change_graph.has_change(&hash) {
                return Err(error::AcceptApplied::UnappliedChange { change: hash });
            }

            if let Some(stored_author) = signatures.author(&hash) {
                let document_author = self.document.get_author_for_actor(candidate.actor_id());

                if document_author.as_ref() != Some(stored_author) {
                    return Err(error::AcceptApplied::AuthorMismatch { change: hash });
                }
            }
        }

        let selection = super::select(&*self, candidates, signatures);
        // Preserve existing acceptance. Only recording the completed proof
        // installs this proposed clock, so dropping it has no effect.
        let mut next = self.accepted.clock.clone();

        // Resolve sequences and actor indices from this document, not from
        // caller-supplied limits. The stored sequences already fit SeqClock.
        for change in selection.selected().iter().flat_map(|c| c.changes()) {
            let hash = change.hash();
            let (actor, seq) = self
                .document
                .change_graph
                .actor_seq_for_hash(&hash)
                .ok_or(error::AcceptApplied::UnappliedChange { change: hash })?;
            next.include(actor, Some(seq));
        }
        let target = &mut self.accepted;

        Ok(AppliedAdmission {
            target,
            next,
            _document: self.document,
            _signatures: signatures,
        })
    }
}

/// A checked update, created only by [`AppliedAcceptance::prepare`].
///
/// Carries its target rather than being transferable to an arbitrary clock.
/// The private constructor path establishes matching document, actor-table and
/// evidence context. Borrowing alone would not establish those relationships.
struct AppliedAdmission<'a> {
    target: &'a mut AcceptedClock,
    next: SeqClock,
    _document: &'a Automerge,
    _signatures: &'a SignatureStore,
}

impl AppliedAdmission<'_> {
    /// Record the prepared update after all fallible checks have succeeded.
    fn record(self) {
        self.target.clock = self.next;
    }
}

mod error {
    use crate::ChangeHash;

    /// Failure to prepare acceptance of already-applied history.
    #[derive(Debug, thiserror::Error)]
    pub(super) enum AcceptApplied {
        #[error("candidate change {change} is not applied in this document")]
        UnappliedChange { change: ChangeHash },
        #[error("signature author for change {change} differs from the document author")]
        AuthorMismatch { change: ChangeHash },
    }
}

#[cfg(test)]
mod tests;
