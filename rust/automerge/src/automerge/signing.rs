//! Document-owned signing state and access to required-mode operations.
//!
//! Create an empty required-mode document with [`Automerge::signing`], then
//! obtain a [`SigningDocument`] through [`Automerge::try_signing`]. The document
//! retains its accepted clock and signatures between handle borrows.
//!
//! These entry points remain crate-private while incoming admission and outgoing
//! signing requirements are being integrated.

use crate::actor::{ActorRemoval, ActorShift, ActorTable};
use crate::admission::{AcceptedHistory, CandidateHistory};
use crate::author::Authors;
use crate::change_graph::ChangeGraph;
use crate::signature::{ed25519, Insert, SignatureStore};
use crate::ChangeHash;

use super::Automerge;

mod accepted;

use accepted::AcceptedClock;

/// The document's mode, together with the state required by that mode.
///
/// Ordinary constructors use `Unsigned`. Required mode is selected only when
/// creating a new document, not by obtaining a handle to an existing one.
#[derive(Clone, Debug, Default)]
pub(super) enum SigningState {
    #[default]
    Unsigned,
    Required(RequiredSigning),
}

impl SigningState {
    pub(super) fn insert_actor(&mut self, actor: &ActorShift) {
        if let Self::Required(signing) = self {
            signing.insert_actor(actor);
        }
    }

    pub(super) fn remove_actor(&mut self, actor: &ActorRemoval) {
        if let Self::Required(signing) = self {
            signing.remove_actor(actor);
        }
    }
}

/// Long-lived required-mode data, owned by the document rather than its handle.
///
/// Cloning copies the retained entries and clock. It does not share mutable state
/// or repeat verification. No mutable access to the accepted clock is exposed.
#[derive(Clone, Debug)]
pub(super) struct RequiredSigning {
    accepted: AcceptedClock,
    signatures: SignatureStore,
}

impl RequiredSigning {
    /// Start with no accepted changes and no retained signatures.
    fn new(actors: &ActorTable) -> Self {
        Self {
            accepted: AcceptedClock::new(actors),
            signatures: SignatureStore::default(),
        }
    }

    fn insert_actor(&mut self, actor: &ActorShift) {
        self.accepted.insert_actor(actor);
    }

    fn remove_actor(&mut self, actor: &ActorRemoval) {
        self.accepted.remove_actor(actor);
    }
}

impl SigningState {
    /// Borrow required state, or report the wrong mode without changing it.
    fn required_mut(&mut self) -> Result<&mut RequiredSigning, error::SigningMode> {
        match self {
            SigningState::Unsigned => Err(error::SigningMode::Required),
            SigningState::Required(signing) => Ok(signing),
        }
    }
}

/// Access to a document already in required signing mode.
///
/// Obtain through [`Automerge::try_signing`]. The handle borrows the document's
/// graph, actor table, author associations, and required state together.
///
/// Required mode does not imply that every local change is already signed or
/// accepted. It also does not imply possession of a signing key.
#[derive(Debug)]
pub(crate) struct SigningDocument<'a> {
    graph: &'a ChangeGraph,
    actors: &'a ActorTable,
    authors: &'a Authors,
    state: &'a mut RequiredSigning,
}

impl<'a> SigningDocument<'a> {
    /// Verify and insert an Ed25519 signature for an already-applied change.
    ///
    /// Uses the change's recorded author, not the current local author. Returns
    /// the store's insertion outcome, including duplicates and capacity outcomes.
    /// Insertion does not change accepted history, document contents, or heads.
    /// Use [`SigningDocument::accept_applied`] to evaluate acceptance separately.
    ///
    /// # Errors
    ///
    /// Returns [`error::InsertSignature`] if the hash is unknown, its actor has no
    /// author, the author cannot supply an Ed25519 key, verification fails, or the
    /// store reports a conflicting author. Errors leave the store and acceptance
    /// unchanged.
    pub(crate) fn insert_signature(
        &mut self,
        hash: &ChangeHash,
        signature: &ed25519::Signature,
    ) -> Result<Insert, error::InsertSignature> {
        let (actor, _) = self
            .graph
            .actor_seq_for_hash(hash)
            .ok_or(error::InsertSignature::UnknownChange { change: *hash })?;
        let actor_id = &self.actors[actor];
        let author = self.authors.get_author_for_actor(actor).ok_or_else(|| {
            error::InsertSignature::MissingAuthor {
                change: *hash,
                actor: actor_id.clone(),
            }
        })?;
        let verifier = ed25519::VerifyingAuthor::new(author).map_err(|e| {
            error::InsertSignature::InvalidAuthorKey {
                change: *hash,
                actor: actor_id.clone(),
                source: e,
            }
        })?;
        let witness = verifier.verify_hash(hash, signature).map_err(|e| {
            error::InsertSignature::Verification {
                change: *hash,
                source: e,
            }
        })?;
        self.state
            .signatures
            .insert_ed25519(witness)
            .map_err(|e| error::InsertSignature::Store {
                change: *hash,
                source: e,
            })
    }

    /// Whether this exact applied change belongs to accepted history.
    ///
    /// Unknown hashes return false.
    pub(crate) fn is_accepted(&self, hash: &ChangeHash) -> bool {
        self.graph
            .actor_seq_for_hash(hash)
            .is_some_and(|(actor, seq)| self.state.accepted.is_accepted_actor_seq(actor, seq))
    }

    /// Evaluate applied candidates and record acceptance of the selected history.
    ///
    /// Insufficient evidence leaves candidates unaccepted and is not an error.
    /// Canonical membership, classification, and actor-chain validity remain input
    /// preconditions. Evidence comes from the document's store, without repeating
    /// verification. Document contents and heads stay unchanged.
    ///
    /// # Errors
    ///
    /// Errors leave acceptance unchanged. Candidates must refer to applied changes,
    /// and stored author associations must agree with the document.
    pub(crate) fn accept_applied(
        &mut self,
        candidates: &[CandidateHistory<'_>],
    ) -> Result<(), error::AcceptApplied> {
        self.prepare(candidates)?.record();
        Ok(())
    }

    /// Prepare an update without changing the document's accepted clock.
    fn prepare<'b>(
        &'b mut self,
        candidates: &[CandidateHistory<'_>],
    ) -> Result<accepted::AppliedAdmission<'b>, error::AcceptApplied> {
        accepted::AppliedAdmission::prepare(self, candidates)
    }

    fn validate_applied(
        &self,
        candidates: &[CandidateHistory<'_>],
    ) -> Result<(), error::AcceptApplied> {
        for change in candidates.iter().flat_map(|c| c.changes()) {
            let hash = change.hash();
            let (actor, _) = self
                .graph
                .actor_seq_for_hash(&hash)
                .ok_or(error::AcceptApplied::UnappliedChange { change: hash })?;

            if let Some(stored_author) = self.state.signatures.author(&hash) {
                let document_author = self.authors.get_author_for_actor(actor);

                if document_author.as_ref() != Some(stored_author) {
                    return Err(error::AcceptApplied::AuthorMismatch { change: hash });
                }
            }
        }
        Ok(())
    }
}

impl AcceptedHistory for SigningDocument<'_> {
    fn contains(&self, hash: &ChangeHash) -> bool {
        self.is_accepted(hash)
    }
}

impl Automerge {
    /// Create a new empty document in required signing mode.
    ///
    /// Starts with no accepted history or retained signatures. Use
    /// [`Automerge::try_signing`] to obtain its [`SigningDocument`] handle.
    ///
    /// This constructor does not provide a signing key. Existing unsigned
    /// documents cannot be converted to required mode.
    pub(crate) fn signing() -> Self {
        let mut doc = Self::new();
        let signing = SigningState::Required(RequiredSigning::new(doc.actors()));
        doc.signing = signing;
        doc
    }

    /// Borrow this document's [`SigningDocument`] handle.
    ///
    /// Checks the existing mode without changing it. Dropping the handle leaves
    /// the accepted clock and retained signatures in the document, ready for
    /// subsequent operations.
    ///
    /// # Errors
    ///
    /// Returns [`error::SigningMode::Required`] if the document is unsigned.
    pub(crate) fn try_signing(&mut self) -> Result<SigningDocument<'_>, error::SigningMode> {
        let state = self.signing.required_mut()?;
        Ok(SigningDocument {
            graph: &self.change_graph,
            actors: &self.ops.actors,
            authors: &self.authors,
            state,
        })
    }
}

pub(crate) mod error {
    use crate::signature;
    use crate::signature::ed25519;
    use crate::{ActorId, ChangeHash};

    /// Failure to verify and insert a signature for an already-applied change.
    #[derive(Debug, thiserror::Error)]
    pub(crate) enum InsertSignature {
        #[error("change {change} is not applied in this document")]
        UnknownChange { change: ChangeHash },
        #[error("actor {actor} of change {change} has no recorded author")]
        MissingAuthor { change: ChangeHash, actor: ActorId },
        #[error(
            "author of actor {actor} for change {change} cannot supply an Ed25519 key: {source}"
        )]
        InvalidAuthorKey {
            change: ChangeHash,
            actor: ActorId,
            #[source]
            source: ed25519::error::VerifyingAuthor,
        },
        #[error("signature verification failed for change {change}: {source}")]
        Verification {
            change: ChangeHash,
            #[source]
            source: ed25519::error::Verification,
        },
        #[error("signature insertion failed for change {change}: {source}")]
        Store {
            change: ChangeHash,
            #[source]
            source: signature::InsertError,
        },
    }

    /// An operation requested a mode that the document does not have.
    #[derive(Debug, PartialEq, Eq, thiserror::Error)]
    pub(crate) enum SigningMode {
        #[error("operation requires a signing document")]
        Required,
    }

    /// Failure to prepare acceptance of already-applied history.
    #[derive(Debug, thiserror::Error)]
    pub(crate) enum AcceptApplied {
        #[error("candidate change {change} is not applied in this document")]
        UnappliedChange { change: ChangeHash },

        #[error("signature author for change {change} differs from the document author")]
        AuthorMismatch { change: ChangeHash },
    }
}

#[cfg(test)]
mod tests;
