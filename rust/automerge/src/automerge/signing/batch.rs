//! Batch signature receipt followed by one coordinated completion pass.
//!
//! [`Automerge::insert_signatures`] verifies each `(hash, signature)` pair
//! against the *historical* author of that applied change and records an owned
//! [`SignatureInsertOutcome`] per occurrence. Individual failures never stop
//! later entries. Once the whole batch has been processed, the document runs a
//! single completion pass: local history proved by the new evidence is
//! published, retained incoming groups that this publication causally enabled
//! are applied (with patches), published local tracking entries are retired,
//! and publication-gated reclamation runs. The pass repeats only while it makes
//! progress, so it terminates without redelivery.
//!
//! Preflight failures (wrong mode, incompatible patch log) return
//! [`SignatureBatchOutcome::Rejected`] before the input iterator is consumed or
//! anything is mutated.

use crate::signature::{ed25519, Insert};
use crate::{AutomergeError, ChangeHash, PatchLog, PatchLogMismatch};

use super::error::{InsertSignature, SigningAccess};
use super::{Automerge, SigningDocument};

/// Failure to verify and insert one signature for an applied change.
pub type SignatureInsertError = InsertSignature;

/// Result of one `(hash, signature)` occurrence in a batch.
#[derive(Debug)]
pub struct SignatureInsertOutcome {
    /// The applied change hash this entry targeted.
    pub hash: ChangeHash,
    /// The signature supplied for `hash`.
    pub signature: ed25519::Signature,
    /// Whether the signature became evidence, was a duplicate, or failed.
    pub result: Result<Insert, SignatureInsertError>,
}

/// A batch was refused before any entry was processed.
#[derive(Debug, thiserror::Error)]
pub enum SignatureRejection {
    /// The document is not in required signing mode, or its author is unusable.
    #[error(transparent)]
    Mode(#[from] SigningAccess),
    /// The supplied patch log belongs to a different document lineage.
    #[error(transparent)]
    PatchLog(#[from] PatchLogMismatch),
}

/// Completion could not apply causally enabled incoming groups.
///
/// Inserted evidence, completed local publication and retained pending groups
/// are preserved; the document and log remain consistent. Retrying without
/// new input may fail the same way.
#[derive(Debug, thiserror::Error)]
pub enum PublicationError {
    /// Admission planning found a conflict, such as two simultaneously
    /// admissible groups claiming the same actor sequence.
    #[error("incoming admission failed after local publication: {0}")]
    Admission(#[source] AutomergeError),
}

/// Outcome of [`Automerge::insert_signatures`].
///
/// `Completed` means the batch was processed and completion ran; it does not
/// mean every change is published. Missing dependencies or proof are normal
/// waiting states.
#[derive(Debug)]
pub enum SignatureBatchOutcome {
    /// Nothing was processed and nothing changed.
    Rejected { error: SignatureRejection },
    /// Every entry has an owned result and completion reached a stable state.
    Completed {
        insertions: Vec<SignatureInsertOutcome>,
    },
    /// Entries were processed and local publication completed, but incoming
    /// application hit a recoverable planning conflict.
    PublicationFailed {
        insertions: Vec<SignatureInsertOutcome>,
        error: PublicationError,
    },
}

impl Automerge {
    /// Verify and insert signatures for applied changes, then complete
    /// publication once.
    ///
    /// Inputs are processed in order; every occurrence, including duplicates,
    /// receives an owned [`SignatureInsertOutcome`]. Each signature is checked
    /// against the author recorded for the change's actor, not the current
    /// local author.
    pub fn insert_signatures(
        &mut self,
        inputs: impl IntoIterator<Item = (ChangeHash, ed25519::Signature)>,
    ) -> SignatureBatchOutcome {
        self.insert_signatures_log_patches(inputs, &mut PatchLog::inactive())
    }

    /// Like [`Self::insert_signatures`], recording any incoming groups applied
    /// by completion into `log`.
    pub fn insert_signatures_log_patches(
        &mut self,
        inputs: impl IntoIterator<Item = (ChangeHash, ed25519::Signature)>,
        log: &mut PatchLog,
    ) -> SignatureBatchOutcome {
        if let Err(error) = self.preflight_signature_batch(log) {
            return SignatureBatchOutcome::Rejected { error };
        }
        let insertions = {
            let mut signing = self
                .try_signing()
                .expect("preflight established required mode and author");
            inputs
                .into_iter()
                .map(|(hash, signature)| SignatureInsertOutcome {
                    hash,
                    signature,
                    result: signing.receive_signature(&hash, &signature),
                })
                .collect()
        };
        match self.complete_publication(log) {
            Ok(()) => SignatureBatchOutcome::Completed { insertions },
            Err(error) => SignatureBatchOutcome::PublicationFailed { insertions, error },
        }
    }

    /// Checks that reject a batch before any input is consumed.
    fn preflight_signature_batch(&mut self, log: &PatchLog) -> Result<(), SignatureRejection> {
        self.try_signing()?;
        log.validate(&self.ops.actors)?;
        Ok(())
    }

    /// Publish newly proved local history and admit pending groups enabled by
    /// it, repeating only while the retry settles a retained group.
    ///
    /// A retry with no new input can only make progress by selecting a pending
    /// group, and every selected group leaves the pending set whether it
    /// applied new history, merely merged proof for already-applied contents
    /// or only advanced publication. Any of those can enable a further local
    /// publication, so another local pass follows. The pending set is finite
    /// and strictly shrinks on each continued iteration, bounding the loop.
    ///
    /// Local publication is attempted first and always retained. Incoming
    /// retry failures surface as [`PublicationError`] with everything done so
    /// far kept.
    pub(super) fn complete_publication(
        &mut self,
        log: &mut PatchLog,
    ) -> Result<(), PublicationError> {
        loop {
            self.try_signing()
                .expect("completion runs on a required-mode document")
                .publish_local();
            let before = self.signing.pending_group_count();
            if before == 0 {
                return Ok(());
            }
            self.retry_pending_groups(log)
                .map_err(PublicationError::Admission)?;
            if self.signing.pending_group_count() == before {
                return Ok(());
            }
        }
    }
}

impl SigningDocument<'_> {
    /// Verify `signature` under the change's historical author and retain it.
    ///
    /// Pure receipt: no publication, retirement or reclamation happens here.
    pub(super) fn receive_signature(
        &mut self,
        hash: &ChangeHash,
        signature: &ed25519::Signature,
    ) -> Result<Insert, InsertSignature> {
        let (actor, _) = self
            .graph
            .actor_seq_for_hash(hash)
            .ok_or(InsertSignature::UnknownChange { change: *hash })?;
        let actor_id = &self.ops.actors[actor];
        let author = self.authors.get_author_for_actor(actor).ok_or_else(|| {
            InsertSignature::MissingAuthor {
                change: *hash,
                actor: actor_id.clone(),
            }
        })?;
        let verifier = ed25519::VerifyingAuthor::new(author).map_err(|e| {
            InsertSignature::InvalidAuthorKey {
                change: *hash,
                actor: actor_id.clone(),
                source: e,
            }
        })?;
        let witness =
            verifier
                .verify_hash(hash, signature)
                .map_err(|e| InsertSignature::Verification {
                    change: *hash,
                    source: e,
                })?;
        let had_evidence =
            super::has_signature(self.graph, self.authors, &self.state.signatures, hash);
        let inserted =
            self.state
                .signatures
                .insert_ed25519(witness)
                .map_err(|e| InsertSignature::Store {
                    change: *hash,
                    source: e,
                })?;
        if !had_evidence
            && super::has_signature(self.graph, self.authors, &self.state.signatures, hash)
        {
            self.state.fragment_readiness.signature_added(hash);
        }
        Ok(inserted)
    }

    /// Publish proved local history, retire published tracking entries and
    /// reclaim evidence made redundant by published covers.
    pub(super) fn publish_local(&mut self) {
        self.accept_local();
        self.state.reclaim_covered(self.graph);
        self.state.retire_published_drafts(self.graph);
    }
}
