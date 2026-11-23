//! Batch signature receipt followed by one coordinated completion pass.
//!
//! [`crate::Automerge::insert_signatures`] verifies each `(hash, signature)` pair
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
use crate::{AutomergeError, ChangeHash, PatchLogMismatch};

use super::error::{InsertSignature, SigningMode};

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
    /// The document is not in required signing mode.
    #[error(transparent)]
    Mode(#[from] SigningMode),
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

/// Outcome of [`crate::Automerge::insert_signatures`].
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
