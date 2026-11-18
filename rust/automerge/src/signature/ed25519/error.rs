//! Failures preparing Ed25519 authors or verifying their signatures.

use std::fmt;

use crate::{ActorId, ChangeHash};

/// Author bytes that cannot supply a usable Ed25519 verifying key.
///
/// Use [`Self::kind`] for a copyable classification. A decoding failure retains
/// the library error as its direct source; a weak-key failure has no source.
/// Private fields and dedicated constructors keep the kind and source paired.
#[derive(Debug, thiserror::Error)]
pub struct VerifyingAuthor {
    kind: VerifyingAuthorKind,
    #[source]
    source: Option<ed25519_dalek::SignatureError>,
}

/// Copyable classification of an author-key preparation failure.
///
/// The original [`VerifyingAuthor`] error retains its full source chain. This
/// kind can be copied into independent batch diagnostics without sharing that
/// source or repeating key preparation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VerifyingAuthorKind {
    /// The author bytes could not be decoded as an Ed25519 verifying key.
    InvalidKey,
    /// The key decodes but has small order and is unsuitable for verification.
    WeakKey,
}

impl VerifyingAuthor {
    pub(super) fn invalid_key(source: ed25519_dalek::SignatureError) -> Self {
        Self {
            kind: VerifyingAuthorKind::InvalidKey,
            source: Some(source),
        }
    }

    pub(super) fn weak_key() -> Self {
        Self {
            kind: VerifyingAuthorKind::WeakKey,
            source: None,
        }
    }

    /// Classify this failure without consuming it or discarding its source.
    pub fn kind(&self) -> VerifyingAuthorKind {
        self.kind
    }
}

impl fmt::Display for VerifyingAuthor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("author bytes are not a usable Ed25519 public key: ")?;
        match &self.source {
            Some(source) => write!(f, "failed to parse key, due to {source}"),
            None => {
                f.write_str("the public key is considered weak, i.e., the public key has low order")
            }
        }
    }
}

/// Failure to find a change's prepared verifying author or verify its signature.
#[derive(Debug, thiserror::Error)]
pub(crate) enum VerifiedSignature {
    /// No successfully prepared author was supplied for the change's actor.
    #[error("no prepared verifying author for actor {actor} of change {change}")]
    MissingVerifyingAuthor {
        /// Hash of the input change lacking verification context.
        change: ChangeHash,
        /// Actor ID whose prepared author is missing, not a document-local index.
        actor: ActorId,
    },
    /// The signature failed strict Ed25519 verification under the prepared key.
    #[error("signature verification failed for change {change}: {source}")]
    Verification {
        /// Hash of the change whose signature could not be verified.
        change: ChangeHash,
        /// The underlying verification failure.
        #[source]
        source: Verification,
    },
}

/// Failure to verify an Ed25519 signature for the supplied author and change hash.
///
/// The author bytes could not be decoded as a public key, or the signature
/// failed strict verification.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct Verification(pub(super) ed25519_dalek::SignatureError);
