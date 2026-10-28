//! Failures preparing Ed25519 authors or verifying their signatures.

use crate::{ActorId, ChangeHash};

/// Author bytes that cannot supply a usable Ed25519 verifying key.
#[derive(Debug, thiserror::Error)]
#[error("author bytes are not a usable Ed25519 public key: {0}")]
pub(crate) struct VerifyingAuthor(#[source] pub(super) VerifyingAuthorInner);

impl From<VerifyingAuthorInner> for VerifyingAuthor {
    fn from(inner: VerifyingAuthorInner) -> Self {
        Self(inner)
    }
}

#[derive(Debug, thiserror::Error)]
pub(super) enum VerifyingAuthorInner {
    #[error("the public key is considered weak, i.e., the public key has low order")]
    WeakKey,
    #[error("failed to parse key, due to {0}")]
    Key(#[source] ed25519_dalek::SignatureError),
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
pub(crate) struct Verification(pub(super) ed25519_dalek::SignatureError);
