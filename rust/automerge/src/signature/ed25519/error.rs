//! Failures preparing Ed25519 authors or verifying their signatures.

/// Author bytes that cannot supply a usable Ed25519 verifying key.
#[derive(Debug, thiserror::Error)]
#[error("author bytes are not a usable Ed25519 public key: {0}")]
pub struct VerifyingAuthor(#[source] pub(super) VerifyingAuthorInner);

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

/// Failure to verify an Ed25519 signature for the supplied author and change hash.
///
/// The author bytes could not be decoded as a public key, or the signature
/// failed strict verification.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct Verification(pub(super) ed25519_dalek::SignatureError);
