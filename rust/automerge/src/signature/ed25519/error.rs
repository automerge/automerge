//! Failures verifying Ed25519 signatures.

/// Failure to verify an Ed25519 signature for the supplied author and change hash.
///
/// The author bytes could not be decoded as a public key, or the signature
/// failed strict verification.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub(crate) struct Verification(pub(super) ed25519_dalek::SignatureError);
