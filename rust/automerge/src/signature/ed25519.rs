//! Native Ed25519 signature representations and verification.
//!
//! A signature representation is not verified evidence. Only successful strict
//! verification against an author's public key binds a change hash and signature.

use std::hash::{Hash, Hasher};

use crate::{Author, ChangeHash};

use super::signing_message;

pub(crate) mod error;

/// An owned, unverified Ed25519 signature representation.
///
/// Use [`Signature::from_bytes`] with the 64 bytes returned by an external
/// signer. Construction accepts every 64-byte array and does not verify it
/// against a key or message. Wrap it with [`super::Signature::ed25519`] to use
/// [`super::verify_signature`]. The cryptographic library used internally is
/// not part of this interface.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Signature {
    inner: ed25519_dalek::Signature,
}

impl Signature {
    /// Construct from the standard 64-byte Ed25519 signature representation.
    ///
    /// This is infallible and does not allocate or establish cryptographic validity.
    pub fn from_bytes(bytes: [u8; 64]) -> Self {
        Self {
            inner: ed25519_dalek::Signature::from_bytes(&bytes),
        }
    }

    /// Return the exact 64 signature bytes without allocating.
    pub fn to_bytes(&self) -> [u8; 64] {
        self.inner.to_bytes()
    }
}

impl Hash for Signature {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.to_bytes().hash(state);
    }
}

/// Strictly check an Ed25519 signature using the supplied author key and hash.
///
/// This is the built-in branch of generic signature dispatch. It does not resolve
/// actor-to-author associations.
///
/// # Errors
///
/// Returns an error for an undecodable public key or failed strict verification.
pub(super) fn verify(
    author: &Author<'_>,
    hash: &ChangeHash,
    signature: &Signature,
) -> Result<(), error::Verification> {
    let key =
        ed25519_dalek::VerifyingKey::try_from(author.as_bytes()).map_err(error::Verification)?;
    key.verify_strict(&signing_message(hash), &signature.inner)
        .map_err(error::Verification)
}
