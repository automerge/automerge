//! Native Ed25519 signature representations and verification evidence.
//!
//! A signature representation or a usable author key is not verified evidence.
//! Only successful strict verification binds a prepared author, change hash, and
//! exact signature. Resolving the expected historical author remains the caller's responsibility.

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

/// An author whose bytes identify a usable Ed25519 verifying key.
///
/// Construct from an author with [`VerifyingAuthor::new`]. The key is decoded
/// from those same bytes and retained for checking signatures across changes.
/// Fields are private so the author cannot disagree with the parsed key.
///
/// This value does not establish a change's actor-to-author association or
/// provide signature evidence. It may borrow author bytes or own them.
#[derive(Clone, Debug)]
pub(crate) struct VerifyingAuthor<'a> {
    author: Author<'a>,
    key: ed25519_dalek::VerifyingKey,
}

impl<'a> VerifyingAuthor<'a> {
    /// Retain the author view and decode its Ed25519 key once.
    ///
    /// # Errors
    ///
    /// Returns [`error::VerifyingAuthor`] for malformed key bytes or a
    /// small-order key. A successful construction is not signature verification.
    pub(crate) fn new(author: Author<'a>) -> Result<Self, error::VerifyingAuthor> {
        let key = ed25519_dalek::VerifyingKey::try_from(author.as_bytes())
            .map_err(error::VerifyingAuthorInner::Key)?;
        if key.is_weak() {
            Err(error::VerifyingAuthorInner::WeakKey.into())
        } else {
            Ok(Self { author, key })
        }
    }

    /// Borrow the author whose bytes supplied the retained key.
    pub(crate) fn author(&self) -> &Author<'a> {
        &self.author
    }

    /// Verify the domain-separated message and return evidence for this hash.
    ///
    /// Uses the retained key with strict verification. The witness binds this
    /// author, the exact hash, and the supplied signature. The caller must
    /// establish that this author is the expected author for the hash.
    ///
    /// # Errors
    ///
    /// Returns [`error::Verification`] when the signature fails strict
    /// verification. Key validation has already happened during construction.
    pub(crate) fn verify_hash<'signature>(
        &self,
        hash: &ChangeHash,
        signature: &'signature Signature,
    ) -> Result<VerifiedSignature<'a, 'signature>, error::Verification> {
        let message = signing_message(hash);
        self.key
            .verify_strict(&message, &signature.inner)
            .map_err(error::Verification)?;
        Ok(VerifiedSignature {
            author: self.author.clone(),
            hash: *hash,
            signature,
        })
    }
}

/// Evidence that a signature verifies for an author and an exact change hash.
///
/// Construct through [`VerifyingAuthor::verify_hash`]. The witness retains the
/// author identity and hash, borrowing the signature and any borrowed author bytes.
/// Cloning does not repeat verification. The caller must establish the expected
/// author for the hash. This witness does not validate history or authorise a write.
#[derive(Clone, Debug)]
pub(crate) struct VerifiedSignature<'author, 'signature> {
    author: Author<'author>,
    hash: ChangeHash,
    signature: &'signature Signature,
}

impl<'author, 'signature> VerifiedSignature<'author, 'signature> {
    /// Borrow the identity under which the signature was verified.
    pub(super) fn author(&self) -> &Author<'author> {
        &self.author
    }

    /// Borrow the change hash covered by the verified signature.
    pub(crate) fn hash(&self) -> &ChangeHash {
        &self.hash
    }

    /// Borrow the Ed25519 signature that passed verification.
    pub(crate) fn signature(&self) -> &'signature Signature {
        self.signature
    }
}

/// Strictly check an Ed25519 signature using the supplied author key and hash.
///
/// This is the built-in branch of generic signature dispatch. It does not resolve
/// actor-to-author associations or produce a verified-signature witness.
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

#[cfg(test)]
mod tests;
