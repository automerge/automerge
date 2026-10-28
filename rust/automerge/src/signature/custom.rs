//! Application-provided verification for non-native signature schemes.
//!
//! Custom verification is independent of whether evidence is eligible for
//! Automerge attestation. Unchecked signature values do not imply a custom
//! verification result, and custom-verified evidence is not modelled here yet.

use crate::Author;

use super::{SignatureBytes, SignatureTag, Verification};

/// Verify opaque signatures belonging to schemes other than built-in Ed25519.
///
/// The dispatcher supplies the complete domain-separated signing message and
/// borrows the original author and size-checked signature bytes. Implementations
/// interpret those bytes according to `tag`, returning [`Verification::Unsupported`]
/// for schemes they do not support. Ed25519 is never delegated to this trait,
/// including signatures with malformed lengths or invalid keys.
///
/// Verification must be a pure function of these inputs, with stable results
/// for the lifetime of the verifier. It must not depend on time, I/O, revocation,
/// or mutable authorisation policy. `Send + Sync` permits sharing but does not
/// enforce this semantic contract.
///
/// A valid verdict establishes only signature verification under this scheme,
/// not eligibility for fragment attestation or export.
pub trait Verifier: Send + Sync {
    /// Check the supplied message without adding the signing-domain prefix again.
    fn verify(
        &self,
        author: &Author<'_>,
        message: &[u8],
        tag: SignatureTag,
        bytes: &SignatureBytes,
    ) -> Verification;
}

#[cfg(test)]
mod tests;
