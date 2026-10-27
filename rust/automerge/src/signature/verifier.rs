//! Pure verification of signatures over domain-separated change hashes.
//!
//! Ed25519 verification uses the supplied author bytes as the public key.
//! Custom verifiers interpret author bytes for their own schemes. Neither path
//! establishes the change's actor-to-author binding or makes admission decisions.

use crate::{Author, ChangeHash};

use super::{
    signing_message, Ed25519Signature, Signature, SignatureBytes, SignatureRepr, SignatureTag,
};

/// The result of checking a signature against a supplied author and change hash.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verification {
    /// The signature verifies under the supplied key and signing-message contract.
    Valid,
    /// The scheme is supported, but the key or signature is malformed or fails verification.
    Invalid,
    /// This verifier does not support the signature's scheme.
    Unsupported,
}

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

/// Check a signature over the domain-separated message for `hash`.
///
/// Typed Ed25519 signatures use Dalek's strict verification rules. Malformed
/// Ed25519 lengths return [`Verification::Invalid`], without being delegated to
/// another scheme. Other tags are delegated to `custom`, if supplied, without
/// Ed25519-specific validation. Its verdict is returned unchanged. Without a
/// custom verifier these tags return [`Verification::Unsupported`].
///
/// For Ed25519, `author` must contain the 32-byte public key. Verification checks
/// only the supplied key, hash, and signature. It does not validate a change's
/// contents, establish its actor-to-author binding, or authorise a write.
///
/// The custom verifier is borrowed, not stored. Dispatch performs no I/O or
/// mutation, and custom implementations must uphold the same purity contract.
pub fn verify(
    author: &Author<'_>,
    hash: &ChangeHash,
    signature: &Signature,
    custom: Option<&dyn Verifier>,
) -> Verification {
    match &signature.repr {
        SignatureRepr::Ed25519(signature) => {
            match verify_ed25519(author, &signing_message(hash), signature) {
                Ok(()) => Verification::Valid,
                Err(_) => Verification::Invalid,
            }
        }
        SignatureRepr::Opaque { tag, .. } if *tag == SignatureTag::ED25519 => Verification::Invalid,
        SignatureRepr::Opaque { tag, bytes } => match custom {
            Some(verifier) => verifier.verify(author, &signing_message(hash), *tag, bytes),
            None => Verification::Unsupported,
        },
    }
}

/// Verify a typed Ed25519 signature against the supplied author key and message.
///
/// The caller supplies the domain-separated signing message. Dalek's strict
/// verification rejects small-order public keys and small-order signature points.
///
/// # Errors
///
/// Returns an error if the author bytes are not a decodable 32-byte public key
/// or the signature fails strict verification. Scheme classification has already
/// happened, so unsupported schemes cannot reach this function.
fn verify_ed25519(
    author: &Author<'_>,
    message: &[u8],
    signature: &Ed25519Signature,
) -> Result<(), ed25519_dalek::SignatureError> {
    let key = ed25519_dalek::VerifyingKey::try_from(author.as_bytes())?;
    key.verify_strict(message, &signature.inner)
}

#[cfg(test)]
mod tests;
