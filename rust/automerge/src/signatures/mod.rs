//! Detached signatures over change hashes.
//!
//! A signature is an annotation on a change hash, stored beside the change
//! rather than inside it, so attaching one never alters the change or its
//! hash. Signatures are retained and relayed whether or not Automerge can
//! interpret them.

mod store;

pub(crate) use store::SignatureStore;

use crate::ChangeHash;

/// A detached signature, paired with the change hash it annotates.
///
/// The hash is not derived from the signature and the signature is not part of
/// the hash: this pairing is the whole of the relationship between a signature
/// and what it signs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Signature {
    pub(crate) change_hash: ChangeHash,
    pub(crate) bytes: Vec<u8>,
}

/// A [`Signature`] borrowed from the document holding it.
///
/// Ordered by change hash, then signature bytes, for canonical encoding.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct SignatureRef<'a> {
    pub(crate) change_hash: ChangeHash,
    pub(crate) bytes: &'a [u8],
}

impl SignatureRef<'_> {
    pub(crate) fn to_owned(self) -> Signature {
        Signature {
            change_hash: self.change_hash,
            bytes: self.bytes.to_vec(),
        }
    }
}

/// The first byte of every signature names its scheme.
const ED25519_TAG: u8 = 0;
const CUSTOM_TAG: u8 = 1;

/// A signature to attach to a change.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignatureInput {
    change_hash: ChangeHash,
    bytes: Vec<u8>,
}

impl SignatureInput {
    /// An Ed25519 signature over the raw bytes of `change_hash`, made with the
    /// key whose public half is the author of the change's actor.
    pub fn ed25519(change_hash: ChangeHash, signature: [u8; 64]) -> Self {
        Self::tagged(change_hash, ED25519_TAG, &signature)
    }

    /// Application-defined bytes which Automerge retains and relays but never
    /// uses for authentication.
    pub fn custom(change_hash: ChangeHash, bytes: impl AsRef<[u8]>) -> Self {
        Self::tagged(change_hash, CUSTOM_TAG, bytes.as_ref())
    }

    fn tagged(change_hash: ChangeHash, tag: u8, payload: &[u8]) -> Self {
        let mut bytes = Vec::with_capacity(payload.len() + 1);
        bytes.push(tag);
        bytes.extend_from_slice(payload);
        Self { change_hash, bytes }
    }

    pub fn change_hash(&self) -> ChangeHash {
        self.change_hash
    }

    pub(crate) fn into_signature(self) -> Signature {
        Signature {
            change_hash: self.change_hash,
            bytes: self.bytes,
        }
    }
}
