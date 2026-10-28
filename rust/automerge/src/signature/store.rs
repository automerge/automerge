//! Bounded verified Ed25519 evidence, indexed by change hash.
//!
//! Insert a verification witness or merge another store's evidence. A resident
//! hash has one fixed author and at least one signature; conflicting authors are
//! rejected without mutation. Each hash retains at most 16 distinct signatures.
//! Raw, unresolved and unsupported signatures belong
//! to incoming groups, not this store. Publication decides which hashes to retain.

use std::collections::btree_map::Entry;
use std::collections::BTreeMap;

use crate::{Author, ChangeHash};

use super::ed25519;

/// Resident per-hash budget, independent of the per-chunk wire bound.
const MAX_SIGNATURES_PER_HASH: usize = 16;

/// Verified evidence only. Every resident record is nonempty and author-bound.
#[derive(Clone, Debug, Default)]
pub(crate) struct SignatureStore {
    by_hash: BTreeMap<ChangeHash, VerifiedChangeSignatures>,
}

#[derive(Clone, Debug)]
struct VerifiedChangeSignatures {
    author: Author<'static>,
    signatures: Ed25519Evidence,
}

/// Nonempty, bounded collection of distinct verified signatures.
#[derive(Clone, Debug)]
struct Ed25519Evidence {
    first: ed25519::Signature,
    rest: Vec<ed25519::Signature>,
}

impl Ed25519Evidence {
    fn new(first: ed25519::Signature) -> Self {
        Self {
            first,
            rest: Vec::new(),
        }
    }

    fn iter(&self) -> impl Iterator<Item = &ed25519::Signature> {
        std::iter::once(&self.first).chain(self.rest.iter())
    }

    fn insert(&mut self, signature: ed25519::Signature) -> Insert {
        if self.iter().any(|existing| *existing == signature) {
            Insert::Duplicate
        } else if 1 + self.rest.len() >= MAX_SIGNATURES_PER_HASH {
            Insert::DroppedOverflow
        } else {
            self.rest.push(signature);
            Insert::Stored
        }
    }
}

/// The effect of inserting a verified signature.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Insert {
    /// A new verified signature was retained.
    Stored,
    /// The signature is already retained for this author and hash.
    Duplicate,
    /// The per-hash budget was full; existing evidence is unchanged.
    DroppedOverflow,
}

/// The witness's author conflicts with the resident record's fixed author.
/// The store is left unchanged.
#[derive(Debug, thiserror::Error)]
pub enum InsertError {
    #[error("Ed25519 evidence for change {change} has a conflicting author")]
    AuthorConflict { change: ChangeHash },
}

impl SignatureStore {
    /// Retain verified evidence, rejecting an author conflict before insertion.
    pub(crate) fn insert_ed25519(
        &mut self,
        witness: ed25519::VerifiedSignature<'_, '_>,
    ) -> Result<Insert, InsertError> {
        let hash = *witness.hash();
        match self.by_hash.entry(hash) {
            Entry::Vacant(entry) => {
                entry.insert(VerifiedChangeSignatures {
                    author: witness.author().clone().into_owned(),
                    signatures: Ed25519Evidence::new(*witness.signature()),
                });
                Ok(Insert::Stored)
            }
            Entry::Occupied(mut entry) => {
                if &entry.get().author != witness.author() {
                    return Err(InsertError::AuthorConflict { change: hash });
                }
                Ok(entry.get_mut().signatures.insert(*witness.signature()))
            }
        }
    }

    /// Borrow the retained author/signature pairs for an exact hash.
    pub(crate) fn ed25519_entries(
        &self,
        hash: &ChangeHash,
    ) -> impl Iterator<Item = (&Author<'static>, &ed25519::Signature)> {
        self.by_hash.get(hash).into_iter().flat_map(|record| {
            record
                .signatures
                .iter()
                .map(|signature| (&record.author, signature))
        })
    }

    /// Borrow hashes holding verified evidence.
    pub(crate) fn verified_hashes(&self) -> impl Iterator<Item = &ChangeHash> {
        self.by_hash.keys()
    }

    /// Retain only records whose change hashes pass `keep`.
    pub(crate) fn retain_hashes(&mut self, mut keep: impl FnMut(&ChangeHash) -> bool) {
        self.by_hash.retain(|hash, _| keep(hash));
    }
}

#[cfg(test)]
mod tests;
