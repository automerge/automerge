//! Independent proof requirements for a structurally validated fragment.
//!
//! Each actor's latest included change must have its own signature. Earlier
//! included changes of that actor are attested through the validated actor chain.
//! Neither a shared author nor a signed descendant outside the fragment can
//! substitute for one of these signatures. Causal admission is a separate step.
//!
//! Incoming evidence comes only from the signature entries attached to this
//! fragment's content chunk. Resident evidence can construct outgoing proofs,
//! but must not repair an incomplete incoming proof.

use std::collections::BTreeMap;

use crate::storage::signatures::HashEntries;
use crate::{Author, ChangeHash};

use super::ed25519;
use super::error::ProofError;
use super::store::SignatureStore;

/// Fixed proof obligations, independent of available signatures and other groups.
///
/// Construction assumes canonical membership, valid actor chains, and established
/// actor–author bindings. This type does not validate fragment structure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FragmentRequirements {
    hashes: Vec<ChangeHash>,
}

impl FragmentRequirements {
    /// Derive the maximum included sequence for each actor, in any member order.
    ///
    /// Actor keys may be IDs for incoming changes or table indices for an applied
    /// graph. They must identify actors, not authors. Boundaries and their ancestry
    /// must already have been excluded from the supplied membership.
    pub(crate) fn for_members<A: Ord>(
        members: impl IntoIterator<Item = (A, u64, ChangeHash)>,
    ) -> Self {
        let mut frontiers = BTreeMap::<A, (u64, ChangeHash)>::new();
        for (actor, seq, hash) in members {
            let frontier = frontiers.entry(actor).or_insert((seq, hash));
            if seq > frontier.0 {
                *frontier = (seq, hash);
            }
        }
        let mut hashes: Vec<_> = frontiers.into_values().map(|(_, hash)| hash).collect();
        hashes.sort_unstable();
        Self { hashes }
    }

    /// Exact hashes whose signatures must accompany an outgoing fragment.
    pub(crate) fn hashes(&self) -> impl ExactSizeIterator<Item = &ChangeHash> {
        self.hashes.iter()
    }

    /// Query retained evidence without changing these complete requirements.
    pub(crate) fn is_satisfied_by(&self, has_signature: impl FnMut(&ChangeHash) -> bool) -> bool {
        self.hashes().all(has_signature)
    }

    /// Verify only this group's attached entries and retain its required proofs.
    ///
    /// `author_for_hash` must resolve established author bindings for this
    /// fragment's members, never infer them from a signature or delivery source.
    /// No document-wide store is consulted. Extra entries, unsupported schemes,
    /// and invalid signatures cannot satisfy a missing obligation. One valid
    /// Ed25519 signature per required hash is sufficient.
    ///
    /// The returned store owns the verified evidence for subsequent retention.
    /// Failure publishes no partial evidence.
    #[allow(dead_code)]
    pub(crate) fn verify_entries<'a>(
        &self,
        attached: &HashEntries,
        mut author_for_hash: impl FnMut(&ChangeHash) -> Option<Author<'a>>,
    ) -> Result<SignatureStore, ProofError> {
        let mut verified = SignatureStore::default();
        for hash in self.hashes() {
            if attached.for_hash(hash).next().is_none() {
                return Err(ProofError::MissingSignature(*hash));
            }
            let author = author_for_hash(hash).ok_or(ProofError::MissingAuthor(*hash))?;
            let witness = verify_frontier_entry(attached, hash, author)?;
            verified
                .insert_ed25519(witness)
                .expect("each frontier has one fixed author and is inserted once");
        }
        Ok(verified)
    }
}

/// Verify one required frontier hash against `attached` using an established `author`.
pub(crate) fn verify_frontier_entry<'a, 's>(
    attached: &'s HashEntries,
    hash: &ChangeHash,
    author: Author<'a>,
) -> Result<ed25519::VerifiedSignature<'a, 's>, ProofError> {
    let verifier =
        ed25519::VerifyingAuthor::new(author).map_err(|source| ProofError::InvalidAuthorKey {
            hash: *hash,
            source,
        })?;
    attached
        .for_hash(hash)
        .filter_map(|entry| entry.as_ed25519())
        .find_map(|signature| verifier.verify_hash(hash, signature).ok())
        .ok_or(ProofError::MissingSignature(*hash))
}

#[cfg(test)]
mod tests;
