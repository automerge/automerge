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

use crate::storage::signatures::{HashEntries, MAX_SIGNATURES_PER_HASH};
use crate::{Author, Change, ChangeHash};

use super::ed25519;
use super::error::ProofError;
use super::store::SignatureStore;

mod evidence;
mod frontiers;

pub(crate) use evidence::assemble_frontiers;

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
        let mut frontiers = BTreeMap::new();
        for (actor, seq, hash) in members {
            frontiers::offer(&mut frontiers, actor, seq, hash);
        }
        Self::from_frontiers(frontiers)
    }

    /// Compute incoming frontiers from precisely this group's delivery.
    pub(crate) fn for_changes(changes: &[Change]) -> Self {
        let frontiers = frontiers::pick(changes, |change| {
            (change.actor_id(), change.seq(), change.hash())
        });
        Self::from_frontiers(frontiers)
    }

    fn from_frontiers<A>(frontiers: BTreeMap<A, (u64, ChangeHash)>) -> Self {
        let mut hashes: Vec<_> = frontiers.into_values().map(|(_, hash)| hash).collect();
        hashes.sort_unstable();
        Self { hashes }
    }

    /// Exact hashes whose signatures must accompany an outgoing fragment.
    pub(crate) fn hashes(&self) -> std::slice::Iter<'_, ChangeHash> {
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
    pub(crate) fn verify_entries<'a>(
        &self,
        attached: &HashEntries,
        author_for_hash: impl Fn(&ChangeHash) -> Option<Author<'a>>,
    ) -> Result<SignatureStore, ProofError> {
        assemble_frontiers(
            &self.hashes,
            SignatureStore::default(),
            |hash, mut verified| {
                if attached.for_hash(&hash).next().is_none() {
                    return Err(ProofError::MissingSignature(hash));
                }
                let author = author_for_hash(&hash).ok_or(ProofError::MissingAuthor(hash))?;
                let witness = verify_frontier_entry(attached, &hash, author)?;
                verified
                    .insert_ed25519(witness)
                    .expect("each frontier has one fixed author and is inserted once");
                Ok(verified)
            },
        )
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
    verify_prepared_frontier(attached, hash, &verifier)
}

/// Use the same exact-hash attachment search with an already decoded key.
pub(crate) fn verify_prepared_frontier<'a, 's>(
    attached: &'s HashEntries,
    hash: &ChangeHash,
    verifier: &ed25519::VerifyingAuthor<'a>,
) -> Result<ed25519::VerifiedSignature<'a, 's>, ProofError> {
    // HashEntries enforces this per-hash bound on construction and parsing.
    // Use a stack slice so the checked search needs no new heap allocation.
    let mut entries = [None; MAX_SIGNATURES_PER_HASH];
    let mut len = 0;
    for entry in attached
        .for_hash(hash)
        .filter_map(|entry| entry.as_ed25519())
    {
        entries[len] = Some(entry);
        len += 1;
    }
    evidence::first_witness(&entries[..len], |entry| {
        verifier.verify_hash(hash, entry?).ok()
    })
    .ok_or(ProofError::MissingSignature(*hash))
}

#[cfg(test)]
mod tests;
