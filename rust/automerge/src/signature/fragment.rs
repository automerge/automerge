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

use std::collections::{BTreeMap, HashMap};

use crate::storage::signatures::{HashEntries, MAX_SIGNATURES_PER_HASH};
use crate::{Author, Change, ChangeHash};

use super::ed25519;
use super::error::ProofError;
use super::store::SignatureStore;

mod frontiers;
mod provisional;
mod scoped;
mod scoped_cache;

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
        Self::verify_hashes(&self.hashes, attached, |hash| author_for_hash(&hash))
    }

    fn verify_hashes<'a>(
        hashes: &[ChangeHash],
        attached: &HashEntries,
        author_for_hash: impl Fn(ChangeHash) -> Option<Author<'a>>,
    ) -> Result<SignatureStore, ProofError> {
        scoped::fresh(
            hashes,
            attached,
            SignatureStore::default(),
            scoped::Operations {
                entries: attached_bucket,
                author: author_for_hash,
                decode: ed25519::VerifyingAuthor::new,
                check: check_entry,
                retain: retain_witness,
            },
        )
        .map_err(proof_error)
    }
}

/// The preverified store cannot be detached from its own frontier list or
/// raw attachment. Only the preparation path can construct this scope.
#[derive(Clone, Debug)]
pub(crate) struct IncomingProof(scoped_cache::Owned<ChangeHash, HashEntries, SignatureStore>);

impl IncomingProof {
    /// Check attached evidence under provisional identities, sharing only
    /// decoded keys with other deliveries in this call.
    pub(crate) fn prepare(
        requirements: FragmentRequirements,
        attachment: HashEntries,
        prepared_keys: &mut HashMap<Author<'static>, ed25519::VerifyingAuthor<'static>>,
        tentative: impl Fn(&ChangeHash) -> Option<Option<Author<'static>>>,
    ) -> Result<Self, ProofError> {
        scoped_cache::Owned::prepare(requirements.hashes, attachment, |hashes, attachment| {
            provisional::verify(
                hashes,
                attachment,
                SignatureStore::default(),
                prepared_keys,
                scoped::Operations {
                    entries: attached_bucket,
                    author: |hash| tentative(&hash),
                    decode: ed25519::VerifyingAuthor::new,
                    check: check_entry,
                    retain: retain_witness,
                },
            )
            .map_err(proof_error)
        })
        .map(Self)
    }

    pub(crate) fn attachment(&self) -> &HashEntries {
        &self.0.attachment
    }

    pub(crate) fn hashes(&self) -> &[ChangeHash] {
        &self.0.hashes
    }

    /// Settle this delivery's provisional proof against its established authors.
    pub(crate) fn settle<'a>(
        &self,
        author_for_hash: impl Fn(&ChangeHash) -> Option<Author<'a>>,
    ) -> Result<SignatureStore, ProofError> {
        scoped_cache::settle(
            self.0.scope(),
            SignatureStore::default(),
            scoped::Operations {
                entries: attached_bucket,
                author: |hash| author_for_hash(&hash),
                decode: ed25519::VerifyingAuthor::new,
                check: check_entry,
                retain: retain_witness,
            },
            |store: &SignatureStore, hash| {
                store
                    .ed25519_entries(&hash)
                    .next()
                    .map(|(author, _)| author)
            },
            SignatureStore::clone,
        )
        .map_err(proof_error)
    }
}

fn check_entry<'a, 's>(
    key: &ed25519::VerifyingAuthor<'a>,
    hash: ChangeHash,
    signature: &'s ed25519::Signature,
) -> Option<ed25519::VerifiedSignature<'a, 's>> {
    key.verify_hash(&hash, signature).ok()
}

fn retain_witness(
    mut store: SignatureStore,
    witness: ed25519::VerifiedSignature<'_, '_>,
) -> SignatureStore {
    store
        .insert_ed25519(witness)
        .expect("each frontier has one fixed author and is inserted once");
    store
}

fn proof_error(
    failure: scoped::Failure<ChangeHash, ed25519::error::VerifyingAuthor>,
) -> ProofError {
    match failure {
        scoped::Failure::MissingSignature(hash) => ProofError::MissingSignature(hash),
        scoped::Failure::MissingAuthor(hash) => ProofError::MissingAuthor(hash),
        scoped::Failure::InvalidKey(hash, source) => ProofError::InvalidAuthorKey { hash, source },
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
fn verify_prepared_frontier<'a, 's>(
    attached: &'s HashEntries,
    hash: &ChangeHash,
    verifier: &ed25519::VerifyingAuthor<'a>,
) -> Result<ed25519::VerifiedSignature<'a, 's>, ProofError> {
    scoped::search(
        &attached_bucket(attached, *hash),
        verifier,
        *hash,
        &|key: &ed25519::VerifyingAuthor<'a>, hash, signature| {
            key.verify_hash(&hash, signature).ok()
        },
    )
    .ok_or(ProofError::MissingSignature(*hash))
}

fn attached_bucket(
    attached: &HashEntries,
    hash: ChangeHash,
) -> scoped::Bucket<&ed25519::Signature, MAX_SIGNATURES_PER_HASH> {
    // Parsing and construction bound the total raw entries per hash. Keep
    // typed entries on the stack, retaining raw presence for error ordering.
    let mut bucket = scoped::Bucket {
        present: false,
        entries: [None; MAX_SIGNATURES_PER_HASH],
        len: 0,
    };
    for entry in attached.for_hash(&hash) {
        bucket.present = true;
        if let Some(signature) = entry.as_ed25519() {
            bucket.entries[bucket.len] = Some(signature);
            bucket.len += 1;
        }
    }
    bucket
}

#[cfg(test)]
mod tests;
