//! Store signature entries and verification evidence by change hash.
//!
//! [`SignatureStore`] supports three steps:
//!
//! 1. Retain signature values with [`SignatureStore::insert_unchecked`].
//!    The expected author need not be known yet.
//! 2. Establish the expected author with [`SignatureStore::associate_author`],
//!    using a [`ResolvedChangeAuthor`] obtained from caller-supplied mappings.
//! 3. Verify stored Ed25519 entries with [`SignatureStore::verify_ed25519`].
//!    Valid signatures become evidence, invalid entries are discarded, and
//!    other schemes remain unchecked.
//!
//! Alternatively, [`SignatureStore::insert_ed25519`] accepts an existing
//! verification witness and retains its author, prepared key, and signature.
//!
//! Author associations remain fixed once established. Prepared Ed25519 keys
//! are retained for reuse, even when no signature entries remain.
//!
//! Each hash shares a limit of 16 distinct signatures across unchecked entries
//! and evidence. The store does not decide which change hashes or fragment
//! proofs should be retained.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;

use crate::{ActorId, Author, Change, ChangeHash};

use super::{ed25519, Signature, SignatureTag};

/// Resident per-hash budget, independent of the per-chunk wire bound.
const MAX_SIGNATURES_PER_HASH: usize = 16;

/// A change hash paired with its expected author.
///
/// Use [`ResolvedChangeAuthor::resolve`] to obtain the author from a
/// caller-supplied actor-to-author map, or
/// [`ResolvedChangeAuthor::from_ed25519`] to reuse a prepared Ed25519 key.
/// Pass the result to [`SignatureStore::associate_author`] to establish
/// the association without verifying signatures.
///
/// The caller must supply the correct actor-to-author association.
/// Neither a usable key nor a signature over the hash establishes that
/// association by itself.
#[derive(Debug)]
pub(crate) struct ResolvedChangeAuthor<'a> {
    change: ChangeHash,
    author: AuthorContext<'a>,
}

impl<'a> ResolvedChangeAuthor<'a> {
    /// Pair the change's hash with the author mapped to its actor.
    ///
    /// Uses [`Change::actor_id`] to select the expected author and
    /// [`Change::hash`] to identify the change. The author need not be an
    /// Ed25519 public key. No signature verification is performed.
    ///
    /// # Errors
    ///
    /// Returns [`AssociationError::MissingAuthor`] if the actor has no entry
    /// in `authors`. The change's author footer is not used as a fallback.
    pub(crate) fn resolve(
        change: &Change,
        authors: &BTreeMap<ActorId, Author<'a>>,
    ) -> Result<Self, AssociationError> {
        let author =
            authors
                .get(change.actor_id())
                .ok_or_else(|| AssociationError::MissingAuthor {
                    change: change.hash(),
                    actor: change.actor_id().clone(),
                })?;
        Ok(Self {
            change: change.hash(),
            author: AuthorContext::IdentityOnly(author.clone()),
        })
    }

    /// Pair the change's hash with its prepared Ed25519 verifying author.
    ///
    /// Uses [`ed25519::AuthorVerifier::verifying_author`] to select the author
    /// for the change's actor. Retains the prepared key without decoding it
    /// again. No signature verification is performed.
    ///
    /// # Errors
    ///
    /// Returns [`AssociationError::MissingAuthor`] if the verifier has no
    /// prepared author for the change's actor.
    pub(crate) fn from_ed25519(
        change: &Change,
        verifier: &ed25519::AuthorVerifier<'_, 'a>,
    ) -> Result<Self, AssociationError> {
        let author =
            verifier
                .verifying_author(change)
                .map_err(|_| AssociationError::MissingAuthor {
                    change: change.hash(),
                    actor: change.actor_id().clone(),
                })?;
        Ok(Self {
            change: change.hash(),
            author: AuthorContext::WithEd25519Key(author.clone()),
        })
    }
}

/// An expected author identity, optionally with a prepared Ed25519 key.
///
/// Both variants identify an author. A prepared key allows repeated
/// verification without decoding the author bytes again.
#[derive(Clone, Debug)]
enum AuthorContext<'a> {
    /// Author identity without a retained Ed25519 key.
    ///
    /// The bytes may still be usable as an Ed25519 key or by a custom verifier.
    IdentityOnly(Author<'a>),

    /// Author identity with the key prepared by [`ed25519::VerifyingAuthor::new`].
    WithEd25519Key(ed25519::VerifyingAuthor<'a>),
}

impl<'a> AuthorContext<'a> {
    fn author(&self) -> &Author<'a> {
        match self {
            Self::IdentityOnly(author) => author,
            Self::WithEd25519Key(author) => author.author(),
        }
    }

    fn into_owned(self) -> AuthorContext<'static> {
        match self {
            Self::IdentityOnly(author) => AuthorContext::IdentityOnly(author.into_owned()),
            Self::WithEd25519Key(author) => AuthorContext::WithEd25519Key(author.into_owned()),
        }
    }

    // Upgrade capability, never replace identity or discard an already prepared key.
    fn incorporate(&mut self, incoming: AuthorContext<'a>) {
        if matches!(self, Self::IdentityOnly(_)) && matches!(incoming, Self::WithEd25519Key(_)) {
            *self = incoming;
        }
    }
}

/// Signature entries and expected authors, indexed by change hash.
///
/// Use [`SignatureStore::insert_unchecked`] to store entries before their author
/// is known. [`SignatureStore::associate_author`] fixes the expected identity
/// without verifying any signatures. [`SignatureStore::verify_ed25519`] retains
/// successful signatures as evidence and removes invalid Ed25519 entries.
///
/// Each hash holds at most 16 distinct signatures across unchecked entries
/// and evidence. Resolved author associations remain even when no entries
/// remain.
#[derive(Clone, Debug, Default)]
pub(crate) struct SignatureStore {
    by_hash: BTreeMap<ChangeHash, ChangeSignatures>,
}

/// Signature entries for one change hash, grouped by author resolution.
///
/// Use [`ChangeSignatures::associate`] to establish the expected author
/// while preserving the stored entries.
#[derive(Clone, Debug)]
enum ChangeSignatures {
    /// Entries whose expected author has not yet been established.
    Unresolved { unchecked: Vec<Signature> },

    /// Entries associated with a fixed expected author, with no verified Ed25519 evidence yet.
    AuthorKnown {
        author: AuthorContext<'static>,
        unchecked: Vec<Signature>,
    },

    /// Entries associated with a fixed expected author and at least one verified Ed25519 signature.
    Verified(Box<VerifiedChangeSignatures>),
}

/// Guaranteed non-empty set of verified Ed25519 signatures for one change hash.
#[derive(Clone, Debug, PartialEq, Eq)]
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

    fn len(&self) -> usize {
        1 + self.rest.len()
    }

    fn contains(&self, signature: &ed25519::Signature) -> bool {
        &self.first == signature || self.rest.contains(signature)
    }

    fn push(&mut self, signature: ed25519::Signature) {
        self.rest.push(signature);
    }

    fn iter(&self) -> impl Iterator<Item = &ed25519::Signature> {
        std::iter::once(&self.first).chain(self.rest.iter())
    }
}

/// Signature entries associated with a fixed expected author and non-empty Ed25519 evidence.
#[derive(Clone, Debug)]
struct VerifiedChangeSignatures {
    author: AuthorContext<'static>,
    unchecked: Vec<Signature>,
    ed25519_evidence: Ed25519Evidence,
}

impl ChangeSignatures {
    fn unchecked(&self) -> &[Signature] {
        match self {
            Self::Unresolved { unchecked } | Self::AuthorKnown { unchecked, .. } => unchecked,
            Self::Verified(verified) => &verified.unchecked,
        }
    }

    fn author(&self) -> Option<&Author<'static>> {
        match self {
            Self::Unresolved { .. } => None,
            Self::AuthorKnown { author, .. } => Some(author.author()),
            Self::Verified(verified) => Some(verified.author.author()),
        }
    }

    /// Establish the expected author while preserving signature entries.
    #[must_use]
    fn associate(&mut self, incoming: AuthorContext<'_>) -> bool {
        match self {
            Self::Unresolved { unchecked } => {
                *self = Self::AuthorKnown {
                    author: incoming.into_owned(),
                    unchecked: std::mem::take(unchecked),
                };
            }
            Self::AuthorKnown { author, .. } => {
                if author.author() != incoming.author() {
                    return false;
                }
                if matches!(author, AuthorContext::IdentityOnly(_))
                    && matches!(incoming, AuthorContext::WithEd25519Key(_))
                {
                    author.incorporate(incoming.into_owned());
                }
            }
            Self::Verified(verified) => {
                if verified.author.author() != incoming.author() {
                    return false;
                }
                if matches!(verified.author, AuthorContext::IdentityOnly(_))
                    && matches!(incoming, AuthorContext::WithEd25519Key(_))
                {
                    verified.author.incorporate(incoming.into_owned());
                }
            }
        }
        true
    }

    fn insert_unchecked(&mut self, signature: Signature) -> Insert {
        let (unchecked, evidence_count) = match self {
            Self::Unresolved { unchecked } | Self::AuthorKnown { unchecked, .. } => (unchecked, 0),
            Self::Verified(verified) => {
                if signature
                    .as_ed25519()
                    .is_some_and(|s| verified.ed25519_evidence.contains(s))
                {
                    return Insert::Duplicate;
                }
                (&mut verified.unchecked, verified.ed25519_evidence.len())
            }
        };
        if unchecked.contains(&signature) {
            return Insert::Duplicate;
        }
        if unchecked.len() + evidence_count >= MAX_SIGNATURES_PER_HASH {
            return Insert::DroppedOverflow;
        }
        unchecked.push(signature);
        Insert::Stored
    }

    /// Retain an Ed25519 signature already verified for this record's author.
    ///
    /// Precondition: `self` must already have an established author (`AuthorKnown` or `Verified`).
    fn insert_verified_ed25519(&mut self, signature: ed25519::Signature) -> Insert {
        match self {
            Self::Unresolved { .. } => unreachable!("author must be established before evidence"),
            Self::AuthorKnown { author, unchecked } => {
                let outcome = if let Some(index) = unchecked
                    .iter()
                    .position(|entry| entry.as_ed25519() == Some(&signature))
                {
                    unchecked.swap_remove(index);
                    Insert::EstablishedEd25519Evidence
                } else if unchecked.len() >= MAX_SIGNATURES_PER_HASH {
                    let index = unchecked
                        .iter()
                        .enumerate()
                        .max_by(|(_, left), (_, right)| signature_order(left, right))
                        .map(|(index, _)| index)
                        .expect("a full unchecked group is nonempty");
                    unchecked.swap_remove(index);
                    Insert::ReplacedUnchecked
                } else {
                    Insert::Stored
                };
                let dummy = AuthorContext::IdentityOnly(Author::from(&[][..])).into_owned();
                let author = std::mem::replace(author, dummy);
                let unchecked = std::mem::take(unchecked);
                *self = Self::Verified(Box::new(VerifiedChangeSignatures {
                    author,
                    unchecked,
                    ed25519_evidence: Ed25519Evidence::new(signature),
                }));
                outcome
            }
            Self::Verified(verified) => verified.insert_ed25519(signature),
        }
    }

    fn verify_ed25519(
        &mut self,
        change: ChangeHash,
    ) -> Result<Ed25519VerificationReport, VerifyEd25519Error> {
        match self {
            Self::Unresolved { unchecked } => {
                if unchecked.iter().any(|s| s.tag() == SignatureTag::ED25519) {
                    Err(VerifyEd25519Error::UnresolvedAuthor { change })
                } else {
                    Ok(Ed25519VerificationReport {
                        skipped_other_schemes: unchecked.len(),
                        ..Ed25519VerificationReport::default()
                    })
                }
            }
            Self::AuthorKnown { author, unchecked } => {
                let skipped = unchecked
                    .iter()
                    .filter(|s| s.tag() != SignatureTag::ED25519)
                    .count();
                let mut report = Ed25519VerificationReport {
                    skipped_other_schemes: skipped,
                    ..Ed25519VerificationReport::default()
                };
                if skipped == unchecked.len() {
                    return Ok(report);
                }
                let prepared = match &*author {
                    AuthorContext::IdentityOnly(a) => {
                        Cow::Owned(ed25519::VerifyingAuthor::new(a.clone()).map_err(|source| {
                            VerifyEd25519Error::InvalidAuthorKey { change, source }
                        })?)
                    }
                    AuthorContext::WithEd25519Key(prepared) => Cow::Borrowed(prepared),
                };
                let mut established: Vec<ed25519::Signature> = Vec::new();
                unchecked.retain(|entry| {
                    if entry.tag() != SignatureTag::ED25519 {
                        return true;
                    }
                    match entry.as_ed25519() {
                        Some(sig) if prepared.verify_hash(&change, sig).is_ok() => {
                            established.push(*sig);
                            report.established_ed25519_evidence += 1;
                        }
                        _ => report.discarded_invalid += 1,
                    }
                    false
                });
                let next_author = match prepared {
                    Cow::Owned(p) => AuthorContext::WithEd25519Key(p),
                    Cow::Borrowed(_) => {
                        let dummy = AuthorContext::IdentityOnly(Author::from(&[][..])).into_owned();
                        std::mem::replace(author, dummy)
                    }
                };
                let mut iter = established.into_iter();
                if let Some(first) = iter.next() {
                    let mut ed25519_evidence = Ed25519Evidence::new(first);
                    ed25519_evidence.rest.extend(iter);
                    let unchecked = std::mem::take(unchecked);
                    *self = Self::Verified(Box::new(VerifiedChangeSignatures {
                        author: next_author,
                        unchecked,
                        ed25519_evidence,
                    }));
                } else {
                    *author = next_author;
                }
                Ok(report)
            }
            Self::Verified(verified) => verified.verify_ed25519(change),
        }
    }
}

impl VerifiedChangeSignatures {
    /// Retain an additional Ed25519 signature for an already-verified record.
    fn insert_ed25519(&mut self, signature: ed25519::Signature) -> Insert {
        if self.ed25519_evidence.contains(&signature) {
            return Insert::Duplicate;
        }
        if let Some(index) = self
            .unchecked
            .iter()
            .position(|entry| entry.as_ed25519() == Some(&signature))
        {
            self.unchecked.swap_remove(index);
            self.ed25519_evidence.push(signature);
            Insert::EstablishedEd25519Evidence
        } else if self.unchecked.len() + self.ed25519_evidence.len() >= MAX_SIGNATURES_PER_HASH {
            Insert::DroppedOverflow
        } else {
            self.ed25519_evidence.push(signature);
            Insert::Stored
        }
    }

    fn verify_ed25519(
        &mut self,
        change: ChangeHash,
    ) -> Result<Ed25519VerificationReport, VerifyEd25519Error> {
        let skipped = self
            .unchecked
            .iter()
            .filter(|s| s.tag() != SignatureTag::ED25519)
            .count();
        let mut report = Ed25519VerificationReport {
            skipped_other_schemes: skipped,
            ..Ed25519VerificationReport::default()
        };
        if skipped == self.unchecked.len() {
            return Ok(report);
        }

        let author = match &self.author {
            AuthorContext::IdentityOnly(author) => {
                let prepared = ed25519::VerifyingAuthor::new(author.clone())
                    .map_err(|source| VerifyEd25519Error::InvalidAuthorKey { change, source })?;
                Cow::Owned(prepared)
            }
            AuthorContext::WithEd25519Key(author) => Cow::Borrowed(author),
        };
        self.unchecked.retain(|entry| {
            if entry.tag() != SignatureTag::ED25519 {
                return true;
            }
            match entry.as_ed25519() {
                Some(signature) if author.verify_hash(&change, signature).is_ok() => {
                    self.ed25519_evidence.push(*signature);
                    report.established_ed25519_evidence += 1;
                }
                _ => report.discarded_invalid += 1,
            }
            false
        });
        if let Cow::Owned(prepared) = author {
            self.author = AuthorContext::WithEd25519Key(prepared);
        }
        Ok(report)
    }
}

// Capacity eviction uses raw (tag, bytes) order, never representation order.
fn signature_order(left: &Signature, right: &Signature) -> Ordering {
    left.tag()
        .cmp(&right.tag())
        .then_with(|| left.with_bytes(|left| right.with_bytes(|right| left.cmp(right))))
}

/// The effect of inserting a signature entry or Ed25519 evidence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Insert {
    /// A new signature was retained with the supplied evidence, if any.
    Stored,
    /// The signature already has at least the supplied evidence.
    Duplicate,
    /// A retained unchecked entry gained Ed25519 evidence without another slot.
    EstablishedEd25519Evidence,
    /// First evidence displaced the largest unchecked entry at capacity.
    ReplacedUnchecked,
    /// The per-hash budget was full, so the new signature was not retained.
    DroppedOverflow,
}

/// Failure to resolve or retain a change's expected author.
///
/// Returned by [`ResolvedChangeAuthor::resolve`],
/// [`ResolvedChangeAuthor::from_ed25519`], or
/// [`SignatureStore::associate_author`].
/// A failed association leaves the store unchanged.
#[derive(Debug, thiserror::Error)]
pub(crate) enum AssociationError {
    /// The actor has no expected author in the supplied mapping.
    #[error("no resolved author for actor {actor} of change {change}")]
    MissingAuthor { change: ChangeHash, actor: ActorId },
    /// The resolved identity disagrees with the record's fixed author.
    #[error("change {change} already has a different expected author")]
    AuthorConflict { change: ChangeHash },
}

/// Failure to insert evidence through [`SignatureStore::insert_ed25519`].
///
/// The witness's author conflicts with the change's expected author.
/// The store is left unchanged.
#[derive(Debug, thiserror::Error)]
pub enum InsertError {
    /// Evidence disagrees with the expected author, even if no evidence exists yet.
    #[error("Ed25519 evidence for change {change} has a conflicting author")]
    AuthorConflict { change: ChangeHash },
}

/// Counts returned by [`SignatureStore::verify_ed25519`] for one change hash.
///
/// Counts newly established evidence, discarded invalid Ed25519 entries,
/// and entries skipped because they use another scheme. Existing evidence
/// is not counted.
///
/// Skipped entries remain unchecked and are counted again on subsequent
/// calls. Skipping a scheme does not imply that a custom verifier would
/// reject or fail to support it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Ed25519VerificationReport {
    /// Previously unchecked entries whose Ed25519 signatures passed verification.
    pub(crate) established_ed25519_evidence: usize,
    /// Malformed or invalid Ed25519 entries removed under the resolved key.
    pub(crate) discarded_invalid: usize,
    /// Entries left unchecked without invoking any custom verifier.
    pub(crate) skipped_other_schemes: usize,
}

/// Failure to establish the context needed for Ed25519 verification.
///
/// Returned by [`SignatureStore::verify_ed25519`] when Ed25519 entries
/// exist but their expected author is unresolved or cannot supply a usable
/// key. The store is left unchanged.
#[derive(Debug, thiserror::Error)]
pub(crate) enum VerifyEd25519Error {
    /// This hash has Ed25519 candidates but no established author association.
    #[error("the expected author for change {change} has not been resolved")]
    UnresolvedAuthor { change: ChangeHash },
    /// Author identity is known but cannot currently supply a usable native key.
    #[error("the expected author for change {change} has no usable Ed25519 key: {source}")]
    InvalidAuthorKey {
        change: ChangeHash,
        #[source]
        source: ed25519::error::VerifyingAuthor,
    },
}

impl SignatureStore {
    /// Associate a change hash with its expected author.
    ///
    /// Accepts an association constructed by [`ResolvedChangeAuthor::resolve`]
    /// or [`ResolvedChangeAuthor::from_ed25519`]. Existing signature entries
    /// are preserved. If the hash is absent, creates an empty resolved record.
    ///
    /// Repeating the same author is permitted. A supplied prepared key is
    /// retained without replacing an existing prepared key. No signature
    /// verification is performed.
    ///
    /// # Errors
    ///
    /// Returns [`AssociationError::AuthorConflict`] if the hash is already
    /// associated with a different author, leaving the store unchanged.
    pub(crate) fn associate_author(
        &mut self,
        association: ResolvedChangeAuthor<'_>,
    ) -> Result<(), AssociationError> {
        let ResolvedChangeAuthor { change, author } = association;
        match self.by_hash.entry(change) {
            Entry::Vacant(entry) => {
                entry.insert(ChangeSignatures::AuthorKnown {
                    author: author.into_owned(),
                    unchecked: Vec::new(),
                });
                Ok(())
            }
            Entry::Occupied(mut entry) => {
                if entry.get_mut().associate(author) {
                    Ok(())
                } else {
                    Err(AssociationError::AuthorConflict { change })
                }
            }
        }
    }

    /// Borrow the expected author associated with `hash`.
    ///
    /// Returns `None` if the hash is absent or its author is unresolved.
    /// A resolved author is returned even when no signature entries remain.
    pub(crate) fn author(&self, hash: &ChangeHash) -> Option<&Author<'static>> {
        self.by_hash.get(hash)?.author()
    }

    /// Verify unchecked Ed25519 signatures for `hash`.
    pub(crate) fn verify_ed25519(
        &mut self,
        hash: &ChangeHash,
    ) -> Result<Ed25519VerificationReport, VerifyEd25519Error> {
        match self.by_hash.get_mut(hash) {
            None => Ok(Ed25519VerificationReport::default()),
            Some(record) => record.verify_ed25519(*hash),
        }
    }

    /// Retain a signature for `change` without verifying it.
    pub(crate) fn insert_unchecked(&mut self, change: ChangeHash, signature: Signature) -> Insert {
        match self.by_hash.entry(change) {
            Entry::Vacant(entry) => {
                entry.insert(ChangeSignatures::Unresolved {
                    unchecked: vec![signature],
                });
                Insert::Stored
            }
            Entry::Occupied(mut entry) => entry.get_mut().insert_unchecked(signature),
        }
    }

    /// Retain the evidence carried by an [`ed25519::VerifiedSignature`].
    pub(crate) fn insert_ed25519(
        &mut self,
        witness: ed25519::VerifiedSignature<'_, '_>,
    ) -> Result<Insert, InsertError> {
        let change = *witness.hash();
        let prepared =
            AuthorContext::WithEd25519Key(witness.verifying_author().clone().into_owned());
        match self.by_hash.entry(change) {
            Entry::Vacant(entry) => {
                entry.insert(ChangeSignatures::Verified(Box::new(
                    VerifiedChangeSignatures {
                        author: prepared,
                        unchecked: Vec::new(),
                        ed25519_evidence: Ed25519Evidence::new(*witness.signature()),
                    },
                )));
                Ok(Insert::Stored)
            }
            Entry::Occupied(mut entry) => {
                if !entry.get_mut().associate(prepared) {
                    return Err(InsertError::AuthorConflict { change });
                }
                Ok(entry
                    .get_mut()
                    .insert_verified_ed25519(*witness.signature()))
            }
        }
    }

    /// Borrow the established Ed25519 signatures and their expected author.
    pub(crate) fn ed25519_entries(
        &self,
        hash: &ChangeHash,
    ) -> impl Iterator<Item = (&Author<'static>, &ed25519::Signature)> {
        self.by_hash
            .get(hash)
            .into_iter()
            .filter_map(|group| match group {
                ChangeSignatures::Verified(verified) => Some(verified),
                ChangeSignatures::Unresolved { .. } | ChangeSignatures::AuthorKnown { .. } => None,
            })
            .flat_map(|verified| {
                verified
                    .ed25519_evidence
                    .iter()
                    .map(|signature| (verified.author.author(), signature))
            })
    }

    /// Borrow the unchecked signatures retained for `hash`.
    pub(crate) fn unchecked_entries(&self, hash: &ChangeHash) -> impl Iterator<Item = &Signature> {
        self.by_hash
            .get(hash)
            .into_iter()
            .flat_map(|group| group.unchecked().iter())
    }

    /// Borrow hashes that currently hold at least one verified Ed25519 signature.
    pub(crate) fn verified_hashes(&self) -> impl Iterator<Item = &ChangeHash> {
        self.by_hash.iter().filter_map(|(hash, group)| {
            matches!(group, ChangeSignatures::Verified(_)).then_some(hash)
        })
    }

    /// Copy verified Ed25519 evidence and prepared author keys from `other`
    /// without repeating cryptographic verification.
    pub(crate) fn merge_verified(&mut self, other: &Self) -> Result<(), InsertError> {
        for (change, record) in &other.by_hash {
            let ChangeSignatures::Verified(src) = record else {
                continue;
            };
            if let Some(dst_author) = self.author(change) {
                if dst_author != src.author.author() {
                    return Err(InsertError::AuthorConflict { change: *change });
                }
            }
        }

        for (&change, record) in &other.by_hash {
            let ChangeSignatures::Verified(src) = record else {
                continue;
            };
            match self.by_hash.entry(change) {
                Entry::Vacant(entry) => {
                    entry.insert(ChangeSignatures::Verified(Box::new(
                        VerifiedChangeSignatures {
                            author: src.author.clone(),
                            unchecked: Vec::new(),
                            ed25519_evidence: src.ed25519_evidence.clone(),
                        },
                    )));
                }
                Entry::Occupied(mut entry) => {
                    let existing = entry.get_mut();
                    let ok = existing.associate(src.author.clone());
                    debug_assert!(ok, "author conflict checked before mutation");
                    for sig in src.ed25519_evidence.iter() {
                        existing.insert_verified_ed25519(*sig);
                    }
                }
            }
        }
        Ok(())
    }

    /// Retain only records whose change hashes pass `keep`.
    pub(crate) fn retain_hashes(&mut self, mut keep: impl FnMut(&ChangeHash) -> bool) {
        self.by_hash.retain(|hash, _| keep(hash));
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.by_hash.len()
    }
}

#[cfg(test)]
mod tests;
