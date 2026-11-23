//! Locally committed unpublished changes and owned signing messages.
//!
//! Only local commits insert drafts. Signature receipt leaves them outstanding
//! until publication removes them. Clones copy provenance independently; imports
//! never establish it. Requests contain no author: the application routes this
//! document's messages to its fixed author's signer.

use crate::signature::{signing_message, SIGNING_MESSAGE_LEN};
use crate::ChangeHash;

/// Exact unpublished local commit hashes, in commit order.
#[derive(Clone, Debug, Default)]
pub(super) struct LocalDrafts(Vec<ChangeHash>);

impl LocalDrafts {
    pub(super) fn insert(&mut self, hash: ChangeHash) {
        self.0.push(hash);
    }

    pub(super) fn remove(&mut self, mut published: impl FnMut(&ChangeHash) -> bool) {
        self.0.retain(|hash| !published(hash));
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = ChangeHash> + '_ {
        self.0.iter().copied()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Exact message to sign for a locally committed change.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SigningRequest {
    hash: ChangeHash,
    message: [u8; SIGNING_MESSAGE_LEN],
}

impl SigningRequest {
    /// The change identified by this request.
    pub fn hash(&self) -> ChangeHash {
        self.hash
    }

    /// Domain-separated message bytes for the external signer.
    pub fn message(&self) -> &[u8] {
        &self.message
    }
}

/// Owned, deduplicated signing work, ordered by change hash.
///
/// Empty requests do not imply publication: signed local work may still be
/// blocked. A later edit does not invalidate a delayed response for these hashes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SigningRequests(Vec<SigningRequest>);

impl SigningRequests {
    pub(super) fn for_hashes(hashes: impl IntoIterator<Item = ChangeHash>) -> Self {
        let mut hashes: Vec<_> = hashes.into_iter().collect();
        hashes.sort_unstable();
        hashes.dedup();
        Self(
            hashes
                .into_iter()
                .map(|hash| SigningRequest {
                    hash,
                    message: signing_message(&hash),
                })
                .collect(),
        )
    }

    /// Borrow requests in hash order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &SigningRequest> {
        self.0.iter()
    }
    /// Number of distinct signatures requested.
    pub fn len(&self) -> usize {
        self.0.len()
    }
    /// Whether no additional signatures are currently requested.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl IntoIterator for SigningRequests {
    type Item = SigningRequest;
    type IntoIter = std::vec::IntoIter<SigningRequest>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a SigningRequests {
    type Item = &'a SigningRequest;
    type IntoIter = std::slice::Iter<'a, SigningRequest>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}
