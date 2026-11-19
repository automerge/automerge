//! Local draft provenance and signing request planning.
//!
//! [`LocalDrafts`] records the exact hashes of changes committed locally on
//! this document that are not yet published. Only a successful local commit
//! adds an entry; imports, loads and clones never do. Receiving a signature
//! or discovering requests leaves entries in place; publication (through
//! local completion or incoming admission) removes them.
//!
//! [`Automerge::request_signatures`] turns outstanding drafts into an owned
//! [`SigningRequests`] snapshot for the document's fixed author. Each
//! [`SigningRequest`] carries the exact hash, the historical [`Author`] bound
//! to that change's actor, and the canonical domain-separated message that
//! an external Ed25519 signer must sign (see [`signing_message`]). Feed the
//! responses to [`Automerge::insert_signatures`].
//!
//! An empty result does not mean everything is published: history can be
//! blocked on another author's unpublished boundary. Use
//! [`Automerge::has_unpublished_changes`] for publication status.

use std::collections::HashSet;

use crate::admission::{self, AcceptedHistory};
use crate::signature::{signing_message, SIGNING_MESSAGE_LEN};
use crate::{Author, ChangeHash, Fragment};

use super::error::SigningAccess;
use super::{has_signature, Automerge, SigningDocument};

/// Exact hashes of locally committed, not yet published changes, in commit order.
#[derive(Clone, Debug, Default)]
pub(super) struct LocalDrafts(Vec<ChangeHash>);

impl LocalDrafts {
    /// Record a successful local commit.
    pub(super) fn record(&mut self, hash: ChangeHash) {
        self.0.push(hash);
    }

    /// Drop every draft that has been published.
    pub(super) fn retire(&mut self, mut is_published: impl FnMut(&ChangeHash) -> bool) {
        self.0.retain(|hash| !is_published(hash));
    }

    pub(super) fn contains(&self, hash: &ChangeHash) -> bool {
        self.0.contains(hash)
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = ChangeHash> + '_ {
        self.0.iter().copied()
    }

    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// One signature the application must produce for a locally committed change.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SigningRequest {
    hash: ChangeHash,
    author: Author<'static>,
    message: [u8; SIGNING_MESSAGE_LEN],
}

impl SigningRequest {
    /// The exact applied change hash to sign.
    pub fn hash(&self) -> ChangeHash {
        self.hash
    }

    /// The author recorded for this change; the signer must hold its key.
    pub fn author(&self) -> &Author<'static> {
        &self.author
    }

    /// The exact bytes to sign: the domain-separated [`signing_message`].
    pub fn message(&self) -> &[u8] {
        &self.message
    }
}

/// Owned snapshot of outstanding signing work, independent of later edits.
///
/// Requests are deduplicated and ordered by hash. Responses may arrive late:
/// a request remains valid for its exact hash even after the document moves
/// on, and duplicates are reported harmlessly by the batch receipt.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SigningRequests(Vec<SigningRequest>);

impl SigningRequests {
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &SigningRequest> {
        self.0.iter()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

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

impl Automerge {
    /// Discover the signatures the fixed local author should produce next.
    ///
    /// Plans over canonical fragments headed by outstanding local drafts. A
    /// fragment is a candidate when every required frontier is either already
    /// verified or an unsigned local draft of this author; candidates are then
    /// filtered to those that could publish together (down to published
    /// history) and reduced to prefer larger covers whenever that keeps the
    /// retained set publishable. The result is the deduplicated, hash-ordered
    /// union of the chosen candidates' missing frontier signatures.
    ///
    /// Discovery is pure: it never mutates the document and never requests
    /// imported hashes. An empty result does not imply full publication.
    ///
    /// # Errors
    ///
    /// Fails if the document is not in required signing mode.
    pub fn request_signatures(&mut self) -> Result<SigningRequests, SigningAccess> {
        let signing = self.try_signing()?;
        Ok(signing.plan_requests())
    }

    /// Whether any applied history is not yet published.
    ///
    /// Read-only. Unsigned documents, empty documents and fully published
    /// documents report `false`. Speculative pending incoming groups are not
    /// applied history and do not count. Not equivalent to having signing
    /// requests: blocked history can be unpublished with nothing to request.
    pub fn has_unpublished_changes(&self) -> bool {
        let super::SigningState::Required(signing) = &self.signing else {
            return false;
        };
        self.change_graph.applied_hashes().iter().any(|hash| {
            let (actor, seq) = self
                .change_graph
                .actor_seq_for_hash(hash)
                .expect("applied hash belongs to the graph");
            !signing.accepted.is_accepted_actor_seq(actor, seq)
        })
    }
}

impl SigningDocument<'_> {
    fn is_eligible_draft(&self, hash: &ChangeHash) -> bool {
        self.state.local_drafts.contains(hash)
            && self
                .graph
                .actor_seq_for_hash(hash)
                .and_then(|(actor, _)| self.authors.get_author_for_actor(actor))
                .is_some_and(|author| author == self.author.author().as_borrowed())
    }

    fn has_evidence(&self, hash: &ChangeHash) -> bool {
        has_signature(self.graph, self.authors, &self.state.signatures, hash)
    }

    /// Structural candidates headed by outstanding drafts whose every frontier
    /// is proved or provable by this author.
    fn signable_candidates(&self) -> Vec<Fragment> {
        let mut candidates = Vec::new();
        let mut seen = HashSet::new();
        for head in self.state.local_drafts.iter() {
            if self.is_accepted(&head) || !seen.insert(head) {
                continue;
            }
            let Some(requirements) = self.graph.fragment_requirements(head) else {
                continue;
            };
            let signable = requirements
                .is_satisfied_by(|hash| self.has_evidence(hash) || self.is_eligible_draft(hash));
            if !signable {
                continue;
            }
            if let Some(fragment) = self.graph.get_historical_fragment(head) {
                candidates.push(fragment);
            }
        }
        candidates
    }

    /// Plan the signatures to request for the current author.
    pub(super) fn plan_requests(&self) -> SigningRequests {
        let candidates = self.signable_candidates();
        let selection = admission::select_fragments(self, &candidates);
        let mut feasible: Vec<Fragment> = selection.selected().cloned().collect();
        feasible.sort_by(|a, b| {
            b.members
                .len()
                .cmp(&a.members.len())
                .then_with(|| a.head.cmp(&b.head))
        });
        let chosen = reduce_covered(self, feasible);

        let mut hashes: Vec<ChangeHash> = chosen
            .iter()
            .filter_map(|fragment| self.graph.fragment_requirements(fragment.head))
            .flat_map(|requirements| requirements.hashes().copied().collect::<Vec<_>>())
            .filter(|hash| !self.has_evidence(hash))
            .collect();
        hashes.sort_unstable();
        hashes.dedup();
        debug_assert!(hashes.iter().all(|hash| self.is_eligible_draft(hash)));

        let requests = hashes
            .into_iter()
            .map(|hash| SigningRequest {
                hash,
                author: self.author.author().clone().into_owned(),
                message: signing_message(&hash),
            })
            .collect();
        SigningRequests(requests)
    }
}

/// Greedily drop candidates whose unpublished members are all covered by
/// retained larger candidates, as long as the retained set still selects
/// completely under causal selection and still covers the same unpublished
/// members. Input must be sorted largest-first for deterministic preference.
fn reduce_covered(accepted: &impl AcceptedHistory, mut retained: Vec<Fragment>) -> Vec<Fragment> {
    let target: HashSet<ChangeHash> = unpublished_members(accepted, &retained);
    let mut index = retained.len();
    while index > 0 {
        index -= 1;
        let candidate = retained.remove(index);
        let covered = candidate
            .members
            .iter()
            .filter(|hash| !accepted.contains(hash))
            .all(|hash| retained.iter().any(|other| other.members.contains(hash)));
        let still_complete = covered && {
            let selection = admission::select_fragments(accepted, &retained);
            selection.selected().len() == retained.len()
                && unpublished_members(accepted, &retained) == target
        };
        if !still_complete {
            retained.insert(index, candidate);
        }
    }
    retained
}

fn unpublished_members(
    accepted: &impl AcceptedHistory,
    fragments: &[Fragment],
) -> HashSet<ChangeHash> {
    fragments
        .iter()
        .flat_map(|fragment| fragment.members.iter().copied())
        .filter(|hash| !accepted.contains(hash))
        .collect()
}
