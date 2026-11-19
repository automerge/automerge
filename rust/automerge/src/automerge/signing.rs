//! Document-owned signing state and access to required-mode operations.
//!
//! Create an empty required-mode document with [`Automerge::signing`] (or load a
//! signed fragment stream with [`crate::LoadOptions::signing`]). Discover
//! signing work with [`Automerge::request_signatures`], sign each request's
//! message externally, and submit the results through
//! [`Automerge::insert_signatures`]; [`Automerge::has_unpublished_changes`]
//! reports whether applied history is still waiting on publication. The
//! document retains its accepted clock, pending groups, and required
//! signatures across those calls; the internal `SigningDocument` handle that
//! implements them is not part of the public API.
//!
//! A required-mode document always has a current [`Author`] whose bytes form a
//! usable Ed25519 verifying key. The author is fixed at construction and
//! validated before any state exists; a new-author fork validates its author
//! first; see [`crate::SigningAuthorError`]. The fixed author writes only
//! through actors it owns: [`Automerge::try_set_actor`] rejects an actor bound
//! to another author, and a transaction on such an actor fails before any
//! operation is applied.
//!
//! # Workflow
//!
//! 1. Create with [`Automerge::signing`] (or [`crate::AutoCommit::signing`])
//!    using the public key bytes as the author; the private key never enters
//!    Automerge. Load published bytes with [`crate::LoadOptions::signing`].
//! 2. Commit locally as usual. Local commits are immediately readable and
//!    become *drafts*: unpublished history only this author can prove.
//! 3. Call [`Automerge::request_signatures`] for an owned [`SigningRequests`]
//!    snapshot. Sign each [`SigningRequest::message`] (exactly those bytes;
//!    see [`crate::signature::signing_message`]) with the key for
//!    [`SigningRequest::author`] outside Automerge.
//! 4. Submit `(hash, signature)` pairs with [`Automerge::insert_signatures`].
//!    Every entry gets an owned result; invalid or stale entries do not stop
//!    the rest. [`SignatureBatchOutcome::Completed`] means the batch was
//!    processed and completion ran once, **not** that everything is
//!    published: history blocked behind dependencies other authors still
//!    have to publish waits. [`SignatureBatchOutcome::PublicationFailed`]
//!    keeps all inserted evidence and completed local publication.
//! 5. To shut down: stop editing, commit, process signing, then wait for
//!    [`Automerge::has_unpublished_changes`] to return `false` with the
//!    application's own timeout. The library has no worker or timer, and a
//!    timeout never relaxes proof requirements. Empty requests with
//!    unpublished history means publication is blocked on peers.
//!    [`Automerge::save`] returns bytes only (durability is the caller's job)
//!    and contains published history with its proofs; drafts stay live in
//!    the document but are not recoverable from the bytes after exit.
//!
//! # Limits
//!
//! - Unsupported in required mode, with checked errors: `fork_at` (signed
//!   historical views), the sync protocol, and merging with an unsigned
//!   document in either direction. Transfer published history with
//!   save/load, incremental saves or merge between signing documents.
//! - Raw inspectors (`get_changes`, `get_change_by_hash`, `fragments`) show
//!   applied drafts; they are not authenticated transport. Only signed groups
//!   with complete proof are accepted on ingress.
//! - Retained incoming groups are bounded per group, not in total; the
//!   absence of a total pending budget is a resource limitation, not a
//!   security bound. There is no state recovery after panic or crash.
//! - [`Automerge::anonymize`] of a signing document is a new unsigned
//!   diagnostic copy without proofs or drafts.

use std::collections::HashSet;
use std::ops::RangeBounds;

use crate::actor::{ActorRemoval, ActorShift, ActorTable};
use crate::admission::{select_fragments, AcceptedHistory};
use crate::author::Authors;
use crate::change_graph::ChangeGraph;
use crate::clock::SeqClock;
use crate::error::SigningAuthorErrorInner;
use crate::op_set2::OpSet;
use crate::signature::{ed25519, Signature, SignatureStore};
use crate::storage::signatures::{HashEntries, SignaturesChunk};
use crate::{Author, Change, ChangeHash, Fragment, SigningAuthorError, TextEncoding};

use super::Automerge;

mod batch;
mod incoming;
pub(crate) mod persistence;
mod readiness;
mod requests;

pub use batch::{
    PublicationError, SignatureBatchOutcome, SignatureInsertError, SignatureInsertOutcome,
    SignatureRejection,
};
use incoming::PendingGroups;
use readiness::{FragmentReadiness, OutgoingSelection};
use requests::LocalDrafts;
pub use requests::{SigningRequest, SigningRequests};

/// Contiguous accepted change sequences, maintained as a dependency-closed prefix.
#[derive(Clone, Debug)]
pub(super) struct AcceptedClock {
    clock: SeqClock,
}

impl AcceptedClock {
    fn new(actors: &ActorTable) -> Self {
        Self {
            clock: SeqClock::new(actors),
        }
    }

    pub(super) fn is_accepted_actor_seq(&self, actor: usize, seq: u32) -> bool {
        self.clock
            .get_for_actor(&actor)
            .is_some_and(|accepted| seq <= accepted.get())
    }

    fn insert_actor(&mut self, actor: &ActorShift) {
        self.clock.shift_actor(actor);
    }

    fn remove_actor(&mut self, actor: &ActorRemoval) {
        self.clock.remove_actor(actor);
    }

    pub(super) fn include_hashes(
        &mut self,
        graph: &ChangeGraph,
        hashes: impl IntoIterator<Item = ChangeHash>,
    ) {
        let mut next = self.clock.clone();
        for hash in hashes {
            let (actor, seq) = graph
                .actor_seq_for_hash(&hash)
                .expect("accepted change belongs to the applied graph");
            next.include(actor, Some(seq));
        }
        self.clock = next;
    }
}

/// The document's signing mode, together with the state required by that mode.
#[derive(Clone, Debug, Default)]
pub(crate) enum SigningState {
    #[default]
    Unsigned,
    Required(RequiredSigning),
}

impl SigningState {
    pub(crate) fn is_required(&self) -> bool {
        matches!(self, Self::Required(_))
    }

    /// Check that `author` may be the local author of a document in this mode.
    ///
    /// Unsigned mode accepts anything, including `None`. Required mode needs an
    /// author whose bytes form a usable Ed25519 verifying key.
    pub(super) fn validate_author(
        &self,
        author: Option<&Author<'static>>,
    ) -> Result<(), SigningAuthorError> {
        match self {
            Self::Unsigned => Ok(()),
            Self::Required(_) => prepare_author(author).map(drop),
        }
    }

    /// Run `f` only when signing is required, propagating its result.
    ///
    /// Unsigned documents skip the check and succeed; required-mode documents
    /// call `f` exactly once. Use this for validation that only applies to
    /// signed history.
    pub(crate) fn with_required<E>(&self, f: impl FnOnce() -> Result<(), E>) -> Result<(), E> {
        match self {
            Self::Unsigned => Ok(()),
            Self::Required(_) => f(),
        }
    }

    pub(super) fn has_pending_groups(&self) -> bool {
        self.pending_group_count() > 0
    }

    /// Number of retained incoming groups still waiting for causal context.
    pub(super) fn pending_group_count(&self) -> usize {
        match self {
            Self::Unsigned => 0,
            Self::Required(signing) => signing.pending.len(),
        }
    }

    pub(super) fn queued_changes(&self) -> impl Iterator<Item = &Change> {
        match self {
            Self::Unsigned => None,
            Self::Required(signing) => Some(signing.pending.queued_changes()),
        }
        .into_iter()
        .flatten()
    }

    pub(super) fn insert_actor(&mut self, actor: &ActorShift) {
        if let Self::Required(signing) = self {
            signing.accepted.insert_actor(actor);
        }
    }

    pub(super) fn remove_actor(&mut self, actor: &ActorRemoval) {
        if let Self::Required(signing) = self {
            signing.accepted.remove_actor(actor);
        }
    }

    /// Whether any locally committed change is still unpublished.
    ///
    /// Signed-but-unpublished drafts count; retained incoming groups do not.
    /// Unsigned documents have no drafts.
    pub(super) fn has_local_drafts(&self) -> bool {
        match self {
            Self::Unsigned => false,
            Self::Required(signing) => !signing.local_drafts.is_empty(),
        }
    }

    /// Record a successful local commit as an unpublished draft.
    ///
    /// Only the local commit path may call this; imports and loads never
    /// create drafts.
    pub(super) fn record_local_change(&mut self, hash: ChangeHash) {
        if let Self::Required(signing) = self {
            signing.local_drafts.record(hash);
        }
    }

    pub(super) fn change_added(
        &mut self,
        graph: &ChangeGraph,
        authors: &Authors,
        hash: &ChangeHash,
    ) {
        if hash.fragment_level() == 0 {
            return;
        }
        if let Self::Required(signing) = self {
            signing.fragment_readiness.register(graph, *hash, |hash| {
                has_signature(graph, authors, &signing.signatures, hash)
            });
            if signing.fragment_readiness.is_ready(hash) {
                signing.reclaim_covered(graph);
            }
        }
    }

    pub(super) fn fragments<R: RangeBounds<usize>>(
        &self,
        graph: &ChangeGraph,
        authors: &Authors,
        levels: R,
    ) -> Vec<Fragment> {
        match self {
            Self::Unsigned => graph.fragments(levels).collect(),
            Self::Required(signing) => {
                let mut fragments = OutgoingSelection::new(graph, &signing.fragment_readiness)
                    .collect(|hash| has_signature(graph, authors, &signing.signatures, hash));
                fragments.retain(|fragment| levels.contains(&fragment.level));
                fragments
            }
        }
    }

    pub(super) fn get_fragment(
        &self,
        graph: &ChangeGraph,
        authors: &Authors,
        head: ChangeHash,
    ) -> Option<Fragment> {
        match self {
            Self::Unsigned => graph.get_fragment(head),
            Self::Required(signing) => {
                let eligible = graph
                    .fragment_requirements(head)
                    .is_some_and(|requirements| {
                        requirements.is_satisfied_by(|hash| {
                            has_signature(graph, authors, &signing.signatures, hash)
                        })
                    });
                eligible
                    .then(|| graph.get_historical_fragment(head))
                    .flatten()
            }
        }
    }

    fn required_mut(&mut self) -> Result<&mut RequiredSigning, error::SigningMode> {
        match self {
            SigningState::Unsigned => Err(error::SigningMode::Required),
            SigningState::Required(signing) => Ok(signing),
        }
    }
}

/// Prepare the Ed25519 verifying key for a required-mode current author.
///
/// Fails if no author is supplied or its bytes are not a usable key. Successful
/// preparation is not signature verification.
fn prepare_author<'a>(
    author: Option<&'a Author<'static>>,
) -> Result<ed25519::VerifyingAuthor<'a>, SigningAuthorError> {
    let author = author.ok_or(SigningAuthorError(SigningAuthorErrorInner::MissingAuthor))?;
    ed25519::VerifyingAuthor::new(author.as_borrowed())
        .map_err(|source| SigningAuthorError(SigningAuthorErrorInner::InvalidKey(source)))
}

/// Long-lived required-mode data owned by the document.
#[derive(Clone, Debug)]
pub(crate) struct RequiredSigning {
    accepted: AcceptedClock,
    signatures: SignatureStore,
    fragment_readiness: FragmentReadiness,
    pending: PendingGroups,
    local_drafts: LocalDrafts,
}

impl RequiredSigning {
    fn new(actors: &ActorTable) -> Self {
        Self {
            accepted: AcceptedClock::new(actors),
            signatures: SignatureStore::default(),
            fragment_readiness: FragmentReadiness::default(),
            pending: PendingGroups::default(),
            local_drafts: LocalDrafts::default(),
        }
    }

    /// Encode the trailing signature chunk for `fragment`, or return `None` to
    /// withhold the fragment if it is non-canonical or lacks a required signature.
    pub(super) fn fragment_proof(
        &self,
        graph: &ChangeGraph,
        authors: &Authors,
        fragment: &Fragment,
    ) -> Option<SignaturesChunk<'static>> {
        let canonical = graph.get_historical_fragment(fragment.head)?;
        // `Automerge::bundle_fragments` accepts caller-supplied `Fragment` values
        // with public fields, so `fragment` may have been constructed or mutated
        // externally; never pair `head`'s proof with non-canonical `members`.
        if !canonical.same_descriptor(fragment) {
            return None;
        }
        let requirements = graph.fragment_requirements(fragment.head)?;
        let entries: Option<Vec<_>> = requirements
            .hashes()
            .map(|hash| {
                signature_for(graph, authors, &self.signatures, hash)
                    .map(|signature| (*hash, Signature::ed25519(*signature)))
            })
            .collect();
        let entries = HashEntries::canonicalize(entries?)
            .expect("one signature per distinct required hash is within wire limits");
        Some(SignaturesChunk::encode(entries))
    }

    /// Reclaim fragment candidates and signature records made redundant by a
    /// *published* replacement cover.
    ///
    /// Retains every frontier of a still-registered candidate, all evidence for
    /// unpublished changes, and published loose fallbacks that no published
    /// cover represents. A cover that is merely signature-ready (exportable but
    /// blocked on an unpublished boundary) never retires anything. The published
    /// cover is recomputed on every call, so publication-only progress reclaims
    /// without new evidence.
    pub(super) fn reclaim_covered(&mut self, graph: &ChangeGraph) {
        let accepted = &self.accepted;
        let represented = self.fragment_readiness.prune_covered(graph, |hash| {
            graph
                .actor_seq_for_hash(hash)
                .is_some_and(|(actor, seq)| accepted.is_accepted_actor_seq(actor, seq))
        });
        let live_frontiers: HashSet<ChangeHash> = self
            .fragment_readiness
            .required_frontiers()
            .copied()
            .collect();

        self.signatures.retain_hashes(|hash| {
            if live_frontiers.contains(hash) {
                return true;
            }
            let Some((actor, seq)) = graph.actor_seq_for_hash(hash) else {
                debug_assert!(
                    false,
                    "retained signature hash {hash} missing from ChangeGraph"
                );
                return false;
            };
            if !accepted.is_accepted_actor_seq(actor, seq) {
                return true;
            }
            // Published hard heads are no longer candidates, so a published
            // cover already represents them; published loose fallbacks survive
            // until a published cover represents them.
            hash.fragment_level() == 0 && !represented.contains(hash)
        });
    }
}

impl RequiredSigning {
    /// Forget local drafts that are now published, whether by local
    /// completion or by incoming admission.
    pub(super) fn retire_published_drafts(&mut self, graph: &ChangeGraph) {
        let accepted = &self.accepted;
        self.local_drafts.retire(|hash| {
            graph
                .actor_seq_for_hash(hash)
                .is_some_and(|(actor, seq)| accepted.is_accepted_actor_seq(actor, seq))
        });
    }
}

pub(super) fn signature_for<'a>(
    graph: &ChangeGraph,
    authors: &Authors,
    signatures: &'a SignatureStore,
    hash: &ChangeHash,
) -> Option<&'a ed25519::Signature> {
    let (actor, _) = graph.actor_seq_for_hash(hash)?;
    let author = authors.get_author_for_actor(actor)?;
    signatures
        .ed25519_entries(hash)
        .find_map(|(signed_author, signature)| (*signed_author == author).then_some(signature))
}

pub(super) fn has_signature(
    graph: &ChangeGraph,
    authors: &Authors,
    signatures: &SignatureStore,
    hash: &ChangeHash,
) -> bool {
    signature_for(graph, authors, signatures, hash).is_some()
}

/// Access to a document in required signing mode, obtained via [`Automerge::try_signing`].
///
/// Holding a handle guarantees the document's current author has been prepared
/// as a usable Ed25519 verifying key. Internal implementation detail: the
/// public surface is [`Automerge::insert_signatures`],
/// [`Automerge::request_signatures`] and [`Automerge::has_unpublished_changes`].
#[derive(Debug)]
pub(crate) struct SigningDocument<'a> {
    graph: &'a ChangeGraph,
    ops: &'a OpSet,
    authors: &'a Authors,
    author: ed25519::VerifyingAuthor<'a>,
    state: &'a mut RequiredSigning,
}

impl<'a> SigningDocument<'a> {
    /// Whether this exact applied change belongs to accepted history.
    pub(crate) fn is_accepted(&self, hash: &ChangeHash) -> bool {
        self.graph
            .actor_seq_for_hash(hash)
            .is_some_and(|(actor, seq)| self.state.accepted.is_accepted_actor_seq(actor, seq))
    }

    fn accept_local(&mut self) {
        let fragments = self.local_candidate_fragments();
        let selection = select_fragments(&*self, &fragments);
        self.state.accepted.include_hashes(
            self.graph,
            selection
                .selected()
                .flat_map(|fragment| fragment.members.iter().copied()),
        );
    }

    /// Independently proved, not-wholly-accepted local descriptors.
    ///
    /// Every fragment requires a signature on its own head (the maximum sequence
    /// for the head's actor), so scanning `verified_hashes` visits only live
    /// signed heads rather than every applied change in the document.
    ///
    /// Each required frontier must carry its own exact signature. Published
    /// membership of a frontier is not proof: a published change's evidence
    /// may have been reclaimed by a published cover, and that cover supports
    /// the previously published contents, not a new candidate including them.
    fn local_candidate_fragments(&self) -> Vec<Fragment> {
        let mut candidates = Vec::new();
        for &head in self.state.signatures.verified_hashes() {
            if self.is_accepted(&head)
                || !has_signature(self.graph, self.authors, &self.state.signatures, &head)
            {
                continue;
            }
            let Some(requirements) = self.graph.fragment_requirements(head) else {
                debug_assert!(
                    false,
                    "verified signature hash {head} missing from ChangeGraph"
                );
                continue;
            };
            if requirements.is_satisfied_by(|hash| {
                has_signature(self.graph, self.authors, &self.state.signatures, hash)
            }) {
                let fragment = self
                    .graph
                    .get_historical_fragment(head)
                    .expect("local head belongs to the graph");
                candidates.push(fragment);
            }
        }
        candidates
    }
}

impl AcceptedHistory for SigningDocument<'_> {
    fn contains(&self, hash: &ChangeHash) -> bool {
        self.is_accepted(hash)
    }
}

impl Automerge {
    /// Create a new empty document in required signing mode with `author` as
    /// its current author.
    ///
    /// # Errors
    ///
    /// Returns [`SigningAuthorError`] if `author` cannot supply a usable Ed25519
    /// verifying key.
    pub fn signing(author: Author<'static>) -> Result<Self, SigningAuthorError> {
        Self::signing_with_encoding(author, TextEncoding::platform_default())
    }

    /// Create a new empty document in required signing mode with an explicit
    /// text encoding. See [`Self::signing`].
    pub fn signing_with_encoding(
        author: Author<'static>,
        encoding: TextEncoding,
    ) -> Result<Self, SigningAuthorError> {
        prepare_author(Some(&author))?;
        let mut doc = Self::new_with_author_and_encoding(Some(author), encoding);
        doc.signing = SigningState::Required(RequiredSigning::new(doc.actors()));
        Ok(doc)
    }

    /// Borrow this document's [`SigningDocument`] handle.
    ///
    /// # Errors
    ///
    /// Fails if the document is not in required signing mode, or if its current
    /// author is missing or unusable. The latter indicates the author invariant
    /// was bypassed; constructors, loads and forks never produce it.
    pub(crate) fn try_signing(&mut self) -> Result<SigningDocument<'_>, error::SigningAccess> {
        let state = self.signing.required_mut()?;
        let author = prepare_author(self.author.as_ref())?;
        Ok(SigningDocument {
            graph: &self.change_graph,
            ops: &self.ops,
            authors: &self.authors,
            author,
            state,
        })
    }
}

pub mod error {
    use crate::signature;
    use crate::signature::ed25519;
    use crate::{ActorId, ChangeHash};

    /// Failure to verify and insert a signature for an already-applied change.
    #[derive(Debug, thiserror::Error)]
    pub enum InsertSignature {
        #[error("change {change} is not applied in this document")]
        UnknownChange { change: ChangeHash },
        #[error("actor {actor} of change {change} has no recorded author")]
        MissingAuthor { change: ChangeHash, actor: ActorId },
        #[error(
            "author of actor {actor} for change {change} cannot supply an Ed25519 key: {source}"
        )]
        InvalidAuthorKey {
            change: ChangeHash,
            actor: ActorId,
            #[source]
            source: ed25519::error::VerifyingAuthor,
        },
        #[error("signature verification failed for change {change}: {source}")]
        Verification {
            change: ChangeHash,
            #[source]
            source: ed25519::error::Verification,
        },
        #[error("signature insertion failed for change {change}: {source}")]
        Store {
            change: ChangeHash,
            #[source]
            source: signature::InsertError,
        },
    }

    /// An operation requested a mode that the document does not have.
    #[derive(Debug, PartialEq, Eq, thiserror::Error)]
    pub enum SigningMode {
        #[error("operation requires a signing document")]
        Required,
    }

    /// Failure to access a document's required signing state, e.g. from
    /// [`crate::Automerge::request_signatures`].
    #[derive(Debug, thiserror::Error)]
    pub enum SigningAccess {
        #[error(transparent)]
        Mode(#[from] SigningMode),
        #[error(transparent)]
        Author(#[from] crate::SigningAuthorError),
    }
}

#[cfg(test)]
mod tests;
