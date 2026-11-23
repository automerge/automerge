//! Document-owned signing state and access to required-mode operations.
//!
//! Create an empty required-mode document with [`crate::Config::signing`] and
//! [`Automerge::from_config`] (or load a
//! signed fragment stream with [`crate::LoadOptions::signing`]). Discover
//! signing work with [`Automerge::signing_requests`], sign each request's
//! message externally, and submit the results through
//! [`Automerge::insert_signatures`]; [`Automerge::has_unpublished_changes`]
//! reports whether local drafts are still waiting on publication. The
//! document retains its published clock, pending groups, and required
//! signatures across those calls; the internal `SigningDocument` handle that
//! implements them is not part of the public API.
//!
//! A required-mode document always has a current [`Author`] whose bytes form a
//! usable Ed25519 verifying key. The author is fixed at construction and
//! validated before any state exists; a new-author fork validates its author
//! first; see [`crate::SigningAuthorError`]. The fixed author writes only
//! through actors it owns: [`Automerge::set_actor`] rejects an actor bound
//! to another identity in either mode. If an import later binds a manually
//! selected actor to another identity, local transaction entry panics before
//! applying operations; prefer automatic actor management.
//!
//! # Workflow
//!
//! 1. Configure with [`crate::Config::signing`], then construct with
//!    [`Automerge::from_config`] or [`crate::AutoCommit::from_config`],
//!    using the public key bytes as the author; the private key never enters
//!    Automerge. Load published bytes with [`crate::LoadOptions::signing`].
//! 2. Commit locally as usual. Local commits are immediately readable and
//!    become *drafts*: unpublished history only this author can prove.
//! 3. Call [`Automerge::signing_requests`] for an owned [`SigningRequests`]
//!    snapshot of every unsigned local draft, without waiting for causal
//!    publication. Sign each [`SigningRequest::message`] (exactly those bytes;
//!    see [`crate::signature::signing_message`]) with the configured author's
//!    key outside Automerge.
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
//!   historical views), arbitrary `bundle`, the sync protocol, and merging with an unsigned
//!   document in either direction. Transfer published history with
//!   save/load, incremental saves or merge between signing documents.
//! - Raw inspectors (`get_changes`, `get_change_by_hash`) show applied
//!   drafts and are not authenticated transport. `fragments` exports
//!   proof-ready descriptors (possibly not yet published, never a bare
//!   unsignable draft); only `bundle_fragments` bytes are authenticated
//!   delivery, and only signed groups with complete proof are admitted on
//!   ingress.
//! - Published snapshots retain the document's actor table, which can include
//!   actor IDs used only by unpublished drafts. No draft operations or change
//!   bodies are saved; this is not a metadata-privacy boundary.
//! - Ingress enforces the existing per-hash byte and signature-entry limits
//!   on the wire; these do not cap a group's contents or total memory. Each
//!   retained incoming group owns its own copy of duplicated data, and there
//!   is no document-wide pending budget: a resource limitation, not a
//!   security bound. There is no state recovery after panic or crash.
//! - [`Automerge::anonymize`] of a signing document is a new unsigned
//!   diagnostic copy without proofs or drafts.

use std::collections::HashSet;
use std::ops::RangeBounds;

use crate::actor::{ActorRemoval, ActorShift, ActorTable};
use crate::admission::{select_fragments, PublishedHistory};
use crate::author::Authors;
use crate::change_graph::ChangeGraph;
use crate::clock::SeqClock;
use crate::config::DocumentAuthor;
use crate::error::SigningAuthorErrorInner;
use crate::op_set2::OpSet;
use crate::signature::error::SigningError;
use crate::signature::Insert;
use crate::signature::{ed25519, Signature, SignatureStore};
use crate::storage::signatures::{HashEntries, SignaturesChunk};
use crate::{Author, Change, ChangeHash, Fragment, PatchLog, SigningAuthorError};
use error::InsertSignature;

use super::Automerge;

mod batch;
mod incoming;
pub(crate) mod persistence;
mod readiness;
mod requests;

use crate::storage::document::CompressConfig;
use crate::storage::load::{LoadedGroup, LoadedGroupBody};
use crate::storage::Document;
use crate::AutomergeError;
pub use batch::{
    PublicationError, SignatureBatchOutcome, SignatureInsertError, SignatureInsertOutcome,
    SignatureRejection,
};
use incoming::{Incoming, PendingGroups, PreparedAdmission};
use persistence::PublishedGroup;
use readiness::{FragmentReadiness, OutgoingSelection};
use requests::LocalDrafts;
pub use requests::{SigningRequest, SigningRequests};

/// Contiguous published change sequences, maintained as a dependency-closed prefix.
#[derive(Clone, Debug)]
pub(super) struct PublishedClock {
    clock: SeqClock,
}

impl PublishedClock {
    fn new(actors: &ActorTable) -> Self {
        Self {
            clock: SeqClock::new(actors),
        }
    }

    pub(super) fn is_published_actor_seq(&self, actor: usize, seq: u32) -> bool {
        self.clock
            .get_for_actor(&actor)
            .is_some_and(|published| seq <= published.get())
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
                .expect("published change belongs to the applied graph");
            next.include(actor, Some(seq));
        }
        self.clock = next;
    }
}

/// The document's signing mode, together with the state required by that mode.
#[derive(Clone, Debug)]
pub(crate) enum SigningState {
    Unsigned { author: Option<Author<'static>> },
    Required(Box<RequiredSigning>),
}

impl Default for SigningState {
    fn default() -> Self {
        Self::Unsigned { author: None }
    }
}

impl SigningState {
    pub(super) fn new(author: DocumentAuthor<'static>, actors: &ActorTable) -> Self {
        match author {
            DocumentAuthor::Unset => Self::default(),
            DocumentAuthor::Set(author) => Self::Unsigned {
                author: Some(author),
            },
            DocumentAuthor::Verifier(author) => {
                Self::Required(Box::new(RequiredSigning::new(actors, author)))
            }
        }
    }

    pub(super) fn author(&self) -> Option<&Author<'static>> {
        match self {
            Self::Unsigned { author } => author.as_ref(),
            Self::Required(signing) => Some(signing.author.author()),
        }
    }

    pub(super) fn document_author(&self) -> DocumentAuthor<'static> {
        match self {
            Self::Unsigned { author } => author.clone().into(),
            Self::Required(signing) => DocumentAuthor::Verifier(signing.author.clone()),
        }
    }

    pub(super) fn prepare_fork_author(
        &self,
        author: Option<Author<'static>>,
        has_pending_edits: bool,
    ) -> Result<DocumentAuthor<'static>, crate::AutomergeError> {
        match self {
            Self::Unsigned { .. } => Ok(author.into()),
            Self::Required(signing) => {
                let author =
                    author.ok_or(SigningAuthorError(SigningAuthorErrorInner::MissingAuthor))?;
                let changed = &author != signing.author.author();
                let prepared = if changed {
                    ed25519::VerifyingAuthor::new(author).map_err(|error| {
                        SigningAuthorError(SigningAuthorErrorInner::InvalidKey(error))
                    })?
                } else {
                    signing.author.clone()
                };
                if changed && (has_pending_edits || !signing.local_drafts.is_empty()) {
                    return Err(
                        crate::signature::error::SigningError::UnpublishedLocalChanges.into(),
                    );
                }
                Ok(DocumentAuthor::Verifier(prepared))
            }
        }
    }
    /// Install only a prepared identity on a freshly cloned, aligned state.
    pub(super) fn replace_fork_author(&mut self, author: DocumentAuthor<'static>) {
        match (self, author) {
            (Self::Unsigned { author }, DocumentAuthor::Unset) => *author = None,
            (Self::Unsigned { author }, DocumentAuthor::Set(value)) => *author = Some(value),
            (Self::Required(state), DocumentAuthor::Verifier(value)) => state.author = value,
            _ => unreachable!("prepared fork must preserve signing mode"),
        }
    }

    pub(crate) fn is_same_mode(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (Self::Unsigned { .. }, Self::Unsigned { .. }) | (Self::Required(_), Self::Required(_))
        )
    }

    pub(crate) fn is_required(&self) -> bool {
        matches!(self, Self::Required(_))
    }

    /// Check eligibility before an AutoCommit operation closes pending edits.
    /// The actual operation obtains aligned signing access after that closure.
    pub(crate) fn require(&self) -> Result<(), error::SigningMode> {
        match self {
            Self::Unsigned { .. } => Err(error::SigningMode::Required),
            Self::Required(_) => Ok(()),
        }
    }

    /// Run `f` only when signing is required, propagating its result.
    ///
    /// Unsigned documents skip the check and succeed; required-mode documents
    /// call `f` exactly once. Use this for validation that only applies to
    /// signed history.
    pub(crate) fn with_required<E>(&self, f: impl FnOnce() -> Result<(), E>) -> Result<(), E> {
        match self {
            Self::Unsigned { .. } => Ok(()),
            Self::Required(_) => f(),
        }
    }

    pub(super) fn has_pending_groups(&self) -> bool {
        self.pending_group_count() > 0
    }

    /// Number of retained incoming groups still waiting for causal context.
    pub(super) fn pending_group_count(&self) -> usize {
        match self {
            Self::Unsigned { .. } => 0,
            Self::Required(signing) => signing.pending.len(),
        }
    }

    pub(super) fn queued_changes(&self) -> impl Iterator<Item = &Change> {
        match self {
            Self::Unsigned { .. } => None,
            Self::Required(signing) => Some(signing.pending.queued_changes()),
        }
        .into_iter()
        .flatten()
    }

    pub(super) fn insert_actor(&mut self, actor: &ActorShift) {
        if let Self::Required(signing) = self {
            signing.published.insert_actor(actor);
        }
    }

    pub(super) fn remove_actor(&mut self, actor: &ActorRemoval) {
        if let Self::Required(signing) = self {
            signing.published.remove_actor(actor);
        }
    }

    /// Whether any locally committed change is still unpublished.
    ///
    /// Signed-but-unpublished drafts count; retained incoming groups do not.
    /// Unsigned documents have no drafts.
    pub(super) fn has_local_drafts(&self) -> bool {
        match self {
            Self::Unsigned { .. } => false,
            Self::Required(signing) => !signing.local_drafts.is_empty(),
        }
    }

    /// Record a successful local commit as an unpublished draft.
    ///
    /// Only the local commit path may call this; imports and loads never
    /// create drafts.
    pub(super) fn record_local_change(&mut self, hash: ChangeHash) {
        if let Self::Required(signing) = self {
            signing.local_drafts.insert(hash);
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
            Self::Unsigned { .. } => graph.fragments(levels).collect(),
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
            Self::Unsigned { .. } => graph.get_fragment(head),
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
            SigningState::Unsigned { .. } => Err(error::SigningMode::Required),
            SigningState::Required(signing) => Ok(signing),
        }
    }
}

/// Long-lived required-mode data owned by the document.
#[derive(Clone, Debug)]
pub(crate) struct RequiredSigning {
    author: ed25519::VerifyingAuthor<'static>,
    published: PublishedClock,
    signatures: SignatureStore,
    fragment_readiness: FragmentReadiness,
    pending: PendingGroups,
    local_drafts: LocalDrafts,
}

impl RequiredSigning {
    fn new(actors: &ActorTable, author: ed25519::VerifyingAuthor<'static>) -> Self {
        Self {
            author,
            published: PublishedClock::new(actors),
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
        let published = &self.published;
        let represented = self.fragment_readiness.prune_covered(graph, |hash| {
            graph
                .actor_seq_for_hash(hash)
                .is_some_and(|(actor, seq)| published.is_published_actor_seq(actor, seq))
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
            if !published.is_published_actor_seq(actor, seq) {
                return true;
            }
            // Published hard heads are no longer candidates, so a published
            // cover already represents them; published loose fallbacks survive
            // until a published cover represents them.
            hash.fragment_level() == 0 && !represented.contains(hash)
        });
    }

    /// Remove local drafts that are now published, whether by local
    /// completion or by incoming admission.
    pub(super) fn remove_published_drafts(&mut self, graph: &ChangeGraph) {
        let published = &self.published;
        self.local_drafts.remove(|hash| {
            graph
                .actor_seq_for_hash(hash)
                .is_some_and(|(actor, seq)| published.is_published_actor_seq(actor, seq))
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
/// [`Automerge::signing_requests`] and [`Automerge::has_unpublished_changes`].
#[derive(Debug)]
pub(crate) struct SigningDocument<'a> {
    graph: &'a ChangeGraph,
    ops: &'a OpSet,
    authors: &'a Authors,
    state: &'a mut RequiredSigning,
}

impl<'a> SigningDocument<'a> {
    fn admit_with_pending(
        &self,
        groups: Vec<crate::LoadedGroup>,
    ) -> Result<PreparedAdmission, crate::AutomergeError> {
        Incoming {
            graph: self.graph,
            ops: self.ops,
            authors: self.authors,
            state: self.state,
        }
        .prepare(groups)
    }

    fn signing_requests(&self) -> SigningRequests {
        SigningRequests::for_hashes(self.state.local_drafts.iter().filter(|hash| {
            !has_signature(self.graph, self.authors, &self.state.signatures, hash)
                && self
                    .graph
                    .actor_seq_for_hash(hash)
                    .and_then(|(actor, _)| self.authors.get_author_for_actor(actor))
                    .is_some_and(|author| {
                        author.as_bytes() == self.state.author.author().as_bytes()
                    })
        }))
    }

    /// Whether this exact applied change belongs to published history.
    pub(crate) fn is_published(&self, hash: &ChangeHash) -> bool {
        self.graph
            .actor_seq_for_hash(hash)
            .is_some_and(|(actor, seq)| self.state.published.is_published_actor_seq(actor, seq))
    }

    fn publish_proved_local(&mut self) {
        let fragments = self.local_candidate_fragments();
        let selection = select_fragments(&*self, &fragments);
        self.state.published.include_hashes(
            self.graph,
            selection
                .selected()
                .flat_map(|fragment| fragment.members.iter().copied()),
        );
    }

    /// Independently proved, not-wholly-published local descriptors.
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
            if self.is_published(&head)
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

impl PublishedHistory for SigningDocument<'_> {
    fn contains(&self, hash: &ChangeHash) -> bool {
        self.is_published(hash)
    }
}

impl Automerge {
    /// Validate proof-bearing input before applying any newly selected contents.
    pub(crate) fn admit_loaded_groups(
        &mut self,
        groups: Vec<crate::LoadedGroup>,
        log: &mut PatchLog,
    ) -> Result<(), crate::AutomergeError> {
        log.validate(&self.ops.actors)?;
        if groups.is_empty() {
            return Ok(());
        }
        self.admit_signing_groups(groups, log)
    }

    fn retry_pending_groups(&mut self, log: &mut PatchLog) -> Result<(), crate::AutomergeError> {
        if !self.signing.has_pending_groups() {
            return Ok(());
        }
        self.admit_signing_groups(Vec::new(), log)
    }

    fn admit_signing_groups(
        &mut self,
        groups: Vec<crate::LoadedGroup>,
        log: &mut PatchLog,
    ) -> Result<(), crate::AutomergeError> {
        let prepared = self
            .try_with_signing(|signing| signing.admit_with_pending(groups))
            .expect("signed admission requires signing mode")?;
        let changes = prepared.changes();
        if !changes.is_empty() {
            self.apply_prepared_changes(changes, log)?;
        }
        // Graph/actor mutation ended; use a fresh aligned view for publication.
        self.try_with_signing(|mut signing| prepared.complete(&mut signing))
            .expect("application preserves signing mode");
        Ok(())
    }

    /// Verify and insert signatures for applied changes, then complete
    /// publication once.
    ///
    /// Inputs are processed in order; every occurrence, including duplicates,
    /// receives an owned [`SignatureInsertOutcome`]. Each signature is checked
    /// against the author recorded for the change's actor, not the current
    /// local author.
    pub fn insert_signatures(
        &mut self,
        inputs: impl IntoIterator<Item = (ChangeHash, ed25519::Signature)>,
    ) -> SignatureBatchOutcome {
        self.insert_signatures_log_patches(inputs, &mut PatchLog::inactive())
    }

    /// Like [`Self::insert_signatures`], recording any incoming groups applied
    /// by completion into `log`.
    pub fn insert_signatures_log_patches(
        &mut self,
        inputs: impl IntoIterator<Item = (ChangeHash, ed25519::Signature)>,
        log: &mut PatchLog,
    ) -> SignatureBatchOutcome {
        let insertions = match self.try_with_signing(|mut signing| {
            log.validate(&signing.ops.actors)?;
            Ok::<_, crate::PatchLogMismatch>(
                inputs
                    .into_iter()
                    .map(|(hash, signature)| SignatureInsertOutcome {
                        hash,
                        signature,
                        result: signing.receive_signature(&hash, &signature),
                    })
                    .collect(),
            )
        }) {
            Err(error) => {
                return SignatureBatchOutcome::Rejected {
                    error: error.into(),
                }
            }
            Ok(Err(error)) => {
                return SignatureBatchOutcome::Rejected {
                    error: error.into(),
                }
            }
            Ok(Ok(insertions)) => insertions,
        };
        match self.complete_publication(log) {
            Ok(()) => SignatureBatchOutcome::Completed { insertions },
            Err(error) => SignatureBatchOutcome::PublicationFailed { insertions, error },
        }
    }

    /// Publish newly proved local history and admit pending groups enabled by
    /// it, repeating only while the retry settles a retained group.
    ///
    /// A retry with no new input can only make progress by selecting a pending
    /// group, and every selected group leaves the pending set whether it
    /// applied new history, merely merged proof for already-applied contents
    /// or only advanced publication. Any of those can enable a further local
    /// publication, so another local pass follows. The pending set is finite
    /// and strictly shrinks on each continued iteration, bounding the loop.
    ///
    /// Local publication is attempted first and always retained. Incoming
    /// retry failures surface as [`PublicationError`] with everything done so
    /// far kept.
    pub(super) fn complete_publication(
        &mut self,
        log: &mut PatchLog,
    ) -> Result<(), PublicationError> {
        loop {
            self.with_signing(|mut signing| signing.publish_local());
            let before = self.signing.pending_group_count();
            if before == 0 {
                return Ok(());
            }
            self.retry_pending_groups(log)
                .map_err(PublicationError::Admission)?;
            if self.signing.pending_group_count() == before {
                return Ok(());
            }
        }
    }
}

impl SigningDocument<'_> {
    /// Verify `signature` under the change's historical author and retain it.
    ///
    /// Pure receipt: no publication, retirement or reclamation happens here.
    pub(super) fn receive_signature(
        &mut self,
        hash: &ChangeHash,
        signature: &ed25519::Signature,
    ) -> Result<Insert, InsertSignature> {
        let (actor, _) = self
            .graph
            .actor_seq_for_hash(hash)
            .ok_or(InsertSignature::UnknownChange { change: *hash })?;
        let actor_id = &self.ops.actors[actor];
        let author = self.authors.get_author_for_actor(actor).ok_or_else(|| {
            InsertSignature::MissingAuthor {
                change: *hash,
                actor: actor_id.clone(),
            }
        })?;
        let prepared;
        let verifier = if author.as_bytes() == self.state.author.author().as_bytes() {
            &self.state.author
        } else {
            prepared = ed25519::VerifyingAuthor::new(author).map_err(|source| {
                InsertSignature::InvalidAuthorKey {
                    change: *hash,
                    actor: actor_id.clone(),
                    source,
                }
            })?;
            &prepared
        };
        let witness =
            verifier
                .verify_hash(hash, signature)
                .map_err(|e| InsertSignature::Verification {
                    change: *hash,
                    source: e,
                })?;
        let had_evidence = has_signature(self.graph, self.authors, &self.state.signatures, hash);
        let inserted =
            self.state
                .signatures
                .insert_ed25519(witness)
                .map_err(|e| InsertSignature::Store {
                    change: *hash,
                    source: e,
                })?;
        if !had_evidence && has_signature(self.graph, self.authors, &self.state.signatures, hash) {
            self.state.fragment_readiness.signature_added(hash);
        }
        Ok(inserted)
    }

    /// Publish proved local history, retire published tracking entries and
    /// reclaim evidence made redundant by published covers.
    pub(super) fn publish_local(&mut self) {
        self.publish_proved_local();
        self.state.reclaim_covered(self.graph);
        self.state.remove_published_drafts(self.graph);
    }
}

impl Automerge {
    /// Return owned messages for every unsigned local draft.
    ///
    /// Discovery does not wait for dependencies to publish and does not consume
    /// requests. Imported changes are never eligible. Empty requests may coexist
    /// with signed-but-unpublished work.
    ///
    /// # Errors
    /// Returns a mode error for unsigned documents.
    pub fn signing_requests(&mut self) -> Result<SigningRequests, error::SigningMode> {
        self.try_with_signing(|signing| signing.signing_requests())
    }

    /// Whether locally committed history still awaits publication.
    ///
    /// Read-only; ignores unapplied incoming groups and uncommitted operations.
    /// Signed-but-unpublished drafts count; unsigned documents return false.
    pub fn has_unpublished_changes(&self) -> bool {
        self.signing.has_local_drafts()
    }

    /// Run a required-mode operation, or return the default result without
    /// evaluating it on an unsigned document.
    pub(crate) fn with_signing<T: Default>(
        &mut self,
        operation: impl FnOnce(SigningDocument<'_>) -> T,
    ) -> T {
        self.try_with_signing(operation).unwrap_or_default()
    }

    /// Run an operation with established required-mode access.
    pub(crate) fn try_with_signing<T>(
        &mut self,
        operation: impl FnOnce(SigningDocument<'_>) -> T,
    ) -> Result<T, error::SigningMode> {
        self.try_signing().map(operation)
    }

    /// Borrow this document's [`SigningDocument`] handle.
    ///
    /// # Errors
    ///
    /// Fails if the document is not in required signing mode. A required
    /// state's verifier was prepared when its configuration was constructed.
    pub(crate) fn try_signing(&mut self) -> Result<SigningDocument<'_>, error::SigningMode> {
        let state = self.signing.required_mut()?;
        Ok(SigningDocument {
            graph: &self.change_graph,
            ops: &self.ops,
            authors: &self.authors,
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
}

#[cfg(test)]
mod tests;

impl Automerge {
    /// Complete published canonical groups with their own proofs, newest first.
    ///
    /// Returns an empty list for unsigned documents. Members already known to
    /// a receiver are included: groups are never truncated. Groups whose
    /// history is entirely within the causal past of `known_heads` are
    /// omitted; a group with any member outside that past is emitted whole.
    pub(crate) fn published_groups(&self, known_heads: &[ChangeHash]) -> Vec<PublishedGroup> {
        let SigningState::Required(signing) = &self.signing else {
            return Vec::new();
        };
        let known = self.change_graph.seq_clock_for_heads(known_heads);
        persistence::published_cover(&self.change_graph, &self.authors, signing)
            .into_iter()
            .filter(|fragment| {
                fragment.members.iter().any(|hash| {
                    let (actor, seq) = self
                        .change_graph
                        .actor_seq_for_hash(hash)
                        .expect("cover member is applied");
                    known
                        .get_for_actor(&actor)
                        .is_none_or(|bound| bound.get() < seq)
                })
            })
            .map(|fragment| {
                let proof = signing
                    .fragment_proof(&self.change_graph, &self.authors, &fragment)
                    .expect("published cover fragments carry complete resident proof");
                let changes = self
                    .get_changes_by_hashes(fragment.members.iter().copied())
                    .expect("published members are applied");
                PublishedGroup {
                    fragment,
                    changes,
                    signatures: proof.into_entries(),
                }
            })
            .collect()
    }

    /// Heads of the published projection (the published subgraph's own heads).
    pub(crate) fn published_heads(&self) -> Vec<ChangeHash> {
        let SigningState::Required(signing) = &self.signing else {
            return self.get_heads();
        };
        self.change_graph.heads_at_clock(&signing.published.clock)
    }

    /// Encode the published projection as a snapshot plus one signatures chunk.
    pub(crate) fn save_published(&self, compress: CompressConfig) -> Vec<u8> {
        let SigningState::Required(signing) = &self.signing else {
            unreachable!("published save requires signing mode");
        };
        let cover = persistence::published_cover(&self.change_graph, &self.authors, signing);
        let entries: Vec<(ChangeHash, Signature)> = cover
            .iter()
            .flat_map(|fragment| {
                let requirements = self
                    .change_graph
                    .fragment_requirements(fragment.head)
                    .expect("cover head is applied");
                requirements
                    .hashes()
                    .map(|hash| {
                        let signature = signature_for(
                            &self.change_graph,
                            &self.authors,
                            &signing.signatures,
                            hash,
                        )
                        .expect("cover fragment has complete resident proof");
                        (*hash, Signature::ed25519(*signature))
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        let entries = HashEntries::canonicalize(entries)
            .expect("one signature per distinct frontier is within wire limits");

        let document = Document::at_clock(
            &self.ops,
            &self.change_graph,
            &signing.published.clock,
            compress,
        );
        let mut bytes = document.into_bytes();
        bytes.extend_from_slice(SignaturesChunk::encode(entries).bytes());
        bytes
    }

    /// Expand snapshot groups in a signed delivery into verified canonical
    /// groups, leaving change groups untouched. Bare snapshots are rejected.
    pub(crate) fn expand_signed_snapshots(
        &self,
        groups: Vec<LoadedGroup>,
    ) -> Result<Vec<LoadedGroup>, AutomergeError> {
        let mut expanded = Vec::with_capacity(groups.len());
        for group in groups {
            match group.body {
                LoadedGroupBody::Document(document) => {
                    let Some(signatures) = &group.signatures else {
                        return Err(SigningError::UnauthenticatedChanges.into());
                    };
                    let snapshot =
                        persistence::verify_snapshot(&document, signatures, self.text_encoding())?;
                    expanded.extend(snapshot.groups);
                }
                LoadedGroupBody::Changes(_) => expanded.push(group),
            }
        }
        Ok(expanded)
    }
}
