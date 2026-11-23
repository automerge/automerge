//! Document-owned signing state and access to required-mode operations.
//!
//! Create a required-mode document with [`crate::Config::signing`] and
//! [`Automerge::from_config`] (or load a
//! signed fragment stream with [`crate::LoadOptions::signing`]), then obtain a
//! [`SigningDocument`] through [`Automerge::try_signing`]. The document retains
//! its accepted clock, pending groups, and required signatures between borrows.

use std::collections::HashSet;
use std::ops::RangeBounds;

use crate::actor::{ActorRemoval, ActorShift, ActorTable};
use crate::admission::{select_fragments, AcceptedHistory};
use crate::author::Authors;
use crate::change_graph::ChangeGraph;
use crate::clock::SeqClock;
use crate::config::DocumentAuthor;
use crate::op_set2::OpSet;
use crate::signature::{ed25519, Insert, Signature, SignatureStore};
use crate::storage::signatures::{HashEntries, SignaturesChunk};
use crate::{Author, Change, ChangeHash, Fragment, PatchLog};

use super::Automerge;

mod requests;
use requests::LocalDrafts;
pub use requests::{SigningRequest, SigningRequests};

mod batch;
mod incoming;
mod readiness;

pub use batch::{
    PublicationError, SignatureBatchOutcome, SignatureInsertError, SignatureInsertOutcome,
    SignatureRejection,
};
use error::InsertSignature;
use incoming::{Incoming, PendingGroups, PreparedAdmission};
use readiness::{FragmentReadiness, OutgoingSelection};

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
            Self::Required(state) => Some(state.author.author()),
        }
    }

    pub(crate) fn fork_identity(
        &self,
        author: Option<Author<'static>>,
        pending: bool,
    ) -> Result<DocumentAuthor<'static>, crate::AutomergeError> {
        match self {
            Self::Unsigned { .. } => Ok(author.into()),
            Self::Required(state) => {
                let author = author.ok_or(crate::SigningAuthorError(
                    crate::error::SigningAuthorErrorInner::MissingAuthor,
                ))?;
                if &author == state.author.author() {
                    return Ok(DocumentAuthor::Verifier(state.author.clone()));
                }
                let prepared = DocumentAuthor::signing(author)?;
                if pending || !state.local_drafts.is_empty() {
                    return Err(
                        crate::signature::error::SigningError::UnpublishedLocalChanges.into(),
                    );
                }
                Ok(prepared)
            }
        }
    }

    pub(crate) fn set_fork_identity(&mut self, identity: DocumentAuthor<'static>) {
        match (self, identity) {
            (Self::Unsigned { author }, DocumentAuthor::Unset) => *author = None,
            (Self::Unsigned { author }, DocumentAuthor::Set(value)) => *author = Some(value),
            (Self::Required(state), DocumentAuthor::Verifier(value)) => state.author = value,
            _ => unreachable!("fork preserves signing mode"),
        }
    }

    pub(super) fn has_local_drafts(&self) -> bool {
        match self {
            Self::Unsigned { .. } => false,
            Self::Required(signing) => !signing.local_drafts.is_empty(),
        }
    }

    pub(crate) fn is_required(&self) -> bool {
        matches!(self, Self::Required(_))
    }

    pub(super) fn has_pending_groups(&self) -> bool {
        self.pending_group_count() > 0
    }

    pub(super) fn pending_group_count(&self) -> usize {
        match self {
            Self::Unsigned { .. } => 0,
            Self::Required(signing) => signing.pending.len(),
        }
    }

    pub(crate) fn require(&self) -> Result<(), error::SigningMode> {
        match self {
            Self::Unsigned { .. } => Err(error::SigningMode::Required),
            Self::Required(_) => Ok(()),
        }
    }

    pub(crate) fn with_required<E>(&self, f: impl FnOnce() -> Result<(), E>) -> Result<(), E> {
        match self {
            Self::Unsigned { .. } => Ok(()),
            Self::Required(_) => f(),
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
            signing.accepted.insert_actor(actor);
        }
    }

    pub(super) fn remove_actor(&mut self, actor: &ActorRemoval) {
        if let Self::Required(signing) = self {
            signing.accepted.remove_actor(actor);
        }
    }

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
    accepted: AcceptedClock,
    signatures: SignatureStore,
    fragment_readiness: FragmentReadiness,
    pending: PendingGroups,
    local_drafts: LocalDrafts,
}

impl RequiredSigning {
    fn new(actors: &ActorTable, author: ed25519::VerifyingAuthor<'static>) -> Self {
        Self {
            author,
            accepted: AcceptedClock::new(actors),
            signatures: SignatureStore::default(),
            fragment_readiness: FragmentReadiness::default(),
            pending: PendingGroups::default(),
            local_drafts: LocalDrafts::default(),
        }
    }

    pub(super) fn remove_published_drafts(&mut self, graph: &ChangeGraph) {
        let accepted = &self.accepted;
        self.local_drafts.remove(|hash| {
            graph
                .actor_seq_for_hash(hash)
                .is_some_and(|(actor, seq)| accepted.is_accepted_actor_seq(actor, seq))
        });
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

    /// Reclaim candidates and proofs only when a published cover replaces them.
    /// Ready-but-unpublished covers must leave published fallback proof intact.
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

fn signature_for<'a>(
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
#[derive(Debug)]
pub struct SigningDocument<'a> {
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
    /// Verify and insert an Ed25519 signature for an already-applied change.
    ///
    /// Prefer [`Automerge::insert_signatures`], which also retries pending
    /// incoming groups. This scalar operation only completes local publication.
    /// Uses the change's recorded author, not the current local author.
    /// Updates outgoing fragment readiness, accepts any local history made
    /// causally admissible by the new proof, and reclaims superseded signatures.
    pub fn insert_signature(
        &mut self,
        hash: &ChangeHash,
        signature: &ed25519::Signature,
    ) -> Result<Insert, InsertSignature> {
        let inserted = self.receive_signature(hash, signature)?;
        self.publish_local();
        Ok(inserted)
    }

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

    pub(super) fn publish_local(&mut self) {
        self.accept_local();
        self.state.reclaim_covered(self.graph);
        self.state.remove_published_drafts(self.graph);
    }

    /// Whether this exact applied change belongs to accepted history.
    pub fn is_accepted(&self, hash: &ChangeHash) -> bool {
        self.graph
            .actor_seq_for_hash(hash)
            .is_some_and(|(actor, seq)| self.state.accepted.is_accepted_actor_seq(actor, seq))
    }

    /// Return the locally authored commit hashes on this document that belong to
    /// its current [`Author`] and still need a signature, in hash order.
    /// Prefer [`Automerge::signing_requests`] for owned messages.
    ///
    /// Only commits created locally on this document are tracked here; changes
    /// imported from outside the document are never returned.
    pub fn required_signatures(&self) -> Vec<ChangeHash> {
        self.signing_requests().iter().map(|r| r.hash()).collect()
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
    /// Owned work for every unsigned local draft; imports never qualify.
    pub fn signing_requests(&mut self) -> Result<SigningRequests, error::SigningMode> {
        self.try_with_signing(|signing| signing.signing_requests())
    }

    /// Whether locally committed drafts await publication, including those with
    /// signatures but blocked dependencies. False in unsigned mode; ignores
    /// unapplied incoming groups, uncommitted operations and visibility masks.
    pub fn has_unpublished_changes(&self) -> bool {
        self.signing.has_local_drafts()
    }

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

    /// Verify each submitted signature against its change's historical author,
    /// then complete local publication and retry causally enabled incoming groups.
    /// Every input occurrence receives an owned outcome; completion does not mean
    /// all local history is published.
    pub fn insert_signatures(
        &mut self,
        inputs: impl IntoIterator<Item = (ChangeHash, ed25519::Signature)>,
    ) -> SignatureBatchOutcome {
        self.insert_signatures_log_patches(inputs, &mut PatchLog::inactive())
    }

    /// Like [`Self::insert_signatures`], recording incoming application in `log`.
    /// Wrong mode or an incompatible log rejects before consuming inputs.
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

    /// Retry while retained groups settle, including proof-only publication that
    /// adds no graph nodes. Local publication stays committed on planning errors.
    /// Every continued iteration removes a group, so the retry is bounded.
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

    pub(crate) fn with_signing<T: Default>(
        &mut self,
        operation: impl FnOnce(SigningDocument<'_>) -> T,
    ) -> T {
        self.try_with_signing(operation).unwrap_or_default()
    }

    pub(crate) fn try_with_signing<T>(
        &mut self,
        operation: impl FnOnce(SigningDocument<'_>) -> T,
    ) -> Result<T, error::SigningMode> {
        self.try_signing().map(operation)
    }

    /// Borrow this document's [`SigningDocument`] handle.
    pub fn try_signing(&mut self) -> Result<SigningDocument<'_>, error::SigningMode> {
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
