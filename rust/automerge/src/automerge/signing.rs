//! Document-owned signing state and access to required-mode operations.
//!
//! Create an empty required-mode document with [`Automerge::signing`] (or load a
//! signed fragment stream with [`crate::LoadOptions::signing`]), then obtain a
//! [`SigningDocument`] through [`Automerge::try_signing`]. The document retains
//! its accepted clock, pending groups, and required signatures between borrows.
//!
//! A required-mode document always has a current [`Author`] whose bytes form a
//! usable Ed25519 verifying key. Construction and author setters validate this
//! before changing any state; see [`crate::SigningAuthorError`].

use std::collections::HashSet;
use std::ops::RangeBounds;

use crate::actor::{ActorRemoval, ActorShift, ActorTable};
use crate::admission::{self, AcceptedHistory};
use crate::author::Authors;
use crate::change_graph::ChangeGraph;
use crate::clock::SeqClock;
use crate::error::SigningAuthorErrorInner;
use crate::op_set2::OpSet;
use crate::signature::{ed25519, Insert, Signature, SignatureStore};
use crate::storage::signatures::{HashEntries, SignaturesChunk};
use crate::{Author, Change, ChangeHash, Fragment, SigningAuthorError, TextEncoding};

use super::Automerge;

mod incoming;
mod readiness;

use incoming::PendingGroups;
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

    /// Check that `author` may become the document's current author in this mode.
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

    pub(super) fn has_pending_groups(&self) -> bool {
        match self {
            Self::Unsigned => false,
            Self::Required(signing) => !signing.pending.is_empty(),
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

    pub(super) fn record_local_change(&mut self, hash: ChangeHash) {
        if let Self::Required(signing) = self {
            signing.local_unsigned.push(hash);
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
    local_unsigned: Vec<ChangeHash>,
}

impl RequiredSigning {
    fn new(actors: &ActorTable) -> Self {
        Self {
            accepted: AcceptedClock::new(actors),
            signatures: SignatureStore::default(),
            fragment_readiness: FragmentReadiness::default(),
            pending: PendingGroups::default(),
            local_unsigned: Vec::new(),
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
///
/// Holding a handle guarantees the document's current author has been prepared
/// as a usable Ed25519 verifying key.
#[derive(Debug)]
pub struct SigningDocument<'a> {
    graph: &'a ChangeGraph,
    ops: &'a OpSet,
    authors: &'a Authors,
    author: ed25519::VerifyingAuthor<'a>,
    state: &'a mut RequiredSigning,
}

impl<'a> SigningDocument<'a> {
    /// Verify and insert an Ed25519 signature for an already-applied change.
    ///
    /// Uses the change's recorded author, not the current local author.
    /// Updates outgoing fragment readiness, accepts any local history made
    /// causally admissible by the new proof, and reclaims superseded signatures.
    pub fn insert_signature(
        &mut self,
        hash: &ChangeHash,
        signature: &ed25519::Signature,
    ) -> Result<Insert, error::InsertSignature> {
        let (actor, _) = self
            .graph
            .actor_seq_for_hash(hash)
            .ok_or(error::InsertSignature::UnknownChange { change: *hash })?;
        let actor_id = &self.ops.actors[actor];
        let author = self.authors.get_author_for_actor(actor).ok_or_else(|| {
            error::InsertSignature::MissingAuthor {
                change: *hash,
                actor: actor_id.clone(),
            }
        })?;
        let verifier = ed25519::VerifyingAuthor::new(author).map_err(|e| {
            error::InsertSignature::InvalidAuthorKey {
                change: *hash,
                actor: actor_id.clone(),
                source: e,
            }
        })?;
        let witness = verifier.verify_hash(hash, signature).map_err(|e| {
            error::InsertSignature::Verification {
                change: *hash,
                source: e,
            }
        })?;
        let had_evidence = has_signature(self.graph, self.authors, &self.state.signatures, hash);
        let inserted = self.state.signatures.insert_ed25519(witness).map_err(|e| {
            error::InsertSignature::Store {
                change: *hash,
                source: e,
            }
        })?;
        if !had_evidence && has_signature(self.graph, self.authors, &self.state.signatures, hash) {
            self.state.fragment_readiness.signature_added(hash);
            self.accept_local();
            self.state.reclaim_covered(self.graph);
        }
        let graph = self.graph;
        let accepted = &self.state.accepted;
        self.state.local_unsigned.retain(|pending| {
            pending != hash
                && !graph
                    .actor_seq_for_hash(pending)
                    .is_some_and(|(actor, seq)| accepted.is_accepted_actor_seq(actor, seq))
        });
        Ok(inserted)
    }

    /// Whether this exact applied change belongs to accepted history.
    pub fn is_accepted(&self, hash: &ChangeHash) -> bool {
        self.graph
            .actor_seq_for_hash(hash)
            .is_some_and(|(actor, seq)| self.state.accepted.is_accepted_actor_seq(actor, seq))
    }

    /// Return the locally authored commit hashes on this document that belong to
    /// its current [`Author`] and still need a signature, in commit order.
    ///
    /// Only commits created locally on this document are tracked here; changes
    /// imported from outside the document are never returned.
    pub fn required_signatures(&self) -> Vec<ChangeHash> {
        let current_author = self.author.author().as_borrowed();
        self.state
            .local_unsigned
            .iter()
            .copied()
            .filter(|hash| {
                self.graph
                    .actor_seq_for_hash(hash)
                    .and_then(|(actor, _)| self.authors.get_author_for_actor(actor))
                    .is_some_and(|author| author == current_author)
            })
            .collect()
    }

    fn accept_local(&mut self) {
        let fragments = self.local_candidate_fragments();
        let selection = admission::select_fragments(&*self, &fragments);
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
                self.is_accepted(hash)
                    || has_signature(self.graph, self.authors, &self.state.signatures, hash)
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
        let mut doc = Self::new_with_encoding(encoding);
        doc.signing = SigningState::Required(RequiredSigning::new(doc.actors()));
        doc.author = Some(author);
        Ok(doc)
    }

    /// Check that `author` may become this document's current author.
    ///
    /// See [`SigningState::validate_author`].
    pub(crate) fn validate_author(
        &self,
        author: Option<&Author<'static>>,
    ) -> Result<(), SigningAuthorError> {
        self.signing.validate_author(author)
    }

    /// Borrow this document's [`SigningDocument`] handle.
    ///
    /// # Errors
    ///
    /// Fails if the document is not in required signing mode, or if its current
    /// author is missing or unusable. The latter indicates the author invariant
    /// was bypassed; constructors and setters never produce it.
    pub fn try_signing(&mut self) -> Result<SigningDocument<'_>, error::SigningAccess> {
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

    /// Failure to obtain a [`super::SigningDocument`] handle.
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
