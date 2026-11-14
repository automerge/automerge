//! Document-owned signing state and access to required-mode operations.
//!
//! Create an empty required-mode document with [`Automerge::signing`], then
//! obtain a [`SigningDocument`] through [`Automerge::try_signing`]. The document
//! retains its accepted clock, readiness state, and required signatures between
//! handle borrows.

use std::collections::HashSet;
use std::ops::RangeBounds;

use crate::actor::{ActorRemoval, ActorShift, ActorTable};
use crate::admission::{self, AcceptedHistory};
use crate::author::Authors;
use crate::change_graph::ChangeGraph;
use crate::clock::SeqClock;
use crate::op_set2::OpSet;
use crate::signature::{ed25519, Insert, Signature, SignatureStore};
use crate::storage::signatures::{HashEntries, SignaturesChunk};
use crate::{ChangeHash, Fragment, TextEncoding};

use super::Automerge;

mod readiness;

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
    #[allow(dead_code)]
    pub(crate) fn is_required(&self) -> bool {
        matches!(self, Self::Required(_))
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

/// Long-lived required-mode data owned by the document.
#[derive(Clone, Debug)]
pub(crate) struct RequiredSigning {
    accepted: AcceptedClock,
    signatures: SignatureStore,
    fragment_readiness: FragmentReadiness,
}

impl RequiredSigning {
    fn new(actors: &ActorTable) -> Self {
        Self {
            accepted: AcceptedClock::new(actors),
            signatures: SignatureStore::default(),
            fragment_readiness: FragmentReadiness::default(),
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

    /// Reclaim covered fragment candidates and any signature records that no
    /// longer have an outgoing, incomplete-candidate, or unaccepted-local obligation.
    pub(super) fn reclaim_covered(&mut self, graph: &ChangeGraph) {
        let mut represented = self
            .fragment_readiness
            .needs_prune()
            .then(|| self.fragment_readiness.prune_covered(graph));
        let live_frontiers: HashSet<ChangeHash> = self
            .fragment_readiness
            .required_frontiers()
            .copied()
            .collect();

        if represented.is_none() {
            let needs_member_cover = self.signatures.verified_hashes().any(|hash| {
                hash.fragment_level() == 0
                    && !live_frontiers.contains(hash)
                    && graph.is_covered_by_fragment_cache(hash)
                    && graph
                        .actor_seq_for_hash(hash)
                        .is_some_and(|(actor, seq)| self.accepted.is_accepted_actor_seq(actor, seq))
            });
            if needs_member_cover {
                represented = Some(
                    OutgoingSelection::new(graph, &self.fragment_readiness).represented_members(),
                );
            }
        }

        let accepted = &self.accepted;
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
            if hash.fragment_level() > 0 {
                return false;
            }
            match &represented {
                Some(represented) => !represented.contains(hash),
                None => true,
            }
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
        Ok(inserted)
    }

    /// Whether this exact applied change belongs to accepted history.
    pub fn is_accepted(&self, hash: &ChangeHash) -> bool {
        self.graph
            .actor_seq_for_hash(hash)
            .is_some_and(|(actor, seq)| self.state.accepted.is_accepted_actor_seq(actor, seq))
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
    /// Create a new empty document in required signing mode.
    pub fn signing() -> Self {
        Self::signing_with_encoding(TextEncoding::platform_default())
    }

    pub(super) fn signing_with_encoding(encoding: TextEncoding) -> Self {
        let mut doc = Self::new_with_encoding(encoding);
        let signing = SigningState::Required(RequiredSigning::new(doc.actors()));
        doc.signing = signing;
        doc
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
