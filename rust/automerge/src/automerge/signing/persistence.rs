//! Published-only persistence for required-mode documents.
//!
//! Saving a signed document emits only *published* history: a document
//! snapshot of the published projection followed by one signatures chunk that
//! carries the complete proof of a selected published cover. Local drafts and
//! speculative pending groups stay in the live document. Nothing about the
//! published clock or the signing mode is serialized; a loader re-derives
//! publication from the snapshot's history and its attached proof.
//!
//! Loading a snapshot in required mode reconstructs the snapshot into an
//! untrusted scratch document, derives the canonical groups of that history,
//! verifies each group's complete proof against the attached entries, and runs
//! causal selection from an empty published clock. Only when every change of
//! the snapshot is published by that selection are the groups admitted into
//! the receiver through ordinary group admission.
//!
//! Incremental output and merge share [`crate::Automerge::published_groups`]: the
//! complete canonical groups (owned changes plus attached proofs) of the
//! published cover, each independently verifiable.

use std::collections::HashSet;

use crate::admission::{select_fragments, PublishedHistory};
use crate::change_graph::ChangeGraph;
use crate::signature::error::{ProofError, SigningError};
use crate::signature::{FragmentRequirements, Signature, SignatureStore};
use crate::storage::document::ReconstructError;
use crate::storage::load::{LoadedGroup, LoadedGroupBody};
use crate::storage::signatures::HashEntries;
use crate::storage::Document;
use crate::{Author, AutomergeError, Change, ChangeHash, Fragment, VerificationMode};

use super::{has_signature, RequiredSigning};

/// A complete canonical group of published history with its own proof.
///
/// Members are the exact original changes (bytes and hashes preserved), and
/// `signatures` holds every required frontier signature for the group. The
/// group carries complete proof of its own members; verifying it may still
/// need causal context (author bindings established by earlier groups), so
/// the *collection* of published groups, delivered oldest first, transfers to
/// an empty required peer.
#[derive(Clone, Debug)]
pub(crate) struct PublishedGroup {
    pub(crate) fragment: Fragment,
    pub(crate) changes: Vec<Change>,
    pub(crate) signatures: HashEntries,
}

impl From<PublishedGroup> for LoadedGroup {
    fn from(group: PublishedGroup) -> Self {
        LoadedGroup {
            body: LoadedGroupBody::Changes(group.changes),
            signatures: Some(group.signatures),
        }
    }
}

struct PublishedSet<'a>(&'a HashSet<ChangeHash>);

impl PublishedHistory for PublishedSet<'_> {
    fn contains(&self, hash: &ChangeHash) -> bool {
        self.0.contains(hash)
    }
}

/// Canonical candidates (historical hard fragments and loose commits) whose
/// members all lie in `published`, ordered by descending node rank of the
/// head (most recently applied head first; size plays no part).
fn published_candidates(graph: &ChangeGraph, published: &HashSet<ChangeHash>) -> Vec<Fragment> {
    let mut heads: Vec<ChangeHash> = published.iter().copied().collect();
    heads.sort_unstable_by_key(|head| std::cmp::Reverse(graph.node_rank(head).unwrap()));
    heads
        .into_iter()
        .filter_map(|head| graph.get_historical_fragment(head))
        .filter(|fragment| fragment.members.iter().all(|m| published.contains(m)))
        .collect()
}

/// Select the published proof cover through the same readiness machinery used
/// by fragment export and reclamation, filtering before cover selection.
///
/// # Panics
/// Published history must retain a complete proved cover. An internal violation
/// is rejected in every build rather than emitting unverifiable bytes.
pub(super) fn published_cover(
    graph: &ChangeGraph,
    authors: &crate::author::Authors,
    signing: &RequiredSigning,
) -> Vec<Fragment> {
    let cover = super::readiness::OutgoingSelection::new(graph, &signing.fragment_readiness)
        .collect_from(
            graph.heads_at_clock(&signing.published.clock),
            |hash| {
                graph.actor_seq_for_hash(hash).is_some_and(|(actor, seq)| {
                    signing.published.is_published_actor_seq(actor, seq)
                })
            },
            |hash| has_signature(graph, authors, &signing.signatures, hash),
        );
    let represented: HashSet<_> = cover
        .iter()
        .flat_map(|f| f.members.iter().copied())
        .collect();
    let expected: usize = graph
        .all_actor_ids()
        .map(|actor| {
            signing
                .published
                .clock
                .get_for_actor(&actor)
                .map_or(0, |seq| seq.get() as usize)
        })
        .sum();
    assert_eq!(
        represented.len(),
        expected,
        "published history must retain complete proof coverage"
    );
    cover
}

/// A snapshot whose history and attached proof have been validated, ready for
/// admission as independent canonical groups.
pub(super) struct VerifiedSnapshot {
    pub(super) groups: Vec<LoadedGroup>,
}

/// Validate a signed snapshot without touching the receiving document.
///
/// Reconstructs the snapshot into a scratch document (checking heads, actor
/// chains and author placement through the ordinary reconstruction rules),
/// derives canonical candidates from that history, keeps those whose complete
/// frontier proof is present and valid in `signatures`, and requires causal
/// selection from an empty clock to publish every change. Bad surplus
/// signatures are ignored; a missing or invalid required signature fails.
pub(super) fn verify_snapshot(
    document: &Document<'_>,
    signatures: &HashEntries,
    text_encoding: crate::TextEncoding,
) -> Result<VerifiedSnapshot, AutomergeError> {
    // Head verification is never relaxed for signed snapshots: the proof is
    // checked against reconstructed hashes, and `VerificationMode::DontCheck`
    // is a debugging aid for unsigned data, not a grant of trust.
    let scratch = document
        .reconstruct(VerificationMode::Check, text_encoding, Default::default())
        .map_err(|error| match error {
            ReconstructError::AuthorOnNonInitialSeq(seq, actor) => {
                AutomergeError::AuthorOnNonInitialSeq(seq, actor)
            }
            other => crate::storage::load::Error::InflateDocument(Box::new(other)).into(),
        })?;
    let graph = &scratch.change_graph;
    let all: HashSet<ChangeHash> = graph.applied_hashes().iter().copied().collect();
    if all.is_empty() {
        return Ok(VerifiedSnapshot { groups: Vec::new() });
    }
    // Every snapshot change must have a contiguous actor chain; reconstruction
    // keeps hashes exact so a tampered body changes hashes and fails heads.
    for hash in &all {
        let (actor, seq) = graph.actor_seq_for_hash(hash).unwrap();
        if seq > 1 {
            let predecessor = graph
                .get_hash_for_actor_seq(actor, u64::from(seq) - 1)
                .map_err(|_| {
                    SigningError::from(crate::signature::error::InvalidFragment::BrokenActorChain(
                        *hash,
                    ))
                })?;
            if !graph.deps(hash).any(|dep| dep == predecessor) {
                return Err(SigningError::from(
                    crate::signature::error::InvalidFragment::BrokenActorChain(*hash),
                )
                .into());
            }
        }
    }

    let author_for = |hash: &ChangeHash| -> Option<Author<'static>> {
        let (actor, _) = graph.actor_seq_for_hash(hash)?;
        scratch
            .authors
            .get_author_for_actor(actor)
            .map(Author::into_owned)
    };

    let mut verified: Vec<(Fragment, SignatureStore)> = Vec::new();
    let mut first_failure: Option<ProofError> = None;
    for fragment in published_candidates(graph, &all) {
        let requirements = graph
            .fragment_requirements(fragment.head)
            .expect("candidate head is applied in the scratch graph");
        match requirements.verify_entries(signatures, author_for) {
            Ok(proof) => verified.push((fragment, proof)),
            Err(error) => {
                first_failure.get_or_insert(error);
            }
        }
    }
    let fragments: Vec<Fragment> = verified.iter().map(|(f, _)| f.clone()).collect();
    let empty = HashSet::new();
    let selection = select_fragments(&PublishedSet(&empty), &fragments);
    let published: HashSet<ChangeHash> = selection
        .selected()
        .flat_map(|fragment| fragment.members.iter().copied())
        .collect();
    if let Some(missing) = all.iter().find(|hash| !published.contains(hash)) {
        let error = first_failure.unwrap_or(ProofError::MissingSignature(*missing));
        return Err(SigningError::from(error).into());
    }

    let selected: Vec<usize> = selection.selected_indices().collect();
    let groups = selected
        .into_iter()
        .map(|index| {
            let (fragment, proof) = &verified[index];
            let changes = scratch
                .get_changes_by_hashes(fragment.members.iter().copied())
                .expect("members are applied in the scratch graph");
            let entries = FragmentRequirements::for_members(
                changes.iter().map(|c| (c.actor_id(), c.seq(), c.hash())),
            )
            .hashes()
            .map(|hash| {
                let (_, signature) = proof
                    .ed25519_entries(hash)
                    .next()
                    .expect("verified proof holds every frontier");
                (*hash, Signature::ed25519(*signature))
            })
            .collect::<Vec<_>>();
            LoadedGroup {
                body: LoadedGroupBody::Changes(changes),
                signatures: Some(
                    HashEntries::canonicalize(entries).expect("one signature per frontier"),
                ),
            }
        })
        .collect();
    Ok(VerifiedSnapshot { groups })
}
