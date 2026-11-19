//! Published-only persistence for required-mode documents.
//!
//! Saving a signed document emits only *published* history: a document
//! snapshot of the published projection followed by one signatures chunk that
//! carries the complete proof of a selected published cover. Local drafts and
//! speculative pending groups stay in the live document. Nothing about the
//! accepted clock or the signing mode is serialized; a loader re-derives
//! publication from the snapshot's history and its attached proof.
//!
//! Loading a snapshot in required mode reconstructs the snapshot into an
//! untrusted scratch document, derives the canonical groups of that history,
//! verifies each group's complete proof against the attached entries, and runs
//! causal selection from an empty published clock. Only when every change of
//! the snapshot is published by that selection are the groups admitted into
//! the receiver through ordinary group admission.
//!
//! Incremental output and merge share [`Automerge::published_groups`]: the
//! complete canonical groups (owned changes plus attached proofs) of the
//! published cover, each independently verifiable.

use std::collections::{BTreeSet, HashSet};

use crate::admission::{self, AcceptedHistory};
use crate::change_graph::ChangeGraph;
use crate::signature::error::{ProofError, SigningError};
use crate::signature::{FragmentRequirements, Signature, SignatureStore};
use crate::storage::document::{CompressConfig, ReconstructError};
use crate::storage::load::{LoadedGroup, LoadedGroupBody};
use crate::storage::signatures::{HashEntries, SignaturesChunk};
use crate::storage::Document;
use crate::{Author, Automerge, AutomergeError, Change, ChangeHash, Fragment, VerificationMode};

use super::{has_signature, RequiredSigning, SigningState};

/// A complete canonical group of published history with its own proof.
///
/// Members are the exact original changes (bytes and hashes preserved), and
/// `signatures` holds every required frontier signature for the group, so the
/// group verifies on an empty required peer without resident evidence.
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

struct AcceptedSet<'a>(&'a HashSet<ChangeHash>);

impl AcceptedHistory for AcceptedSet<'_> {
    fn contains(&self, hash: &ChangeHash) -> bool {
        self.0.contains(hash)
    }
}

/// Every applied hash that lies within the accepted clock.
fn published_hashes(graph: &ChangeGraph, signing: &RequiredSigning) -> HashSet<ChangeHash> {
    graph
        .applied_hashes()
        .iter()
        .filter(|hash| {
            graph
                .actor_seq_for_hash(hash)
                .is_some_and(|(actor, seq)| signing.accepted.is_accepted_actor_seq(actor, seq))
        })
        .copied()
        .collect()
}

/// Canonical candidates (historical hard fragments and loose commits) whose
/// members all lie in `published`, newest first.
fn published_candidates(graph: &ChangeGraph, published: &HashSet<ChangeHash>) -> Vec<Fragment> {
    let mut heads: Vec<ChangeHash> = published.iter().copied().collect();
    heads.sort_unstable_by_key(|head| std::cmp::Reverse(graph.node_rank(head).unwrap()));
    heads
        .into_iter()
        .filter_map(|head| graph.get_historical_fragment(head))
        .filter(|fragment| fragment.members.iter().all(|m| published.contains(m)))
        .collect()
}

/// Greedy cover of `published` by proved candidates, largest/newest first.
///
/// Candidates are filtered to published membership *before* selection, so an
/// unpublished (merely ready) cover can never hide a published fallback. Every
/// returned fragment has a complete resident proof.
fn published_cover(
    graph: &ChangeGraph,
    authors: &crate::author::Authors,
    signing: &RequiredSigning,
    published: &HashSet<ChangeHash>,
) -> Vec<Fragment> {
    let mut represented: HashSet<ChangeHash> = HashSet::new();
    let mut cover = Vec::new();
    for fragment in published_candidates(graph, published) {
        if fragment.members.iter().all(|m| represented.contains(m)) {
            continue;
        }
        let Some(requirements) = graph.fragment_requirements(fragment.head) else {
            continue;
        };
        if !requirements
            .is_satisfied_by(|hash| has_signature(graph, authors, &signing.signatures, hash))
        {
            continue;
        }
        represented.extend(fragment.members.iter().copied());
        cover.push(fragment);
    }
    debug_assert!(
        published.iter().all(|hash| represented.contains(hash)),
        "published history is always covered by proved candidates"
    );
    cover
}

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
        let published = published_hashes(&self.change_graph, signing);
        let mut needed: BTreeSet<ChangeHash> = published.iter().copied().collect();
        self.filter_changes(known_heads, &mut needed)
            .expect("filtering applied hashes cannot fail");
        published_cover(&self.change_graph, &self.authors, signing, &published)
            .into_iter()
            .filter(|fragment| fragment.members.iter().any(|m| needed.contains(m)))
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

    /// Heads of the published projection (the heads of the accepted subgraph).
    pub(crate) fn published_heads(&self) -> Vec<ChangeHash> {
        let SigningState::Required(signing) = &self.signing else {
            return self.get_heads();
        };
        let published = published_hashes(&self.change_graph, signing);
        let mut has_dependent = HashSet::new();
        for hash in &published {
            for dep in self.change_graph.deps(hash) {
                has_dependent.insert(dep);
            }
        }
        let mut heads: Vec<_> = published
            .into_iter()
            .filter(|hash| !has_dependent.contains(hash))
            .collect();
        heads.sort_unstable();
        heads
    }

    /// Encode the published projection as a snapshot plus one signatures chunk.
    pub(crate) fn save_published(&self, compress: CompressConfig) -> Vec<u8> {
        let SigningState::Required(signing) = &self.signing else {
            unreachable!("published save requires signing mode");
        };
        let published = published_hashes(&self.change_graph, signing);
        let cover = published_cover(&self.change_graph, &self.authors, signing, &published);
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
                        let signature = super::signature_for(
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

        // Project published history into a scratch unsigned document that is
        // reconstruction, not local commits: no provenance or drafts arise.
        let mut projection = Automerge::new_with_encoding(self.text_encoding());
        let mut ordered: Vec<ChangeHash> = published.into_iter().collect();
        ordered.sort_unstable_by_key(|hash| self.change_graph.node_rank(hash).unwrap());
        let changes = self
            .get_changes_by_hashes(ordered)
            .expect("published members are applied");
        projection
            .apply_changes(changes)
            .expect("published history is dependency-closed and already validated");
        debug_assert!(projection.staged.is_empty());

        let document = Document::new(&projection.ops, &projection.change_graph, compress);
        let mut bytes = document.into_bytes();
        bytes.extend_from_slice(SignaturesChunk::encode(entries).bytes());
        bytes
    }
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
    let selection = admission::select_fragments(&AcceptedSet(&empty), &fragments);
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

impl Automerge {
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
                    let snapshot = verify_snapshot(&document, signatures, self.text_encoding())?;
                    expanded.extend(snapshot.groups);
                }
                LoadedGroupBody::Changes(_) => expanded.push(group),
            }
        }
        Ok(expanded)
    }
}
