//! Validate, hold, and admit incoming content/proof groups in signing mode.
//!
//! Incoming admission runs in five stages:
//! 1. **Intrinsic group validation** (`validate_incoming_batch`): checks single-headed
//!    reachability, level bounds, intra-group actor chains, and attached proof
//!    presence (plus cryptographic verification for any already-known authors)
//!    before touching document state.
//! 2. **Causal ancestry & author resolution** (`resolve_unapplied_ancestry`):
//!    topologically orders unapplied changes reachable from `ChangeGraph`, checks
//!    cross-group `(actor, seq - 1)` links, and inherits `Author`s from `seq == 1`.
//! 3. **Canonical fragment & proof verification** (`verify_resolved_groups`):
//!    keeps groups with missing ancestors waiting, and checks resolved groups
//!    against DAG-derived canonical membership and their own attached signatures.
//! 4. **Causal selection & equivocation check** (`plan_group_admission`):
//!    selects the maximal subset causally closed down to `AcceptedClock` and
//!    orders unapplied changes topologically before any mutation.
//! 5. **Completion**: the document coordinator applies borrowed selected
//!    contents, then `PreparedAdmission::complete` merges proofs, publishes
//!    history and removes completed or invalidated pending groups.

use std::collections::{BTreeMap, HashMap, HashSet};

use crate::admission::{select_fragments, AcceptedHistory};
use crate::change_graph::ChangeGraph;
use crate::signature::error::{InvalidFragment, ProofError, SigningError};
use crate::signature::{
    assemble_frontiers, ed25519, verify_frontier_entry, verify_prepared_frontier,
    FragmentRequirements, Signature, SignatureStore,
};
use crate::storage::load::{LoadedGroup, LoadedGroupBody};
use crate::storage::signatures::HashEntries;
use crate::{ActorId, Author, AutomergeError, Change, ChangeHash, Fragment};

use super::{has_signature, SigningDocument};

mod actor_chains;
mod actor_slots;
mod apply_order;
mod author_checks;
mod boundary_summary;
mod group_membership;
mod topology;
mod walk;

use walk::{walk, Visit};

/// An intrinsically validated incoming group waiting for missing causal history.
#[derive(Clone, Debug)]
pub(super) struct PendingGroup {
    head: ChangeHash,
    changes: Vec<Change>,
    signatures: HashEntries,
    /// Frontiers already cryptographically verified against established authors.
    preverified: SignatureStore,
}

impl PendingGroup {
    fn same_delivery(&self, other: &Self) -> bool {
        if self.head != other.head
            || self.changes.len() != other.changes.len()
            || self.signatures != other.signatures
        {
            return false;
        }
        let left: HashSet<_> = self.changes.iter().map(Change::hash).collect();
        self.changes.len() == left.len()
            && other
                .changes
                .iter()
                .all(|change| left.contains(&change.hash()))
    }
}

/// Causally blocked incoming groups retained with group-scoped proof entries.
#[derive(Clone, Debug, Default)]
pub(super) struct PendingGroups {
    pub(super) groups: Vec<PendingGroup>,
}

impl PendingGroups {
    pub(super) fn len(&self) -> usize {
        self.groups.len()
    }

    pub(super) fn queued_changes(&self) -> impl Iterator<Item = &Change> {
        self.groups.iter().flat_map(|group| group.changes.iter())
    }

    fn push_dedup(&mut self, incoming: PendingGroup) {
        if !self
            .groups
            .iter()
            .any(|existing| existing.same_delivery(&incoming))
        {
            self.groups.push(incoming);
        }
    }
}

/// Whether a candidate came from an earlier call's pending queue or the current call.
///
/// A newly supplied group that fails validation aborts the current call with an
/// error. A previously queued orphan whose missing author/ancestry arrives later
/// and disproves it is silently dropped so forged orphans cannot block honest
/// history delivered in subsequent calls.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Origin {
    ExistingPending,
    IncomingBatch,
}

struct CandidateGroup {
    origin: Origin,
    group: PendingGroup,
}

/// Unapplied candidate changes reachable from `ChangeGraph` without missing
/// dependencies, annotated with topological order and inherited actor authors.
struct ResolvedAncestry<'a> {
    changes_by_hash: HashMap<ChangeHash, &'a Change>,
    resolved_topo: Vec<ChangeHash>,
    topo_rank: HashMap<ChangeHash, usize>,
    valid_authors: HashMap<ChangeHash, Option<Author<'static>>>,
    broken_chain_cause: HashMap<ChangeHash, ChangeHash>,
    has_any_equivocation: bool,
}

type BoundarySummary = boundary_summary::Summary<ActorId, ChangeHash>;

/// Memoized boundary causal pasts shared across fragment checks in one batch.
#[derive(Default)]
struct BoundaryPastMemo {
    by_boundary: HashMap<ChangeHash, BoundarySummary>,
}

enum GroupEvaluation {
    WaitingForAncestry,
    Verified {
        fragment: Fragment,
        proof: SignatureStore,
    },
}

struct VerifiedGroup {
    candidate_index: usize,
    fragment: Fragment,
    proof: SignatureStore,
}

struct EvaluatedGroups {
    verified: Vec<VerifiedGroup>,
    waiting_candidate_indices: Vec<usize>,
}

struct AdmissionPlan {
    selected_verified_indices: Vec<usize>,
    ordered_unapplied_hashes: Vec<ChangeHash>,
    accepted_member_hashes: Vec<ChangeHash>,
    remaining_candidate_indices: HashSet<usize>,
}

struct AcceptedGraph<'a> {
    graph: &'a ChangeGraph,
    signing: &'a super::RequiredSigning,
}

impl AcceptedHistory for AcceptedGraph<'_> {
    fn contains(&self, hash: &ChangeHash) -> bool {
        self.graph
            .actor_seq_for_hash(hash)
            .is_some_and(|(actor, seq)| self.signing.accepted.is_accepted_actor_seq(actor, seq))
    }
}

/// Required-mode view for incoming validation and causal planning.
pub(super) struct Incoming<'a> {
    pub(super) graph: &'a ChangeGraph,
    pub(super) ops: &'a crate::op_set2::OpSet,
    pub(super) authors: &'a crate::author::Authors,
    pub(super) state: &'a super::RequiredSigning,
}

impl Incoming<'_> {
    fn has_actor_seq(&self, change: &Change) -> bool {
        self.ops
            .lookup_actor(change.actor_id())
            .is_some_and(|actor| self.graph.seq_for_actor(actor) >= change.seq())
    }

    fn actor_slot<'a>(&self, change: &'a Change) -> ((&'a ActorId, u64), bool) {
        (
            (change.actor_id(), change.seq()),
            self.has_actor_seq(change),
        )
    }

    /// Plan without mutating pending history. Blocked groups remain pending;
    /// disproved old groups are traced and dropped on completion. Invalid new
    /// input or planning conflicts leave the document's pending owner unchanged.
    pub(super) fn prepare(
        &self,
        groups: Vec<LoadedGroup>,
    ) -> Result<PreparedAdmission, AutomergeError> {
        let incoming = self.validate_incoming_batch(groups)?;
        let candidates =
            Self::collect_candidate_groups(self.state.pending.groups.clone(), incoming);
        let ancestry = self.resolve_unapplied_ancestry(&candidates);
        let evaluated = self.verify_resolved_groups(&candidates, &ancestry)?;
        let plan = self.plan_group_admission(&candidates, &ancestry, &evaluated)?;
        Ok(PreparedAdmission {
            candidates,
            verified: evaluated.verified,
            plan,
        })
    }

    // --- Stage 1: Intrinsic per-group validation ---

    fn validate_incoming_batch(
        &self,
        groups: Vec<LoadedGroup>,
    ) -> Result<Vec<PendingGroup>, AutomergeError> {
        let mut incoming_actor_seqs: HashMap<(ActorId, u64), ChangeHash> = HashMap::new();
        let mut incoming_actor_authors: HashMap<ActorId, Option<Author<'static>>> = HashMap::new();
        let mut prepared_keys: HashMap<Author<'static>, ed25519::VerifyingAuthor<'static>> =
            HashMap::new();

        for group in &groups {
            if let LoadedGroupBody::Changes(changes) = &group.body {
                for change in changes {
                    if change.seq() == 1 && !self.graph.has_change(&change.hash()) {
                        incoming_actor_authors
                            .entry(change.actor_id().clone())
                            .or_insert_with(|| change.author().map(Author::into_owned));
                    }
                }
            }
        }

        groups
            .into_iter()
            .map(|group| {
                self.validate_incoming_group(
                    group,
                    &mut incoming_actor_seqs,
                    &incoming_actor_authors,
                    &mut prepared_keys,
                )
            })
            .collect()
    }

    fn validate_incoming_group(
        &self,
        group: LoadedGroup,
        incoming_actor_seqs: &mut HashMap<(ActorId, u64), ChangeHash>,
        incoming_actor_authors: &HashMap<ActorId, Option<Author<'static>>>,
        prepared_keys: &mut HashMap<Author<'static>, ed25519::VerifyingAuthor<'static>>,
    ) -> Result<PendingGroup, AutomergeError> {
        let changes = match group.body {
            // Signed snapshots are expanded into verified canonical groups
            // before admission; a snapshot reaching this point is bare.
            LoadedGroupBody::Document(_) => return Err(SigningError::UnauthenticatedChanges.into()),
            LoadedGroupBody::Changes(changes) => changes,
        };
        if changes.is_empty() {
            return Err(SigningError::from(InvalidFragment::Empty).into());
        }

        let in_group = self.check_group_members(&changes, incoming_actor_seqs)?;
        let head = Self::find_single_reachable_head(&changes, &in_group)?;
        let group_seq1_authors = Self::check_intra_group_actor_chains(&changes)?;
        let Some(signatures) = group.signatures else {
            let requirements = FragmentRequirements::for_changes(&changes);
            let missing = *requirements.hashes().next().unwrap();
            return Err(SigningError::from(ProofError::MissingSignature(missing)).into());
        };
        let preverified = self.verify_known_author_frontiers(
            &changes,
            &in_group,
            &signatures,
            &group_seq1_authors,
            incoming_actor_authors,
            prepared_keys,
        )?;

        Ok(PendingGroup {
            head,
            changes,
            signatures,
            preverified,
        })
    }

    fn check_group_members<'a>(
        &self,
        changes: &'a [Change],
        incoming_actor_seqs: &mut HashMap<(ActorId, u64), ChangeHash>,
    ) -> Result<HashMap<ChangeHash, &'a Change>, AutomergeError> {
        let mut in_group = HashMap::with_capacity(changes.len());
        for change in changes {
            let seq = change.seq();
            if seq == 0 || u32::try_from(seq).is_err() {
                return Err(AutomergeError::InvalidSeq(seq));
            }
            if change.author().is_some() && seq != 1 {
                return Err(AutomergeError::author_on_non_initial_seq(change));
            }
            let hash = change.hash();
            if in_group.insert(hash, change).is_some() {
                return Err(SigningError::from(InvalidFragment::DuplicateMember(hash)).into());
            }
            if !self.graph.has_change(&hash) && self.has_actor_seq(change) {
                return Err(AutomergeError::duplicate_seq(change));
            }
            let key = (change.actor_id().clone(), seq);
            if let Some(&existing_hash) = incoming_actor_seqs.get(&key) {
                if existing_hash != hash {
                    return Err(AutomergeError::duplicate_seq(change));
                }
            } else {
                incoming_actor_seqs.insert(key, hash);
            }
        }
        Ok(in_group)
    }

    fn find_single_reachable_head(
        changes: &[Change],
        in_group: &HashMap<ChangeHash, &Change>,
    ) -> Result<ChangeHash, AutomergeError> {
        let mut has_dependent = HashSet::new();
        for change in changes {
            for dep in change.deps() {
                if in_group.contains_key(dep) {
                    has_dependent.insert(*dep);
                }
            }
        }
        let mut heads = changes
            .iter()
            .filter(|change| !has_dependent.contains(&change.hash()));
        let (Some(head_change), None) = (heads.next(), heads.next()) else {
            return Err(SigningError::from(InvalidFragment::NotSingleHeaded).into());
        };
        let head = head_change.hash();

        let reachable = walk(
            vec![head],
            |node| {
                in_group[&node]
                    .deps()
                    .iter()
                    .copied()
                    .filter(|dep| in_group.contains_key(dep))
                    .collect()
            },
            |_| Visit::KeepAndExpand,
        );
        if reachable.len() != changes.len() {
            return Err(SigningError::from(InvalidFragment::NotSingleHeaded).into());
        }

        let level = head.fragment_level();
        if level == 0 {
            if changes.len() != 1 {
                return Err(SigningError::from(InvalidFragment::NonSingletonLoose(head)).into());
            }
        } else {
            for change in changes {
                let member = change.hash();
                if member != head && member.fragment_level() >= level {
                    return Err(SigningError::from(InvalidFragment::InvalidMemberLevel {
                        head,
                        member,
                    })
                    .into());
                }
            }
        }
        Ok(head)
    }

    fn check_intra_group_actor_chains(
        changes: &[Change],
    ) -> Result<HashMap<&ActorId, Option<Author<'_>>>, AutomergeError> {
        let mut by_actor: BTreeMap<&ActorId, Vec<&Change>> = BTreeMap::new();
        for change in changes {
            by_actor.entry(change.actor_id()).or_default().push(change);
        }
        let mut group_seq1_authors = HashMap::new();
        for (actor, actor_changes) in &mut by_actor {
            actor_changes.sort_unstable_by_key(|change| change.seq());
            if actor_changes[0].seq() == 1 {
                group_seq1_authors.insert(*actor, actor_changes[0].author());
            }
            for pair in actor_changes.windows(2) {
                if pair[1].seq() != pair[0].seq() + 1 || !pair[1].deps().contains(&pair[0].hash()) {
                    return Err(SigningError::from(InvalidFragment::BrokenActorChain(
                        pair[1].hash(),
                    ))
                    .into());
                }
            }
        }
        Ok(group_seq1_authors)
    }

    fn verify_known_author_frontiers(
        &self,
        changes: &[Change],
        in_group: &HashMap<ChangeHash, &Change>,
        signatures: &HashEntries,
        group_seq1_authors: &HashMap<&ActorId, Option<Author<'_>>>,
        incoming_actor_authors: &HashMap<ActorId, Option<Author<'static>>>,
        prepared_keys: &mut HashMap<Author<'static>, ed25519::VerifyingAuthor<'static>>,
    ) -> Result<SignatureStore, AutomergeError> {
        let requirements = FragmentRequirements::for_changes(changes);
        let (preverified, _) = assemble_frontiers(
            requirements.hashes().as_slice(),
            (SignatureStore::default(), prepared_keys),
            |hash, (mut preverified, prepared_keys)| -> Result<_, AutomergeError> {
                let frontier = &hash;
                let actor = in_group[frontier].actor_id();
                if signatures
                    .for_hash(frontier)
                    .filter_map(Signature::as_ed25519)
                    .next()
                    .is_none()
                {
                    return Err(SigningError::from(ProofError::MissingSignature(*frontier)).into());
                }

                let known_author: Option<Option<Author<'_>>> =
                    if let Some(actor_idx) = self.ops.lookup_actor(actor) {
                        (self.graph.seq_for_actor(actor_idx) > 0)
                            .then(|| self.authors.get_author_for_actor(actor_idx))
                    } else if let Some(author) = group_seq1_authors.get(actor) {
                        Some(author.clone())
                    } else {
                        incoming_actor_authors
                            .get(actor)
                            .map(|opt| opt.as_ref().map(Author::as_borrowed))
                    };

                if let Some(expected_author) = known_author {
                    let author = expected_author
                        .ok_or_else(|| SigningError::from(ProofError::MissingAuthor(*frontier)))?
                        .into_owned();
                    let verifier = match prepared_keys.get(&author) {
                        Some(verifier) => verifier,
                        None => {
                            let prepared = ed25519::VerifyingAuthor::new(author.clone()).map_err(
                                |source| {
                                    SigningError::from(ProofError::InvalidAuthorKey {
                                        hash: *frontier,
                                        source,
                                    })
                                },
                            )?;
                            prepared_keys.entry(author).or_insert(prepared)
                        }
                    };
                    let witness = verify_prepared_frontier(signatures, frontier, verifier)
                        .map_err(SigningError::from)?;
                    preverified
                        .insert_ed25519(witness)
                        .expect("each frontier has one fixed author");
                }
                Ok((preverified, prepared_keys))
            },
        )?;
        Ok(preverified)
    }

    // --- Stage 2: Causal ancestry & actor-author resolution ---

    fn collect_candidate_groups(
        existing_pending: Vec<PendingGroup>,
        incoming: Vec<PendingGroup>,
    ) -> Vec<CandidateGroup> {
        let mut candidates = Vec::with_capacity(existing_pending.len() + incoming.len());
        candidates.extend(existing_pending.into_iter().map(|group| CandidateGroup {
            origin: Origin::ExistingPending,
            group,
        }));
        candidates.extend(incoming.into_iter().map(|group| CandidateGroup {
            origin: Origin::IncomingBatch,
            group,
        }));
        candidates
    }

    fn resolve_unapplied_ancestry<'a>(
        &self,
        candidates: &'a [CandidateGroup],
    ) -> ResolvedAncestry<'a> {
        let mut changes_by_hash: HashMap<ChangeHash, &'a Change> = HashMap::new();
        for candidate in candidates {
            for change in &candidate.group.changes {
                let hash = change.hash();
                if !self.graph.has_change(&hash) {
                    changes_by_hash.entry(hash).or_insert(change);
                }
            }
        }

        let hashes: Vec<_> = changes_by_hash.keys().copied().collect();
        let has_any_equivocation =
            actor_slots::find_conflict(&hashes, |hash| self.actor_slot(changes_by_hash[&hash]))
                .is_some();
        let (resolved_topo, topo_rank) = self.topological_sort_unapplied(&hashes, &changes_by_hash);
        let (valid_authors, broken_chain_cause) =
            self.propagate_actor_authors(&resolved_topo, &topo_rank, &changes_by_hash);

        ResolvedAncestry {
            changes_by_hash,
            resolved_topo,
            topo_rank,
            valid_authors,
            broken_chain_cause,
            has_any_equivocation,
        }
    }

    fn topological_sort_unapplied(
        &self,
        hashes: &[ChangeHash],
        changes_by_hash: &HashMap<ChangeHash, &Change>,
    ) -> (Vec<ChangeHash>, HashMap<ChangeHash, usize>) {
        // Give every unapplied candidate an index for the worklist. Each
        // unapplied dependency occurrence contributes one to the count;
        // exactly those with a candidate parent have a reverse edge to
        // subtract later. Applied dependencies contribute neither.
        let index_by_hash: HashMap<_, _> = hashes
            .iter()
            .enumerate()
            .map(|(index, &hash)| (hash, index))
            .collect();
        let mut waiting = vec![0; hashes.len()];
        let mut dependents = vec![Vec::new(); hashes.len()];
        for (index, hash) in hashes.iter().enumerate() {
            for dep in changes_by_hash[hash].deps() {
                if !self.graph.has_change(dep) {
                    waiting[index] += 1;
                    if let Some(&parent) = index_by_hash.get(dep) {
                        dependents[parent].push(index);
                    }
                }
            }
        }
        drop(index_by_hash);

        let resolved_topo: Vec<_> = topology::resolve(&dependents, waiting)
            .into_iter()
            .map(|index| hashes[index])
            .collect();
        let topo_rank = resolved_topo
            .iter()
            .enumerate()
            .map(|(rank, &hash)| (hash, rank))
            .collect();
        (resolved_topo, topo_rank)
    }

    fn propagate_actor_authors(
        &self,
        resolved_topo: &[ChangeHash],
        topo_rank: &HashMap<ChangeHash, usize>,
        changes_by_hash: &HashMap<ChangeHash, &Change>,
    ) -> (
        HashMap<ChangeHash, Option<Author<'static>>>,
        HashMap<ChangeHash, ChangeHash>,
    ) {
        use actor_chains::{AuthorSource, Candidate, Dependency, Resolution};

        let rows: Vec<_> = resolved_topo
            .iter()
            .map(|hash| {
                let change = changes_by_hash[hash];
                Candidate {
                    actor: change.actor_id(),
                    seq: change.seq(),
                    actor_already_applied: change.seq() == 1 && self.has_actor_seq(change),
                    deps: change
                        .deps()
                        .iter()
                        .map(|dep| {
                            if let Some((slot, seq)) = self.graph.actor_seq_for_hash(dep) {
                                Dependency::Applied {
                                    actor: &self.ops.actors[slot],
                                    seq: u64::from(seq),
                                    slot,
                                }
                            } else {
                                Dependency::Candidate(topo_rank[dep])
                            }
                        })
                        .collect(),
                }
            })
            .collect();

        let mut valid_authors = HashMap::new();
        let mut broken_chain_cause = HashMap::new();
        for (&hash, resolution) in resolved_topo.iter().zip(actor_chains::propagate(&rows)) {
            match resolution {
                Resolution::Linked(source) => {
                    let author = match source {
                        AuthorSource::Initial(index) => {
                            changes_by_hash[&resolved_topo[index]].author()
                        }
                        AuthorSource::Applied(slot) => self.authors.get_author_for_actor(slot),
                    };
                    valid_authors.insert(hash, author.map(Author::into_owned));
                }
                Resolution::Broken(index) => {
                    broken_chain_cause.insert(hash, resolved_topo[index]);
                }
            }
        }
        (valid_authors, broken_chain_cause)
    }

    // --- Stage 3: Canonical fragment & attached proof verification ---

    fn verify_resolved_groups(
        &self,
        candidates: &[CandidateGroup],
        ancestry: &ResolvedAncestry<'_>,
    ) -> Result<EvaluatedGroups, AutomergeError> {
        let mut verified = Vec::new();
        let mut waiting_candidate_indices = Vec::new();
        let mut boundary_memo = BoundaryPastMemo::default();

        for (candidate_index, candidate) in candidates.iter().enumerate() {
            match self.evaluate_single_group(&candidate.group, ancestry, &mut boundary_memo) {
                Ok(GroupEvaluation::WaitingForAncestry) => {
                    waiting_candidate_indices.push(candidate_index);
                }
                Ok(GroupEvaluation::Verified { fragment, proof }) => {
                    verified.push(VerifiedGroup {
                        candidate_index,
                        fragment,
                        proof,
                    });
                }
                Err(error) => match candidate.origin {
                    Origin::IncomingBatch => return Err(error),
                    Origin::ExistingPending => {
                        tracing::debug!(
                            head = %candidate.group.head,
                            reason = %error,
                            "dropping disproved pending group"
                        );
                        continue;
                    }
                },
            }
        }

        Ok(EvaluatedGroups {
            verified,
            waiting_candidate_indices,
        })
    }

    fn evaluate_single_group(
        &self,
        group: &PendingGroup,
        ancestry: &ResolvedAncestry<'_>,
        boundary_memo: &mut BoundaryPastMemo,
    ) -> Result<GroupEvaluation, AutomergeError> {
        if let Some(&cause) = group
            .changes
            .iter()
            .find_map(|c| ancestry.broken_chain_cause.get(&c.hash()))
        {
            return Err(SigningError::from(InvalidFragment::BrokenActorChain(cause)).into());
        }

        let all_resolved = group.changes.iter().all(|change| {
            let hash = change.hash();
            self.graph.has_change(&hash) || ancestry.valid_authors.contains_key(&hash)
        });
        if !all_resolved {
            return Ok(GroupEvaluation::WaitingForAncestry);
        }

        if ancestry.has_any_equivocation {
            if let Some(conflict) =
                self.find_ancestry_equivocation(&group.head, &ancestry.changes_by_hash)
            {
                return Err(AutomergeError::duplicate_seq(conflict));
            }
        }

        let canonical =
            self.derive_canonical_fragment(group.head, &ancestry.changes_by_hash, boundary_memo);
        let group_hashes: HashSet<ChangeHash> = group.changes.iter().map(Change::hash).collect();
        if !group_membership::matches(&canonical.members, &group_hashes) {
            return Err(
                SigningError::from(InvalidFragment::NonCanonicalMembership(group.head)).into(),
            );
        }

        let requirements = FragmentRequirements::for_changes(&group.changes);
        let resolve_author = |hash: &ChangeHash| -> Option<Author<'static>> {
            if let Some((actor_idx, _)) = self.graph.actor_seq_for_hash(hash) {
                self.authors
                    .get_author_for_actor(actor_idx)
                    .map(Author::into_owned)
            } else {
                ancestry.valid_authors.get(hash).cloned().flatten()
            }
        };

        // Reuse Stage 1's preverified store whenever every frontier already
        // verified under the exact author established by causal ancestry.
        let can_reuse_preverified = author_checks::cache_matches(
            requirements.hashes().as_slice(),
            |frontier| resolve_author(&frontier),
            |frontier| {
                group
                    .preverified
                    .ed25519_entries(&frontier)
                    .next()
                    .map(|(author, _)| author)
            },
        );
        let proof = if can_reuse_preverified {
            group.preverified.clone()
        } else {
            requirements
                .verify_entries(&group.signatures, resolve_author)
                .map_err(SigningError::from)?
        };

        Ok(GroupEvaluation::Verified {
            fragment: canonical,
            proof,
        })
    }

    fn find_ancestry_equivocation<'a>(
        &self,
        head: &ChangeHash,
        changes_by_hash: &HashMap<ChangeHash, &'a Change>,
    ) -> Option<&'a Change> {
        let ancestry = walk(
            vec![*head],
            |node| changes_by_hash[&node].deps().to_vec(),
            |node| {
                if self.graph.has_change(&node) {
                    Visit::Ignore
                } else {
                    Visit::KeepAndExpand
                }
            },
        );
        actor_slots::find_conflict(&ancestry, |hash| self.actor_slot(changes_by_hash[&hash]))
            .map(|hash| changes_by_hash[&hash])
    }

    fn derive_canonical_fragment(
        &self,
        head: ChangeHash,
        changes_by_hash: &HashMap<ChangeHash, &Change>,
        memo: &mut BoundaryPastMemo,
    ) -> Fragment {
        let parents_of = |hash: &ChangeHash| -> Vec<ChangeHash> {
            if self.graph.has_change(hash) {
                self.graph.deps(hash).collect()
            } else {
                changes_by_hash[hash].deps().to_vec()
            }
        };

        let level = head.fragment_level();
        if level == 0 {
            return Fragment {
                head,
                level: 0,
                boundary: parents_of(&head),
                checkpoints: Vec::new(),
                members: vec![head],
            };
        }

        let boundary = walk(
            parents_of(&head),
            |node| parents_of(&node),
            |node| {
                if node.fragment_level() >= level {
                    Visit::Keep
                } else {
                    Visit::Expand
                }
            },
        );

        let mut boundary_past = BoundarySummary::new();
        for &b in &boundary {
            boundary_past.merge(&self.boundary_past_for(b, changes_by_hash, memo));
        }
        let seeds_vec: Vec<ChangeHash> = boundary_past.seeds.into_iter().collect();
        let graph_boundary_clock = self.graph.seq_clock_for_heads(&seeds_vec);

        let in_boundary_past = |hash: &ChangeHash| -> bool {
            if let Some((actor_idx, seq)) = self.graph.actor_seq_for_hash(hash) {
                graph_boundary_clock
                    .get_for_actor(&actor_idx)
                    .is_some_and(|covered| seq <= covered.get())
            } else {
                let change = changes_by_hash[hash];
                boundary_past
                    .maxima
                    .get(change.actor_id())
                    .is_some_and(|&max_seq| change.seq() <= max_seq)
            }
        };

        let members = walk(
            vec![head],
            |node| parents_of(&node),
            |node| {
                if node == head || !in_boundary_past(&node) {
                    Visit::KeepAndExpand
                } else {
                    Visit::Ignore
                }
            },
        );
        let checkpoints = members
            .iter()
            .copied()
            .filter(|hash| *hash != head && hash.fragment_level() > 0)
            .collect();

        Fragment {
            head,
            level,
            boundary,
            checkpoints,
            members,
        }
    }

    fn boundary_past_for(
        &self,
        boundary_node: ChangeHash,
        changes_by_hash: &HashMap<ChangeHash, &Change>,
        memo: &mut BoundaryPastMemo,
    ) -> BoundarySummary {
        use boundary_summary::Contribution;

        if let Some(cached) = memo.by_boundary.get(&boundary_node) {
            return cached.clone();
        }
        if self.graph.has_change(&boundary_node) {
            let mut result = BoundarySummary::new();
            result.seed(boundary_node);
            return result;
        }

        let visited = walk(
            vec![boundary_node],
            |node| changes_by_hash[&node].deps().to_vec(),
            |node| {
                if self.graph.has_change(&node) || memo.by_boundary.contains_key(&node) {
                    Visit::Keep
                } else {
                    Visit::KeepAndExpand
                }
            },
        );
        let result = boundary_summary::summarize(&visited, |node| {
            if self.graph.has_change(&node) {
                Contribution::Applied(node)
            } else if let Some(cached) = memo.by_boundary.get(&node) {
                Contribution::Memoized(cached)
            } else {
                let change = changes_by_hash[&node];
                Contribution::Candidate(change.actor_id().clone(), change.seq())
            }
        });
        memo.by_boundary.insert(boundary_node, result.clone());
        result
    }

    // --- Stage 4: Causal selection & admission planning ---

    fn plan_group_admission(
        &self,
        candidates: &[CandidateGroup],
        ancestry: &ResolvedAncestry<'_>,
        evaluated: &EvaluatedGroups,
    ) -> Result<AdmissionPlan, AutomergeError> {
        let signing = self.state;
        let fragments: Vec<Fragment> = evaluated
            .verified
            .iter()
            .map(|v| v.fragment.clone())
            .collect();
        let accepted_view = AcceptedGraph {
            graph: self.graph,
            signing,
        };
        let selection = select_fragments(&accepted_view, &fragments);
        let selected_verified_indices: Vec<usize> = selection.selected_indices().collect();

        let accepted_member_hashes: Vec<_> = selected_verified_indices
            .iter()
            .flat_map(|&v_idx| {
                &candidates[evaluated.verified[v_idx].candidate_index]
                    .group
                    .changes
            })
            .map(Change::hash)
            .collect();

        let mut ranks = apply_order::collect_ranks(&accepted_member_hashes, |hash| {
            (!self.graph.has_change(&hash)).then(|| ancestry.topo_rank[&hash])
        });
        if let Some(conflict) = actor_slots::find_conflict(&ranks, |rank| {
            self.actor_slot(ancestry.changes_by_hash[&ancestry.resolved_topo[rank]])
        }) {
            return Err(AutomergeError::duplicate_seq(
                ancestry.changes_by_hash[&ancestry.resolved_topo[conflict]],
            ));
        }
        apply_order::sort_ranks(&mut ranks);
        let ordered_unapplied_hashes = ranks
            .into_iter()
            .map(|rank| ancestry.resolved_topo[rank])
            .collect();

        let mut remaining_candidate_indices: HashSet<usize> = evaluated
            .waiting_candidate_indices
            .iter()
            .copied()
            .collect();
        for v_idx in selection.pending_indices() {
            remaining_candidate_indices.insert(evaluated.verified[v_idx].candidate_index);
        }

        Ok(AdmissionPlan {
            selected_verified_indices,
            ordered_unapplied_hashes,
            accepted_member_hashes,
            remaining_candidate_indices,
        })
    }

    fn pending_group_remains_viable(&self, group: &PendingGroup) -> bool {
        let signing = self.state;
        let all_accepted = group.changes.iter().all(|change| {
            self.graph
                .actor_seq_for_hash(&change.hash())
                .is_some_and(|(actor, seq)| signing.accepted.is_accepted_actor_seq(actor, seq))
        });
        if all_accepted {
            return false;
        }
        for change in &group.changes {
            let hash = change.hash();
            if !self.graph.has_change(&hash) && self.has_actor_seq(change) {
                return false;
            }
        }
        let in_group: HashMap<ChangeHash, &Change> =
            group.changes.iter().map(|c| (c.hash(), c)).collect();
        let requirements = FragmentRequirements::for_changes(&group.changes);
        for frontier in requirements.hashes() {
            let Some(actor_idx) = self.ops.lookup_actor(in_group[frontier].actor_id()) else {
                continue;
            };
            if self.graph.seq_for_actor(actor_idx) == 0 {
                continue;
            }
            let Some(author) = self.authors.get_author_for_actor(actor_idx) else {
                return false;
            };
            if verify_frontier_entry(&group.signatures, frontier, author).is_err() {
                return false;
            }
        }
        true
    }
}

/// Owned input and plan survive releasing the immutable signing view before apply.
pub(super) struct PreparedAdmission {
    candidates: Vec<CandidateGroup>,
    verified: Vec<VerifiedGroup>,
    plan: AdmissionPlan,
}

impl PreparedAdmission {
    pub(super) fn changes(&self) -> Vec<&Change> {
        let by_hash: HashMap<ChangeHash, &Change> = self
            .candidates
            .iter()
            .flat_map(|candidate| {
                candidate
                    .group
                    .changes
                    .iter()
                    .map(|change| (change.hash(), change))
            })
            .collect();
        self.plan
            .ordered_unapplied_hashes
            .iter()
            .map(|hash| by_hash[hash])
            .collect()
    }

    pub(super) fn complete(self, signing: &mut SigningDocument<'_>) {
        for index in self.plan.selected_verified_indices {
            let proof = &self.verified[index].proof;
            let newly_verified: Vec<_> = proof
                .verified_hashes()
                .filter(|hash| {
                    !has_signature(
                        signing.graph,
                        signing.authors,
                        &signing.state.signatures,
                        hash,
                    )
                })
                .copied()
                .collect();
            signing
                .state
                .signatures
                .merge_verified(proof)
                .expect("admitted proof must match historical authors");
            for hash in newly_verified {
                signing.state.fragment_readiness.signature_added(&hash);
            }
        }
        signing
            .state
            .accepted
            .include_hashes(signing.graph, self.plan.accepted_member_hashes);
        signing.state.reclaim_covered(signing.graph);
        signing.state.remove_published_drafts(signing.graph);
        let incoming = Incoming {
            graph: signing.graph,
            ops: signing.ops,
            authors: signing.authors,
            state: signing.state,
        };
        let mut pending = PendingGroups::default();
        for (index, candidate) in self.candidates.into_iter().enumerate() {
            if self.plan.remaining_candidate_indices.contains(&index)
                && incoming.pending_group_remains_viable(&candidate.group)
            {
                pending.push_dedup(candidate.group);
            }
        }
        signing.state.pending = pending;
    }
}

#[cfg(test)]
mod boundary_tests;
