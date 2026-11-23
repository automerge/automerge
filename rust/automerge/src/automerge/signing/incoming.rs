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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

use crate::admission::{select_fragments, AcceptedHistory};
use crate::change_graph::ChangeGraph;
use crate::signature::error::{InvalidFragment, ProofError, SigningError};
use crate::signature::{
    ed25519, verify_frontier_entry, FragmentRequirements, Signature, SignatureStore,
};
use crate::storage::load::{LoadedGroup, LoadedGroupBody};
use crate::storage::signatures::HashEntries;
use crate::{ActorId, Author, AutomergeError, Change, ChangeHash, Fragment};

use super::{has_signature, SigningDocument};

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
    topo_rank: HashMap<ChangeHash, usize>,
    valid_authors: HashMap<ChangeHash, Option<Author<'static>>>,
    broken_chain_cause: HashMap<ChangeHash, ChangeHash>,
    has_any_equivocation: bool,
}

/// Memoized boundary causal pasts shared across fragment checks in one batch.
#[derive(Default)]
struct BoundaryPastMemo {
    by_boundary: HashMap<ChangeHash, (HashMap<ActorId, u64>, BTreeSet<ChangeHash>)>,
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
            // Snapshots are not admissible in required mode at this boundary.
            LoadedGroupBody::Document => return Err(SigningError::SnapshotNotSupported.into()),
            LoadedGroupBody::Changes(changes) => changes,
        };
        if changes.is_empty() {
            return Err(SigningError::from(InvalidFragment::Empty).into());
        }

        let in_group = self.check_group_members(&changes, incoming_actor_seqs)?;
        let head = Self::find_single_reachable_head(&changes, &in_group)?;
        let group_seq1_authors = Self::check_intra_group_actor_chains(&changes)?;
        let Some(signatures) = group.signatures else {
            let requirements = FragmentRequirements::for_members(
                changes
                    .iter()
                    .map(|change| (change.actor_id(), change.seq(), change.hash())),
            );
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

        let mut reachable = HashSet::from([head]);
        let mut queue = VecDeque::from([head]);
        while let Some(curr) = queue.pop_front() {
            for dep in in_group[&curr].deps() {
                if in_group.contains_key(dep) && reachable.insert(*dep) {
                    queue.push_back(*dep);
                }
            }
        }
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
        let requirements = FragmentRequirements::for_members(
            changes
                .iter()
                .map(|change| (change.actor_id(), change.seq(), change.hash())),
        );
        let mut preverified = SignatureStore::default();
        for frontier in requirements.hashes() {
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
                        let prepared =
                            ed25519::VerifyingAuthor::new(author.clone()).map_err(|source| {
                                SigningError::from(ProofError::InvalidAuthorKey {
                                    hash: *frontier,
                                    source,
                                })
                            })?;
                        prepared_keys.entry(author).or_insert(prepared)
                    }
                };
                let witness = signatures
                    .for_hash(frontier)
                    .filter_map(Signature::as_ed25519)
                    .find_map(|sig| verifier.verify_hash(frontier, sig).ok())
                    .ok_or_else(|| SigningError::from(ProofError::MissingSignature(*frontier)))?;
                preverified
                    .insert_ed25519(witness)
                    .expect("each frontier has one fixed author");
            }
        }
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
        let mut seen_actor_seqs: HashMap<(&'a ActorId, u64), ChangeHash> = HashMap::new();
        let mut has_any_equivocation = false;

        for candidate in candidates {
            for change in &candidate.group.changes {
                let hash = change.hash();
                if self.graph.has_change(&hash) {
                    continue;
                }
                if self.has_actor_seq(change) {
                    has_any_equivocation = true;
                }
                let key = (change.actor_id(), change.seq());
                if let Some(&prev) = seen_actor_seqs.get(&key) {
                    if prev != hash {
                        has_any_equivocation = true;
                    }
                } else {
                    seen_actor_seqs.insert(key, hash);
                }
                changes_by_hash.entry(hash).or_insert(change);
            }
        }

        let (resolved_topo, topo_rank) = self.topological_sort_unapplied(&changes_by_hash);
        let (valid_authors, broken_chain_cause) =
            self.propagate_actor_authors(&resolved_topo, &changes_by_hash);

        ResolvedAncestry {
            changes_by_hash,
            topo_rank,
            valid_authors,
            broken_chain_cause,
            has_any_equivocation,
        }
    }

    fn topological_sort_unapplied(
        &self,
        changes_by_hash: &HashMap<ChangeHash, &Change>,
    ) -> (Vec<ChangeHash>, HashMap<ChangeHash, usize>) {
        let mut waiting: HashMap<ChangeHash, usize> = HashMap::with_capacity(changes_by_hash.len());
        let mut dependents: HashMap<ChangeHash, Vec<ChangeHash>> = HashMap::new();
        let mut ready = VecDeque::new();
        for (&hash, &change) in changes_by_hash {
            let mut missing_deps = 0usize;
            for dep in change.deps() {
                if !self.graph.has_change(dep) {
                    missing_deps += 1;
                    if changes_by_hash.contains_key(dep) {
                        dependents.entry(*dep).or_default().push(hash);
                    }
                }
            }
            waiting.insert(hash, missing_deps);
            if missing_deps == 0 {
                ready.push_back(hash);
            }
        }

        let mut resolved_topo = Vec::with_capacity(changes_by_hash.len());
        let mut topo_rank = HashMap::new();
        while let Some(hash) = ready.pop_front() {
            topo_rank.insert(hash, resolved_topo.len());
            resolved_topo.push(hash);
            if let Some(children) = dependents.remove(&hash) {
                for child in children {
                    let count = waiting.get_mut(&child).unwrap();
                    *count -= 1;
                    if *count == 0 {
                        ready.push_back(child);
                    }
                }
            }
        }
        (resolved_topo, topo_rank)
    }

    fn propagate_actor_authors(
        &self,
        resolved_topo: &[ChangeHash],
        changes_by_hash: &HashMap<ChangeHash, &Change>,
    ) -> (
        HashMap<ChangeHash, Option<Author<'static>>>,
        HashMap<ChangeHash, ChangeHash>,
    ) {
        let mut valid_authors: HashMap<ChangeHash, Option<Author<'static>>> = HashMap::new();
        let mut broken_chain_cause: HashMap<ChangeHash, ChangeHash> = HashMap::new();

        for &hash in resolved_topo {
            let change = changes_by_hash[&hash];
            if let Some(&cause) = change
                .deps()
                .iter()
                .find_map(|dep| broken_chain_cause.get(dep))
            {
                broken_chain_cause.insert(hash, cause);
                continue;
            }
            if change.seq() == 1 {
                let already_applied = self
                    .ops
                    .lookup_actor(change.actor_id())
                    .is_some_and(|idx| self.graph.seq_for_actor(idx) > 0);
                if already_applied {
                    broken_chain_cause.insert(hash, hash);
                } else {
                    valid_authors.insert(hash, change.author().map(Author::into_owned));
                }
            } else {
                match self.find_predecessor_author(change, changes_by_hash, &valid_authors) {
                    Some(author) => {
                        valid_authors.insert(hash, author);
                    }
                    None => {
                        broken_chain_cause.insert(hash, hash);
                    }
                }
            }
        }
        (valid_authors, broken_chain_cause)
    }

    fn find_predecessor_author(
        &self,
        change: &Change,
        changes_by_hash: &HashMap<ChangeHash, &Change>,
        valid_authors: &HashMap<ChangeHash, Option<Author<'static>>>,
    ) -> Option<Option<Author<'static>>> {
        let target_seq = change.seq() - 1;
        for dep in change.deps() {
            if let Some((actor_idx, seq)) = self.graph.actor_seq_for_hash(dep) {
                if u64::from(seq) == target_seq && &self.ops.actors[actor_idx] == change.actor_id()
                {
                    return Some(
                        self.authors
                            .get_author_for_actor(actor_idx)
                            .map(Author::into_owned),
                    );
                }
            } else if let Some(author) = valid_authors.get(dep) {
                let pred = changes_by_hash[dep];
                if pred.actor_id() == change.actor_id() && pred.seq() == target_seq {
                    return Some(author.clone());
                }
            }
        }
        None
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
        let matches_canonical = canonical.members.len() == group_hashes.len()
            && canonical
                .members
                .iter()
                .all(|member| group_hashes.contains(member));
        if !matches_canonical {
            return Err(
                SigningError::from(InvalidFragment::NonCanonicalMembership(group.head)).into(),
            );
        }

        let requirements = FragmentRequirements::for_members(
            group
                .changes
                .iter()
                .map(|c| (c.actor_id(), c.seq(), c.hash())),
        );
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
        let can_reuse_preverified = requirements.hashes().all(|frontier| {
            let expected = resolve_author(frontier);
            expected.is_some()
                && group
                    .preverified
                    .ed25519_entries(frontier)
                    .next()
                    .is_some_and(|(verified_author, _)| Some(verified_author) == expected.as_ref())
        });
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
        let mut seen = HashSet::new();
        let mut actor_seqs: HashMap<(&ActorId, u64), ChangeHash> = HashMap::new();
        let mut stack = vec![*head];
        while let Some(hash) = stack.pop() {
            if self.graph.has_change(&hash) || !seen.insert(hash) {
                continue;
            }
            let change = changes_by_hash[&hash];
            if self.has_actor_seq(change) {
                return Some(change);
            }
            let key = (change.actor_id(), change.seq());
            if let Some(&prev) = actor_seqs.get(&key) {
                if prev != hash {
                    return Some(change);
                }
            } else {
                actor_seqs.insert(key, hash);
            }
            stack.extend(change.deps().iter().copied());
        }
        None
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

        let mut boundary = Vec::new();
        let mut seen_boundary = HashSet::new();
        let mut stack = parents_of(&head);
        while let Some(node) = stack.pop() {
            if !seen_boundary.insert(node) {
                continue;
            }
            if node.fragment_level() >= level {
                boundary.push(node);
            } else {
                stack.extend(parents_of(&node));
            }
        }

        let mut candidate_clock: HashMap<ActorId, u64> = HashMap::new();
        let mut graph_boundary_seeds: BTreeSet<ChangeHash> = BTreeSet::new();
        for &b in &boundary {
            let (b_clock, b_seeds) = self.boundary_past_for(b, changes_by_hash, memo);
            for (actor, seq) in b_clock {
                candidate_clock
                    .entry(actor)
                    .and_modify(|curr| *curr = (*curr).max(seq))
                    .or_insert(seq);
            }
            graph_boundary_seeds.extend(b_seeds);
        }
        let seeds_vec: Vec<ChangeHash> = graph_boundary_seeds.into_iter().collect();
        let graph_boundary_clock = self.graph.seq_clock_for_heads(&seeds_vec);

        let in_boundary_past = |hash: &ChangeHash| -> bool {
            if let Some((actor_idx, seq)) = self.graph.actor_seq_for_hash(hash) {
                graph_boundary_clock
                    .get_for_actor(&actor_idx)
                    .is_some_and(|covered| seq <= covered.get())
            } else {
                let change = changes_by_hash[hash];
                candidate_clock
                    .get(change.actor_id())
                    .is_some_and(|&max_seq| change.seq() <= max_seq)
            }
        };

        let mut members = Vec::new();
        let mut visited = HashSet::from([head]);
        let mut queue = VecDeque::from([head]);
        while let Some(curr) = queue.pop_front() {
            members.push(curr);
            for parent in parents_of(&curr) {
                if !visited.contains(&parent) && !in_boundary_past(&parent) {
                    visited.insert(parent);
                    queue.push_back(parent);
                }
            }
        }
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
    ) -> (HashMap<ActorId, u64>, BTreeSet<ChangeHash>) {
        if let Some(cached) = memo.by_boundary.get(&boundary_node) {
            return cached.clone();
        }
        if self.graph.has_change(&boundary_node) {
            return (HashMap::new(), BTreeSet::from([boundary_node]));
        }

        let mut clock: HashMap<ActorId, u64> = HashMap::new();
        let mut graph_seeds: BTreeSet<ChangeHash> = BTreeSet::new();
        let mut visited = HashSet::new();
        let mut stack = vec![boundary_node];
        while let Some(node) = stack.pop() {
            if self.graph.has_change(&node) {
                graph_seeds.insert(node);
                continue;
            }
            if let Some((cached_clock, cached_seeds)) = memo.by_boundary.get(&node) {
                for (actor, &seq) in cached_clock {
                    clock
                        .entry(actor.clone())
                        .and_modify(|curr| *curr = (*curr).max(seq))
                        .or_insert(seq);
                }
                graph_seeds.extend(cached_seeds.iter().copied());
                continue;
            }
            if !visited.insert(node) {
                continue;
            }
            let change = changes_by_hash[&node];
            clock
                .entry(change.actor_id().clone())
                .and_modify(|curr| *curr = (*curr).max(change.seq()))
                .or_insert(change.seq());
            stack.extend(change.deps().iter().copied());
        }
        let result = (clock, graph_seeds);
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

        let mut selected_unapplied: HashSet<ChangeHash> = HashSet::new();
        let mut selected_actor_seqs: HashMap<(ActorId, u64), ChangeHash> = HashMap::new();
        let mut accepted_member_hashes: Vec<ChangeHash> = Vec::new();
        for &v_idx in &selected_verified_indices {
            let group = &candidates[evaluated.verified[v_idx].candidate_index].group;
            for change in &group.changes {
                let hash = change.hash();
                accepted_member_hashes.push(hash);
                if self.graph.has_change(&hash) {
                    continue;
                }
                if self.has_actor_seq(change) {
                    return Err(AutomergeError::duplicate_seq(change));
                }
                let key = (change.actor_id().clone(), change.seq());
                if let Some(&prev) = selected_actor_seqs.get(&key) {
                    if prev != hash {
                        return Err(AutomergeError::duplicate_seq(change));
                    }
                } else {
                    selected_actor_seqs.insert(key, hash);
                }
                selected_unapplied.insert(hash);
            }
        }

        let mut ordered_unapplied_hashes: Vec<ChangeHash> =
            selected_unapplied.into_iter().collect();
        ordered_unapplied_hashes.sort_unstable_by_key(|hash| ancestry.topo_rank[hash]);

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
        let requirements = FragmentRequirements::for_members(
            group
                .changes
                .iter()
                .map(|c| (c.actor_id(), c.seq(), c.hash())),
        );
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
