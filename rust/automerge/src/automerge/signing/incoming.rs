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

use std::collections::{HashMap, HashSet};

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
mod author_projection;
mod boundary_cutoff;
mod boundary_memo;
mod boundary_summary;
mod candidate_origin;
mod canonical_shape;
mod delivery;
mod evaluation;
mod evaluation_batch;
mod group_membership;
mod intrinsic_chain;
mod intrinsic_index;
mod intrinsic_shape;
mod pending;
mod plan_contents;
mod publication;
mod snapshot_index;
mod topology;
mod topology_input;
mod walk;

use pending::Route;
pub(super) use publication::run as run_admission;
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
        delivery::same_members(&self.changes, &other.changes, Change::hash)
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
}

type CandidateGroup = candidate_origin::Candidate<PendingGroup>;

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

/// One memo belongs to one immutable applied graph and candidate view. In
/// particular, it does not survive document apply or follow actor-table shifts.
struct BoundaryPastMemo<'view, 'change> {
    graph: &'view ChangeGraph,
    changes_by_hash: &'view HashMap<ChangeHash, &'change Change>,
    cache: boundary_memo::Cache<ActorId, ChangeHash>,
}

impl<'view, 'change> BoundaryPastMemo<'view, 'change> {
    fn new(
        graph: &'view ChangeGraph,
        changes_by_hash: &'view HashMap<ChangeHash, &'change Change>,
    ) -> Self {
        Self {
            graph,
            changes_by_hash,
            cache: boundary_memo::Cache::new(),
        }
    }

    fn summary(&mut self, roots: &[ChangeHash]) -> BoundarySummary {
        let graph = self.graph;
        let changes_by_hash = self.changes_by_hash;
        self.cache.union(roots, &|hash| {
            if graph.has_change(&hash) {
                boundary_memo::Node::Applied
            } else {
                let change = changes_by_hash[&hash];
                boundary_memo::Node::Candidate {
                    actor: change.actor_id().clone(),
                    seq: change.seq(),
                    parents: change.deps(),
                }
            }
        })
    }
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
        let candidates = candidate_origin::candidates(self.state.pending.groups.clone(), incoming);
        let ancestry = self.resolve_unapplied_ancestry(&candidates);
        let evaluated = self.verify_resolved_groups(&candidates, &ancestry)?;
        let AdmissionPlan {
            selected_verified_indices,
            ordered_unapplied_hashes,
            accepted_member_hashes,
            remaining_candidate_indices,
        } = self.plan_group_admission(&candidates, &ancestry, &evaluated)?;
        let selected_proofs =
            plan_contents::take_selected(evaluated.verified, &selected_verified_indices, |group| {
                group.proof
            });
        Ok(PreparedAdmission {
            candidates,
            selected_proofs,
            ordered_unapplied_hashes,
            accepted_member_hashes,
            remaining_candidate_indices,
        })
    }

    // --- Stage 1: Intrinsic per-group validation ---

    fn validate_incoming_batch(
        &self,
        groups: Vec<LoadedGroup>,
    ) -> Result<Vec<PendingGroup>, AutomergeError> {
        let incoming_actor_authors = snapshot_index::build(
            &groups,
            LoadedGroup::changes,
            |change| {
                (change.seq() == 1 && !self.graph.has_change(&change.hash())).then(|| {
                    (
                        change.actor_id().clone(),
                        change.author().map(Author::into_owned),
                    )
                })
            },
            snapshot_index::Keep::First,
        );

        evaluation_batch::validate_batch(
            groups,
            (HashMap::new(), HashMap::new()),
            |(mut incoming_actor_seqs, mut prepared_keys), group| {
                let validated = self.validate_incoming_group(
                    group,
                    &mut incoming_actor_seqs,
                    &incoming_actor_authors,
                    &mut prepared_keys,
                );
                ((incoming_actor_seqs, prepared_keys), validated)
            },
        )
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
        use intrinsic_index::{Fault, Member};

        intrinsic_index::index(
            changes,
            |change| Member {
                actor: change.actor_id().clone(),
                seq: change.seq(),
                hash: change.hash(),
                has_author: change.author().is_some(),
                conflicts_with_applied: !self.graph.has_change(&change.hash())
                    && self.has_actor_seq(change),
            },
            incoming_actor_seqs,
        )
        .map_err(|(index, fault)| {
            let change = &changes[index];
            match fault {
                Fault::Sequence => AutomergeError::InvalidSeq(change.seq()),
                Fault::MisplacedAuthor => AutomergeError::author_on_non_initial_seq(change),
                Fault::DuplicateHash => {
                    SigningError::from(InvalidFragment::DuplicateMember(change.hash())).into()
                }
                Fault::ConflictingSlot => AutomergeError::duplicate_seq(change),
            }
        })
    }

    fn find_single_reachable_head(
        changes: &[Change],
        in_group: &HashMap<ChangeHash, &Change>,
    ) -> Result<ChangeHash, AutomergeError> {
        use intrinsic_shape::LevelFault;

        let members: Vec<_> = changes.iter().map(Change::hash).collect();
        let head = intrinsic_shape::unique_head(
            &members,
            |node| in_group[&node].deps(),
            |dep| in_group.contains_key(&dep),
        )
        .ok_or_else(|| SigningError::from(InvalidFragment::NotSingleHeaded))?;

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
        if !intrinsic_shape::spans(&members, &reachable) {
            return Err(SigningError::from(InvalidFragment::NotSingleHeaded).into());
        }

        intrinsic_shape::levels(&members, head, |hash| hash.fragment_level()).map_err(|fault| {
            SigningError::from(match fault {
                LevelFault::NonSingletonLoose => InvalidFragment::NonSingletonLoose(head),
                LevelFault::Member(member) => InvalidFragment::InvalidMemberLevel { head, member },
            })
        })?;
        Ok(head)
    }

    fn check_intra_group_actor_chains(
        changes: &[Change],
    ) -> Result<HashMap<&ActorId, Option<Author<'_>>>, AutomergeError> {
        let mut rows: Vec<_> = changes
            .iter()
            .enumerate()
            .map(|(index, change)| (change.actor_id(), change.seq(), index))
            .collect();
        intrinsic_chain::check(&mut rows, |predecessor, child| {
            changes[child].deps().contains(&changes[predecessor].hash())
        })
        .map_err(|child| {
            SigningError::from(InvalidFragment::BrokenActorChain(changes[child].hash()))
        })?;
        Ok(changes
            .iter()
            .filter(|change| change.seq() == 1)
            .map(|change| (change.actor_id(), change.author()))
            .collect())
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

    fn resolve_unapplied_ancestry<'a>(
        &self,
        candidates: &'a [CandidateGroup],
    ) -> ResolvedAncestry<'a> {
        let changes_by_hash = snapshot_index::build(
            candidates,
            |candidate| candidate.group.changes.as_slice(),
            |change| (!self.graph.has_change(&change.hash())).then_some((change.hash(), change)),
            snapshot_index::Keep::First,
        );

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
        let index_by_hash = plan_contents::positions(hashes);
        let (waiting, dependents) = topology_input::build(
            hashes,
            |hash| changes_by_hash[&hash].deps(),
            |dep| {
                if self.graph.has_change(&dep) {
                    topology_input::Parent::Applied
                } else if let Some(&parent) = index_by_hash.get(&dep) {
                    topology_input::Parent::Candidate(parent)
                } else {
                    topology_input::Parent::Missing
                }
            },
        );
        drop(index_by_hash);

        let resolved_topo: Vec<_> = topology::resolve(&dependents, waiting)
            .into_iter()
            .map(|index| hashes[index])
            .collect();
        let topo_rank = plan_contents::positions(&resolved_topo);
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
        use actor_chains::{Candidate, Dependency};

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

        let outcomes = actor_chains::propagate(&rows);
        author_projection::materialize(
            resolved_topo,
            &outcomes,
            |index| {
                changes_by_hash[&resolved_topo[index]]
                    .author()
                    .map(Author::into_owned)
            },
            |slot| {
                self.authors
                    .get_author_for_actor(slot)
                    .map(Author::into_owned)
            },
        )
    }

    // --- Stage 3: Canonical fragment & attached proof verification ---

    fn verify_resolved_groups(
        &self,
        candidates: &[CandidateGroup],
        ancestry: &ResolvedAncestry<'_>,
    ) -> Result<EvaluatedGroups, AutomergeError> {
        use evaluation_batch::Outcome;

        let (waiting_candidate_indices, verified) = evaluation_batch::assess(
            candidates,
            BoundaryPastMemo::new(self.graph, &ancestry.changes_by_hash),
            |mut memo, candidate| {
                let evaluation = self.evaluate_single_group(&candidate.group, ancestry, &mut memo);
                let outcome =
                    pending::route(candidate.origin, evaluation).map(|route| match route {
                        Route::Evaluated(GroupEvaluation::WaitingForAncestry) => Outcome::Waiting,
                        Route::Evaluated(GroupEvaluation::Verified { fragment, proof }) => {
                            Outcome::Verified((fragment, proof))
                        }
                        Route::Disproved(error) => {
                            tracing::debug!(
                                head = %candidate.group.head,
                                reason = %error,
                                "dropping disproved pending group"
                            );
                            Outcome::Dropped
                        }
                    });
                (memo, outcome)
            },
        )?;
        Ok(EvaluatedGroups {
            verified: verified
                .into_iter()
                .map(|(candidate_index, (fragment, proof))| VerifiedGroup {
                    candidate_index,
                    fragment,
                    proof,
                })
                .collect(),
            waiting_candidate_indices,
        })
    }

    fn evaluate_single_group(
        &self,
        group: &PendingGroup,
        ancestry: &ResolvedAncestry<'_>,
        boundary_memo: &mut BoundaryPastMemo<'_, '_>,
    ) -> Result<GroupEvaluation, AutomergeError> {
        let status = evaluation::ancestry(&group.changes, |change| {
            let hash = change.hash();
            if let Some(&cause) = ancestry.broken_chain_cause.get(&hash) {
                Err(SigningError::from(InvalidFragment::BrokenActorChain(cause)).into())
            } else {
                Ok(self.graph.has_change(&hash) || ancestry.valid_authors.contains_key(&hash))
            }
        });
        let (_, result) = evaluation::decide(
            status,
            boundary_memo,
            || {
                ancestry
                    .has_any_equivocation
                    .then(|| {
                        self.find_ancestry_equivocation(&group.head, &ancestry.changes_by_hash)
                            .map(AutomergeError::duplicate_seq)
                    })
                    .flatten()
            },
            |memo| {
                let canonical = Self::derive_canonical_fragment(group.head, memo);
                let group_hashes = group.changes.iter().map(Change::hash).collect();
                let result = if group_membership::matches(&canonical.members, &group_hashes) {
                    Ok(canonical)
                } else {
                    Err(
                        SigningError::from(InvalidFragment::NonCanonicalMembership(group.head))
                            .into(),
                    )
                };
                (memo, result)
            },
            || self.verify_group_evidence(group, ancestry),
        );
        result.map(|resolution| match resolution {
            Some((fragment, proof)) => GroupEvaluation::Verified { fragment, proof },
            None => GroupEvaluation::WaitingForAncestry,
        })
    }

    fn verify_group_evidence(
        &self,
        group: &PendingGroup,
        ancestry: &ResolvedAncestry<'_>,
    ) -> Result<SignatureStore, AutomergeError> {
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
        if can_reuse_preverified {
            Ok(group.preverified.clone())
        } else {
            requirements
                .verify_entries(&group.signatures, resolve_author)
                .map_err(|error| SigningError::from(error).into())
        }
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
        head: ChangeHash,
        memo: &mut BoundaryPastMemo<'_, '_>,
    ) -> Fragment {
        let graph = memo.graph;
        let changes_by_hash = memo.changes_by_hash;
        let parents_of = |hash: &ChangeHash| -> Vec<ChangeHash> {
            if graph.has_change(hash) {
                graph.deps(hash).collect()
            } else {
                changes_by_hash[hash].deps().to_vec()
            }
        };

        let shape = canonical_shape::assemble(
            head,
            |hash| hash.fragment_level(),
            |hash| parents_of(&hash),
            |roots, level| {
                walk(
                    roots,
                    |node| parents_of(&node),
                    |node| {
                        if node.fragment_level() >= level {
                            Visit::Keep
                        } else {
                            Visit::Expand
                        }
                    },
                )
            },
            |boundary| {
                let boundary_past = memo.summary(boundary);
                let seeds_vec: Vec<_> = boundary_past.seeds.iter().copied().collect();
                let graph_boundary_clock = graph.seq_clock_for_heads(&seeds_vec);
                let in_boundary_past = |hash| {
                    boundary_cutoff::covers(
                        &boundary_past,
                        hash,
                        |hash| graph.actor_seq_for_hash(&hash),
                        |actor| {
                            graph_boundary_clock
                                .get_for_actor(&actor)
                                .map(|seq| seq.get())
                        },
                        |hash| {
                            let change = changes_by_hash[&hash];
                            (change.actor_id(), change.seq())
                        },
                    )
                };

                walk(
                    vec![head],
                    |node| parents_of(&node),
                    |node| {
                        if node == head || !in_boundary_past(node) {
                            Visit::KeepAndExpand
                        } else {
                            Visit::Ignore
                        }
                    },
                )
            },
        );
        Fragment {
            head: shape.head,
            level: shape.level,
            boundary: shape.boundary,
            members: shape.members,
            checkpoints: shape.checkpoints,
        }
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

        let accepted_member_hashes = plan_contents::selected_members(
            &selected_verified_indices,
            |verified_index| {
                candidates[evaluated.verified[verified_index].candidate_index]
                    .group
                    .changes
                    .as_slice()
            },
            Change::hash,
        );

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

        let excluded: Vec<_> = selection.pending_indices().collect();
        let remaining_candidate_indices =
            plan_contents::blocked(&evaluated.waiting_candidate_indices, &excluded, |index| {
                evaluated.verified[index].candidate_index
            });

        Ok(AdmissionPlan {
            selected_verified_indices,
            ordered_unapplied_hashes,
            accepted_member_hashes,
            remaining_candidate_indices,
        })
    }

    fn pending_group_remains_viable(&self, group: &PendingGroup) -> bool {
        pending::recheck(
            &group.changes,
            |change| {
                self.graph
                    .actor_seq_for_hash(&change.hash())
                    .is_some_and(|(actor, seq)| {
                        self.state.accepted.is_accepted_actor_seq(actor, seq)
                    })
            },
            |change| self.graph.has_change(&change.hash()) || !self.has_actor_seq(change),
            || self.pending_frontiers_viable(group),
        )
    }

    fn pending_frontiers_viable(&self, group: &PendingGroup) -> bool {
        let in_group: HashMap<ChangeHash, &Change> =
            group.changes.iter().map(|c| (c.hash(), c)).collect();
        let requirements = FragmentRequirements::for_changes(&group.changes);
        pending::all(
            requirements.hashes().as_slice(),
            &|frontier: &ChangeHash| {
                let Some(actor_idx) = self.ops.lookup_actor(in_group[frontier].actor_id()) else {
                    return true;
                };
                if self.graph.seq_for_actor(actor_idx) == 0 {
                    return true;
                }
                self.authors
                    .get_author_for_actor(actor_idx)
                    .is_some_and(|author| {
                        verify_frontier_entry(&group.signatures, frontier, author).is_ok()
                    })
            },
        )
    }
}

/// Owned input and plan survive releasing the immutable signing view before apply.
pub(super) struct PreparedAdmission {
    candidates: Vec<CandidateGroup>,
    selected_proofs: Vec<SignatureStore>,
    ordered_unapplied_hashes: Vec<ChangeHash>,
    accepted_member_hashes: Vec<ChangeHash>,
    remaining_candidate_indices: HashSet<usize>,
}

impl PreparedAdmission {
    pub(super) fn changes(&self) -> Vec<&Change> {
        let by_hash = snapshot_index::build(
            &self.candidates,
            |candidate| candidate.group.changes.as_slice(),
            |change| Some((change.hash(), change)),
            snapshot_index::Keep::Last,
        );
        plan_contents::ordered_refs(&self.ordered_unapplied_hashes, |hash| by_hash[&hash])
    }

    pub(super) fn complete(self, signing: &mut SigningDocument<'_>) {
        use publication::Publish;

        let signing = publication::publish(signing, &self.selected_proofs, |signing, step| {
            match step {
                Publish::MergeProof(proof) => {
                    Self::merge_selected_proof(signing, proof);
                }
                Publish::IncludeMembers => signing
                    .state
                    .accepted
                    .include_prepared(signing.graph, &self.accepted_member_hashes),
                Publish::Reclaim => signing.state.reclaim_covered(signing.graph),
                Publish::RemoveDrafts => signing.state.remove_published_drafts(signing.graph),
            }
            signing
        });
        let incoming = Incoming {
            graph: signing.graph,
            ops: signing.ops,
            authors: signing.authors,
            state: signing.state,
        };
        let retained = pending::rebuild(
            self.candidates,
            |index| self.remaining_candidate_indices.contains(&index),
            |candidate| incoming.pending_group_remains_viable(&candidate.group),
            |left, right| left.group.same_delivery(&right.group),
        );
        signing.state.pending = PendingGroups {
            groups: retained
                .into_iter()
                .map(|candidate| candidate.group)
                .collect(),
        };
    }

    fn merge_selected_proof(signing: &mut SigningDocument<'_>, proof: &SignatureStore) {
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
}

#[cfg(test)]
mod boundary_tests;
#[cfg(test)]
mod policy_tests;
