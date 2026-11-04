//! Select whole history candidates using accepted history and signature evidence.
//!
//! Candidates are assumed canonical, structurally valid, and mutually compatible
//! with each other and accepted history. Actor–author bindings must already be
//! established and agree with the supplied [`SignatureStore`]. Selection does
//! not validate fragments or repeat cryptographic verification.
//!
//! Signatures are associated with change hashes, not with delivery sources.
//! They can justify admission only through signed changes and supporting
//! ancestry available in accepted history or the selected candidates.
//!
//! [`select`] leaves all inputs unchanged and returns a selection, not completed
//! admission. Applying changes atomically and retaining evidence are separate work.

use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;

use crate::signature::SignatureStore;
use crate::{ActorId, Change, ChangeHash, Fragment};

/// History considered as a whole for admission.
///
/// A novel loose change requires evidence on its own hash. A canonical
/// fragment can use same-actor–author descendant evidence across its members
/// and other available history, even when the fragment has only one member.
#[derive(Debug)]
pub(crate) enum CandidateHistory<'a> {
    Loose(&'a Change),
    CanonicalFragment {
        /// The caller supplies a canonical descriptor, not a sender assertion.
        fragment: &'a Fragment,
        /// Borrowed changes matching the descriptor's complete membership.
        ///
        /// Members need not be contiguous, and overlapping candidates can share
        /// the same change. This vector owns references, not change contents.
        members: Vec<&'a Change>,
    },
}

impl<'a> CandidateHistory<'a> {
    /// Iterate borrowed members in their supplied order without consuming the candidate.
    pub(crate) fn changes(&self) -> impl ExactSizeIterator<Item = &'a Change> + '_ {
        let members = match self {
            CandidateHistory::Loose(change) => std::slice::from_ref(change),
            CandidateHistory::CanonicalFragment { members, .. } => members.as_slice(),
        };
        members.iter().copied()
    }
}

/// Query membership in fixed, dependency-closed accepted history.
///
/// Dependency-closed means that whenever a change is accepted, every change it
/// depends on is also accepted. This applies recursively to all ancestors,
/// including dependencies from other actors.
///
/// For example, if `c` depends on `b` and `b` depends on `a`, reporting `c` as
/// accepted requires both `b` and `a` to be accepted too. Accepting `a` alone
/// does not require accepting its descendants `b` or `c`.
///
/// This guarantee lets selection stop walking dependencies at an accepted change:
/// everything behind it is already accepted and needs no fresh attestation.
/// Implementations must uphold this guarantee, not merely report whether a change
/// is applied or has a signature. Answers must remain unchanged throughout a
/// selection calculation.
pub(crate) trait AcceptedHistory {
    /// Whether this exact change hash belongs to accepted history.
    fn contains(&self, hash: &ChangeHash) -> bool;
}

/// The result of deciding which candidates can be admitted together.
///
/// Every input candidate appears exactly once in either [`Selection::selected`]
/// or [`Selection::pending`]. Repeated input entries are counted separately.
/// The order of each list is unspecified.
///
/// Apply the selected candidates together, retaining their supporting evidence.
/// Pending candidates await additional signatures or supporting history before
/// selection is retried.
#[derive(Debug)]
#[must_use]
pub(crate) struct Selection<'candidate, 'change> {
    selected: Vec<&'candidate CandidateHistory<'change>>,
    pending: Vec<&'candidate CandidateHistory<'change>>,
}

impl<'candidate, 'change> Selection<'candidate, 'change> {
    /// Candidates that can be admitted together, not necessarily individually.
    pub(crate) fn selected(&self) -> &[&'candidate CandidateHistory<'change>] {
        &self.selected
    }

    /// Candidates not selected, without rejecting or evicting them.
    pub(crate) fn pending(&self) -> &[&'candidate CandidateHistory<'change>] {
        &self.pending
    }
}

/// Select the admissible subset without mutating any input.
///
/// Canonical membership, matching member changes, valid actor chains, and
/// compatibility are caller-established preconditions. Missing causal
/// dependencies and insufficient attestation are selection concerns, not
/// construction errors. Evidence is read from `signatures` by change hash,
/// regardless of which delivery supplied it.
///
/// The result describes admissibility against these inputs. It must not be
/// treated as authorization to apply against a different history or evidence
/// state. Atomic application and evidence retention are separate work.
///
/// Starts with all candidates and excludes those with unmet requirements.
/// A work list propagates causal exclusions when a change loses its last
/// surviving candidate source. Accepted changes remain available throughout.
/// Internal dependencies need no notifications because a surviving candidate
/// supplies its own members.
///
/// After draining causal exclusions, the next pass rebuilds available history
/// and attestation. Losing signed history can invalidate attestation even when
/// no causal dependency disappears. Ancestry visits are shared per actor within
/// each pass, never across passes. The calculation stops when no candidates
/// are excluded and returns the greatest admissible subset under the input contract.
pub(crate) fn select<'candidate, 'change>(
    accepted: &impl AcceptedHistory,
    candidates: &'candidate [CandidateHistory<'change>],
    signatures: &SignatureStore,
) -> Selection<'candidate, 'change> {
    Selector::new(accepted, candidates).run(accepted, signatures)
}

/// State for one admission-selection calculation.
///
/// Construct with [`Selector::new`] and consume with [`Selector::run`]. Candidate
/// exclusions persist between rounds, while available history and attestation
/// are rebuilt in a [`Round`].
///
/// All candidate indices refer to positions in the original input slice,
/// [`Selector::candidates`]. Repeated candidates remain separate input occurrences.
struct Selector<'candidate, 'change> {
    /// Original input, retained so candidate indices stay stable.
    candidates: &'candidate [CandidateHistory<'change>],

    /// Number of not-yet-excluded candidate occurrences supplying each hash.
    ///
    /// Zero-count entries remain present. A zero count makes a change
    /// unavailable only if it is not already accepted.
    candidate_counts: HashMap<ChangeHash, usize>,
    /// Maps a dependency hash to the indices of candidates that require it.
    ///
    /// Records only dependencies outside each candidate's own membership and
    /// outside accepted history. If such a change becomes unavailable, the
    /// listed candidates must be excluded.
    ///
    /// Each candidate index appears at most once per dependency hash.
    dependents: HashMap<ChangeHash, Vec<usize>>,

    /// Whether each candidate's exclusion has already been processed.
    excluded: Vec<bool>,
    /// Work stack of candidate indices awaiting exclusion.
    ///
    /// Several missing dependencies may queue the same index. The
    /// [`Selector::excluded`] flags prevent processing an occurrence twice.
    to_exclude: Vec<usize>,
    /// Candidates still being considered, paired with their original input indices.
    ///
    /// Each index occurs once. Filtering preserves input order, although result
    /// ordering is not part of the selection contract.
    remaining: Vec<(usize, &'candidate CandidateHistory<'change>)>,
    /// Candidates whose exclusions have been processed.
    pending: Vec<&'candidate CandidateHistory<'change>>,
}

impl<'candidate, 'change> Selector<'candidate, 'change> {
    /// Prepare selection bookkeeping with every candidate initially included.
    ///
    /// Count how many candidates contain each change, and record which candidates
    /// need each dependency.
    ///
    /// A dependency need not be recorded when the candidate contains it itself,
    /// or when it is already accepted. In either case, excluding another candidate
    /// cannot make that dependency unavailable.
    ///
    /// The caller establishes the input preconditions described by [`select`].
    /// Construction does not validate fragments or establish admission status.
    fn new(
        accepted: &impl AcceptedHistory,
        candidates: &'candidate [CandidateHistory<'change>],
    ) -> Self {
        // Count input occurrences, including duplicate candidates. Valid canonical
        // membership has no duplicate hashes within one candidate.
        let candidate_counts: HashMap<ChangeHash, usize> = {
            let mut counts = HashMap::new();
            for candidate in candidates {
                for change in candidate.changes() {
                    *counts.entry(change.hash()).or_default() += 1;
                }
            }
            counts
        };
        // Stable original indices of candidates requiring external, non-accepted
        // dependencies. Deduplicate notifications within each candidate.
        let dependents: HashMap<ChangeHash, Vec<usize>> = {
            let mut dependents = HashMap::new();

            for (index, candidate) in candidates.iter().enumerate() {
                let member_hashes: HashSet<ChangeHash> =
                    candidate.changes().map(Change::hash).collect();

                let dependencies: HashSet<ChangeHash> = candidate
                    .changes()
                    .flat_map(|change| change.deps().iter().copied())
                    .filter(|dependency| {
                        !member_hashes.contains(dependency) && !accepted.contains(dependency)
                    })
                    .collect();

                for dependency in dependencies {
                    dependents
                        .entry(dependency)
                        .or_insert_with(Vec::new)
                        .push(index);
                }
            }

            dependents
        };
        Self {
            candidates,
            candidate_counts,
            dependents,
            excluded: vec![false; candidates.len()],
            to_exclude: Vec::new(),
            remaining: candidates.iter().enumerate().collect(),
            pending: Vec::new(),
        }
    }

    /// Run selection to completion using [`Selector::step`].
    ///
    /// Use the same accepted history supplied to [`Selector::new`]. Accepted
    /// history and signature evidence remain fixed throughout the calculation.
    fn run(
        mut self,
        accepted: &impl AcceptedHistory,
        signatures: &SignatureStore,
    ) -> Selection<'candidate, 'change> {
        loop {
            match self.step(accepted, signatures) {
                ControlFlow::Continue(next) => self = next,
                ControlFlow::Break(selection) => return selection,
            }
        }
    }

    /// Evaluate one round and process all resulting exclusions.
    ///
    /// Builds a [`Round`] from the remaining candidate changes, using accepted
    /// membership to stop ancestry traversal. Checks candidate requirements and
    /// calls [`Selector::exclude_and_propagate`].
    ///
    /// Returns [`ControlFlow::Continue`] with updated state when at least one
    /// candidate was excluded. Another round is needed because the removed history
    /// may have supplied attestation for surviving candidates.
    ///
    /// Returns [`ControlFlow::Break`] with the final [`Selection`] when no candidates
    /// were excluded. Every continuation reduces the number of remaining candidates,
    /// so repeated steps terminate.
    ///
    /// Use the same accepted history and signature evidence for every step.
    fn step(
        mut self,
        accepted: &impl AcceptedHistory,
        signatures: &SignatureStore,
    ) -> ControlFlow<Selection<'candidate, 'change>, Self> {
        let mut selected = Vec::new();
        let round = Round::new(
            self.remaining.iter().flat_map(|(_, c)| c.changes()),
            accepted,
            signatures,
        );

        for (index, candidate) in self.remaining.iter().copied() {
            if round.meets_requirements(candidate, accepted, signatures) {
                selected.push((index, candidate));
            } else {
                self.to_exclude.push(index);
            }
        }

        self.exclude_and_propagate(accepted);

        selected.retain(|(index, _)| !self.excluded[*index]);

        if selected.len() == self.remaining.len() {
            return ControlFlow::Break(self.finish());
        }

        self.remaining = selected;
        ControlFlow::Continue(self)
    }

    /// Exclude queued candidates and any candidates that lose required dependencies.
    ///
    /// Each candidate occurrence is processed once, even if queued repeatedly.
    /// Removing it reduces the counts of candidates supplying its member changes.
    ///
    /// When a count reaches zero, candidates requiring that change are queued too,
    /// unless the change is already accepted. Processing continues until the work
    /// stack is empty.
    ///
    /// This updates causal availability only. The next [`Round`] recalculates
    /// attestation from the surviving history.
    fn exclude_and_propagate(&mut self, accepted: &impl AcceptedHistory) {
        while let Some(index) = self.to_exclude.pop() {
            // Multiple missing dependencies may queue the same occurrence.
            // Record it and subtract its contributions exactly once.
            if self.excluded[index] {
                continue;
            }

            self.excluded[index] = true;
            self.pending.push(&self.candidates[index]);
            for change in self.candidates[index].changes() {
                let hash = change.hash();
                let count = self
                    .candidate_counts
                    .get_mut(&hash)
                    .expect("every candidate member was counted during initialization");

                *count -= 1;

                if *count == 0 && !accepted.contains(&hash) {
                    if let Some(affected) = self.dependents.get(&hash) {
                        self.to_exclude.extend(affected.iter().copied());
                    }
                }
            }
        }
    }

    /// Consume the completed calculation and return its final partition.
    ///
    /// Called by [`Selector::step`] once every remaining candidate satisfies its
    /// requirements and all queued exclusions have been processed.
    ///
    /// Both partitions borrow the original candidates. Their order is unspecified.
    fn finish(self) -> Selection<'candidate, 'change> {
        Selection {
            selected: self
                .remaining
                .into_iter()
                .map(|(_, candidate)| candidate)
                .collect(),
            pending: self.pending,
        }
    }
}

/// History and attestation available for one selection round.
///
/// Construct with [`Round::new`] and check candidates with
/// [`Round::meets_requirements`]. Rebuild after exclusions, because removed
/// history may have supplied dependencies or attestation.
///
/// This snapshot does not establish that all dependencies are available.
struct Round<'a> {
    /// Candidate members not already covered by accepted history.
    available: HashMap<ChangeHash, &'a Change>,
    /// Changes covered by direct evidence or a signed same-actor descendant
    /// within this round's available history.
    attested: HashSet<ChangeHash>,
}

impl<'a> Round<'a> {
    /// Gather unaccepted candidate history and determine which changes it can attest.
    ///
    /// Omits accepted members, deduplicates the rest by hash, and reads established
    /// Ed25519 evidence from [`SignatureStore`].
    ///
    /// Signed changes attest themselves and reachable ancestors of the same actor.
    /// Traversal can pass through other actors' changes without attesting them.
    /// Actor–author bindings are assumed consistent.
    ///
    /// Walks stop at hashes absent from the candidate map. Accepted dependencies
    /// need no traversal because all their ancestors are already accepted. Truly
    /// unavailable dependencies fail the separate [`Round::meets_requirements`] check.
    fn new(
        changes: impl IntoIterator<Item = &'a Change>,
        accepted: &impl AcceptedHistory,
        signatures: &SignatureStore,
    ) -> Self {
        let (available, directly_signed): (HashMap<ChangeHash, &Change>, Vec<&Change>) = changes
            .into_iter()
            .filter(|change| !accepted.contains(&change.hash()))
            .fold(
                (HashMap::new(), Vec::new()),
                |(mut available, mut directly_signed), change| {
                    let hash = change.hash();
                    let first_occurrence = available.insert(hash, change).is_none();
                    if first_occurrence && signatures.ed25519_entries(&hash).next().is_some() {
                        directly_signed.push(change);
                    }
                    (available, directly_signed)
                },
            );
        let mut attested: HashSet<ChangeHash> = directly_signed.iter().map(|c| c.hash()).collect();
        // Reuse completed walks only for the same actor and available history.
        let mut visited_by_actor: HashMap<&ActorId, HashSet<ChangeHash>> = HashMap::new();

        for signed in &directly_signed {
            let mut to_visit = signed.deps().to_vec();
            let visited = visited_by_actor.entry(signed.actor_id()).or_default();

            while let Some(hash) = to_visit.pop() {
                if !visited.insert(hash) {
                    continue;
                }

                let Some(change) = available.get(&hash) else {
                    continue;
                };

                if change.actor_id() == signed.actor_id() {
                    attested.insert(hash);
                }

                to_visit.extend(change.deps().iter().copied());
            }
        }

        Self {
            available,
            attested,
        }
    }

    /// Check a candidate's dependencies and attestation against this round.
    ///
    /// Every member's dependencies must be available. Already accepted members
    /// need no fresh evidence. Novel loose changes require direct evidence on their
    /// own hashes, while novel fragment members require coverage in
    /// [`Round::attested`].
    ///
    /// Passing is provisional. Excluding other candidates may remove dependencies
    /// or attestation support, requiring another round.
    ///
    /// Use the same signature store supplied to [`Round::new`].
    fn meets_requirements(
        &self,
        candidate: &CandidateHistory<'_>,
        accepted: &impl AcceptedHistory,
        signatures: &SignatureStore,
    ) -> bool {
        let dependencies_available = candidate.changes().all(|change| {
            accepted.contains(&change.hash())
                || change.deps().iter().all(|dependency| {
                    accepted.contains(dependency) || self.available.contains_key(dependency)
                })
        });

        let attestation_satisfied = candidate.changes().all(|change| {
            let hash = change.hash();

            if accepted.contains(&hash) {
                return true;
            }

            match candidate {
                CandidateHistory::Loose(_) => signatures.ed25519_entries(&hash).next().is_some(),
                CandidateHistory::CanonicalFragment { .. } => self.attested.contains(&hash),
            }
        });

        dependencies_available && attestation_satisfied
    }
}

#[cfg(test)]
mod tests;
