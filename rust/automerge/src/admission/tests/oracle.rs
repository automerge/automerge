//! Exhaustive subset reference for small, compatible admission inputs.
//!
//! The expected signed hashes come from fixture choices, not SignatureStore.
//! This checker must not call the production ancestry walk or selection helpers.

use std::collections::{BTreeMap, BTreeSet};

use crate::{Change, ChangeHash};

use crate::admission::{select, AcceptedHistory, CandidateHistory};
use crate::signature::SignatureStore;

use super::retain_signature;

mod extended;
mod fixtures;
mod generated;

// Pattern-match the input directly rather than sharing CandidateHistory::changes.
fn members<'a>(candidate: &'a CandidateHistory<'_>) -> &'a [Change] {
    match candidate {
        CandidateHistory::Loose(change) => std::slice::from_ref(*change),
        CandidateHistory::CanonicalFragment { members, .. } => members,
    }
}

/// Check whether exactly the candidates selected by `subset` can be admitted.
///
/// Bit `i` selects `candidates[i]`. Available history is the union of accepted
/// changes and selected members, deduplicated by hash. Every dependency must
/// occur in that union, otherwise the subset is rejected before checking evidence.
///
/// Every selected member must then satisfy its attestation requirement:
/// - Already accepted members need no fresh evidence.
/// - Novel loose changes need direct evidence on their own hashes.
/// - Novel fragment members need evidence on themselves or an available
///   same-actor descendant. Sharing an author across different actors is insufficient.
///
/// `signed` independently records hashes with eligible evidence, regardless of
/// which delivery supplied it. A hash in this set cannot act as a witness unless
/// its change is in the available history. No cryptographic verification occurs here.
///
/// Canonical membership, compatible history, and consistent actor–author bindings
/// are input preconditions. Actor equality therefore identifies the same binding.
/// This reference checks ancestry with a reachability table rather than the
/// production selector's signed-root walks. It neither excludes candidates nor
/// searches for the greatest selection. [`expected_selection`] performs that search.
fn admissible(
    accepted: &[Change],
    candidates: &[CandidateHistory<'_>],
    signed: &BTreeSet<ChangeHash>,
    subset: usize,
) -> bool {
    let chosen: Vec<_> = candidates
        .iter()
        .enumerate()
        .filter(|(index, _)| subset & (1 << index) != 0)
        .map(|(_, candidate)| candidate)
        .collect();
    let mut by_hash = BTreeMap::new();
    for change in accepted {
        by_hash.insert(change.hash(), change);
    }
    for candidate in &chosen {
        for change in members(candidate) {
            by_hash.insert(change.hash(), change);
        }
    }
    let changes: Vec<_> = by_hash.values().copied().collect();
    let indices: BTreeMap<_, _> = changes
        .iter()
        .enumerate()
        .map(|(index, change)| (change.hash(), index))
        .collect();
    let n = changes.len();
    // reachable[from][to] means following dependencies from `from` reaches `to`.
    // Self-links allow a change's own signature to attest it.
    let mut reachable = vec![vec![false; n]; n];
    for (i, change) in changes.iter().enumerate() {
        reachable[i][i] = true;
        for dependency in change.deps() {
            let Some(&j) = indices.get(dependency) else {
                return false;
            };
            reachable[i][j] = true;
        }
    }
    // Floyd–Warshall transitive closure over available nodes only. Each outer
    // iteration permits another node as an intermediate step, so no topological
    // ordering is required. Reachability alone says nothing about attestation.
    for via in 0..n {
        for from in 0..n {
            for to in 0..n {
                reachable[from][to] |= reachable[from][via] && reachable[via][to];
            }
        }
    }

    let accepted_hashes: BTreeSet<_> = accepted.iter().map(Change::hash).collect();
    chosen.iter().all(|candidate| {
        members(candidate).iter().all(|member| {
            let hash = member.hash();
            if accepted_hashes.contains(&hash) {
                return true;
            }
            match candidate {
                CandidateHistory::Loose(_) => signed.contains(&hash),
                CandidateHistory::CanonicalFragment { .. } => {
                    let target = indices[&hash];
                    changes.iter().enumerate().any(|(source, witness)| {
                        signed.contains(&witness.hash())
                            && witness.actor_id() == member.actor_id()
                            && reachable[source][target]
                    })
                }
            }
        })
    })
}

/// Find the greatest admissible selection by checking every candidate subset.
///
/// Returns a bitmask where bit `i` selects `candidates[i]`. Each subset is checked
/// independently with [`admissible`], including combinations whose candidates
/// cannot be admitted individually. The result unions all successful subsets,
/// so it contains every admissible selection rather than merely one safe choice.
///
/// With compatible history and fixed evidence, combining admissible subsets
/// preserves their support. This does not imply that adding an arbitrary candidate
/// is safe. The final assertion checks that the union is itself admissible, making
/// it the greatest selection under the contract.
///
/// # Panics
///
/// Panics if there are more than 6 candidates, bounding the search to 64 subsets,
/// or if the union of admissible subsets is not admissible.
fn expected_selection(
    accepted: &[Change],
    candidates: &[CandidateHistory<'_>],
    signed: &BTreeSet<ChangeHash>,
) -> usize {
    assert!(candidates.len() <= 6, "oracle is deliberately bounded");
    let mut union = 0;
    for subset in 0..(1 << candidates.len()) {
        if admissible(accepted, candidates, signed, subset) {
            union |= subset;
        }
    }
    assert!(
        admissible(accepted, candidates, signed, union),
        "admissible subsets must have an admissible union"
    );
    union
}

/// Check that [`select`] returns the greatest admissible selection and an exact
/// partition of the supplied candidates.
///
/// Production reads `store`, while [`expected_selection`] reads the independently
/// recorded `signed` hashes. The caller must populate both with matching evidence
/// and provide valid accepted history.
///
/// Returned references are matched to input positions by pointer identity, so
/// comparison does not depend on output order or candidate content equality.
/// Every input must appear exactly once across selected and pending, with no
/// foreign references, duplicates, overlap, or omissions.
///
/// The selected subset must pass [`admissible`] and equal the oracle's greatest
/// selection. These assertions distinguish unsafe admission from unnecessary
/// exclusion, including an implementation that always selects nothing. `context`
/// identifies the fixture and variation in failure diagnostics.
fn assert_matches(
    accepted: &[Change],
    candidates: &[CandidateHistory<'_>],
    store: &SignatureStore,
    signed: &BTreeSet<ChangeHash>,
    context: &str,
) {
    let expected = expected_selection(accepted, candidates, signed);
    let accepted = AcceptedHistory::from_admitted(accepted);
    let result = select(&accepted, candidates, store);
    let mut seen = vec![false; candidates.len()];
    let mut actual = 0;
    for (is_selected, partition) in [(true, result.selected()), (false, result.pending())] {
        for entry in partition {
            let index = candidates
                .iter()
                .position(|candidate| std::ptr::eq(*entry, candidate))
                .expect("result must refer to a supplied candidate");
            assert!(
                !seen[index],
                "duplicate or overlapping partition entry: {context}"
            );
            seen[index] = true;
            if is_selected {
                actual |= 1 << index;
            }
        }
    }
    assert!(
        seen.iter().all(|seen| *seen),
        "incomplete partition: {context}"
    );
    assert!(
        admissible(accepted.changes(), candidates, signed, actual),
        "unsafe selection: {context}, selected={actual:b}"
    );
    assert_eq!(
        actual, expected,
        "not the greatest admissible selection: {context}"
    );
}

/// Compare production and reference results across evidence and candidate
/// availability for one fixed history fixture.
///
/// Enumerates every signature subset containing `fixture.required_signed`, which
/// preserves the evidence required by the accepted prefix. Each subset gets an
/// independent signed-hash record and a fresh store populated with real verified
/// signatures. Their agreement is checked before calling [`assert_matches`].
///
/// For each evidence configuration, tries every supplied-candidate subset in
/// forward and reverse order. Evidence remains stored even when its signed change
/// is absent from those candidates, exercising the available-history restriction.
/// [`expected_selection`] then enumerates possible selections within each supplied
/// subset. The fixture's accepted history remains fixed throughout.
///
/// Returns the number of comparisons, including both order passes for empty and
/// single-candidate inputs. Coverage is exhaustive over these availability choices,
/// not over arbitrary histories, all candidate permutations, or delivery schedules.
fn check_variations(name: &str, fixture: fixtures::Fixture) -> usize {
    let all = fixture.candidates();
    let accepted = &fixture.changes[..fixture.accepted_len];
    let mut comparisons = 0;
    for signatures in 0usize..(1 << fixture.changes.len()) {
        if signatures & fixture.required_signed != fixture.required_signed {
            continue;
        }
        let signed: BTreeSet<_> = fixture
            .changes
            .iter()
            .enumerate()
            .filter(|(index, _)| signatures & (1 << index) != 0)
            .map(|(_, change)| change.hash())
            .collect();
        let mut store = SignatureStore::default();
        for change in &fixture.changes {
            if signed.contains(&change.hash()) {
                retain_signature(&mut store, change, &fixture.keys[change.actor_id()]);
            }
        }
        // The symbolic evidence record and real store are checked independently
        // at the seam. The reference itself never queries the store.
        for change in &fixture.changes {
            assert_eq!(
                store.ed25519_entries(&change.hash()).count(),
                usize::from(signed.contains(&change.hash()))
            );
        }
        for supplied in 0usize..(1 << all.len()) {
            let mut candidates: Vec<_> = all
                .iter()
                .enumerate()
                .filter(|(index, _)| supplied & (1 << index) != 0)
                .map(|(_, candidate)| match candidate {
                    CandidateHistory::Loose(change) => CandidateHistory::Loose(change),
                    CandidateHistory::CanonicalFragment { fragment, members } => {
                        CandidateHistory::CanonicalFragment { fragment, members }
                    }
                })
                .collect();
            for reverse in [false, true] {
                if reverse {
                    candidates.reverse();
                }
                let context = format!(
                    "{name}: signatures={signatures:b}, supplied={supplied:b}, reverse={reverse}"
                );
                assert_matches(accepted, &candidates, &store, &signed, &context);
                comparisons += 1;
            }
        }
    }
    comparisons
}

#[test]
fn selection_matches_reference_for_all_fixture_variations() {
    let comparisons = [
        ("overlap", fixtures::overlapping()),
        ("loose chain", fixtures::loose_chain()),
        ("internal chain", fixtures::internal_chain()),
        ("mutual support", fixtures::mutual_support()),
        ("accepted prefix", fixtures::accepted_prefix()),
    ]
    .into_iter()
    .map(|(name, fixture)| check_variations(name, fixture))
    .sum::<usize>();
    // Includes both candidate orders. Subsets are enumerated inside each comparison.
    assert_eq!(comparisons, 384);
}

#[test]
fn reference_accepts_the_empty_selection() {
    assert!(admissible(&[], &[], &BTreeSet::new(), 0));
}

#[test]
fn reference_loose_chain_requires_both_direct_signatures_and_dependencies() {
    let fixture = fixtures::loose_chain();
    let candidates = fixture.candidates();
    let a1 = fixture.changes[0].hash();
    let a2 = fixture.changes[1].hash();
    let a3 = fixture.changes[2].hash();

    // Only the empty selection works with just the final loose change signed.
    for subset in 0..8 {
        assert_eq!(
            admissible(&[], &candidates, &BTreeSet::from([a3]), subset),
            subset == 0
        );
    }
    // Signing the first two changes admits exactly the dependency-closed prefixes.
    for subset in 0..8 {
        assert_eq!(
            admissible(&[], &candidates, &BTreeSet::from([a1, a2]), subset),
            matches!(subset, 0 | 1 | 3),
        );
    }
}

#[test]
fn reference_fragment_evidence_covers_ancestors_not_descendants() {
    let fixture = fixtures::internal_chain();
    let candidates = fixture.candidates();
    let earlier = fixture.changes[1].hash();
    let tip = fixture.changes[2].hash();

    assert!(!admissible(&[], &candidates, &BTreeSet::from([earlier]), 1));
    assert!(admissible(&[], &candidates, &BTreeSet::from([tip]), 1));
}

#[test]
fn reference_distinguishes_shared_direct_evidence_from_unavailable_descendants() {
    let fixture = fixtures::overlapping();
    let candidates = fixture.candidates();
    let [a, x, c, f] = fixture.changes.iter().map(Change::hash).collect::<Vec<_>>()[..] else {
        panic!("expected four-change overlap fixture");
    };
    let mut signed = BTreeSet::from([c, f]);
    // G is blocked on x, so c cannot help admit F without G's history.
    assert_eq!(expected_selection(&[], &candidates, &signed), 0);
    // Direct evidence for a works with F alone, irrespective of delivery source.
    signed.insert(a);
    assert_eq!(expected_selection(&[], &candidates, &signed), 0b01);
    // Evidence for x instead enables G and the cross-fragment witness c.
    signed.remove(&a);
    signed.insert(x);
    assert_eq!(expected_selection(&[], &candidates, &signed), 0b11);
}

#[test]
fn reference_admits_mutual_support_only_as_a_joint_selection() {
    let fixture = fixtures::mutual_support();
    let candidates = fixture.candidates();
    let signed = BTreeSet::from([fixture.changes[1].hash(), fixture.changes[2].hash()]);
    assert!(!admissible(&[], &candidates, &signed, 0b01));
    assert!(!admissible(&[], &candidates, &signed, 0b10));
    assert!(admissible(&[], &candidates, &signed, 0b11));
    assert_eq!(expected_selection(&[], &candidates, &signed), 0b11);
}

#[test]
fn reference_keeps_an_accepted_prefix_independent_of_new_signatures() {
    let fixture = fixtures::accepted_prefix();
    let candidates = fixture.candidates();
    let accepted = &fixture.changes[..2];
    let mut signed = BTreeSet::from([fixture.changes[1].hash()]);
    // Candidate 0 repeats a1, already accepted without retaining sig(a1).
    // Candidate 1 is the novel, as-yet-unsigned loose change a3.
    assert_eq!(expected_selection(accepted, &candidates, &signed), 0b01);
    signed.insert(fixture.changes[2].hash());
    assert_eq!(expected_selection(accepted, &candidates, &signed), 0b11);
}
