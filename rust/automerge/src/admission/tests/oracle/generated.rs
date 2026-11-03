//! Bounded recipes and scenario families, not production fragment validation.
//! Real hashes, author footers and verified signatures exercise the selector.
//! Topology is specified before hashing so shrinking does not depend on hash order.

use std::collections::BTreeSet;
use std::num::NonZeroU64;

use ed25519_dalek::SigningKey;
use proptest::prelude::*;

use crate::admission::tests::AcceptedHistoryArray;
use crate::admission::{select, CandidateHistory};
use crate::change::encode_author_footer;
use crate::signature::SignatureStore;
use crate::{ActorId, Author, Automerge, Change, ChangeHash, ExpandedChange, Fragment};

use super::super::retain_signature;
use super::{admissible, assert_matches};

// Tuple fields: actor, extra-dependency bits, positive level, delivered, signed.
// Out-of-range dependency bits are ignored; the actor predecessor is always added.
type Step = (u8, u8, bool, bool, bool);

#[derive(Clone, Debug)]
struct Recipe {
    steps: Vec<Step>,
    accepted_len: usize,
    shared_authors: bool,
}

struct Materialized {
    changes: Vec<Change>,
    groups: Vec<(Option<Fragment>, Vec<Change>)>,
    signed: BTreeSet<ChangeHash>,
    store: SignatureStore,
    accepted_len: usize,
}

impl Materialized {
    fn candidates(&self) -> Vec<CandidateHistory<'_>> {
        self.groups
            .iter()
            .map(|(fragment, members)| match fragment {
                Some(fragment) => CandidateHistory::CanonicalFragment {
                    fragment,
                    members: members.iter().collect(),
                },
                None => CandidateHistory::Loose(&members[0]),
            })
            .collect()
    }
}

fn closure(roots: impl IntoIterator<Item = usize>, deps: &[Vec<usize>]) -> BTreeSet<usize> {
    let mut result = BTreeSet::new();
    let mut pending: Vec<_> = roots.into_iter().collect();
    while let Some(index) = pending.pop() {
        if result.insert(index) {
            pending.extend(deps[index].iter().copied());
        }
    }
    result
}

// Follow the intended immutable-ancestry rule: stop at the first equal-or-higher
// level ancestor on each dependency path, then subtract all boundary ancestry.
// With levels 0/1 these are the nearest positive-level ancestors on each path.
// Do not copy the unfinished cache's boundary, which can retain older covered heads.
fn canonical_members(
    head: usize,
    levels: &[usize],
    deps: &[Vec<usize>],
) -> (Vec<usize>, Vec<usize>) {
    let mut boundary = BTreeSet::new();
    let mut visited = BTreeSet::new();
    let mut pending = deps[head].clone();
    while let Some(index) = pending.pop() {
        if !visited.insert(index) {
            continue;
        }
        if levels[index] >= levels[head] {
            boundary.insert(index);
        } else {
            pending.extend(deps[index].iter().copied());
        }
    }
    let excluded = closure(boundary.iter().copied(), deps);
    let members = closure([head], deps)
        .difference(&excluded)
        .copied()
        .collect();
    (boundary.into_iter().collect(), members)
}

fn materialize(recipe: &Recipe) -> Materialized {
    assert!(recipe.steps.len() <= 6);
    let accepted_len = recipe.accepted_len.min(recipe.steps.len());
    let mut changes: Vec<Change> = Vec::new();
    let mut dependencies: Vec<Vec<usize>> = Vec::new();
    let mut levels = Vec::new();
    let mut previous = [None; 3];
    let mut sequences = [0u64; 3];
    let mut keys = Vec::new();
    for (index, &(actor, dep_bits, positive, _, _)) in recipe.steps.iter().enumerate() {
        let actor = usize::from(actor % 3);
        let key = SigningKey::from_bytes(
            &[if recipe.shared_authors {
                7
            } else {
                7 + actor as u8
            }; 32],
        );
        let author = Author::from(key.verifying_key().to_bytes().to_vec());
        let mut deps: BTreeSet<_> = (0..index)
            .filter(|dep| dep_bits & (1 << dep) != 0)
            .collect();
        if let Some(prev) = previous[actor] {
            deps.insert(prev);
        }
        let deps: Vec<_> = deps.into_iter().collect();
        sequences[actor] += 1;
        let seq = sequences[actor];
        let level = usize::from(positive);
        let footer = if seq == 1 { Some(author.clone()) } else { None };
        let mut hashes: Vec<_> = deps.iter().map(|dep| changes[*dep].hash()).collect();
        hashes.sort();
        let change = (0..20_000)
            .find_map(|time| {
                let change = Change::from(ExpandedChange {
                    operations: Vec::new(),
                    actor_id: ActorId::from([actor as u8 + 1]),
                    hash: None,
                    seq,
                    start_op: NonZeroU64::new(1).unwrap(),
                    time,
                    message: None,
                    deps: hashes.clone(),
                    // ExpandedChange conversion uses extra_bytes, not its author field.
                    extra_bytes: encode_author_footer(&footer).into_owned(),
                    author: footer.clone(),
                });
                (change.hash().fragment_level() == level).then_some(change)
            })
            .expect("bounded deterministic level search exhausted");
        assert_eq!(change.author(), footer);
        if let Some(prev) = previous[actor] {
            assert!(change.deps().contains(&changes[prev].hash()));
        }
        previous[actor] = Some(index);
        changes.push(change);
        dependencies.push(deps);
        levels.push(level);
        keys.push(key);
    }
    // Test fixture guard only: empty operations still form valid real changes.
    // Production admission and its canonical fragment cache are not used here.
    let mut doc = Automerge::new();
    doc.apply_changes(changes.clone())
        .expect("recipe must produce compatible history");

    let mut groups = Vec::new();
    let mut covered = BTreeSet::new();
    for head in 0..changes.len() {
        if levels[head] == 0 {
            continue;
        }
        let (boundary, members) = canonical_members(head, &levels, &dependencies);
        covered.extend(members.iter().copied());
        if !recipe.steps[head].3 {
            continue;
        }
        let fragment = Fragment {
            head: changes[head].hash(),
            level: levels[head],
            boundary: boundary.iter().map(|i| changes[*i].hash()).collect(),
            checkpoints: members
                .iter()
                .filter(|i| **i != head && levels[**i] > 0)
                .map(|i| changes[*i].hash())
                .collect(),
            members: members.iter().map(|i| changes[*i].hash()).collect(),
        };
        // Fixture guards, not validation performed by the admission selector.
        assert!(fragment.members.contains(&fragment.head));
        assert!(fragment
            .boundary
            .iter()
            .all(|hash| !fragment.members.contains(hash)));
        assert_eq!(fragment.level, fragment.head.fragment_level());
        let expected_checkpoints: BTreeSet<_> = fragment
            .members
            .iter()
            .copied()
            .filter(|hash| *hash != fragment.head && hash.fragment_level() > 0)
            .collect();
        assert_eq!(
            fragment
                .checkpoints
                .iter()
                .copied()
                .collect::<BTreeSet<_>>(),
            expected_checkpoints
        );
        groups.push((
            Some(fragment),
            members.iter().map(|i| changes[*i].clone()).collect(),
        ));
    }
    for index in 0..changes.len() {
        if levels[index] == 0 && !covered.contains(&index) && recipe.steps[index].3 {
            groups.push((None, vec![changes[index].clone()]));
        }
    }
    let mut signed = BTreeSet::new();
    let mut store = SignatureStore::default();
    for (index, change) in changes.iter().enumerate() {
        // Every accepted-prefix change has its own signature: accepted state
        // is valid by construction, not by calling the selector under test.
        if index < accepted_len || recipe.steps[index].4 {
            signed.insert(change.hash());
            retain_signature(&mut store, change, &keys[index]);
        }
    }
    Materialized {
        changes,
        groups,
        signed,
        store,
        accepted_len,
    }
}

fn selected_heads(result: &[&CandidateHistory<'_>]) -> BTreeSet<ChangeHash> {
    result
        .iter()
        .map(|candidate| match candidate {
            CandidateHistory::Loose(change) => change.hash(),
            CandidateHistory::CanonicalFragment { fragment, .. } => fragment.head,
        })
        .collect()
}

fn check_recipe(recipe: &Recipe) {
    let data = materialize(recipe);
    let accepted = &data.changes[..data.accepted_len];
    let candidates = data.candidates();
    let context = format!("{recipe:?}");
    assert_matches(accepted, &candidates, &data.store, &data.signed, &context);
    let accepted_view = AcceptedHistoryArray::from_admitted(accepted);
    let result = select(&accepted_view, &candidates, &data.store);
    let baseline = selected_heads(result.selected());

    // Holding additional signatures cannot remove an admissible selection.
    // No capacity eviction occurs: each hash receives at most one signature.
    let mut more_evidence = recipe.clone();
    for step in &mut more_evidence.steps {
        step.4 = true;
    }
    let more = materialize(&more_evidence);
    let more_candidates = more.candidates();
    let more_accepted = AcceptedHistoryArray::from_admitted(&more.changes[..more.accepted_len]);
    assert_matches(
        more_accepted.changes(),
        &more_candidates,
        &more.store,
        &more.signed,
        &format!("all evidence {context}"),
    );
    let after = selected_heads(select(&more_accepted, &more_candidates, &more.store).selected());
    assert!(
        baseline.is_subset(&after),
        "additional evidence reduced selection: {context}"
    );

    // Likewise for additional compatible candidates with the same evidence.
    let mut more_history = recipe.clone();
    for step in &mut more_history.steps {
        step.3 = true;
    }
    let more = materialize(&more_history);
    let more_candidates = more.candidates();
    let more_accepted = AcceptedHistoryArray::from_admitted(&more.changes[..more.accepted_len]);
    assert_matches(
        more_accepted.changes(),
        &more_candidates,
        &more.store,
        &more.signed,
        &format!("all candidates {context}"),
    );
    let after = selected_heads(select(&more_accepted, &more_candidates, &more.store).selected());
    assert!(
        baseline.is_subset(&after),
        "additional candidates reduced selection: {context}"
    );

    // Remove only pending history, keeping the global store unchanged.
    let survivors: Vec<_> = result
        .selected()
        .iter()
        .map(|candidate| match candidate {
            CandidateHistory::Loose(change) => CandidateHistory::Loose(change),
            CandidateHistory::CanonicalFragment { fragment, members } => {
                CandidateHistory::CanonicalFragment {
                    fragment,
                    members: members.clone(),
                }
            }
        })
        .collect();
    let stable = select(&accepted_view, &survivors, &data.store);
    assert!(
        stable.pending().is_empty(),
        "survivors are not stable: {context}"
    );
    assert_eq!(selected_heads(stable.selected()), baseline);

    // Member order has no semantic meaning. In particular, a dependency of a
    // non-head member must not be missed by only inspecting the first member.
    let reordered_members: Vec<Vec<_>> = data
        .groups
        .iter()
        .map(|(_, members)| members.iter().rev().cloned().collect())
        .collect();
    let reordered: Vec<_> = data
        .groups
        .iter()
        .zip(&reordered_members)
        .map(|((fragment, _), members)| match fragment {
            Some(fragment) => CandidateHistory::CanonicalFragment {
                fragment,
                members: members.iter().collect(),
            },
            None => CandidateHistory::Loose(&members[0]),
        })
        .collect();
    assert_matches(
        accepted,
        &reordered,
        &data.store,
        &data.signed,
        &format!("member order {context}"),
    );
    assert_eq!(
        selected_heads(select(&accepted_view, &reordered, &data.store).selected()),
        baseline
    );

    let mut rotated = data.candidates();
    if !rotated.is_empty() {
        rotated.rotate_left(1);
    }
    assert_matches(
        accepted,
        &rotated,
        &data.store,
        &data.signed,
        &format!("rotated {context}"),
    );
    assert_eq!(
        selected_heads(select(&accepted_view, &rotated, &data.store).selected()),
        baseline
    );

    let mut reversed = data.candidates();
    reversed.reverse();
    assert_matches(
        accepted,
        &reversed,
        &data.store,
        &data.signed,
        &format!("reversed {context}"),
    );
    assert_eq!(
        selected_heads(select(&accepted_view, &reversed, &data.store).selected()),
        baseline
    );

    // Duplicate deliveries must still produce an exact partition of input
    // occurrences, even when their fragment head and members coincide.
    if let Some(first) = candidates.first().filter(|_| candidates.len() < 6) {
        let mut duplicate = data.candidates();
        duplicate.push(match first {
            CandidateHistory::Loose(change) => CandidateHistory::Loose(change),
            CandidateHistory::CanonicalFragment { fragment, members } => {
                CandidateHistory::CanonicalFragment {
                    fragment,
                    members: members.clone(),
                }
            }
        });
        assert_matches(
            accepted,
            &duplicate,
            &data.store,
            &data.signed,
            &format!("duplicate {context}"),
        );
    }
}

#[test]
fn canonical_fixture_builder_excludes_boundary_ancestry() {
    let deps = vec![vec![], vec![0], vec![0, 1]];
    let (boundary, members) = canonical_members(2, &[0, 1, 1], &deps);
    assert_eq!(boundary, [1]);
    assert_eq!(members, [2]);
    let (boundary, members) = canonical_members(1, &[0, 1, 1], &deps);
    assert!(boundary.is_empty());
    assert_eq!(members, [0, 1]);
}

#[test]
fn chained_level_one_heads_stop_at_the_first_boundary_on_each_path() {
    let data = materialize(&Recipe {
        steps: vec![
            (0, 0, true, true, true),
            (0, 0, true, true, true),
            (0, 0, true, true, true),
        ],
        accepted_len: 0,
        shared_authors: false,
    });
    let hashes: Vec<_> = data.changes.iter().map(Change::hash).collect();
    assert_eq!(data.changes[2].deps(), &[hashes[1]]);
    let third = data.groups[2].0.as_ref().unwrap();
    // The root is behind the immediate predecessor. It is not independently
    // encountered as a boundary on this head's only dependency path.
    assert_eq!(third.head, hashes[2]);
    assert_eq!(third.level, 1);
    assert_eq!(third.boundary, [hashes[1]]);
    assert_eq!(third.members, [hashes[2]]);
    assert!(third.checkpoints.is_empty());
    assert_matches(
        &[],
        &data.candidates(),
        &data.store,
        &data.signed,
        "three chained level-one heads",
    );

    // If the head also directly depends on the root, that separate path does
    // encounter the root as a boundary. Do not blindly reduce boundary ancestry.
    let (boundary, members) = canonical_members(2, &[1, 1, 1], &[vec![], vec![0], vec![0, 1]]);
    assert_eq!(boundary, [0, 1]);
    assert_eq!(members, [2]);
}

#[test]
fn canonical_fixture_builder_matches_known_fragment_descriptors() {
    // Compare only known fixture graphs, not arbitrary current cache output.
    for fixture in [
        super::fixtures::internal_chain(),
        super::fixtures::overlapping(),
        super::fixtures::mutual_support(),
    ] {
        let changes = &fixture.changes;
        let levels: Vec<_> = changes.iter().map(|c| c.hash().fragment_level()).collect();
        let deps: Vec<Vec<usize>> = changes
            .iter()
            .map(|change| {
                change
                    .deps()
                    .iter()
                    .map(|hash| changes.iter().position(|c| c.hash() == *hash).unwrap())
                    .collect()
            })
            .collect();
        for candidate in fixture.candidates() {
            let CandidateHistory::CanonicalFragment { fragment, .. } = candidate else {
                continue;
            };
            let head = changes
                .iter()
                .position(|c| c.hash() == fragment.head)
                .unwrap();
            let (boundary, members) = canonical_members(head, &levels, &deps);
            let hashes = |indices: Vec<usize>| {
                indices
                    .into_iter()
                    .map(|i| changes[i].hash())
                    .collect::<BTreeSet<_>>()
            };
            assert_eq!(
                hashes(boundary),
                fragment.boundary.iter().copied().collect()
            );
            assert_eq!(hashes(members), fragment.members.iter().copied().collect());
        }
    }
}

#[test]
fn three_round_fragment_exclusion_cascade() {
    // Z={a0,z}, Y={b1,ay}, X={b1,c0,b2}. X fails on c0, Y then loses
    // b2's evidence for b1, and Z finally loses ay's evidence for a0,z.
    let mut recipe = Recipe {
        steps: vec![
            (0, 0, false, true, false),
            (0, 0, true, true, false),
            (1, 0, false, true, false),
            (2, 0, false, true, false),
            (0, 4, true, true, true),
            (1, 8, true, true, true),
        ],
        accepted_len: 0,
        shared_authors: false,
    };
    let data = materialize(&recipe);
    let candidates = data.candidates();
    assert_eq!(candidates.len(), 3);
    assert_matches(
        &[],
        &candidates,
        &data.store,
        &data.signed,
        "three-round cascade",
    );
    let accepted = AcceptedHistoryArray::default();
    let selection = select(&accepted, &candidates, &data.store);
    assert!(selection.selected().is_empty());
    assert_eq!(selection.pending().len(), 3);

    recipe.steps[3].4 = true;
    let data = materialize(&recipe);
    let candidates = data.candidates();
    assert_matches(
        &[],
        &candidates,
        &data.store,
        &data.signed,
        "cascade with blocker signed",
    );
    let selection = select(&accepted, &candidates, &data.store);
    assert_eq!(selection.selected().len(), 3);
    assert!(selection.pending().is_empty());
}

#[test]
fn recipe_materialization_uses_real_signatures_and_valid_author_chains() {
    let data = materialize(&Recipe {
        steps: vec![
            (0, 0, false, true, false),
            (1, 1, true, true, true),
            (0, 2, true, true, true),
        ],
        accepted_len: 0,
        shared_authors: true,
    });
    assert_eq!(data.groups.len(), 2);
    assert_eq!(data.signed.len(), 2);
    let candidates = data.candidates();
    assert!(!admissible(&[], &candidates, &data.signed, 1));
    assert!(!admissible(&[], &candidates, &data.signed, 2));
    assert!(admissible(&[], &candidates, &data.signed, 3));
    check_recipe(&Recipe {
        steps: vec![
            (0, 0, false, true, false),
            (1, 1, true, true, true),
            (0, 2, true, true, true),
        ],
        accepted_len: 0,
        shared_authors: true,
    });
}

#[test]
fn generalized_chains_and_exclusion_cascades() {
    for length in 1..=6 {
        for signed_mask in 0u8..(1 << length) {
            for fragment_tip in [false, true] {
                let recipe = Recipe {
                    steps: (0..length)
                        .map(|i| {
                            (
                                0,
                                0,
                                fragment_tip && i + 1 == length,
                                true,
                                signed_mask & (1 << i) != 0,
                            )
                        })
                        .collect(),
                    accepted_len: 0,
                    shared_authors: false,
                };
                let data = materialize(&recipe);
                let candidates = data.candidates();
                assert_matches(
                    &[],
                    &candidates,
                    &data.store,
                    &data.signed,
                    &format!("chain {recipe:?}"),
                );
                let accepted = AcceptedHistoryArray::default();
                let selection = select(&accepted, &candidates, &data.store);
                // Independent known answers for these families.
                let expected = if fragment_tip {
                    usize::from(signed_mask & (1 << (length - 1)) != 0)
                } else {
                    (0..length)
                        .take_while(|i| signed_mask & (1 << i) != 0)
                        .count()
                };
                assert_eq!(selection.selected().len(), expected, "{recipe:?}");
            }
        }
    }
}

#[test]
fn targeted_branch_diamond_and_mutual_support_recipes() {
    let cases = [
        // Diamond, two actors share a key in the second pass. Signature at
        // the merge must not attest different actors on either branch.
        vec![
            (0, 0, false, true, false),
            (1, 1, false, true, false),
            (2, 1, false, true, false),
            (0, 6, true, true, true),
        ],
        // Historical branch with cross-fragment evidence.
        vec![
            (0, 0, false, true, false),
            (1, 1, false, true, false),
            (0, 2, true, true, true),
            (2, 1, true, true, true),
        ],
        // Mutual support: both canonical candidates are necessary.
        vec![
            (0, 0, false, true, false),
            (1, 1, true, true, true),
            (0, 2, true, true, true),
        ],
        // An external dependency is carried by a non-head member: root A is
        // a boundary and not supplied. Head B3 directly depends only on B2.
        vec![
            (0, 0, true, false, true),
            (1, 1, false, true, false),
            (1, 0, false, true, false),
            (1, 0, true, true, true),
        ],
        // Six loose changes: five dependent signatures cannot rescue the root.
        vec![
            (0, 0, false, true, false),
            (0, 0, false, true, true),
            (0, 0, false, true, true),
            (0, 0, false, true, true),
            (0, 0, false, true, true),
            (0, 0, false, true, true),
        ],
    ];
    for steps in cases {
        for accepted_len in [0, 1, steps.len()] {
            for shared_authors in [false, true] {
                check_recipe(&Recipe {
                    steps: steps.clone(),
                    accepted_len,
                    shared_authors,
                });
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn bounded_dags_match_subset_oracle(
        steps in prop::collection::vec((0u8..3, any::<u8>(), any::<bool>(), any::<bool>(), any::<bool>()), 0..7),
        accepted_len in 0usize..7,
        shared_authors in any::<bool>(),
    ) {
        check_recipe(&Recipe { steps, accepted_len, shared_authors });
    }
}
