use std::cell::RefCell;
use std::collections::HashSet;

use proptest::prelude::*;

use super::{select_fragments, PublishedHistory};
use crate::{ChangeHash, Fragment};

#[derive(Default)]
struct SetHistory {
    hashes: HashSet<ChangeHash>,
    queries: RefCell<Vec<ChangeHash>>,
}

impl SetHistory {
    fn new(hashes: impl IntoIterator<Item = ChangeHash>) -> Self {
        Self {
            hashes: hashes.into_iter().collect(),
            queries: RefCell::new(Vec::new()),
        }
    }
}

impl PublishedHistory for SetHistory {
    fn contains(&self, hash: &ChangeHash) -> bool {
        self.queries.borrow_mut().push(*hash);
        self.hashes.contains(hash)
    }
}

fn hash(id: u8) -> ChangeHash {
    ChangeHash([id; 32])
}

fn frag(head: u8, members: &[u8], boundary: &[u8]) -> Fragment {
    Fragment {
        head: hash(head),
        level: usize::from(members.len() > 1),
        boundary: boundary.iter().copied().map(hash).collect(),
        checkpoints: Vec::new(),
        members: members.iter().copied().map(hash).collect(),
    }
}

/// Independent brute-force subset oracle: find the largest subset of candidate
/// indices whose boundaries are all satisfied by `published` or subset members.
fn oracle_selected_indices(published: &SetHistory, fragments: &[Fragment]) -> Vec<usize> {
    assert!(fragments.len() <= 12);
    let mut best_mask = 0usize;
    let mut best_count = 0usize;
    for mask in 0..(1usize << fragments.len()) {
        let count = mask.count_ones() as usize;
        let supplied: HashSet<ChangeHash> = fragments
            .iter()
            .enumerate()
            .filter(|(i, _)| mask & (1 << i) != 0)
            .flat_map(|(_, f)| f.members.iter().copied())
            .collect();
        let closed = fragments
            .iter()
            .enumerate()
            .filter(|(i, _)| mask & (1 << i) != 0)
            .all(|(_, f)| {
                f.boundary
                    .iter()
                    .all(|dep| published.hashes.contains(dep) || supplied.contains(dep))
            });
        if closed && count >= best_count {
            best_mask = mask;
            best_count = count;
        }
    }
    (0..fragments.len())
        .filter(|i| best_mask & (1 << i) != 0)
        .collect()
}

#[test]
fn missing_dependency_propagates_exclusion_along_causal_chain() {
    let published = SetHistory::default();
    // f1 waits on missing 1; f2 waits on f1; f3 has no missing boundary.
    let candidates = [
        frag(2, &[2], &[1]),
        frag(3, &[3, 4], &[2]),
        frag(5, &[5], &[]),
    ];
    let selection = select_fragments(&published, &candidates);
    assert_eq!(selection.selected_indices().collect::<Vec<_>>(), [2]);
    assert_eq!(selection.pending_indices().collect::<Vec<_>>(), [0, 1]);
}

#[test]
fn overlapping_supplier_keeps_dependent_admissible_until_last_supplier_is_excluded() {
    let published = SetHistory::default();
    // Both f_bad and f_good supply member 2; f_bad lacks boundary 1, f_good is closed.
    // Dependent f_child needs 2 and remains admissible via f_good!
    let f_bad = frag(3, &[2, 3], &[1]);
    let f_good = frag(4, &[2, 4], &[]);
    let f_child = frag(5, &[5], &[2]);
    let candidates = [f_bad.clone(), f_good, f_child.clone()];
    let selection = select_fragments(&published, &candidates);
    assert_eq!(selection.selected_indices().collect::<Vec<_>>(), [1, 2]);
    assert_eq!(selection.pending_indices().collect::<Vec<_>>(), [0]);

    // Without f_good, excluding f_bad removes the last supplier of 2 and excludes f_child.
    let without_good = [f_bad, f_child];
    let selection = select_fragments(&published, &without_good);
    assert_eq!(selection.selected().len(), 0);
    assert_eq!(selection.pending_indices().collect::<Vec<_>>(), [0, 1]);
}

#[test]
fn duplicate_occurrences_and_published_boundaries_preserve_exact_partition() {
    let published = SetHistory::new([hash(1), hash(99)]);
    let f = frag(2, &[2], &[1]);
    let waiting = frag(4, &[4], &[3]);
    let candidates = [f.clone(), waiting.clone(), f, waiting];
    let selection = select_fragments(&published, &candidates);
    assert_eq!(selection.selected_indices().collect::<Vec<_>>(), [0, 2]);
    assert_eq!(selection.pending_indices().collect::<Vec<_>>(), [1, 3]);
    // Unrelated published history is never scanned.
    assert!(!published.queries.borrow().contains(&hash(99)));
}

proptest! {
    #[test]
    fn selection_matches_independent_maximal_closed_subset_oracle(
        published_ids in prop::collection::vec(1u8..14, 0..5),
        specs in prop::collection::vec(
            (
                1u8..14,
                prop::collection::vec(1u8..14, 1..4),
                prop::collection::vec(1u8..14, 0..4),
            ),
            0..9,
        ),
    ) {
        let published = SetHistory::new(published_ids.into_iter().map(hash));
        let fragments: Vec<Fragment> = specs
            .into_iter()
            .map(|(head, mut members, boundary)| {
                if !members.contains(&head) {
                    members.push(head);
                }
                members.sort_unstable();
                members.dedup();
                frag(head, &members, &boundary)
            })
            .collect();

        let expected = oracle_selected_indices(&published, &fragments);
        let selection = select_fragments(&published, &fragments);
        let actual_selected: Vec<usize> = selection.selected_indices().collect();
        let actual_pending: Vec<usize> = selection.pending_indices().collect();

        prop_assert_eq!(&actual_selected, &expected);
        prop_assert_eq!(actual_selected.len() + actual_pending.len(), fragments.len());
        for i in 0..fragments.len() {
            prop_assert_eq!(actual_selected.contains(&i), !actual_pending.contains(&i));
        }
    }
}
