use std::collections::BTreeSet;

use crate::admission::tests::AcceptedHistoryArray;
use crate::admission::{select, CandidateHistory};
use crate::{Change, Fragment};

use super::super::assert_matches;
use super::{at_level, complete_fragment, evidence, valid_history};

// The accepted history is a single level-1 fragment with two actor chains.
// Its only retained signatures are the final changes for each actor. This
// fixture supplies its own admission argument rather than calling select.
fn accepted_fragment(rounds: usize) -> (Vec<Change>, Fragment, [usize; 2]) {
    let mut changes: Vec<Change> = Vec::new();
    let mut previous: [Option<usize>; 2] = [None; 2];
    for i in 0..rounds * 2 {
        let actor = i % 2;
        let mut deps = Vec::new();
        if let Some(last) = changes.last() {
            deps.push(last.hash());
        }
        if let Some(index) = previous[actor] {
            deps.push(changes[index].hash());
        }
        deps.sort();
        deps.dedup();
        changes.push(at_level(
            actor as u8,
            (i / 2 + 1) as u64,
            &deps,
            usize::from(i + 1 == rounds * 2),
        ));
        previous[actor] = Some(i);
    }
    valid_history(&changes);
    let fragment = complete_fragment(&changes);
    (changes, fragment, [rounds * 2 - 2, rounds * 2 - 1])
}

#[test]
fn pruned_accepted_fragment_supports_new_loose_and_overlapping_candidates() {
    for rounds in 2..=5 {
        let (accepted_changes, accepted_fragment, frontiers) = accepted_fragment(rounds);
        let accepted = AcceptedHistoryArray::from_admitted(&accepted_changes);
        let root = accepted_changes[0].hash();
        let tip = accepted_changes.last().unwrap().hash();
        let new = at_level(
            0,
            rounds as u64 + 1,
            &[accepted_changes[frontiers[0]].hash(), tip],
            0,
        );

        // Historical branch: actor C starts from the old root. Its level-1
        // fragment overlaps the accepted fragment but does not contain its tips.
        let branch_head = at_level(2, 1, &[root], 1);
        let branch_changes = vec![accepted_changes[0].clone(), branch_head];
        let branch_fragment = complete_fragment(&branch_changes);
        let mut all = accepted_changes.clone();
        all.push(new);
        all.push(branch_changes[1].clone());
        valid_history(&all);
        let candidates = [
            CandidateHistory::Loose(&all[accepted_changes.len()]),
            CandidateHistory::CanonicalFragment {
                fragment: &branch_fragment,
                members: branch_changes.iter().collect(),
            },
            CandidateHistory::CanonicalFragment {
                fragment: &accepted_fragment,
                members: accepted_changes.iter().collect(),
            },
        ];
        for mask in 0..4 {
            let mut indices = frontiers.to_vec();
            if mask & 1 != 0 {
                indices.push(accepted_changes.len());
            }
            if mask & 2 != 0 {
                indices.push(accepted_changes.len() + 1);
            }
            let (store, signed) = evidence(&all, &indices);
            assert_eq!(
                signed
                    .iter()
                    .filter(|hash| accepted_changes.iter().any(|c| c.hash() == **hash))
                    .count(),
                2
            );
            assert!(!signed.contains(&root));
            assert_matches(
                &accepted_changes,
                &candidates,
                &store,
                &signed,
                &format!("pruned rounds={rounds}, mask={mask}"),
            );
            let result = select(&accepted, &candidates, &store);
            assert_eq!(
                result.selected().len(),
                1 + (mask as u32).count_ones() as usize
            );
            assert_eq!(
                result.pending().len(),
                2 - (mask as u32).count_ones() as usize
            );
            assert!(result
                .selected()
                .iter()
                .any(|c| std::ptr::eq(*c, &candidates[2])));

            if mask & 2 != 0 {
                // The very same branch works from empty accepted state if the
                // accepted fragment is supplied as ordinary candidate history.
                let supporting = [
                    CandidateHistory::CanonicalFragment {
                        fragment: &branch_fragment,
                        members: branch_changes.iter().collect(),
                    },
                    CandidateHistory::CanonicalFragment {
                        fragment: &accepted_fragment,
                        members: accepted_changes.iter().collect(),
                    },
                ];
                assert_matches(
                    &[],
                    &supporting,
                    &store,
                    &signed,
                    "pruned history supplied alongside branch",
                );
                assert_eq!(
                    select(&AcceptedHistoryArray::default(), &supporting, &store)
                        .selected()
                        .len(),
                    2
                );
                // Stored signatures on missing old tips cannot help the branch alone.
                let result = select(&AcceptedHistoryArray::default(), &supporting[..1], &store);
                assert!(result.selected().is_empty());
            }
        }
        assert_eq!(
            frontiers
                .iter()
                .map(|i| accepted_changes[*i].actor_id())
                .collect::<BTreeSet<_>>()
                .len(),
            2
        );
    }
}
