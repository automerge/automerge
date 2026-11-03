//! Source-count invariants for the causal exclusion work list.
//! These tests exercise results and input partitions, not private queue order.

use std::collections::BTreeSet;

use crate::admission::tests::AcceptedHistoryArray;
use crate::admission::{select, CandidateHistory};
use crate::{Change, ChangeHash};

use super::super::assert_matches;
use super::{at_level, complete_fragment, evidence, valid_history};

fn partitions_for_orders(
    accepted: &[Change],
    candidates: &[CandidateHistory<'_>],
    store: &crate::signature::SignatureStore,
    signed: &BTreeSet<ChangeHash>,
    selected_indices: &[usize],
) {
    // Enumerate permutations: the work list's LIFO order must not affect results.
    fn permutations(indices: &mut [usize], start: usize, visit: &mut impl FnMut(&[usize])) {
        if start == indices.len() {
            visit(indices);
            return;
        }
        for index in start..indices.len() {
            indices.swap(start, index);
            permutations(indices, start + 1, visit);
            indices.swap(start, index);
        }
    }
    let mut indices: Vec<_> = (0..candidates.len()).collect();
    permutations(&mut indices, 0, &mut |order| {
        let ordered: Vec<_> = order
            .iter()
            .map(|i| match &candidates[*i] {
                CandidateHistory::Loose(change) => CandidateHistory::Loose(change),
                CandidateHistory::CanonicalFragment { fragment, members } => {
                    CandidateHistory::CanonicalFragment {
                        fragment,
                        members: members.clone(),
                    }
                }
            })
            .collect();
        assert_matches(
            accepted,
            &ordered,
            store,
            signed,
            &format!("propagation order={order:?}"),
        );
        let accepted = AcceptedHistoryArray::from_admitted(accepted);
        let result = select(&accepted, &ordered, store);
        assert_eq!(result.selected().len(), selected_indices.len());
        assert_eq!(
            result.pending().len(),
            ordered.len() - selected_indices.len()
        );
        for (position, original) in order.iter().enumerate() {
            let selected_count = result
                .selected()
                .iter()
                .filter(|c| std::ptr::eq(**c, &ordered[position]))
                .count();
            let pending_count = result
                .pending()
                .iter()
                .filter(|c| std::ptr::eq(**c, &ordered[position]))
                .count();
            assert_eq!(
                selected_count,
                usize::from(selected_indices.contains(original))
            );
            assert_eq!(pending_count, 1 - selected_count);
        }
    });
}

#[test]
fn shared_change_disappears_only_after_its_last_candidate_is_excluded() {
    // F={s,f}, G={s,g}, loose H depends only on s, not on either fragment head.
    let s = at_level(0, 1, &[], 0);
    let f = at_level(0, 2, &[s.hash()], 1);
    let g = at_level(1, 1, &[s.hash()], 1);
    let h = at_level(2, 1, &[s.hash()], 0);
    let all = vec![s.clone(), f.clone(), g.clone(), h];
    valid_history(&all);
    let f_members = vec![s.clone(), f];
    let g_members = vec![s, g];
    let f_fragment = complete_fragment(&f_members);
    let g_fragment = complete_fragment(&g_members);
    let candidates = [
        CandidateHistory::CanonicalFragment {
            fragment: &f_fragment,
            members: f_members.iter().collect(),
        },
        CandidateHistory::CanonicalFragment {
            fragment: &g_fragment,
            members: g_members.iter().collect(),
        },
        CandidateHistory::Loose(&all[3]),
    ];

    // G fails, but F still supplies s. H must remain selected.
    let (store, signed) = evidence(&all, &[1, 3]);
    partitions_for_orders(&[], &candidates, &store, &signed, &[0, 2]);

    // Both suppliers fail. Even direct sig(s) cannot replace s's lost history.
    let (store, signed) = evidence(&all, &[0, 3]);
    partitions_for_orders(&[], &candidates, &store, &signed, &[]);
}

#[test]
fn accepted_change_survives_removal_of_its_only_pending_supplier() {
    let s = at_level(0, 1, &[], 0);
    let f = at_level(0, 2, &[s.hash()], 1);
    let h = at_level(1, 1, &[s.hash()], 0);
    let all = vec![s, f, h];
    valid_history(&all);
    let fragment = complete_fragment(&all[..2]);
    let candidates = [
        CandidateHistory::CanonicalFragment {
            fragment: &fragment,
            members: all[..2].iter().collect(),
        },
        CandidateHistory::Loose(&all[2]),
    ];
    let (store, signed) = evidence(&all, &[0, 2]);
    // F is unsigned, but removing it must not withdraw already-accepted s.
    partitions_for_orders(&all[..1], &candidates, &store, &signed, &[1]);
    // Control: without accepted history the only supplier of s is removed.
    partitions_for_orders(&[], &candidates, &store, &signed, &[]);
}

#[test]
fn multiple_missing_dependencies_exclude_each_candidate_occurrence_once() {
    // G={s,t,g} is blocked on actor B's t. Its removal makes both s and t
    // unavailable, notifying each duplicate occurrence of H twice.
    let s = at_level(0, 1, &[], 0);
    let t = at_level(1, 1, &[s.hash()], 0);
    let g = at_level(0, 2, &[s.hash(), t.hash()], 1);
    let h = at_level(2, 1, &[s.hash(), t.hash()], 0);
    let all = vec![s, t, g, h];
    valid_history(&all);
    let fragment = complete_fragment(&all[..3]);
    let candidates = [
        CandidateHistory::CanonicalFragment {
            fragment: &fragment,
            members: all[..3].iter().collect(),
        },
        CandidateHistory::Loose(&all[3]),
        CandidateHistory::Loose(&all[3]),
    ];
    let (store, signed) = evidence(&all, &[2, 3]);
    partitions_for_orders(&[], &candidates, &store, &signed, &[]);
    // Control: with t signed, every input occurrence survives, including both Hs.
    let (store, signed) = evidence(&all, &[1, 2, 3]);
    partitions_for_orders(&[], &candidates, &store, &signed, &[0, 1, 2]);
}
