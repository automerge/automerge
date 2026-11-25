//! Select the largest causally closed subset of already-proved fragments.
//!
//! Canonical fragment membership, actor chains, and cryptographic proofs are
//! established before calling this selector. Because each fragment's proof is
//! independent of other fragments, excluding an unresolvable fragment can only
//! withdraw causal support, never invalidate another fragment's proof. Selection
//! therefore converges in a single exclusion worklist pass.

use std::collections::{HashMap, HashSet};

use crate::{ChangeHash, Fragment};

mod selection;

/// Membership in dependency-closed accepted history.
pub(crate) trait AcceptedHistory {
    fn contains(&self, hash: &ChangeHash) -> bool;
}

/// Partition of input fragment occurrences into causally admissible and waiting sets.
#[derive(Debug)]
#[must_use]
pub(crate) struct Selection<'a> {
    selected: Vec<(usize, &'a Fragment)>,
    pending: Vec<(usize, &'a Fragment)>,
}

impl<'a> Selection<'a> {
    pub(crate) fn selected(&self) -> impl ExactSizeIterator<Item = &'a Fragment> + '_ {
        self.selected.iter().map(|(_, fragment)| *fragment)
    }

    pub(crate) fn selected_indices(&self) -> impl ExactSizeIterator<Item = usize> + '_ {
        self.selected.iter().map(|(index, _)| *index)
    }

    pub(crate) fn pending_indices(&self) -> impl ExactSizeIterator<Item = usize> + '_ {
        self.pending.iter().map(|(index, _)| *index)
    }
}

/// Select every proved fragment whose causal boundary is satisfied by `accepted`
/// and/or other selected fragments.
pub(crate) fn select_fragments<'a>(
    accepted: &impl AcceptedHistory,
    fragments: &'a [Fragment],
) -> Selection<'a> {
    // Index only hashes mentioned by the candidates, never all of accepted
    // history. The count includes each supplying member occurrence.
    let mut hash_ids = HashMap::new();
    let mut counts = Vec::new();
    let member_ids: Vec<Vec<_>> = fragments
        .iter()
        .map(|fragment| {
            fragment
                .members
                .iter()
                .map(|&hash| {
                    let id = *hash_ids.entry(hash).or_insert_with(|| {
                        counts.push(0);
                        counts.len() - 1
                    });
                    counts[id] += 1;
                    id
                })
                .collect()
        })
        .collect();

    let mut required_by = vec![Vec::new(); counts.len()];
    for (index, fragment) in fragments.iter().enumerate() {
        let members: HashSet<_> = fragment.members.iter().copied().collect();
        let dependencies: HashSet<_> = fragment
            .boundary
            .iter()
            .copied()
            .filter(|hash| !members.contains(hash) && !accepted.contains(hash))
            .collect();
        for dependency in dependencies {
            let id = *hash_ids.entry(dependency).or_insert_with(|| {
                required_by.push(Vec::new());
                counts.push(0);
                counts.len() - 1
            });
            required_by[id].push(index);
        }
    }
    drop(hash_ids);
    let excluded = selection::exclude(&member_ids, &required_by, counts);

    let mut selected = Vec::new();
    let mut pending = Vec::new();
    for (index, fragment) in fragments.iter().enumerate() {
        if excluded[index] {
            pending.push((index, fragment));
        } else {
            selected.push((index, fragment));
        }
    }
    Selection { selected, pending }
}

#[cfg(test)]
mod tests;
