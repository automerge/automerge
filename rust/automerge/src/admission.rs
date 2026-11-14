//! Select the largest causally closed subset of already-proved fragments.
//!
//! Canonical fragment membership, actor chains, and cryptographic proofs are
//! established before calling this selector. Because each fragment's proof is
//! independent of other fragments, excluding an unresolvable fragment can only
//! withdraw causal support, never invalidate another fragment's proof. Selection
//! therefore converges in a single exclusion worklist pass.

use std::collections::{HashMap, HashSet};

use crate::{ChangeHash, Fragment};

/// Membership in dependency-closed published history.
pub(crate) trait PublishedHistory {
    fn contains(&self, hash: &ChangeHash) -> bool;
}

/// Input fragment occurrences whose causal boundaries are satisfied.
#[derive(Debug)]
#[must_use]
pub(crate) struct Selection<'a> {
    selected: Vec<(usize, &'a Fragment)>,
}

impl<'a> Selection<'a> {
    pub(crate) fn selected(&self) -> impl ExactSizeIterator<Item = &'a Fragment> + '_ {
        self.selected.iter().map(|(_, fragment)| *fragment)
    }

    #[cfg(test)]
    pub(crate) fn selected_indices(&self) -> impl ExactSizeIterator<Item = usize> + '_ {
        self.selected.iter().map(|(index, _)| *index)
    }
}

/// Select every proved fragment whose causal boundary is satisfied by `published`
/// and/or other selected fragments.
pub(crate) fn select_fragments<'a>(
    published: &impl PublishedHistory,
    fragments: &'a [Fragment],
) -> Selection<'a> {
    let mut counts: HashMap<ChangeHash, usize> = HashMap::new();
    for fragment in fragments {
        for &hash in &fragment.members {
            *counts.entry(hash).or_default() += 1;
        }
    }

    let mut dependents: HashMap<ChangeHash, Vec<usize>> = HashMap::new();
    let mut to_exclude = Vec::new();
    for (index, fragment) in fragments.iter().enumerate() {
        let members: HashSet<_> = fragment.members.iter().copied().collect();
        let dependencies: HashSet<_> = fragment
            .boundary
            .iter()
            .copied()
            .filter(|hash| !members.contains(hash) && !published.contains(hash))
            .collect();
        for dependency in dependencies {
            if !counts.contains_key(&dependency) {
                to_exclude.push(index);
            }
            dependents.entry(dependency).or_default().push(index);
        }
    }

    let mut excluded = vec![false; fragments.len()];
    while let Some(index) = to_exclude.pop() {
        if excluded[index] {
            continue;
        }
        excluded[index] = true;
        for &hash in &fragments[index].members {
            let count = counts.get_mut(&hash).expect("fragment member was counted");
            *count -= 1;
            if *count == 0 && !published.contains(&hash) {
                if let Some(waiting) = dependents.get(&hash) {
                    to_exclude.extend(waiting.iter().copied());
                }
            }
        }
    }

    let mut selected = Vec::new();
    for (index, fragment) in fragments.iter().enumerate() {
        if !excluded[index] {
            selected.push((index, fragment));
        }
    }
    Selection { selected }
}

#[cfg(test)]
mod tests;
