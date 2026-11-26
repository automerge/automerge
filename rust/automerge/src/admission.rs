//! Select the largest causally closed subset of already-proved fragments.
//!
//! Canonical fragment membership, actor chains, and cryptographic proofs are
//! established before calling this selector. Because each fragment's proof is
//! independent of other fragments, excluding an unresolvable fragment can only
//! withdraw causal support, never invalidate another fragment's proof. Selection
//! therefore converges in a single exclusion worklist pass.

use crate::{ChangeHash, Fragment};

mod projection;
mod selection;

pub(crate) use projection::{select as select_mask, FragmentView};

/// Membership in dependency-closed accepted history.
pub(crate) trait AcceptedHistory {
    fn contains(&self, hash: &ChangeHash) -> bool;
}

/// Partition of input fragment occurrences into causally admissible and waiting sets.
#[derive(Debug)]
#[must_use]
pub(crate) struct Selection<'a> {
    selected: Vec<(usize, &'a Fragment)>,
    #[cfg(test)]
    pending: Vec<(usize, &'a Fragment)>,
}

impl<'a> Selection<'a> {
    pub(crate) fn selected(&self) -> impl ExactSizeIterator<Item = &'a Fragment> + '_ {
        self.selected.iter().map(|(_, fragment)| *fragment)
    }

    pub(crate) fn selected_indices(&self) -> impl ExactSizeIterator<Item = usize> + '_ {
        self.selected.iter().map(|(index, _)| *index)
    }

    #[cfg(test)]
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
    let excluded = exclude_by_view(accepted, fragments, |fragment| fragment);
    let mut selected = Vec::new();
    #[cfg(test)]
    let mut pending = Vec::new();
    for (index, fragment) in fragments.iter().enumerate() {
        if excluded[index] {
            #[cfg(test)]
            pending.push((index, fragment));
        } else {
            selected.push((index, fragment));
        }
    }
    Selection {
        selected,
        #[cfg(test)]
        pending,
    }
}

/// Select directly on the supplied occurrence order; return its exclusion mask.
/// Callers with attached proofs need not copy descriptors into a second list.
pub(crate) fn exclude_by_view<'a, C: 'a>(
    accepted: &impl AcceptedHistory,
    occurrences: &'a [C],
    fragment_of: impl Fn(&'a C) -> &'a Fragment,
) -> Vec<bool> {
    select_mask(
        occurrences,
        |occurrence| {
            let fragment = fragment_of(occurrence);
            FragmentView {
                members: &fragment.members,
                boundary: &fragment.boundary,
            }
        },
        |hash| accepted.contains(&hash),
    )
}

#[cfg(test)]
mod tests;
