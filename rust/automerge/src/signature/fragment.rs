//! Independent proof requirements for a structurally validated fragment.
//!
//! Each actor's latest included change must have its own signature. Earlier
//! included changes of that actor are attested through the validated actor chain.
//! Neither a shared author nor a signed descendant outside the fragment can
//! substitute for one of these signatures. Causal admission is a separate step.

use std::collections::BTreeMap;

use crate::ChangeHash;

/// Fixed proof obligations, independent of available signatures and other groups.
///
/// Construction assumes canonical membership, valid actor chains, and established
/// actor–author bindings. This type does not validate fragment structure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FragmentRequirements {
    hashes: Vec<ChangeHash>,
}

impl FragmentRequirements {
    /// Derive the maximum included sequence for each actor, in any member order.
    ///
    /// Actor keys may be IDs for incoming changes or table indices for an applied
    /// graph. They must identify actors, not authors. Boundaries and their ancestry
    /// must already have been excluded from the supplied membership.
    pub(crate) fn for_members<A: Ord>(
        members: impl IntoIterator<Item = (A, u64, ChangeHash)>,
    ) -> Self {
        let mut frontiers = BTreeMap::<A, (u64, ChangeHash)>::new();
        for (actor, seq, hash) in members {
            let frontier = frontiers.entry(actor).or_insert((seq, hash));
            if seq > frontier.0 {
                *frontier = (seq, hash);
            }
        }
        let mut hashes: Vec<_> = frontiers.into_values().map(|(_, hash)| hash).collect();
        hashes.sort_unstable();
        Self { hashes }
    }

    /// Exact hashes whose signatures must accompany an outgoing fragment.
    pub(crate) fn hashes(&self) -> impl ExactSizeIterator<Item = &ChangeHash> {
        self.hashes.iter()
    }

    /// Query retained evidence without changing these complete requirements.
    pub(crate) fn is_satisfied_by(&self, has_signature: impl FnMut(&ChangeHash) -> bool) -> bool {
        self.hashes().all(has_signature)
    }
}

#[cfg(test)]
mod tests;
