//! Known history grouping retained independently of staged change contents.

use std::collections::HashSet;

use crate::{change_graph::ChangeGraph, ChangeHash, Fragment};

/// Known grouping supplied by an internal history producer.
///
/// A group owns hashes and metadata, not change contents. Staged contents without
/// known grouping remain unclassified rather than implicitly loose. A fragment
/// keeps its full membership even when some members are already applied.
#[derive(Debug, Clone)]
pub(crate) enum StagedGroup {
    /// An explicitly identified loose change, not simply an individual delivery.
    Loose(ChangeHash),
    /// A fragment descriptor supplied under the producer's canonicality contract.
    Fragment(Fragment),
}

impl StagedGroup {
    /// Construct a [`StagedGroup`] from a [`Fragment`].
    ///
    /// A loose change is constructed if it is a 0-level [`Fragment`].
    pub(super) fn from_fragment(fragment: Fragment) -> Self {
        assert_fragment_invariants(&fragment);
        if fragment.level == 0 {
            assert_loose_change_invariants(&fragment);
            Self::Loose(fragment.members[0])
        } else {
            Self::Fragment(fragment)
        }
    }

    /// Borrow the group's head hash.
    ///
    /// For a loose change, this is the change's own hash.
    pub(super) fn head(&self) -> &ChangeHash {
        match self {
            StagedGroup::Loose(hash) => hash,
            StagedGroup::Fragment(fragment) => &fragment.head,
        }
    }

    /// Borrow the hashes of all members in this group.
    ///
    /// A loose change has one member: itself. A fragment retains its full
    /// membership, including any members already applied to the document.
    pub(super) fn members(&self) -> &[ChangeHash] {
        match self {
            StagedGroup::Loose(hash) => std::slice::from_ref(hash),
            StagedGroup::Fragment(fragment) => &fragment.members,
        }
    }

    /// Whether both groups describe the same history, ignoring hash-list order.
    ///
    /// Compares fragment metadata as well as membership. This does not validate
    /// canonicality or change either descriptor's ordering.
    pub(super) fn same_descriptor(&self, other: &Self) -> bool {
        match (self, other) {
            (StagedGroup::Loose(a), StagedGroup::Loose(b)) => a == b,
            (StagedGroup::Fragment(a), StagedGroup::Fragment(b)) => {
                a.head == b.head
                    && a.level == b.level
                    && same_hashes(&a.members, &b.members)
                    && same_hashes(&a.boundary, &b.boundary)
                    && same_hashes(&a.checkpoints, &b.checkpoints)
            }
            (StagedGroup::Loose(_), StagedGroup::Fragment(_))
            | (StagedGroup::Fragment(_), StagedGroup::Loose(_)) => false,
        }
    }

    /// Returns `true` if the [`StagedGroup::members`] are all in the given
    /// [`ChangeGraph`].
    pub(super) fn is_fully_applied(&self, graph: &ChangeGraph) -> bool {
        self.members().iter().all(|hash| graph.has_change(hash))
    }
}

fn same_hashes(left: &[ChangeHash], right: &[ChangeHash]) -> bool {
    left.iter().collect::<HashSet<_>>() == right.iter().collect::<HashSet<_>>()
}

fn assert_fragment_invariants(fragment: &Fragment) {
    let head = &fragment.head;
    assert_eq!(
        fragment.level,
        fragment.head.fragment_level(),
        "fragment {} has an inconsistent level",
        head
    );
    assert!(
        fragment.members.contains(&fragment.head),
        "fragment {} must contain its head",
        head
    );
    assert_unique(head, &fragment.members);
    assert_unique(head, &fragment.boundary);
    assert_unique(head, &fragment.checkpoints);
}

fn assert_unique(head: &ChangeHash, hashes: &[ChangeHash]) {
    let mut seen = HashSet::with_capacity(hashes.len());
    assert!(
        hashes.iter().all(|hash| seen.insert(hash)),
        "fragment {} must have unique members, checkpoints, and boundaries",
        head,
    );
}

fn assert_loose_change_invariants(fragment: &Fragment) {
    assert_eq!(
        fragment.members.as_slice(),
        &[fragment.head],
        "loose fragment {} must contain only its head",
        fragment.head
    );
    assert!(
        fragment.checkpoints.is_empty(),
        "loose fragment {} must have no checkpoints",
        fragment.head
    );
}
