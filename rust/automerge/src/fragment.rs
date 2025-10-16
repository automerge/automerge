//! EXPERIMENTAL history fragmentation

use std::collections::{HashMap, HashSet};

use crate::{Change, ChangeHash};

/// EXPERIMENTAL: A "fragment" of Automerge history.
///
/// `Fragment`s are a consistent unit of document history,
/// which may end before the complete history is covered.
/// In this way, a document can be broken up into a series
/// of `Fragment`s that are consistent across replicas.
///
/// This is an experimental API, the fragmet API is subject to change
/// and so should not be used in production just yet.
#[derive(Debug, Clone)]
pub struct Fragment {
    head_hash: ChangeHash,
    members: HashSet<ChangeHash>,
    boundary: HashMap<ChangeHash, Change>,
}

impl Fragment {
    pub fn new(
        head_hash: ChangeHash,
        members: HashSet<ChangeHash>,
        boundary: HashMap<ChangeHash, Change>,
    ) -> Self {
        Self {
            head_hash,
            members,
            boundary,
        }
    }

    /// The "newest" element of the fragment.
    ///
    /// This hash provides a stable point from which
    /// the restof the fragment is built.
    pub fn head_hash(&self) -> ChangeHash {
        self.head_hash
    }

    /// All members of the fragment.
    ///
    /// This includes all history between the `head_hash`
    /// and the `boundary` (not including the boundary elements).
    pub fn members(&self) -> &HashSet<ChangeHash> {
        &self.members
    }

    /// The boundary from which the next set of fragments would be built.
    pub fn boundary(&self) -> &HashMap<ChangeHash, Change> {
        &self.boundary
    }
}
