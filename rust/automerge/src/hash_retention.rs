use crate::automerge::{AuditMode, Automerge};
use crate::change_graph::{ChangeGraph, Members, NodeIdx, UncheckedHashes};
use crate::storage::bundle::Bundle;
use crate::types::ChangeHash;
use crate::AutomergeError;

mod sealed {
    pub trait Sealed {}
    impl Sealed for super::Full {}
    impl Sealed for super::Retained {}
}

pub trait HashRetention:
    sealed::Sealed + std::fmt::Debug + Clone + Default + Send + Sync + 'static
{
    #[doc(hidden)]
    const AUDIT: AuditMode;

    /// Every node, hashed or not.
    #[doc(hidden)]
    fn len(&self) -> usize;

    #[doc(hidden)]
    fn get(&self, node: NodeIdx) -> Option<ChangeHash>;

    #[doc(hidden)]
    fn try_get(&self, node: NodeIdx) -> Result<ChangeHash, UncheckedHashes> {
        self.get(node).ok_or(UncheckedHashes)
    }

    #[doc(hidden)]
    fn iter(&self) -> impl Iterator<Item = (NodeIdx, ChangeHash)> + '_;

    #[doc(hidden)]
    fn push(&mut self, hash: ChangeHash);

    /// Appends `n` nodes, with their hashes when given.
    #[doc(hidden)]
    fn extend(&mut self, n: usize, hashes: Option<&[ChangeHash]>);

    #[doc(hidden)]
    fn has_every_hash(&self) -> bool;

    #[doc(hidden)]
    fn all(&self) -> Option<&[ChangeHash]>;

    /// In node order.
    #[doc(hidden)]
    fn leveled_members(&self, members: &Members<'_>) -> Vec<(NodeIdx, ChangeHash)>;

    /// Whether the store kept the hash.
    #[doc(hidden)]
    fn learn(&mut self, node: NodeIdx, hash: ChangeHash) -> bool;

    #[doc(hidden)]
    fn collect_garbage(graph: &mut ChangeGraph<Self>);

    #[doc(hidden)]
    fn from_full(doc: Automerge<Full>) -> Automerge<Self>;

    #[doc(hidden)]
    fn apply_bundle(doc: &mut Automerge<Self>, bundle: Bundle) -> Result<(), AutomergeError>;
}

/// Every change hash is kept: audit mode.
#[derive(Debug, Clone, Default)]
pub struct Full(Vec<ChangeHash>);

impl Full {
    pub(crate) fn new(hashes: Vec<ChangeHash>) -> Self {
        Full(hashes)
    }

    pub(crate) fn hash(&self, node: NodeIdx) -> ChangeHash {
        self.0[node.0 as usize]
    }
}

impl HashRetention for Full {
    const AUDIT: AuditMode = AuditMode::Enabled;

    fn len(&self) -> usize {
        self.0.len()
    }

    fn get(&self, node: NodeIdx) -> Option<ChangeHash> {
        self.0.get(node.0 as usize).copied()
    }

    fn iter(&self) -> impl Iterator<Item = (NodeIdx, ChangeHash)> + '_ {
        self.0
            .iter()
            .enumerate()
            .map(|(i, h)| (NodeIdx(i as u32), *h))
    }

    fn push(&mut self, hash: ChangeHash) {
        self.0.push(hash);
    }

    fn extend(&mut self, n: usize, hashes: Option<&[ChangeHash]>) {
        let hashes = hashes.expect("audit mode is given every hash");
        assert_eq!(hashes.len(), n);
        self.0.extend_from_slice(hashes);
    }

    fn has_every_hash(&self) -> bool {
        true
    }

    fn all(&self) -> Option<&[ChangeHash]> {
        Some(&self.0)
    }

    fn learn(&mut self, _node: NodeIdx, _hash: ChangeHash) -> bool {
        false
    }

    fn collect_garbage(_graph: &mut ChangeGraph<Self>) {}

    fn from_full(doc: Automerge<Full>) -> Automerge<Self> {
        doc
    }

    fn apply_bundle(doc: &mut Automerge<Self>, bundle: Bundle) -> Result<(), AutomergeError> {
        doc.apply_bundle_verified(bundle)
    }

    fn leveled_members(&self, members: &Members<'_>) -> Vec<(NodeIdx, ChangeHash)> {
        members
            .nodes()
            .iter()
            .filter_map(|n| Some((*n, self.get(*n)?)))
            .filter(|(_, h)| h.fragment_level() > 0)
            .collect()
    }
}

/// Only the retained set of hashes is kept.
#[derive(Debug, Clone, Default)]
pub struct Retained {
    map: crate::types::SmallHashMap<NodeIdx, ChangeHash>,
    len: usize,
}

impl Retained {
    pub(crate) fn new(map: crate::types::SmallHashMap<NodeIdx, ChangeHash>, len: usize) -> Self {
        Retained { map, len }
    }

    pub(crate) fn forget(&mut self, node: NodeIdx) {
        self.map.remove(&node);
    }
}

impl HashRetention for Retained {
    const AUDIT: AuditMode = AuditMode::Disabled;

    fn len(&self) -> usize {
        self.len
    }

    fn get(&self, node: NodeIdx) -> Option<ChangeHash> {
        self.map.get(&node).copied()
    }

    fn iter(&self) -> impl Iterator<Item = (NodeIdx, ChangeHash)> + '_ {
        self.map.iter().map(|(n, h)| (*n, *h))
    }

    fn push(&mut self, hash: ChangeHash) {
        self.map.insert(NodeIdx(self.len as u32), hash);
        self.len += 1;
    }

    fn extend(&mut self, n: usize, hashes: Option<&[ChangeHash]>) {
        for (i, hash) in hashes.into_iter().flatten().enumerate() {
            self.map.insert(NodeIdx((self.len + i) as u32), *hash);
        }
        self.len += n;
    }

    fn has_every_hash(&self) -> bool {
        self.map.len() == self.len
    }

    fn all(&self) -> Option<&[ChangeHash]> {
        None
    }

    fn learn(&mut self, node: NodeIdx, hash: ChangeHash) -> bool {
        self.map.insert(node, hash);
        true
    }

    fn collect_garbage(graph: &mut ChangeGraph<Self>) {
        graph.retain_hashes_only();
    }

    fn from_full(doc: Automerge<Full>) -> Automerge<Self> {
        doc.disable_audit_mode()
    }

    fn apply_bundle(doc: &mut Automerge<Self>, bundle: Bundle) -> Result<(), AutomergeError> {
        doc.apply_bundle_columns(bundle, None)
    }

    fn leveled_members(&self, members: &Members<'_>) -> Vec<(NodeIdx, ChangeHash)> {
        // walk the smaller side: a small export of a document that hasn't
        // been GC'd holds far more hashes than members
        if members.nodes().len() <= self.map.len() {
            return members
                .nodes()
                .iter()
                .filter_map(|n| Some((*n, *self.map.get(n)?)))
                .filter(|(_, h)| h.fragment_level() > 0)
                .collect();
        }
        let mut found: Vec<_> = self
            .map
            .iter()
            .filter(|(n, h)| h.fragment_level() > 0 && members.position(**n).is_some())
            .map(|(n, h)| (*n, *h))
            .collect();
        found.sort_unstable_by_key(|(n, _)| *n);
        found
    }
}
