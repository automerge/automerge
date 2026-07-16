use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::num::NonZeroU32;
use std::ops::Add;
use std::ops::RangeBounds;

use crate::storage::{BundleMetadata, DepRef};
use crate::{
    clock::{Clock, SeqClock},
    error::AutomergeError,
    op_set2::{change::BuildChangeMetadata, ActorIdx, ValueMeta},
    storage::columns::compression::Uncompressed,
    storage::columns::BadColumnLayout,
    storage::document::ReconstructError as LoadError,
    storage::{Columns, Document, RawColumn, RawColumns},
    types::OpId,
    Change, ChangeHash,
};

/// The graph of changes
///
/// This is a sort of adjacency list based representation, except that instead of using linked
/// lists, we keep all the edges and nodes in two vecs and reference them by index which plays nice
/// with the cache

#[derive(Debug, Default, Clone)]
pub(crate) struct ChangeGraph {
    edges: Vec<Edge>,
    hashes: Hashes,
    actors: Vec<ActorIdx>,
    parents: Vec<Option<EdgeIdx>>,
    seq: Vec<u32>,
    max_ops: Vec<u32>,
    max_op: u32,
    num_ops: hexane::Column<u64>,
    timestamps: hexane::DeltaColumn<i64>,
    messages: hexane::Column<Option<String>>,
    extra_bytes_meta: hexane::PrefixColumn<ValueMeta>,
    extra_bytes_raw: Vec<u8>,
    heads: BTreeSet<ChangeHash>,
    nodes_by_hash: HashMap<ChangeHash, NodeIdx>,
    clock_cache: HashMap<NodeIdx, SeqClock>,
    seq_index: Vec<Vec<NodeIdx>>,
    fragment_top: SeqClock,
    fragments: Vec<FragmentNode>,
}

pub(crate) struct ChangeGraphCols {
    graph: ChangeGraph,
    /// `(node index, hash)` pairs from the document's hash columns:
    /// fragment-level (> 0) hashes plus loose commits and anchors,
    /// excluding the heads (those live in the head-index suffix).
    saved_hashes: Vec<(u32, ChangeHash)>,
}

impl ChangeGraphCols {
    /// Whether the document carried hash columns (fragment hashes)
    pub(crate) fn has_saved_hashes(&self) -> bool {
        !self.saved_hashes.is_empty()
    }
}

const CACHE_STEP: u32 = 16;

/// The hashes of the changes in a [`ChangeGraph`], which may be incomplete.
///
/// Computing change hashes requires reconstructing and hashing every change,
/// which a load is allowed to skip. In that case only the hashes learned at
/// load time (the document's heads) and the hashes of changes added since are
/// known.
#[derive(Debug, Clone)]
pub(crate) enum Hashes {
    /// Every node's hash is known and validated.
    Checked(Vec<ChangeHash>),
    /// Only hashes learned at or after load are known.
    Unchecked {
        /// The number of nodes in the graph at load time. Nodes at or beyond
        /// this index were added after load and always have known hashes.
        watermark: u32,
        /// `tail[i]` is the hash of node `watermark + i`
        tail: Vec<ChangeHash>,
        /// Pre-load nodes with known hashes: the load-time heads, paired
        /// with their nodes via the document's head index suffix, plus —
        /// when `fragment_hashes` is set — the hashes imported from the
        /// document's hash columns. The pairing is as claimed by the
        /// (unverified) document; `rebuild_hash_graph` confirms it.
        pre: HashMap<NodeIdx, ChangeHash>,
        /// `pre` additionally contains the hash of every history node
        /// with `fragment_level() > 0` plus every loose commit and
        /// anchor (imported from the document's hash columns), which is
        /// enough to build fragments — the "fragment hashes" state.
        fragment_hashes: bool,
    },
}

impl Default for Hashes {
    fn default() -> Self {
        Hashes::Checked(Vec::new())
    }
}

impl Hashes {
    fn len(&self) -> usize {
        match self {
            Self::Checked(v) => v.len(),
            Self::Unchecked {
                watermark, tail, ..
            } => *watermark as usize + tail.len(),
        }
    }

    fn is_checked(&self) -> bool {
        matches!(self, Self::Checked(_))
    }

    /// Whether every hash a fragment needs (fragment heads, checkpoints,
    /// boundaries, loose commits) is known.
    fn has_fragment_hashes(&self) -> bool {
        match self {
            Self::Checked(_) => true,
            Self::Unchecked {
                fragment_hashes, ..
            } => *fragment_hashes,
        }
    }

    fn state(&self) -> crate::HashGraphState {
        match self {
            Self::Checked(_) => crate::HashGraphState::Checked,
            Self::Unchecked {
                fragment_hashes: true,
                ..
            } => crate::HashGraphState::FragmentHashes,
            Self::Unchecked { .. } => crate::HashGraphState::Unchecked,
        }
    }

    fn get(&self, idx: NodeIdx) -> Option<ChangeHash> {
        match self {
            Self::Checked(v) => v.get(idx.0 as usize).copied(),
            Self::Unchecked {
                watermark,
                tail,
                pre,
                ..
            } => {
                if idx.0 >= *watermark {
                    tail.get((idx.0 - watermark) as usize).copied()
                } else {
                    pre.get(&idx).copied()
                }
            }
        }
    }

    fn try_get(&self, idx: NodeIdx) -> Result<ChangeHash, UncheckedHashes> {
        self.get(idx).ok_or(UncheckedHashes)
    }

    fn push(&mut self, hash: ChangeHash) {
        match self {
            Self::Checked(v) => v.push(hash),
            Self::Unchecked { tail, .. } => tail.push(hash),
        }
    }

    /// Record that `n` nodes with unknown hashes are being appended.
    ///
    /// A checked graph downgrades to the fragment-hashes state — every
    /// hash known so far moves to `pre`, so anything a fragment needs
    /// is still available. An unchecked graph folds its (always-known)
    /// tail into `pre`. Either way the watermark moves past the new
    /// nodes, preserving the invariant that nodes at or beyond it have
    /// known hashes.
    fn extend_unknown(&mut self, n: usize) {
        if n == 0 {
            return;
        }
        let new_watermark = (self.len() + n) as u32;
        match self {
            Self::Checked(v) => {
                let pre = v
                    .iter()
                    .enumerate()
                    .map(|(i, h)| (NodeIdx(i as u32), *h))
                    .collect();
                *self = Self::Unchecked {
                    watermark: new_watermark,
                    tail: Vec::new(),
                    pre,
                    fragment_hashes: true,
                };
            }
            Self::Unchecked {
                watermark,
                tail,
                pre,
                ..
            } => {
                for (i, h) in tail.drain(..).enumerate() {
                    pre.insert(NodeIdx(*watermark + i as u32), h);
                }
                *watermark = new_watermark;
            }
        }
    }
}

/// The result of looking a hash up in a [`ChangeGraph`]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HashLookup {
    /// The hash names this node
    Found(NodeIdx),
    /// The hash definitely does not name a change in this document
    Absent,
    /// The hash graph is unchecked and we cannot tell whether this hash
    /// names a change in this document
    Unknown,
}

/// Hashes resolved to node indexes
struct ResolvedHashes {
    nodes: Vec<NodeIdx>,
    /// Hashes which definitely do not name changes in this document
    missing: Vec<ChangeHash>,
}

/// The hash graph is unchecked and the requested operation needs hashes we
/// do not have
#[derive(Debug, thiserror::Error)]
#[error("the hash graph has not been built, call rebuild_hash_graph() first")]
pub(crate) struct UncheckedHashes;

/// The document's stored hash columns are malformed or disagree with the
/// recomputed change hashes
#[derive(Debug, thiserror::Error)]
#[error("the document's change-hash columns are invalid")]
pub(crate) struct InvalidHashColumn;

/// The document's head index suffix does not describe the change graph's
/// childless nodes
#[derive(Debug, thiserror::Error)]
#[error("the document's head indexes are invalid")]
pub(crate) struct BadHeadIndexes;

impl From<UncheckedHashes> for AutomergeError {
    fn from(_: UncheckedHashes) -> Self {
        AutomergeError::UncheckedHashGraph
    }
}

#[derive(Hash, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct NodeIdx(u32);

impl Add<usize> for NodeIdx {
    type Output = Self;

    fn add(self, other: usize) -> Self {
        NodeIdx(self.0 + other as u32)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct EdgeIdx(NonZeroU32);

impl EdgeIdx {
    fn new(value: usize) -> Self {
        EdgeIdx(NonZeroU32::new(value as u32 + 1).unwrap())
    }
    fn get(&self) -> usize {
        self.0.get() as usize - 1
    }
}

#[derive(PartialEq, Debug, Clone)]
struct Edge {
    // Edges are always child -> parent so we only store the target, the child is implicit
    // as you get the edge from the child
    target: NodeIdx,
    next: Option<EdgeIdx>,
}

/// A member change of a bundle being applied without conversion into
/// [`Change`]s — everything the graph needs except the change's hash.
#[derive(Debug, Clone)]
pub(crate) struct FragmentMember<'a> {
    /// The member's actor as a document actor index
    pub(crate) actor: usize,
    pub(crate) seq: u64,
    pub(crate) max_op: u64,
    pub(crate) num_ops: u64,
    pub(crate) timestamp: i64,
    pub(crate) message: Option<String>,
    pub(crate) extra: Cow<'a, [u8]>,
    pub(crate) deps: Vec<FragmentDep>,
}

/// A [`FragmentMember`]'s dependency: another member of the same bundle
/// (by its position in the member list, which is topological order) or
/// a node already in the graph.
#[derive(Debug, Clone, Copy)]
pub(crate) enum FragmentDep {
    Member(usize),
    Node(NodeIdx),
}

impl ChangeGraph {
    pub(crate) fn new(num_actors: usize) -> Self {
        Self {
            edges: Vec::new(),
            nodes_by_hash: HashMap::new(),
            hashes: Hashes::default(),
            actors: Vec::new(),
            max_ops: Vec::new(),
            max_op: 0,
            num_ops: hexane::Column::new(),
            seq: Vec::new(),
            parents: Vec::new(),
            messages: hexane::Column::new(),
            timestamps: hexane::DeltaColumn::new(),
            extra_bytes_meta: hexane::PrefixColumn::new(),
            extra_bytes_raw: Vec::new(),
            heads: BTreeSet::new(),
            clock_cache: HashMap::new(),
            seq_index: vec![vec![]; num_actors],
            fragments: vec![],
            fragment_top: SeqClock::new(num_actors),
        }
    }

    pub(crate) fn all_actor_ids(&self) -> impl Iterator<Item = usize> + '_ {
        self.seq_index.iter().enumerate().map(|(i, _)| i)
    }

    pub(crate) fn actor_ids(&self) -> impl Iterator<Item = usize> + '_ {
        self.seq_index
            .iter()
            .enumerate()
            .filter_map(|(i, v)| if !v.is_empty() { Some(i) } else { None })
    }

    pub(crate) fn unused_actors(&self) -> impl Iterator<Item = usize> + '_ {
        self.seq_index
            .iter()
            .enumerate()
            .filter_map(|(i, v)| if v.is_empty() { Some(i) } else { None })
    }

    pub(crate) fn heads(&self) -> impl Iterator<Item = ChangeHash> + '_ {
        self.heads.iter().cloned()
    }

    /// Whether `heads` is exactly the set of current heads (order and
    /// duplicates ignored).
    pub(crate) fn heads_are_current(&self, heads: &[ChangeHash]) -> bool {
        // duplicates can only shrink the set, so fewer entries than heads
        // can never match
        if heads.len() < self.heads.len() {
            return false;
        }
        heads.iter().copied().collect::<BTreeSet<_>>() == self.heads
    }

    /// The node index of each head, in the same order as [`Self::heads`].
    ///
    /// The document format writes heads and head indices as positionally
    /// corresponding lists, so order matters here.
    pub(crate) fn head_indexes(&self) -> impl Iterator<Item = u64> + '_ {
        self.heads.iter().map(|h| {
            self.nodes_by_hash
                .get(h)
                .expect("every head has a known node")
                .0 as u64
        })
    }

    pub(crate) fn num_actors(&self) -> usize {
        self.seq_index.len()
    }

    pub(crate) fn insert_actor(&mut self, idx: usize) {
        if self.seq_index.len() != idx {
            for actor_index in &mut self.actors {
                if actor_index.0 >= idx as u32 {
                    actor_index.0 += 1;
                }
            }
        }
        for clock in self.clock_cache.values_mut() {
            clock.rewrite_with_new_actor(idx)
        }
        for f in &mut self.fragments {
            f.clock.rewrite_with_new_actor(idx)
        }
        self.fragment_top.rewrite_with_new_actor(idx);
        self.seq_index.insert(idx, vec![]);
    }

    pub(crate) fn remove_actor(&mut self, idx: usize) {
        for actor_index in &mut self.actors {
            if actor_index.0 > idx as u32 {
                actor_index.0 -= 1;
            }
        }
        if self.seq_index.get(idx).is_some() {
            assert!(self.seq_index[idx].is_empty());
            self.seq_index.remove(idx);
        }
        for clock in &mut self.clock_cache.values_mut() {
            clock.remove_actor(idx)
        }
        for fragment in &mut self.fragments {
            fragment.clock.remove_actor(idx)
        }
        self.fragment_top.remove_actor(idx);
    }

    pub(crate) fn len(&self) -> usize {
        self.actors.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.actors.is_empty()
    }

    #[cfg(test)]
    fn hash_to_index(&self, hash: &ChangeHash) -> Option<usize> {
        self.nodes_by_hash.get(hash).map(|n| n.0 as usize)
    }

    pub(crate) fn index_to_hash(&self, index: usize) -> Option<ChangeHash> {
        self.hashes.get(NodeIdx(index as u32))
    }

    pub(crate) fn try_index_to_hash(&self, index: usize) -> Result<ChangeHash, UncheckedHashes> {
        self.hashes.try_get(NodeIdx(index as u32))
    }

    pub(crate) fn is_checked(&self) -> bool {
        self.hashes.is_checked()
    }

    pub(crate) fn state(&self) -> crate::HashGraphState {
        self.hashes.state()
    }

    pub(crate) fn max_op(&self) -> u64 {
        self.max_op as u64
    }

    pub(crate) fn max_op_for_actor(&self, actor_index: usize) -> u64 {
        self.seq_index
            .get(actor_index)
            .and_then(|s| s.last())
            .and_then(|index| self.max_ops.get(index.0 as usize).cloned())
            .unwrap_or(0) as u64
    }

    pub(crate) fn seq_for_actor(&self, actor: usize) -> u64 {
        self.seq_index
            .get(actor)
            .map(|v| v.len() as u64)
            .unwrap_or(0)
    }

    /// The clock covering the whole document: every actor's current op
    /// counter.
    pub(crate) fn current_clock(&self) -> Clock {
        Clock(
            (0..self.seq_index.len())
                .map(|a| self.max_op_for_actor(a) as u32)
                .collect(),
        )
    }

    /// The seq clock covering the whole document: every actor's current
    /// seq.
    pub(crate) fn current_seq_clock(&self) -> SeqClock {
        let mut clock = SeqClock::new(self.num_actors());
        for (a, seqs) in self.seq_index.iter().enumerate() {
            clock.include(a, u32::try_from(seqs.len()).ok().filter(|n| *n > 0));
        }
        clock
    }

    fn deps_iter(&self) -> impl Iterator<Item = NodeIdx> + '_ {
        self.node_ids().flat_map(|n| self.parents(n))
    }

    fn num_deps(&self) -> impl Iterator<Item = usize> + '_ {
        self.node_ids().map(|n| self.parents(n).count())
    }

    fn node_ids(&self) -> impl Iterator<Item = NodeIdx> {
        let end = self.len() as u32;
        (0..end).map(NodeIdx)
    }

    /// The `(node index, hash)` pairs the hash columns persist — see
    /// `encode`. Empty on a plain unchecked graph.
    fn stored_hashes(&self) -> Vec<(u32, ChangeHash)> {
        if !self.hashes.has_fragment_hashes() {
            return Vec::new();
        }
        let n = self.len();
        let covered = |i: usize| {
            let actor = usize::from(self.actors[i]);
            self.fragment_top.get_for_actor(&actor) >= NonZeroU32::new(self.seq[i])
        };
        let mut store = vec![false; n];
        for i in 0..n {
            let Some(hash) = self.hashes.get(NodeIdx(i as u32)) else {
                continue;
            };
            if hash.fragment_level() > 0 {
                store[i] = true;
            } else if !covered(i) {
                // a loose commit — plus its covered level-0 parents
                // (anchors), which its fragment boundary will need
                store[i] = true;
                for p in self.parents(NodeIdx(i as u32)) {
                    let pi = p.0 as usize;
                    if covered(pi)
                        && self
                            .hashes
                            .get(p)
                            .is_some_and(|ph| ph.fragment_level() == 0)
                    {
                        store[pi] = true;
                    }
                }
            }
        }
        (0..n)
            .filter(|i| store[*i])
            .filter_map(|i| {
                let hash = self.hashes.get(NodeIdx(i as u32))?;
                // the head-index suffix already stores the heads
                if self.heads.contains(&hash) {
                    return None;
                }
                Some((i as u32, hash))
            })
            .collect()
    }

    pub(crate) fn encode(&self, out: &mut Vec<u8>) -> RawColumns<Uncompressed> {
        use hexane::EncoderApi;
        use ids::*;

        let actor = hexane::Encoder::<ActorIdx>::encode_to(out, self.actors.iter().copied());
        let seq =
            hexane::DeltaEncoder::<usize>::encode_to(out, self.seq.iter().map(|s| *s as usize));
        let max_op =
            hexane::DeltaEncoder::<usize>::encode_to(out, self.max_ops.iter().map(|m| *m as usize));
        let time_start = out.len();
        out.extend_from_slice(&self.timestamps.save());
        let time = time_start..out.len();
        let message = self.messages.save_to_unless(out, None);

        let num_deps = hexane::Encoder::<usize>::encode_to(out, self.num_deps());
        let deps =
            hexane::DeltaEncoder::<usize>::encode_to(out, self.deps_iter().map(|n| n.0 as usize));

        // FIXME - we could eliminate this column if empty but meta isnt all null
        let meta = self.extra_bytes_meta.save_to(out);
        let raw = out.len()..out.len() + self.extra_bytes_raw.len();
        out.extend(&self.extra_bytes_raw);

        let mut cols = vec![
            RawColumn::new(ACTOR_COL_SPEC, actor),
            RawColumn::new(SEQ_COL_SPEC, seq),
            RawColumn::new(MAX_OP_COL_SPEC, max_op),
            RawColumn::new(TIME_COL_SPEC, time),
            RawColumn::new(MESSAGE_COL_SPEC, message),
            RawColumn::new(DEPS_COUNT_COL_SPEC, num_deps),
            RawColumn::new(DEPS_VAL_COL_SPEC, deps),
            RawColumn::new(EXTRA_META_COL_SPEC, meta),
            RawColumn::new(EXTRA_VAL_COL_SPEC, raw),
        ];

        // ── the hash columns ──
        // Persist every hash a future fragment-hashes load needs:
        // fragment-level (> 0) hashes, loose commits (not covered by any
        // cached fragment), and anchors (covered level-0 parents of loose
        // commits, needed for loose fragment boundaries). Heads are
        // excluded — the head-index suffix already stores them. Skipped
        // entirely on a plain unchecked graph, whose interior hashes are
        // unknown anyway.
        let stored = self.stored_hashes();
        if !stored.is_empty() {
            let index =
                hexane::DeltaEncoder::<u64>::encode_to(out, stored.iter().map(|(i, _)| *i as u64));
            // one identical meta entry per hash — a single RLE run whose
            // only job is making the raw value column structurally legal
            let hash_meta = hexane::Encoder::<ValueMeta>::encode_to(
                out,
                stored.iter().map(|(_, h)| ValueMeta::from(h.as_ref())),
            );
            let hash_raw_start = out.len();
            for (_, h) in &stored {
                out.extend_from_slice(h.as_ref());
            }
            cols.push(RawColumn::new(HASH_INDEX_COL_SPEC, index));
            cols.push(RawColumn::new(HASH_META_COL_SPEC, hash_meta));
            cols.push(RawColumn::new(HASH_VAL_COL_SPEC, hash_raw_start..out.len()));
        }

        cols.into_iter().collect()
    }

    pub(crate) fn validate(
        bytes: usize,
        cols: &RawColumns<Uncompressed>,
    ) -> Result<RawColumns<Uncompressed>, BadColumnLayout> {
        use ids::*;
        let _ = Columns::parse2(bytes, cols.iter())?;
        Ok(cols
            .iter()
            .filter(|col| {
                matches!(
                    col.spec(),
                    ACTOR_COL_SPEC
                        | SEQ_COL_SPEC
                        | MAX_OP_COL_SPEC
                        | TIME_COL_SPEC
                        | MESSAGE_COL_SPEC
                        | DEPS_COUNT_COL_SPEC
                        | DEPS_VAL_COL_SPEC
                        | EXTRA_META_COL_SPEC
                        | EXTRA_VAL_COL_SPEC
                        | HASH_INDEX_COL_SPEC
                        | HASH_META_COL_SPEC
                        | HASH_VAL_COL_SPEC
                )
            })
            .cloned()
            .collect())
    }

    fn opid_to_node(&self, id: OpId) -> Option<NodeIdx> {
        let actor_indices = self.seq_index.get(id.actor())?;
        let counter = id.counter();
        let index = actor_indices
            .binary_search_by(|n| {
                let i = n.0 as usize;
                let num_ops = self.num_ops.get(i).unwrap_or_default();
                let max_op = self.max_ops[i];
                let start = max_op as u64 - num_ops + 1;
                if counter < start {
                    Ordering::Greater
                } else if (max_op as u64) < counter {
                    Ordering::Less
                } else {
                    Ordering::Equal
                }
            })
            .ok()?;
        Some(actor_indices[index])
    }

    /// The (actor index, seq) of the change containing the given op.
    ///
    /// This never needs hashes so it works on unchecked graphs.
    pub(crate) fn opid_to_actor_seq(&self, id: OpId) -> Option<(usize, u64)> {
        let node = self.opid_to_node(id)?;
        let i = node.0 as usize;
        Some((usize::from(self.actors[i]), self.seq[i] as u64))
    }

    pub(crate) fn deps_for_hash(
        &self,
        hash: &ChangeHash,
    ) -> impl Iterator<Item = Result<ChangeHash, UncheckedHashes>> + '_ {
        let node_idx = self.nodes_by_hash.get(hash);
        let mut edge_idx = node_idx.and_then(|n| self.parents[n.0 as usize]);
        std::iter::from_fn(move || {
            let this_edge_idx = edge_idx?;
            let edge = &self.edges[this_edge_idx.get()];
            edge_idx = edge.next;
            Some(self.hashes.try_get(edge.target))
        })
    }

    fn lookup_hash(&self, hash: &ChangeHash) -> HashLookup {
        if let Some(n) = self.nodes_by_hash.get(hash) {
            return HashLookup::Found(*n);
        }
        match &self.hashes {
            Hashes::Checked(_) => HashLookup::Absent,
            Hashes::Unchecked { .. } => HashLookup::Unknown,
        }
    }

    /// Resolve a set of hashes to node indexes.
    ///
    /// Hashes which definitely don't name changes in this document are
    /// returned in `missing` (callers decide whether that's a skip or an
    /// error). If the graph is unchecked and a hash is not one of the known
    /// ones this errors.
    fn resolve_hashes<'b, I: IntoIterator<Item = &'b ChangeHash>>(
        &self,
        hashes: I,
    ) -> Result<ResolvedHashes, UncheckedHashes> {
        let mut nodes = Vec::new();
        let mut missing = Vec::new();
        for hash in hashes {
            match self.lookup_hash(hash) {
                HashLookup::Found(n) => nodes.push(n),
                HashLookup::Absent => missing.push(*hash),
                HashLookup::Unknown => return Err(UncheckedHashes),
            }
        }
        Ok(ResolvedHashes { nodes, missing })
    }

    /// Resolve the (sorted) deps of a new local change to node indexes.
    pub(crate) fn dep_indexes(
        &self,
        sorted_deps: &[ChangeHash],
    ) -> Result<Vec<u64>, UncheckedHashes> {
        sorted_deps
            .iter()
            .map(|hash| match self.lookup_hash(hash) {
                HashLookup::Found(n) => Ok(n.0 as u64),
                HashLookup::Absent | HashLookup::Unknown => Err(UncheckedHashes),
            })
            .collect()
    }

    pub(crate) fn has_change(&self, hash: &ChangeHash) -> Result<bool, UncheckedHashes> {
        match self.lookup_hash(hash) {
            HashLookup::Found(_) => Ok(true),
            HashLookup::Absent => Ok(false),
            HashLookup::Unknown => Err(UncheckedHashes),
        }
    }

    pub(crate) fn get_bundle_metadata<I>(
        &self,
        hashes: I,
    ) -> impl Iterator<Item = Result<BundleMetadata<'_>, MissingDep>>
    where
        I: IntoIterator<Item = ChangeHash>,
    {
        // resolve to nodes, then build node-based (positions are member
        // list order, which must be topological, i.e. node order)
        let mut nodes = Vec::new();
        let mut missing = None;
        for hash in hashes {
            match self.nodes_by_hash.get(&hash) {
                Some(n) => nodes.push(*n),
                None => {
                    missing = Some(MissingDep(hash));
                    break;
                }
            }
        }
        nodes.sort_unstable();
        let err = missing.into_iter().map(Err);
        let ok = if err.len() > 0 { Vec::new() } else { nodes };
        self.bundle_metadata_for_nodes(ok).chain(err)
    }

    /// Bundle metadata for a set of member nodes, deps pre-resolved to
    /// member positions or external hashes. Only the *external* (boundary)
    /// hashes need to be known, so this works on a graph in the
    /// fragment-hashes state. `nodes` must be sorted ascending.
    pub(crate) fn bundle_metadata_for_nodes(
        &self,
        nodes: Vec<NodeIdx>,
    ) -> impl Iterator<Item = Result<BundleMetadata<'_>, MissingDep>> {
        debug_assert!(nodes.is_sorted());
        let pos_of: HashMap<NodeIdx, usize> =
            nodes.iter().enumerate().map(|(p, n)| (*n, p)).collect();
        nodes.into_iter().map(move |index| {
            let i = index.0 as usize;
            let actor = self.actors[i].into();
            let timestamp = self.timestamps.get(i).unwrap_or_default();
            let max_op = self.max_ops[i] as u64;
            let num_ops = self.num_ops.get(i).unwrap_or_default();
            let message = self.messages.get(i).flatten().map(Cow::Borrowed);

            let meta = self.extra_bytes_meta.get(i).unwrap();
            let meta_range = meta.prefix() as usize..meta.total() as usize;
            let extra = Cow::Borrowed(&self.extra_bytes_raw[meta_range]);

            let deps = self
                .parents(index)
                .map(|p| match pos_of.get(&p) {
                    Some(pos) => Ok(DepRef::Internal(*pos)),
                    None => self
                        .hashes
                        .get(p)
                        .map(DepRef::External)
                        .ok_or(MissingDep(ChangeHash([0; 32]))),
                })
                .collect::<Result<Vec<_>, _>>()?;

            let start_op = max_op - num_ops + 1;
            let seq = self.seq[i] as u64;
            Ok(BundleMetadata {
                actor,
                seq,
                start_op,
                max_op,
                timestamp,
                message,
                extra,
                deps,
                builder: i,
            })
        })
    }

    pub(crate) fn get_build_metadata<I>(
        &self,
        hashes: I,
    ) -> Result<Vec<BuildChangeMetadata<'_>>, crate::AutomergeError>
    where
        I: IntoIterator<Item = ChangeHash>,
    {
        let indexes: Vec<_> = hashes
            .into_iter()
            .map(|hash| match self.lookup_hash(&hash) {
                // on an unchecked graph an unknown hash is indistinguishable
                // from a not-yet-computed one — refuse rather than guess
                HashLookup::Found(n) => Ok(n),
                HashLookup::Absent => Err(crate::AutomergeError::from(MissingDep(hash))),
                HashLookup::Unknown => Err(crate::AutomergeError::UncheckedHashGraph),
            })
            .collect::<Result<_, _>>()?;

        Ok(self.get_build_metadata_for_indexes(indexes))
    }

    pub(crate) fn iter(&self) -> ChangeIter<'_> {
        ChangeIter {
            index: 0,
            actors: self.actors.iter(),
            seq: self.seq.iter(),
            max_ops: self.max_ops.iter(),
            num_ops: self.num_ops.iter(),
            timestamps: self.timestamps.iter(),
            messages: self.messages.iter(),
            extra_bytes_meta: self
                .extra_bytes_meta
                .iter_range(0..self.extra_bytes_meta.len()),
            graph: self,
        }
    }

    fn get_build_metadata_for_indexes<I>(&self, indexes: I) -> Vec<BuildChangeMetadata<'_>>
    where
        I: IntoIterator<Item = NodeIdx>,
    {
        let changes = indexes
            .into_iter()
            .map(|index| {
                let i = index.0 as usize;
                let actor = self.actors[i].into();
                let timestamp = self.timestamps.get(i).unwrap_or_default();
                let max_op = self.max_ops[i] as u64;
                let num_ops = self.num_ops.get(i).unwrap_or_default();
                let message = self.messages.get(i).flatten().map(Cow::Borrowed);

                let meta = self.extra_bytes_meta.get(i).unwrap();
                let meta_range = meta.prefix() as usize..meta.total() as usize;
                let extra = Cow::Borrowed(&self.extra_bytes_raw[meta_range]);

                let deps = self.parents(index).map(|p| p.0 as u64).collect::<Vec<_>>();
                let start_op = max_op - num_ops + 1;
                let seq = self.seq[i] as u64;
                BuildChangeMetadata {
                    actor,
                    seq,
                    start_op,
                    max_op,
                    timestamp,
                    message,
                    extra,
                    deps,
                    builder: i,
                }
            })
            .collect();
        changes
    }

    fn get_build_indexes(&self, clock: SeqClock) -> Vec<NodeIdx> {
        let mut change_indexes: Vec<NodeIdx> = Vec::new();
        // walk the state from the given deps clock and add them into the vec
        for (actor_index, actor_changes) in self.seq_index.iter().enumerate() {
            if let Some(seq) = clock.get_for_actor(&actor_index) {
                // find the change in this actors sequence of changes that corresponds to the max_op
                // recorded for them in the clock
                change_indexes.extend(&actor_changes[seq.get() as usize..]);
            } else {
                change_indexes.extend(&actor_changes[..]);
            }
        }

        // ensure the changes are still in sorted order
        change_indexes.sort_unstable();

        change_indexes
    }

    pub(crate) fn get_hashes(
        &self,
        have_deps: &[ChangeHash],
    ) -> Result<Cow<'_, [ChangeHash]>, UncheckedHashes> {
        match (&self.hashes, have_deps.is_empty()) {
            (Hashes::Checked(all), true) => Ok(Cow::Borrowed(all)),
            (Hashes::Unchecked { .. }, true) => Err(UncheckedHashes),
            _ => {
                let clock = self.seq_clock_for_heads(have_deps)?;
                Ok(Cow::Owned(
                    self.get_build_indexes(clock)
                        .into_iter()
                        .map(|node| self.hashes.try_get(node))
                        .collect::<Result<_, _>>()?,
                ))
            }
        }
    }

    pub(crate) fn get_build_metadata_clock(
        &self,
        have_deps: &[ChangeHash],
    ) -> Result<Vec<BuildChangeMetadata<'_>>, UncheckedHashes> {
        let clock = self.seq_clock_for_heads(have_deps)?;
        Ok(self.get_build_metadata_for_seq_clock(clock))
    }

    pub(crate) fn get_build_metadata_for_seq_clock(
        &self,
        clock: SeqClock,
    ) -> Vec<BuildChangeMetadata<'_>> {
        let change_indexes = self.get_build_indexes(clock);
        self.get_build_metadata_for_indexes(change_indexes)
    }

    pub(crate) fn get_hash_for_actor_seq(
        &self,
        actor: usize,
        seq: u64,
    ) -> Result<ChangeHash, AutomergeError> {
        let node = self
            .seq_index
            .get(actor)
            .and_then(|v| v.get(seq as usize - 1))
            .ok_or(AutomergeError::InvalidSeq(seq))?;
        self.hashes
            .try_get(*node)
            .map_err(|_| AutomergeError::UncheckedHashGraph)
    }

    fn update_heads(&mut self, change: &Change) {
        for d in change.deps() {
            self.heads.remove(d);
        }
        self.heads.insert(change.hash());
    }

    pub(crate) fn add_nodes<
        'a,
        I: Iterator<Item = (&'a Change, usize)> + ExactSizeIterator + Clone,
    >(
        &mut self,
        iter: I,
    ) {
        self.actors
            .extend(iter.clone().map(|(_, a)| ActorIdx::from(a)));
        self.seq.extend(iter.clone().map(|(c, _)| c.seq() as u32));
        self.max_ops
            .extend(iter.clone().map(|(c, _)| c.max_op() as u32));
        self.num_ops
            .extend(iter.clone().map(|(c, _)| c.len() as u64));
        self.timestamps
            .extend(iter.clone().map(|(c, _)| c.timestamp()));
        self.messages.extend(iter.clone().map(|(c, _)| c.message()));
        self.extra_bytes_meta
            .extend(iter.clone().map(|(c, _)| ValueMeta::from(c.extra_bytes())));
        self.parents.extend(std::iter::repeat_n(None, iter.len()));
        for (c, _) in iter {
            self.extra_bytes_raw.extend_from_slice(c.extra_bytes());
        }
    }

    fn add_changes<'a, I: Iterator<Item = (&'a Change, usize)> + ExactSizeIterator + Clone>(
        &mut self,
        iter: I,
    ) -> Result<(), AddChangeError> {
        let node = NodeIdx(self.len() as u32);

        self.add_nodes(iter.clone());

        for (i, (change, actor)) in iter.enumerate() {
            let node_idx = node + i;
            let hash = change.hash();
            self.max_op = std::cmp::max(self.max_op, change.max_op() as u32);
            self.hashes.push(hash);
            debug_assert!(!self.nodes_by_hash.contains_key(&hash));
            self.nodes_by_hash.insert(hash, node_idx);
            self.update_heads(change);

            assert!(actor < self.seq_index.len());
            assert_eq!(self.seq_index[actor].len() + 1, change.seq() as usize);
            self.seq_index[actor].push(node_idx);

            let ResolvedHashes { nodes, missing } = self.resolve_hashes(change.deps().iter())?;
            if let Some(missing) = missing.first() {
                // callers check deps before calling us
                return Err(MissingDep(*missing).into());
            }
            for parent in nodes {
                self.add_parent(node_idx, parent);
            }

            if (node_idx + 1).0 % CACHE_STEP == 0 {
                self.cache_clock(node_idx);
            }

            self.cache_fragment(node_idx);
        }
        Ok(())
    }

    pub(crate) fn get_fragment(
        &self,
        head: ChangeHash,
        actors: &[crate::ActorId],
    ) -> Option<Fragment> {
        let n = self.nodes_by_hash.get(&head).copied()?;
        if head.fragment_level() == 0 {
            self.loose_commit(n, actors)
        } else {
            assert!(self.fragments.is_sorted_by(|a, b| a.head.0 < b.head.0));
            self.fragments
                .binary_search_by_key(&n.0, |f| f.head.0)
                .ok()
                .map(|i| self.fragments[i].export(self, actors))
        }
    }

    /// The `(actor, seq)` identity of a node — always derivable, hash
    /// graph state notwithstanding.
    pub(crate) fn change_id(&self, n: NodeIdx, actors: &[crate::ActorId]) -> ChangeId {
        let i = n.0 as usize;
        ChangeId {
            actor: actors[usize::from(self.actors[i])].clone(),
            seq: self.seq[i] as u64,
        }
    }

    /// Resolve a [`ChangeId`] back to its node.
    pub(crate) fn node_for_change_id(
        &self,
        id: &ChangeId,
        actors: &[crate::ActorId],
    ) -> Option<NodeIdx> {
        let actor_idx = actors.iter().position(|a| a == &id.actor)?;
        if id.seq == 0 {
            return None;
        }
        self.seq_index
            .get(actor_idx)?
            .get(id.seq as usize - 1)
            .copied()
    }

    fn loose_commit(&self, n: NodeIdx, actors: &[crate::ActorId]) -> Option<Fragment> {
        let head = self.hashes.get(n)?;
        assert_eq!(head.fragment_level(), 0);
        // on an unchecked graph a parent hash may be unknown, in which
        // case the fragment boundary cannot be described: no fragment
        let boundary = self
            .parents(n)
            .map(|p| self.hashes.get(p))
            .collect::<Option<Vec<_>>>()?;
        let members = vec![self.change_id(n, actors)];
        let checkpoints = vec![];
        let level = head.fragment_level();
        Some(Fragment {
            head,
            level,
            boundary,
            checkpoints,
            members,
        })
    }

    pub(crate) fn fragments<'a, R: RangeBounds<usize> + 'a>(
        &'a self,
        heads: &'a [ChangeHash],
        levels: R,
        actors: &'a [crate::ActorId],
    ) -> impl Iterator<Item = Fragment> + 'a {
        let heads = if levels.contains(&0) { heads } else { &[] };
        self.loose_fragments(heads, actors).chain(
            self.fragments
                .iter()
                .rev()
                .filter(move |f| {
                    self.hashes
                        .get(f.head)
                        .is_some_and(|h| levels.contains(&h.fragment_level()))
                })
                .map(|f| f.export(self, actors)),
        )
    }

    fn loose_fragments<'a>(
        &'a self,
        heads: &'a [ChangeHash],
        actors: &'a [crate::ActorId],
    ) -> impl Iterator<Item = Fragment> + 'a {
        let nodes = heads
            .iter()
            .filter(|h| h.fragment_level() == 0)
            .filter_map(|h| self.nodes_by_hash.get(h).copied());
        self.bfs_until_clock(nodes, &self.fragment_top)
            .filter_map(move |n| self.loose_commit(n, actors))
    }

    /// The member nodes of a fragment: `node`'s ancestry back to `clock`.
    fn fragment_nodes<'a>(
        &'a self,
        node: NodeIdx,
        clock: &'a SeqClock,
    ) -> impl Iterator<Item = NodeIdx> + 'a {
        self.bfs_until_clock([node], clock)
    }

    fn bfs_until_clock<'a, I>(
        &'a self,
        seed: I,
        clock: &'a SeqClock,
    ) -> impl Iterator<Item = NodeIdx> + 'a
    where
        I: IntoIterator<Item = NodeIdx>,
    {
        let mut to_visit: VecDeque<_> = seed.into_iter().collect();
        let mut seen: HashSet<_> = to_visit.iter().copied().collect();

        std::iter::from_fn(move || {
            let idx = to_visit.pop_front()?;
            for p in self.parents(idx) {
                if !seen.contains(&p) {
                    let actor = self.actors[p.0 as usize].into();
                    let seq = self.seq[p.0 as usize];
                    if clock.get_for_actor(&actor) < NonZeroU32::new(seq) {
                        seen.insert(p);
                        to_visit.push_back(p);
                    }
                }
            }
            Some(idx)
        })
    }

    /// Order fragments so every fragment's external member deps land in
    /// earlier fragments — the order `apply_fragment` needs.
    ///
    /// Sorting by head node index is not enough: a loose commit on a
    /// concurrent branch can predate (by node index) the head of the
    /// fragment covering its parents. So this is a proper topological
    /// sort of the fragment DAG, using head node index to break ties
    /// deterministically.
    pub(crate) fn sort_fragments_for_apply(
        &self,
        fragments: &mut Vec<Fragment>,
        actors: &[crate::ActorId],
    ) {
        let n = fragments.len();

        // which fragment owns each member node
        let mut owner: HashMap<NodeIdx, usize> = HashMap::new();
        for (i, f) in fragments.iter().enumerate() {
            for m in &f.members {
                if let Some(node) = self.node_for_change_id(m, actors) {
                    owner.insert(node, i);
                }
            }
        }

        // fragment-level dependency edges from the members' parents
        let mut indegree = vec![0usize; n];
        let mut children: Vec<Vec<usize>> = vec![Vec::new(); n];
        let mut seen: HashSet<(usize, usize)> = HashSet::new();
        for (&node, &i) in &owner {
            for p in self.parents(node) {
                if let Some(&j) = owner.get(&p) {
                    if j != i && seen.insert((j, i)) {
                        children[j].push(i);
                        indegree[i] += 1;
                    }
                }
            }
        }

        // Kahn's algorithm, popping the ready fragment with the
        // smallest head node index
        let head_node = |f: &Fragment| self.node_by_hash(&f.head);
        let mut ready: BTreeSet<(Option<NodeIdx>, usize)> = indegree
            .iter()
            .enumerate()
            .filter(|(_, d)| **d == 0)
            .map(|(i, _)| (head_node(&fragments[i]), i))
            .collect();
        let mut order = Vec::with_capacity(n);
        while let Some(&(key, i)) = ready.iter().next() {
            ready.remove(&(key, i));
            order.push(i);
            for &c in &children[i] {
                indegree[c] -= 1;
                if indegree[c] == 0 {
                    ready.insert((head_node(&fragments[c]), c));
                }
            }
        }
        debug_assert_eq!(order.len(), n, "fragment dependencies form a cycle");

        let mut pos = vec![0usize; n];
        for (rank, i) in order.iter().enumerate() {
            pos[*i] = rank;
        }
        let mut indexed: Vec<(usize, Fragment)> =
            std::mem::take(fragments).into_iter().enumerate().collect();
        indexed.sort_by_key(|(i, _)| pos[*i]);
        *fragments = indexed.into_iter().map(|(_, f)| f).collect();
    }

    pub(crate) fn cache_fragments(&mut self) {
        // idempotent: rebuild_hash_graph re-runs this after upgrading the
        // graph, so start from scratch
        self.fragments.clear();
        self.fragment_top = SeqClock::new(self.num_actors());
        for n in 0..self.hashes.len() {
            self.cache_fragment(NodeIdx(n as u32))
        }
    }

    fn cache_fragment(&mut self, head: NodeIdx) {
        // the fragment index needs the fragment-level hashes; on a plain
        // unchecked graph it would be silently incomplete
        if !self.hashes.has_fragment_hashes() {
            return;
        }
        let Some(hash) = self.hashes.get(head) else {
            return;
        };
        let level = hash.fragment_level();
        if level == 0 {
            return;
        }
        let mut deps = vec![];
        let mut supercede = vec![];
        let clock = self.calculate_clock(vec![head]);
        for (i, f) in self.fragments.iter().enumerate().rev() {
            if clock.covers(&f.clock) {
                let Some(f_hash) = self.hashes.get(f.head) else {
                    continue;
                };
                if f_hash.fragment_level() >= level {
                    deps.push(f.head);
                } else {
                    supercede.push(i);
                }
            }
        }
        for i in supercede {
            self.fragments.remove(i);
        }
        SeqClock::merge(&mut self.fragment_top, &clock);
        self.fragments.push(FragmentNode { head, deps, clock });
    }

    pub(crate) fn node_by_hash(&self, hash: &ChangeHash) -> Option<NodeIdx> {
        self.nodes_by_hash.get(hash).copied()
    }

    pub(crate) fn node_for_actor_seq(&self, actor: usize, seq: u64) -> Option<NodeIdx> {
        if seq == 0 {
            return None;
        }
        self.seq_index.get(actor)?.get(seq as usize - 1).copied()
    }

    pub(crate) fn hash_for_node(&self, node: NodeIdx) -> Option<ChangeHash> {
        self.hashes.get(node)
    }

    /// Append the member changes of a bundle without knowing their
    /// hashes.
    ///
    /// The members must be in topological order and each member's seq
    /// must extend its actor's chain — callers validate both. A checked
    /// graph downgrades to the fragment-hashes state (see
    /// [`Hashes::extend_unknown`]); the new nodes have no hash until
    /// `rebuild_hash_graph` recomputes them, so they cannot appear in
    /// `nodes_by_hash`, `heads` or the fragment index yet.
    pub(crate) fn add_fragment_members(&mut self, members: Vec<FragmentMember<'_>>) {
        let base = NodeIdx(self.len() as u32);

        self.hashes.extend_unknown(members.len());

        self.actors
            .extend(members.iter().map(|m| ActorIdx::from(m.actor)));
        self.seq.extend(members.iter().map(|m| m.seq as u32));
        self.max_ops.extend(members.iter().map(|m| m.max_op as u32));
        self.num_ops.extend(members.iter().map(|m| m.num_ops));
        self.timestamps.extend(members.iter().map(|m| m.timestamp));
        self.messages
            .extend(members.iter().map(|m| m.message.clone()));
        self.extra_bytes_meta
            .extend(members.iter().map(|m| ValueMeta::from(m.extra.as_ref())));
        self.parents
            .extend(std::iter::repeat_n(None, members.len()));
        for m in &members {
            self.extra_bytes_raw.extend_from_slice(&m.extra);
        }

        for (i, m) in members.iter().enumerate() {
            let node_idx = base + i;
            self.max_op = std::cmp::max(self.max_op, m.max_op as u32);

            assert!(m.actor < self.seq_index.len());
            assert_eq!(self.seq_index[m.actor].len() + 1, m.seq as usize);
            self.seq_index[m.actor].push(node_idx);

            for d in &m.deps {
                let parent = match d {
                    FragmentDep::Member(j) => {
                        debug_assert!(*j < i);
                        base + *j
                    }
                    FragmentDep::Node(n) => *n,
                };
                self.add_parent(node_idx, parent);
                // a parent that was a head is now covered
                if let Some(h) = self.hashes.get(parent) {
                    self.heads.remove(&h);
                }
            }

            if (node_idx + 1).0 % CACHE_STEP == 0 {
                self.cache_clock(node_idx);
            }
        }
    }

    /// Record the (unverified, until `rebuild_hash_graph`) hash of a
    /// node whose hash was unknown — a fragment head, checkpoint or
    /// boundary/dep pairing learned from an applied bundle. Makes the
    /// hash resolvable; maintains the fragment index for fragment-level
    /// hashes. No-op on a checked graph or for post-load nodes, whose
    /// hashes are already known.
    pub(crate) fn record_node_hash(&mut self, node: NodeIdx, hash: ChangeHash) {
        match &mut self.hashes {
            Hashes::Checked(_) => return,
            Hashes::Unchecked { watermark, pre, .. } => {
                if node.0 >= *watermark {
                    // tail nodes always have known hashes
                    return;
                }
                pre.insert(node, hash);
            }
        }
        self.nodes_by_hash.insert(hash, node);
        self.cache_fragment(node);
    }

    /// [`Self::record_node_hash`] for a fragment's head — the unique
    /// childless member — whose hash also joins the heads.
    pub(crate) fn record_fragment_head(&mut self, node: NodeIdx, hash: ChangeHash) {
        self.record_node_hash(node, hash);
        self.heads.insert(hash);
    }

    pub(crate) fn add_change(
        &mut self,
        change: &Change,
        actor: usize,
    ) -> Result<(), AddChangeError> {
        let hash = change.hash();

        if self.nodes_by_hash.contains_key(&hash) {
            return Ok(());
        }

        for h in change.deps().iter() {
            if !self.has_change(h)? {
                return Err(MissingDep(*h).into());
            }
        }

        self.add_changes([(change, actor)].into_iter())
    }

    fn cache_clock(&mut self, node_idx: NodeIdx) -> SeqClock {
        let mut clock = SeqClock::new(self.num_actors());
        let mut to_visit = BTreeSet::from([node_idx]);

        self.calculate_clock_inner(&mut clock, &mut to_visit, CACHE_STEP as usize * 2);

        for n in to_visit {
            let sub = self.cache_clock(n);
            SeqClock::merge(&mut clock, &sub);
        }

        self.clock_cache.insert(node_idx, clock.clone());

        clock
    }

    fn add_parent(&mut self, child_idx: NodeIdx, parent_idx: NodeIdx) {
        let new_edge_idx = EdgeIdx::new(self.edges.len());
        self.edges.push(Edge {
            target: parent_idx,
            next: None,
        });

        let child = &mut self.parents[child_idx.0 as usize];
        if let Some(edge_idx) = child {
            let mut edge = &mut self.edges[edge_idx.get()];
            while let Some(next) = edge.next {
                edge = &mut self.edges[next.get()];
            }
            edge.next = Some(new_edge_idx);
        } else {
            *child = Some(new_edge_idx);
        }
    }

    pub(crate) fn deps(
        &self,
        hash: &ChangeHash,
    ) -> impl Iterator<Item = Result<ChangeHash, UncheckedHashes>> + '_ {
        let mut iter = self.nodes_by_hash.get(hash).map(|node| self.parents(*node));
        std::iter::from_fn(move || {
            let next = iter.as_mut()?.next()?;
            Some(self.hashes.try_get(next))
        })
    }

    fn parents(&self, node_idx: NodeIdx) -> impl Iterator<Item = NodeIdx> + '_ {
        let mut edge_idx = self.parents[node_idx.0 as usize];
        std::iter::from_fn(move || {
            let this_edge_idx = edge_idx?;
            let edge = &self.edges[this_edge_idx.get()];
            edge_idx = edge.next;
            Some(edge.target)
        })
    }

    /// Resolve heads to nodes, silently skipping hashes which definitely
    /// aren't in this document.
    fn heads_to_nodes(&self, heads: &[ChangeHash]) -> Result<Vec<NodeIdx>, UncheckedHashes> {
        Ok(self.resolve_hashes(heads.iter())?.nodes)
    }

    #[allow(dead_code)]
    pub(crate) fn clock_at(&self, heads: &[ChangeHash]) -> Result<Clock, UncheckedHashes> {
        let nodes = self.heads_to_nodes(heads)?;
        Ok(self.clock_for_nodes(nodes))
    }

    /// Clock for `heads`, silently skipping hashes not in this document —
    /// the pre-unchecked-load semantics of every `*_at` read.
    ///
    /// Hashes known on an unchecked graph (the load heads and any change
    /// added since) resolve normally, so historical reads at the load heads
    /// work without the hash graph.
    pub(crate) fn clock_for_heads_lossy(&self, heads: &[ChangeHash]) -> Clock {
        let nodes = heads
            .iter()
            .filter_map(|h| self.nodes_by_hash.get(h).copied())
            .collect();
        self.clock_for_nodes(nodes)
    }

    /// Like [`Self::clock_for_heads_lossy`] but returning the seq clock.
    pub(crate) fn seq_clock_for_heads_lossy(&self, heads: &[ChangeHash]) -> SeqClock {
        let nodes = heads
            .iter()
            .filter_map(|h| self.nodes_by_hash.get(h).copied())
            .collect();
        self.calculate_clock(nodes)
    }

    fn clock_for_nodes(&self, nodes: Vec<NodeIdx>) -> Clock {
        self.calculate_clock(nodes)
            .iter()
            .map(|(actor, seq)| {
                self.seq_index
                    .get(actor)
                    .and_then(|v| v.get(seq?.get() as usize - 1))
                    .and_then(|i| self.max_ops.get(i.0 as usize))
                    .copied()
            })
            .collect()
    }

    pub(crate) fn seq_clock_for_heads(
        &self,
        heads: &[ChangeHash],
    ) -> Result<SeqClock, UncheckedHashes> {
        let nodes = self.heads_to_nodes(heads)?;
        Ok(self.calculate_clock(nodes))
    }

    fn clock_data_for(&self, idx: NodeIdx) -> Option<u32> {
        Some(*self.seq.get(idx.0 as usize)?)
    }

    fn calculate_clock(&self, nodes: Vec<NodeIdx>) -> SeqClock {
        let mut clock = SeqClock::new(self.num_actors());
        let mut to_visit = nodes.into_iter().collect::<BTreeSet<_>>();

        self.calculate_clock_inner(&mut clock, &mut to_visit, usize::MAX);

        assert!(to_visit.is_empty());

        clock
    }

    fn calculate_clock_inner(
        &self,
        clock: &mut SeqClock,
        to_visit: &mut BTreeSet<NodeIdx>,
        limit: usize,
    ) {
        let mut visited = BTreeSet::new();

        // The merge of every complete ancestor closure absorbed so far. A
        // cached clock covers the *entire* ancestry of its node, so any
        // node whose (actor, seq) is <= `covered` is an ancestor of an
        // already-absorbed closure (via its own actor's chain) and can be
        // dropped along with its whole subtree. Without this the walk is a
        // supercritical branching process on merge-heavy graphs: hitting a
        // cached node only stops one branch while the rest of the frontier
        // keeps fanning out.
        let mut covered = SeqClock::new(self.num_actors());

        while let Some(idx) = to_visit.pop_last() {
            assert!(!visited.contains(&idx));
            assert!(visited.len() <= self.len());
            visited.insert(idx);

            let actor = self.actors[idx.0 as usize];
            let data = self.clock_data_for(idx);

            if let (Some(d), Some(c)) = (data, covered.get_for_actor(&actor.into())) {
                if d <= c.get() {
                    continue;
                }
            }

            clock.include(actor.into(), data);

            if let Some(cached) = self.clock_cache.get(&idx) {
                SeqClock::merge(clock, cached);
                SeqClock::merge(&mut covered, cached);
            } else {
                to_visit.extend(self.parents(idx).filter(|p| !visited.contains(p)));
                if visited.len() > limit {
                    break;
                }
            }
        }
    }

    /// Install freshly recomputed hashes (one per node, in node order) and
    /// flip the graph to checked.
    ///
    /// Every hash we already knew — including the head pairing the document
    /// claimed at load time and the recorded heads themselves — must agree
    /// with the recomputed ones, otherwise the document lied and the
    /// offending hash is returned.
    pub(crate) fn install_checked_hashes(
        &mut self,
        hashes: Vec<ChangeHash>,
    ) -> Result<(), ChangeHash> {
        assert_eq!(hashes.len(), self.len(), "one hash per node");

        // previously known hashes (the claimed head pairing and everything
        // added since load) must match
        for idx in self.node_ids() {
            if let Some(known) = self.hashes.get(idx) {
                if hashes[idx.0 as usize] != known {
                    return Err(known);
                }
            }
        }

        // the recorded heads must be exactly the hashes of the childless
        // nodes
        let mut has_child = vec![false; self.len()];
        for edge in &self.edges {
            has_child[edge.target.0 as usize] = true;
        }
        let computed_heads: BTreeSet<ChangeHash> = (0..self.len())
            .filter(|n| !has_child[*n])
            .map(|n| hashes[n])
            .collect();
        if computed_heads != self.heads {
            let bad = self
                .heads
                .difference(&computed_heads)
                .next()
                .or_else(|| computed_heads.difference(&self.heads).next())
                .copied()
                .expect("unequal sets differ somewhere");
            return Err(bad);
        }

        self.nodes_by_hash = hashes
            .iter()
            .enumerate()
            .map(|(i, h)| (*h, NodeIdx(i as u32)))
            .collect();
        self.hashes = Hashes::Checked(hashes);
        Ok(())
    }

    /// Populate `clock_cache` with the clock of every `CACHE_STEP`th node.
    ///
    /// One forward pass in index order: `clock(i)` is the merge of its
    /// parents' clocks plus its own `(actor, seq)` entry. A node's row is
    /// dead once its last child has consumed it, so the live rows are
    /// bounded by the width of the unmerged frontier, not the graph size.
    fn cache_clocks(&mut self) {
        let n = self.len();
        if n < CACHE_STEP as usize {
            return; // nothing would be cached
        }

        fn alloc(pool: &mut Vec<SeqClock>, free: &mut Vec<u32>, width: usize) -> u32 {
            free.pop().unwrap_or_else(|| {
                pool.push(SeqClock::new(width));
                (pool.len() - 1) as u32
            })
        }

        fn two_rows(pool: &mut [SeqClock], dst: usize, src: usize) -> (&mut SeqClock, &SeqClock) {
            debug_assert_ne!(dst, src);
            if dst < src {
                let (lo, hi) = pool.split_at_mut(src);
                (&mut lo[dst], &hi[0])
            } else {
                let (lo, hi) = pool.split_at_mut(dst);
                (&mut hi[0], &lo[src])
            }
        }

        let num_actors = self.num_actors();

        let mut pending_children = vec![0u32; n];
        for edge in &self.edges {
            pending_children[edge.target.0 as usize] += 1;
        }

        const DEAD: u32 = u32::MAX;
        let mut slot_of = vec![DEAD; n]; // node -> pool slot while its row is live
        let mut pool: Vec<SeqClock> = Vec::new();
        let mut free: Vec<u32> = Vec::new();
        let mut parent_buf: Vec<usize> = Vec::new();

        for i in 0..n {
            let idx = NodeIdx(i as u32);

            parent_buf.clear();
            for p in self.parents(idx) {
                let p = p.0 as usize;
                // a change is only appended once its parents are present
                debug_assert!(p < i, "change graph is topologically ordered");
                parent_buf.push(p);
            }

            // acquire a row holding the merge of all parent clocks
            let slot = match parent_buf.split_first() {
                Some((&first, rest)) => {
                    let first_slot = slot_of[first];
                    debug_assert_ne!(first_slot, DEAD);
                    let slot = if pending_children[first] == 1 {
                        // we are the sole remaining child: take the row as is
                        slot_of[first] = DEAD;
                        first_slot
                    } else {
                        let s = alloc(&mut pool, &mut free, num_actors);
                        let (dst, src) = two_rows(&mut pool, s as usize, first_slot as usize);
                        dst.0.copy_from_slice(&src.0);
                        s
                    };
                    for &p in rest {
                        let p_slot = slot_of[p];
                        if p_slot == DEAD || p_slot == slot {
                            continue; // duplicate dep
                        }
                        let (dst, src) = two_rows(&mut pool, slot as usize, p_slot as usize);
                        SeqClock::merge(dst, src);
                    }
                    slot
                }
                None => {
                    let s = alloc(&mut pool, &mut free, num_actors);
                    pool[s as usize].0.fill(None);
                    s
                }
            };

            for &p in &parent_buf {
                pending_children[p] -= 1;
                if pending_children[p] == 0 && slot_of[p] != DEAD {
                    free.push(slot_of[p]);
                    slot_of[p] = DEAD;
                }
            }

            let actor = self.actors[i];
            pool[slot as usize].include(actor.into(), self.clock_data_for(idx));

            if (i as u32 + 1) % CACHE_STEP == 0 {
                self.clock_cache.insert(idx, pool[slot as usize].clone());
            }

            if pending_children[i] == 0 {
                free.push(slot); // no children will ever read this row
            } else {
                slot_of[i] = slot;
            }
        }
    }

    pub(crate) fn remove_ancestors(
        &self,
        changes: &mut BTreeSet<ChangeHash>,
        heads: &[ChangeHash],
    ) -> Result<(), UncheckedHashes> {
        let nodes = self.heads_to_nodes(heads)?;
        let mut unchecked = false;
        self.traverse_ancestors(nodes, |idx| {
            match self.hashes.get(idx) {
                Some(hash) => {
                    changes.remove(&hash);
                }
                None => unchecked = true,
            }
            true
        });
        if unchecked {
            Err(UncheckedHashes)
        } else {
            Ok(())
        }
    }

    fn traverse_ancestors<F: FnMut(NodeIdx) -> bool>(&self, mut to_visit: Vec<NodeIdx>, mut f: F) {
        let mut visited = BTreeSet::new();

        while let Some(idx) = to_visit.pop() {
            if visited.contains(&idx) {
                continue;
            } else {
                visited.insert(idx);
            }
            if f(idx) {
                to_visit.extend(self.parents(idx));
            }
        }
    }
}

impl ChangeGraphCols {
    pub(crate) fn iter(&self) -> ChangeIter<'_> {
        self.graph.iter()
    }

    pub(crate) fn finalize(self, changes: &[Change]) -> Result<ChangeGraph, InvalidHashColumn> {
        let mut graph = self.graph;
        debug_assert_eq!(changes.len(), graph.len());
        debug_assert!(graph.hashes.is_checked() && graph.hashes.len() == 0);

        // The encoded change columns only contain each change's maximum op.
        // `load()` estimates op counts from dependencies, but that is ambiguous
        // for an isolated actor whose first change can start above counter 1.
        // Reconstruction has the verified changes, so use their exact lengths.
        graph.num_ops = changes.iter().map(|change| change.len() as u64).collect();

        for (i, c) in changes.iter().enumerate() {
            let hash = c.hash();
            let node_idx = NodeIdx(i as u32);
            graph.nodes_by_hash.insert(hash, node_idx);
            graph.hashes.push(hash)
        }

        // a checked load recomputed every hash — the document's stored
        // hash columns must agree
        for (i, saved) in &self.saved_hashes {
            if graph.hashes.get(NodeIdx(*i)) != Some(*saved) {
                return Err(InvalidHashColumn);
            }
        }

        // The heads loaded from the document header are untrusted: replace
        // them with the computed heads (the hashes of the childless nodes).
        // Under `VerificationMode::Check` the caller verifies the two match;
        // under `DontCheck` this corrects a lying header.
        let mut has_child = vec![false; graph.len()];
        for edge in &graph.edges {
            has_child[edge.target.0 as usize] = true;
        }
        graph.heads = (0..graph.len() as u32)
            .filter(|n| !has_child[*n as usize])
            .filter_map(|n| graph.hashes.get(NodeIdx(n)))
            .collect();

        graph.cache_clocks();

        graph.cache_fragments();

        Ok(graph)
    }

    /// Finish loading without computing any change hashes.
    ///
    /// The only hashes known are the document's heads, paired with their
    /// nodes via the document's head index suffix (`heads[i]` names node
    /// `head_indexes[i]`). The pairing is validated structurally (indexes
    /// in range, distinct, childless nodes) but the hashes themselves are
    /// unverified until `rebuild_hash_graph`.
    pub(crate) fn finalize_unchecked(
        self,
        heads: &[ChangeHash],
        head_indexes: &[u64],
    ) -> Result<ChangeGraph, BadHeadIndexes> {
        let mut graph = self.graph;
        debug_assert!(graph.hashes.is_checked() && graph.hashes.len() == 0);

        if heads.len() != head_indexes.len() {
            return Err(BadHeadIndexes);
        }

        // the head nodes must be exactly the childless nodes
        let mut has_child = vec![false; graph.len()];
        for edge in &graph.edges {
            has_child[edge.target.0 as usize] = true;
        }
        let num_childless = has_child.iter().filter(|c| !**c).count();
        if num_childless != head_indexes.len() {
            return Err(BadHeadIndexes);
        }

        let mut pre = HashMap::with_capacity(heads.len());
        for (hash, index) in heads.iter().zip(head_indexes.iter()) {
            let i = *index as usize;
            if i >= graph.len() || has_child[i] {
                return Err(BadHeadIndexes);
            }
            let node = NodeIdx(*index as u32);
            if pre.insert(node, *hash).is_some() {
                // duplicate index
                return Err(BadHeadIndexes);
            }
            graph.nodes_by_hash.insert(*hash, node);
        }

        // import the hash columns (fragment-level hashes, loose commits
        // and anchors) — trusted like the head pairing until
        // `rebuild_hash_graph` verifies them
        let fragment_hashes = !self.saved_hashes.is_empty();
        for (i, hash) in self.saved_hashes {
            if i as usize >= graph.len() {
                return Err(BadHeadIndexes);
            }
            let node = NodeIdx(i);
            match pre.insert(node, hash) {
                // the column must not contradict the head pairing
                Some(prev) if prev != hash => return Err(BadHeadIndexes),
                _ => {}
            }
            graph.nodes_by_hash.insert(hash, node);
        }

        graph.hashes = Hashes::Unchecked {
            watermark: graph.len() as u32,
            tail: Vec::new(),
            pre,
            fragment_hashes,
        };

        graph.cache_clocks();

        // in the fragment-hashes state the fragment index is usable —
        // build it now (no-op on a plain unchecked graph)
        graph.cache_fragments();

        Ok(graph)
    }

    pub(crate) fn load(doc: &Document<'_>) -> Result<Self, LoadError> {
        use ids::*;

        let num_actors = doc.actors().len();
        let meta = doc.change_meta();
        let bytes = doc.change_bytes();

        let actor_bytes = meta.bytes(ACTOR_COL_SPEC, bytes);
        let seq_bytes = meta.bytes(SEQ_COL_SPEC, bytes);
        let max_op_bytes = meta.bytes(MAX_OP_COL_SPEC, bytes);
        let time_bytes = meta.bytes(TIME_COL_SPEC, bytes);
        let message_bytes = meta.bytes(MESSAGE_COL_SPEC, bytes);
        let deps_count_bytes = meta.bytes(DEPS_COUNT_COL_SPEC, bytes);
        let deps_val_bytes = meta.bytes(DEPS_VAL_COL_SPEC, bytes);
        let extra_meta_bytes = meta.bytes(EXTRA_META_COL_SPEC, bytes);

        let extra_bytes_raw = meta.bytes(EXTRA_VAL_COL_SPEC, bytes).to_vec();

        // the hash columns: a delta column of node indices plus the raw
        // 32-byte hashes (the metadata column is only there to keep the
        // value column legal — its contents are implied and regenerated
        // on save)
        let hash_index_bytes = meta.bytes(HASH_INDEX_COL_SPEC, bytes);
        let hash_val_bytes = meta.bytes(HASH_VAL_COL_SPEC, bytes);
        let saved_hashes = {
            let indices: Vec<u64> = hexane::DeltaColumn::<u64>::load(hash_index_bytes)?
                .iter()
                .collect();
            if hash_val_bytes.len() != 32 * indices.len() {
                return Err(LoadError::InvalidHashColumns);
            }
            let mut saved = Vec::with_capacity(indices.len());
            let mut prev: Option<u64> = None;
            for (i, chunk) in indices.iter().zip(hash_val_bytes.chunks_exact(32)) {
                if prev.is_some_and(|p| p >= *i) {
                    return Err(LoadError::InvalidHashColumns);
                }
                prev = Some(*i);
                let idx = u32::try_from(*i).map_err(|_| LoadError::InvalidHashColumns)?;
                let mut hash = [0u8; 32];
                hash.copy_from_slice(chunk);
                saved.push((idx, ChangeHash(hash)));
            }
            saved
        };

        let actors: Vec<ActorIdx> = hexane::decoder::<ActorIdx>(actor_bytes).collect();
        let max_ops: Vec<u32> = hexane::DeltaDecoder::<u32>::new(max_op_bytes).collect();
        let max_op = max_ops.iter().copied().max().unwrap_or(0);
        let seq: Vec<u32> = hexane::DeltaDecoder::<u32>::new(seq_bytes).collect();

        if let Some(a) = actors.iter().copied().map(usize::from).max() {
            if a >= num_actors {
                return Err(LoadError::InvalidActorId(a));
            }
        }

        let len = actors.len();

        let opts = hexane::LoadOpts::new().with_length(len);

        let timestamps = hexane::DeltaColumn::<i64>::load_with(time_bytes, opts.with_fill(0i64))?;
        let messages =
            hexane::Column::<Option<String>>::load_with(message_bytes, opts.with_fill(None))?;
        let extra_bytes_meta =
            hexane::PrefixColumn::<ValueMeta>::load_with(extra_meta_bytes, opts)?;

        if max_ops.len() != len {
            return Err(LoadError::InvalidColumnLength(MAX_OP_COL_SPEC));
        }
        if seq.len() != len {
            return Err(LoadError::InvalidColumnLength(SEQ_COL_SPEC));
        }
        if timestamps.len() != len {
            return Err(LoadError::InvalidColumnLength(TIME_COL_SPEC));
        }
        if messages.len() != len {
            return Err(LoadError::InvalidColumnLength(MESSAGE_COL_SPEC));
        }

        let mut seq_index = vec![vec![]; num_actors];
        for (i, actor) in actors.iter().enumerate() {
            let actor = actor.0 as usize;
            seq_index[actor].push(NodeIdx(i as u32));
        }

        let mut parents = Vec::with_capacity(len);
        let mut edges = vec![];

        let deps_count: Vec<u32> = hexane::decoder::<u32>(deps_count_bytes).collect();
        let mut deps_val_iter = hexane::DeltaDecoder::<u32>::new(deps_val_bytes);

        let mut num_ops_vec = Vec::with_capacity(len);
        for (i, d) in deps_count.iter().enumerate() {
            let d = *d as usize;
            if d == 0 {
                num_ops_vec.push(max_ops[i] as u64);
                parents.push(None);
                continue;
            }

            parents.push(Some(EdgeIdx::new(edges.len())));
            let mut last_max_op = 0;
            for e in 0..d {
                let dep = deps_val_iter
                    .next()
                    .ok_or(LoadError::InvalidColumnLength(DEPS_VAL_COL_SPEC))?;
                // hostile bytes: deps must reference earlier changes — the
                // format stores changes in topological order, `max_ops[dep]`
                // below indexes by it, and the clock-cache sweep relies on
                // parents preceding children
                if dep as usize >= i {
                    return Err(LoadError::InvalidDepIndex);
                }
                let target = NodeIdx(dep);
                let next = EdgeIdx::new(edges.len() + 1);
                let next = if e + 1 == d { None } else { Some(next) };
                last_max_op = std::cmp::max(last_max_op, max_ops[dep as usize]);
                edges.push(Edge { target, next })
            }
            if last_max_op > max_ops[i] {
                return Err(LoadError::InvalidMaxOp);
            }
            num_ops_vec.push(max_ops[i] as u64 - last_max_op as u64);
        }
        let num_ops: hexane::Column<u64> = num_ops_vec.into_iter().collect();

        let heads = doc.heads().iter().copied().collect();

        if parents.len() != len {
            return Err(LoadError::InvalidColumnLength(DEPS_COUNT_COL_SPEC));
        }

        // blank - to be filled out later
        let clock_cache = HashMap::default();
        let hashes = Hashes::default();
        let nodes_by_hash = HashMap::new();
        let fragments = vec![];
        let fragment_top = SeqClock::new(num_actors);

        if let Some((last, _)) = saved_hashes.last() {
            if *last as usize >= len {
                return Err(LoadError::InvalidHashColumns);
            }
        }

        Ok(ChangeGraphCols {
            saved_hashes,
            graph: ChangeGraph {
                edges,
                hashes,
                actors,
                parents,
                seq,
                max_ops,
                max_op,
                num_ops,
                timestamps,
                messages,
                extra_bytes_meta,
                extra_bytes_raw,
                heads,
                nodes_by_hash,
                clock_cache,
                seq_index,
                fragments,
                fragment_top,
            },
        })
    }
}

#[derive(Debug, thiserror::Error)]
#[error("attempted to derive a clock for a change with dependencies we don't have")]
pub struct MissingDep(ChangeHash);

#[derive(Debug, thiserror::Error)]
pub(crate) enum AddChangeError {
    #[error(transparent)]
    MissingDep(#[from] MissingDep),
    #[error(transparent)]
    Unchecked(#[from] UncheckedHashes),
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        time::{SystemTime, UNIX_EPOCH},
    };

    use crate::{
        make_rng,
        op_set2::{change::build_change, op_set::ResolvedAction, OpSet, TxOp},
        transaction::Transactable,
        types::{ObjMeta, OpId, OpType},
        ActorId, AutoCommit, Automerge, TextEncoding, ROOT,
    };
    use rand::RngExt;

    use super::*;

    #[test]
    fn cache_clocks_sweep_matches_backward_walk() {
        let mut builder = TestGraphBuilder::new();
        let a = builder.actor();
        let b = builder.actor();
        let c = builder.actor();

        // two roots, then interleaved cross-merges between a and b with an
        // occasional long single-actor chain (exercises the row-steal path)
        // and a third actor joining late
        let mut last_a = builder.change(&a, 1, &[]);
        let mut last_b = builder.change(&b, 1, &[]);
        for i in 0..20 {
            last_a = builder.change(&a, 1, &[last_a, last_b]);
            last_b = builder.change(&b, 1, &[last_b, last_a]);
            if i % 5 == 0 {
                for _ in 0..7 {
                    last_a = builder.change(&a, 1, &[last_a]);
                }
            }
        }
        let mut last_c = builder.change(&c, 1, &[last_a, last_b]);
        for _ in 0..20 {
            last_c = builder.change(&c, 1, &[last_c]);
        }

        let graph = builder.build();
        assert!(graph.len() > 2 * CACHE_STEP as usize);

        // the sweep's cache entries must match clocks computed by the plain
        // backward walk on a cache-free graph
        let mut swept = graph.clone();
        swept.clock_cache.clear();
        swept.cache_clocks();

        let mut bare = graph.clone();
        bare.clock_cache.clear();

        assert_eq!(swept.clock_cache.len(), graph.len() / CACHE_STEP as usize);
        for (idx, clock) in &swept.clock_cache {
            assert_eq!((idx.0 + 1) % CACHE_STEP, 0);
            assert_eq!(clock, &bare.calculate_clock(vec![*idx]), "node {idx:?}");
        }
    }

    #[test]
    fn clock_by_heads() {
        let mut builder = TestGraphBuilder::new();
        let actor1 = builder.actor();
        let actor2 = builder.actor();
        let actor3 = builder.actor();
        let change1 = builder.change(&actor1, 10, &[]);
        let change2 = builder.change(&actor2, 20, &[change1]);
        let change3 = builder.change(&actor3, 30, &[change1]);
        let change4 = builder.change(&actor1, 10, &[change2, change3]);
        let graph = builder.build();

        // todo - why 4?
        let mut expected_clock = SeqClock::new(3);
        expected_clock.include(builder.index(&actor1), Some(2));
        expected_clock.include(builder.index(&actor2), Some(1));
        expected_clock.include(builder.index(&actor3), Some(1));

        let clock = graph.seq_clock_for_heads(&[change4]).unwrap();
        assert_eq!(clock, expected_clock);
    }

    #[test]
    fn remove_ancestors() {
        let mut builder = TestGraphBuilder::new();
        let actor1 = builder.actor();
        let actor2 = builder.actor();
        let actor3 = builder.actor();
        let change1 = builder.change(&actor1, 10, &[]);
        let change2 = builder.change(&actor2, 20, &[change1]);
        let change3 = builder.change(&actor3, 30, &[change1]);
        let change4 = builder.change(&actor1, 10, &[change2, change3]);
        let graph = builder.build();

        let mut changes = vec![change1, change2, change3, change4]
            .into_iter()
            .collect::<BTreeSet<_>>();
        let heads = vec![change2];
        graph.remove_ancestors(&mut changes, &heads).unwrap();

        let expected_changes = vec![change3, change4].into_iter().collect::<BTreeSet<_>>();

        assert_eq!(changes, expected_changes);
    }

    struct TestGraphBuilder {
        actors: Vec<ActorId>,
        changes: Vec<Change>,
        graph: ChangeGraph,
        seqs_by_actor: BTreeMap<ActorId, u64>,
    }

    impl TestGraphBuilder {
        fn new() -> Self {
            TestGraphBuilder {
                actors: Vec::new(),
                changes: Vec::new(),
                graph: ChangeGraph::new(0),
                seqs_by_actor: BTreeMap::new(),
            }
        }

        fn actor(&mut self) -> ActorId {
            let actor = ActorId::random();
            self.graph.insert_actor(self.actors.len());
            self.actors.push(actor.clone());
            actor
        }

        fn index(&self, actor: &ActorId) -> usize {
            self.actors.iter().position(|a| a == actor).unwrap()
        }

        /// Create a change with `num_new_ops` and `parents` for `actor`
        ///
        /// The `start_op` and `seq` of the change will be computed from the
        /// previous changes for the same actor.
        fn change(
            &mut self,
            actor: &ActorId,
            num_new_ops: usize,
            parents: &[ChangeHash],
        ) -> ChangeHash {
            let osd = OpSet::from_actors(self.actors.clone(), TextEncoding::platform_default());

            let start_op = parents
                .iter()
                .map(|c| {
                    self.changes
                        .iter()
                        .find(|change| change.hash() == *c)
                        .unwrap()
                        .max_op()
                })
                .max()
                .unwrap_or(0)
                + 1;

            let actor_idx = self.index(actor);
            let ops = (0..num_new_ops)
                .map(|opnum| {
                    TxOp::map(
                        OpId::new(start_op + opnum as u64, actor_idx),
                        ObjMeta::root(),
                        0,
                        ResolvedAction::VisibleUpdate(OpType::Put("value".into())),
                        "key".to_string(),
                        vec![],
                    )
                })
                .collect::<Vec<_>>();

            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            let seq = self.seqs_by_actor.entry(actor.clone()).or_insert(1);
            let meta = BuildChangeMetadata {
                actor: actor_idx,
                builder: 0,
                deps: parents
                    .iter()
                    .map(|h| self.graph.hash_to_index(h).unwrap() as u64)
                    .collect(),
                seq: *seq,
                max_op: start_op + ops.len() as u64 - 1,
                start_op,
                timestamp,
                message: None,
                extra: Cow::Owned(vec![]),
            };
            let change = Change::new(build_change(&ops, &meta, &self.graph, &osd.actors));
            *seq = seq.checked_add(1).unwrap();
            let hash = change.hash();
            self.graph.add_change(&change, actor_idx).unwrap();
            self.changes.push(change);
            hash
        }

        fn build(&self) -> ChangeGraph {
            let mut graph = ChangeGraph::new(self.actors.len());
            for change in &self.changes {
                let actor_idx = self.index(change.actor_id());
                graph.add_change(change, actor_idx).unwrap();
            }
            graph
        }

        fn all_hashes(&self) -> Vec<ChangeHash> {
            self.changes.iter().map(|c| c.hash()).collect()
        }

        fn all_change_ids(&self) -> Vec<ChangeId> {
            self.changes
                .iter()
                .map(|c| ChangeId {
                    actor: c.actor_id().clone(),
                    seq: c.seq(),
                })
                .collect()
        }

        /// hash of each change keyed by its `(actor, seq)` id
        fn hash_of(&self) -> BTreeMap<(ActorId, u64), ChangeHash> {
            self.changes
                .iter()
                .map(|c| ((c.actor_id().clone(), c.seq()), c.hash()))
                .collect()
        }
    }

    fn member_hash(hash_of: &BTreeMap<(ActorId, u64), ChangeHash>, id: &ChangeId) -> ChangeHash {
        hash_of[&(id.actor.clone(), id.seq)]
    }

    #[test]
    fn fragments_cover_all_changes() {
        // Create a long linear chain — with ~1000 changes, we expect several
        // with fragment_level >= 1 (roughly 1 in 256).
        let mut builder = TestGraphBuilder::new();
        let actor = builder.actor();
        let mut prev = vec![];
        for _ in 0..1000 {
            let h = builder.change(&actor, 1, &prev);
            prev = vec![h];
        }
        let graph = builder.build();
        let all_ids: BTreeSet<_> = builder
            .all_change_ids()
            .into_iter()
            .map(|id| (id.actor, id.seq))
            .collect();
        let heads: Vec<_> = graph.heads().collect();

        let fragments: Vec<_> = graph.fragments(&heads, .., &builder.actors).collect();

        // Collect all member ids across all fragments
        // (members may appear in multiple fragments — this is expected)
        let mut covered: BTreeSet<(ActorId, u64)> = BTreeSet::new();
        for f in &fragments {
            for m in &f.members {
                covered.insert((m.actor.clone(), m.seq));
            }
        }

        // Every change must appear in at least one fragment
        let missing: Vec<_> = all_ids.difference(&covered).collect();
        assert!(
            missing.is_empty(),
            "changes not covered by any fragment: {:?}",
            missing,
        );
    }

    fn assert_fragment_invariants(
        fragments: &[Fragment],
        hash_of: &BTreeMap<(ActorId, u64), ChangeHash>,
    ) {
        for f in fragments {
            // level must match the fragment_level of the id hash
            assert_eq!(
                f.level,
                f.head.fragment_level(),
                "fragment level mismatch for {:?}",
                f.head
            );

            // id must be in members
            assert!(
                f.members.iter().any(|m| member_hash(hash_of, m) == f.head),
                "fragment id {:?} not found in its own members",
                f.head
            );

            // deps must be equal or higher level than the fragment
            for dep in &f.boundary {
                assert!(
                    dep.fragment_level() >= f.level,
                    "fragment {:?} (level {}) has dep {:?} with lower level {}",
                    f.head,
                    f.level,
                    dep,
                    dep.fragment_level(),
                );
            }

            // members must not contain a change with a higher level than the id
            for m in &f.members {
                let h = member_hash(hash_of, m);
                assert!(
                    h.fragment_level() <= f.level,
                    "fragment {:?} (level {}) contains {:?} with higher level {}",
                    f.head,
                    f.level,
                    h,
                    h.fragment_level(),
                );
            }
        }
    }

    #[test]
    fn fragment_id_and_level_consistent() {
        let mut builder = TestGraphBuilder::new();
        let actor = builder.actor();
        let mut prev = vec![];
        for _ in 0..1000 {
            let h = builder.change(&actor, 1, &prev);
            prev = vec![h];
        }
        let graph = builder.build();
        let heads: Vec<_> = graph.heads().collect();
        let fragments: Vec<_> = graph.fragments(&heads, .., &builder.actors).collect();

        assert_fragment_invariants(&fragments, &builder.hash_of());
    }

    #[test]
    fn fragments_work_with_concurrent_actors() {
        let mut builder = TestGraphBuilder::new();
        let actor1 = builder.actor();
        let actor2 = builder.actor();

        // Build two concurrent chains that merge periodically
        let root = builder.change(&actor1, 1, &[]);
        let mut tip1 = root;
        let mut tip2 = root;
        for i in 0..500 {
            tip1 = builder.change(&actor1, 1, &[tip1]);
            tip2 = builder.change(&actor2, 1, &[tip2]);
            if i % 50 == 49 {
                // merge
                let merge = builder.change(&actor1, 1, &[tip1, tip2]);
                tip1 = merge;
                tip2 = merge;
            }
        }
        let graph = builder.build();
        let all_ids: BTreeSet<_> = builder
            .all_change_ids()
            .into_iter()
            .map(|id| (id.actor, id.seq))
            .collect();
        let heads: Vec<_> = graph.heads().collect();
        let fragments: Vec<_> = graph.fragments(&heads, .., &builder.actors).collect();

        let mut covered: BTreeSet<(ActorId, u64)> = BTreeSet::new();
        for f in &fragments {
            for m in &f.members {
                covered.insert((m.actor.clone(), m.seq));
            }
        }

        let missing: Vec<_> = all_ids.difference(&covered).collect();
        assert!(
            missing.is_empty(),
            "changes not covered by any fragment: {:?}",
            missing,
        );

        assert_fragment_invariants(&fragments, &builder.hash_of());
    }

    #[test]
    fn fragment_deps_reference_known_hashes() {
        let mut builder = TestGraphBuilder::new();
        let actor = builder.actor();
        let mut prev = vec![];
        for _ in 0..1000 {
            let h = builder.change(&actor, 1, &prev);
            prev = vec![h];
        }
        let graph = builder.build();
        let all_hashes: BTreeSet<_> = builder.all_hashes().into_iter().collect();
        let heads: Vec<_> = graph.heads().collect();
        let fragments: Vec<_> = graph.fragments(&heads, .., &builder.actors).collect();
        let fragment_ids: BTreeSet<_> = fragments.iter().map(|f| f.head).collect();

        for f in &fragments {
            for dep in &f.boundary {
                assert!(
                    all_hashes.contains(dep),
                    "fragment {:?} has dep {:?} not in change graph",
                    f.head,
                    dep
                );
                // Deps of cached fragments (level > 0) should point to other fragment ids
                // Deps of loose fragments (level == 0) point to change-level parents
                if f.level > 0 {
                    assert!(
                        fragment_ids.contains(dep) || dep.fragment_level() == 0,
                        "cached fragment {:?} has dep {:?} that is not a fragment id",
                        f.head,
                        dep
                    );
                }
            }
        }
    }

    #[test]
    fn fragments_filtered_by_levels() {
        // 5000 changes gives ~20 expected level-1 fragments (1 hash in 256)
        // so seeing zero cached fragments would be extraordinarily unlikely.
        let mut builder = TestGraphBuilder::new();
        let actor = builder.actor();
        let mut prev = vec![];
        for _ in 0..5000 {
            let h = builder.change(&actor, 1, &prev);
            prev = vec![h];
        }
        let graph = builder.build();
        let heads: Vec<_> = graph.heads().collect();

        let all: Vec<_> = graph.fragments(&heads, .., &builder.actors).collect();
        let loose: Vec<_> = graph.fragments(&heads, 0..=0, &builder.actors).collect();
        let cached: Vec<_> = graph.fragments(&heads, 1.., &builder.actors).collect();

        // loose + cached partition the full range
        assert_eq!(loose.len() + cached.len(), all.len());
        assert!(!loose.is_empty());
        assert!(
            !cached.is_empty(),
            "expected at least one cached fragment from 5000 changes",
        );

        for f in &loose {
            assert_eq!(f.level, 0, "0..=0 returned a non-zero level fragment");
        }
        for f in &cached {
            assert!(f.level >= 1, "1.. returned a level-0 fragment");
        }

        // empty range yields nothing
        assert_eq!(graph.fragments(&heads, 0..0, &builder.actors).count(), 0);
    }

    #[test]
    fn get_fragment_returns_loose_and_cached() {
        let mut builder = TestGraphBuilder::new();
        let actor = builder.actor();
        let mut prev = vec![];
        for _ in 0..5000 {
            let h = builder.change(&actor, 1, &prev);
            prev = vec![h];
        }
        let graph = builder.build();
        let heads: Vec<_> = graph.heads().collect();

        let loose: Vec<_> = graph.fragments(&heads, 0..=0, &builder.actors).collect();
        let cached: Vec<_> = graph.fragments(&heads, 1.., &builder.actors).collect();
        assert!(!loose.is_empty());
        assert!(!cached.is_empty(), "expected at least one cached fragment");

        // get_fragment on a loose (level 0) commit hash returns an equivalent Fragment
        let l = &loose[0];
        let got = graph
            .get_fragment(l.head, &builder.actors)
            .expect("loose fragment exists");
        assert_eq!(got, *l);

        // get_fragment on a cached (level >= 1) fragment id returns an equivalent Fragment
        let c = &cached[0];
        let got = graph
            .get_fragment(c.head, &builder.actors)
            .expect("cached fragment exists");
        assert_eq!(got, *c);

        // unknown hash returns None
        assert!(graph
            .get_fragment(ChangeHash([0xff; 32]), &builder.actors)
            .is_none());
    }

    #[test]
    fn bundle_fragments_roundtrips_through_load_incremental() {
        let mut rng = make_rng();
        let mut doc = Automerge::new();

        for _ in 0..1_000 {
            let key = format!("k{}", rng.random::<u32>() % 32);
            let value = (rng.random::<u32>() % 1000) as i64;
            let mut tx = doc.transaction();
            tx.put(ROOT, key, value).unwrap();
            tx.commit();
        }

        let fragments = doc.fragments(..).unwrap();

        let bundles = doc.bundle_fragments(fragments).unwrap();

        let joined: Vec<u8> = bundles.into_iter().flatten().collect();

        let mut loaded = AutoCommit::new();
        loaded.load_incremental(&joined).unwrap();

        assert_eq!(doc.get_heads(), loaded.get_heads());

        let a = doc.save();
        let b = loaded.save();
        assert_eq!(a, b);
    }

    /// Regression test: `bundle()` must be insensitive to the order in which
    /// callers provide change hashes. The change-metadata columns inside a
    /// bundle are RLE/delta encoded, so if `from_meta` didn't sort its input
    /// the column data would thrash and the bundle would inflate 10–20× on
    /// common workloads (this is what `bundle_fragments` was hitting because
    /// `Fragment::members` is in topological iteration order, not start_op
    /// order).
    #[test]
    fn bundle_size_is_independent_of_input_hash_order() {
        use crate::transaction::Transactable;
        use crate::ROOT;

        let mut doc = Automerge::new();
        doc.set_actor(crate::ActorId::from(b"alice" as &[u8]))
            .unwrap();
        let mut tx = doc.transaction();
        tx.put(ROOT, "counter", 0i64).unwrap();
        tx.commit();
        for i in 1..=1_000_i64 {
            let mut tx = doc.transaction();
            tx.put(ROOT, "counter", i).unwrap();
            tx.commit();
        }

        let hashes: Vec<_> = doc
            .get_changes(&[])
            .unwrap()
            .iter()
            .map(|c| c.hash())
            .collect();

        let sorted_bytes = doc.bundle(hashes.iter().copied()).unwrap().bytes().len();

        let mut reversed = hashes.clone();
        reversed.reverse();
        let reversed_bytes = doc.bundle(reversed).unwrap().bytes().len();

        // Implementation must internally sort; the two bundles' sizes should
        // be identical (or at worst within a handful of bytes from differing
        // varint widths). They must NOT differ by an order of magnitude.
        assert_eq!(
            sorted_bytes, reversed_bytes,
            "bundle size depends on input hash order (sorted={}, reversed={}). \
             from_meta must sort by start_op before encoding columns.",
            sorted_bytes, reversed_bytes
        );

        // Sanity: the bundle should be within a small constant factor of the
        // doc's save_nocompress() size — the underlying columnar encoding is
        // the same, just without DEFLATE.
        let snc = doc.save_nocompress().len();
        assert!(
            sorted_bytes < snc * 2,
            "bundle of all changes ({} B) is suspiciously larger than \
             save_nocompress() ({} B); columns may not be packing.",
            sorted_bytes,
            snc
        );
    }
}

impl ExactSizeIterator for ChangeIter<'_> {
    fn len(&self) -> usize {
        self.graph.len() - self.index
    }
}

pub(crate) struct ChangeIter<'a> {
    index: usize,
    actors: std::slice::Iter<'a, ActorIdx>,
    seq: std::slice::Iter<'a, u32>,
    max_ops: std::slice::Iter<'a, u32>,
    num_ops: hexane::Iter<'a, u64>,
    timestamps: hexane::DeltaIter<'a, i64>,
    messages: hexane::Iter<'a, Option<String>>,
    extra_bytes_meta: hexane::prefix::PrefixIter<'a, ValueMeta>,
    graph: &'a ChangeGraph,
}

impl<'a> Iterator for ChangeIter<'a> {
    type Item = BuildChangeMetadata<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let i = self.index;
        self.index += 1;
        let actor = (*self.actors.next()?).into();
        let seq = *self.seq.next()? as u64;
        let max_op = *self.max_ops.next()? as u64;
        let num_ops = self.num_ops.next().unwrap_or_default();
        let timestamp = self.timestamps.next().unwrap_or_default();
        let message = self.messages.next().flatten().map(Cow::Borrowed);

        let start_op = max_op - num_ops + 1;

        let meta = self.extra_bytes_meta.next()?;
        let meta_range = meta.prefix() as usize..meta.total() as usize;
        let extra = Cow::Borrowed(&self.graph.extra_bytes_raw[meta_range]);
        let deps = self
            .graph
            .parents(NodeIdx(i as u32))
            .map(|n| n.0 as u64)
            .collect();
        Some(BuildChangeMetadata {
            actor,
            seq,
            start_op,
            max_op,
            timestamp,
            message,
            extra,
            deps,
            builder: 0,
        })
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        let i = self.index + n;
        self.index += n + 1;

        let actor = (*self.actors.nth(n)?).into();
        let seq = *self.seq.nth(n)? as u64;
        let max_op = *self.max_ops.nth(0)? as u64;
        let num_ops = self.num_ops.next().unwrap_or_default();
        let timestamp = self.timestamps.next().unwrap_or_default();
        let message = self.messages.next().flatten().map(Cow::Borrowed);

        let start_op = max_op - num_ops + 1;

        let meta = self.extra_bytes_meta.delta_nth(n)?;
        let meta_start = meta.delta as usize;
        let meta_range = meta_start..(meta_start + meta.pv.value.length());
        let extra = Cow::Borrowed(&self.graph.extra_bytes_raw[meta_range]);

        let deps = self
            .graph
            .parents(NodeIdx(i as u32))
            .map(|n| n.0 as u64)
            .collect();

        Some(BuildChangeMetadata {
            actor,
            seq,
            start_op,
            max_op,
            timestamp,
            message,
            extra,
            deps,
            builder: 0,
        })
    }
}

#[derive(Debug, PartialEq, Clone)]
struct FragmentNode {
    head: NodeIdx,
    deps: Vec<NodeIdx>,
    clock: SeqClock,
}

impl FragmentNode {
    fn export(&self, graph: &ChangeGraph, actors: &[crate::ActorId]) -> Fragment {
        let expect = "fragment index requires the fragment-hashes state";
        let head = graph.hashes.get(self.head).expect(expect);
        let level = head.fragment_level();
        let boundary = self
            .deps
            .iter()
            .map(|d| graph.hashes.get(*d).expect(expect))
            .collect();
        let clock = graph.calculate_clock(self.deps.clone());
        let nodes: Vec<_> = graph.fragment_nodes(self.head, &clock).collect();
        // interior hashes may be unknown in the fragment-hashes state,
        // but checkpoint (level > 0) hashes are always present in it
        let checkpoints = nodes
            .iter()
            .filter_map(|n| graph.hashes.get(*n))
            .filter(|h| h.fragment_level() > 0)
            .collect();
        let members = nodes.iter().map(|n| graph.change_id(*n, actors)).collect();
        Fragment {
            head,
            level,
            boundary,
            checkpoints,
            members,
        }
    }
}

/// EXPERIMENTAL: A section of the change graph identified by its head hash.
///
/// This is an experimental API, it may change or be removed without warning.
#[doc(hidden)]
#[derive(Debug, PartialEq, Clone)]
pub struct Fragment {
    pub head: ChangeHash,
    pub level: usize,
    pub boundary: Vec<ChangeHash>,
    pub checkpoints: Vec<ChangeHash>,
    /// The changes this fragment covers. Identified by `(actor, seq)`
    /// rather than hash so fragments can be produced in the
    /// fragment-hashes state, where interior change hashes are unknown.
    pub members: Vec<ChangeId>,
}

/// Identifies a change by `(actor, seq)` — derivable from the change
/// graph's structure without knowing the change's hash.
///
/// This is an experimental API, it may change or be removed without warning.
#[doc(hidden)]
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct ChangeId {
    pub actor: crate::ActorId,
    pub seq: u64,
}

impl std::fmt::Display for ChangeId {
    /// `"{seq}@{actor}"`, the same shape as object ids and cursors.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}@{}", self.seq, self.actor)
    }
}

impl std::str::FromStr for ChangeId {
    type Err = ParseChangeIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (seq, actor) = s.split_once('@').ok_or(ParseChangeIdError)?;
        let seq: u64 = seq.parse().map_err(|_| ParseChangeIdError)?;
        if seq == 0 {
            return Err(ParseChangeIdError);
        }
        let actor = hex::decode(actor).map_err(|_| ParseChangeIdError)?;
        Ok(ChangeId {
            actor: crate::ActorId::from(actor),
            seq,
        })
    }
}

/// Error parsing a [`ChangeId`] from its `"{seq}@{actor}"` form.
#[doc(hidden)]
#[derive(Debug, thiserror::Error)]
#[error("invalid change id: expected \"{{seq}}@{{actor}}\"")]
pub struct ParseChangeIdError;

/// How much of the document's change-hash graph is known.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum HashGraphState {
    /// Every change hash is known and validated; all APIs work.
    Checked,
    /// Change hashes are unverified and only partially known, but every
    /// hash needed to build fragments (fragment heads, checkpoints,
    /// boundaries, loose commits) is available. Fragment APIs work;
    /// other hash-dependent APIs error until
    /// [`rebuild_hash_graph`](crate::Automerge::rebuild_hash_graph).
    FragmentHashes,
    /// Only the load-time heads and post-load change hashes are known.
    /// Hash-dependent APIs (including fragments) error until
    /// [`rebuild_hash_graph`](crate::Automerge::rebuild_hash_graph).
    Unchecked,
}

#[rustfmt::skip]
pub(crate) mod ids {
    use crate::storage::{columns::ColumnId, ColumnSpec};

    const ACTOR_COL_ID: ColumnId = ColumnId::new(0);
    const SEQ_COL_ID: ColumnId = ColumnId::new(0);
    const MAX_OP_COL_ID: ColumnId = ColumnId::new(1);
    const TIME_COL_ID: ColumnId = ColumnId::new(2);
    const MESSAGE_COL_ID: ColumnId = ColumnId::new(3);
    const DEPS_COL_ID: ColumnId = ColumnId::new(4);
    const EXTRA_COL_ID: ColumnId = ColumnId::new(5);

    pub(super) const ACTOR_COL_SPEC:      ColumnSpec = ColumnSpec::new_actor(ACTOR_COL_ID);
    pub(super) const SEQ_COL_SPEC:        ColumnSpec = ColumnSpec::new_delta(SEQ_COL_ID);
    pub(super) const MAX_OP_COL_SPEC:     ColumnSpec = ColumnSpec::new_delta(MAX_OP_COL_ID);
    pub(super) const TIME_COL_SPEC:       ColumnSpec = ColumnSpec::new_delta(TIME_COL_ID);
    pub(super) const MESSAGE_COL_SPEC:    ColumnSpec = ColumnSpec::new_string(MESSAGE_COL_ID);
    pub(super) const DEPS_COUNT_COL_SPEC: ColumnSpec = ColumnSpec::new_group(DEPS_COL_ID);
    pub(super) const DEPS_VAL_COL_SPEC:   ColumnSpec = ColumnSpec::new_delta(DEPS_COL_ID);
    pub(super) const EXTRA_META_COL_SPEC: ColumnSpec = ColumnSpec::new_value_metadata(EXTRA_COL_ID);
    pub(super) const EXTRA_VAL_COL_SPEC:  ColumnSpec = ColumnSpec::new_value(EXTRA_COL_ID);
    const HASH_COL_ID: ColumnId = ColumnId::new(6);
    pub(super) const HASH_INDEX_COL_SPEC: ColumnSpec = ColumnSpec::new_delta(HASH_COL_ID);
    pub(super) const HASH_META_COL_SPEC:  ColumnSpec = ColumnSpec::new_value_metadata(HASH_COL_ID);
    pub(super) const HASH_VAL_COL_SPEC:   ColumnSpec = ColumnSpec::new_value(HASH_COL_ID);
}
