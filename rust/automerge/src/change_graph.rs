use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};
use std::num::{NonZeroU32, NonZeroU64};
use std::ops::Add;
use std::ops::RangeBounds;

use crate::change_id::ChangeId;
use crate::storage::{ChangeSetMetadata, DepRef};
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
    hashes: Hashes,
    actors: Vec<ActorIdx>,
    /// Parent (dependency) edges: `dep_range[n]` is the `(offset, count)`
    /// of node `n`'s parents within `dep_target`.
    ///
    /// The wire format already stores deps as a group column, so a load is
    /// a straight copy — offsets are the count column's running sum,
    /// targets are the value column verbatim. This data is append-only
    /// (nodes arrive in topological order and a node's parents are written
    /// with it), so no entry is ever moved.
    ///
    /// Replaces a per-node linked list whose `add_parent` walked to the
    /// tail on every edge — quadratic in a node's dep count — and whose
    /// traversal pointer-chased a `Vec<Edge>`. These are plain `Vec`s, not
    /// hexane columns, deliberately: `parents()` is read per node inside
    /// the clock-cache ancestry walks, where an O(1) index and a
    /// contiguous slice beat a compressed column's `get_prefix`.
    dep_range: Vec<(u32, u32)>,
    dep_target: Vec<NodeIdx>,
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
    /// Whether a new fragment frees its covered hashes immediately
    /// ([`GcMode::Auto`], the default) or waits to be asked
    /// ([`GcMode::Manual`]).
    gc_mode: crate::GcMode,
    /// Set when a GC was skipped under [`GcMode::Manual`].
    gc_owed: bool,
}

pub(crate) struct ChangeGraphCols {
    graph: ChangeGraph,
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
    /// Audit mode: every node's hash is known and validated.
    Full(Vec<ChangeHash>),
    /// Outside audit mode only the *retained set* is kept: the heads,
    /// loose commits (level-0 changes above the fragment frontier),
    /// fragment heads and checkpoints, and the deps/anchors needed to
    /// reconstruct them. When a new fragment usurps prior fragments the
    /// hashes it covers are freed (see
    /// [`ChangeGraph::gc_retained_hashes`]).
    Retained {
        map: HashMap<NodeIdx, ChangeHash>,
        /// the graph's node count, kept so `len()` stays O(1)
        len: usize,
    },
}

impl Default for Hashes {
    fn default() -> Self {
        // fresh documents default to AuditMode::Disabled
        Hashes::Retained {
            map: HashMap::new(),
            len: 0,
        }
    }
}

impl Hashes {
    fn len(&self) -> usize {
        match self {
            Self::Full(v) => v.len(),
            Self::Retained { len, .. } => *len,
        }
    }

    fn is_full(&self) -> bool {
        matches!(self, Self::Full(_))
    }

    fn audit_mode(&self) -> crate::AuditMode {
        match self {
            Self::Full(_) => crate::AuditMode::Enabled,
            Self::Retained { .. } => crate::AuditMode::Disabled,
        }
    }

    fn get(&self, idx: NodeIdx) -> Option<ChangeHash> {
        match self {
            Self::Full(v) => v.get(idx.0 as usize).copied(),
            Self::Retained { map, .. } => map.get(&idx).copied(),
        }
    }

    /// Every node whose hash is known, in no particular order.
    ///
    /// Outside audit mode this is the retained set — a few hundred
    /// entries on a document with a hundred thousand changes — which is
    /// why the retention rule reads it rather than the node range.
    fn iter(&self) -> impl Iterator<Item = (NodeIdx, ChangeHash)> + '_ {
        let full = match self {
            Self::Full(v) => Some(v.iter().enumerate().map(|(i, h)| (NodeIdx(i as u32), *h))),
            Self::Retained { .. } => None,
        };
        let retained = match self {
            Self::Full(_) => None,
            Self::Retained { map, .. } => Some(map.iter().map(|(n, h)| (*n, *h))),
        };
        full.into_iter()
            .flatten()
            .chain(retained.into_iter().flatten())
    }

    fn try_get(&self, idx: NodeIdx) -> Result<ChangeHash, UncheckedHashes> {
        self.get(idx).ok_or(UncheckedHashes)
    }

    fn push(&mut self, hash: ChangeHash) {
        match self {
            Self::Full(v) => v.push(hash),
            Self::Retained { map, len } => {
                // a new change is loose until a fragment covers it, so
                // its hash is retained
                map.insert(NodeIdx(*len as u32), hash);
                *len += 1;
            }
        }
    }

    /// Record that `n` nodes with unknown hashes are being appended
    /// (fragment members applied without reconstructing their changes).
    ///
    /// Never legal in audit mode: there the fragment fast path is not
    /// taken — fragments convert to changes and every hash is computed.
    fn extend_without_hashes(&mut self, n: usize) {
        if n == 0 {
            return;
        }
        match self {
            Self::Full(_) => {
                unreachable!("the fragment fast path never runs in audit mode")
            }
            Self::Retained { len, .. } => *len += n,
        }
    }
}

/// Resolution of an incoming change against a [`ChangeGraph`], for the
/// apply path — see [`ChangeGraph::lookup_change_for_apply`]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ApplyLookup {
    /// The change is already in the graph
    Present,
    /// The change is not in the graph
    Absent,
    /// A different change already occupies the change's `(actor, seq)`
    Equivocation,
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

/// The requested operation needs hashes outside the retained set, which
/// are only kept in audit mode
#[derive(Debug, thiserror::Error)]
#[error("this operation needs change hashes that are not retained, call enable_audit_mode() first")]
pub(crate) struct UncheckedHashes;

/// The document's head index suffix does not describe the change graph's
/// childless nodes
#[derive(Debug, thiserror::Error)]
#[error("the document's head indexes are invalid")]
pub(crate) struct BadHeadIndexes;

impl From<UncheckedHashes> for AutomergeError {
    fn from(_: UncheckedHashes) -> Self {
        AutomergeError::AuditModeRequired
    }
}

#[derive(Hash, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct NodeIdx(pub(crate) u32);

impl Add<usize> for NodeIdx {
    type Output = Self;

    fn add(self, other: usize) -> Self {
        NodeIdx(self.0 + other as u32)
    }
}

/// The first `n` items of `iter`, filling with `default` once it runs
/// out — an elided column decodes as empty and stands for a column of
/// defaults.
fn pad<T: Clone>(iter: impl Iterator<Item = T>, default: T, n: usize) -> impl Iterator<Item = T> {
    iter.chain(std::iter::repeat(default)).take(n)
}

/// A change with no extra bytes: zero-length, and typed as bytes like
/// every other entry in the extra column.
const NO_EXTRA: ValueMeta = ValueMeta::bytes(0);

/// A member change of a change set being applied without conversion into
/// [`Change`]s — everything the graph needs except the change's hash.
#[derive(Debug, Clone)]
pub(crate) struct ChangeSetMember<'a> {
    /// The member's actor as a document actor index
    pub(crate) actor: usize,
    pub(crate) seq: u64,
    pub(crate) max_op: u64,
    pub(crate) num_ops: u64,
    pub(crate) timestamp: i64,
    pub(crate) message: Option<String>,
    pub(crate) extra: Cow<'a, [u8]>,
    pub(crate) deps: Vec<ChangeSetDep>,
}

/// A [`ChangeSetMember`]'s dependency: another member of the same change set
/// (by its position in the member list, which is topological order) or
/// a node already in the graph.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ChangeSetDep {
    Member(usize),
    Node(NodeIdx),
}

impl ChangeGraph {
    pub(crate) fn new(num_actors: usize) -> Self {
        Self {
            gc_mode: crate::GcMode::default(),
            gc_owed: false,
            nodes_by_hash: HashMap::new(),
            hashes: Hashes::default(),
            actors: Vec::new(),
            max_ops: Vec::new(),
            max_op: 0,
            num_ops: hexane::Column::new(),
            seq: Vec::new(),
            dep_range: Vec::new(),
            dep_target: Vec::new(),
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

    /// Every node, ascending — which is topological order.
    pub(crate) fn all_nodes(&self) -> Vec<NodeIdx> {
        (0..self.len() as u32).map(NodeIdx).collect()
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

    pub(crate) fn hash_to_index(&self, hash: &ChangeHash) -> Option<usize> {
        self.nodes_by_hash.get(hash).map(|n| n.0 as usize)
    }

    pub(crate) fn index_to_hash(&self, index: usize) -> Option<ChangeHash> {
        self.hashes.get(NodeIdx(index as u32))
    }

    pub(crate) fn try_index_to_hash(&self, index: usize) -> Result<ChangeHash, UncheckedHashes> {
        self.hashes.try_get(NodeIdx(index as u32))
    }

    pub(crate) fn is_audit_enabled(&self) -> bool {
        self.hashes.is_full()
    }

    pub(crate) fn audit_mode(&self) -> crate::AuditMode {
        self.hashes.audit_mode()
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

    /// Whether the fragment frontier has reached node `n` — i.e. some
    /// cached fragment's clock covers it. A clock comparison against the
    /// node's own `(actor, seq)`, not an ancestry walk.
    fn is_covered(&self, n: NodeIdx) -> bool {
        let i = n.0 as usize;
        let actor = usize::from(self.actors[i]);
        self.fragment_top.get_for_actor(&actor) >= NonZeroU32::new(self.seq[i])
    }

    /// The retention rule: which known-hash nodes must keep their hashes
    /// outside audit mode. Fragment heads and checkpoints (any node with
    /// `fragment_level() > 0`), loose commits (level-0 nodes above the
    /// fragment frontier) plus their covered level-0 parents (anchors —
    /// their fragment boundaries need them). Heads are not included; add
    /// them when the caller needs the full retained set.
    ///
    /// Driven by the *hashes*, not by the node range. Only a node whose
    /// hash is still known can be retained, so iterating the retained map
    /// visits every candidate — where walking `0..len()` spent a hash
    /// lookup per node in the graph to find the same few hundred. On a
    /// 93k-change document that walk was the entire cost of the retention
    /// GC (and of every `save`), and it recomputed a set the map already
    /// delimited.
    fn retained_nodes(&self) -> BTreeSet<NodeIdx> {
        let mut keep = BTreeSet::new();
        for (n, hash) in self.hashes.iter() {
            if hash.fragment_level() > 0 {
                keep.insert(n);
            } else if !self.is_covered(n) {
                // a loose commit — plus its covered level-0 parents
                // (anchors), which its fragment boundary will need
                keep.insert(n);
                for p in self.parents(n) {
                    if self.is_covered(p)
                        && self
                            .hashes
                            .get(p)
                            .is_some_and(|ph| ph.fragment_level() == 0)
                    {
                        keep.insert(p);
                    }
                }
            }
        }
        // every actor's tip: committing as an actor names its latest
        // change by hash
        for changes in &self.seq_index {
            if let Some(tip) = changes.last() {
                keep.insert(*tip);
            }
        }
        keep
    }

    /// Drop every hash outside the retained set and switch to (or stay
    /// in) the [`Hashes::Retained`] representation — the disable-audit
    /// transition, also the GC run when fragments usurp prior coverage.
    pub(crate) fn retain_hashes_only(&mut self) {
        let keep = self.retained_nodes();
        // the hash count, NOT the graph's node count: during
        // `add_changes` the nodes are all added up front while hashes
        // are pushed one at a time, and a GC firing mid-loop (a new
        // change formed a fragment) must leave the push cursor where
        // it was
        let len = self.hashes.len();
        // the heads are retained too, and a head's node need not be in
        // `keep` (a covered head with a late-arriving child)
        let map: HashMap<NodeIdx, ChangeHash> = self
            .hashes
            .iter()
            .filter(|(n, hash)| keep.contains(n) || self.heads.contains(hash))
            .collect();
        self.nodes_by_hash.retain(|_, n| map.contains_key(n));
        self.hashes = Hashes::Retained { map, len };
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

        let cols = vec![
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

    pub(crate) fn deps_for_hash(
        &self,
        hash: &ChangeHash,
    ) -> impl Iterator<Item = Result<ChangeHash, UncheckedHashes>> + '_ {
        let parents = self
            .nodes_by_hash
            .get(hash)
            .map(|n| self.parent_slice(*n))
            .unwrap_or(&[]);
        parents.iter().map(move |p| self.hashes.try_get(*p))
    }

    fn lookup_hash(&self, hash: &ChangeHash) -> HashLookup {
        if let Some(n) = self.nodes_by_hash.get(hash) {
            return HashLookup::Found(*n);
        }
        match &self.hashes {
            // audit mode knows every hash: not found means not here
            Hashes::Full(_) => HashLookup::Absent,
            // while the retained map is still complete (nothing freed
            // yet — fresh documents stay complete until a fragment is
            // cached) a miss is just as definitive
            Hashes::Retained { map, len } if map.len() == *len => HashLookup::Absent,
            // otherwise an unknown hash may merely be freed
            Hashes::Retained { .. } => HashLookup::Unknown,
        }
    }

    /// [`Self::has_change`] for the apply path, where an unknown hash
    /// must not error: an incoming change is resolved through its
    /// `(actor, seq)` instead.
    ///
    /// Outside audit mode a change whose `(actor, seq)` the graph covers
    /// but whose hash was freed is trusted to *be* the covered change —
    /// the same identify-by-`(actor, seq)` rule the fragment apply path
    /// uses. When the covered slot's hash IS retained and differs, the
    /// change is an equivocation.
    pub(crate) fn lookup_change_for_apply(
        &self,
        hash: &ChangeHash,
        id: &ChangeId,
        actors: &[crate::ActorId],
    ) -> ApplyLookup {
        if self.nodes_by_hash.contains_key(hash) {
            return ApplyLookup::Present;
        }
        let Some(node) = self.node_for_change_id(id, actors) else {
            return ApplyLookup::Absent;
        };
        match self.hashes.get(node) {
            // the slot's hash is known and it is not this change's
            Some(_) => ApplyLookup::Equivocation,
            // the slot's hash was freed: trust the (actor, seq) identity
            None => ApplyLookup::Present,
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

    pub(crate) fn has_change(&self, hash: &ChangeHash) -> Result<bool, UncheckedHashes> {
        match self.lookup_hash(hash) {
            HashLookup::Found(_) => Ok(true),
            HashLookup::Absent => Ok(false),
            HashLookup::Unknown => Err(UncheckedHashes),
        }
    }

    pub(crate) fn get_change_set_metadata<I>(
        &self,
        hashes: I,
    ) -> impl Iterator<Item = Result<ChangeSetMetadata<'_>, MissingDep>>
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
                    missing = Some(MissingDep);
                    break;
                }
            }
        }
        nodes.sort_unstable();
        let err = missing.into_iter().map(Err);
        let ok = if err.len() > 0 { Vec::new() } else { nodes };
        self.change_set_metadata_for_nodes(ok).chain(err)
    }

    /// Change set metadata for a set of member nodes, deps pre-resolved to
    /// member positions or external hashes. Only the *external* (boundary)
    /// hashes need to be known, so this works on a graph in the
    /// fragment-hashes state. `nodes` must be sorted ascending.
    pub(crate) fn change_set_metadata_for_nodes(
        &self,
        nodes: Vec<NodeIdx>,
    ) -> impl Iterator<Item = Result<ChangeSetMetadata<'_>, MissingDep>> {
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
                    None => self.hashes.get(p).map(DepRef::External).ok_or(MissingDep),
                })
                .collect::<Result<Vec<_>, _>>()?;

            let start_op = max_op - num_ops + 1;
            let seq = self.seq[i] as u64;
            Ok(ChangeSetMetadata {
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
                HashLookup::Absent => Err(crate::AutomergeError::from(MissingDep)),
                HashLookup::Unknown => Err(crate::AutomergeError::AuditModeRequired),
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

    /// `node` and every ancestor whose hash the GC freed, ascending —
    /// the set a rebuild must reconstruct.
    pub(crate) fn nodes_back_to_retained(&self, node: NodeIdx) -> Vec<NodeIdx> {
        let mut members: BTreeSet<NodeIdx> = BTreeSet::new();
        members.insert(node);
        let mut pending = vec![node];
        while let Some(n) = pending.pop() {
            for p in self.parent_slice(n).to_vec() {
                if members.contains(&p) || self.hashes.get(p).is_some() {
                    continue;
                }
                members.insert(p);
                pending.push(p);
            }
        }
        // NodeIdx order is insertion order, which is topological
        members.into_iter().collect()
    }

    /// Whether every dep falling outside `nodes` still has its hash, and
    /// so can be named in a change set's boundary.
    pub(crate) fn boundary_is_nameable(&self, nodes: &[NodeIdx]) -> bool {
        if self.hashes.is_full() {
            return true;
        }
        nodes.iter().all(|n| {
            self.parent_slice(*n)
                .iter()
                .all(|p| nodes.binary_search(p).is_ok() || self.hashes.get(*p).is_some())
        })
    }

    /// Smallest fragment whose extent covers `deps`. Reversed so the
    /// first match is the finest. Not binary-searchable: concurrent
    /// fragments interleave.
    fn smallest_fragment_covering(&self, deps: &SeqClock) -> Option<&FragmentNode> {
        self.fragments.iter().rev().find(|f| f.clock.covers(deps))
    }

    /// A boundary the GC has left unnameable, moved back to the deps of
    /// the smallest fragment reaching that far. Those are fragment heads,
    /// which are always retained, so one step suffices. `None` when
    /// nothing needs widening.
    pub(crate) fn widen_boundary_to_fragment(&self, deps: &SeqClock) -> Option<SeqClock> {
        if self.hashes.is_full() {
            return None;
        }
        let f = self.smallest_fragment_covering(deps)?;
        Some(self.calculate_clock(f.deps.clone()))
    }

    /// The nodes a seq clock does *not* cover, ascending — which is node
    /// order, which is topological order.
    pub(crate) fn get_build_indexes(&self, clock: SeqClock) -> Vec<NodeIdx> {
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
            (Hashes::Full(all), true) => Ok(Cow::Borrowed(all)),
            (Hashes::Retained { .. }, true) => Err(UncheckedHashes),
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
        for (c, _) in iter {
            self.extra_bytes_raw.extend_from_slice(c.extra_bytes());
        }
    }

    pub(crate) fn add_changes<
        'a,
        I: Iterator<Item = (&'a Change, usize)> + ExactSizeIterator + Clone,
    >(
        &mut self,
        iter: I,
    ) -> Result<(), AddChangeError> {
        let node = NodeIdx(self.len() as u32);
        let mut new_fragment = false;

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
            if !missing.is_empty() {
                // callers check deps before calling us
                return Err(MissingDep.into());
            }
            self.push_parents(node_idx, nodes);

            if (node_idx + 1).0.is_multiple_of(CACHE_STEP) {
                self.cache_clock(node_idx);
            }

            // GC deferred to the end of the batch: later changes in
            // this batch may still resolve their deps by hash
            new_fragment |= self.cache_fragment_inner(node_idx);
        }
        if new_fragment {
            self.gc_retained_hashes();
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
            debug_assert!(self.fragments.is_sorted_by_key(|f| f.sort_key()));
            let key = (std::cmp::Reverse(head.fragment_level()), n);
            self.fragments
                .binary_search_by_key(&key, |f| f.sort_key())
                .ok()
                .map(|i| self.cached_fragment(&self.fragments[i], actors))
        }
    }

    /// The `(actor, seq)` identity of a node — always derivable, hash
    /// graph state notwithstanding.
    pub(crate) fn change_id(&self, n: NodeIdx, actors: &[crate::ActorId]) -> ChangeId {
        let i = n.0 as usize;
        let actor_idx = usize::from(self.actors[i]);
        ChangeId::from_doc_seq(self.seq[i] as u64, actors[actor_idx].clone(), actor_idx)
    }

    /// Resolve a [`ChangeId`] back to its node, verifying the id's
    /// actor index hint. Hash-free.
    pub(crate) fn node_for_change_id(
        &self,
        id: &ChangeId,
        actors: &[crate::ActorId],
    ) -> Option<NodeIdx> {
        let hint = id.actor_idx_hint();
        let actor_idx = if actors.get(hint) == Some(id.actor()) {
            hint
        } else {
            actors.binary_search(id.actor()).ok()?
        };
        self.seq_index
            .get(actor_idx)?
            .get(id.seq() as usize - 1)
            .copied()
    }

    /// The retained hash of the change named by `id`, if any.
    pub(crate) fn hash_for_change_id(
        &self,
        id: &ChangeId,
        actors: &[crate::ActorId],
    ) -> Option<ChangeHash> {
        self.hashes.get(self.node_for_change_id(id, actors)?)
    }

    /// Like [`Self::hash_for_change_id`] but distinguishes "no such
    /// change" ([`AutomergeError::InvalidSeq`]) from "the hash was
    /// freed" ([`AutomergeError::AuditModeRequired`]).
    pub(crate) fn get_hash_for_change_id(
        &self,
        id: &ChangeId,
        actors: &[crate::ActorId],
    ) -> Result<ChangeHash, AutomergeError> {
        let node = self
            .node_for_change_id(id, actors)
            .ok_or(AutomergeError::InvalidSeq(id.seq()))?;
        self.hashes
            .try_get(node)
            .map_err(|_| AutomergeError::AuditModeRequired)
    }

    /// The [`ChangeId`] of the op's containing change — hash-free.
    pub(crate) fn opid_to_change_id(
        &self,
        id: OpId,
        actors: &[crate::ActorId],
    ) -> Option<ChangeId> {
        let node = self.opid_to_node(id)?;
        Some(self.change_id(node, actors))
    }

    /// The [`ChangeId`] for a hash: `Ok(None)` when the hash is
    /// definitively absent, error when retained hashes cannot tell.
    pub(crate) fn change_id_for_hash(
        &self,
        hash: &ChangeHash,
        actors: &[crate::ActorId],
    ) -> Result<Option<ChangeId>, UncheckedHashes> {
        let node = match self.lookup_hash(hash) {
            HashLookup::Found(n) => n,
            HashLookup::Absent => return Ok(None),
            HashLookup::Unknown => return Err(UncheckedHashes),
        };
        Ok(Some(self.change_id(node, actors)))
    }

    pub(crate) fn seq_clock_for_nodes(&self, nodes: Vec<NodeIdx>) -> SeqClock {
        self.calculate_clock(nodes)
    }

    /// The current heads as sorted change ids — canonical, so two
    /// documents with equal heads report identical lists.
    pub(crate) fn head_change_ids(&self, actors: &[crate::ActorId]) -> Vec<ChangeId> {
        let mut ids: Vec<ChangeId> = self
            .heads
            .iter()
            .filter_map(|h| self.nodes_by_hash.get(h))
            .map(|n| self.change_id(*n, actors))
            .collect();
        ids.sort_unstable();
        ids
    }

    /// Whether `nodes` is exactly the current head set (order and
    /// duplicates ignored). Head hashes are always known.
    pub(crate) fn nodes_are_heads(&self, nodes: &[NodeIdx]) -> bool {
        let head_nodes: std::collections::BTreeSet<NodeIdx> = self
            .heads
            .iter()
            .filter_map(|h| self.nodes_by_hash.get(h))
            .copied()
            .collect();
        let given: std::collections::BTreeSet<NodeIdx> = nodes.iter().copied().collect();
        head_nodes == given
    }

    /// A loose commit as its own single-member fragment.
    fn loose_commit(&self, n: NodeIdx, actors: &[crate::ActorId]) -> Option<Fragment> {
        let head = self.hashes.get(n)?;
        assert_eq!(head.fragment_level(), 0);
        // on an unchecked graph a parent hash may be unknown, in which
        // case the fragment boundary cannot be described: no fragment
        let boundary = self
            .parents(n)
            .map(|p| self.hashes.get(p))
            .collect::<Option<Vec<_>>>()?;
        Some(self.export_fragment(head, 0, boundary, &[n], actors))
    }

    /// A cached fragment, resolved down to its member nodes and out.
    fn cached_fragment(&self, f: &FragmentNode, actors: &[crate::ActorId]) -> Fragment {
        let expect = "fragment index requires the fragment-hashes state";
        let head = self.hashes.get(f.head).expect(expect);
        let boundary = f
            .deps
            .iter()
            .map(|d| self.hashes.get(*d).expect(expect))
            .collect();
        let clock = self.calculate_clock(f.deps.clone());
        let nodes = self.fragment_nodes(f.head, &clock);
        self.export_fragment(head, f.level, boundary, &nodes, actors)
    }

    fn export_fragment(
        &self,
        head: ChangeHash,
        level: usize,
        boundary: Vec<ChangeHash>,
        nodes: &[NodeIdx],
        actors: &[crate::ActorId],
    ) -> Fragment {
        // interior hashes may be unknown in the fragment-hashes state,
        // but checkpoint (level > 0) hashes are always present in it
        let checkpoints = nodes
            .iter()
            .filter_map(|n| self.hashes.get(*n))
            .filter(|h| h.fragment_level() > 0)
            .collect();
        let members = nodes.iter().map(|n| self.change_id(*n, actors)).collect();
        Fragment {
            head,
            level,
            boundary,
            checkpoints,
            members,
        }
    }

    /// The fragments covering `heads` at the given levels, in the order
    /// `apply_change_set_opsment` needs them: coarsest first, and within a level
    /// oldest first. Nothing here sorts — the index is maintained in
    /// that order ([`FragmentNode::sort_key`]) and loose commits, all
    /// level 0, follow it in causal order.
    ///
    /// That order — level descending, head node index ascending — is an
    /// apply order because whoever supplies a fragment's external deps
    /// always precedes it:
    ///
    /// * a fragment's deps are recorded only for fragments of level >=
    ///   its own ([`Self::cache_fragment_inner`]), and a dep's head is
    ///   an ancestor of the head, so it has a lower node index — a
    ///   same-level supplier is always older;
    /// * a fragment that usurps another has strictly greater level than
    ///   the one it absorbs, so a survivor whose boundary was usurped
    ///   finds its changes in a coarser fragment, which comes first —
    ///   node index alone gets this wrong, since the usurper is
    ///   concurrent with the survivor and can be much newer;
    /// * loose commits are the nodes above `fragment_top`, so nothing
    ///   cached depends on one; they come last, and among themselves
    ///   node index is topological because a parent's node index is
    ///   always lower than its child's.
    pub(crate) fn fragments<R: RangeBounds<usize>>(
        &self,
        heads: &[ChangeHash],
        levels: R,
        actors: &[crate::ActorId],
    ) -> Vec<Fragment> {
        let mut out: Vec<Fragment> = self
            .fragments
            .iter()
            .filter(|f| levels.contains(&f.level))
            .map(|f| self.cached_fragment(f, actors))
            .collect();
        if levels.contains(&0) {
            out.extend(self.loose_commits(heads, actors));
        }
        debug_assert!(
            out.is_sorted_by_key(|f| (std::cmp::Reverse(f.level), self.node_by_hash(&f.head))),
            "fragments are not in apply order",
        );
        out
    }

    /// The loose commits above the fragment index, oldest first.
    fn loose_commits(&self, heads: &[ChangeHash], actors: &[crate::ActorId]) -> Vec<Fragment> {
        let nodes = heads
            .iter()
            .filter(|h| h.fragment_level() == 0)
            .filter_map(|h| self.nodes_by_hash.get(h).copied());
        self.ancestry_until_clock(nodes, &self.fragment_top)
            .filter_map(|n| self.loose_commit(n, actors))
            .collect()
    }

    /// Everything reachable from `heads` that `dep_clock` does not
    /// cover, oldest first.
    pub(crate) fn members_between(
        &self,
        heads: impl IntoIterator<Item = NodeIdx>,
        dep_clock: &SeqClock,
    ) -> Vec<NodeIdx> {
        self.ancestry_until_clock(heads, dep_clock).collect()
    }

    /// The member nodes of a fragment, oldest first: `node`'s ancestry
    /// back to `clock`.
    fn fragment_nodes(&self, node: NodeIdx, clock: &SeqClock) -> Vec<NodeIdx> {
        self.ancestry_until_clock([node], clock).collect()
    }

    /// [`Self::rev_ancestry_until_clock`] in causal order: parents
    /// before children.
    fn ancestry_until_clock<'a, I>(
        &'a self,
        seed: I,
        clock: &'a SeqClock,
    ) -> impl Iterator<Item = NodeIdx> + 'a
    where
        I: IntoIterator<Item = NodeIdx>,
    {
        let mut nodes: Vec<_> = self.rev_ancestry_until_clock(seed, clock).collect();
        nodes.reverse();
        nodes.into_iter()
    }

    /// The ancestry of `seed` back to `clock`, newest node first.
    ///
    /// Not a breadth-first search: the frontier is a priority queue
    /// popped largest-index-first, so this visits nodes in strictly
    /// descending node index. Only parents are ever pushed, and a
    /// parent's node index is always lower than its child's (see
    /// [`Self::add_parent`]), so the popped index can only decrease and
    /// the output is a reverse topological order — every node is
    /// emitted before any of its parents. Reverse it for causal order.
    ///
    /// That also means a popped node can never be pushed again — anything
    /// pushed afterwards is smaller — so the frontier alone dedupes and
    /// no visited set is needed.
    fn rev_ancestry_until_clock<'a, I>(
        &'a self,
        seed: I,
        clock: &'a SeqClock,
    ) -> impl Iterator<Item = NodeIdx> + 'a
    where
        I: IntoIterator<Item = NodeIdx>,
    {
        let mut to_visit: BTreeSet<_> = seed.into_iter().collect();

        std::iter::from_fn(move || {
            let idx = to_visit.pop_last()?;
            for p in self.parents(idx) {
                let actor = self.actors[p.0 as usize].into();
                let seq = self.seq[p.0 as usize];
                if clock.get_for_actor(&actor) < NonZeroU32::new(seq) {
                    to_visit.insert(p);
                }
            }
            Some(idx)
        })
    }

    pub(crate) fn cache_fragments(&mut self) {
        // idempotent: enable_audit_mode re-runs this after upgrading the
        // graph, so start from scratch. Hash GC is skipped during the
        // bulk rebuild — a fresh load imports exactly the retained set.
        self.fragments.clear();
        self.fragment_top = SeqClock::new(self.num_actors());
        for n in 0..self.hashes.len() {
            self.cache_fragment_inner(NodeIdx(n as u32));
        }
    }

    /// Outside audit mode, a new fragment frees the hashes it now covers
    /// (its members become interior history; only heads, checkpoints,
    /// loose commits and anchors stay retained). Callers batching many
    /// [`Self::cache_fragment_inner`] calls run this once at the end —
    /// a GC firing mid-batch would free dep hashes that later changes
    /// in the same batch still resolve by hash.
    ///
    /// Under [`GcMode::Manual`] this only records that a GC is owed:
    /// freeing here would drop hashes that a later minimal
    /// `save_incremental` still needs to name its boundary. The owner
    /// runs [`Self::run_gc`] once it has saved.
    fn gc_retained_hashes(&mut self) {
        if self.gc_mode == crate::GcMode::Manual {
            self.gc_owed = true;
            return;
        }
        if !self.hashes.is_full() {
            self.retain_hashes_only();
        }
    }

    /// Run a deferred retention GC. Idempotent, and a no-op in audit
    /// mode, where nothing is freed at all.
    pub(crate) fn run_gc(&mut self) {
        self.gc_owed = false;
        if !self.hashes.is_full() {
            self.retain_hashes_only();
        }
    }

    pub(crate) fn gc_mode(&self) -> crate::GcMode {
        self.gc_mode
    }

    pub(crate) fn set_gc_mode(&mut self, mode: crate::GcMode) {
        self.gc_mode = mode;
    }

    /// Whether a deferred GC is pending — a fragment freed coverage
    /// while in [`GcMode::Manual`].
    pub(crate) fn gc_owed(&self) -> bool {
        self.gc_owed
    }

    /// Returns whether a fragment was cached (the caller owes a
    /// [`Self::gc_retained_hashes`] once its batch completes).
    fn cache_fragment_inner(&mut self, head: NodeIdx) -> bool {
        let Some(hash) = self.hashes.get(head) else {
            return false;
        };
        let level = hash.fragment_level();
        if level == 0 {
            return false;
        }
        let mut deps = vec![];
        let mut supercede = vec![];
        let clock = self.calculate_clock(vec![head]);
        for (i, f) in self.fragments.iter().enumerate().rev() {
            if clock.covers(&f.clock) {
                if f.level >= level {
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
        let node = FragmentNode {
            head,
            level,
            deps,
            clock,
        };
        // hold the index in apply order (see [`Self::fragments`]): a new
        // fragment joins its level's run, not the end of the array. Its
        // own level's fragments are all older, so it lands at that run's
        // end — but a *finer* fragment cached earlier sorts after it.
        let pos = self
            .fragments
            .partition_point(|f| f.sort_key() < node.sort_key());
        self.fragments.insert(pos, node);
        true
    }

    pub(crate) fn node_by_hash(&self, hash: &ChangeHash) -> Option<NodeIdx> {
        self.nodes_by_hash.get(hash).copied()
    }

    pub(crate) fn hash_for_node(&self, node: NodeIdx) -> Option<ChangeHash> {
        self.hashes.get(node)
    }

    /// Append the member changes of a change set without knowing their
    /// hashes.
    ///
    /// The members must be in topological order and each member's seq
    /// must extend its actor's chain — callers validate both. Only legal
    /// outside audit mode (audit-mode fragment application converts to
    /// changes instead); the new nodes have no hash, so they cannot
    /// appear in `nodes_by_hash`, `heads` or the fragment index yet.
    pub(crate) fn add_change_set_members(&mut self, members: Vec<ChangeSetMember<'_>>) {
        let base = NodeIdx(self.len() as u32);

        self.hashes.extend_without_hashes(members.len());

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
        for m in &members {
            self.extra_bytes_raw.extend_from_slice(&m.extra);
        }

        let mut parent_buf: Vec<NodeIdx> = Vec::new();
        for (i, m) in members.iter().enumerate() {
            let node_idx = base + i;
            self.max_op = std::cmp::max(self.max_op, m.max_op as u32);

            assert!(m.actor < self.seq_index.len());
            assert_eq!(self.seq_index[m.actor].len() + 1, m.seq as usize);
            self.seq_index[m.actor].push(node_idx);

            parent_buf.clear();
            parent_buf.extend(m.deps.iter().map(|d| match d {
                ChangeSetDep::Member(j) => {
                    debug_assert!(*j < i);
                    base + *j
                }
                ChangeSetDep::Node(n) => *n,
            }));
            for &parent in &parent_buf {
                // a parent that was a head is now covered
                if let Some(h) = self.hashes.get(parent) {
                    self.heads.remove(&h);
                }
            }
            self.push_parents(node_idx, parent_buf.iter().copied());
        }
        // one forward sweep over the appended range, instead of an
        // ancestry walk every CACHE_STEP nodes
        self.cache_clocks_from(base.0 as usize);
    }

    /// Append a change set's member changes straight from its columns.
    ///
    /// The columnar twin of [`Self::add_change_set_members`], and the same
    /// shape as [`ChangeGraphCols::load`]: each of the change set's change
    /// columns is decoded once, in one pass, into the graph's own column
    /// — no per-member struct, no dep `Vec` per member, no re-encode. The
    /// caller has already resolved the members' actors and sequence
    /// numbers (it needs them to decide which members to keep) and the
    /// external deps, so nothing here has to consult the document.
    ///
    /// Only valid when *every* member is being kept: a member's deps name
    /// other members by position, which is the node offset from `base`
    /// only if none were skipped. The partial (overlap) case goes through
    /// [`Self::add_change_set_members`].
    ///
    /// `ext_nodes` holds the resolved node of each of the change set's
    /// external deps, in the change set's dep order — a dep index at or above
    /// the member count indexes it.
    ///
    /// The columns are validated where they are read, and every read that
    /// can fail happens before the graph is touched: a malformed change set
    /// leaves the graph exactly as it was.
    pub(crate) fn add_change_set_members_cols(
        &mut self,
        cols: &crate::storage::ChangeSetChangeCols<'_>,
        member_actors: &[ActorIdx],
        member_seqs: &[NonZeroU64],
        actor_map: &[usize],
        ext_nodes: &[NodeIdx],
    ) -> Result<(), AutomergeError> {
        let bad = |s: &'static str| AutomergeError::MalformedChangeSet(s);
        let base = NodeIdx(self.len() as u32);
        let n = member_actors.len();
        debug_assert_eq!(n, member_seqs.len());

        // ── decode, validating ──────────────────────────────────────

        // The change set stores num_ops, timestamps, messages and the extra
        // widths in exactly the encodings the graph keeps them in, so
        // they are not decoded at all: load them as columns and splice
        // the slabs in below. An absent column loads as `n` defaults.
        let opts = hexane::LoadOpts::new().with_length(n);
        let num_ops = hexane::Column::<u64>::load_with(cols.num_ops, opts.with_fill(1u64))
            .map_err(|_| bad("invalid member op-count column"))?;
        let timestamps =
            hexane::DeltaColumn::<i64>::load_with(cols.timestamp, opts.with_fill(0i64))
                .map_err(|_| bad("invalid member timestamp column"))?;
        let messages =
            hexane::Column::<Option<String>>::load_with(cols.message, opts.with_fill(None))
                .map_err(|_| bad("invalid member message column"))?;
        let extra_meta =
            hexane::PrefixColumn::<ValueMeta>::load_with(cols.extra_meta, opts.with_fill(NO_EXTRA))
                .map_err(|_| bad("invalid member extra column"))?;
        // the extra bytes themselves are one contiguous run, so the raw
        // column is a single copy; its length is the meta column's total
        let extra_end = extra_meta.sum_range(0..n) as usize;
        if extra_end > cols.extra.len() {
            return Err(bad("member extra bytes overrun the column"));
        }

        // `max_ops` is a plain `Vec` in the graph (the clock walks index
        // it), so unlike the columns above it is decoded
        let mut max_ops = Vec::with_capacity(n);
        for m in cols.max_ops().take(n) {
            let Some(m) = m else {
                return Err(bad("short member max_op column"));
            };
            max_ops.push(m as u32);
        }
        if max_ops.len() != n {
            return Err(bad("short member max_op column"));
        }

        // deps: the wire form is already CSR — a count per member and a
        // flat value column — so the only work is turning each value into
        // a node index. Values below the member count name members of
        // this change set (their node is `base` + the value), the rest index
        // `ext_nodes`.
        let mut dep_range = Vec::with_capacity(n);
        let mut dep_target: Vec<NodeIdx> = Vec::new();
        let mut dep_values = cols.dep_values();
        let dep_base = self.dep_target.len() as u32;
        for (i, count) in pad(cols.dep_counts().map(|c| c.unwrap_or(0)), 0, n).enumerate() {
            let off = dep_base + dep_target.len() as u32;
            for _ in 0..count {
                let Some(Some(d)) = dep_values.next() else {
                    return Err(bad("short member dep column"));
                };
                let d = d as usize;
                let parent = if d < n {
                    // members arrive in topological order, which node
                    // index order has to preserve — the clock sweep and
                    // every ancestry walk read it that way
                    if d >= i {
                        return Err(bad("member dep is not an earlier member"));
                    }
                    base + d
                } else {
                    let Some(node) = ext_nodes.get(d - n).copied() else {
                        return Err(bad("member dep index out of range"));
                    };
                    node
                };
                dep_target.push(parent);
            }
            dep_range.push((off, dep_target.len() as u32 + dep_base - off));
        }

        // ── commit ──────────────────────────────────────────────────

        self.hashes.extend_without_hashes(n);
        self.actors.extend(
            member_actors
                .iter()
                .map(|a| ActorIdx::from(actor_map[usize::from(*a)])),
        );
        self.seq.extend(member_seqs.iter().map(|s| s.get() as u32));
        self.max_op = std::cmp::max(self.max_op, max_ops.iter().copied().max().unwrap_or(0));
        self.max_ops.extend(max_ops);

        // slab-level copies: no value is encoded twice, and with an empty
        // graph (a fresh document) the copy inverts and adopts the
        // change set's slabs outright
        let tail = hexane::Splice {
            pos: base.0 as usize,
            ..Default::default()
        };
        self.num_ops.copy_ranges(num_ops, [tail.clone()]);
        self.timestamps.copy_ranges(timestamps, [tail.clone()]);
        self.messages.copy_ranges(messages, [tail.clone()]);
        self.extra_bytes_meta.copy_ranges(extra_meta, [tail]);
        self.extra_bytes_raw
            .extend_from_slice(&cols.extra[..extra_end]);

        debug_assert_eq!(self.dep_range.len(), base.0 as usize);
        for parent in &dep_target {
            // a parent that was a head is now covered. Only nodes that
            // were already in the graph can be heads — a member of this
            // change set has no hash yet — so this skips the whole appended
            // range.
            if *parent < base {
                if let Some(h) = self.hashes.get(*parent) {
                    self.heads.remove(&h);
                }
            }
        }
        self.dep_range.extend(dep_range);
        self.dep_target.extend(dep_target);

        for i in 0..n {
            let actor = actor_map[usize::from(member_actors[i])];
            assert!(actor < self.seq_index.len());
            assert_eq!(
                self.seq_index[actor].len() + 1,
                member_seqs[i].get() as usize
            );
            self.seq_index[actor].push(base + i);
        }

        // one forward sweep over the appended range, instead of an
        // ancestry walk every CACHE_STEP nodes
        self.cache_clocks_from(base.0 as usize);
        Ok(())
    }

    /// Record the (unverified, until `enable_audit_mode`) hash of a
    /// node whose hash was unknown — a fragment head, checkpoint or
    /// boundary/dep pairing learned from an applied change set. Makes the
    /// hash resolvable; maintains the fragment index for fragment-level
    /// hashes. No-op on a checked graph or for post-load nodes, whose
    /// hashes are already known.
    /// Record a node's hash, returning whether that formed a new fragment
    /// — in which case the caller owes a [`Self::gc_after_batch`].
    ///
    /// Recording many hashes in one go — a change set's boundary, external
    /// deps, head and checkpoints — must not GC per hash: each pass is
    /// O(graph), so per-hash makes a fragment chain quadratic in the
    /// document. It is also what [`Self::gc_retained_hashes`] already
    /// asks batching callers to do.
    #[must_use = "the caller owes a gc_after_batch"]
    pub(crate) fn record_node_hash(&mut self, node: NodeIdx, hash: ChangeHash) -> bool {
        // idempotent: every fragment's boundary re-names earlier
        // fragment heads, so a chain apply records the same pairing
        // over and over — and re-caching a fragment head costs a full
        // O(graph) clock walk plus a duplicate fragment-index entry
        if let Some(known) = self.nodes_by_hash.get(&hash) {
            debug_assert_eq!(*known, node, "hash recorded for two nodes");
            return false;
        }
        match &mut self.hashes {
            // audit mode already knows every hash
            Hashes::Full(_) => return false,
            Hashes::Retained { map, .. } => {
                map.insert(node, hash);
            }
        }
        self.nodes_by_hash.insert(hash, node);
        self.cache_fragment_inner(node)
    }

    /// [`Self::record_node_hash`] for a fragment's head — the unique
    /// childless member — whose hash also joins the heads.
    #[must_use = "the caller owes a gc_after_batch"]
    pub(crate) fn record_fragment_head(&mut self, node: NodeIdx, hash: ChangeHash) -> bool {
        let cached = self.record_node_hash(node, hash);
        self.heads.insert(hash);
        cached
    }

    /// Run the retention GC a batch of deferred hash records owes.
    pub(crate) fn gc_after_batch(&mut self) {
        self.gc_retained_hashes();
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
                return Err(MissingDep.into());
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

    /// Write node `child_idx`'s parents. Must be called once per node, in
    /// ascending node order — the CSR layout only appends.
    fn push_parents(&mut self, child_idx: NodeIdx, parents: impl IntoIterator<Item = NodeIdx>) {
        debug_assert_eq!(
            self.dep_range.len(),
            child_idx.0 as usize,
            "nodes must receive their parents in order"
        );
        let off = self.dep_target.len() as u32;
        for parent_idx in parents {
            // a change is only ever added once its deps are in the graph,
            // so a parent's node index is always lower — node index order
            // is a topological order, which `rev_ancestry_until_clock`
            // walks by
            debug_assert!(parent_idx < child_idx, "parent added after its child");
            self.dep_target.push(parent_idx);
        }
        self.dep_range
            .push((off, self.dep_target.len() as u32 - off));
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

    /// A node's parents, as hashes — the ones still known. A fragment
    /// boundary names changes the receiver must already have, so a
    /// parent whose hash was freed is one the receiver identifies by
    /// dep id instead.
    pub(crate) fn parent_hashes(&self, node_idx: NodeIdx) -> Vec<ChangeHash> {
        self.parents(node_idx)
            .filter_map(|p| self.hashes.get(p))
            .collect()
    }

    fn parents(&self, node_idx: NodeIdx) -> impl Iterator<Item = NodeIdx> + '_ {
        self.parent_slice(node_idx).iter().copied()
    }

    /// Node `n`'s parents. Empty for a node whose edges have not been
    /// written yet — during a bulk append the node columns run ahead of
    /// `dep_range`, and only already-appended (lower) nodes are walked.
    fn parent_slice(&self, node_idx: NodeIdx) -> &[NodeIdx] {
        match self.dep_range.get(node_idx.0 as usize) {
            Some(&(off, count)) => &self.dep_target[off as usize..off as usize + count as usize],
            None => &[],
        }
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

    pub(crate) fn clock_for_nodes(&self, nodes: Vec<NodeIdx>) -> Clock {
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
        for target in &self.dep_target {
            has_child[target.0 as usize] = true;
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
        self.hashes = Hashes::Full(hashes);
        Ok(())
    }

    /// Populate `clock_cache` with the clock of every `CACHE_STEP`th node.
    ///
    /// One forward pass in index order: `clock(i)` is the merge of its
    /// parents' clocks plus its own `(actor, seq)` entry. A node's row is
    /// dead once its last child has consumed it, so the live rows are
    /// bounded by the width of the unmerged frontier, not the graph size.
    fn cache_clocks(&mut self) {
        self.cache_clocks_from(0)
    }

    /// [`Self::cache_clocks`] restricted to nodes `base..`, for a batch
    /// appended onto an existing graph.
    ///
    /// New nodes may depend on older ones, whose clocks are not in the
    /// pool — those are materialized once each, up front. A fragment
    /// attaches at a handful of points, so that is a few walks rather
    /// than one per cached node, which is what calling `cache_clock`
    /// inside the append loop cost.
    fn cache_clocks_from(&mut self, base: usize) {
        let n = self.len();
        if n < CACHE_STEP as usize || n <= base {
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

        const DEAD: u32 = u32::MAX;
        let mut slot_of = vec![DEAD; n]; // node -> pool slot while its row is live
        let mut pool: Vec<SeqClock> = Vec::new();
        let mut free: Vec<u32> = Vec::new();
        let mut parent_buf: Vec<usize> = Vec::new();

        // only children inside the swept range keep a row alive; a seeded
        // row from before `base` is pinned for the whole sweep
        let mut pending_children = vec![0u32; n];
        let mut seeds: Vec<usize> = Vec::new();
        for i in base..n {
            for p in self.parents(NodeIdx(i as u32)) {
                let p = p.0 as usize;
                pending_children[p] += 1;
                if p < base && slot_of[p] == DEAD {
                    slot_of[p] = 0; // mark; the real slot is assigned below
                    seeds.push(p);
                }
            }
        }
        for &p in &seeds {
            let clock = self.calculate_clock(vec![NodeIdx(p as u32)]);
            pool.push(clock);
            slot_of[p] = (pool.len() - 1) as u32;
            // pin: never freed, so it stays readable for every child
            pending_children[p] = u32::MAX;
        }

        for i in base..n {
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
                    let slot = if pending_children[first] == 1 && first >= base {
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
                if pending_children[p] == u32::MAX {
                    continue; // pinned seed row
                }
                pending_children[p] -= 1;
                if pending_children[p] == 0 && slot_of[p] != DEAD {
                    free.push(slot_of[p]);
                    slot_of[p] = DEAD;
                }
            }

            let actor = self.actors[i];
            pool[slot as usize].include(actor.into(), self.clock_data_for(idx));

            if (i as u32 + 1).is_multiple_of(CACHE_STEP) {
                self.clock_cache.insert(idx, pool[slot as usize].clone());
            }

            if pending_children[i] == 0 && i >= base {
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

    pub(crate) fn finalize(self, changes: &[Change]) -> ChangeGraph {
        let mut graph = self.graph;
        debug_assert_eq!(changes.len(), graph.len());
        debug_assert!(graph.hashes.len() == 0);
        // a full (audit) load: every hash is known
        graph.hashes = Hashes::Full(Vec::with_capacity(changes.len()));

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

        // The heads loaded from the document header are untrusted: replace
        // them with the computed heads (the hashes of the childless nodes).
        // Under `VerificationMode::Check` the caller verifies the two match;
        // under `DontCheck` this corrects a lying header.
        let mut has_child = vec![false; graph.len()];
        for target in &graph.dep_target {
            has_child[target.0 as usize] = true;
        }
        graph.heads = (0..graph.len() as u32)
            .filter(|n| !has_child[*n as usize])
            .filter_map(|n| graph.hashes.get(NodeIdx(n)))
            .collect();

        graph.cache_clocks();

        graph.cache_fragments();

        graph
    }

    /// Finish loading without computing any change hashes.
    ///
    /// The only hashes known are the document's heads, paired with their
    /// nodes via the document's head index suffix (`heads[i]` names node
    /// `head_indexes[i]`). The pairing is validated structurally (indexes
    /// in range, distinct, childless nodes) but the hashes themselves are
    /// unverified until `enable_audit_mode`.
    pub(crate) fn finalize_unchecked(
        self,
        heads: &[ChangeHash],
        head_indexes: &[u64],
    ) -> Result<ChangeGraph, BadHeadIndexes> {
        let mut graph = self.graph;
        debug_assert!(graph.hashes.len() == 0);

        if heads.len() != head_indexes.len() {
            return Err(BadHeadIndexes);
        }

        // the head nodes must be exactly the childless nodes
        let mut has_child = vec![false; graph.len()];
        for target in &graph.dep_target {
            has_child[target.0 as usize] = true;
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

        let len = graph.len();
        graph.hashes = Hashes::Retained { map: pre, len };

        graph.cache_clocks();

        // the retained set is fragment-sufficient by construction —
        // build the fragment index now
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

        // CSR straight off the wire: the format already stores deps as a
        // group column, so the offsets are the count column's running sum
        // and the targets are the value column verbatim — no expansion
        let mut dep_range: Vec<(u32, u32)> = Vec::with_capacity(len);
        let mut dep_target: Vec<NodeIdx> = Vec::new();

        let deps_count: Vec<u32> = hexane::decoder::<u32>(deps_count_bytes).collect();
        let mut deps_val_iter = hexane::DeltaDecoder::<u32>::new(deps_val_bytes);

        let mut num_ops_vec = Vec::with_capacity(len);
        for (i, d) in deps_count.iter().enumerate() {
            let d = *d as usize;
            if d == 0 {
                num_ops_vec.push(max_ops[i] as u64);
                dep_range.push((dep_target.len() as u32, 0));
                continue;
            }

            let off = dep_target.len() as u32;
            let mut last_max_op = 0;
            for _ in 0..d {
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
                last_max_op = std::cmp::max(last_max_op, max_ops[dep as usize]);
                dep_target.push(NodeIdx(dep));
            }
            dep_range.push((off, d as u32));
            if last_max_op > max_ops[i] {
                return Err(LoadError::InvalidMaxOp);
            }
            num_ops_vec.push(max_ops[i] as u64 - last_max_op as u64);
        }
        let num_ops: hexane::Column<u64> = num_ops_vec.into_iter().collect();

        let heads = doc.heads().iter().copied().collect();

        if dep_range.len() != len {
            return Err(LoadError::InvalidColumnLength(DEPS_COUNT_COL_SPEC));
        }

        // blank - to be filled out later
        let clock_cache = HashMap::default();
        let hashes = Hashes::default();
        let nodes_by_hash = HashMap::new();
        let fragments = vec![];
        let fragment_top = SeqClock::new(num_actors);

        Ok(ChangeGraphCols {
            graph: ChangeGraph {
                gc_mode: crate::GcMode::default(),
                gc_owed: false,
                hashes,
                actors,
                dep_range,
                dep_target,
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

/// A change names a dependency this document cannot resolve.
#[derive(Debug, thiserror::Error)]
#[error("attempted to derive a clock for a change with dependencies we don't have")]
pub struct MissingDep;

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
        collections::{BTreeMap, HashSet},
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
            let mut graph = ChangeGraph::new(0);
            // audit mode: the tests resolve hashes freely, and random
            // change hashes can otherwise form fragments and free them
            graph.hashes = Hashes::Full(Vec::new());
            TestGraphBuilder {
                actors: Vec::new(),
                changes: Vec::new(),
                graph,
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
            // audit mode, like the builder's own graph: the tests
            // resolve hashes freely, and random change hashes can
            // otherwise form fragments and free them
            graph.hashes = Hashes::Full(Vec::new());
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
                .map(|c| ChangeId::from_doc_seq(c.seq(), c.actor_id().clone(), 0))
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
        hash_of[&(id.actor().clone(), id.seq())]
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
            .map(|id| (id.actor().clone(), id.seq()))
            .collect();
        let heads: Vec<_> = graph.heads().collect();

        let fragments: Vec<_> = graph.fragments(&heads, .., &builder.actors);

        // Collect all member ids across all fragments
        // (members may appear in multiple fragments — this is expected)
        let mut covered: BTreeSet<(ActorId, u64)> = BTreeSet::new();
        for f in &fragments {
            for m in &f.members {
                covered.insert((m.actor().clone(), m.seq()));
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
        let fragments: Vec<_> = graph.fragments(&heads, .., &builder.actors);

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
            .map(|id| (id.actor().clone(), id.seq()))
            .collect();
        let heads: Vec<_> = graph.heads().collect();
        let fragments: Vec<_> = graph.fragments(&heads, .., &builder.actors);

        let mut covered: BTreeSet<(ActorId, u64)> = BTreeSet::new();
        for f in &fragments {
            for m in &f.members {
                covered.insert((m.actor().clone(), m.seq()));
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

    /// Fragments come back in apply order: every fragment's boundary,
    /// and every dep of its members, is covered by the fragment itself
    /// or by an earlier one. Concurrent branches are what makes this
    /// non-trivial — neither fragment-index order nor head node order
    /// gets it right on its own.
    #[test]
    fn fragments_are_returned_in_apply_order() {
        let mut builder = TestGraphBuilder::new();
        let actor1 = builder.actor();
        let actor2 = builder.actor();
        let actor3 = builder.actor();

        // long concurrent branches — long enough that a branch can grow
        // its own cached fragments before the merge
        let root = builder.change(&actor1, 1, &[]);
        let mut tips = [root, root, root];
        for i in 0..1_200 {
            tips[0] = builder.change(&actor1, 1, &[tips[0]]);
            tips[1] = builder.change(&actor2, 1, &[tips[1]]);
            tips[2] = builder.change(&actor3, 1, &[tips[2]]);
            if i % 400 == 399 {
                let merge = builder.change(&actor1, 1, &tips);
                tips = [merge, merge, merge];
            }
        }
        let graph = builder.build();
        let heads: Vec<_> = graph.heads().collect();
        let fragments = graph.fragments(&heads, .., &builder.actors);
        let hash_of = builder.hash_of();

        let mut applied: BTreeSet<ChangeHash> = BTreeSet::new();
        for f in &fragments {
            let members: BTreeSet<ChangeHash> =
                f.members.iter().map(|m| member_hash(&hash_of, m)).collect();
            for dep in &f.boundary {
                assert!(
                    applied.contains(dep) || members.contains(dep),
                    "fragment {:?} applies before its boundary {:?}",
                    f.head,
                    dep,
                );
            }
            for m in &members {
                for d in graph.deps(m) {
                    let d = d.unwrap();
                    assert!(
                        members.contains(&d) || applied.contains(&d),
                        "fragment {:?} member {:?} applies before its dep {:?}",
                        f.head,
                        m,
                        d,
                    );
                }
            }
            applied.extend(members);
        }
        assert_eq!(
            applied,
            builder.all_hashes().into_iter().collect::<BTreeSet<_>>(),
            "fragments do not cover every change",
        );
    }

    /// Probe: is the fragment index's own order (ascending head node
    /// index) already a valid apply order for the cached fragments?
    /// Needs a level-2 fragment (1 hash in 65536) on one branch while a
    /// level-1 fragment survives on a concurrent one.
    /// cargo test -p automerge --release --lib probe_fragment_index_order -- --ignored --nocapture
    #[test]
    #[ignore]
    fn probe_fragment_index_order() {
        use crate::transaction::Transactable;
        let n: u64 = std::env::var("PROBE_CHANGES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(200_000);

        let mut base = AutoCommit::new();
        for i in 0..2_000u64 {
            base.put(ROOT, "k", i as i64).unwrap();
            base.commit();
        }
        let saved = base.save();
        let mut a = AutoCommit::load(&saved).unwrap();
        let mut b = AutoCommit::load(&saved).unwrap();
        a.set_actor(ActorId::random());
        b.set_actor(ActorId::random());
        // merge is a hash-level operation
        a.enable_audit_mode().unwrap();
        b.enable_audit_mode().unwrap();
        for i in 0..2_000u64 {
            a.put(ROOT, "a", i as i64).unwrap();
            a.commit();
        }
        for i in 0..n {
            b.put(ROOT, "b", i as i64).unwrap();
            b.commit();
        }
        a.merge(&mut b).unwrap();

        // rebuild the index the way a load does: in node order
        let bytes = a.save();
        let doc = Automerge::load(&bytes).unwrap();
        let graph = &doc.change_graph;

        // what `fragments` returns: level descending, node index ascending
        let returned = doc.fragments(..).unwrap();
        let cached: Vec<_> = returned.iter().filter(|f| f.level > 0).cloned().collect();
        // the same fragments in node index order alone
        let mut by_node = cached.clone();
        by_node.sort_by_key(|f| graph.node_by_hash(&f.head).unwrap().0);

        // the index is no longer keyed on node index alone: every cached
        // fragment must still be findable
        for f in &cached {
            assert_eq!(
                graph.get_fragment(f.head, &doc.ops.actors).as_ref(),
                Some(f),
                "get_fragment missed a level-{} fragment",
                f.level,
            );
        }
        let mut levels: BTreeMap<usize, usize> = BTreeMap::new();
        for f in &returned {
            *levels.entry(f.level).or_default() += 1;
        }
        println!("fragments: {} levels: {levels:?}", returned.len());
        println!("index order == returned order: {}", by_node == cached);

        // every boundary, and every dep of every member, must already be
        // covered by this fragment or an earlier one
        let check = |order: &[Fragment]| {
            let mut applied: HashSet<NodeIdx> = HashSet::new();
            let mut violations = 0;
            for f in order {
                let members: HashSet<NodeIdx> = f
                    .members
                    .iter()
                    .map(|m| graph.node_for_change_id(m, &doc.ops.actors).unwrap())
                    .collect();
                let mut needs: Vec<NodeIdx> = f
                    .boundary
                    .iter()
                    .map(|d| graph.node_by_hash(d).unwrap())
                    .collect();
                for m in &members {
                    needs.extend(graph.parents(*m));
                }
                for n in needs {
                    if !applied.contains(&n) && !members.contains(&n) {
                        violations += 1;
                    }
                }
                applied.extend(members);
            }
            violations
        };
        println!("violations, node index order:     {}", check(&by_node));
        println!("violations, cached as returned:   {}", check(&cached));
        println!("violations, all as returned:      {}", check(&returned));
        assert_eq!(check(&returned), 0, "returned order is not an apply order");
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
        let fragments: Vec<_> = graph.fragments(&heads, .., &builder.actors);
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

        let all: Vec<_> = graph.fragments(&heads, .., &builder.actors);
        let loose: Vec<_> = graph.fragments(&heads, 0..=0, &builder.actors);
        let cached: Vec<_> = graph.fragments(&heads, 1.., &builder.actors);

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
        assert_eq!(graph.fragments(&heads, 0..0, &builder.actors).len(), 0);
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

        let loose: Vec<_> = graph.fragments(&heads, 0..=0, &builder.actors);
        let cached: Vec<_> = graph.fragments(&heads, 1.., &builder.actors);
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
    fn change_sets_for_fragments_roundtrips_through_load_incremental() {
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

        let change_sets = doc.change_sets_for_fragments(fragments).unwrap();

        let joined: Vec<u8> = change_sets.into_iter().flatten().collect();

        let mut loaded = AutoCommit::new();
        loaded.load_incremental(&joined).unwrap();

        assert_eq!(doc.get_heads(), loaded.get_heads());

        let a = doc.save();
        let b = loaded.save();
        assert_eq!(a, b);
    }

    /// Apply the same fragments through the walk and the manifold
    /// paths; the resulting documents must be byte-identical.
    #[test]
    fn fragment_apply_manifold_matches_walk() {
        use crate::read::ReadDoc;
        let mut rng = make_rng();
        let mut doc = AutoCommit::new().with_actor(rng.random());
        let text = doc.put_object(ROOT, "text", crate::ObjType::Text).unwrap();
        let map = doc.put_object(ROOT, "map", crate::ObjType::Map).unwrap();
        doc.put(&map, "c", crate::ScalarValue::counter(0)).unwrap();
        for i in 0..40 {
            if i % 8 == 0 {
                doc.commit();
            }
            let len = doc.length(&text);
            match rng.random_range(0..5u32) {
                0 if len > 1 => {
                    let at = rng.random_range(0..len as u32) as usize;
                    doc.splice_text(&text, at, 1, "").unwrap();
                }
                1 => {
                    let k = format!("k{}", rng.random_range(0..6u32));
                    doc.put(&map, k, rng.random_range(0..100i64)).unwrap();
                }
                2 => {
                    doc.increment(&map, "c", 1).unwrap();
                }
                _ => {
                    let at = rng.random_range(0..=len as u32) as usize;
                    doc.splice_text(&text, at, 0, "x").unwrap();
                }
            }
        }
        doc.commit();

        let fragments = doc.doc.fragments(..).unwrap();
        let change_sets: Vec<_> = fragments
            .iter()
            .map(|f| doc.doc.change_set_for_fragment(f).unwrap())
            .collect();

        let apply_all = || {
            let mut d = Automerge::new();
            for b in &change_sets {
                d.apply_change_set(b.clone()).unwrap();
            }
            d
        };

        let walk = apply_all();
        let manifold = apply_all();

        assert_eq!(walk.get_heads(), manifold.get_heads());
        assert_eq!(walk.save(), manifold.save(), "docs diverge");
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
    level: usize,
    deps: Vec<NodeIdx>,
    clock: SeqClock,
}

impl FragmentNode {
    /// The fragment index's order, which is also the apply order: level
    /// descending, then head node index ascending.
    fn sort_key(&self) -> (std::cmp::Reverse<usize>, NodeIdx) {
        (std::cmp::Reverse(self.level), self.head)
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
    /// The changes this fragment covers. Identified by [`ChangeId`]
    /// rather than hash so fragments can be produced outside audit
    /// mode, where interior change hashes may be freed.
    pub members: Vec<ChangeId>,
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
    // ColumnId 6 was the change-hash column group, written only on the
    // `hashless` branch: a document that carries it still parses, and
    // `validate` filters it out. Do not reuse the id.
}
