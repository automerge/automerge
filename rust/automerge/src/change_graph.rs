use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::num::{NonZeroU32, NonZeroU64};
use std::ops::Add;
use std::ops::{Range, RangeBounds};

use crate::author::Authors;
use crate::change_id::ChangeId;
use crate::hash_retention::{Full, HashRetention, Retained};
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
pub struct ChangeGraph<H: HashRetention = Retained> {
    hashes: H,
    actors: Vec<ActorIdx>,
    /// `dep_range[n]` is the `(offset, count)` of node `n`'s parents within
    /// `dep_target`. Plain `Vec`s, not hexane columns: `parents()` is hot in
    /// the clock-cache ancestry walks.
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
    gc_mode: crate::automerge::GcMode,
    gc_owed: bool,
}

#[derive(Debug)]
pub struct ChangeGraphCols {
    graph: ChangeGraph,
}

const CACHE_STEP: u32 = 16;

/// Sorted, distinct nodes. `position` is O(1) when they are contiguous,
/// otherwise a binary search.
#[derive(Debug)]
pub struct Members<'a> {
    nodes: &'a [NodeIdx],
    contiguous: bool,
}

impl<'a> Members<'a> {
    pub(crate) fn new(nodes: &'a [NodeIdx]) -> Self {
        debug_assert!(
            nodes.windows(2).all(|w| w[0] < w[1]),
            "members must be sorted and distinct"
        );
        let contiguous = match (nodes.first(), nodes.last()) {
            (Some(first), Some(last)) => (last.0 - first.0) as usize + 1 == nodes.len(),
            _ => true,
        };
        Members { nodes, contiguous }
    }

    pub(crate) fn nodes(&self) -> &'a [NodeIdx] {
        self.nodes
    }

    pub(crate) fn position(&self, n: NodeIdx) -> Option<usize> {
        if self.contiguous {
            let i = n.0.checked_sub(self.nodes.first()?.0)? as usize;
            (i < self.nodes.len()).then_some(i)
        } else {
            self.nodes.binary_search(&n).ok()
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ApplyLookup {
    Present,
    Absent,
    /// A different change already occupies the change's `(actor, seq)`
    Equivocation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HashLookup {
    Found(NodeIdx),
    Absent,
    /// The graph is unchecked, so it can't tell whether the hash is a change.
    Unknown,
}

struct ResolvedHashes {
    nodes: Vec<NodeIdx>,
    missing: Vec<ChangeHash>,
}

#[derive(Debug, thiserror::Error)]
#[error("this operation needs change hashes that are not retained, call enable_audit_mode() first")]
pub struct UncheckedHashes;

#[derive(Debug, thiserror::Error)]
#[error("the document's head indexes are invalid")]
pub struct BadHeadIndexes;

impl From<UncheckedHashes> for AutomergeError {
    fn from(_: UncheckedHashes) -> Self {
        AutomergeError::AuditModeRequired
    }
}

#[derive(Hash, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct NodeIdx(pub(crate) u32);

impl Add<usize> for NodeIdx {
    type Output = Self;

    fn add(self, other: usize) -> Self {
        NodeIdx(self.0 + other as u32)
    }
}

/// An elided column decodes as empty and stands for a column of defaults.
fn pad<T: Clone>(iter: impl Iterator<Item = T>, default: T, n: usize) -> impl Iterator<Item = T> {
    iter.chain(std::iter::repeat(default)).take(n)
}

const NO_EXTRA: ValueMeta = ValueMeta::bytes(0);

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

/// `Member` is a position in the change set's (topologically ordered) member list.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ChangeSetDep {
    Member(usize),
    Node(NodeIdx),
}

/// Ops above which a loose commit keeps its hash even under
/// [`NameHashes::Anchors`](crate::automerge::NameHashes::Anchors): omitting a
/// hash saves ~34 bytes but costs ~0.6us of rehash per op on load.
const REHASHABLE_OPS: u64 = 8;

/// Microseconds of extra load an omission may cost before
/// [`NameHashes::Anchors`](crate::automerge::NameHashes::Anchors) names the hashes instead.
/// The rehash sweeps the omitted changes' whole id span, so one straggler
/// can cost far more than the 34 bytes it saves.
const REHASH_BUDGET_US: u64 = 1_000;

/// Estimated rehash nanoseconds per op of id span swept.
const SWEEP_NS_PER_OP: u64 = 400;

/// Estimated rehash nanoseconds per op the omitted changes carry.
const CARRY_NS_PER_OP: u64 = 760;

impl<H: HashRetention> ChangeGraph<H> {
    pub(crate) fn new(num_actors: usize) -> Self {
        Self {
            gc_mode: crate::automerge::GcMode::default(),
            gc_owed: false,
            nodes_by_hash: HashMap::new(),
            hashes: H::default(),
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

    /// In the same order as [`Self::heads`].
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

    /// Ascending, which is topological order.
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

    pub(crate) fn current_clock(&self) -> Clock {
        Clock(
            (0..self.seq_index.len())
                .map(|a| self.max_op_for_actor(a) as u32)
                .collect(),
        )
    }

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

    fn is_covered_by(&self, n: NodeIdx, frontier: &SeqClock) -> bool {
        let i = n.0 as usize;
        let actor = usize::from(self.actors[i]);
        frontier.get_for_actor(&actor) >= NonZeroU32::new(self.seq[i])
    }

    pub(crate) fn is_below_fragment_top(&self, node: NodeIdx) -> bool {
        self.is_covered_by(node, &self.fragment_top)
    }

    /// The nodes that keep their hashes outside audit mode, heads excepted.
    fn retained_nodes(&self) -> BTreeSet<NodeIdx> {
        self.retained_from(
            self.retention_candidates(&self.fragment_top),
            &self.fragment_top,
        )
    }

    /// Loose commits are candidates even when hashless: they still anchor
    /// their parents.
    fn retention_candidates<'a>(
        &'a self,
        frontier: &'a SeqClock,
    ) -> impl Iterator<Item = NodeIdx> + 'a {
        let loose = self.uncovered_tails(frontier).flatten().copied();
        self.hashes.iter().map(|(n, _)| n).chain(loose)
    }

    /// Each actor's nodes above `frontier`.
    fn uncovered_tails<'a>(
        &'a self,
        frontier: &'a SeqClock,
    ) -> impl Iterator<Item = &'a [NodeIdx]> + 'a {
        self.seq_index.iter().enumerate().map(|(a, seqs)| {
            let covered = frontier
                .get_for_actor(&a)
                .map_or(0, |s| s.get() as usize)
                .min(seqs.len());
            &seqs[covered..]
        })
    }

    /// May include anchors and actor tips outside `candidates`.
    fn retained_from(
        &self,
        candidates: impl Iterator<Item = NodeIdx>,
        frontier: &SeqClock,
    ) -> BTreeSet<NodeIdx> {
        let mut keep = BTreeSet::new();
        for n in candidates {
            if self.fragment_level(n) > 0 {
                keep.insert(n);
            } else if !self.is_covered_by(n, frontier) {
                keep.insert(n);
                keep.extend(self.anchors_of(n, frontier));
            }
        }
        // committing as an actor names its latest change by hash
        keep.extend(self.seq_index.iter().filter_map(|c| c.last()).copied());
        keep
    }

    /// The covered level-0 parents of loose commit `n`, which its future
    /// fragment boundary names.
    fn anchors_of<'a>(
        &'a self,
        n: NodeIdx,
        frontier: &'a SeqClock,
    ) -> impl Iterator<Item = NodeIdx> + 'a {
        self.parents(n)
            .filter(|p| self.is_covered_by(*p, frontier) && self.fragment_level(*p) == 0)
    }

    /// 0 when the hash was freed, which never happens to a fragment head.
    fn fragment_level(&self, node: NodeIdx) -> usize {
        self.hashes.get(node).map_or(0, |h| h.fragment_level())
    }

    /// The hashes among `nodes` a receiver of them must retain, each paired
    /// with its position in `nodes`. Unless `names` is
    /// [`NameHashes::All`](crate::automerge::NameHashes::All), hashes the
    /// receiver can cheaply rehash are omitted.
    pub(crate) fn hashes_to_retain(
        &self,
        nodes: &[NodeIdx],
        names: crate::automerge::NameHashes,
    ) -> Vec<(usize, ChangeHash)> {
        let members = Members::new(nodes);
        let carried: Vec<NodeIdx> = self.leveled_members(&members).map(|(n, _)| n).collect();
        let frontier = self.calculate_clock(carried.clone());
        let loose = self.loose_members(&members, &frontier);
        let candidates = carried.into_iter().chain(loose);
        let retained = self.retained_from(candidates, &frontier);
        let omittable = |n: &NodeIdx| {
            !self.is_covered_by(*n, &frontier)
                && self.num_ops.get(n.0 as usize).unwrap_or_default() <= REHASHABLE_OPS
        };
        let name_all = names == crate::automerge::NameHashes::All
            || self.rehash_cost_us(retained.iter().filter(|n| omittable(n))) > REHASH_BUDGET_US;
        retained
            .into_iter()
            .filter(|n| name_all || !omittable(n))
            .filter_map(|n| Some((members.position(n)?, self.hashes.get(n)?)))
            .collect()
    }

    /// Walks whichever is smaller, the members or the uncovered tails, so a
    /// small change set never walks the whole document.
    fn loose_members(&self, members: &Members<'_>, frontier: &SeqClock) -> Vec<NodeIdx> {
        let tails: Vec<&[NodeIdx]> = self.uncovered_tails(frontier).collect();
        if tails.iter().map(|t| t.len()).sum::<usize>() <= members.nodes().len() {
            tails
                .into_iter()
                .flatten()
                .copied()
                .filter(|n| members.position(*n).is_some())
                .collect()
        } else {
            members
                .nodes()
                .iter()
                .copied()
                .filter(|n| !self.is_covered_by(*n, frontier))
                .collect()
        }
    }

    /// Fragment heads and checkpoints among `members`, in node order.
    pub(crate) fn leveled_members<'a>(
        &'a self,
        members: &'a Members<'a>,
    ) -> impl Iterator<Item = (NodeIdx, ChangeHash)> + 'a {
        self.hashes.leveled_members(members).into_iter()
    }

    /// Estimated microseconds a receiver spends rehashing `omitted`.
    fn rehash_cost_us<'n>(&self, omitted: impl Iterator<Item = &'n NodeIdx>) -> u64 {
        let (mut lo, mut hi, mut carried) = (u64::MAX, 0u64, 0u64);
        for n in omitted {
            let max_op = self.max_ops[n.0 as usize] as u64;
            let ops = self.num_ops.get(n.0 as usize).unwrap_or_default();
            lo = lo.min((max_op + 1).saturating_sub(ops));
            hi = hi.max(max_op);
            carried += ops;
        }
        if carried == 0 {
            return 0;
        }
        ((hi + 1 - lo) * SWEEP_NS_PER_OP + carried * CARRY_NS_PER_OP) / 1_000
    }

    fn retained_store(&self) -> Retained {
        let keep = self.retained_nodes();
        // not the node count: a GC in the middle of `add_changes` must leave
        // the hash push cursor where it was
        let len = self.hashes.len();
        // a covered head need not be in `keep`
        let map: HashMap<NodeIdx, ChangeHash> = self
            .hashes
            .iter()
            .filter(|(n, hash)| keep.contains(n) || self.heads.contains(hash))
            .collect();
        Retained::new(map, len)
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
        if self.hashes.has_every_hash() {
            HashLookup::Absent
        } else {
            HashLookup::Unknown
        }
    }

    /// Like [`Self::has_change`], but an unknown hash is resolved through the
    /// change's `(actor, seq)`: a covered slot whose hash was freed is
    /// trusted to be this change.
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
            Some(_) => ApplyLookup::Equivocation,
            None => ApplyLookup::Present,
        }
    }

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

    fn authors_in(
        &self,
        nodes: Range<usize>,
    ) -> impl Iterator<Item = (usize, crate::Author<'_>)> + '_ {
        // a prefix column's `get` walks from the start: iterate to stay linear
        self.extra_bytes_meta
            .iter_range(nodes.clone())
            .zip(nodes)
            .filter(|(_, i)| self.seq[*i] == 1)
            .filter_map(|(meta, i)| {
                let extra = &self.extra_bytes_raw[meta.prefix() as usize..meta.total() as usize];
                let author = crate::change::decode_author_footer(extra)?;
                Some((self.actors[i].into(), author))
            })
    }

    fn assign_authors(&self, nodes: Range<usize>, authors: &mut Authors) {
        for (actor, author) in self.authors_in(nodes) {
            authors.assign_author(author.into_owned(), actor);
        }
    }

    /// The authors `members` (`(actor index, seq, extra bytes)`) assign.
    /// Errors if a member names a second author for an actor.
    fn member_authors<'b>(
        members: impl Iterator<Item = (usize, u64, &'b [u8])>,
        authors: &Authors,
        actor_ids: &[crate::ActorId],
    ) -> Result<Vec<(usize, crate::Author<'b>)>, AutomergeError> {
        let mut claimed = HashSet::new();
        let mut assigned = Vec::new();
        for (actor, seq, extra) in members {
            if extra.is_empty() {
                continue;
            }
            let Some(author) = crate::change::decode_author_footer(extra) else {
                continue;
            };
            if authors.get_author_for_actor(actor).is_some() || !claimed.insert(actor) {
                return Err(AutomergeError::DuplicateAuthor(
                    author.into_owned(),
                    actor_ids[actor].clone(),
                    seq,
                ));
            }
            if seq == 1 {
                assigned.push((actor, author));
            }
        }
        Ok(assigned)
    }

    /// A dep outside `nodes` is written by hash, so its hash must be retained.
    pub(crate) fn write_change_set_changes<'a>(
        &'a self,
        nodes: &[NodeIdx],
        writer: &mut crate::storage::ChangeSetChangeWriter<'a>,
        mapper: &mut crate::op_set2::change::ActorMapper<'_>,
    ) -> Result<(), MissingDep> {
        debug_assert!(nodes.is_sorted());
        use crate::storage::DeltaRunGrouper;
        let members = Members::new(nodes);
        let (mut seq, mut max_op, mut deps) =
            <(DeltaRunGrouper, DeltaRunGrouper, DeltaRunGrouper)>::default();
        for range in node_ranges(nodes) {
            let n = range.len();
            for run in self.actors[range.clone()].chunk_by(|a, b| a == b) {
                mapper.process_actor(usize::from(run[0]));
                writer.actor.append_n(run[0], run.len());
            }
            DeltaRunGrouper::extend(
                &mut seq,
                &mut writer.seq,
                self.seq[range.clone()].iter().map(|s| *s as i64),
            );
            DeltaRunGrouper::extend(
                &mut max_op,
                &mut writer.max_op,
                self.max_ops[range.clone()].iter().map(|m| *m as i64),
            );
            for run in self.num_ops.iter_range(range.clone()).runs() {
                writer.num_ops.append_n(run.value, run.count);
            }
            for run in self.timestamps.iter_range(range.clone()).runs() {
                writer.timestamp.append_run(run);
            }
            for run in self.messages.iter_range(range.clone()).runs() {
                writer.message.append_n(run.value, run.count);
            }
            for run in self.extra_bytes_meta.iter_range(range.clone()).runs() {
                writer.extra_meta.append_n(run.value, run.count);
            }
            let start = self.extra_bytes_meta.get_prefix(range.start) as usize;
            let end = self.extra_bytes_meta.get_prefix(range.end) as usize;
            writer
                .extra
                .extend_from_slice(&self.extra_bytes_raw[start..end]);
            let mut counts = range
                .clone()
                .map(|i| self.parent_slice(NodeIdx(i as u32)).len());
            if let Some(mut count) = counts.next() {
                let mut run = 1;
                for c in counts {
                    if c == count {
                        run += 1;
                    } else {
                        writer.dep_count.append_n(count as u32, run);
                        (count, run) = (c, 1);
                    }
                }
                writer.dep_count.append_n(count as u32, run);
            }
            for i in range {
                for p in self.parent_slice(NodeIdx(i as u32)) {
                    let idx = match members.position(*p) {
                        Some(pos) => pos as i64,
                        None => writer.external_dep_index(self.hashes.get(*p).ok_or(MissingDep)?),
                    };
                    deps.push(&mut writer.deps, idx);
                }
            }
            writer.len += n;
        }
        seq.flush(&mut writer.seq);
        max_op.flush(&mut writer.max_op);
        deps.flush(&mut writer.deps);
        Ok(())
    }

    /// `(actor, seq, start_op, max_op)` for each node.
    pub(crate) fn op_spans<'a>(
        &'a self,
        nodes: &'a [NodeIdx],
    ) -> impl Iterator<Item = (usize, u64, u64, u64)> + 'a {
        node_ranges(nodes).into_iter().flat_map(move |range| {
            range
                .clone()
                .zip(self.num_ops.iter_range(range))
                .map(move |(i, num_ops)| {
                    let max_op = self.max_ops[i] as u64;
                    let start_op = max_op + 1 - num_ops;
                    (
                        usize::from(self.actors[i]),
                        self.seq[i] as u64,
                        start_op,
                        max_op,
                    )
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
            len: self.len(),
            extra_bytes_raw: &self.extra_bytes_raw,
            dep_range: &self.dep_range,
            dep_target: &self.dep_target,
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

    /// `seeds` and every ancestor whose hash was freed, ascending. Pass a
    /// whole run at once: separate calls re-walk shared ancestors.
    pub(crate) fn nodes_back_to_retained(
        &self,
        seeds: impl IntoIterator<Item = NodeIdx>,
    ) -> Vec<NodeIdx> {
        let mut members: BTreeSet<NodeIdx> = seeds.into_iter().collect();
        let mut pending: Vec<NodeIdx> = members.iter().copied().collect();
        while let Some(n) = pending.pop() {
            for p in self.parent_slice(n).to_vec() {
                if members.contains(&p) || self.hashes.get(p).is_some() {
                    continue;
                }
                members.insert(p);
                pending.push(p);
            }
        }
        members.into_iter().collect()
    }

    pub(crate) fn unhashed_retained_nodes(&self, delivered: Range<u32>) -> Vec<NodeIdx> {
        self.retained_from(delivered.map(NodeIdx), &self.fragment_top)
            .into_iter()
            .filter(|n| self.hashes.get(*n).is_none())
            .collect()
    }

    /// Whether every dep outside `nodes` still has its hash.
    pub(crate) fn boundary_is_nameable(&self, nodes: &[NodeIdx]) -> bool {
        if self.hashes.has_every_hash() {
            return true;
        }
        nodes.iter().all(|n| {
            self.parent_slice(*n)
                .iter()
                .all(|p| nodes.binary_search(p).is_ok() || self.hashes.get(*p).is_some())
        })
    }

    /// Later fragments are finer. Not binary-searchable: concurrent
    /// fragments interleave.
    fn smallest_fragment_covering(&self, deps: &SeqClock) -> Option<&FragmentNode> {
        self.fragments.iter().rev().find(|f| f.clock.covers(deps))
    }

    /// A boundary the GC has left unnameable, moved back to the deps of the
    /// smallest fragment covering it. `None` when nothing needs widening.
    pub(crate) fn widen_boundary_to_fragment(&self, deps: &SeqClock) -> Option<SeqClock> {
        if self.hashes.has_every_hash() {
            return None;
        }
        let f = self.smallest_fragment_covering(deps)?;
        Some(self.calculate_clock(f.deps.clone()))
    }

    /// The nodes `clock` does *not* cover, ascending.
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
        match (self.hashes.all(), have_deps.is_empty()) {
            (Some(all), true) => Ok(Cow::Borrowed(all)),
            (None, true) => Err(UncheckedHashes),
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
        authors: &mut Authors,
    ) -> Result<(), AddChangeError> {
        let node = NodeIdx(self.len() as u32);
        let mut new_fragment = false;

        self.add_nodes(iter.clone());
        self.assign_authors(node.0 as usize..self.len(), authors);

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

    pub(crate) fn change_id(&self, n: NodeIdx, actors: &[crate::ActorId]) -> ChangeId {
        let i = n.0 as usize;
        let actor_idx = usize::from(self.actors[i]);
        ChangeId::from_doc_seq(self.seq[i] as u64, actors[actor_idx].clone(), actor_idx)
    }

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

    pub(crate) fn opid_to_change_id(
        &self,
        id: OpId,
        actors: &[crate::ActorId],
    ) -> Option<ChangeId> {
        let node = self.opid_to_node(id)?;
        Some(self.change_id(node, actors))
    }

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

    /// Sorted, so documents with equal heads give identical lists.
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

    /// Ignores order and duplicates.
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
        let boundary = self
            .parents(n)
            .map(|p| self.hashes.get(p))
            .collect::<Option<Vec<_>>>()?;
        Some(self.export_fragment(head, 0, boundary, &[n], actors))
    }

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
        // interior hashes may be freed; checkpoint hashes never are
        let checkpoints = nodes
            .iter()
            .filter_map(|n| self.hashes.get(*n))
            .filter(|h| *h != head && h.fragment_level() > 0)
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

    /// The fragments covering `heads` at `levels`, in apply order: coarsest
    /// first, oldest first within a level.
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

    fn fragment_nodes(&self, node: NodeIdx, clock: &SeqClock) -> Vec<NodeIdx> {
        self.ancestry_until_clock([node], clock).collect()
    }

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

    /// Pops in descending node index and a parent's index is always lower
    /// than its child's, so the frontier alone dedupes.
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
        // no hash GC: a fresh load already holds exactly the retained set
        self.fragments.clear();
        self.fragment_top = SeqClock::new(self.num_actors());
        for n in 0..self.hashes.len() {
            self.cache_fragment_inner(NodeIdx(n as u32));
        }
    }

    /// Run once at the end of a batch: a GC mid-batch would free dep hashes
    /// that later changes in the batch still resolve by hash.
    fn gc_retained_hashes(&mut self) {
        if self.gc_mode == crate::automerge::GcMode::Manual {
            self.gc_owed = true;
            return;
        }
        H::collect_garbage(self);
    }

    pub(crate) fn run_gc(&mut self) {
        self.gc_owed = false;
        H::collect_garbage(self);
    }

    pub(crate) fn gc_mode(&self) -> crate::automerge::GcMode {
        self.gc_mode
    }

    pub(crate) fn set_gc_mode(&mut self, mode: crate::automerge::GcMode) {
        self.gc_mode = mode;
    }

    pub(crate) fn gc_owed(&self) -> bool {
        self.gc_owed
    }

    /// Whether a fragment was cached, in which case the caller owes a
    /// [`Self::gc_retained_hashes`].
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
        // keep apply order (see [`Self::fragments`])
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

    /// Record a node's hash, returning whether that formed a new fragment,
    /// in which case the caller owes a [`Self::gc_after_batch`]. Batch
    /// these: each GC is O(graph).
    #[must_use = "the caller owes a gc_after_batch"]
    pub(crate) fn record_node_hash(&mut self, node: NodeIdx, hash: ChangeHash) -> bool {
        // a chain apply re-records fragment heads; re-caching one would cost
        // an O(graph) clock walk and a duplicate index entry
        if let Some(known) = self.nodes_by_hash.get(&hash) {
            debug_assert_eq!(*known, node, "hash recorded for two nodes");
            return false;
        }
        // audit mode already knows every hash
        if !self.hashes.learn(node, hash) {
            return false;
        }
        self.nodes_by_hash.insert(hash, node);
        self.cache_fragment_inner(node)
    }

    /// [`Self::record_node_hash`], also adding the hash to the heads.
    #[must_use = "the caller owes a gc_after_batch"]
    pub(crate) fn record_fragment_head(&mut self, node: NodeIdx, hash: ChangeHash) -> bool {
        let cached = self.record_node_hash(node, hash);
        self.heads.insert(hash);
        cached
    }

    pub(crate) fn gc_after_batch(&mut self) {
        self.gc_retained_hashes();
    }

    pub(crate) fn add_change(
        &mut self,
        change: &Change,
        actor: usize,
        authors: &mut Authors,
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

        self.add_changes([(change, actor)].into_iter(), authors)
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

    fn push_parents(&mut self, child_idx: NodeIdx, parents: impl IntoIterator<Item = NodeIdx>) {
        debug_assert_eq!(
            self.dep_range.len(),
            child_idx.0 as usize,
            "nodes must receive their parents in order"
        );
        let off = self.dep_target.len() as u32;
        for parent_idx in parents {
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

    /// Skips parents whose hash was freed.
    pub(crate) fn parent_hashes(&self, node_idx: NodeIdx) -> Vec<ChangeHash> {
        self.parents(node_idx)
            .filter_map(|p| self.hashes.get(p))
            .collect()
    }

    fn parents(&self, node_idx: NodeIdx) -> impl Iterator<Item = NodeIdx> + '_ {
        self.parent_slice(node_idx).iter().copied()
    }

    /// Empty for a node whose edges aren't written yet: in a bulk append the
    /// node columns run ahead of `dep_range`.
    fn parent_slice(&self, node_idx: NodeIdx) -> &[NodeIdx] {
        match self.dep_range.get(node_idx.0 as usize) {
            Some(&(off, count)) => &self.dep_target[off as usize..off as usize + count as usize],
            None => &[],
        }
    }

    /// Skips hashes that are definitely not in this document.
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

        // The merge of every cached clock absorbed so far: a node it covers
        // is already accounted for, subtree and all. Without this pruning the
        // walk fans out exponentially on merge-heavy graphs.
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

    /// `Err` is a hash the graph held that the recomputed `hashes`
    /// contradict.
    pub(crate) fn verify_hashes(&self, hashes: &[ChangeHash]) -> Result<(), ChangeHash> {
        assert_eq!(hashes.len(), self.len(), "one hash per node");

        for idx in self.node_ids() {
            if let Some(known) = self.hashes.get(idx) {
                if hashes[idx.0 as usize] != known {
                    return Err(known);
                }
            }
        }

        let has_child = self.has_child_mask();
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

        Ok(())
    }

    pub(crate) fn into_full(self, hashes: Vec<ChangeHash>) -> ChangeGraph<Full> {
        assert_eq!(hashes.len(), self.len(), "one hash per node");
        let nodes_by_hash = hashes
            .iter()
            .enumerate()
            .map(|(i, h)| (*h, NodeIdx(i as u32)))
            .collect();
        let mut graph = self.with_hashes(Full::new(hashes));
        graph.nodes_by_hash = nodes_by_hash;
        graph.cache_fragments();
        graph
    }

    fn has_child_mask(&self) -> Vec<bool> {
        let mut has_child = vec![false; self.len()];
        for target in &self.dep_target {
            has_child[target.0 as usize] = true;
        }
        has_child
    }

    /// `nodes_by_hash` is the caller's to keep in step with `hashes`.
    fn with_hashes<H2: HashRetention>(self, hashes: H2) -> ChangeGraph<H2> {
        let ChangeGraph {
            hashes: _,
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
            fragment_top,
            fragments,
            gc_mode,
            gc_owed,
        } = self;
        ChangeGraph {
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
            fragment_top,
            fragments,
            gc_mode,
            gc_owed,
        }
    }

    /// Populate `clock_cache` with the clock of every `CACHE_STEP`th node.
    /// Memory is bounded by the graph's width, not its size.
    fn cache_clocks(&mut self) {
        self.cache_clocks_from(0)
    }

    /// [`Self::cache_clocks`] for nodes `base..` appended onto an existing
    /// graph. Costs one full clock walk per older node they depend on.
    fn cache_clocks_from(&mut self, base: usize) {
        let n = self.len();
        if n < CACHE_STEP as usize || n <= base {
            return;
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
        const PINNED: u32 = u32::MAX;
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
            pending_children[p] = PINNED;
        }

        for i in base..n {
            let idx = NodeIdx(i as u32);

            parent_buf.clear();
            for p in self.parents(idx) {
                let p = p.0 as usize;
                debug_assert!(p < i, "change graph is topologically ordered");
                parent_buf.push(p);
            }

            let slot = match parent_buf.split_first() {
                Some((&first, rest)) => {
                    let first_slot = slot_of[first];
                    debug_assert_ne!(first_slot, DEAD);
                    let slot = if pending_children[first] == 1 && first >= base {
                        // sole remaining child: reuse the row
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
                if pending_children[p] == PINNED {
                    continue;
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
                free.push(slot);
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

impl ChangeGraph<Retained> {
    /// Append a change set's members without their hashes. Members must be
    /// in topological order, each extending its actor's seq chain.
    pub(crate) fn add_change_set_members(
        &mut self,
        members: Vec<ChangeSetMember<'_>>,
        authors: &mut Authors,
        actor_ids: &[crate::ActorId],
    ) -> Result<(), AutomergeError> {
        let new_authors: Vec<_> = Self::member_authors(
            members.iter().map(|m| (m.actor, m.seq, m.extra.as_ref())),
            authors,
            actor_ids,
        )?
        .into_iter()
        .map(|(actor, author)| (actor, author.into_owned()))
        .collect();
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
                if let Some(h) = self.hashes.get(parent) {
                    self.heads.remove(&h);
                }
            }
            self.push_parents(node_idx, parent_buf.iter().copied());
        }
        self.cache_clocks_from(base.0 as usize);
        for (actor, author) in new_authors {
            authors.assign_author(author.into_owned(), actor);
        }
        Ok(())
    }

    /// [`Self::add_change_set_members`] straight from the change set's
    /// columns. Only valid when *every* member is kept. `ext_nodes` holds the
    /// node of each external dep, in the change set's dep order. A malformed
    /// change set leaves the graph untouched.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn add_change_set_members_cols(
        &mut self,
        cols: &crate::storage::ChangeSetChangeCols<'_>,
        member_actors: &[ActorIdx],
        member_seqs: &[NonZeroU64],
        actor_map: &[usize],
        ext_nodes: &[NodeIdx],
        authors: &mut Authors,
        actor_ids: &[crate::ActorId],
    ) -> Result<(), AutomergeError> {
        let bad = |s: &'static str| AutomergeError::MalformedChangeSet(s);
        let base = NodeIdx(self.len() as u32);
        let n = member_actors.len();
        debug_assert_eq!(n, member_seqs.len());

        // already in the graph's encodings: spliced in below without decoding
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
        let extra_end = extra_meta.sum_range(0..n) as usize;
        if extra_end > cols.extra.len() {
            return Err(bad("member extra bytes overrun the column"));
        }
        let new_authors = Self::member_authors(
            extra_meta.iter().take(n).enumerate().map(|(i, m)| {
                (
                    actor_map[usize::from(member_actors[i])],
                    member_seqs[i].get(),
                    &cols.extra[m.prefix() as usize..m.total() as usize],
                )
            }),
            authors,
            actor_ids,
        )?;

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

        self.hashes.extend_without_hashes(n);
        self.actors.extend(
            member_actors
                .iter()
                .map(|a| ActorIdx::from(actor_map[usize::from(*a)])),
        );
        self.seq.extend(member_seqs.iter().map(|s| s.get() as u32));
        self.max_op = std::cmp::max(self.max_op, max_ops.iter().copied().max().unwrap_or(0));
        self.max_ops.extend(max_ops);

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
            // appended members have no hash yet, so can't be heads
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

        self.cache_clocks_from(base.0 as usize);
        for (actor, author) in new_authors {
            authors.assign_author(author.into_owned(), actor);
        }
        Ok(())
    }

    pub(crate) fn retain_hashes_only(&mut self) {
        let store = self.retained_store();
        self.nodes_by_hash.retain(|_, n| store.get(*n).is_some());
        self.hashes = store;
    }
}

impl ChangeGraph<Full> {
    pub(crate) fn into_retained(self) -> ChangeGraph<Retained> {
        let store = self.retained_store();
        let mut graph = self.with_hashes(store);
        let ChangeGraph {
            hashes,
            nodes_by_hash,
            ..
        } = &mut graph;
        nodes_by_hash.retain(|_, n| hashes.get(*n).is_some());
        graph
    }

    pub(crate) fn hash_of(&self, node: NodeIdx) -> ChangeHash {
        self.hashes.hash(node)
    }
}

impl ChangeGraphCols {
    pub(crate) fn iter(&self) -> ChangeIter<'_> {
        self.graph.iter()
    }

    pub(crate) fn num_actors(&self) -> usize {
        self.graph.num_actors()
    }

    pub(crate) fn finalize(self, changes: &[Change], authors: &mut Authors) -> ChangeGraph<Full> {
        debug_assert!(self.graph.hashes.len() == 0);
        let mut graph = self
            .graph
            .with_hashes(Full::new(Vec::with_capacity(changes.len())));
        graph.assign_authors(0..graph.len(), authors);
        debug_assert_eq!(changes.len(), graph.len());

        // The encoded change columns only contain each change's maximum op.
        // `load()` estimates op counts from dependencies, but that is ambiguous
        // for an isolated actor whose first change can start above counter 1.
        // Reconstruction has the verified changes, so use their exact lengths.
        graph.num_ops = changes.iter().map(|change| change.len() as u64).collect();

        for (i, c) in changes.iter().enumerate() {
            let hash = c.hash();
            let node_idx = NodeIdx(i as u32);
            graph.nodes_by_hash.insert(hash, node_idx);
            graph.hashes.push(hash);
        }

        // the header's heads are untrusted
        let has_child = graph.has_child_mask();
        graph.heads = (0..graph.len() as u32)
            .filter(|n| !has_child[*n as usize])
            .filter_map(|n| graph.hashes.get(NodeIdx(n)))
            .collect();

        graph.cache_clocks();

        graph.cache_fragments();

        graph
    }

    /// Finish loading without computing change hashes: only the heads are
    /// known (`heads[i]` names node `head_indexes[i]`), and they are
    /// unverified.
    pub(crate) fn finalize_unchecked(
        self,
        heads: &[ChangeHash],
        head_indexes: &[u64],
        authors: &mut Authors,
    ) -> Result<ChangeGraph<Retained>, BadHeadIndexes> {
        let mut graph = self.graph;
        graph.assign_authors(0..graph.len(), authors);
        debug_assert!(graph.hashes.len() == 0);

        if heads.len() != head_indexes.len() {
            return Err(BadHeadIndexes);
        }

        let has_child = graph.has_child_mask();
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
                return Err(BadHeadIndexes);
            }
            graph.nodes_by_hash.insert(*hash, node);
        }

        let len = graph.len();
        graph.hashes = Retained::new(pre, len);

        graph.cache_clocks();

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
                // untrusted input: everything downstream assumes parents precede children
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
        let hashes = Retained::default();
        let nodes_by_hash = HashMap::new();
        let fragments = vec![];
        let fragment_top = SeqClock::new(num_actors);

        Ok(ChangeGraphCols {
            graph: ChangeGraph {
                gc_mode: crate::automerge::GcMode::default(),
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
    use std::collections::{BTreeMap, HashSet};

    use crate::{
        make_rng,
        op_set2::{change::build_change, op_set::ResolvedAction, OpSet, TxOp},
        tx::Transactable,
        types::{ObjMeta, OpId, OpType},
        ActorId, TextEncoding, ROOT,
    };

    use crate::autocommit::AutoCommit;

    use crate::automerge::Automerge;
    use rand::RngExt;

    use super::*;

    #[test]
    fn cache_clocks_sweep_matches_backward_walk() {
        let mut builder = TestGraphBuilder::new();
        let a = builder.actor();
        let b = builder.actor();
        let c = builder.actor();

        // long single-actor chains exercise the row-reuse path
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
        graph: ChangeGraph<Full>,
        seqs_by_actor: BTreeMap<ActorId, u64>,
        rng: rand::rngs::SmallRng,
    }

    impl TestGraphBuilder {
        fn new() -> Self {
            // random hashes can form fragments, which would free hashes the tests resolve
            let graph = ChangeGraph::<Full>::new(0);
            TestGraphBuilder {
                actors: Vec::new(),
                changes: Vec::new(),
                graph,
                seqs_by_actor: BTreeMap::new(),
                rng: crate::make_rng(),
            }
        }

        fn actor(&mut self) -> ActorId {
            use rand::RngExt;
            let actor = ActorId::from(self.rng.random::<[u8; 16]>().to_vec());
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

            let timestamp = 0;
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
            self.graph
                .add_change(&change, actor_idx, &mut Authors::default())
                .unwrap();
            self.changes.push(change);
            hash
        }

        fn build(&self) -> ChangeGraph<Full> {
            let mut graph = ChangeGraph::<Full>::new(self.actors.len());
            let mut authors = Authors::with_actors(self.actors.len());
            for change in &self.changes {
                let actor_idx = self.index(change.actor_id());
                graph.add_change(change, actor_idx, &mut authors).unwrap();
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

        fn hash_of(&self) -> BTreeMap<(ActorId, u64), ChangeHash> {
            self.changes
                .iter()
                .map(|c| ((c.actor_id().clone(), c.seq()), c.hash()))
                .collect()
        }
    }

    #[test]
    fn member_authors_rejects_a_second_author() {
        let actors = vec![ActorId::from(&[1][..]), ActorId::from(&[2][..])];
        let x = crate::Author::from(vec![1, 1]);
        let y = crate::Author::from(vec![2, 2]);
        let footer = |a: &crate::Author<'static>| {
            crate::change::encode_author_footer(&Some(a.clone())).into_owned()
        };
        let (fx, fy) = (footer(&x), footer(&y));

        // non-footer extra bytes are not claims
        let authors = Authors::with_actors(2);
        let ok = [(0, 1, &fx[..]), (1, 1, &fy[..]), (0, 2, &[9, 9][..])];
        <ChangeGraph>::member_authors(ok.into_iter(), &authors, &actors).unwrap();

        // the actor already has an author
        let mut authors = Authors::with_actors(2);
        authors.assign_author(x.clone(), 0);
        let err = <ChangeGraph>::member_authors([(0, 2, &fy[..])].into_iter(), &authors, &actors)
            .unwrap_err();
        assert!(
            matches!(err, AutomergeError::DuplicateAuthor(a, actor, 2) if a == y && actor == actors[0])
        );

        // two members of the change set claim the same actor
        let authors = Authors::with_actors(2);
        let twice = [(1, 1, &fx[..]), (1, 2, &fy[..])];
        let err = <ChangeGraph>::member_authors(twice.into_iter(), &authors, &actors).unwrap_err();
        assert!(matches!(err, AutomergeError::DuplicateAuthor(_, actor, 2) if actor == actors[1]));
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

            // Checkpoints must exclude both the head and the boundary.
            assert!(!f.checkpoints.contains(&f.head));
            for boundary in &f.boundary {
                assert!(!f.checkpoints.contains(boundary));
            }
            for checkpoint in &f.checkpoints {
                assert!(f
                    .members
                    .iter()
                    .any(|m| member_hash(hash_of, m) == *checkpoint));
                assert!(checkpoint.fragment_level() > 0);
            }

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
    fn fragment_checkpoints_exclude_head_and_boundary() {
        // Fix the actor and change contents so fragment levels are deterministic.
        let mut doc = AutoCommit::new().with_actor(ActorId::from(&[1][..]));
        for value in 0..1500 {
            doc.put(ROOT, "counter", value).unwrap();
            doc.commit();
        }

        let fragments = doc.fragments(1..);
        assert!(!fragments.is_empty(), "expected bundled fragments");
        assert!(
            fragments.iter().any(|f| !f.boundary.is_empty()),
            "expected a fragment with a boundary to exercise boundary exclusion",
        );
        for fragment in fragments {
            let head_id = doc.hash_to_change_id(&fragment.head).unwrap().unwrap();
            assert!(fragment.members.contains(&head_id));
            assert!(
                !fragment.checkpoints.contains(&fragment.head),
                "fragment {:?} contains its own head as a checkpoint",
                fragment.head,
            );
            for boundary in &fragment.boundary {
                assert!(
                    !fragment.checkpoints.contains(boundary),
                    "fragment {:?} contains boundary {:?} as a checkpoint",
                    fragment.head,
                    boundary,
                );
            }
            assert_eq!(doc.get_fragment(fragment.head), Some(fragment));
        }
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

    /// Concurrent branches are what make apply order non-trivial.
    #[test]
    fn fragments_are_returned_in_apply_order() {
        let mut builder = TestGraphBuilder::new();
        let actor1 = builder.actor();
        let actor2 = builder.actor();
        let actor3 = builder.actor();

        // long enough for a branch to grow its own fragments before the merge
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

    /// Is ascending head node index alone a valid apply order?
    /// cargo test -p automerge --release --lib probe_fragment_index_order -- --ignored --nocapture
    #[test]
    #[ignore]
    fn probe_fragment_index_order() {
        use crate::tx::Transactable;
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
        let mut a = a.enable_audit_mode().unwrap();
        let mut b = b.enable_audit_mode().unwrap();
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

        let returned = doc.fragments(..);
        let cached: Vec<_> = returned.iter().filter(|f| f.level > 0).cloned().collect();
        let mut by_node = cached.clone();
        by_node.sort_by_key(|f| graph.node_by_hash(&f.head).unwrap().0);

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
        assert!(
            !cached.is_empty(),
            "expected at least one cached fragment from 5000 changes",
        );
        // ~1 run in 256 the head hash itself has a leading zero byte:
        // the head is then a cached fragment head, every change is
        // covered, and an empty loose set is the correct answer
        if heads.iter().all(|h| h.fragment_level() > 0) {
            assert!(loose.is_empty(), "cached head must cover everything");
        } else {
            assert!(!loose.is_empty());
        }

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
    fn fragments_with_cached_head() {
        // force the rare branch the two tests above only hit by luck:
        // rebuild small graphs until the head hash has a leading zero
        // byte (expected ~256 tries), then loose must be empty and the
        // cached fragments must cover every change
        for _ in 0..10_000 {
            let mut builder = TestGraphBuilder::new();
            let actor = builder.actor();
            let mut prev = vec![];
            for _ in 0..8 {
                let h = builder.change(&actor, 1, &prev);
                prev = vec![h];
            }
            if prev[0].fragment_level() == 0 {
                continue;
            }
            let graph = builder.build();
            let heads: Vec<_> = graph.heads().collect();

            let loose: Vec<_> = graph.fragments(&heads, 0..=0, &builder.actors);
            assert!(loose.is_empty(), "cached head must cover everything");

            let cached: Vec<_> = graph.fragments(&heads, 1.., &builder.actors);
            let hash_of = builder.hash_of();
            let covered: BTreeSet<ChangeHash> = cached
                .iter()
                .flat_map(|f| f.members.iter().map(|m| member_hash(&hash_of, m)))
                .collect();
            let all: BTreeSet<ChangeHash> = builder.all_hashes().into_iter().collect();
            assert_eq!(covered, all, "cached fragments must cover all changes");
            return;
        }
        panic!("no level>=1 head hash in 10k tries (p ~ 1e-17)");
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
        assert!(!cached.is_empty(), "expected at least one cached fragment");

        // get_fragment on a loose (level 0) commit hash returns an
        // equivalent Fragment. Loose can be legitimately empty (~1 run
        // in 256) when the head hash itself is a fragment head
        if let Some(l) = loose.first() {
            let got = graph.get_fragment(l.head, &builder.actors).unwrap();
            assert_eq!(got, *l);
        } else {
            assert!(heads.iter().all(|h| h.fragment_level() > 0));
        }

        // get_fragment on a cached (level >= 1) fragment id returns an equivalent Fragment
        let c = &cached[0];
        let got = graph.get_fragment(c.head, &builder.actors).unwrap();
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

        let fragments = doc.fragments(..);

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

        let fragments = doc.doc.fragments(..);
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
        self.len - self.index
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
    len: usize,
    extra_bytes_raw: &'a [u8],
    dep_range: &'a [(u32, u32)],
    dep_target: &'a [NodeIdx],
}

impl ChangeIter<'_> {
    fn deps(&self, i: usize) -> Vec<u64> {
        match self.dep_range.get(i) {
            Some(&(off, count)) => self.dep_target[off as usize..(off + count) as usize]
                .iter()
                .map(|n| n.0 as u64)
                .collect(),
            None => Vec::new(),
        }
    }
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
        let extra = Cow::Borrowed(&self.extra_bytes_raw[meta_range]);
        let deps = self.deps(i);
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
        let extra = Cow::Borrowed(&self.extra_bytes_raw[meta_range]);

        let deps = self.deps(i);

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
    /// Also the apply order.
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
    /// Non-zero-level members of the fragment, excluding its head.
    pub checkpoints: Vec<ChangeHash>,
    /// By [`ChangeId`], since interior change hashes may have been freed.
    pub members: Vec<ChangeId>,
}

/// The maximal runs of consecutive indexes in sorted `nodes`.
fn node_ranges(nodes: &[NodeIdx]) -> Vec<Range<usize>> {
    let mut ranges: Vec<Range<usize>> = Vec::new();
    for n in nodes {
        let i = n.0 as usize;
        match ranges.last_mut() {
            Some(r) if r.end == i => r.end = i + 1,
            _ => ranges.push(i..i + 1),
        }
    }
    ranges
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
    // ColumnId 6 (a retired change-hash column) still parses and `validate`
    // filters it out: don't reuse it.
}
