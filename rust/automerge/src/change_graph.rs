use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::num::NonZeroU32;
use std::ops::Add;

use hexane::{
    ColGroupIter, ColumnCursor, ColumnData, ColumnDataIter, DeltaCursor, PackError, StrCursor,
    UIntCursor,
};

use crate::storage::BundleMetadata;
use crate::{
    clock::{Clock, SeqClock},
    error::AutomergeError,
    op_set2::{change::BuildChangeMetadata, ActorCursor, ActorIdx, MetaCursor, ValueMeta},
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

#[derive(Debug, PartialEq, Default, Clone)]
pub(crate) struct ChangeGraph {
    edges: Vec<Edge>,
    hashes: Vec<ChangeHash>,
    actors: Vec<ActorIdx>,
    parents: Vec<Option<EdgeIdx>>,
    seq: Vec<u32>,
    max_ops: Vec<u32>,
    max_op: u32,
    num_ops: ColumnData<UIntCursor>,
    timestamps: ColumnData<DeltaCursor>,
    messages: ColumnData<StrCursor>,
    extra_bytes_meta: ColumnData<MetaCursor>,
    extra_bytes_raw: Vec<u8>,
    heads: BTreeSet<ChangeHash>,
    nodes_by_hash: HashMap<ChangeHash, NodeIdx>,
    clock_cache: HashMap<NodeIdx, SeqClock>,
    seq_index: Vec<Vec<NodeIdx>>,
    fragment_top: SeqClock,
    fragments: Vec<FragmentNode>,
}

pub(crate) struct ChangeGraphCols(ChangeGraph);

const CACHE_STEP: u32 = 16;

#[derive(Hash, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct NodeIdx(u32);

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

impl ChangeGraph {
    pub(crate) fn new(num_actors: usize) -> Self {
        Self {
            edges: Vec::new(),
            nodes_by_hash: HashMap::new(),
            hashes: Vec::new(),
            actors: Vec::new(),
            max_ops: Vec::new(),
            max_op: 0,
            num_ops: ColumnData::new(),
            seq: Vec::new(),
            parents: Vec::new(),
            messages: ColumnData::new(),
            timestamps: ColumnData::new(),
            extra_bytes_meta: ColumnData::new(),
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

    pub(crate) fn head_indexes(&self) -> impl Iterator<Item = u64> + '_ {
        self.heads
            .iter()
            .map(|h| self.nodes_by_hash.get(h).unwrap().0 as u64)
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

    pub(crate) fn index_to_hash(&self, index: usize) -> Option<&ChangeHash> {
        self.hashes.get(index)
    }

    pub(crate) fn max_op(&self) -> u64 {
        self.max_op as u64
    }

    pub(crate) fn max_op_for_actor(&mut self, actor_index: usize) -> u64 {
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

    fn deps_iter(&self) -> impl Iterator<Item = NodeIdx> + '_ {
        self.node_ids().flat_map(|n| self.parents(n))
    }

    fn num_deps(&self) -> impl Iterator<Item = usize> + '_ {
        self.node_ids().map(|n| self.parents(n).count())
    }

    fn node_ids(&self) -> impl Iterator<Item = NodeIdx> {
        let end = self.hashes.len() as u32;
        (0..end).map(NodeIdx)
    }

    pub(crate) fn encode(&self, out: &mut Vec<u8>) -> RawColumns<Uncompressed> {
        use ids::*;

        let actor_iter = self.actors.iter().map(as_actor);
        let actor = ActorCursor::encode_unless_empty(out, actor_iter);

        let seq_iter = self.seq.iter().map(as_seq);
        let seq = DeltaCursor::encode_unless_empty(out, seq_iter);

        let max_op_iter = self.max_ops.iter().map(as_max_op);
        let max_op = DeltaCursor::encode_unless_empty(out, max_op_iter);

        let time = self.timestamps.save_to_unless_empty(out);

        let message = self.messages.save_to_unless_empty(out);

        let num_deps_iter = self.num_deps().map(as_num_deps);
        let num_deps = UIntCursor::encode_unless_empty(out, num_deps_iter);

        let deps_iter = self.deps_iter().map(as_deps);
        let deps = DeltaCursor::encode_unless_empty(out, deps_iter);

        // FIXME - we could eliminate this column if empty but meta isnt all null
        let meta = self.extra_bytes_meta.save_to_unless_empty(out);
        let raw = out.len()..out.len() + self.extra_bytes_raw.len();
        out.extend(&self.extra_bytes_raw);

        [
            RawColumn::new(ACTOR_COL_SPEC, actor),
            RawColumn::new(SEQ_COL_SPEC, seq),
            RawColumn::new(MAX_OP_COL_SPEC, max_op),
            RawColumn::new(TIME_COL_SPEC, time),
            RawColumn::new(MESSAGE_COL_SPEC, message),
            RawColumn::new(DEPS_COUNT_COL_SPEC, num_deps),
            RawColumn::new(DEPS_VAL_COL_SPEC, deps),
            RawColumn::new(EXTRA_META_COL_SPEC, meta),
            RawColumn::new(EXTRA_VAL_COL_SPEC, raw),
        ]
        .into_iter()
        .collect()
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

    pub(crate) fn opid_to_hash(&self, id: OpId) -> Option<ChangeHash> {
        let actor_indices = self.seq_index.get(id.actor())?;
        let counter = id.counter();
        let index = actor_indices
            .binary_search_by(|n| {
                let i = n.0 as usize;
                let num_ops = *self.num_ops.get(i).flatten().unwrap_or_default();
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
        let node_idx = actor_indices[index];
        self.hashes.get(node_idx.0 as usize).cloned()
    }

    pub(crate) fn deps_for_hash(&self, hash: &ChangeHash) -> impl Iterator<Item = ChangeHash> + '_ {
        let node_idx = self.nodes_by_hash.get(hash);
        let mut edge_idx = node_idx.and_then(|n| self.parents[n.0 as usize]);
        std::iter::from_fn(move || {
            let this_edge_idx = edge_idx?;
            let edge = &self.edges[this_edge_idx.get()];
            edge_idx = edge.next;
            let hash = self.hashes[edge.target.0 as usize];
            Some(hash)
        })
    }

    pub(crate) fn has_change(&self, hash: &ChangeHash) -> bool {
        self.nodes_by_hash.contains_key(hash)
    }

    pub(crate) fn get_bundle_metadata<I>(
        &self,
        hashes: I,
    ) -> impl Iterator<Item = Result<BundleMetadata<'_>, MissingDep>>
    where
        I: IntoIterator<Item = ChangeHash>,
    {
        hashes.into_iter().map(|hash| {
            let index = self
                .nodes_by_hash
                .get(&hash)
                .cloned()
                .ok_or(MissingDep(hash))?;
            let i = index.0 as usize;
            let actor = self.actors[i].into();
            let timestamp = *self.timestamps.get(i).flatten().unwrap_or_default();
            let max_op = self.max_ops[i] as u64;
            let num_ops = *self.num_ops.get(i).flatten().unwrap_or_default();
            let message = self.messages.get(i).flatten();

            // FIXME - this needs a test
            let meta = self.extra_bytes_meta.get_with_acc(i).unwrap();
            let meta_range =
                meta.acc.as_usize()..(meta.acc.as_usize() + meta.item.unwrap().length());
            let extra = Cow::Borrowed(&self.extra_bytes_raw[meta_range]);

            let deps = self
                .parents(index)
                .map(|p| self.hashes[p.0 as usize])
                .collect::<Vec<_>>();

            let start_op = max_op - num_ops + 1;
            let seq = self.seq[i] as u64;
            Ok(BundleMetadata {
                hash,
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
    ) -> Result<Vec<BuildChangeMetadata<'_>>, MissingDep>
    where
        I: IntoIterator<Item = ChangeHash>,
    {
        let indexes: Vec<_> = hashes
            .into_iter()
            .map(|hash| {
                self.nodes_by_hash
                    .get(&hash)
                    .cloned()
                    .ok_or(MissingDep(hash))
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
            extra_bytes_meta: self.extra_bytes_meta.iter().with_acc(),
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
                let timestamp = *self.timestamps.get(i).flatten().unwrap_or_default();
                let max_op = self.max_ops[i] as u64;
                let num_ops = *self.num_ops.get(i).flatten().unwrap_or_default();
                let message = self.messages.get(i).flatten();

                // FIXME - this needs a test
                let meta = self.extra_bytes_meta.get_with_acc(i).unwrap();
                let meta_range =
                    meta.acc.as_usize()..(meta.acc.as_usize() + meta.item.unwrap().length());
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

    pub(crate) fn get_hashes(&self, have_deps: &[ChangeHash]) -> Cow<'_, [ChangeHash]> {
        if have_deps.is_empty() {
            Cow::Borrowed(&self.hashes)
        } else {
            let clock = self.seq_clock_for_heads(have_deps);
            Cow::Owned(
                self.get_build_indexes(clock)
                    .into_iter()
                    .filter_map(|node| self.hashes.get(node.0 as usize))
                    .copied()
                    .collect(),
            )
        }
    }

    pub(crate) fn get_build_metadata_clock(
        &self,
        have_deps: &[ChangeHash],
    ) -> Vec<BuildChangeMetadata<'_>> {
        let clock = self.seq_clock_for_heads(have_deps);
        let change_indexes = self.get_build_indexes(clock);
        self.get_build_metadata_for_indexes(change_indexes)
    }

    pub(crate) fn get_hash_for_actor_seq(
        &self,
        actor: usize,
        seq: u64,
    ) -> Result<ChangeHash, AutomergeError> {
        self.seq_index
            .get(actor)
            .and_then(|v| v.get(seq as usize - 1))
            .and_then(|i| self.hashes.get(i.0 as usize))
            .ok_or(AutomergeError::InvalidSeq(seq))
            .copied()
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
        self.messages
            .extend(iter.clone().map(|(c, _)| c.message().cloned()));
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
    ) -> Result<(), MissingDep> {
        let node = NodeIdx(self.hashes.len() as u32);

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

            for parent_hash in change.deps().iter() {
                self.add_parent(node_idx, parent_hash);
            }

            if (node_idx + 1).0 % CACHE_STEP == 0 {
                self.cache_clock(node_idx);
            }

            self.cache_fragment(node_idx);
        }
        Ok(())
    }

    pub(crate) fn fragments<'a>(
        &'a self,
        heads: &'a [ChangeHash],
    ) -> impl Iterator<Item = Fragment> + 'a {
        self.loose_fragments(heads)
            .chain(self.fragments.iter().rev().map(|f| f.export(self)))
    }

    fn loose_fragments<'a>(
        &'a self,
        heads: &'a [ChangeHash],
    ) -> impl Iterator<Item = Fragment> + 'a {
        let nodes = heads
            .iter()
            .filter(|h| h.fragment_level() == 0)
            .filter_map(|h| self.nodes_by_hash.get(h).copied());
        self.bfs_until_clock(nodes, &self.fragment_top)
            .map(move |n| {
                let id = self.hashes[n.0 as usize];
                assert_eq!(id.fragment_level(), 0);
                let deps = self.parents(n).map(|p| self.hashes[p.0 as usize]).collect();
                let content = vec![id];
                let level = id.fragment_level();
                Fragment {
                    id,
                    level,
                    deps,
                    content,
                }
            })
    }

    fn fragment_content<'a>(
        &'a self,
        node: NodeIdx,
        clock: &'a SeqClock,
    ) -> impl Iterator<Item = ChangeHash> + 'a {
        self.bfs_until_clock([node], clock)
            .map(|n| self.hashes[n.0 as usize])
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

    fn cache_fragments(&mut self) {
        for n in 0..self.hashes.len() {
            self.cache_fragment(NodeIdx(n as u32))
        }
    }

    fn cache_fragment(&mut self, id: NodeIdx) {
        let hash = &self.hashes[id.0 as usize];
        let level = hash.fragment_level();
        if level == 0 {
            return;
        }
        let mut deps = vec![];
        let mut supercede = vec![];
        let clock = self.calculate_clock(vec![id]);
        for (i, f) in self.fragments.iter().enumerate().rev() {
            if clock.covers(&f.clock) {
                if self.hashes[f.id.0 as usize].fragment_level() >= level {
                    deps.push(f.id);
                } else {
                    supercede.push(i);
                }
            }
        }
        for i in supercede {
            self.fragments.remove(i);
        }
        SeqClock::merge(&mut self.fragment_top, &clock);
        self.fragments.push(FragmentNode { id, deps, clock });
    }

    pub(crate) fn add_change(&mut self, change: &Change, actor: usize) -> Result<(), MissingDep> {
        let hash = change.hash();

        if self.nodes_by_hash.contains_key(&hash) {
            return Ok(());
        }

        for h in change.deps().iter() {
            if !self.nodes_by_hash.contains_key(h) {
                return Err(MissingDep(*h));
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

    fn add_parent(&mut self, child_idx: NodeIdx, parent_hash: &ChangeHash) {
        debug_assert!(self.nodes_by_hash.contains_key(parent_hash));
        let parent_idx = *self.nodes_by_hash.get(parent_hash).unwrap();
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

    pub(crate) fn deps(&self, hash: &ChangeHash) -> impl Iterator<Item = ChangeHash> + '_ {
        let mut iter = self.nodes_by_hash.get(hash).map(|node| self.parents(*node));
        std::iter::from_fn(move || {
            let next = iter.as_mut()?.next()?;
            self.hashes.get(next.0 as usize).copied()
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

    fn heads_to_nodes(&self, heads: &[ChangeHash]) -> Vec<NodeIdx> {
        heads
            .iter()
            .filter_map(|h| self.nodes_by_hash.get(h))
            .copied()
            .collect()
    }

    pub(crate) fn clock_for_heads(&self, heads: &[ChangeHash]) -> Clock {
        let nodes = self.heads_to_nodes(heads);
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

    pub(crate) fn seq_clock_for_heads(&self, heads: &[ChangeHash]) -> SeqClock {
        let nodes = self.heads_to_nodes(heads);
        self.calculate_clock(nodes)
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

        while let Some(idx) = to_visit.pop_last() {
            assert!(!visited.contains(&idx));
            assert!(visited.len() <= self.hashes.len());
            visited.insert(idx);

            let actor = self.actors[idx.0 as usize];
            let data = self.clock_data_for(idx);
            clock.include(actor.into(), data);

            if let Some(cached) = self.clock_cache.get(&idx) {
                SeqClock::merge(clock, cached);
            } else {
                to_visit.extend(self.parents(idx).filter(|p| !visited.contains(p)));
                if visited.len() > limit {
                    break;
                }
            }
        }
    }

    pub(crate) fn remove_ancestors(
        &self,
        changes: &mut BTreeSet<ChangeHash>,
        heads: &[ChangeHash],
    ) {
        let nodes = self.heads_to_nodes(heads);
        self.traverse_ancestors(nodes, |idx| {
            let hash = &self.hashes[idx.0 as usize];
            changes.remove(hash);
            true
        });
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
    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    pub(crate) fn iter(&self) -> ChangeIter<'_> {
        self.0.iter()
    }

    pub(crate) fn finalize(self, changes: &[Change]) -> ChangeGraph {
        let mut graph = self.0;
        debug_assert_eq!(changes.len(), graph.len());
        debug_assert!(graph.hashes.is_empty());

        for c in changes {
            let hash = c.hash();
            let node_idx = NodeIdx(graph.hashes.len() as u32);
            graph.nodes_by_hash.insert(hash, node_idx);
            graph.hashes.push(hash)
        }

        for n in 0..(graph.len() as u32) {
            if (n + 1) % CACHE_STEP == 0 {
                graph.cache_clock(NodeIdx(n));
            }
        }

        graph.cache_fragments();

        graph
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

        let actors = to_vec(ActorCursor::iter(actor_bytes))?;
        let max_ops = to_u32_vec(DeltaCursor::iter(max_op_bytes))?;
        let max_op = max_ops.iter().copied().max().unwrap_or(0);
        let seq = to_u32_vec(DeltaCursor::iter(seq_bytes))?;

        if let Some(a) = actors.iter().copied().map(usize::from).max() {
            if a >= num_actors {
                return Err(LoadError::InvalidActorId(a));
            }
        }

        let len = actors.len();

        let timestamps = ColumnData::load_unless_empty(time_bytes, len)?;
        let messages = ColumnData::load_unless_empty(message_bytes, len)?;
        let extra_bytes_meta = ColumnData::load_unless_empty(extra_meta_bytes, len)?;

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

        let deps_count = UIntCursor::iter(deps_count_bytes).map(to_u32);
        let mut deps_val = DeltaCursor::iter(deps_val_bytes).map(to_u32);

        let mut num_ops = Vec::with_capacity(len);
        for (i, d) in deps_count.enumerate() {
            let d = d? as usize;
            if d == 0 {
                num_ops.push(max_ops[i] as u64);
                parents.push(None);
                continue;
            }

            parents.push(Some(EdgeIdx::new(edges.len())));
            let mut last_max_op = 0;
            for e in 0..d {
                let dep = deps_val.next();
                let dep = dep.ok_or(LoadError::InvalidColumnLength(DEPS_VAL_COL_SPEC))??;
                let target = NodeIdx(dep);
                let next = EdgeIdx::new(edges.len() + 1);
                let next = if e + 1 == d { None } else { Some(next) };
                last_max_op = std::cmp::max(last_max_op, max_ops[dep as usize]);
                edges.push(Edge { target, next })
            }
            if last_max_op > max_ops[i] {
                return Err(LoadError::InvalidMaxOp);
            }
            num_ops.push(max_ops[i] as u64 - last_max_op as u64);
        }
        let num_ops = num_ops.into_iter().collect();

        let heads = doc.heads().iter().copied().collect();

        if parents.len() != len {
            return Err(LoadError::InvalidColumnLength(DEPS_COUNT_COL_SPEC));
        }

        // blank - to be filled out later
        let clock_cache = HashMap::default();
        let hashes = vec![];
        let nodes_by_hash = HashMap::new();
        let fragments = vec![];
        let fragment_top = SeqClock::new(num_actors);

        Ok(ChangeGraphCols(ChangeGraph {
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
        }))
    }
}

fn as_num_deps(num: usize) -> Option<Cow<'static, u64>> {
    Some(Cow::Owned(num as u64))
}

fn as_seq(seq: &u32) -> Option<Cow<'_, i64>> {
    Some(Cow::Owned(*seq as i64))
}

fn as_actor(actor_index: &ActorIdx) -> Option<Cow<'_, ActorIdx>> {
    Some(Cow::Borrowed(actor_index))
}

fn as_max_op(m: &u32) -> Option<Cow<'_, i64>> {
    Some(Cow::Owned(*m as i64))
}

fn as_deps(n: NodeIdx) -> Option<Cow<'static, i64>> {
    Some(Cow::Owned(n.0 as i64))
}

#[derive(Debug, thiserror::Error)]
#[error("attempted to derive a clock for a change with dependencies we don't have")]
pub struct MissingDep(ChangeHash);

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        time::{SystemTime, UNIX_EPOCH},
    };

    use crate::{
        op_set2::{change::build_change, op_set::ResolvedAction, OpSet, TxOp},
        types::{ObjMeta, OpId, OpType},
        ActorId, TextEncoding,
    };

    use super::*;

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

        let clock = graph.seq_clock_for_heads(&[change4]);
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
        graph.remove_ancestors(&mut changes, &heads);

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
        let all_hashes: BTreeSet<_> = builder.all_hashes().into_iter().collect();
        let heads: Vec<_> = graph.heads().collect();

        let fragments: Vec<_> = graph.fragments(&heads).collect();

        // Collect all content hashes across all fragments
        // (hashes may appear in multiple fragments — this is expected)
        let mut covered: BTreeSet<ChangeHash> = BTreeSet::new();
        for f in &fragments {
            for h in &f.content {
                covered.insert(*h);
            }
        }

        // Every change must appear in at least one fragment
        let missing: Vec<_> = all_hashes.difference(&covered).collect();
        assert!(
            missing.is_empty(),
            "changes not covered by any fragment: {:?}",
            missing,
        );
    }

    fn assert_fragment_invariants(fragments: &[Fragment]) {
        for f in fragments {
            // level must match the fragment_level of the id hash
            assert_eq!(
                f.level,
                f.id.fragment_level(),
                "fragment level mismatch for {:?}",
                f.id
            );

            // id must be in content
            assert!(
                f.content.contains(&f.id),
                "fragment id {:?} not found in its own content",
                f.id
            );

            // deps must be equal or higher level than the fragment
            for dep in &f.deps {
                assert!(
                    dep.fragment_level() >= f.level,
                    "fragment {:?} (level {}) has dep {:?} with lower level {}",
                    f.id,
                    f.level,
                    dep,
                    dep.fragment_level(),
                );
            }

            // content must not contain a hash with a higher level than the id
            for h in &f.content {
                assert!(
                    h.fragment_level() <= f.level,
                    "fragment {:?} (level {}) contains {:?} with higher level {}",
                    f.id,
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
        let fragments: Vec<_> = graph.fragments(&heads).collect();

        assert_fragment_invariants(&fragments);
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
        let all_hashes: BTreeSet<_> = builder.all_hashes().into_iter().collect();
        let heads: Vec<_> = graph.heads().collect();
        let fragments: Vec<_> = graph.fragments(&heads).collect();

        let mut covered: BTreeSet<ChangeHash> = BTreeSet::new();
        for f in &fragments {
            for h in &f.content {
                covered.insert(*h);
            }
        }

        let missing: Vec<_> = all_hashes.difference(&covered).collect();
        assert!(
            missing.is_empty(),
            "changes not covered by any fragment: {:?}",
            missing,
        );

        assert_fragment_invariants(&fragments);
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
        let fragments: Vec<_> = graph.fragments(&heads).collect();
        let fragment_ids: BTreeSet<_> = fragments.iter().map(|f| f.id).collect();

        for f in &fragments {
            for dep in &f.deps {
                assert!(
                    all_hashes.contains(dep),
                    "fragment {:?} has dep {:?} not in change graph",
                    f.id,
                    dep
                );
                // Deps of cached fragments (level > 0) should point to other fragment ids
                // Deps of loose fragments (level == 0) point to change-level parents
                if f.level > 0 {
                    assert!(
                        fragment_ids.contains(dep) || dep.fragment_level() == 0,
                        "cached fragment {:?} has dep {:?} that is not a fragment id",
                        f.id,
                        dep
                    );
                }
            }
        }
    }
}

fn to_vec<'a, I, T>(iter: I) -> Result<Vec<T>, PackError>
where
    I: Iterator<Item = Result<Option<Cow<'a, T>>, PackError>>,
    T: Copy + Default + 'a,
{
    iter.map(squish).collect()
}

fn squish<T>(i: Result<Option<Cow<'_, T>>, PackError>) -> Result<T, PackError>
where
    T: Copy + Default,
{
    match i {
        Err(e) => Err(e),
        Ok(Some(i)) => Ok(*i),
        Ok(None) => Ok(T::default()),
    }
}

fn to_u32<T>(i: Result<Option<Cow<'_, T>>, PackError>) -> Result<u32, PackError>
where
    T: TryInto<u32> + Copy + Default,
{
    match i {
        Err(e) => Err(e),
        Ok(Some(i)) => Ok((*i).try_into().unwrap_or(0)),
        Ok(None) => Ok(0),
    }
}

fn to_u32_vec<'a, I, T>(iter: I) -> Result<Vec<u32>, PackError>
where
    I: Iterator<Item = Result<Option<Cow<'a, T>>, PackError>>,
    T: TryInto<u32> + Copy + Default + 'a,
{
    iter.map(to_u32).collect()
}

pub(crate) struct ChangeIter<'a> {
    index: usize,
    actors: std::slice::Iter<'a, ActorIdx>,
    seq: std::slice::Iter<'a, u32>,
    max_ops: std::slice::Iter<'a, u32>,
    num_ops: ColumnDataIter<'a, UIntCursor>,
    timestamps: ColumnDataIter<'a, DeltaCursor>,
    messages: ColumnDataIter<'a, StrCursor>,
    extra_bytes_meta: ColGroupIter<'a, MetaCursor>,
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
        let num_ops = *self.num_ops.next().flatten().unwrap_or_default();
        let timestamp = *self.timestamps.next().flatten().unwrap_or_default();
        let message = self.messages.next().flatten();
        let start_op = max_op - num_ops + 1;

        let meta = self.extra_bytes_meta.next()?;
        let meta_range = meta.acc.as_usize()..(meta.acc.as_usize() + meta.item.unwrap().length());
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
        let num_ops = *self.num_ops.nth(0).flatten().unwrap_or_default();
        let timestamp = *self.timestamps.nth(0).flatten().unwrap_or_default();
        let message = self.messages.nth(0).flatten();
        let start_op = max_op - num_ops + 1;

        let meta = self.extra_bytes_meta.shift_acc(0)?;
        let meta_range = meta.acc.as_usize()..(meta.acc.as_usize() + meta.item.unwrap().length());
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
    id: NodeIdx,
    deps: Vec<NodeIdx>,
    clock: SeqClock,
}

impl FragmentNode {
    fn export(&self, graph: &ChangeGraph) -> Fragment {
        let id = graph.hashes[self.id.0 as usize];
        let level = id.fragment_level();
        let deps = self
            .deps
            .iter()
            .map(|d| graph.hashes[d.0 as usize])
            .collect();
        let clock = graph.calculate_clock(self.deps.clone());
        let content = graph.fragment_content(self.id, &clock).collect();
        Fragment {
            id,
            level,
            deps,
            content,
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct Fragment {
    pub id: ChangeHash,
    pub level: usize,
    pub deps: Vec<ChangeHash>,
    pub content: Vec<ChangeHash>,
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
}
