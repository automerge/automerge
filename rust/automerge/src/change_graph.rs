use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};

use packer::{ColumnCursor, DeltaCursor, RawCursor, StrCursor, UIntCursor};

use crate::{
    clock::{Clock, ClockData},
    columnar::column_range::{DepsRange, ValueRange},
    error::AutomergeError,
    op_set2::{change::BuildChangeMetadata, ActorCursor, ActorIdx, MetaCursor, ValueMeta},
    storage::{Columns, DocChangeColumns},
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
    hashes: Vec<ChangeHash>,
    actors: Vec<ActorIdx>,
    seq: Vec<u64>,
    max_ops: Vec<u64>,
    num_ops: Vec<u32>,
    parents: Vec<Option<EdgeIdx>>,
    timestamps: Vec<i64>,
    messages: Vec<Option<String>>,
    extra_bytes: Vec<Vec<u8>>,
    heads: BTreeSet<ChangeHash>,
    nodes_by_hash: HashMap<ChangeHash, NodeIdx>,
    clock_cache: Vec<Clock>,
    seq_index: Vec<Vec<NodeIdx>>,
}

const CACHE_STEP: u32 = 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct NodeIdx(u32);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct EdgeIdx(u32);

#[derive(Debug, Clone)]
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
            num_ops: Vec::new(),
            seq: Vec::new(),
            parents: Vec::new(),
            messages: Vec::new(),
            timestamps: Vec::new(),
            extra_bytes: Vec::new(),
            heads: BTreeSet::new(),
            clock_cache: Vec::new(),
            seq_index: vec![vec![]; num_actors],
        }
    }

    pub(crate) fn with_capacity(changes: usize, deps: usize, num_actors: usize) -> Self {
        Self {
            edges: Vec::with_capacity(deps),
            nodes_by_hash: HashMap::new(),
            hashes: Vec::with_capacity(changes),
            actors: Vec::with_capacity(changes),
            max_ops: Vec::with_capacity(changes),
            num_ops: Vec::with_capacity(changes),
            seq: Vec::with_capacity(changes),
            parents: Vec::with_capacity(changes),
            messages: Vec::with_capacity(changes),
            timestamps: Vec::with_capacity(changes),
            extra_bytes: Vec::with_capacity(changes),
            heads: BTreeSet::new(),
            clock_cache: Vec::with_capacity(changes / CACHE_STEP as usize + 1),
            seq_index: vec![vec![]; num_actors],
        }
    }

    pub(crate) fn actor_ids(&self) -> impl Iterator<Item = usize> + '_ {
        self.seq_index
            .iter()
            .enumerate()
            .filter_map(|(i, v)| if !v.is_empty() { Some(i) } else { None })
    }

    pub(crate) fn heads(&self) -> impl Iterator<Item = ChangeHash> + '_ {
        self.heads.iter().cloned()
    }

    pub(crate) fn head_indexes(&self) -> impl Iterator<Item = u64> + '_ {
        self.heads
            .iter()
            .map(|h| self.nodes_by_hash.get(h).unwrap().0 as u64)
    }

    pub(crate) fn insert_actor(&mut self, idx: usize) {
        if self.seq_index.len() != idx {
            for actor_index in &mut self.actors {
                if actor_index.0 >= idx as u32 {
                    actor_index.0 += 1;
                }
            }
            // FIXME - this could get expensive - lookout
            for clock in &mut self.clock_cache {
                clock.rewrite_with_new_actor(idx)
            }
        }
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
        for clock in &mut self.clock_cache {
            clock.remove_actor(idx)
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.hashes.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.hashes.is_empty()
    }

    pub(crate) fn hash_to_index(&self, hash: &ChangeHash) -> Option<usize> {
        self.nodes_by_hash.get(hash).map(|n| n.0 as usize)
    }

    pub(crate) fn index_to_hash(&self, index: usize) -> Option<&ChangeHash> {
        self.hashes.get(index)
    }

    pub(crate) fn max_op_for_actor(&mut self, actor_index: usize) -> u64 {
        self.seq_index
            .get(actor_index)
            .and_then(|s| s.last())
            .and_then(|index| self.max_ops.get(index.0 as usize).cloned())
            .unwrap_or(0)
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

    pub(crate) fn encode(&self, out: &mut Vec<u8>) -> DocChangeColumns {
        let actor_iter = self.actors.iter().map(as_actor);
        let actor = ActorCursor::encode(out, actor_iter, false).into();

        let seq_iter = self.seq.iter().map(as_seq);
        let seq = DeltaCursor::encode(out, seq_iter, false).into();

        let max_op_iter = self.max_ops.iter().map(as_max_op);
        let max_op = DeltaCursor::encode(out, max_op_iter, false).into();

        let time_iter = self.timestamps.iter().map(as_timestamp);
        let time = DeltaCursor::encode(out, time_iter, false).into();

        let message_iter = self.messages.iter().map(as_message);
        let message = StrCursor::encode(out, message_iter, false).into();

        let num_deps_iter = self.num_deps().map(as_num_deps);
        let num_deps = UIntCursor::encode(out, num_deps_iter, false).into();

        let deps_iter = self.deps_iter().map(as_deps);
        let deps = DeltaCursor::encode(out, deps_iter, false).into();

        let meta_iter = self.extra_bytes.iter().map(as_meta);
        let meta = MetaCursor::encode(out, meta_iter, false).into();

        let raw_iter = self.extra_bytes.iter().map(as_extra_bytes);
        let raw = RawCursor::encode(out, raw_iter, false).into();

        DocChangeColumns {
            actor,
            seq,
            max_op,
            time,
            message,
            deps: DepsRange::new(num_deps, deps),
            extra: ValueRange::new(meta, raw),
            other: Columns::empty(),
        }
    }

    pub(crate) fn opid_to_hash(&self, id: OpId) -> Option<ChangeHash> {
        let actor_indices = self.seq_index.get(id.actor())?;
        let counter = id.counter();
        let index = actor_indices
            .binary_search_by(|n| {
                let i = n.0 as usize;
                let num_ops = self.num_ops[i];
                let max_op = self.max_ops[i];
                let start = max_op - num_ops as u64 + 1;
                if counter < start {
                    Ordering::Greater
                } else if max_op <= counter {
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
            let edge = &self.edges[this_edge_idx.0 as usize];
            edge_idx = edge.next;
            let hash = self.hashes[edge.target.0 as usize];
            Some(hash)
        })
    }

    pub(crate) fn has_change(&self, hash: &ChangeHash) -> bool {
        self.nodes_by_hash.contains_key(hash)
    }

    pub(crate) fn get_build_metadata<I>(
        &self,
        hashes: I,
    ) -> Result<(Vec<BuildChangeMetadata<'_>>, usize), MissingDep>
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

    fn get_build_metadata_for_indexes<I>(&self, indexes: I) -> (Vec<BuildChangeMetadata<'_>>, usize)
    where
        I: IntoIterator<Item = NodeIdx>,
    {
        let mut num_deps = 0;
        let changes = indexes
            .into_iter()
            .map(|index| {
                let i = index.0 as usize;
                let actor = self.actors[i].into();
                let timestamp = self.timestamps[i];
                let max_op = self.max_ops[i];
                let num_ops = self.num_ops[i];
                let message = self.messages[i].as_deref().map(Cow::Borrowed);
                let extra = Cow::Borrowed(self.extra_bytes[i].as_slice());
                let deps = self.parents(index).map(|p| p.0 as u64).collect::<Vec<_>>();
                num_deps += deps.len();
                let start_op = max_op - num_ops as u64 + 1;
                let seq = self.seq[i];
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
        (changes, num_deps)
    }

    pub(crate) fn get_build_metadata_clock(
        &self,
        have_deps: &[ChangeHash],
    ) -> (Vec<BuildChangeMetadata<'_>>, usize) {
        // get the clock for the given deps
        let clock = self.clock_for_heads(have_deps);

        // get the documents current clock

        let mut change_indexes: Vec<NodeIdx> = Vec::new();
        // walk the state from the given deps clock and add them into the vec
        for (actor_index, actor_changes) in self.seq_index.iter().enumerate() {
            if let Some(clock_data) = clock.get_for_actor(&actor_index) {
                // find the change in this actors sequence of changes that corresponds to the max_op
                // recorded for them in the clock
                change_indexes.extend(&actor_changes[clock_data.seq as usize..]);
            } else {
                change_indexes.extend(&actor_changes[..]);
            }
        }

        // ensure the changes are still in sorted order
        change_indexes.sort_unstable();

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

    pub(crate) fn add_change(
        &mut self,
        change: &Change,
        actor_idx: usize,
    ) -> Result<(), MissingDep> {
        let actor_idx = ActorIdx::from(actor_idx);
        let hash = change.hash();
        if self.nodes_by_hash.contains_key(&hash) {
            return Ok(());
        }
        let parent_indices = change
            .deps()
            .iter()
            .map(|h| self.nodes_by_hash.get(h).copied().ok_or(MissingDep(*h)))
            .collect::<Result<Vec<_>, _>>()?;
        let change_seq = change.seq();
        self.update_heads(change);
        let node_idx = self.add_node(actor_idx, change);
        self.index_by_seq(actor_idx, node_idx, change_seq);
        self.nodes_by_hash.insert(hash, node_idx);
        for parent_idx in parent_indices {
            self.add_parent(node_idx, parent_idx);
        }
        if let Some(cached_idx) = Self::node_to_cache(&node_idx, CACHE_STEP) {
            assert_eq!(cached_idx, self.clock_cache.len());
            let clock = self.calculate_clock(vec![node_idx]);
            self.clock_cache.push(clock)
        }
        Ok(())
    }

    fn index_by_seq(&mut self, actor_index: ActorIdx, node_idx: NodeIdx, seq: u64) {
        let actor_index = actor_index.0 as usize;
        assert!(actor_index < self.seq_index.len());
        assert_eq!(self.seq_index[actor_index].len() + 1, seq as usize);
        self.seq_index[actor_index].push(node_idx);
    }

    fn add_node(&mut self, actor_index: ActorIdx, change: &Change) -> NodeIdx {
        let idx = NodeIdx(self.hashes.len() as u32);
        self.hashes.push(change.hash());
        self.actors.push(actor_index);
        self.seq.push(change.seq());
        self.max_ops.push(change.max_op());
        self.num_ops.push(change.len() as u32);
        self.timestamps.push(change.timestamp());
        self.messages.push(change.message().cloned());
        self.extra_bytes.push(change.extra_bytes().to_vec());
        self.parents.push(None);
        idx
    }

    fn add_parent(&mut self, child_idx: NodeIdx, parent_idx: NodeIdx) {
        let new_edge_idx = EdgeIdx(self.edges.len() as u32);
        let new_edge = Edge {
            target: parent_idx,
            next: None,
        };
        self.edges.push(new_edge);

        //let child = &mut self.nodes[child_idx.0 as usize];
        let child = &mut self.parents[child_idx.0 as usize];
        if let Some(edge_idx) = child {
            let mut edge = &mut self.edges[edge_idx.0 as usize];
            while let Some(next) = edge.next {
                edge = &mut self.edges[next.0 as usize];
            }
            edge.next = Some(new_edge_idx);
        } else {
            *child = Some(new_edge_idx);
        }
    }

    fn parents(&self, node_idx: NodeIdx) -> impl Iterator<Item = NodeIdx> + '_ {
        let mut edge_idx = self.parents[node_idx.0 as usize];
        std::iter::from_fn(move || {
            let this_edge_idx = edge_idx?;
            let edge = &self.edges[this_edge_idx.0 as usize];
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
        assert_eq!(
            self.clock_cache.len(),
            self.hashes.len() / CACHE_STEP as usize
        );
        self.calculate_clock(nodes)
    }

    fn node_to_cache(idx: &NodeIdx, step: u32) -> Option<usize> {
        assert!(step > 2);
        if (idx.0 + 1) % step == 0 {
            Some(((idx.0 + 1) / step - 1) as usize)
        } else {
            None
        }
    }

    fn calculate_clock(&self, nodes: Vec<NodeIdx>) -> Clock {
        let mut clock = Clock::new();

        self.traverse_ancestors(nodes, |idx| {
            clock.include(
                self.actors[idx.0 as usize].into(),
                ClockData {
                    max_op: self.max_ops[idx.0 as usize],
                    seq: self.seq[idx.0 as usize],
                },
            );
            if let Some(cached_idx) = Self::node_to_cache(&idx, CACHE_STEP) {
                if cached_idx < self.clock_cache.len() {
                    let ancestor_clock = &self.clock_cache[cached_idx];
                    clock = Clock::merge(&clock, ancestor_clock);
                    return false; // dont look at ancestors
                }
            }
            true // do look at ancestors
        });

        clock
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

    /// Call `f` for each (node, hash) in the graph, starting from the given heads
    ///
    /// No guarantees are made about the order of traversal but each node will only be visited
    /// once.
    fn traverse_ancestors<F: FnMut(NodeIdx) -> bool>(
        &self,
        mut to_visit: Vec<NodeIdx>,
        mut f: F,
    ) {
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

fn as_num_deps(num: usize) -> Option<Cow<'static, u64>> {
    Some(Cow::Owned(num as u64))
}

fn as_message(m: &Option<String>) -> Option<Cow<'_, str>> {
    m.as_deref().map(Cow::Borrowed)
}

fn as_seq(seq: &u64) -> Option<Cow<'_, i64>> {
    Some(Cow::Owned(*seq as i64))
}

fn as_actor(actor_index: &ActorIdx) -> Option<Cow<'_, ActorIdx>> {
    Some(Cow::Borrowed(actor_index))
}

fn as_max_op(m: &u64) -> Option<Cow<'_, i64>> {
    Some(Cow::Owned(*m as i64))
}

fn as_timestamp(n: &i64) -> Option<Cow<'_, i64>> {
    Some(Cow::Owned(*n))
}

fn as_deps(n: NodeIdx) -> Option<Cow<'static, i64>> {
    Some(Cow::Owned(n.0 as i64))
}

fn as_meta(b: &Vec<u8>) -> Option<Cow<'_, ValueMeta>> {
    Some(Cow::Owned(ValueMeta::from(b.as_slice())))
}

fn as_extra_bytes(b: &Vec<u8>) -> Option<Cow<'_, [u8]>> {
    Some(Cow::Borrowed(b.as_slice()))
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
        clock::ClockData,
        op_set2::{change::build_change, KeyRef, OpBuilder2, OpSet},
        types::{ObjId, ObjMeta, ObjType, OpId},
        ActorId,
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

        let mut expected_clock = Clock::new();
        expected_clock.include(builder.index(&actor1), ClockData { max_op: 50, seq: 2 });
        expected_clock.include(builder.index(&actor2), ClockData { max_op: 30, seq: 1 });
        expected_clock.include(builder.index(&actor3), ClockData { max_op: 40, seq: 1 });

        let clock = graph.clock_for_heads(&[change4]);
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
            let osd = OpSet::from_actors(self.actors.clone());

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
            let root = ObjMeta {
                id: ObjId::root(),
                typ: ObjType::Map,
            };
            let ops = (0..num_new_ops)
                .map(|opnum| OpBuilder2 {
                    obj: root,
                    pos: 0,
                    index: 0,
                    id: OpId::new(start_op + opnum as u64, actor_idx),
                    action: crate::OpType::Put("value".into()),
                    key: KeyRef::Map(Cow::Owned("key".into())),
                    pred: vec![],
                    insert: false,
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
    }

    #[test]
    fn node_to_cache() {
        assert_eq!(None, ChangeGraph::node_to_cache(&NodeIdx(0), 4));
        assert_eq!(None, ChangeGraph::node_to_cache(&NodeIdx(1), 4));
        assert_eq!(None, ChangeGraph::node_to_cache(&NodeIdx(2), 4));
        assert_eq!(Some(0), ChangeGraph::node_to_cache(&NodeIdx(3), 4));
        assert_eq!(None, ChangeGraph::node_to_cache(&NodeIdx(4), 4));
        assert_eq!(None, ChangeGraph::node_to_cache(&NodeIdx(5), 4));
        assert_eq!(None, ChangeGraph::node_to_cache(&NodeIdx(6), 4));
        assert_eq!(Some(1), ChangeGraph::node_to_cache(&NodeIdx(7), 4));
    }
}
