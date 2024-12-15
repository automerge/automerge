use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::num::NonZeroU64;

use crate::{
    clock::{Clock, ClockData},
    error::AutomergeError,
    op_set2::{
        change::{ChangeMetadata, ExtraChangeMetadata},
        ActorIdx,
    },
    Change, ChangeHash,
};

/// The graph of changes
///
/// This is a sort of adjacency list based representation, except that instead of using linked
/// lists, we keep all the edges and nodes in two vecs and reference them by index which plays nice
/// with the cache
#[derive(Debug, Default, Clone)]
pub(crate) struct ChangeGraph {
    nodes: Vec<ChangeNode>,
    edges: Vec<Edge>,
    hashes: Vec<ChangeHash>,
    nodes_by_hash: HashMap<ChangeHash, NodeIdx>,
    clock_cache: Vec<Clock>,
    seq_index: Vec<Vec<NodeIdx>>,
}

const CACHE_STEP: u32 = 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct NodeIdx(u32);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct EdgeIdx(u32);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct HashIdx(u32);

#[derive(Debug, Clone)]
struct Edge {
    // Edges are always child -> parent so we only store the target, the child is implicit
    // as you get the edge from the child
    target: NodeIdx,
    next: Option<EdgeIdx>,
}

#[derive(PartialEq, Debug, Clone)]
struct ChangeNode {
    hash_idx: HashIdx,
    actor_index: ActorIdx,
    seq: u64,
    max_op: u64,
    num_ops: u32,
    timestamp: i64,
    message: Option<String>,
    extra_bytes: Vec<u8>,
    parents: Option<EdgeIdx>,
}

impl ChangeGraph {
    pub(crate) fn new() -> Self {
        Self {
            nodes: Vec::new(),
            edges: Vec::new(),
            nodes_by_hash: HashMap::new(),
            hashes: Vec::new(),
            clock_cache: Vec::new(),
            seq_index: Vec::new(),
        }
    }

    pub(crate) fn with_capacity(changes: usize, deps: usize) -> Self {
        Self {
            nodes: Vec::with_capacity(changes),
            edges: Vec::with_capacity(deps),
            nodes_by_hash: HashMap::new(),
            hashes: Vec::with_capacity(changes),
            clock_cache: Vec::with_capacity(changes / CACHE_STEP as usize + 1),
            seq_index: Vec::new(),
        }
    }

    pub(crate) fn rewrite_with_new_actor(&mut self, idx: usize) {
        for node in &mut self.nodes {
            if node.actor_index.0 >= idx as u32 {
                node.actor_index.0 += 1;
            }
        }
        self.seq_index.insert(idx, vec![]);
        // FIXME - this could get expensive - lookout
        for clock in &mut self.clock_cache {
            clock.rewrite_with_new_actor(idx)
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.nodes.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub(crate) fn hash_to_index(&self, hash: &ChangeHash) -> Option<usize> {
        self.nodes_by_hash.get(hash).map(|n| n.0 as usize)
    }

    pub(crate) fn index_to_hash(&self, index: usize) -> Option<&ChangeHash> {
        self.hashes.get(index)
    }

    pub(crate) fn seq_for_actor(&self, actor: usize) -> u64 {
        self.seq_index
            .get(actor)
            .map(|v| v.len() as u64)
            .unwrap_or(0)
    }

    pub(crate) fn deps_for_hash(&self, hash: &ChangeHash) -> impl Iterator<Item = ChangeHash> + '_ {
        let node_idx = self.nodes_by_hash.get(hash);
        let mut edge_idx = node_idx.and_then(|n| self.nodes[n.0 as usize].parents);
        std::iter::from_fn(move || {
            let this_edge_idx = edge_idx?;
            let edge = &self.edges[this_edge_idx.0 as usize];
            edge_idx = edge.next;
            let hash_idx = self.nodes[edge.target.0 as usize].hash_idx;
            let hash = self.hashes[hash_idx.0 as usize];
            Some(hash)
        })
    }

    pub(crate) fn has_change(&self, hash: &ChangeHash) -> bool {
        self.nodes_by_hash.contains_key(hash)
    }

    pub(crate) fn get_max_op(&self, index: usize) -> Option<u64> {
        self.nodes.get(index).map(|n| n.max_op)
    }

    pub(crate) fn get_metadata_for_hash(
        &self,
        hash: &ChangeHash,
    ) -> Option<(ChangeMetadata<'_>, Vec<ChangeHash>)> {
        let index = self.hash_to_index(hash)?;
        self.get_metadata(index)
    }

    pub(crate) fn get_metadata(
        &self,
        index: usize,
    ) -> Option<(ChangeMetadata<'_>, Vec<ChangeHash>)> {
        let node = self.nodes.get(index)?;
        let actor = node.actor_index.into();
        let timestamp = node.timestamp;
        let message = node.message.as_deref().map(|n| Cow::Borrowed(n));
        let extra_bytes = Cow::Borrowed(node.extra_bytes.as_slice());
        let deps = self
            .parents(NodeIdx(index as u32))
            .map(|p| self.nodes[p.0 as usize].hash_idx)
            .map(|h| self.hashes[h.0 as usize])
            .collect();
        let start_op = NonZeroU64::new(node.max_op - node.num_ops as u64 + 1)?;
        let seq = node.seq;
        Some((
            ChangeMetadata {
                actor,
                seq,
                start_op,
                timestamp,
                message,
                extra_bytes,
            },
            deps,
        ))
    }

    pub(crate) fn get_metadata2<I>(
        &self,
        hashes: I,
    ) -> Result<(Vec<ExtraChangeMetadata<'_>>, usize), MissingDep>
    where
        I: IntoIterator<Item = ChangeHash>,
    {
        let mut indexes: Vec<_> = hashes
            .into_iter()
            .map(|hash| {
                self.nodes_by_hash
                    .get(&hash)
                    .cloned()
                    .ok_or(MissingDep(hash))
            })
            .collect::<Result<_, _>>()?;

        Ok(self.get_metadata_for_indexes(indexes))
    }

    fn get_metadata_for_indexes<I>(&self, indexes: I) -> (Vec<ExtraChangeMetadata<'_>>, usize)
    where
        I: IntoIterator<Item = NodeIdx>,
    {
        let mut num_deps = 0;
        let changes = indexes
            .into_iter()
            .map(|index| {
                let node = self.nodes.get(index.0 as usize).unwrap();
                let actor = node.actor_index.into();
                let timestamp = node.timestamp;
                let max_op = node.max_op;
                let message = node.message.as_deref().map(|n| Cow::Borrowed(n));
                let extra = Cow::Borrowed(node.extra_bytes.as_slice());
                let deps = self.parents(index).map(|p| p.0 as u64).collect::<Vec<_>>();
                num_deps += deps.len();
                let start_op = node.max_op - node.num_ops as u64 + 1;
                let seq = node.seq;
                ExtraChangeMetadata {
                    actor,
                    seq,
                    start_op,
                    max_op,
                    timestamp,
                    message,
                    extra,
                    deps,
                    builder: 0,
                }
            })
            .collect();
        (changes, num_deps)
    }

    pub(crate) fn get_metadata_clock(
        &self,
        have_deps: &[ChangeHash],
    ) -> (Vec<ExtraChangeMetadata<'_>>, usize) {
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

        self.get_metadata_for_indexes(change_indexes)
    }

    pub(crate) fn get_hash_for_actor_seq(
        &self,
        actor: usize,
        seq: u64,
    ) -> Result<ChangeHash, AutomergeError> {
        self.seq_index
            .get(actor)
            .and_then(|v| v.get(seq as usize - 1))
            //.and_then(|&i| self.change_graph.get_hash(i).copied())
            .and_then(|i| self.nodes.get(i.0 as usize))
            .and_then(|n| self.hashes.get(n.hash_idx.0 as usize))
            .ok_or(AutomergeError::InvalidSeq(seq))
            .copied()
    }

    pub(crate) fn get_hash(&self, index: usize) -> Option<&ChangeHash> {
        self.nodes
            .get(index)
            .and_then(|n| self.hashes.get(n.hash_idx.0 as usize))
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
        while actor_index >= self.seq_index.len() {
            self.seq_index.push(vec![]);
        }
        assert_eq!(self.seq_index[actor_index].len() + 1, seq as usize);
        self.seq_index[actor_index].push(node_idx);
    }

    fn add_node(&mut self, actor_index: ActorIdx, change: &Change) -> NodeIdx {
        let idx = NodeIdx(self.nodes.len() as u32);
        let hash_idx = self.add_hash(change.hash());
        self.nodes.push(ChangeNode {
            hash_idx,
            actor_index,
            seq: change.seq(),
            max_op: change.max_op(),
            timestamp: change.timestamp(),
            message: change.message().cloned(),
            extra_bytes: change.extra_bytes().to_vec(),
            num_ops: change.len() as u32,
            parents: None,
        });
        idx
    }

    fn add_hash(&mut self, hash: ChangeHash) -> HashIdx {
        let idx = HashIdx(self.hashes.len() as u32);
        self.hashes.push(hash);
        idx
    }

    fn add_parent(&mut self, child_idx: NodeIdx, parent_idx: NodeIdx) {
        let new_edge_idx = EdgeIdx(self.edges.len() as u32);
        let new_edge = Edge {
            target: parent_idx,
            next: None,
        };
        self.edges.push(new_edge);

        let child = &mut self.nodes[child_idx.0 as usize];
        if let Some(edge_idx) = child.parents {
            let mut edge = &mut self.edges[edge_idx.0 as usize];
            while let Some(next) = edge.next {
                edge = &mut self.edges[next.0 as usize];
            }
            edge.next = Some(new_edge_idx);
        } else {
            child.parents = Some(new_edge_idx);
        }
    }

    fn parents(&self, node_idx: NodeIdx) -> impl Iterator<Item = NodeIdx> + '_ {
        let mut edge_idx = self.nodes[node_idx.0 as usize].parents;
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
            self.nodes.len() / CACHE_STEP as usize
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

        self.traverse_ancestors(nodes, |node, idx| {
            clock.include(
                node.actor_index.into(),
                ClockData {
                    max_op: node.max_op,
                    seq: node.seq,
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
        self.traverse_ancestors(nodes, |node, _idx| {
            let hash = &self.hashes[node.hash_idx.0 as usize];
            changes.remove(hash);
            true
        });
    }

    /// Call `f` for each (node, hash) in the graph, starting from the given heads
    ///
    /// No guarantees are made about the order of traversal but each node will only be visited
    /// once.
    fn traverse_ancestors<F: FnMut(&ChangeNode, NodeIdx) -> bool>(
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
            let node = &self.nodes[idx.0 as usize];
            if f(node, idx) {
                to_visit.extend(self.parents(idx));
            }
        }
    }
}

pub(crate) struct DepIter<'a> {
    graph: &'a ChangeGraph,
    parent: Option<EdgeIdx>,
}

impl<'a> Iterator for DepIter<'a> {
    type Item = ChangeHash;
    fn next(&mut self) -> Option<ChangeHash> {
        let edge = self.graph.edges.get(self.parent?.0 as usize)?;
        self.parent = edge.next;
        let node = self.graph.nodes.get(edge.target.0 as usize)?;
        self.graph.hashes.get(node.hash_idx.0 as usize).cloned()
    }
}

#[derive(Debug, thiserror::Error)]
#[error("attempted to derive a clock for a change with dependencies we don't have")]
pub struct MissingDep(ChangeHash);

#[cfg(test)]
mod tests {
    use std::{
        num::NonZeroU64,
        time::{SystemTime, UNIX_EPOCH},
    };

    use crate::{
        clock::ClockData,
        op_set2::{Key, OpBuilder2, OpSet},
        storage::change::ChangeBuilder,
        storage::convert::ob_as_actor_id,
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
        seqs_by_actor: BTreeMap<ActorId, u64>,
    }

    impl TestGraphBuilder {
        fn new() -> Self {
            TestGraphBuilder {
                actors: Vec::new(),
                changes: Vec::new(),
                seqs_by_actor: BTreeMap::new(),
            }
        }

        fn actor(&mut self) -> ActorId {
            let actor = ActorId::random();
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
                    key: Key::Map("key".into()),
                    pred: vec![],
                    insert: false,
                })
                .collect::<Vec<_>>();

            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            let seq = self.seqs_by_actor.entry(actor.clone()).or_insert(1);
            let change = Change::new(
                ChangeBuilder::new()
                    .with_dependencies(parents.to_vec())
                    .with_start_op(NonZeroU64::new(start_op).unwrap())
                    .with_actor(actor.clone())
                    .with_seq(*seq)
                    .with_timestamp(timestamp)
                    .build(ops.iter().map(|o| ob_as_actor_id(&osd, o)))
                    .unwrap(),
            );
            *seq = seq.checked_add(1).unwrap();
            let hash = change.hash();
            self.changes.push(change);
            hash
        }

        fn build(&self) -> ChangeGraph {
            let mut graph = ChangeGraph::new();
            for change in &self.changes {
                let actor_idx = self.index(change.actor_id()).into();
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
