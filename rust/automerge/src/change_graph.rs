use std::collections::{BTreeMap, BTreeSet};

#[cfg(feature = "optree-visualisation")]
use std::collections::HashMap;

use crate::{
    clock::{Clock, ClockData},
    Change, ChangeHash,
};

#[cfg(feature = "optree-visualisation")]
mod visualise;

/// The graph of changes
///
/// This is a sort of adjacency list based representation, except that instead of using linked
/// lists, we keep all the edges and nodes in two vecs and reference them by index which plays nice
/// with the cache
#[derive(Debug, Clone)]
pub(crate) struct ChangeGraph {
    nodes: Vec<ChangeNode>,
    edges: Vec<Edge>,
    hashes: Vec<ChangeHash>,
    nodes_by_hash: BTreeMap<ChangeHash, NodeIdx>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct NodeIdx(u32);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct EdgeIdx(u32);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct HashIdx(u32);

#[derive(Debug, Clone)]
struct Edge {
    parent: NodeIdx,
    child: NodeIdx,
    next: Edges,
}

impl Edge {
    fn next(&self) -> Option<EdgeIdx> {
        self.next.0
    }
}

#[derive(Debug, Clone, Copy)]
struct Edges(Option<EdgeIdx>);

impl Edges {
    fn empty() -> Self {
        Self(None)
    }

    fn is_empty(&self) -> bool {
        self.0.is_none()
    }

    fn add_edge(&mut self, new_edge_idx: EdgeIdx, edges: &mut [Edge]) {
        if let Some(edge_idx) = self.0 {
            let mut edge = &mut edges[edge_idx.0 as usize];
            while let Some(next) = edge.next() {
                edge = &mut edges[next.0 as usize];
            }
            edge.next = Edges(Some(new_edge_idx));
        } else {
            self.0 = Some(new_edge_idx);
        }
    }
}

#[derive(Debug, Clone)]
struct ChangeNode {
    hash_idx: HashIdx,
    actor_index: usize,
    seq: u64,
    max_op: u64,
    parents: Edges,
    children: Edges,
}

impl ChangeGraph {
    pub(crate) fn new() -> Self {
        Self {
            nodes: Vec::new(),
            edges: Vec::new(),
            nodes_by_hash: BTreeMap::new(),
            hashes: Vec::new(),
        }
    }

    pub(crate) fn add_change(
        &mut self,
        change: &Change,
        actor_idx: usize,
    ) -> Result<(), MissingDep> {
        let hash = change.hash();
        if self.nodes_by_hash.contains_key(&hash) {
            return Ok(());
        }
        let parent_indices = change
            .deps()
            .iter()
            .map(|h| self.nodes_by_hash.get(h).copied().ok_or(MissingDep(*h)))
            .collect::<Result<Vec<_>, _>>()?;
        let node_idx = self.add_node(actor_idx, change);
        self.nodes_by_hash.insert(hash, node_idx);
        for parent_idx in parent_indices {
            self.add_parent(node_idx, parent_idx);
        }
        Ok(())
    }

    fn add_node(&mut self, actor_index: usize, change: &Change) -> NodeIdx {
        let idx = NodeIdx(self.nodes.len() as u32);
        let hash_idx = self.add_hash(change.hash());
        self.nodes.push(ChangeNode {
            hash_idx,
            actor_index,
            seq: change.seq(),
            max_op: change.max_op(),
            parents: Edges::empty(),
            children: Edges::empty(),
        });
        idx
    }

    fn add_hash(&mut self, hash: ChangeHash) -> HashIdx {
        let idx = HashIdx(self.hashes.len() as u32);
        self.hashes.push(hash);
        idx
    }

    fn add_parent(&mut self, child_idx: NodeIdx, parent_idx: NodeIdx) {
        let child_edge_idx = EdgeIdx(self.edges.len() as u32);
        let child_edge = Edge {
            parent: parent_idx,
            child: child_idx,
            next: Edges::empty(),
        };
        self.edges.push(child_edge);

        let child = &mut self.nodes[child_idx.0 as usize];
        child.parents.add_edge(child_edge_idx, &mut self.edges);

        let parent_edge_idx = EdgeIdx(self.edges.len() as u32);
        let parent_edge = Edge {
            parent: parent_idx,
            child: child_idx,
            next: Edges::empty(),
        };
        self.edges.push(parent_edge);

        let parent = &mut self.nodes[parent_idx.0 as usize];
        parent.children.add_edge(parent_edge_idx, &mut self.edges);
    }

    fn parents(&self, node_idx: NodeIdx) -> impl Iterator<Item = NodeIdx> + '_ {
        let mut edge_idx = self.nodes[node_idx.0 as usize].parents;
        std::iter::from_fn(move || {
            let this_edge_idx = edge_idx.0?;
            let edge = &self.edges[this_edge_idx.0 as usize];
            edge_idx = edge.next;
            Some(edge.parent)
        })
    }

    fn children(&self, node_idx: NodeIdx) -> Children<'_> {
        Children {
            graph: self,
            edges: self.nodes[node_idx.0 as usize].children,
        }
    }

    pub(crate) fn clock_for_heads(&self, heads: &[ChangeHash]) -> Clock {
        let mut clock = Clock::new();

        self.traverse_ancestors(heads, |node, _hash| {
            clock.include(
                node.actor_index,
                ClockData {
                    max_op: node.max_op,
                    seq: node.seq,
                },
            );
        });

        clock
    }

    pub(crate) fn remove_ancestors(
        &self,
        changes: &mut BTreeSet<ChangeHash>,
        heads: &[ChangeHash],
    ) {
        self.traverse_ancestors(heads, |_node, hash| {
            changes.remove(hash);
        });
    }

    /// Call `f` for each (node, hash) in the graph, starting from the given heads
    ///
    /// No guarantees are made about the order of traversal but each node will only be visited
    /// once.
    fn traverse_ancestors<F: FnMut(&ChangeNode, &ChangeHash)>(
        &self,
        heads: &[ChangeHash],
        mut f: F,
    ) {
        let mut to_visit = heads
            .iter()
            .filter_map(|h| self.nodes_by_hash.get(h))
            .copied()
            .collect::<Vec<_>>();

        let mut visited = BTreeSet::new();

        while let Some(idx) = to_visit.pop() {
            if visited.contains(&idx) {
                continue;
            } else {
                visited.insert(idx);
            }
            let node = &self.nodes[idx.0 as usize];
            let hash = &self.hashes[node.hash_idx.0 as usize];
            f(node, hash);
            to_visit.extend(self.parents(idx));
        }
    }

    fn roots(&self) -> impl Iterator<Item = NodeIdx> + '_ {
        self.nodes.iter().enumerate().filter_map(|(idx, node)| {
            if node.parents.is_empty() {
                Some(NodeIdx(idx as u32))
            } else {
                None
            }
        })
    }

    /// An iterator over the transitive dependencies of `hash` in topological order
    #[allow(dead_code)]
    pub(crate) fn deps_topo(&self, hash: &ChangeHash) -> impl Iterator<Item = &ChangeHash> {
        let node = self.nodes_by_hash.get(hash).copied();
        let ancestors = Deps::new(self, node).collect::<BTreeSet<_>>();
        let node_idxes = (0_u32..self.nodes.len() as u32)
            .map(NodeIdx)
            .collect::<BTreeSet<_>>();
        let non_ancestors = node_idxes.difference(&ancestors).copied().collect();
        Topo::pruning(self, non_ancestors)
    }

    /// A topological traversal of the graph
    pub(crate) fn topo(&self) -> impl Iterator<Item = &ChangeHash> {
        Topo::new(self)
    }

    /// Return a grpahviz representation of the change graph
    ///
    /// Any changes which are in `labels` will be labelled with the corresponding string. Otherwise
    /// the label will be `change_{n}` where `n` is a unique integer for each change.
    #[allow(dead_code)]
    #[cfg(feature = "optree-visualisation")]
    pub(crate) fn visualise(&self, labels: HashMap<ChangeHash, String>) -> String {
        let mut out = Vec::new();
        let labelled = visualise::LabelledGraph::new(self, labels);
        dot::render(&labelled, &mut out).unwrap();
        String::from_utf8_lossy(&out[..]).to_string()
    }
}

struct Children<'a> {
    graph: &'a ChangeGraph,
    edges: Edges,
}

impl<'a> Iterator for Children<'a> {
    type Item = NodeIdx;

    fn next(&mut self) -> Option<Self::Item> {
        let edge_idx = self.edges.0?;
        let edge = &self.graph.edges[edge_idx.0 as usize];
        self.edges = edge.next;
        Some(edge.child)
    }
}

struct Topo<'a> {
    graph: &'a ChangeGraph,
    to_process: Vec<NodeIdx>,
    visited: BTreeSet<NodeIdx>,
    prune: BTreeSet<NodeIdx>,
}

impl<'a> Topo<'a> {
    fn new(graph: &'a ChangeGraph) -> Self {
        Self {
            graph,
            to_process: graph.roots().collect(),
            visited: BTreeSet::new(),
            prune: BTreeSet::new(),
        }
    }

    fn pruning(graph: &'a ChangeGraph, prune: BTreeSet<NodeIdx>) -> Self {
        Self {
            graph,
            to_process: graph.roots().collect(),
            visited: BTreeSet::new(),
            prune,
        }
    }
}

impl<'a> Iterator for Topo<'a> {
    type Item = &'a ChangeHash;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.to_process.pop()?;
        self.visited.insert(next);
        for child in self.graph.children(next) {
            if self.graph.parents(child).all(|p| self.visited.contains(&p))
                && !self.visited.contains(&child)
                && !self.prune.contains(&child)
            {
                self.to_process.push(child);
            }
        }
        let hash = &self.graph.hashes[self.graph.nodes[next.0 as usize].hash_idx.0 as usize];
        Some(hash)
    }
}

struct Deps<'a> {
    to_process: Vec<NodeIdx>,
    visited: BTreeSet<NodeIdx>,
    graph: &'a ChangeGraph,
}

impl<'a> Deps<'a> {
    fn new(graph: &'a ChangeGraph, node: Option<NodeIdx>) -> Self {
        let to_process = node.map(|n| vec![n]).unwrap_or_default();
        Self {
            to_process,
            visited: BTreeSet::new(),
            graph,
        }
    }
}

impl<'a> Iterator for Deps<'a> {
    type Item = NodeIdx;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.to_process.pop()?;
        for parent in self.graph.parents(next) {
            if !self.visited.contains(&parent) {
                self.to_process.push(parent);
            }
        }
        Some(next)
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
        op_tree::OpSetMetadata,
        storage::{change::ChangeBuilder, convert::op_as_actor_id},
        types::{Key, ObjId, Op, OpId, OpIds},
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

    #[test]
    fn topo() {
        let mut builder = TestGraphBuilder::new();
        let actor1 = builder.actor();
        let actor2 = builder.actor();
        let actor3 = builder.actor();
        let change1 = builder.change(&actor1, 10, &[]);
        let change2 = builder.change(&actor2, 20, &[change1]);
        let change3 = builder.change(&actor3, 30, &[change1]);
        let change4 = builder.change(&actor1, 10, &[change2, change3]);
        let graph = builder.build();

        let topo = graph.topo().collect::<Vec<_>>();
        if topo != vec![&change1, &change2, &change3, &change4]
            && topo != vec![&change1, &change3, &change2, &change4]
        {
            panic!("not topological: {:?}", topo);
        }
    }

    #[test]
    fn deps_topo() {
        let mut builder = TestGraphBuilder::new();
        let actor1 = builder.actor();
        let actor2 = builder.actor();
        let actor3 = builder.actor();
        let change1 = builder.change(&actor1, 10, &[]);
        let change2 = builder.change(&actor2, 20, &[change1]);
        let change3 = builder.change(&actor3, 30, &[change1]);
        let _change4 = builder.change(&actor1, 10, &[change2, change3]);
        let change5 = builder.change(&actor3, 1, &[change3]);
        let graph = builder.build();

        let topo = graph.deps_topo(&change5).collect::<Vec<_>>();
        assert_eq!(topo, vec![&change1, &change3, &change5]);
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
            let mut meta = OpSetMetadata::from_actors(self.actors.clone());
            let key = meta.props.cache("key".to_string());

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
                .map(|opnum| Op {
                    id: OpId::new(start_op + opnum as u64, actor_idx),
                    action: crate::OpType::Put("value".into()),
                    key: Key::Map(key),
                    succ: OpIds::empty(),
                    pred: OpIds::empty(),
                    insert: false,
                })
                .collect::<Vec<_>>();

            let root = ObjId::root();
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
                    .build(ops.iter().map(|op| op_as_actor_id(&root, op, &meta)))
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
                let actor_idx = self.index(change.actor_id());
                graph.add_change(change, actor_idx).unwrap();
            }
            graph
        }
    }
}
