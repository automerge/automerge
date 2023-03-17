use std::{borrow::Cow, collections::HashMap};

use super::{Edge, NodeIdx};
use crate::ChangeHash;

pub(super) struct LabelledGraph<'a> {
    graph: &'a super::ChangeGraph,
    labels: HashMap<ChangeHash, String>,
}

impl<'a> LabelledGraph<'a> {
    pub(super) fn new(graph: &'a super::ChangeGraph, labels: HashMap<ChangeHash, String>) -> Self {
        LabelledGraph { graph, labels }
    }
}

impl<'a> dot::GraphWalk<'a, NodeIdx, Edge> for LabelledGraph<'a> {
    fn nodes(&'a self) -> dot::Nodes<'a, NodeIdx> {
        Cow::Owned(
            (0..(self.graph.nodes.len() as u32))
                .map(NodeIdx)
                .collect::<Vec<_>>(),
        )
    }

    fn edges(&'a self) -> dot::Edges<'a, Edge> {
        Cow::Borrowed(self.graph.edges.as_slice())
    }

    fn source(&'a self, edge: &Edge) -> NodeIdx {
        edge.parent
    }

    fn target(&'a self, edge: &Edge) -> NodeIdx {
        edge.child
    }
}

impl<'a> dot::Labeller<'a, NodeIdx, Edge> for LabelledGraph<'a> {
    fn graph_id(&'a self) -> dot::Id<'a> {
        dot::Id::new("ChangeGraph").unwrap()
    }

    fn node_id(&'a self, node_idx: &NodeIdx) -> dot::Id<'a> {
        let node = &self.graph.nodes[node_idx.0 as usize];
        let hash = &self.graph.hashes[node.hash_idx.0 as usize];
        if let Some(label) = self.labels.get(hash) {
            if let Ok(id) = dot::Id::new(label) {
                return id;
            }
        }
        dot::Id::new(format!("change_{}", node_idx.0)).unwrap()
    }
}
