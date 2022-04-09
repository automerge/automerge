use crate::object_data::ObjectData;
use crate::types::ObjId;
use fxhash::FxHasher;
use std::{borrow::Cow, collections::HashMap, hash::BuildHasherDefault};

use rand::Rng;

#[derive(Copy, Clone, PartialEq, Hash, Eq)]
pub(crate) struct NodeId(u64);

impl Default for NodeId {
    fn default() -> Self {
        let mut rng = rand::thread_rng();
        let u = rng.gen();
        NodeId(u)
    }
}

#[derive(Clone)]
pub(crate) struct Node<'a> {
    id: NodeId,
    children: Vec<NodeId>,
    node_type: NodeType<'a>,
    metadata: &'a crate::op_set::OpSetMetadata,
}

#[derive(Clone)]
pub(crate) enum NodeType<'a> {
    ObjRoot(crate::types::ObjId),
    ObjTreeNode(ObjId, &'a crate::op_tree::OpTreeNode),
}

#[derive(Clone)]
pub(crate) struct Edge {
    parent_id: NodeId,
    child_id: NodeId,
}

pub(crate) struct GraphVisualisation<'a> {
    nodes: HashMap<NodeId, Node<'a>>,
    actor_shorthands: HashMap<usize, String>,
}

impl<'a> GraphVisualisation<'a> {
    pub(super) fn construct(
        objects: &'a HashMap<crate::types::ObjId, ObjectData, BuildHasherDefault<FxHasher>>,
        metadata: &'a crate::op_set::OpSetMetadata,
    ) -> GraphVisualisation<'a> {
        let mut nodes = HashMap::new();
        for (obj_id, object_data) in objects {
            if let Some(root_node) = &object_data.ops.root_node {
                let tree_id = Self::construct_nodes(root_node, obj_id, &mut nodes, metadata);
                let obj_tree_id = NodeId::default();
                nodes.insert(
                    obj_tree_id,
                    Node {
                        id: obj_tree_id,
                        children: vec![tree_id],
                        node_type: NodeType::ObjRoot(*obj_id),
                        metadata,
                    },
                );
            }
        }
        let mut actor_shorthands = HashMap::new();
        for actor in 0..metadata.actors.len() {
            actor_shorthands.insert(actor, format!("actor{}", actor));
        }
        GraphVisualisation {
            nodes,
            actor_shorthands,
        }
    }

    fn construct_nodes(
        node: &'a crate::op_tree::OpTreeNode,
        objid: &ObjId,
        nodes: &mut HashMap<NodeId, Node<'a>>,
        m: &'a crate::op_set::OpSetMetadata,
    ) -> NodeId {
        let node_id = NodeId::default();
        let mut child_ids = Vec::new();
        for child in &node.children {
            let child_id = Self::construct_nodes(child, objid, nodes, m);
            child_ids.push(child_id);
        }
        nodes.insert(
            node_id,
            Node {
                id: node_id,
                children: child_ids,
                node_type: NodeType::ObjTreeNode(*objid, node),
                metadata: m,
            },
        );
        node_id
    }
}

impl<'a> dot::GraphWalk<'a, &'a Node<'a>, Edge> for GraphVisualisation<'a> {
    fn nodes(&'a self) -> dot::Nodes<'a, &'a Node<'a>> {
        Cow::Owned(self.nodes.values().collect::<Vec<_>>())
    }

    fn edges(&'a self) -> dot::Edges<'a, Edge> {
        let mut edges = Vec::new();
        for node in self.nodes.values() {
            for child in &node.children {
                edges.push(Edge {
                    parent_id: node.id,
                    child_id: *child,
                });
            }
        }
        Cow::Owned(edges)
    }

    fn source(&'a self, edge: &Edge) -> &'a Node<'a> {
        self.nodes.get(&edge.parent_id).unwrap()
    }

    fn target(&'a self, edge: &Edge) -> &'a Node<'a> {
        self.nodes.get(&edge.child_id).unwrap()
    }
}

impl<'a> dot::Labeller<'a, &'a Node<'a>, Edge> for GraphVisualisation<'a> {
    fn graph_id(&'a self) -> dot::Id<'a> {
        dot::Id::new("OpSet").unwrap()
    }

    fn node_id(&'a self, n: &&Node<'a>) -> dot::Id<'a> {
        dot::Id::new(format!("node_{}", n.id.0)).unwrap()
    }

    fn node_shape(&'a self, node: &&'a Node<'a>) -> Option<dot::LabelText<'a>> {
        let shape = match node.node_type {
            NodeType::ObjTreeNode(_, _) => dot::LabelText::label("none"),
            NodeType::ObjRoot(_) => dot::LabelText::label("ellipse"),
        };
        Some(shape)
    }

    fn node_label(&'a self, n: &&Node<'a>) -> dot::LabelText<'a> {
        match n.node_type {
            NodeType::ObjTreeNode(objid, tree_node) => dot::LabelText::HtmlStr(
                OpTable::create(tree_node, &objid, n.metadata, &self.actor_shorthands)
                    .to_html()
                    .into(),
            ),
            NodeType::ObjRoot(objid) => {
                dot::LabelText::label(print_opid(&objid.0, &self.actor_shorthands))
            }
        }
    }
}

struct OpTable {
    rows: Vec<OpTableRow>,
}

impl OpTable {
    fn create<'a>(
        node: &'a crate::op_tree::OpTreeNode,
        obj: &ObjId,
        metadata: &crate::op_set::OpSetMetadata,
        actor_shorthands: &HashMap<usize, String>,
    ) -> Self {
        let rows = node
            .elements
            .iter()
            .map(|e| OpTableRow::create(e, obj, metadata, actor_shorthands))
            .collect();
        OpTable { rows }
    }

    fn to_html(&self) -> String {
        let rows = self
            .rows
            .iter()
            .map(|r| r.to_html())
            .collect::<Vec<_>>()
            .join("");
        format!(
            "<table cellspacing=\"0\">\
            <tr>\
                <td>op</td>\
                <td>obj</td>\
                <td>prop</td>\
                <td>action</td>\
                <td>succ</td>\
            </tr>\
            <hr/>\
            {}\
            </table>",
            rows
        )
    }
}

struct OpTableRow {
    obj_id: String,
    op_id: String,
    prop: String,
    op_description: String,
    succ: String,
}

impl OpTableRow {
    fn to_html(&self) -> String {
        let rows = [
            &self.op_id,
            &self.obj_id,
            &self.prop,
            &self.op_description,
            &self.succ,
        ];
        let row = rows
            .iter()
            .map(|r| format!("<td>{}</td>", &r))
            .collect::<String>();
        format!("<tr>{}</tr>", row)
    }
}

impl OpTableRow {
    fn create(
        op: &super::types::Op,
        obj: &ObjId,
        metadata: &crate::op_set::OpSetMetadata,
        actor_shorthands: &HashMap<usize, String>,
    ) -> Self {
        let op_description = match &op.action {
            crate::OpType::Delete => "del".to_string(),
            crate::OpType::Put(v) => format!("set {}", v),
            crate::OpType::Make(obj) => format!("make {}", obj),
            crate::OpType::Increment(v) => format!("inc {}", v),
        };
        let prop = match op.key {
            crate::types::Key::Map(k) => metadata.props[k].clone(),
            crate::types::Key::Seq(e) => print_opid(&e.0, actor_shorthands),
        };
        let succ = op
            .succ
            .iter()
            .map(|s| format!(",{}", print_opid(s, actor_shorthands)))
            .collect();
        OpTableRow {
            op_description,
            obj_id: print_opid(&obj.0, actor_shorthands),
            op_id: print_opid(&op.id, actor_shorthands),
            prop,
            succ,
        }
    }
}

fn print_opid(opid: &crate::types::OpId, actor_shorthands: &HashMap<usize, String>) -> String {
    format!("{}@{}", opid.counter(), actor_shorthands[&opid.actor()])
}
