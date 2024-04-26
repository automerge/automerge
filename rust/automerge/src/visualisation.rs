use crate::types::{ObjId, Op};
use fxhash::FxHasher;
use std::fmt::Write;
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
    osd: &'a crate::op_set::OpSetData,
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
        trees: &'a HashMap<
            crate::types::ObjId,
            crate::op_tree::OpTree,
            BuildHasherDefault<FxHasher>,
        >,
        osd: &'a crate::op_set::OpSetData,
    ) -> GraphVisualisation<'a> {
        let mut nodes = HashMap::new();
        for (obj_id, tree) in trees {
            if let Some(root_node) = &tree.internal.root_node {
                let tree_id = Self::construct_nodes(root_node, obj_id, &mut nodes, osd);
                let obj_tree_id = NodeId::default();
                nodes.insert(
                    obj_tree_id,
                    Node {
                        id: obj_tree_id,
                        children: vec![tree_id],
                        node_type: NodeType::ObjRoot(*obj_id),
                        osd,
                    },
                );
            }
        }
        let mut actor_shorthands = HashMap::new();
        for actor in 0..osd.actors.len() {
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
        osd: &'a crate::op_set::OpSetData,
    ) -> NodeId {
        let node_id = NodeId::default();
        let mut child_ids = Vec::new();
        for child in &node.children {
            let child_id = Self::construct_nodes(child, objid, nodes, osd);
            child_ids.push(child_id);
        }
        nodes.insert(
            node_id,
            Node {
                id: node_id,
                children: child_ids,
                node_type: NodeType::ObjTreeNode(*objid, node),
                osd,
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
                OpTable::create(tree_node, &objid, n.osd, &self.actor_shorthands)
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
    fn create(
        node: &crate::op_tree::OpTreeNode,
        obj: &ObjId,
        osd: &crate::op_set::OpSetData,
        actor_shorthands: &HashMap<usize, String>,
    ) -> Self {
        let rows = node
            .elements
            .iter()
            .map(|e| OpTableRow::create(e.as_op(osd), obj, osd, actor_shorthands))
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
                <td>pred</td>\
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
    pred: String,
}

impl OpTableRow {
    fn to_html(&self) -> String {
        let rows = [
            &self.op_id,
            &self.obj_id,
            &self.prop,
            &self.op_description,
            &self.succ,
            &self.pred,
        ];
        let row = rows.iter().fold(String::new(), |mut output, r| {
            let _ = write!(output, "<td>{}</td>", r);
            output
        });
        format!("<tr>{}</tr>", row)
    }
}

impl OpTableRow {
    fn create(
        op: Op<'_>,
        obj: &ObjId,
        osd: &crate::op_set::OpSetData,
        actor_shorthands: &HashMap<usize, String>,
    ) -> Self {
        let op_description = match &op.action() {
            crate::OpType::Delete => "del".to_string(),
            crate::OpType::Put(crate::ScalarValue::F64(v)) => format!("set {:.2}", v),
            crate::OpType::Put(v) => format!("set {}", v),
            crate::OpType::Make(obj) => format!("make {}", obj),
            crate::OpType::Increment(v) => format!("inc {}", v),
            crate::OpType::MarkBegin(_, m) => format!("markBegin {}", m),
            crate::OpType::MarkEnd(m) => format!("markEnd {}", m),
        };
        let prop = match op.key() {
            crate::types::Key::Map(k) => osd.props[*k].clone(),
            crate::types::Key::Seq(e) => print_opid(&e.0, actor_shorthands),
        };
        let succ = op.succ().fold(String::new(), |mut output, s| {
            let _ = write!(output, ",{}", print_opid(s.id(), actor_shorthands));
            output
        });
        let pred = op.pred().fold(String::new(), |mut output, p| {
            let _ = write!(output, ",{}", print_opid(p.id(), actor_shorthands));
            output
        });
        OpTableRow {
            op_description,
            obj_id: print_opid(&obj.0, actor_shorthands),
            op_id: print_opid(op.id(), actor_shorthands),
            prop,
            succ,
            pred,
        }
    }
}

fn print_opid(opid: &crate::types::OpId, actor_shorthands: &HashMap<usize, String>) -> String {
    format!("{}@{}", opid.counter(), actor_shorthands[&opid.actor()])
}
