use std::fmt::Debug;

use automerge_protocol::{ActorId, OpId};

#[derive(Clone, Debug, PartialEq)]
pub struct SequenceTree<T> {
    nodes: Vec<SequenceTreeNode<T>>,
    length: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SequenceTreeNode<T> {
    actor: ActorId,
    start_counter: u64,
    start_index: usize,
    elements: Vec<T>,
}

impl<T> SequenceTree<T>
where
    T: Clone + Debug,
{
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            length: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.length
    }

    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    pub fn insert(&mut self, index: usize, opid: OpId, element: T) {
        if index == self.len() {
            self.push_back(opid, element);
            return;
        }
        self.length += 1;
        let mut new_node_needed = false;
        let mut split_node = None;

        for node in &mut self.nodes {
            if index < node.start_index {
                node.start_index += 1
            } else if index == node.start_index {
                node.start_index += 1;
                new_node_needed = true;
            } else if index > node.start_index && index < node.start_index + node.elements.len() {
                let other_elements = node.elements.split_off(index - node.start_index);
                assert!(!node.elements.is_empty());
                if !other_elements.is_empty() {
                    split_node = Some(SequenceTreeNode {
                        actor: node.actor.clone(),
                        start_counter: node.start_counter + (index - node.start_index) as u64,
                        start_index: index + 1,
                        elements: other_elements,
                    });
                }

                new_node_needed = true
            } else if index > node.start_index && index == node.start_index + node.elements.len() {
                // at the end, may be able to add it if correct actorid and counter
                if opid.1 == node.actor && opid.0 == node.start_counter + node.elements.len() as u64
                {
                    node.elements.push(element.clone())
                } else {
                    new_node_needed = true
                }
            } else {
                // do nothing
            }
        }

        if new_node_needed {
            self.nodes.push(SequenceTreeNode {
                actor: opid.1,
                start_counter: opid.0,
                start_index: index,
                elements: vec![element],
            })
        }

        if let Some(node) = split_node {
            self.nodes.push(node)
        }

        let any_empty = self.nodes.iter().any(|n| n.elements.is_empty());
        if any_empty {
            let mut indices = self
                .nodes
                .iter()
                .map(|i| (i.start_index, i.elements.len()))
                .collect::<Vec<_>>();
            indices.sort_unstable_by_key(|v| v.0);
            dbg!(&self, index, indices);
            panic!("empty insert {:?}", index)
        }
    }

    pub fn push_back(&mut self, opid: OpId, element: T) {
        let mut new_node_needed = false;
        let len = self.len();
        self.length += 1;
        if self.nodes.is_empty() {
            new_node_needed = true
        } else {
            for node in &mut self.nodes {
                if node.start_index + node.elements.len() == len {
                    if opid.1 == node.actor
                        && opid.0 == node.start_counter + node.elements.len() as u64
                    {
                        node.elements.push(element.clone())
                    } else {
                        new_node_needed = true;
                        break;
                    }
                }
            }
        }

        if new_node_needed {
            self.nodes.push(SequenceTreeNode {
                actor: opid.1,
                start_counter: opid.0,
                start_index: self.len() - 1,
                elements: vec![element],
            })
        }
        let any_empty = self.nodes.iter().any(|n| n.elements.is_empty());
        if any_empty {
            let mut indices = self
                .nodes
                .iter()
                .map(|i| (i.start_index, i.elements.len()))
                .collect::<Vec<_>>();
            indices.sort_unstable_by_key(|v| v.0);
            dbg!(&self, indices);
            panic!("empty push_back")
        }
    }

    pub fn get(&self, index: usize) -> Option<(OpId, &T)> {
        for node in &self.nodes {
            if index >= node.start_index && index < node.start_index + node.elements.len() {
                return node.elements.get(index - node.start_index).map(|v| {
                    (
                        OpId(
                            node.start_counter + (index - node.start_index) as u64,
                            node.actor.clone(),
                        ),
                        v,
                    )
                });
            }
        }
        let mut indices = self
            .nodes
            .iter()
            .map(|i| (i.start_index, i.elements.len()))
            .collect::<Vec<_>>();
        indices.sort_unstable_by_key(|v| v.0);
        dbg!(&self, index, indices);
        None
    }

    pub fn get_mut(&mut self, index: usize) -> Option<(OpId, &mut T)> {
        for node in &mut self.nodes {
            if index >= node.start_index && index < node.start_index + node.elements.len() {
                let counter = node.start_counter + (index - node.start_index) as u64;
                let actor = node.actor.clone();
                return node
                    .elements
                    .get_mut(index - node.start_index)
                    .map(|v| (OpId(counter, actor), v));
            }
        }
        None
    }

    pub fn remove(&mut self, index: usize) -> T {
        self.length -= 1;
        let mut split_node = None;

        let mut to_return = None;
        let mut node_to_remove = None;

        for (i, node) in self.nodes.iter_mut().enumerate() {
            if index < node.start_index {
                node.start_index -= 1
            } else if index == node.start_index {
                node.start_counter += 1;
                node.start_index -= 1;
                if node.elements.len() == 1 {
                    node_to_remove = Some(i)
                }
                to_return = Some(node.elements.remove(0))
            } else if index > node.start_index && index < node.start_index + node.elements.len() {
                let other_elements = node.elements.split_off((index - node.start_index) + 1);
                if !other_elements.is_empty() {
                    split_node = Some(SequenceTreeNode {
                        actor: node.actor.clone(),
                        start_counter: node.start_counter + (index - node.start_index) as u64,
                        start_index: index,
                        elements: other_elements,
                    });
                }

                if node.elements.len() == 1 {
                    node_to_remove = Some(i)
                }
                to_return = Some(node.elements.remove(index - node.start_index))
            } else if index > node.start_index && index == node.start_index + node.elements.len() {
                // at the end, may be able to add it if correct actorid and counter
                if node.elements.len() == 1 {
                    node_to_remove = Some(i)
                }
                to_return = Some(node.elements.remove(node.elements.len() - 1))
            } else {
                // do nothing
            }
        }

        if let Some(i) = node_to_remove {
            self.nodes.remove(i);
        }

        if let Some(node) = split_node {
            self.nodes.push(node)
        }

        let any_empty = self.nodes.iter().any(|n| n.elements.is_empty());
        if any_empty {
            let mut indices = self
                .nodes
                .iter()
                .map(|i| (i.start_index, i.elements.len()))
                .collect::<Vec<_>>();
            indices.sort_unstable_by_key(|v| v.0);
            dbg!(&self, index, indices);
            panic!("empty remove {:?}", index)
        }

        to_return.unwrap()
    }

    pub fn set(&mut self, index: usize, element: T) -> T {
        // TODO
        todo!()
    }
}
