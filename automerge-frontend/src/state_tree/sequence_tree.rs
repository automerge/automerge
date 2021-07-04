use std::collections::HashMap;

use automerge_protocol::{ActorId, OpId};

pub struct SequenceTree<T> {
    nodes: Vec<SequenceTreeNode<T>>,
}

pub struct SequenceTreeNode<T> {
        actor: ActorId,
        start_counter:usize,
        start_index: usize,
        elements: Vec<T>,
}

impl<T> SequenceTree<T> {
    pub fn new() -> Self {
        Self {
            nodes:Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        todo!()
    }

    pub fn insert(&mut self, index: usize, opid: OpId, element: T) {
        // add 1 to length
        //
        // go through nodes trying to insert

        for node in nodes {
            if node.
        }
    }
}

impl<T> SequenceTreeNode<T> {
    pub fn new() -> Self {
        Self::Children { nodes: Vec::new() }
    }

    pub fn insert(&mut self, index: usize, opid: OpId, element: T) {
        match self {
            SequenceTreeNode::Items {
                actor,
                start_index,
                elements,
            } => todo!(),
            SequenceTreeNode::Children { nodes } => {
                for node in nodes {
                    if node
                }
            },
        }
    }
}
