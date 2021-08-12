use std::fmt::Debug;

use automerge_protocol::OpId;

#[derive(Clone, Debug, PartialEq)]
pub struct SequenceTree<T> {
    root_node: SequenceTreeNode<T>,
}

#[derive(Clone, Debug, PartialEq)]
enum SequenceTreeNode<T> {
    Leaf {
        opid: OpId,
        elements: Vec<T>,
    },
    Node {
        left: Option<Box<SequenceTreeNode<T>>>,
        right: Option<Box<SequenceTreeNode<T>>>,
        len: usize,
    },
}

impl<T> SequenceTree<T>
where
    T: Clone + Debug,
{
    pub fn new() -> Self {
        Self {
            root_node: SequenceTreeNode::Node {
                left: None,
                right: None,
                len: 0,
            },
        }
    }

    pub fn len(&self) -> usize {
        self.root_node.len()
    }

    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    pub fn insert(&mut self, index: usize, opid: OpId, element: T) {
        self.root_node.insert(index, opid, element);
    }

    pub fn push_back(&mut self, opid: OpId, element: T) {
        let l = self.len();
        self.insert(l, opid, element)
    }

    pub fn get(&self, index: usize) -> Option<(OpId, &T)> {
        self.root_node.get(index)
    }

    pub fn get_mut(&mut self, index: usize) -> Option<(OpId, &mut T)> {
        self.root_node.get_mut(index)
    }

    pub fn remove(&mut self, index: usize) -> T {
        self.root_node.remove(index)
    }

    pub fn set(&mut self, index: usize, element: T) -> T {
        self.root_node.set(index, element)
    }
}

impl<T> SequenceTreeNode<T>
where
    T: Clone + Debug,
{
    pub fn len(&self) -> usize {
        match self {
            SequenceTreeNode::Leaf { elements, .. } => elements.len(),
            SequenceTreeNode::Node { len, .. } => *len,
        }
    }

    pub fn insert(&mut self, index: usize, opid: OpId, element: T) -> bool {
        match self {
            SequenceTreeNode::Leaf {
                opid: leaf_opid,
                elements,
            } => {
                if leaf_opid.1 == opid.1 {
                    // has our actor, see if the sequence counter fits in
                    if index == elements.len() {
                        // pushing onto the end so index may be rle-able
                        if leaf_opid.0 + elements.len() as u64 == opid.0 {
                            // is the next in sequence so just append
                            elements.push(element);
                            true
                        } else {
                            // may need to split the node
                            false
                        }
                    } else {
                        // need to split
                        false
                    }
                } else {
                    // need to make a new node
                    false
                }
            }
            SequenceTreeNode::Node { left, right, len } => {
                let left_len = left.as_ref().map_or(0, |l| l.len());
                *len += 1;
                if index > left_len {
                    if let Some(right_child) = right {
                        if !right_child.insert(index - left_len, opid, element) {
                            // failed to insert, need to split the node
                            let right_child = std::mem::take(right);
                            if let SequenceTreeNode::Leaf { opid, mut elements } =
                                *right_child.unwrap()
                            {
                                let right_elements = elements.split_off(index - left_len);
                                let len = elements.len() + right_elements.len();

                                let l = if elements.is_empty() {
                                    None
                                } else {
                                    Some(Box::new(SequenceTreeNode::Leaf {
                                        elements,
                                        opid: opid.clone(),
                                    }))
                                };
                                let r = if right_elements.is_empty() {
                                    None
                                } else {
                                    Some(Box::new(SequenceTreeNode::Leaf {
                                        opid: OpId(opid.0 + (index - left_len) as u64 + 1, opid.1),
                                        elements: right_elements,
                                    }))
                                };
                                *right = Some(Box::new(SequenceTreeNode::Node {
                                    left: l,
                                    right: r,
                                    len,
                                }));
                                true
                            } else {
                                unreachable!("found non leaf on split")
                            }
                        } else {
                            // added to elements
                            true
                        }
                    } else {
                        *right = Some(Box::new(SequenceTreeNode::Leaf {
                            opid,
                            elements: vec![element],
                        }));
                        true
                    }
                } else if let Some(left_child) = left {
                    if !left_child.insert(index, opid, element) {
                        // failed to insert, need to split the node
                        let left_child = std::mem::take(left);
                        if let SequenceTreeNode::Leaf { opid, mut elements } = *left_child.unwrap()
                        {
                            let right_elements = elements.split_off(index);
                            let len = elements.len() + right_elements.len();

                            let l = if elements.is_empty() {
                                None
                            } else {
                                Some(Box::new(SequenceTreeNode::Leaf {
                                    elements,
                                    opid: opid.clone(),
                                }))
                            };
                            let r = if right_elements.is_empty() {
                                None
                            } else {
                                Some(Box::new(SequenceTreeNode::Leaf {
                                    opid: OpId(opid.0 + index as u64 + 1, opid.1),
                                    elements: right_elements,
                                }))
                            };
                            *left = Some(Box::new(SequenceTreeNode::Node {
                                left: l,
                                right: r,
                                len,
                            }));
                            true
                        } else {
                            unreachable!("found non leaf on split")
                        }
                    } else {
                        // added to elements
                        true
                    }
                } else {
                    *left = Some(Box::new(SequenceTreeNode::Leaf {
                        opid,
                        elements: vec![element],
                    }));
                    true
                }
            }
        }
    }

    pub fn remove(&mut self, index: usize) -> T {
        match self {
            SequenceTreeNode::Leaf {
                opid: _,
                elements: _,
            } => {
                unreachable!("shouldn't be calling remove on a leaf, just a node")
            }
            SequenceTreeNode::Node { left, right, len } => {
                let left_len = left.as_ref().map_or(0, |l| l.len());
                *len -= 1;
                if index > left_len {
                    if let Some(right_child) = right {
                        if let SequenceTreeNode::Leaf { opid: _, elements } = &**right_child {
                            todo!();

                            // let right_child = std::mem::take(right);
                            // if let SequenceTreeNode::Leaf {
                            //     opid: _,
                            //     elements,
                            //     len: _,
                            // } = *right_child.unwrap()
                            // {
                            //     element
                            // } else {
                            //     unreachable!("was leaf then wasn't leaf")
                            // }
                        } else {
                            right_child.remove(index - left_len)
                        }
                    } else {
                        unreachable!("no right child")
                    }
                } else if let Some(left_child) = left {
                    if let SequenceTreeNode::Leaf { opid: _, elements } = &mut **left_child {
                        if index + 1 == elements.len() {
                            // removing from the end, no split needed
                            return elements.remove(index);
                        } else {
                            // need to split
                            todo!()
                        }
                        todo!();
                        // let left_child = std::mem::take(left);
                        // if let SequenceTreeNode::Leaf {
                        //     opid: _,
                        //     element,
                        //     len: _,
                        // } = *left_child.unwrap()
                        // {
                        //     element
                        // } else {
                        //     unreachable!("was leaf then wasn't leaf")
                        // }
                    } else {
                        left_child.remove(index)
                    }
                } else {
                    unreachable!("no left child")
                }
            }
        }
    }

    pub fn set(&mut self, index: usize, element: T) -> T {
        match self {
            SequenceTreeNode::Leaf { opid: _, elements } => {
                let old = elements.get_mut(index).unwrap();
                std::mem::replace(old, element)
            }
            SequenceTreeNode::Node {
                left,
                right,
                len: _,
            } => {
                let left_len = left.as_ref().map_or(0, |l| l.len());
                if index > left_len {
                    if let Some(right) = right {
                        right.set(index - left_len, element)
                    } else {
                        unreachable!("set on non existant index")
                    }
                } else {
                    if let Some(left) = left {
                        left.set(index, element)
                    } else {
                        unreachable!("set on non existant index")
                    }
                }
            }
        }
    }

    pub fn get(&self, index: usize) -> Option<(OpId, &T)> {
        match &self {
            SequenceTreeNode::Leaf { opid, elements } => elements
                .get(index)
                .map(|e| (OpId(opid.0 + elements.len() as u64, opid.1.clone()), e)),
            SequenceTreeNode::Node {
                left,
                right,
                len: _,
            } => {
                let left_len = left.as_ref().map_or(0, |l| l.len());
                if index > left_len {
                    right.as_ref().and_then(|r| r.get(index - left_len))
                } else {
                    left.as_ref().and_then(|l| l.get(index))
                }
            }
        }
    }

    pub fn get_mut(&mut self, index: usize) -> Option<(OpId, &mut T)> {
        match self {
            SequenceTreeNode::Leaf { opid, elements } => {
                let len = elements.len();

                elements
                    .get_mut(index)
                    .map(|e| (OpId(opid.0 + len as u64, opid.1.clone()), e))
            }
            SequenceTreeNode::Node {
                left,
                right,
                len: _,
            } => {
                let left_len = left.as_ref().map_or(0, |l| l.len());
                if index > left_len {
                    right.as_mut().and_then(|r| r.get_mut(index - left_len))
                } else {
                    left.as_mut().and_then(|l| l.get_mut(index))
                }
            }
        }
    }
}
