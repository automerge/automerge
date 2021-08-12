use std::fmt::Debug;

use automerge_protocol::OpId;

#[derive(Clone, Debug, PartialEq)]
pub struct SequenceTree<T> {
    root_node: SequenceTreeNode<T>,
}

#[derive(Clone, Debug, PartialEq)]
enum SequenceTreeNode<T> {
    Leaf(OpId, T),
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
        self.root_node.insert(index, opid, element)
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
            SequenceTreeNode::Leaf(..) => 1,
            SequenceTreeNode::Node { len, .. } => *len,
        }
    }

    pub fn insert(&mut self, index: usize, opid: OpId, element: T) {
        match self {
            SequenceTreeNode::Leaf(_old_opid, _old_element) => {
                let leaf = std::mem::replace(
                    self,
                    SequenceTreeNode::Node {
                        left: None,
                        right: None,
                        len: 0,
                    },
                );

                if let SequenceTreeNode::Leaf(old_opid, old_element) = leaf {
                    let left = Some(Box::new(SequenceTreeNode::Leaf(old_opid, old_element)));
                    let right = Some(Box::new(SequenceTreeNode::Leaf(opid, element)));
                    *self = SequenceTreeNode::Node {
                        left,
                        right,
                        len: 2,
                    };
                } else {
                    unreachable!("was leaf then not a leaf")
                }
            }
            SequenceTreeNode::Node { left, right, len } => {
                let left_len = left.as_ref().map_or(0, |l| l.len());
                *len += 1;
                if index > left_len {
                    if let Some(right) = right {
                        right.insert(index - left_len, opid, element)
                    } else {
                        *right = Some(Box::new(SequenceTreeNode::Leaf(opid, element)))
                    }
                } else {
                    if let Some(left) = left {
                        left.insert(index, opid, element)
                    } else {
                        *left = Some(Box::new(SequenceTreeNode::Leaf(opid, element)))
                    }
                }
            }
        }
    }

    pub fn remove(&mut self, index: usize) -> T {
        match self {
            SequenceTreeNode::Leaf(_old_opid, _old_element) => {
                unreachable!("shouldn't be calling remove on a leaf, just a node")
            }
            SequenceTreeNode::Node { left, right, len } => {
                let left_len = left.as_ref().map_or(0, |l| l.len());
                *len -= 1;
                if index > left_len {
                    if let Some(right_child) = right {
                        if let SequenceTreeNode::Leaf(_opid, _element) = &**right_child {
                            let right_child = std::mem::take(right);
                            if let SequenceTreeNode::Leaf(_, element) = *right_child.unwrap() {
                                element
                            } else {
                                unreachable!("was leaf then wasn't leaf")
                            }
                        } else {
                            right_child.remove(index - left_len)
                        }
                    } else {
                        unreachable!("no right child")
                    }
                } else {
                    if let Some(left_child) = left {
                        if let SequenceTreeNode::Leaf(_opid, _element) = &**left_child {
                            let left_child = std::mem::take(left);
                            if let SequenceTreeNode::Leaf(_, element) = *left_child.unwrap() {
                                element
                            } else {
                                unreachable!("was leaf then wasn't leaf")
                            }
                        } else {
                            left_child.remove(index)
                        }
                    } else {
                        unreachable!("no left child")
                    }
                }
            }
        }
    }

    pub fn set(&mut self, index: usize, element: T) -> T {
        match self {
            SequenceTreeNode::Leaf(_, old_element) => std::mem::replace(old_element, element),
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
            SequenceTreeNode::Leaf(opid, element) => Some((opid.clone(), element)),
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
            SequenceTreeNode::Leaf(opid, element) => Some((opid.clone(), element)),
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
