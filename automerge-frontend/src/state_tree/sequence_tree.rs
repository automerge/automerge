use std::fmt::Debug;

use automerge_protocol::{ActorId, OpId};

#[derive(Clone, Debug, PartialEq)]
pub struct SequenceTree<T> {
    root_node: SequenceTreeNode<T>,
}

#[derive(Clone, Debug, PartialEq)]
enum SequenceTreeInner<T> {
    Leaf(OpId, T),
    Node {
        left: Option<Box<SequenceTreeNode<T>>>,
        right: Option<Box<SequenceTreeNode<T>>>,
        len: usize,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct SequenceTreeNode<T> {
    inner: SequenceTreeInner<T>,
}

impl<T> SequenceTree<T>
where
    T: Clone + Debug,
{
    pub fn new() -> Self {
        Self {
            root_node: SequenceTreeNode {
                inner: SequenceTreeInner::Node {
                    left: None,
                    right: None,
                    len: 0,
                },
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
        match self.inner {
            SequenceTreeInner::Leaf(..) => 1,
            SequenceTreeInner::Node { len, .. } => len,
        }
    }

    pub fn insert(&mut self, index: usize, opid: OpId, element: T) {
        match &mut self.inner {
            SequenceTreeInner::Leaf(old_opid, old_element) => {
                let left = Some(Box::new(SequenceTreeNode {
                    inner: SequenceTreeInner::Leaf(old_opid.clone(), old_element.clone()),
                }));
                let right = Some(Box::new(SequenceTreeNode {
                    inner: SequenceTreeInner::Leaf(opid, element),
                }));
                self.inner = SequenceTreeInner::Node {
                    left,
                    right,
                    len: 2,
                };
            }
            SequenceTreeInner::Node { left, right, len } => {
                let left_len = left.as_ref().map_or(0, |l| l.len());
                *len += 1;
                if index > left_len {
                    if let Some(right) = right {
                        right.insert(index - left_len, opid, element)
                    } else {
                        *right = Some(Box::new(SequenceTreeNode {
                            inner: SequenceTreeInner::Leaf(opid, element),
                        }))
                    }
                } else {
                    if let Some(left) = left {
                        left.insert(index, opid, element)
                    } else {
                        *left = Some(Box::new(SequenceTreeNode {
                            inner: SequenceTreeInner::Leaf(opid, element),
                        }))
                    }
                }
            }
        }
    }

    pub fn remove(&mut self, index: usize) -> T {
        match &mut self.inner {
            SequenceTreeInner::Leaf(old_opid, old_element) => {
                unreachable!("shouldn't be calling remove on a leaf, just a node")
            }
            SequenceTreeInner::Node { left, right, len } => {
                let left_len = left.as_ref().map_or(0, |l| l.len());
                *len -= 1;
                if index > left_len {
                    if let Some(right_child) = right {
                        if let SequenceTreeInner::Leaf(opid, element) = &right_child.inner {
                            let el = element.clone();
                            *right = None;
                            el
                        } else {
                            right_child.remove(index - left_len)
                        }
                    } else {
                        unreachable!("no right child")
                    }
                } else {
                    if let Some(left_child) = left {
                        if let SequenceTreeInner::Leaf(opid, element) = &left_child.inner {
                            let el = element.clone();
                            *left = None;
                            el
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
        match &mut self.inner {
            SequenceTreeInner::Leaf(_, old_element) => std::mem::replace(old_element, element),
            SequenceTreeInner::Node { left, right, len } => {
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
        match &self.inner {
            SequenceTreeInner::Leaf(opid, element) => Some((opid.clone(), element)),
            SequenceTreeInner::Node { left, right, len } => {
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
        match &mut self.inner {
            SequenceTreeInner::Leaf(opid, element) => Some((opid.clone(), element)),
            SequenceTreeInner::Node { left, right, len } => {
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
