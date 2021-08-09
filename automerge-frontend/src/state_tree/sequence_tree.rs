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
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct SequenceTreeNode<T> {
    inner: SequenceTreeInner<T>,
    len: usize,
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
                },
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
        todo!()
    }
}

impl<T> SequenceTreeNode<T>
where
    T: Clone + Debug,
{
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn insert(&mut self, index: usize, opid: OpId, element: T) {
        match &mut self.inner {
            SequenceTreeInner::Leaf(old_opid, old_element) => {
                let left = Some(Box::new(SequenceTreeNode {
                    inner: SequenceTreeInner::Leaf(old_opid.clone(), old_element.clone()),
                    len: 1,
                }));
                let right = Some(Box::new(SequenceTreeNode {
                    inner: SequenceTreeInner::Leaf(opid, element),
                    len: 1,
                }));
                self.inner = SequenceTreeInner::Node { left, right };
                self.len = 2;
            }
            SequenceTreeInner::Node { left, right } => {
                let left_len = left.as_ref().map_or(0, |l| l.len());
                self.len += 1;
                if index > left_len {
                    if let Some(right) = right {
                        right.insert(index - left_len, opid, element)
                    } else {
                        *right = Some(Box::new(SequenceTreeNode {
                            inner: SequenceTreeInner::Leaf(opid, element),
                            len: 1,
                        }))
                    }
                } else {
                    if let Some(left) = left {
                        left.insert(index, opid, element)
                    } else {
                        *left = Some(Box::new(SequenceTreeNode {
                            inner: SequenceTreeInner::Leaf(opid, element),
                            len: 1,
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
            SequenceTreeInner::Node { left, right } => {
                let left_len = left.as_ref().map_or(0, |l| l.len());
                self.len -= 1;
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

    pub fn get(&self, index: usize) -> Option<(OpId, &T)> {
        match &self.inner {
            SequenceTreeInner::Leaf(opid, element) => Some((opid.clone(), element)),
            SequenceTreeInner::Node { left, right } => {
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
            SequenceTreeInner::Node { left, right } => {
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

impl<T> SequenceTreeInner<T>
where
    T: Clone + Debug,
{
    fn len(&self) -> usize {
        match self {
            Self::Leaf(..) => 1,
            Self::Node { left, right } => {
                left.as_ref().map_or(0, |l| l.len()) + right.as_ref().map_or(0, |r| r.len())
            }
        }
    }
}
