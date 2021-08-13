use std::fmt::Debug;

use automerge_protocol::OpId;

const FULL_AMOUNT: usize = 9;

#[derive(Clone, Debug, PartialEq)]
pub struct SequenceTree<T> {
    root_node: SequenceTreeNode<T>,
}

#[derive(Clone, Debug, PartialEq)]
struct SequenceTreeNode<T> {
    elements: Vec<(OpId, T)>,
    children: Vec<Box<SequenceTreeNode<T>>>,
}

impl<T> SequenceTree<T>
where
    T: Clone + Debug,
{
    pub fn new() -> Self {
        Self {
            root_node: SequenceTreeNode {
                elements: Vec::new(),
                children: Vec::new(),
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
        println!("insert {}", index);
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
        println!("remove {}", index);
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
        self.elements.len() + self.children.iter().map(|c| c.len()).sum::<usize>()
    }

    pub fn insert(&mut self, mut index: usize, opid: OpId, element: T) {
        self.try_split();
        if self.children.is_empty() {
            // leaf node
            self.elements.insert(index, (opid, element));
        } else {
            // internal node
            for c in &mut self.children {
                let c_len = c.len();
                if index < c_len {
                    c.insert(index, opid, element);
                    break;
                } else if index == c_len {
                    self.elements.insert(index, (opid, element));
                    break;
                } else {
                    index -= c_len;
                }
            }
        }
    }

    pub fn remove(&mut self, index: usize) -> T {
        todo!()
    }

    pub fn set(&mut self, index: usize, element: T) -> T {
        todo!()
    }

    // try to split this node if it is full
    fn try_split(&mut self) {
        if self.elements.len() == FULL_AMOUNT {
            // do the split
            todo!("split")
        }
    }

    pub fn get(&self, mut index: usize) -> Option<(OpId, &T)> {
        let mut i = 0;
        if self.children.is_empty() {
            return self.elements.get(index).map(|(o, t)| (o.clone(), t));
        } else {
            for c in &self.children {
                let c_len = c.len();
                if index < c_len {
                    return c.get(index);
                } else if index == c_len {
                    return self.elements.get(i).map(|(o, t)| (o.clone(), t));
                } else {
                    index -= c_len;
                    i += 1;
                }
            }
        }
        None
    }

    pub fn get_mut(&mut self, mut index: usize) -> Option<(OpId, &mut T)> {
        let mut i = 0;
        if self.children.is_empty() {
            return self.elements.get_mut(index).map(|(o, t)| (o.clone(), t));
        } else {
            for c in &mut self.children {
                let c_len = c.len();
                if index < c_len {
                    return c.get_mut(index);
                } else if index == c_len {
                    return self.elements.get_mut(i).map(|(o, t)| (o.clone(), t));
                } else {
                    index -= c_len;
                    i += 1;
                }
            }
        }
        None
    }
}
