use std::fmt::Debug;

use automerge_protocol::OpId;

const T: usize = 3;

#[derive(Clone, Debug)]
pub struct SequenceTree<T> {
    root_node: Option<SequenceTreeNode<T>>,
}

#[derive(Clone, Debug, PartialEq)]
struct SequenceTreeNode<T> {
    elements: Vec<Box<(OpId, T)>>,
    children: Vec<SequenceTreeNode<T>>,
    length: usize,
}

impl<T> SequenceTree<T>
where
    T: Clone + Debug,
{
    pub fn new() -> Self {
        Self { root_node: None }
    }

    pub fn len(&self) -> usize {
        self.root_node.as_ref().map_or(0, |n| n.len())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn iter(&self) -> Iter<'_, T> {
        Iter {
            inner: self,
            index: 0,
        }
    }

    pub fn insert(&mut self, mut index: usize, opid: OpId, element: T) {
        let old_len = self.len();
        if let Some(root) = self.root_node.as_mut() {
            if root.is_full() {
                let original_len = root.len();
                let new_root = SequenceTreeNode {
                    elements: Vec::new(),
                    children: Vec::new(),
                    length: 0,
                };

                // move new_root to root position
                let old_root = std::mem::replace(root, new_root);

                root.length += old_root.len();
                root.children.push(old_root);
                root.split_child(0);

                let mut i = 0;
                if root.children[0].len() < index {
                    i += 1;
                    index -= root.children[0].len() + 1
                }
                assert_eq!(original_len, root.len());
                root.length += 1;
                root.children[i].insert_non_full(index, opid, element)
            } else {
                root.insert_non_full(index, opid, element)
            }
        } else {
            self.root_node = Some(SequenceTreeNode {
                elements: vec![Box::new((opid, element))],
                children: Vec::new(),
                length: 1,
            })
        }
        assert_eq!(self.len(), old_len + 1, "{:#?}", self);
    }

    pub fn push_back(&mut self, opid: OpId, element: T) {
        let l = self.len();
        self.insert(l, opid, element)
    }

    pub fn get(&self, index: usize) -> Option<(OpId, &T)> {
        self.root_node.as_ref().and_then(|n| n.get(index))
    }

    pub fn get_mut(&mut self, index: usize) -> Option<(OpId, &mut T)> {
        self.root_node.as_mut().and_then(|n| n.get_mut(index))
    }

    pub fn remove(&mut self, index: usize) -> T {
        if let Some(root) = self.root_node.as_mut() {
            let old = root.remove(index);

            if root.elements.is_empty() {
                if root.is_leaf() {
                    self.root_node = None;
                } else {
                    self.root_node = Some(root.children[0].clone());
                }
            }

            old
        } else {
            panic!("remove from empty tree")
        }
    }

    pub fn set(&mut self, index: usize, element: T) -> T {
        self.root_node.as_mut().unwrap().set(index, element)
    }
}

impl<T> SequenceTreeNode<T>
where
    T: Clone + Debug,
{
    pub fn len(&self) -> usize {
        self.length
    }

    fn is_leaf(&self) -> bool {
        self.children.is_empty()
    }

    fn is_full(&self) -> bool {
        self.elements.len() >= 2 * T - 1
    }

    fn insert_non_full(&mut self, index: usize, opid: OpId, element: T) {
        assert!(!self.is_full());
        if self.is_leaf() {
            // leaf

            self.length += 1;
            self.elements.insert(index, Box::new((opid, element)));
        } else {
            // not a leaf

            let num_children = self.children.len();
            let mut cumulative_len = 0;
            for (child_index, c) in self.children.iter_mut().enumerate() {
                if cumulative_len + c.len() >= index {
                    // insert into c
                    if c.is_full() {
                        self.split_child(child_index);

                        let mut cumulative_len = 0;
                        for c in self.children.iter_mut() {
                            if cumulative_len + c.len() >= index {
                                c.insert_non_full(index - cumulative_len, opid, element);
                                self.length += 1;
                                break;
                            } else {
                                cumulative_len += c.len() + 1;
                            }
                        }
                    } else {
                        c.insert_non_full(index - cumulative_len, opid, element);
                        self.length += 1;
                    }
                    break;
                } else if child_index == num_children - 1 {
                    c.insert_non_full(index - cumulative_len, opid, element);
                    self.length += 1;
                    break;
                } else {
                    cumulative_len += c.len() + 1
                }
            }
        }
    }

    // A utility function to split the child y of this node
    // Note that y must be full when this function is called
    fn split_child(&mut self, full_child_index: usize) {
        let original_len_self = self.len();
        // Create a new node which is going to store (t-1) keys
        // of y
        let mut z = SequenceTreeNode {
            elements: Vec::new(),
            children: Vec::new(),
            length: 0,
        };

        let full_child = &mut self.children[full_child_index];
        let original_len = full_child.len();
        assert!(full_child.is_full());

        z.elements = full_child.elements.split_off(T);

        if !full_child.is_leaf() {
            z.children = full_child.children.split_off(T);
        }

        let middle = full_child.elements.remove(T - 1);

        full_child.length =
            full_child.elements.len() + full_child.children.iter().map(|c| c.len()).sum::<usize>();
        z.length = z.elements.len() + z.children.iter().map(|c| c.len()).sum::<usize>();

        let z_len = z.len();

        let full_child_len = full_child.len();

        self.children.insert(full_child_index + 1, z);

        self.elements.insert(full_child_index, middle);

        assert_eq!(full_child_len + z_len + 1, original_len, "{:#?}", self);

        assert_eq!(original_len_self, self.len());
    }

    fn remove_from_leaf(&mut self, index: usize) -> T {
        self.elements.remove(index).1
    }

    fn remove_from_non_leaf(&mut self, index: usize) -> T {
        todo!()
    }

    pub fn remove(&mut self, index: usize) -> T {
        let mut total_index = 0;
        for (ci, child) in self.children.iter().enumerate() {
            if total_index + child.len() > index {
                // in that child
                todo!("remove in a child")
            } else if total_index + child.len() == index {
                // in this node
                if self.is_leaf() {
                    return self.remove_from_leaf(ci);
                } else {
                    todo!("delete internal key")
                }
            } else {
                // should be later on in the loop
                total_index += child.len();
                continue;
            }
        }
        panic!("index not found to remove")
    }

    pub fn set(&mut self, mut index: usize, element: T) -> T {
        let mut i = 0;
        if self.is_leaf() {
            let (_, old_element) = &mut **self.elements.get_mut(i).unwrap();
            std::mem::replace(old_element, element)
        } else {
            for c in &mut self.children {
                let c_len = c.len();
                if index < c_len {
                    return c.set(index, element);
                } else if index == c_len {
                    let (_, old_element) = &mut **self.elements.get_mut(i).unwrap();
                    return std::mem::replace(old_element, element);
                } else {
                    index -= c_len;
                    i += 1;
                }
            }
            panic!("Invalid index to set")
        }
    }

    pub fn get(&self, mut index: usize) -> Option<(OpId, &T)> {
        let mut i = 0;
        if self.is_leaf() {
            return self.elements.get(index).map(|b| (b.0.clone(), &b.1));
        } else {
            for c in &self.children {
                let c_len = c.len();
                if index < c_len {
                    return c.get(index);
                } else if index == c_len {
                    return self.elements.get(i).map(|b| (b.0.clone(), &b.1));
                } else {
                    index -= c_len + 1;
                    i += 1;
                }
            }
        }
        None
    }

    pub fn get_mut(&mut self, mut index: usize) -> Option<(OpId, &mut T)> {
        let mut i = 0;
        if self.is_leaf() {
            return self
                .elements
                .get_mut(index)
                .map(|b| (b.0.clone(), &mut b.1));
        } else {
            for c in &mut self.children {
                let c_len = c.len();
                if index < c_len {
                    return c.get_mut(index);
                } else if index == c_len {
                    return self.elements.get_mut(i).map(|b| (b.0.clone(), &mut b.1));
                } else {
                    index -= c_len + 1;
                    i += 1;
                }
            }
        }
        None
    }
}

impl<T> PartialEq for SequenceTree<T>
where
    T: Clone + Debug + PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len() && self.iter().zip(other.iter()).all(|(a, b)| a == b)
    }
}

pub struct Iter<'a, T> {
    inner: &'a SequenceTree<T>,
    index: usize,
}

impl<'a, T> Iterator for Iter<'a, T>
where
    T: Clone + Debug,
{
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        self.index += 1;
        self.inner.get(self.index - 1).map(|(_, t)| t)
    }
}

#[cfg(test)]
mod tests {
    use automerge_protocol::ActorId;

    use super::*;

    #[test]
    fn push_back() {
        let mut t = SequenceTree::new();
        let actor = ActorId::random();

        t.push_back(actor.op_id_at(1), ());
        t.push_back(actor.op_id_at(2), ());
        t.push_back(actor.op_id_at(3), ());
        t.push_back(actor.op_id_at(4), ());
        t.push_back(actor.op_id_at(5), ());
        t.push_back(actor.op_id_at(6), ());
        t.push_back(actor.op_id_at(8), ());
        t.push_back(actor.op_id_at(100), ());
    }

    #[test]
    fn insert() {
        let mut t = SequenceTree::new();
        let actor = ActorId::random();

        t.insert(0, actor.op_id_at(1), ());
        t.insert(1, actor.op_id_at(1), ());
        t.insert(0, actor.op_id_at(1), ());
        t.insert(0, actor.op_id_at(1), ());
        t.insert(0, actor.op_id_at(1), ());
        t.insert(3, actor.op_id_at(1), ());
        t.insert(4, actor.op_id_at(1), ());
    }

    #[test]
    fn insert_book() {
        let mut t = SequenceTree::new();
        let actor = ActorId::random();

        for i in 0..100 {
            t.insert(i % 2, actor.op_id_at(1), ());
        }
    }

    #[test]
    fn insert_book_vec() {
        let mut t = SequenceTree::new();
        let actor = ActorId::random();
        let mut v = Vec::new();

        for i in 0..100 {
            t.insert(i % 3, actor.op_id_at(1), ());
            v.insert(i % 3, ());

            assert_eq!(v, t.iter().copied().collect::<Vec<_>>())
        }
    }

    fn arb_indices() -> impl Strategy<Value = Vec<usize>> {
        proptest::collection::vec(any::<usize>(), 0..10).prop_map(|v| {
            let mut len = 0;
            v.into_iter()
                .map(|i| {
                    len += 1;
                    i % len
                })
                .collect::<Vec<_>>()
        })
    }

    use proptest::prelude::*;

    proptest! {

        #[test]
        fn proptest_insert(indices in arb_indices()) {
            let mut t = SequenceTree::new();
            let actor = ActorId::random();
            let mut v = Vec::new();

            for i in indices{
                if i <= v.len() {
                    t.insert(i % 3, actor.op_id_at(1), ());
                    v.insert(i % 3, ());
                } else {
                    return Err(proptest::test_runner::TestCaseError::reject("index out of bounds"))
                }

                assert_eq!(v, t.iter().copied().collect::<Vec<_>>())
            }
        }

    }
}
