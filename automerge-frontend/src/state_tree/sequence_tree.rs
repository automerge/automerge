use std::{
    cmp::{min, Ordering},
    fmt::Debug,
};

use automerge_protocol::OpId;

#[derive(Clone, Debug)]
pub struct SequenceTree<T, const B: usize> {
    root_node: Option<SequenceTreeNode<T, B>>,
}

impl<T> Default for SequenceTree<T, 25>
where
    T: Clone + Debug,
{
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, PartialEq)]
struct SequenceTreeNode<T, const B: usize> {
    elements: Vec<Box<(OpId, T)>>,
    children: Vec<SequenceTreeNode<T, B>>,
    length: usize,
}

impl<T, const B: usize> SequenceTree<T, B>
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

    pub fn iter(&self) -> Iter<'_, T, B> {
        Iter {
            inner: self,
            index: 0,
        }
    }

    pub fn insert(&mut self, mut index: usize, opid: OpId, element: T) {
        let old_len = self.len();
        if let Some(root) = self.root_node.as_mut() {
            #[cfg(debug_assertions)]
            root.calculate_length();
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
            #[cfg(debug_assertions)]
            let len = root.calculate_length();
            let old = root.remove(index);

            if root.elements.is_empty() {
                if root.is_leaf() {
                    self.root_node = None;
                } else {
                    self.root_node = Some(root.children.remove(0));
                }
            }

            #[cfg(debug_assertions)]
            debug_assert_eq!(
                len,
                self.root_node.as_ref().map_or(0, |r| r.calculate_length()) + 1
            );
            old.1
        } else {
            panic!("remove from empty tree")
        }
    }

    pub fn set(&mut self, index: usize, element: T) -> T {
        self.root_node.as_mut().unwrap().set(index, element)
    }
}

impl<T, const B: usize> SequenceTreeNode<T, B>
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
        self.elements.len() >= 2 * B - 1
    }

    fn insert_non_full(&mut self, index: usize, opid: OpId, element: T) {
        assert!(!self.is_full());
        if self.is_leaf() {
            self.length += 1;
            self.elements.insert(index, Box::new((opid, element)));
        } else {
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

        z.elements = full_child.elements.split_off(B);

        if !full_child.is_leaf() {
            z.children = full_child.children.split_off(B);
        }

        let middle = full_child.elements.remove(B - 1);

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

    fn remove_from_leaf(&mut self, index: usize) -> Box<(OpId, T)> {
        self.length -= 1;
        self.elements.remove(index)
    }

    fn remove_element_from_non_leaf(&mut self, index: usize, child_index: usize) -> Box<(OpId, T)> {
        self.length -= 1;
        if self.children[child_index].elements.len() >= B {
            let total_index: usize = self.children[0..child_index]
                .iter()
                .map(|c| c.len() + 1)
                .sum();
            // recursively delete index - 1 in predecessor_node
            let predecessor = self.children[child_index].remove(index - 1 - total_index);
            // replace element with that one
            std::mem::replace(&mut self.elements[child_index], predecessor)
        } else {
            // predecessor_node.elements.len() < T
            if self.children[child_index + 1].elements.len() >= B {
                // recursively delete index + 1 in successor_node
                let total_index: usize = self.children[0..child_index + 1]
                    .iter()
                    .map(|c| c.len() + 1)
                    .sum();
                let successor = self.children[child_index + 1].remove(index + 1 - total_index);
                // replace element with that one
                std::mem::replace(&mut self.elements[child_index], successor)
            } else {
                let middle_element = self.elements.remove(child_index);
                let successor_child = self.children.remove(child_index + 1);
                self.children[child_index].merge(middle_element, successor_child);

                let total_index: usize = self.children[0..child_index]
                    .iter()
                    .map(|c| c.len() + 1)
                    .sum();
                self.children[child_index].remove(index - total_index)
            }
        }
    }

    fn remove_from_internal_child(
        &mut self,
        index: usize,
        mut child_index: usize,
    ) -> Box<(OpId, T)> {
        if self.children[child_index].elements.len() < B
            && if child_index > 0 {
                self.children[child_index - 1].elements.len() < B
            } else {
                true
            }
            && if child_index + 1 < self.children.len() {
                self.children[child_index + 1].elements.len() < B
            } else {
                true
            }
        {
            // if the child and its immediate siblings have t-1 elements merge the child
            // with one sibling, moving an element from this node into the new merged node
            // to be the median

            if child_index > 0 {
                let middle = self.elements.remove(child_index - 1);
                self.length -= 1;

                // use the predessor sibling
                let predecessor = self.children.remove(child_index - 1);
                self.length -= predecessor.len();
                child_index -= 1;

                self.children[child_index].elements.insert(0, middle);
                self.children[child_index].length += 1;
                self.length += 1;

                for element in predecessor.elements.into_iter().rev() {
                    self.children[child_index].elements.insert(0, element);
                    self.children[child_index].length += 1;
                    self.length += 1;
                }
                for child in predecessor.children.into_iter().rev() {
                    self.children[child_index].length += child.len();
                    self.length += child.len();
                    self.children[child_index].children.insert(0, child);
                }
            } else {
                let middle = self.elements.remove(child_index);
                self.length -= 1;

                // use the sucessor sibling
                let successor = self.children.remove(child_index + 1);
                self.length -= successor.len();

                self.children[child_index].elements.push(middle);
                self.children[child_index].length += 1;
                self.length += 1;

                for elements in successor.elements {
                    self.children[child_index].elements.push(elements);
                    self.children[child_index].length += 1;
                    self.length += 1;
                }
                for children in successor.children {
                    self.children[child_index].length += children.len();
                    self.length += children.len();
                    self.children[child_index].children.push(children);
                }
            }
        } else if self.children[child_index].elements.len() < B {
            if child_index > 0
                && self.children.get(child_index - 1).is_some()
                && self.children[child_index - 1].elements.len() >= B
            {
                let predecessor_elements_len = self.children[child_index - 1].elements.len();
                let predecessor_children_len = self.children[child_index - 1].children.len();

                let last_element = self.children[child_index - 1]
                    .elements
                    .remove(predecessor_elements_len - 1);
                assert!(!self.children[child_index - 1].elements.is_empty());
                self.children[child_index - 1].length -= 1;
                self.length -= 1;

                if !self.children[child_index - 1].is_leaf() {
                    let last_child = self.children[child_index - 1]
                        .children
                        .remove(predecessor_children_len - 1);
                    self.children[child_index - 1].length -= last_child.len();
                    self.children[child_index].length += last_child.len();
                    self.children[child_index].children.insert(0, last_child);
                }

                let parent_element =
                    std::mem::replace(&mut self.elements[child_index - 1], last_element);

                self.children[child_index]
                    .elements
                    .insert(0, parent_element);
                self.children[child_index].length += 1;
                self.length += 1;
            } else if self.children.get(child_index + 1).is_some()
                && self.children[child_index + 1].elements.len() >= B
            {
                let first_element = self.children[child_index + 1].elements.remove(0);

                assert!(!self.children[child_index + 1].elements.is_empty());

                self.children[child_index + 1].length -= 1;
                self.length -= 1;

                if !self.children[child_index + 1].is_leaf() {
                    let first_child = self.children[child_index + 1].children.remove(0);
                    self.children[child_index + 1].length -= first_child.len();
                    self.children[child_index].length += first_child.len();
                    let child_children_len = self.children[child_index].children.len();
                    self.children[child_index]
                        .children
                        .insert(child_children_len, first_child);
                }

                let parent_element =
                    std::mem::replace(&mut self.elements[child_index], first_element);

                let child_elements_len = self.children[child_index].elements.len();
                self.children[child_index].length += 1;
                self.length += 1;
                self.children[child_index]
                    .elements
                    .insert(child_elements_len, parent_element);
            }
        }
        self.length -= 1;
        let total_index: usize = self.children[0..child_index]
            .iter()
            .map(|c| c.len() + 1)
            .sum();
        self.children[child_index].remove(index - total_index)
    }

    fn calculate_length(&self) -> usize {
        let l = self.elements.len()
            + self
                .children
                .iter()
                .map(|c| c.calculate_length())
                .sum::<usize>();
        assert_eq!(self.len(), l, "{:#?}", self);

        l
    }

    pub fn remove(&mut self, index: usize) -> Box<(OpId, T)> {
        let original_len = self.len();
        if self.is_leaf() {
            let v = self.remove_from_leaf(index);
            assert_eq!(original_len, self.len() + 1);
            debug_assert_eq!(self.calculate_length(), self.len());
            v
        } else {
            let mut total_index = 0;
            for (ci, child) in self.children.iter().enumerate() {
                match (total_index + child.len()).cmp(&index) {
                    Ordering::Less => {
                        // should be later on in the loop
                        total_index += child.len() + 1;
                        continue;
                    }
                    Ordering::Equal => {
                        if ci + 1 == self.children.len() {
                            let v = self.remove_element_from_non_leaf(index, ci - 1);
                            assert_eq!(original_len, self.len() + 1);
                            debug_assert_eq!(self.calculate_length(), self.len());
                            return v;
                        } else {
                            let v = self.remove_element_from_non_leaf(index, ci);
                            assert_eq!(original_len, self.len() + 1);
                            debug_assert_eq!(self.calculate_length(), self.len());
                            return v;
                        }
                    }
                    Ordering::Greater => {
                        let v = self.remove_from_internal_child(index, ci);
                        assert_eq!(original_len, self.len() + 1);
                        debug_assert_eq!(self.calculate_length(), self.len());
                        return v;
                    }
                }
            }
            panic!(
                "index not found to remove {} {} {} {}",
                index,
                total_index,
                self.len(),
                self.calculate_length()
            );
        }
    }

    fn merge(&mut self, key: Box<(OpId, T)>, sibling: SequenceTreeNode<T, B>) {
        self.elements.push(key);
        for element in sibling.elements {
            self.elements.push(element);
        }
        for child in sibling.children {
            self.children.push(child)
        }
        self.length += sibling.length + 1;
        assert!(self.is_full());
    }

    pub fn set(&mut self, mut index: usize, element: T) -> T {
        let mut i = 0;
        if self.is_leaf() {
            let (_, old_element) = &mut **self.elements.get_mut(i).unwrap();
            std::mem::replace(old_element, element)
        } else {
            for c in &mut self.children {
                let c_len = c.len();
                match index.cmp(&c_len) {
                    Ordering::Less => {
                        return c.set(index, element);
                    }
                    Ordering::Equal => {
                        let (_, old_element) = &mut **self.elements.get_mut(i).unwrap();
                        return std::mem::replace(old_element, element);
                    }
                    Ordering::Greater => {
                        index -= c_len;
                        i += 1;
                    }
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
                match index.cmp(&c_len) {
                    Ordering::Less => {
                        return c.get(index);
                    }
                    Ordering::Equal => {
                        return self.elements.get(i).map(|b| (b.0.clone(), &b.1));
                    }
                    Ordering::Greater => {
                        index -= c_len + 1;
                        i += 1;
                    }
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
                match index.cmp(&c_len) {
                    Ordering::Less => {
                        return c.get_mut(index);
                    }
                    Ordering::Equal => {
                        return self.elements.get_mut(i).map(|b| (b.0.clone(), &mut b.1));
                    }
                    Ordering::Greater => {
                        index -= c_len + 1;
                        i += 1;
                    }
                }
            }
        }
        None
    }
}

impl<T, const B: usize> PartialEq for SequenceTree<T, B>
where
    T: Clone + Debug + PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len() && self.iter().zip(other.iter()).all(|(a, b)| a == b)
    }
}

pub struct Iter<'a, T, const B: usize> {
    inner: &'a SequenceTree<T, B>,
    index: usize,
}

impl<'a, T, const B: usize> Iterator for Iter<'a, T, B>
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
        let mut t = SequenceTree::default();
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
        let mut t = SequenceTree::default();
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
        let mut t = SequenceTree::default();
        let actor = ActorId::random();

        for i in 0..100 {
            t.insert(i % 2, actor.op_id_at(1), ());
        }
    }

    #[test]
    fn insert_book_vec() {
        let mut t = SequenceTree::default();
        let actor = ActorId::random();
        let mut v = Vec::new();

        for i in 0..100 {
            t.insert(i % 3, actor.op_id_at(1), ());
            v.insert(i % 3, ());

            assert_eq!(v, t.iter().copied().collect::<Vec<_>>())
        }
    }

    fn arb_indices() -> impl Strategy<Value = Vec<usize>> {
        proptest::collection::vec(any::<usize>(), 0..1000).prop_map(|v| {
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
            let mut t = SequenceTree::<usize, 3>::new();
            let actor = ActorId::random();
            let mut v = Vec::new();

            for i in indices{
                if i <= v.len() {
                    t.insert(i % 3, actor.op_id_at(1), i);
                    v.insert(i % 3, i);
                } else {
                    return Err(proptest::test_runner::TestCaseError::reject("index out of bounds"))
                }

                assert_eq!(v, t.iter().copied().collect::<Vec<_>>())
            }
        }

    }

    proptest! {

        #[test]
        fn proptest_remove(inserts in arb_indices(), removes in arb_indices()) {
            let mut t = SequenceTree::<usize, 3>::new();
            let actor = ActorId::random();
            let mut v = Vec::new();

            for i in inserts {
                if i <= v.len() {
                    t.insert(i , actor.op_id_at(1), i);
                    v.insert(i , i);
                } else {
                    return Err(proptest::test_runner::TestCaseError::reject("index out of bounds"))
                }

                assert_eq!(v, t.iter().copied().collect::<Vec<_>>())
            }

            for i in removes {
                if i < v.len() {
                    let tr = t.remove(i);
                    let vr = v.remove(i);
                    assert_eq!(tr, vr);
                } else {
                    return Err(proptest::test_runner::TestCaseError::reject("index out of bounds"))
                }

                assert_eq!(v, t.iter().copied().collect::<Vec<_>>())
            }
        }

    }
}
