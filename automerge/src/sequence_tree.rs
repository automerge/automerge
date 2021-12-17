use std::{
    cmp::{min, Ordering},
    fmt::Debug,
    mem,
};

pub type SequenceTree<T> = SequenceTreeInternal<T, 25>;

#[derive(Clone, Debug)]
pub struct SequenceTreeInternal<T, const B: usize> {
    root_node: Option<SequenceTreeNode<T, B>>,
}

#[derive(Clone, Debug, PartialEq)]
struct SequenceTreeNode<T, const B: usize> {
    elements: Vec<T>,
    children: Vec<SequenceTreeNode<T, B>>,
    length: usize,
}

impl<T, const B: usize> SequenceTreeInternal<T, B>
where
    T: Clone + Debug,
{
    /// Construct a new, empty, sequence.
    pub fn new() -> Self {
        Self { root_node: None }
    }

    /// Get the length of the sequence.
    pub fn len(&self) -> usize {
        self.root_node.as_ref().map_or(0, |n| n.len())
    }

    /// Check if the sequence is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Create an iterator through the sequence.
    pub fn iter(&self) -> Iter<'_, T, B> {
        Iter {
            inner: self,
            index: 0,
        }
    }

    /// Insert the `element` into the sequence at `index`.
    ///
    /// # Panics
    ///
    /// Panics if `index > len`.
    pub fn insert(&mut self, index: usize, element: T) {
        let old_len = self.len();
        if let Some(root) = self.root_node.as_mut() {
            #[cfg(debug_assertions)]
            root.check();

            if root.is_full() {
                let original_len = root.len();
                let new_root = SequenceTreeNode::new();

                // move new_root to root position
                let old_root = mem::replace(root, new_root);

                root.length += old_root.len();
                root.children.push(old_root);
                root.split_child(0);

                assert_eq!(original_len, root.len());

                // after splitting the root has one element and two children, find which child the
                // index is in
                let first_child_len = root.children[0].len();
                let (child, insertion_index) = if first_child_len < index {
                    (&mut root.children[1], index - (first_child_len + 1))
                } else {
                    (&mut root.children[0], index)
                };
                root.length += 1;
                child.insert_into_non_full_node(insertion_index, element)
            } else {
                root.insert_into_non_full_node(index, element)
            }
        } else {
            self.root_node = Some(SequenceTreeNode {
                elements: vec![element],
                children: Vec::new(),
                length: 1,
            })
        }
        assert_eq!(self.len(), old_len + 1, "{:#?}", self);
    }

    /// Push the `element` onto the back of the sequence.
    pub fn push(&mut self, element: T) {
        let l = self.len();
        self.insert(l, element)
    }

    /// Get the `element` at `index` in the sequence.
    pub fn get(&self, index: usize) -> Option<&T> {
        self.root_node.as_ref().and_then(|n| n.get(index))
    }

    /// Get the `element` at `index` in the sequence.
    pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        self.root_node.as_mut().and_then(|n| n.get_mut(index))
    }

    /// Removes the element at `index` from the sequence.
    ///
    /// # Panics
    ///
    /// Panics if `index` is out of bounds.
    pub fn remove(&mut self, index: usize) -> T {
        if let Some(root) = self.root_node.as_mut() {
            #[cfg(debug_assertions)]
            let len = root.check();
            let old = root.remove(index);

            if root.elements.is_empty() {
                if root.is_leaf() {
                    self.root_node = None;
                } else {
                    self.root_node = Some(root.children.remove(0));
                }
            }

            #[cfg(debug_assertions)]
            debug_assert_eq!(len, self.root_node.as_ref().map_or(0, |r| r.check()) + 1);
            old
        } else {
            panic!("remove from empty tree")
        }
    }

    /// Update the `element` at `index` in the sequence, returning the old value.
    ///
    /// # Panics
    ///
    /// Panics if `index > len`
    pub fn set(&mut self, index: usize, element: T) -> T {
        self.root_node.as_mut().unwrap().set(index, element)
    }
}

impl<T, const B: usize> SequenceTreeNode<T, B>
where
    T: Clone + Debug,
{
    fn new() -> Self {
        Self {
            elements: Vec::new(),
            children: Vec::new(),
            length: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.length
    }

    fn is_leaf(&self) -> bool {
        self.children.is_empty()
    }

    fn is_full(&self) -> bool {
        self.elements.len() >= 2 * B - 1
    }

    /// Returns the child index and the given index adjusted for the cumulative index before that
    /// child.
    fn find_child_index(&self, index: usize) -> (usize, usize) {
        let mut cumulative_len = 0;
        for (child_index, child) in self.children.iter().enumerate() {
            if cumulative_len + child.len() >= index {
                return (child_index, index - cumulative_len);
            } else {
                cumulative_len += child.len() + 1;
            }
        }
        panic!("index not found in node")
    }

    fn insert_into_non_full_node(&mut self, index: usize, element: T) {
        assert!(!self.is_full());
        if self.is_leaf() {
            self.length += 1;
            self.elements.insert(index, element);
        } else {
            let (child_index, sub_index) = self.find_child_index(index);
            let child = &mut self.children[child_index];

            if child.is_full() {
                self.split_child(child_index);

                // child structure has changed so we need to find the index again
                let (child_index, sub_index) = self.find_child_index(index);
                let child = &mut self.children[child_index];
                child.insert_into_non_full_node(sub_index, element);
            } else {
                child.insert_into_non_full_node(sub_index, element);
            }
            self.length += 1;
        }
    }

    // A utility function to split the child `full_child_index` of this node
    // Note that `full_child_index` must be full when this function is called.
    fn split_child(&mut self, full_child_index: usize) {
        let original_len_self = self.len();

        // Create a new node which is going to store (B-1) keys
        // of the full child.
        let mut successor_sibling = SequenceTreeNode::new();

        let full_child = &mut self.children[full_child_index];
        let original_len = full_child.len();
        assert!(full_child.is_full());

        successor_sibling.elements = full_child.elements.split_off(B);

        if !full_child.is_leaf() {
            successor_sibling.children = full_child.children.split_off(B);
        }

        let middle = full_child.elements.pop().unwrap();

        full_child.length =
            full_child.elements.len() + full_child.children.iter().map(|c| c.len()).sum::<usize>();

        successor_sibling.length = successor_sibling.elements.len()
            + successor_sibling
                .children
                .iter()
                .map(|c| c.len())
                .sum::<usize>();

        let z_len = successor_sibling.len();

        let full_child_len = full_child.len();

        self.children
            .insert(full_child_index + 1, successor_sibling);

        self.elements.insert(full_child_index, middle);

        assert_eq!(full_child_len + z_len + 1, original_len, "{:#?}", self);

        assert_eq!(original_len_self, self.len());
    }

    fn remove_from_leaf(&mut self, index: usize) -> T {
        self.length -= 1;
        self.elements.remove(index)
    }

    fn remove_element_from_non_leaf(&mut self, index: usize, element_index: usize) -> T {
        self.length -= 1;
        if self.children[element_index].elements.len() >= B {
            let total_index = self.cumulative_index(element_index);
            // recursively delete index - 1 in predecessor_node
            let predecessor = self.children[element_index].remove(index - 1 - total_index);
            // replace element with that one
            mem::replace(&mut self.elements[element_index], predecessor)
        } else if self.children[element_index + 1].elements.len() >= B {
            // recursively delete index + 1 in successor_node
            let total_index = self.cumulative_index(element_index + 1);
            let successor = self.children[element_index + 1].remove(index + 1 - total_index);
            // replace element with that one
            mem::replace(&mut self.elements[element_index], successor)
        } else {
            let middle_element = self.elements.remove(element_index);
            let successor_child = self.children.remove(element_index + 1);
            self.children[element_index].merge(middle_element, successor_child);

            let total_index = self.cumulative_index(element_index);
            self.children[element_index].remove(index - total_index)
        }
    }

    fn cumulative_index(&self, child_index: usize) -> usize {
        self.children[0..child_index]
            .iter()
            .map(|c| c.len() + 1)
            .sum()
    }

    fn remove_from_internal_child(&mut self, index: usize, mut child_index: usize) -> T {
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
            // if the child and its immediate siblings have B-1 elements merge the child
            // with one sibling, moving an element from this node into the new merged node
            // to be the median

            if child_index > 0 {
                let middle = self.elements.remove(child_index - 1);

                // use the predessor sibling
                let successor = self.children.remove(child_index);
                child_index -= 1;

                self.children[child_index].merge(middle, successor);
            } else {
                let middle = self.elements.remove(child_index);

                // use the sucessor sibling
                let successor = self.children.remove(child_index + 1);

                self.children[child_index].merge(middle, successor);
            }
        } else if self.children[child_index].elements.len() < B {
            if child_index > 0
                && self
                    .children
                    .get(child_index - 1)
                    .map_or(false, |c| c.elements.len() >= B)
            {
                let last_element = self.children[child_index - 1].elements.pop().unwrap();
                assert!(!self.children[child_index - 1].elements.is_empty());
                self.children[child_index - 1].length -= 1;

                let parent_element =
                    mem::replace(&mut self.elements[child_index - 1], last_element);

                self.children[child_index]
                    .elements
                    .insert(0, parent_element);
                self.children[child_index].length += 1;

                if let Some(last_child) = self.children[child_index - 1].children.pop() {
                    self.children[child_index - 1].length -= last_child.len();
                    self.children[child_index].length += last_child.len();
                    self.children[child_index].children.insert(0, last_child);
                }
            } else if self
                .children
                .get(child_index + 1)
                .map_or(false, |c| c.elements.len() >= B)
            {
                let first_element = self.children[child_index + 1].elements.remove(0);
                self.children[child_index + 1].length -= 1;

                assert!(!self.children[child_index + 1].elements.is_empty());

                let parent_element = mem::replace(&mut self.elements[child_index], first_element);

                self.children[child_index].length += 1;
                self.children[child_index].elements.push(parent_element);

                if !self.children[child_index + 1].is_leaf() {
                    let first_child = self.children[child_index + 1].children.remove(0);
                    self.children[child_index + 1].length -= first_child.len();
                    self.children[child_index].length += first_child.len();

                    self.children[child_index].children.push(first_child);
                }
            }
        }
        self.length -= 1;
        let total_index = self.cumulative_index(child_index);
        self.children[child_index].remove(index - total_index)
    }

    fn check(&self) -> usize {
        let l = self.elements.len() + self.children.iter().map(|c| c.check()).sum::<usize>();
        assert_eq!(self.len(), l, "{:#?}", self);

        l
    }

    pub fn remove(&mut self, index: usize) -> T {
        let original_len = self.len();
        if self.is_leaf() {
            let v = self.remove_from_leaf(index);
            assert_eq!(original_len, self.len() + 1);
            debug_assert_eq!(self.check(), self.len());
            v
        } else {
            let mut total_index = 0;
            for (child_index, child) in self.children.iter().enumerate() {
                match (total_index + child.len()).cmp(&index) {
                    Ordering::Less => {
                        // should be later on in the loop
                        total_index += child.len() + 1;
                        continue;
                    }
                    Ordering::Equal => {
                        let v = self.remove_element_from_non_leaf(
                            index,
                            min(child_index, self.elements.len() - 1),
                        );
                        assert_eq!(original_len, self.len() + 1);
                        debug_assert_eq!(self.check(), self.len());
                        return v;
                    }
                    Ordering::Greater => {
                        let v = self.remove_from_internal_child(index, child_index);
                        assert_eq!(original_len, self.len() + 1);
                        debug_assert_eq!(self.check(), self.len());
                        return v;
                    }
                }
            }
            panic!(
                "index not found to remove {} {} {} {}",
                index,
                total_index,
                self.len(),
                self.check()
            );
        }
    }

    fn merge(&mut self, middle: T, successor_sibling: SequenceTreeNode<T, B>) {
        self.elements.push(middle);
        self.elements.extend(successor_sibling.elements);
        self.children.extend(successor_sibling.children);
        self.length += successor_sibling.length + 1;
        assert!(self.is_full());
    }

    pub fn set(&mut self, index: usize, element: T) -> T {
        if self.is_leaf() {
            let old_element = self.elements.get_mut(index).unwrap();
            mem::replace(old_element, element)
        } else {
            let mut cumulative_len = 0;
            for (child_index, child) in self.children.iter_mut().enumerate() {
                match (cumulative_len + child.len()).cmp(&index) {
                    Ordering::Less => {
                        cumulative_len += child.len() + 1;
                    }
                    Ordering::Equal => {
                        let old_element = self.elements.get_mut(child_index).unwrap();
                        return mem::replace(old_element, element);
                    }
                    Ordering::Greater => {
                        return child.set(index - cumulative_len, element);
                    }
                }
            }
            panic!("Invalid index to set: {} but len was {}", index, self.len())
        }
    }

    pub fn get(&self, index: usize) -> Option<&T> {
        if self.is_leaf() {
            return self.elements.get(index);
        } else {
            let mut cumulative_len = 0;
            for (child_index, child) in self.children.iter().enumerate() {
                match (cumulative_len + child.len()).cmp(&index) {
                    Ordering::Less => {
                        cumulative_len += child.len() + 1;
                    }
                    Ordering::Equal => return self.elements.get(child_index),
                    Ordering::Greater => {
                        return child.get(index - cumulative_len);
                    }
                }
            }
        }
        None
    }

    pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        if self.is_leaf() {
            return self.elements.get_mut(index);
        } else {
            let mut cumulative_len = 0;
            for (child_index, child) in self.children.iter_mut().enumerate() {
                match (cumulative_len + child.len()).cmp(&index) {
                    Ordering::Less => {
                        cumulative_len += child.len() + 1;
                    }
                    Ordering::Equal => return self.elements.get_mut(child_index),
                    Ordering::Greater => {
                        return child.get_mut(index - cumulative_len);
                    }
                }
            }
        }
        None
    }
}

impl<T, const B: usize> Default for SequenceTreeInternal<T, B>
where
    T: Clone + Debug,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T, const B: usize> PartialEq for SequenceTreeInternal<T, B>
where
    T: Clone + Debug + PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len() && self.iter().zip(other.iter()).all(|(a, b)| a == b)
    }
}

impl<'a, T, const B: usize> IntoIterator for &'a SequenceTreeInternal<T, B>
where
    T: Clone + Debug,
{
    type Item = &'a T;

    type IntoIter = Iter<'a, T, B>;

    fn into_iter(self) -> Self::IntoIter {
        Iter {
            inner: self,
            index: 0,
        }
    }
}

pub struct Iter<'a, T, const B: usize> {
    inner: &'a SequenceTreeInternal<T, B>,
    index: usize,
}

impl<'a, T, const B: usize> Iterator for Iter<'a, T, B>
where
    T: Clone + Debug,
{
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        self.index += 1;
        self.inner.get(self.index - 1)
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.index += n + 1;
        self.inner.get(self.index - 1)
    }
}

#[cfg(test)]
mod tests {
    use crate::ActorId;

    use super::*;

    #[test]
    fn push_back() {
        let mut t = SequenceTree::new();
        let actor = ActorId::random();

        t.push(actor.op_id_at(1));
        t.push(actor.op_id_at(2));
        t.push(actor.op_id_at(3));
        t.push(actor.op_id_at(4));
        t.push(actor.op_id_at(5));
        t.push(actor.op_id_at(6));
        t.push(actor.op_id_at(8));
        t.push(actor.op_id_at(100));
    }

    #[test]
    fn insert() {
        let mut t = SequenceTree::new();
        let actor = ActorId::random();

        t.insert(0, actor.op_id_at(1));
        t.insert(1, actor.op_id_at(1));
        t.insert(0, actor.op_id_at(1));
        t.insert(0, actor.op_id_at(1));
        t.insert(0, actor.op_id_at(1));
        t.insert(3, actor.op_id_at(1));
        t.insert(4, actor.op_id_at(1));
    }

    #[test]
    fn insert_book() {
        let mut t = SequenceTree::new();

        for i in 0..100 {
            t.insert(i % 2, ());
        }
    }

    #[test]
    fn insert_book_vec() {
        let mut t = SequenceTree::new();
        let mut v = Vec::new();

        for i in 0..100 {
            t.insert(i % 3, ());
            v.insert(i % 3, ());

            assert_eq!(v, t.iter().copied().collect::<Vec<_>>())
        }
    }

    /*
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
    */

    //    use proptest::prelude::*;

    /*
        proptest! {

            #[test]
            fn proptest_insert(indices in arb_indices()) {
                let mut t = SequenceTreeInternal::<usize, 3>::new();
                let actor = ActorId::random();
                let mut v = Vec::new();

                for i in indices{
                    if i <= v.len() {
                        t.insert(i % 3, i);
                        v.insert(i % 3, i);
                    } else {
                        return Err(proptest::test_runner::TestCaseError::reject("index out of bounds"))
                    }

                    assert_eq!(v, t.iter().copied().collect::<Vec<_>>())
                }
            }

        }
    */

    /*
        proptest! {

            #[test]
            fn proptest_remove(inserts in arb_indices(), removes in arb_indices()) {
                let mut t = SequenceTreeInternal::<usize, 3>::new();
                let actor = ActorId::random();
                let mut v = Vec::new();

                for i in inserts {
                    if i <= v.len() {
                        t.insert(i , i);
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
    */
}
