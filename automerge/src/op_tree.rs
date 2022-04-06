use std::{
    cmp::{min, Ordering},
    fmt::Debug,
    mem,
};

pub(crate) use crate::op_set::OpSetMetadata;
use crate::types::{Op, OpId};
use crate::{
    clock::Clock,
    query::{self, Index, QueryResult, TreeQuery},
};
use std::collections::HashSet;

pub(crate) const B: usize = 16;

#[allow(dead_code)]
pub(crate) type OpTree = OpTreeInternal;

#[derive(Clone, Debug)]
pub(crate) struct OpTreeInternal {
    pub(crate) root_node: Option<OpTreeNode>,
}

#[derive(Clone, Debug)]
pub(crate) struct OpTreeNode {
    pub(crate) elements: Vec<Op>,
    pub(crate) children: Vec<OpTreeNode>,
    pub index: Index,
    length: usize,
}

impl OpTreeInternal {
    /// Construct a new, empty, sequence.
    pub fn new() -> Self {
        Self { root_node: None }
    }

    /// Get the length of the sequence.
    pub fn len(&self) -> usize {
        self.root_node.as_ref().map_or(0, |n| n.len())
    }

    pub fn keys(&self) -> Option<query::Keys> {
        self.root_node.as_ref().map(query::Keys::new)
    }

    pub fn keys_at(&self, clock: Clock) -> Option<query::KeysAt> {
        self.root_node
            .as_ref()
            .map(|root| query::KeysAt::new(root, clock))
    }

    pub fn search<'a, 'b: 'a, Q>(&'b self, mut query: Q, m: &OpSetMetadata) -> Q
    where
        Q: TreeQuery<'a>,
    {
        self.root_node
            .as_ref()
            .map(|root| match query.query_node_with_metadata(root, m) {
                QueryResult::Descend => root.search(&mut query, m),
                _ => true,
            });
        query
    }

    /// Create an iterator through the sequence.
    pub fn iter(&self) -> Iter {
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
    pub fn insert(&mut self, index: usize, element: Op) {
        let old_len = self.len();
        if let Some(root) = self.root_node.as_mut() {
            #[cfg(debug_assertions)]
            root.check();

            if root.is_full() {
                let original_len = root.len();
                let new_root = OpTreeNode::new();

                // move new_root to root position
                let old_root = mem::replace(root, new_root);

                root.length += old_root.len();
                root.index = old_root.index.clone();
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
                root.index.insert(&element);
                child.insert_into_non_full_node(insertion_index, element)
            } else {
                root.insert_into_non_full_node(index, element)
            }
        } else {
            let mut root = OpTreeNode::new();
            root.insert_into_non_full_node(index, element);
            self.root_node = Some(root)
        }
        assert_eq!(self.len(), old_len + 1, "{:#?}", self);
    }

    /// Get the `element` at `index` in the sequence.
    pub fn get(&self, index: usize) -> Option<&Op> {
        self.root_node.as_ref().and_then(|n| n.get(index))
    }

    // this replaces get_mut() because it allows the indexes to update correctly
    pub fn update<F>(&mut self, index: usize, f: F)
    where
        F: FnMut(&mut Op),
    {
        if self.len() > index {
            self.root_node.as_mut().unwrap().update(index, f);
        }
    }

    /// Removes the element at `index` from the sequence.
    ///
    /// # Panics
    ///
    /// Panics if `index` is out of bounds.
    pub fn remove(&mut self, index: usize) -> Op {
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
}

impl OpTreeNode {
    fn new() -> Self {
        Self {
            elements: Vec::new(),
            children: Vec::new(),
            index: Default::default(),
            length: 0,
        }
    }

    pub fn search<'a, 'b: 'a, Q>(&'b self, query: &mut Q, m: &OpSetMetadata) -> bool
    where
        Q: TreeQuery<'a>,
    {
        if self.is_leaf() {
            for e in &self.elements {
                if query.query_element_with_metadata(e, m) == QueryResult::Finish {
                    return true;
                }
            }
            false
        } else {
            for (child_index, child) in self.children.iter().enumerate() {
                match query.query_node_with_metadata(child, m) {
                    QueryResult::Descend => {
                        if child.search(query, m) {
                            return true;
                        }
                    }
                    QueryResult::Finish => return true,
                    QueryResult::Next => (),
                }
                if let Some(e) = self.elements.get(child_index) {
                    if query.query_element_with_metadata(e, m) == QueryResult::Finish {
                        return true;
                    }
                }
            }
            false
        }
    }

    pub fn len(&self) -> usize {
        self.length
    }

    fn reindex(&mut self) {
        let mut index = Index::new();
        for c in &self.children {
            index.merge(&c.index);
        }
        for e in &self.elements {
            index.insert(e);
        }
        self.index = index
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

    fn insert_into_non_full_node(&mut self, index: usize, element: Op) {
        assert!(!self.is_full());

        self.index.insert(&element);

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

        let full_child = &mut self.children[full_child_index];

        // Create a new node which is going to store (B-1) keys
        // of the full child.
        let mut successor_sibling = OpTreeNode::new();

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

        full_child.reindex();
        successor_sibling.reindex();

        self.children
            .insert(full_child_index + 1, successor_sibling);

        self.elements.insert(full_child_index, middle);

        assert_eq!(full_child_len + z_len + 1, original_len, "{:#?}", self);

        assert_eq!(original_len_self, self.len());
    }

    fn remove_from_leaf(&mut self, index: usize) -> Op {
        self.length -= 1;
        self.elements.remove(index)
    }

    fn remove_element_from_non_leaf(&mut self, index: usize, element_index: usize) -> Op {
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

    fn remove_from_internal_child(&mut self, index: usize, mut child_index: usize) -> Op {
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
                self.children[child_index - 1].index.remove(&last_element);

                let parent_element =
                    mem::replace(&mut self.elements[child_index - 1], last_element);

                self.children[child_index].index.insert(&parent_element);
                self.children[child_index]
                    .elements
                    .insert(0, parent_element);
                self.children[child_index].length += 1;

                if let Some(last_child) = self.children[child_index - 1].children.pop() {
                    self.children[child_index - 1].length -= last_child.len();
                    self.children[child_index - 1].reindex();
                    self.children[child_index].length += last_child.len();
                    self.children[child_index].children.insert(0, last_child);
                    self.children[child_index].reindex();
                }
            } else if self
                .children
                .get(child_index + 1)
                .map_or(false, |c| c.elements.len() >= B)
            {
                let first_element = self.children[child_index + 1].elements.remove(0);
                self.children[child_index + 1].index.remove(&first_element);
                self.children[child_index + 1].length -= 1;

                assert!(!self.children[child_index + 1].elements.is_empty());

                let parent_element = mem::replace(&mut self.elements[child_index], first_element);

                self.children[child_index].length += 1;
                self.children[child_index].index.insert(&parent_element);
                self.children[child_index].elements.push(parent_element);

                if !self.children[child_index + 1].is_leaf() {
                    let first_child = self.children[child_index + 1].children.remove(0);
                    self.children[child_index + 1].length -= first_child.len();
                    self.children[child_index + 1].reindex();
                    self.children[child_index].length += first_child.len();

                    self.children[child_index].children.push(first_child);
                    self.children[child_index].reindex();
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

    pub fn remove(&mut self, index: usize) -> Op {
        let original_len = self.len();
        if self.is_leaf() {
            let v = self.remove_from_leaf(index);
            self.index.remove(&v);
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
                        self.index.remove(&v);
                        assert_eq!(original_len, self.len() + 1);
                        debug_assert_eq!(self.check(), self.len());
                        return v;
                    }
                    Ordering::Greater => {
                        let v = self.remove_from_internal_child(index, child_index);
                        self.index.remove(&v);
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

    fn merge(&mut self, middle: Op, successor_sibling: OpTreeNode) {
        self.index.insert(&middle);
        self.index.merge(&successor_sibling.index);
        self.elements.push(middle);
        self.elements.extend(successor_sibling.elements);
        self.children.extend(successor_sibling.children);
        self.length += successor_sibling.length + 1;
        assert!(self.is_full());
    }

    /// Update the operation at the given index using the provided function.
    ///
    /// This handles updating the indices after the update.
    pub fn update<F>(&mut self, index: usize, f: F) -> (Op, &Op)
    where
        F: FnOnce(&mut Op),
    {
        if self.is_leaf() {
            let new_element = self.elements.get_mut(index).unwrap();
            let old_element = new_element.clone();
            f(new_element);
            self.index.replace(&old_element, new_element);
            (old_element, new_element)
        } else {
            let mut cumulative_len = 0;
            let len = self.len();
            for (child_index, child) in self.children.iter_mut().enumerate() {
                match (cumulative_len + child.len()).cmp(&index) {
                    Ordering::Less => {
                        cumulative_len += child.len() + 1;
                    }
                    Ordering::Equal => {
                        let new_element = self.elements.get_mut(child_index).unwrap();
                        let old_element = new_element.clone();
                        f(new_element);
                        self.index.replace(&old_element, new_element);
                        return (old_element, new_element);
                    }
                    Ordering::Greater => {
                        let (old_element, new_element) = child.update(index - cumulative_len, f);
                        self.index.replace(&old_element, new_element);
                        return (old_element, new_element);
                    }
                }
            }
            panic!("Invalid index to set: {} but len was {}", index, len)
        }
    }

    pub fn last(&self) -> &Op {
        if self.is_leaf() {
            // node is never empty so this is safe
            self.elements.last().unwrap()
        } else {
            // if not a leaf then there is always at least one child
            self.children.last().unwrap().last()
        }
    }

    pub fn get(&self, index: usize) -> Option<&Op> {
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
}

impl Default for OpTreeInternal {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for OpTreeInternal {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len() && self.iter().zip(other.iter()).all(|(a, b)| a == b)
    }
}

impl<'a> IntoIterator for &'a OpTreeInternal {
    type Item = &'a Op;

    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        Iter {
            inner: self,
            index: 0,
        }
    }
}

pub(crate) struct Iter<'a> {
    inner: &'a OpTreeInternal,
    index: usize,
}

impl<'a> Iterator for Iter<'a> {
    type Item = &'a Op;

    fn next(&mut self) -> Option<Self::Item> {
        self.index += 1;
        self.inner.get(self.index - 1)
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.index += n + 1;
        self.inner.get(self.index - 1)
    }
}

#[derive(Debug, Clone, PartialEq)]
struct CounterData {
    pos: usize,
    val: i64,
    succ: HashSet<OpId>,
    op: Op,
}

#[cfg(test)]
mod tests {
    use crate::legacy as amp;
    use crate::types::{Op, OpId};

    use super::*;

    fn op() -> Op {
        let zero = OpId(0, 0);
        Op {
            id: zero,
            action: amp::OpType::Put(0.into()),
            key: zero.into(),
            succ: vec![],
            pred: vec![],
            insert: false,
        }
    }

    #[test]
    fn insert() {
        let mut t = OpTree::new();

        t.insert(0, op());
        t.insert(1, op());
        t.insert(0, op());
        t.insert(0, op());
        t.insert(0, op());
        t.insert(3, op());
        t.insert(4, op());
    }

    #[test]
    fn insert_book() {
        let mut t = OpTree::new();

        for i in 0..100 {
            t.insert(i % 2, op());
        }
    }

    #[test]
    fn insert_book_vec() {
        let mut t = OpTree::new();
        let mut v = Vec::new();

        for i in 0..100 {
            t.insert(i % 3, op());
            v.insert(i % 3, op());

            assert_eq!(v, t.iter().cloned().collect::<Vec<_>>())
        }
    }
}
