#![allow(dead_code)]

use std::{
    cmp::{min, Ordering},
    fmt::Debug,
    mem,
};

use crate::{key_cmp, lamport_cmp};
use crate::{ElemId, IndexedCache, Key, ObjId, Op, OpId, ScalarValue};
use automerge_protocol as amp;
use fxhash::FxBuildHasher;
use std::collections::{HashMap, HashSet};

pub(crate) type OpTree = OpTreeInternal<64>;

#[derive(Clone, Debug)]
pub(crate) struct OpTreeInternal<const B: usize> {
    root_node: Option<OpTreeNode<B>>,
}

#[derive(Clone, Debug)]
pub(crate) struct OpTreeNode<const B: usize> {
    elements: Vec<Op>,
    children: Vec<OpTreeNode<B>>,
    pub index: Index,
    depth: usize,
    length: usize,
}

pub(crate) trait TreeQuery<const B: usize> {
    fn query_child(&mut self, child: &OpTreeNode<B>) -> QueryResult;
    fn done(&self) -> bool;
    fn query_element(&mut self, element: &Op) -> QueryResult;
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum QueryResult {
    Next,
    Decend,
    Finish,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Index {
    pub visible: HashMap<ObjId, HashMap<ElemId, usize, FxBuildHasher>, FxBuildHasher>,
    pub lens: HashMap<ObjId, usize, FxBuildHasher>,
    pub ops: HashSet<OpId, FxBuildHasher>,
}

impl Index {
    fn new() -> Self {
        Index {
            visible: Default::default(),
            lens: Default::default(),
            ops: Default::default(),
        }
    }

    fn insert(&mut self, op: &Op) {
        self.ops.insert(op.id);
        if op.succ.is_empty() {
            if let Some(elem) = op.elemid() {
                let sub = self.visible.entry(op.obj).or_default();
                match sub.get(&elem).copied() {
                    None => {
                        sub.insert(elem, 1);
                        self.lens.entry(op.obj).and_modify(|n| *n += 1).or_insert(1);
                    }
                    Some(n) => {
                        sub.insert(elem, n + 1);
                    }
                }
            }
        }
    }

    fn remove(&mut self, op: &Op) {
        self.ops.remove(&op.id);
        if op.succ.is_empty() {
            let mut sub_empty = false;
            if let Some(elem) = op.elemid() {
                if let Some(c) = self.visible.get_mut(&op.obj) {
                    if let Some(d) = c.get(&elem).copied() {
                        if d == 1 {
                            c.remove(&elem);
                            self.lens.entry(op.obj).and_modify(|n| *n -= 1);
                            sub_empty = c.is_empty();
                        } else {
                            c.insert(elem, d - 1);
                        }
                    }
                }
            }
            if sub_empty {
                self.visible.remove(&op.obj);
                self.lens.remove(&op.obj);
            }
        }
    }

    fn merge(&mut self, other: &Index) {
        for id in &other.ops {
            self.ops.insert(*id);
        }
        for (obj, sub) in other.visible.iter() {
            let local_obj = self.visible.entry(*obj).or_default();
            for (elem, n) in sub.iter() {
                match local_obj.get(elem).cloned() {
                    None => {
                        local_obj.insert(*elem, 1);
                        self.lens.entry(*obj).and_modify(|o| *o += 1).or_insert(1);
                    }
                    Some(m) => {
                        local_obj.insert(*elem, m + n);
                    }
                }
            }
        }
    }
}

impl Default for Index {
    fn default() -> Self {
        Self::new()
    }
}

impl<const B: usize> OpTreeInternal<B> {
    /// Construct a new, empty, sequence.
    pub fn new() -> Self {
        Self { root_node: None }
    }

    /// Get the length of the sequence.
    pub fn len(&self) -> usize {
        self.root_node.as_ref().map_or(0, |n| n.len())
    }

    pub fn list_len(&self, obj: &ObjId) -> usize {
        self.root_node.as_ref().map_or(0, |n| n.list_len(obj))
    }

    pub fn depth(&self) -> usize {
        self.root_node.as_ref().map(|root| root.depth).unwrap_or(0)
    }

    pub fn audit(&mut self) {
        if let Some(root) = self.root_node.as_mut() {
            root.audit()
        }
    }

    pub fn query<Q>(&self, query: &mut Q) -> bool
    where
        Q: TreeQuery<B>,
    {
        self.root_node
            .as_ref()
            .map(|root| match query.query_child(root) {
                QueryResult::Decend => {
                    root.query(query);
                    query.done()
                }
                QueryResult::Finish => true,
                QueryResult::Next => false,
            })
            .unwrap_or(false)
    }

    pub fn seek_prop(
        &self,
        obj: &ObjId,
        key: &Key,
        actors: &IndexedCache<amp::ActorId>,
        props: &IndexedCache<String>,
    ) -> usize {
        self.binary_search_by(|op| match lamport_cmp(actors, op.obj.0, obj.0) {
            Ordering::Equal => key_cmp(&op.key, key, props),
            n => n,
        })
    }
    pub fn seek_obj(&self, obj: &ObjId, actors: &IndexedCache<amp::ActorId>) -> usize {
        self.binary_search_by(|op| lamport_cmp(actors, op.obj.0, obj.0))
    }

    pub fn binary_search_by<F>(&self, f: F) -> usize
    where
        F: Fn(&Op) -> Ordering,
    {
        let mut right = self.len();
        let mut left = 0;
        while left < right {
            let seq = (left + right) / 2;
            if f(self.get(seq).unwrap()) == Ordering::Less {
                left = seq + 1;
            } else {
                right = seq;
            }
        }
        left
    }

    /// Check if the sequence is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Create an iterator through the sequence.
    pub fn iter(&self) -> Iter<'_, B> {
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
                let new_root = OpTreeNode::new(root.depth + 1);

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
            let mut root = OpTreeNode::new(1);
            root.insert_into_non_full_node(index, element);
            self.root_node = Some(root)
        }
        assert_eq!(self.len(), old_len + 1, "{:#?}", self);
    }

    /// Push the `element` onto the back of the sequence.
    pub fn push(&mut self, element: Op) {
        let l = self.len();
        self.insert(l, element)
    }

    /// Get the `element` at `index` in the sequence.
    pub fn get(&self, index: usize) -> Option<&Op> {
        self.root_node.as_ref().and_then(|n| n.get(index))
    }

    /// Get the `element` at `index` in the sequence.
    pub fn get_mut(&mut self, index: usize) -> Option<&mut Op> {
        // FIXME - no index update
        self.root_node.as_mut().and_then(|n| n.get_mut(index))
    }

    //    pub fn binary_search_by<F>(&self, f: F) -> usize
    //      where F: Fn(&Op) -> Ordering
    pub fn replace<F>(&mut self, index: usize, mut f: F) -> Option<Op>
    where
        F: FnMut(&mut Op),
    {
        if self.len() > index {
            let op = self.get(index).unwrap().clone();
            let mut new_op = op.clone();
            f(&mut new_op);
            self.set(index, new_op);
            Some(op)
        } else {
            None
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

    /// Update the `element` at `index` in the sequence, returning the old value.
    ///
    /// # Panics
    ///
    /// Panics if `index > len`
    pub fn set(&mut self, index: usize, element: Op) -> Op {
        self.root_node.as_mut().unwrap().set(index, element)
    }
}

impl<const B: usize> OpTreeNode<B> {
    fn new(depth: usize) -> Self {
        Self {
            elements: Vec::new(),
            children: Vec::new(),
            index: Default::default(),
            depth,
            length: 0,
        }
    }

    pub fn query<Q>(&self, query: &mut Q) -> bool
    where
        Q: TreeQuery<B>,
    {
        if self.is_leaf() {
            for e in &self.elements {
                if query.query_element(e) == QueryResult::Finish {
                    return true;
                }
            }
            false
        } else {
            for (child_index, child) in self.children.iter().enumerate() {
                match query.query_child(child) {
                    QueryResult::Decend => {
                        if child.query(query) {
                            break;
                        }
                    }
                    QueryResult::Finish => break,
                    QueryResult::Next => (),
                }
                if let Some(e) = self.elements.get(child_index) {
                    if query.query_element(e) == QueryResult::Finish {
                        break;
                    }
                }
            }
            query.done()
        }
    }

    pub fn len(&self) -> usize {
        self.length
    }

    pub fn list_len(&self, obj: &ObjId) -> usize {
        self.index.lens.get(obj).copied().unwrap_or(0)
    }

    fn audit(&mut self) {
        let old = self.index.clone();
        self.reindex();
        if old != self.index {
            let mut objs: Vec<_> = old
                .visible
                .keys()
                .chain(self.index.visible.keys())
                .collect();
            objs.sort();
            objs.dedup();
            for o in objs {
                let a = old.visible.get(o).cloned().unwrap_or_default();
                let b = self.index.visible.get(o).cloned().unwrap_or_default();
                let mut keys: Vec<_> = a.keys().chain(b.keys()).collect();
                keys.sort();
                keys.dedup();
                for k in keys {
                    let a = a.get(k);
                    let b = b.get(k);
                    if a != b {
                        println!("key={:?} obj={:?} {:?} NE {:?}", k, o, a, b);
                    }
                }
            }
            panic!("Not Eq");
        }
        for c in self.children.iter_mut() {
            c.audit()
        }
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
        let mut successor_sibling = OpTreeNode::new(full_child.depth);

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

    fn merge(&mut self, middle: Op, successor_sibling: OpTreeNode<B>) {
        self.index.insert(&middle);
        self.index.merge(&successor_sibling.index);
        self.elements.push(middle);
        self.elements.extend(successor_sibling.elements);
        self.children.extend(successor_sibling.children);
        self.length += successor_sibling.length + 1;
        assert!(self.is_full());
    }

    pub fn set(&mut self, index: usize, element: Op) -> Op {
        if self.is_leaf() {
            let old_element = self.elements.get_mut(index).unwrap();
            self.index.remove(old_element);
            self.index.insert(&element);
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
                        self.index.remove(old_element);
                        self.index.insert(&element);
                        return mem::replace(old_element, element);
                    }
                    Ordering::Greater => {
                        self.index.insert(&element);
                        let old_element = child.set(index - cumulative_len, element);
                        self.index.remove(&old_element);
                        return old_element
                    }
                }
            }
            panic!("Invalid index to set: {} but len was {}", index, self.len())
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

    pub fn get_mut(&mut self, index: usize) -> Option<&mut Op> {
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

impl<const B: usize> Default for OpTreeInternal<B> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const B: usize> PartialEq for OpTreeInternal<B> {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len() && self.iter().zip(other.iter()).all(|(a, b)| a == b)
    }
}

impl<'a, const B: usize> IntoIterator for &'a OpTreeInternal<B> {
    type Item = &'a Op;

    type IntoIter = Iter<'a, B>;

    fn into_iter(self) -> Self::IntoIter {
        Iter {
            inner: self,
            index: 0,
        }
    }
}

pub(crate) struct Iter<'a, const B: usize> {
    inner: &'a OpTreeInternal<B>,
    index: usize,
}

impl<'a, const B: usize> Iterator for Iter<'a, B> {
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

fn is_visible(op: &Op, pos: usize, counters: &mut HashMap<OpId, CounterData>) -> bool {
    let mut visible = false;
    match op.action {
        amp::OpType::Set(amp::ScalarValue::Counter(val)) => {
            counters.insert(
                op.id,
                CounterData {
                    pos,
                    val,
                    succ: op.succ.iter().cloned().collect(),
                    op: op.clone(),
                },
            );
            if op.succ.is_empty() {
                visible = true;
            }
        }
        amp::OpType::Inc(inc_val) => {
            for id in &op.pred {
                if let Some(mut entry) = counters.get_mut(id) {
                    entry.succ.remove(&op.id);
                    entry.val += inc_val;
                    entry.op.action = amp::OpType::Set(ScalarValue::Counter(entry.val));
                    if entry.succ.is_empty() {
                        visible = true;
                    }
                }
            }
        }
        _ => {
            if op.succ.is_empty() {
                visible = true;
            }
        }
    };
    visible
}
fn visible_op(op: &Op, counters: &HashMap<OpId, CounterData>) -> Op {
    for pred in &op.pred {
        // FIXME - delete a counter? - entry.succ.empty()?
        if let Some(entry) = counters.get(pred) {
            return entry.op.clone();
        }
    }
    op.clone()
}

#[cfg(test)]
mod tests {
    use crate::{Op, OpId};
    use automerge_protocol as amp;

    use super::*;

    fn op(n: usize) -> Op {
        let zero = OpId(0, 0);
        Op {
            change: n,
            id: zero,
            action: amp::OpType::Set(0.into()),
            obj: zero.into(),
            key: zero.into(),
            succ: vec![],
            pred: vec![],
            insert: false,
        }
    }

    #[test]
    fn push_back() {
        let mut t = OpTree::new();

        t.push(op(1));
        t.push(op(2));
        t.push(op(3));
        t.push(op(4));
        t.push(op(5));
        t.push(op(6));
        t.push(op(8));
        t.push(op(100));
    }

    #[test]
    fn insert() {
        let mut t = OpTree::new();

        t.insert(0, op(1));
        t.insert(1, op(1));
        t.insert(0, op(1));
        t.insert(0, op(1));
        t.insert(0, op(1));
        t.insert(3, op(1));
        t.insert(4, op(1));
    }

    #[test]
    fn insert_book() {
        let mut t = OpTree::new();

        for i in 0..100 {
            t.insert(i % 2, op(i));
        }
    }

    #[test]
    fn insert_book_vec() {
        let mut t = OpTree::new();
        let mut v = Vec::new();

        for i in 0..100 {
            t.insert(i % 3, op(i));
            v.insert(i % 3, op(i));

            assert_eq!(v, t.iter().cloned().collect::<Vec<_>>())
        }
    }

    /*
        #[test]
        fn test_depth() {
            let mut t = OpTree::new();

            assert_eq!(t.depth(),0);

            for i in 0..5000 {
                t.insert(0, op(i));
            }
            assert_eq!(t.depth(),3);

            for _ in 0..1000 { t.remove(t.len() / 2); }
            assert_eq!(t.depth(),3);

            for _ in 0..1000 { t.remove(t.len() / 2); }
            assert_eq!(t.depth(),3);

            for _ in 0..1000 { t.remove(t.len() / 2); }
            assert_eq!(t.depth(),3);

            for _ in 0..1000 { t.remove(t.len() / 2); }
            assert_eq!(t.depth(),2);

            for _ in 0..950 { t.remove(t.len() / 2); }
            assert_eq!(t.depth(),2);

            for _ in 0..30 { t.remove(t.len() / 2); }
            assert_eq!(t.depth(),1);

        }
    */

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
                let mut t = OpTreeInternal::<usize, 3>::new();
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
                let mut t = OpTreeInternal::<usize, 3>::new();
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
