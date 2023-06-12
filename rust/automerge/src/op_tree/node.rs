use std::{
    cmp::{min, Ordering},
    fmt::Debug,
    mem,
};

pub(crate) use crate::op_set::OpSetMetadata;
use crate::query::{ChangeVisibility, Index, QueryResult, TreeQuery};
use crate::types::{Op, OpId};
pub const B: usize = 16;

#[derive(Clone, Debug)]
pub(crate) struct OpTreeNode {
    pub(crate) children: Vec<OpTreeNode>,
    pub(crate) elements: Vec<usize>,
    pub(crate) index: Index,
    pub(crate) length: usize,
}

impl OpTreeNode {
    pub(crate) fn new() -> Self {
        Self {
            elements: Vec::new(),
            children: Vec::new(),
            index: Default::default(),
            length: 0,
        }
    }

    fn search_element<'a, 'b: 'a, Q>(
        &'b self,
        query: &mut Q,
        m: &OpSetMetadata,
        ops: &'a [Op],
        index: usize,
    ) -> bool
    where
        Q: TreeQuery<'a>,
    {
        if let Some(e) = self.elements.get(index) {
            if query.query_element_with_metadata(&ops[*e], m) == QueryResult::Finish {
                return true;
            }
        }
        false
    }

    pub(crate) fn search<'a, 'b: 'a, Q>(
        &'b self,
        query: &mut Q,
        m: &'a OpSetMetadata,
        ops: &'a [Op],
    ) -> bool
    where
        Q: TreeQuery<'a>,
    {
        if self.is_leaf() {
            for e in self.elements.iter() {
                if query.query_element_with_metadata(&ops[*e], m) == QueryResult::Finish {
                    return true;
                }
            }
            false
        } else {
            for (child_index, child) in self.children.iter().enumerate() {
                // descend and try find it
                match query.query_node_with_metadata(child, m, ops) {
                    QueryResult::Descend => {
                        if child.search(query, m, ops) {
                            return true;
                        }
                    }
                    QueryResult::Finish => return true,
                    QueryResult::Next => (),
                }
                if self.search_element(query, m, ops, child_index) {
                    return true;
                }
            }
            false
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.length
    }

    fn reindex(&mut self, ops: &[Op]) {
        let mut index = Index::new();
        for c in &self.children {
            index.merge(&c.index);
        }
        for i in &self.elements {
            index.insert(&ops[*i]);
        }
        index.block = self.regenerate_block(ops);
        self.index = index
    }

    pub(crate) fn is_leaf(&self) -> bool {
        self.children.is_empty()
    }

    pub(crate) fn is_full(&self) -> bool {
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
        panic!("index {} not found in node with len {}", index, self.len())
    }

    pub(crate) fn insert_into_non_full_node(&mut self, index: usize, element: usize, ops: &[Op]) {
        assert!(!self.is_full());

        let op = &ops[element];
        self.index.insert(op);

        if self.is_leaf() {
            self.length += 1;
            self.elements.insert(index, element);
        } else {
            let (child_index, sub_index) = self.find_child_index(index);
            let child = &mut self.children[child_index];

            if child.is_full() {
                self.split_child(child_index, ops);

                // child structure has changed so we need to find the index again
                let (child_index, sub_index) = self.find_child_index(index);
                let child = &mut self.children[child_index];
                child.insert_into_non_full_node(sub_index, element, ops);
            } else {
                child.insert_into_non_full_node(sub_index, element, ops);
            }
            self.length += 1;
        }
        self.index.block = self.regenerate_block(ops);
    }

    // A utility function to split the child `full_child_index` of this node
    // Note that `full_child_index` must be full when this function is called.
    pub(crate) fn split_child(&mut self, full_child_index: usize, ops: &[Op]) {
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

        full_child.reindex(ops);
        successor_sibling.reindex(ops);

        self.children
            .insert(full_child_index + 1, successor_sibling);

        self.elements.insert(full_child_index, middle);

        assert_eq!(full_child_len + z_len + 1, original_len, "{:#?}", self);

        assert_eq!(original_len_self, self.len());
    }

    fn remove_from_leaf(&mut self, index: usize) -> usize {
        self.length -= 1;
        self.elements.remove(index)
    }

    fn remove_element_from_non_leaf(
        &mut self,
        index: usize,
        element_index: usize,
        ops: &[Op],
    ) -> usize {
        self.length -= 1;
        if self.children[element_index].elements.len() >= B {
            let total_index = self.cumulative_index(element_index);
            // recursively delete index - 1 in predecessor_node
            let predecessor = self.children[element_index].remove(index - 1 - total_index, ops);
            // replace element with that one
            mem::replace(&mut self.elements[element_index], predecessor)
        } else if self.children[element_index + 1].elements.len() >= B {
            // recursively delete index + 1 in successor_node
            let total_index = self.cumulative_index(element_index + 1);
            let successor = self.children[element_index + 1].remove(index + 1 - total_index, ops);
            // replace element with that one
            mem::replace(&mut self.elements[element_index], successor)
        } else {
            let middle_element = self.elements.remove(element_index);
            let successor_child = self.children.remove(element_index + 1);
            self.children[element_index].merge(middle_element, successor_child, ops);

            let total_index = self.cumulative_index(element_index);
            self.children[element_index].remove(index - total_index, ops)
        }
    }

    fn cumulative_index(&self, child_index: usize) -> usize {
        self.children[0..child_index]
            .iter()
            .map(|c| c.len() + 1)
            .sum()
    }

    fn remove_from_internal_child(
        &mut self,
        index: usize,
        mut child_index: usize,
        ops: &[Op],
    ) -> usize {
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

                self.children[child_index].merge(middle, successor, ops);
            } else {
                let middle = self.elements.remove(child_index);

                // use the sucessor sibling
                let successor = self.children.remove(child_index + 1);

                self.children[child_index].merge(middle, successor, ops);
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
                self.children[child_index - 1]
                    .index
                    .remove(&ops[last_element]);

                let parent_element =
                    mem::replace(&mut self.elements[child_index - 1], last_element);

                self.children[child_index]
                    .index
                    .insert(&ops[parent_element]);
                self.children[child_index]
                    .elements
                    .insert(0, parent_element);
                self.children[child_index].length += 1;

                if let Some(last_child) = self.children[child_index - 1].children.pop() {
                    self.children[child_index - 1].length -= last_child.len();
                    self.children[child_index - 1].reindex(ops);
                    self.children[child_index].length += last_child.len();
                    self.children[child_index].children.insert(0, last_child);
                    self.children[child_index].reindex(ops);
                }
            } else if self
                .children
                .get(child_index + 1)
                .map_or(false, |c| c.elements.len() >= B)
            {
                let first_element = self.children[child_index + 1].elements.remove(0);
                self.children[child_index + 1]
                    .index
                    .remove(&ops[first_element]);
                self.children[child_index + 1].length -= 1;

                assert!(!self.children[child_index + 1].elements.is_empty());

                let parent_element = mem::replace(&mut self.elements[child_index], first_element);

                self.children[child_index].length += 1;
                self.children[child_index]
                    .index
                    .insert(&ops[parent_element]);
                self.children[child_index].elements.push(parent_element);

                if !self.children[child_index + 1].is_leaf() {
                    let first_child = self.children[child_index + 1].children.remove(0);
                    self.children[child_index + 1].length -= first_child.len();
                    self.children[child_index + 1].reindex(ops);
                    self.children[child_index].length += first_child.len();

                    self.children[child_index].children.push(first_child);
                    self.children[child_index].reindex(ops);
                }
            }
        }
        self.length -= 1;
        let total_index = self.cumulative_index(child_index);
        self.children[child_index].remove(index - total_index, ops)
    }

    pub(crate) fn check(&self) -> usize {
        let l = self.elements.len() + self.children.iter().map(|c| c.check()).sum::<usize>();
        assert_eq!(self.len(), l, "{:#?}", self);

        l
    }

    pub(crate) fn remove(&mut self, index: usize, ops: &[Op]) -> usize {
        let original_len = self.len();
        if self.is_leaf() {
            let v = self.remove_from_leaf(index);
            self.index.remove(&ops[v]);
            self.index.block = self.regenerate_block(ops);
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
                            ops,
                        );
                        self.index.remove(&ops[v]);
                        self.index.block = self.regenerate_block(ops);
                        assert_eq!(original_len, self.len() + 1);
                        debug_assert_eq!(self.check(), self.len());
                        return v;
                    }
                    Ordering::Greater => {
                        let v = self.remove_from_internal_child(index, child_index, ops);
                        self.index.remove(&ops[v]);
                        self.index.block = self.regenerate_block(ops);
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

    fn merge(&mut self, middle: usize, successor_sibling: OpTreeNode, ops: &[Op]) {
        self.index.insert(&ops[middle]);
        self.index.merge(&successor_sibling.index);
        self.elements.push(middle);
        self.elements.extend(successor_sibling.elements);
        self.children.extend(successor_sibling.children);
        self.length += successor_sibling.length + 1;
        self.index.block = self.regenerate_block(ops);
        assert!(self.is_full());
    }

    fn block(&self) -> Option<OpId> {
        self.index.block
    }

    fn regenerate_block(&self, ops: &[Op]) -> Option<OpId> {
        if self.is_leaf() {
            self.elements
                .iter()
                .rev()
                .filter_map(|e| ops[*e].visible_block())
                .next()
        } else {
            let mut elems = self.elements.iter().rev().map(|e| ops[*e].visible_block());
            let mut nodes = self
                .children
                .iter()
                .rev()
                .filter_map(|c| c.block().or_else(|| elems.next().flatten()));
            nodes.next()
        }
    }

    /// Update the operation at the given index using the provided function.
    ///
    /// This handles updating the indices after the update.
    pub(crate) fn update<'a>(
        &mut self,
        index: usize,
        vis: ChangeVisibility<'a>,
        ops: &[Op],
    ) -> ChangeVisibility<'a> {
        if self.is_leaf() {
            self.index.block = self.regenerate_block(ops);
            self.index.change_vis(vis)
        } else {
            let mut cumulative_len = 0;
            let len = self.len();
            for (_child_index, child) in self.children.iter_mut().enumerate() {
                match (cumulative_len + child.len()).cmp(&index) {
                    Ordering::Less => {
                        cumulative_len += child.len() + 1;
                    }
                    Ordering::Equal => {
                        self.index.block = self.regenerate_block(ops);
                        return self.index.change_vis(vis);
                    }
                    Ordering::Greater => {
                        let vis = child.update(index - cumulative_len, vis, ops);
                        self.index.block = self.regenerate_block(ops);
                        return self.index.change_vis(vis);
                    }
                }
            }
            panic!("Invalid index to set: {} but len was {}", index, len)
        }
    }

    pub(crate) fn last(&self) -> usize {
        if self.is_leaf() {
            // node is never empty so this is safe
            *self.elements.last().unwrap()
        } else {
            // if not a leaf then there is always at least one child
            self.children.last().unwrap().last()
        }
    }

    pub(crate) fn get(&self, index: usize) -> Option<usize> {
        if self.is_leaf() {
            return self.elements.get(index).copied();
        } else {
            let mut cumulative_len = 0;
            for (child_index, child) in self.children.iter().enumerate() {
                match (cumulative_len + child.len()).cmp(&index) {
                    Ordering::Less => {
                        cumulative_len += child.len() + 1;
                    }
                    Ordering::Equal => return self.elements.get(child_index).copied(),
                    Ordering::Greater => {
                        return child.get(index - cumulative_len);
                    }
                }
            }
        }
        None
    }
}
