use std::{
    cmp::{min, Ordering},
    fmt::Debug,
    mem,
};

use crate::op_set::Op;
pub(crate) use crate::op_set::{OpIdx, OpSetData};
use crate::{
    query::{ChangeVisibility, Index, QueryResult, TreeQuery},
    types::OpId,
};

pub const B: usize = 16;

#[derive(Clone, Debug)]
pub(crate) struct OpTreeNode {
    pub(crate) children: Vec<OpTreeNode>,
    pub(crate) elements: Vec<OpIdx>,
    pub(crate) index: Option<Index>,
    pub(crate) length: usize,
}

impl OpTreeNode {
    pub(crate) fn new(has_index: bool) -> Self {
        let index = if has_index { Some(Index::new()) } else { None };
        Self {
            elements: Vec::new(),
            children: Vec::new(),
            index,
            length: 0,
        }
    }

    fn search_element<'a, 'b: 'a, Q>(
        &'b self,
        query: &mut Q,
        m: &'a OpSetData,
        index: usize,
    ) -> bool
    where
        Q: TreeQuery<'a>,
    {
        if let Some(idx) = self.elements.get(index) {
            if query.query_element(idx.as_op(m)) == QueryResult::Finish {
                return true;
            }
        }
        false
    }

    pub(crate) fn search<'a, 'b: 'a, Q>(&'b self, query: &mut Q, m: &'a OpSetData) -> bool
    where
        Q: TreeQuery<'a>,
    {
        if self.is_leaf() {
            for idx in self.elements.iter() {
                if query.query_element(idx.as_op(m)) == QueryResult::Finish {
                    return true;
                }
            }
            false
        } else {
            for (child_index, child) in self.children.iter().enumerate() {
                // descend and try find it
                if let Some(index) = &child.index {
                    match query.query_node(child, index, m) {
                        QueryResult::Descend => {
                            if child.search(query, m) {
                                return true;
                            }
                        }
                        QueryResult::Finish => return true,
                        QueryResult::Next => (),
                    }
                } else if child.search(query, m) {
                    return true;
                }
                if self.search_element(query, m, child_index) {
                    return true;
                }
            }
            false
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.length
    }

    fn reindex(&mut self, osd: &OpSetData) {
        if self.index.is_some() {
            let mut index = Index::new();
            for c in &self.children {
                if let Some(i) = &c.index {
                    index.merge(i);
                }
            }
            for i in &self.elements {
                index.insert(i.as_op(osd));
                index.block = self.regenerate_block(osd);
            }
            self.index = Some(index)
        }
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

    pub(crate) fn add_index(&mut self, osd: &OpSetData) -> &Index {
        if self.index.is_none() {
            let mut index = Index::new();
            for c in &mut self.children {
                index.merge(c.add_index(osd))
            }
            for i in &self.elements {
                index.insert(i.as_op(osd));
            }
            self.index = Some(index);
        }
        self.index.as_ref().unwrap()
    }

    #[allow(dead_code)]
    pub(crate) fn drop_index(&mut self) {
        self.index = None;
        for c in &mut self.children {
            c.drop_index();
        }
    }

    pub(crate) fn index_insert(&mut self, op: Op<'_>) {
        if let Some(index) = &mut self.index {
            index.insert(op)
        }
    }

    pub(crate) fn index_remove(&mut self, op: Op<'_>) {
        if let Some(index) = &mut self.index {
            index.remove(op);
        }
    }

    pub(crate) fn index_regenerate_block(&mut self, osd: &OpSetData) {
        if self.index.is_some() {
            self.index.as_mut().unwrap().block = self.regenerate_block(osd);
        }
    }

    pub(crate) fn insert_into_non_full_node(
        &mut self,
        index: usize,
        element: OpIdx,
        osd: &OpSetData,
    ) {
        assert!(!self.is_full());

        self.index_insert(element.as_op(osd));

        if self.is_leaf() {
            self.length += 1;
            self.elements.insert(index, element);
        } else {
            let (child_index, sub_index) = self.find_child_index(index);
            let child = &mut self.children[child_index];

            if child.is_full() {
                self.split_child(child_index, osd);

                // child structure has changed so we need to find the index again
                let (child_index, sub_index) = self.find_child_index(index);
                let child = &mut self.children[child_index];
                child.insert_into_non_full_node(sub_index, element, osd);
            } else {
                child.insert_into_non_full_node(sub_index, element, osd);
            }
            self.length += 1;
        }
        self.index_regenerate_block(osd);
    }

    // A utility function to split the child `full_child_index` of this node
    // Note that `full_child_index` must be full when this function is called.
    pub(crate) fn split_child(&mut self, full_child_index: usize, m: &OpSetData) {
        let original_len_self = self.len();

        let full_child = &mut self.children[full_child_index];

        // Create a new node which is going to store (B-1) keys
        // of the full child.
        let mut successor_sibling = OpTreeNode::new(self.index.is_some());

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

        full_child.reindex(m);
        successor_sibling.reindex(m);

        self.children
            .insert(full_child_index + 1, successor_sibling);

        self.elements.insert(full_child_index, middle);

        assert_eq!(full_child_len + z_len + 1, original_len, "{:#?}", self);

        assert_eq!(original_len_self, self.len());
    }

    fn remove_from_leaf(&mut self, index: usize) -> OpIdx {
        self.length -= 1;
        self.elements.remove(index)
    }

    fn remove_element_from_non_leaf(
        &mut self,
        index: usize,
        element_index: usize,
        m: &OpSetData,
    ) -> OpIdx {
        self.length -= 1;
        if self.children[element_index].elements.len() >= B {
            let total_index = self.cumulative_index(element_index);
            // recursively delete index - 1 in predecessor_node
            let predecessor = self.children[element_index].remove(index - 1 - total_index, m);
            // replace element with that one
            mem::replace(&mut self.elements[element_index], predecessor)
        } else if self.children[element_index + 1].elements.len() >= B {
            // recursively delete index + 1 in successor_node
            let total_index = self.cumulative_index(element_index + 1);
            let successor = self.children[element_index + 1].remove(index + 1 - total_index, m);
            // replace element with that one
            mem::replace(&mut self.elements[element_index], successor)
        } else {
            let middle_element = self.elements.remove(element_index);
            let successor_child = self.children.remove(element_index + 1);
            self.children[element_index].merge(middle_element, successor_child, m);

            let total_index = self.cumulative_index(element_index);
            self.children[element_index].remove(index - total_index, m)
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
        osd: &OpSetData,
    ) -> OpIdx {
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

                self.children[child_index].merge(middle, successor, osd);
            } else {
                let middle = self.elements.remove(child_index);

                // use the sucessor sibling
                let successor = self.children.remove(child_index + 1);

                self.children[child_index].merge(middle, successor, osd);
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
                self.children[child_index - 1].index_remove(last_element.as_op(osd));

                let parent_element =
                    mem::replace(&mut self.elements[child_index - 1], last_element);

                self.children[child_index].index_insert(parent_element.as_op(osd));
                self.children[child_index]
                    .elements
                    .insert(0, parent_element);
                self.children[child_index].length += 1;

                if let Some(last_child) = self.children[child_index - 1].children.pop() {
                    self.children[child_index - 1].length -= last_child.len();
                    self.children[child_index - 1].reindex(osd);
                    self.children[child_index].length += last_child.len();
                    self.children[child_index].children.insert(0, last_child);
                    self.children[child_index].reindex(osd);
                }
            } else if self
                .children
                .get(child_index + 1)
                .map_or(false, |c| c.elements.len() >= B)
            {
                let first_element = self.children[child_index + 1].elements.remove(0);
                self.children[child_index + 1].index_remove(first_element.as_op(osd));
                self.children[child_index + 1].length -= 1;

                assert!(!self.children[child_index + 1].elements.is_empty());

                let parent_element = mem::replace(&mut self.elements[child_index], first_element);

                self.children[child_index].length += 1;
                self.children[child_index].index_insert(parent_element.as_op(osd));
                self.children[child_index].elements.push(parent_element);

                if !self.children[child_index + 1].is_leaf() {
                    let first_child = self.children[child_index + 1].children.remove(0);
                    self.children[child_index + 1].length -= first_child.len();
                    self.children[child_index + 1].reindex(osd);
                    self.children[child_index].length += first_child.len();

                    self.children[child_index].children.push(first_child);
                    self.children[child_index].reindex(osd);
                }
            }
        }
        self.length -= 1;
        let total_index = self.cumulative_index(child_index);
        self.children[child_index].remove(index - total_index, osd)
    }

    pub(crate) fn check(&self) -> usize {
        let l = self.elements.len() + self.children.iter().map(|c| c.check()).sum::<usize>();
        assert_eq!(self.len(), l, "{:#?}", self);

        l
    }

    pub(crate) fn remove(&mut self, index: usize, osd: &OpSetData) -> OpIdx {
        let original_len = self.len();
        if self.is_leaf() {
            let v = self.remove_from_leaf(index);
            self.index_remove(v.as_op(osd));
            self.index_regenerate_block(osd);
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
                            osd,
                        );
                        self.index_remove(v.as_op(osd));
                        self.index_regenerate_block(osd);
                        assert_eq!(original_len, self.len() + 1);
                        debug_assert_eq!(self.check(), self.len());
                        return v;
                    }
                    Ordering::Greater => {
                        let v = self.remove_from_internal_child(index, child_index, osd);
                        self.index_remove(v.as_op(osd));
                        self.index_regenerate_block(osd);
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

    fn merge(&mut self, middle: OpIdx, successor_sibling: OpTreeNode, osd: &OpSetData) {
        if let Some(index) = &mut self.index {
            if let Some(succ_index) = &successor_sibling.index {
                index.insert(middle.as_op(osd));
                index.merge(succ_index);
            }
        }
        self.elements.push(middle);
        self.elements.extend(successor_sibling.elements);
        self.children.extend(successor_sibling.children);
        self.length += successor_sibling.length + 1;
        self.index_regenerate_block(osd);
        assert!(self.is_full());
    }

    fn block(&self) -> Option<OpId> {
        self.index.as_ref().and_then(|i| i.block)
    }

    fn regenerate_block(&self, osd: &OpSetData) -> Option<OpId> {
        if self.is_leaf() {
            self.elements
                .iter()
                .rev()
                .find_map(|e| e.as_op(osd).visible_block())
        } else {
            let mut elems = self
                .elements
                .iter()
                .rev()
                .map(|e| e.as_op(osd).visible_block());
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
        osd: &OpSetData,
    ) -> Option<ChangeVisibility<'a>> {
        if self.is_leaf() {
            self.index.as_mut().map(|index| index.change_vis(vis))
        } else {
            let mut cumulative_len = 0;
            let len = self.len();
            for child in self.children.iter_mut() {
                match (cumulative_len + child.len()).cmp(&index) {
                    Ordering::Less => {
                        cumulative_len += child.len() + 1;
                    }
                    Ordering::Equal => {
                        return self.index.as_mut().map(|index| index.change_vis(vis));
                    }
                    Ordering::Greater => {
                        if let Some(vis) = child.update(index - cumulative_len, vis, osd) {
                            self.index_regenerate_block(osd);
                            return self.index.as_mut().map(|index| index.change_vis(vis));
                        } else {
                            return None;
                        }
                    }
                }
            }
            panic!("Invalid index to set: {} but len was {}", index, len)
        }
    }

    pub(crate) fn last(&self) -> OpIdx {
        if self.is_leaf() {
            // node is never empty so this is safe
            *self.elements.last().unwrap()
        } else {
            // if not a leaf then there is always at least one child
            self.children.last().unwrap().last()
        }
    }

    pub(crate) fn get(&self, index: usize) -> Option<OpIdx> {
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
