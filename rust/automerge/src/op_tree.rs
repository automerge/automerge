use std::{fmt::Debug, mem, ops::RangeBounds};

pub(crate) use crate::op_set::OpSetMetadata;
use crate::{
    clock::Clock,
    query::{self, ChangeVisibility, QueryResult, TreeQuery},
};
use crate::{
    types::{ObjId, Op, OpId},
    ObjType,
};
use std::collections::HashSet;

mod iter;
mod node;

pub(crate) use iter::OpTreeIter;
#[allow(unused)]
pub(crate) use node::OpTreeNode;
pub use node::B;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OpTree {
    pub(crate) internal: OpTreeInternal,
    pub(crate) objtype: ObjType,
    /// The id of the parent object, root has no parent.
    pub(crate) parent: Option<ObjId>,
    /// record the last list index and tree position
    /// inserted into the op_set - this allows us to
    /// short circuit the query if the follow op is another
    /// insert or delete at the same spot
    pub(crate) last_insert: Option<(usize, usize)>,
}

impl OpTree {
    pub(crate) fn new() -> Self {
        Self {
            internal: Default::default(),
            objtype: ObjType::Map,
            parent: None,
            last_insert: None,
        }
    }

    pub(crate) fn iter(&self) -> OpTreeIter<'_> {
        self.internal.iter()
    }

    pub(crate) fn len(&self) -> usize {
        self.internal.len()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct OpTreeInternal {
    pub(crate) root_node: Option<OpTreeNode>,
    pub(crate) ops: Vec<Op>,
}

impl OpTreeInternal {
    /// Construct a new, empty, sequence.
    pub(crate) fn new() -> Self {
        Self {
            root_node: None,
            ops: vec![],
        }
    }

    /// Get the length of the sequence.
    pub(crate) fn len(&self) -> usize {
        self.root_node.as_ref().map_or(0, |n| n.len())
    }

    pub(crate) fn keys(&self) -> Option<query::Keys<'_>> {
        if self.root_node.is_some() {
            Some(query::Keys::new(self))
        } else {
            None
        }
    }

    pub(crate) fn keys_at(&self, clock: Clock) -> Option<query::KeysAt<'_>> {
        if self.root_node.is_some() {
            Some(query::KeysAt::new(self, clock))
        } else {
            None
        }
    }

    pub(crate) fn map_range<'a, R: RangeBounds<String>>(
        &'a self,
        range: R,
        meta: &'a OpSetMetadata,
    ) -> Option<query::MapRange<'a, R>> {
        if self.root_node.is_some() {
            Some(query::MapRange::new(range, self, meta))
        } else {
            None
        }
    }

    pub(crate) fn map_range_at<'a, R: RangeBounds<String>>(
        &'a self,
        range: R,
        meta: &'a OpSetMetadata,
        clock: Clock,
    ) -> Option<query::MapRangeAt<'a, R>> {
        if self.root_node.is_some() {
            Some(query::MapRangeAt::new(range, self, meta, clock))
        } else {
            None
        }
    }

    pub(crate) fn list_range<R: RangeBounds<usize>>(
        &self,
        range: R,
    ) -> Option<query::ListRange<'_, R>> {
        if self.root_node.is_some() {
            Some(query::ListRange::new(range, self))
        } else {
            None
        }
    }

    pub(crate) fn list_range_at<R: RangeBounds<usize>>(
        &self,
        range: R,
        clock: Clock,
    ) -> Option<query::ListRangeAt<'_, R>> {
        if self.root_node.is_some() {
            Some(query::ListRangeAt::new(range, clock, self))
        } else {
            None
        }
    }

    pub(crate) fn search<'a, 'b: 'a, Q>(&'b self, mut query: Q, m: &OpSetMetadata) -> Q
    where
        Q: TreeQuery<'a>,
    {
        self.root_node.as_ref().map(|root| {
            match query.query_node_with_metadata(root, m, &self.ops) {
                QueryResult::Descend => root.search(&mut query, m, &self.ops, None),
                QueryResult::Skip(skip) => root.search(&mut query, m, &self.ops, Some(skip)),
                _ => true,
            }
        });
        query
    }

    /// Create an iterator through the sequence.
    pub(crate) fn iter(&self) -> OpTreeIter<'_> {
        iter::OpTreeIter::new(self)
    }

    /// Insert the `element` into the sequence at `index`.
    ///
    /// # Panics
    ///
    /// Panics if `index > len`.
    pub(crate) fn insert(&mut self, index: usize, op: Op) {
        assert!(
            index <= self.len(),
            "tried to insert at {} but len is {}",
            index,
            self.len()
        );

        let element = self.ops.len();
        self.ops.push(op);

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
                root.split_child(0, &self.ops);

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
                root.index.insert(&self.ops[element]);
                child.insert_into_non_full_node(insertion_index, element, &self.ops)
            } else {
                root.insert_into_non_full_node(index, element, &self.ops)
            }
        } else {
            let mut root = OpTreeNode::new();
            root.insert_into_non_full_node(index, element, &self.ops);
            self.root_node = Some(root)
        }
        assert_eq!(self.len(), old_len + 1, "{:#?}", self);
    }

    /// Get the `element` at `index` in the sequence.
    pub(crate) fn get(&self, index: usize) -> Option<&Op> {
        self.root_node
            .as_ref()
            .and_then(|n| n.get(index))
            .map(|n| &self.ops[n])
    }

    // this replaces get_mut() because it allows the indexes to update correctly
    pub(crate) fn update<F>(&mut self, index: usize, f: F)
    where
        F: FnOnce(&mut Op),
    {
        if self.len() > index {
            let n = self.root_node.as_ref().unwrap().get(index).unwrap();
            let new_element = self.ops.get_mut(n).unwrap();
            let old_vis = new_element.visible();
            f(new_element);
            let vis = ChangeVisibility {
                old_vis,
                new_vis: new_element.visible(),
                op: new_element,
            };
            self.root_node.as_mut().unwrap().update(index, vis);
        }
    }

    /// Removes the element at `index` from the sequence.
    ///
    /// # Panics
    ///
    /// Panics if `index` is out of bounds.
    pub(crate) fn remove(&mut self, index: usize) -> Op {
        if let Some(root) = self.root_node.as_mut() {
            #[cfg(debug_assertions)]
            let len = root.check();
            let old = root.remove(index, &self.ops);

            if root.elements.is_empty() {
                if root.is_leaf() {
                    self.root_node = None;
                } else {
                    self.root_node = Some(root.children.remove(0));
                }
            }

            #[cfg(debug_assertions)]
            debug_assert_eq!(len, self.root_node.as_ref().map_or(0, |r| r.check()) + 1);
            self.ops[old].clone()
        } else {
            panic!("remove from empty tree")
        }
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
    use crate::types::{Op, OpId, OpType};

    use super::*;

    fn op() -> Op {
        let zero = OpId::new(0, 0);
        Op {
            id: zero,
            action: OpType::Put(0.into()),
            key: zero.into(),
            succ: Default::default(),
            pred: Default::default(),
            insert: false,
        }
    }

    #[test]
    fn insert() {
        let mut t: OpTree = OpTree::new();

        t.internal.insert(0, op());
        t.internal.insert(1, op());
        t.internal.insert(0, op());
        t.internal.insert(0, op());
        t.internal.insert(0, op());
        t.internal.insert(3, op());
        t.internal.insert(4, op());
    }

    #[test]
    fn insert_book() {
        let mut t: OpTree = OpTree::new();

        for i in 0..100 {
            t.internal.insert(i % 2, op());
        }
    }

    #[test]
    fn insert_book_vec() {
        let mut t: OpTree = OpTree::new();
        let mut v = Vec::new();

        for i in 0..100 {
            t.internal.insert(i % 3, op());
            v.insert(i % 3, op());

            assert_eq!(v, t.internal.iter().cloned().collect::<Vec<_>>())
        }
    }
}
