use crate::iter::TopOps;
use crate::marks::MarkSet;
pub(crate) use crate::op_set::OpSetMetadata;
use crate::patches::PatchLog;
use crate::{
    clock::Clock,
    query::{self, ChangeVisibility, Index, QueryResult, TreeQuery},
    Automerge,
};
use crate::{
    types::{Key, ListEncoding, ObjId, ObjMeta, Op, OpId, Prop},
    ObjType, OpType,
};
use std::cmp::Ordering;
use std::rc::Rc;
use std::{fmt::Debug, mem};
#[cfg(feature = "optree-visualisation")]
use get_size::GetSize;

mod iter;
mod node;

pub(crate) use iter::OpTreeIter;
#[allow(unused)]
pub(crate) use node::OpTreeNode;
pub use node::B;

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "optree-visualisation", derive(GetSize))]
pub(crate) struct OpTree {
    pub(crate) internal: OpTreeInternal,
    pub(crate) objtype: ObjType,
    /// The id of the parent object, root has no parent.
    pub(crate) parent: Option<ObjId>,
    /// record the last list index and tree position
    /// inserted into the op_set - this allows us to
    /// short circuit the query if the follow op is another
    /// insert or delete at the same spot
    pub(crate) last_insert: Option<LastInsert>,
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "optree-visualisation", derive(GetSize))]
pub(crate) struct LastInsert {
    pub(crate) pos: usize,
    pub(crate) index: usize,
    pub(crate) width: usize,
    pub(crate) key: Key,
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

    pub(crate) fn index(&self, encoding: ListEncoding) -> Option<&Index> {
        let node = self.internal.root_node.as_ref()?;
        if encoding == ListEncoding::List || node.index.has_never_seen_puts() {
            Some(&node.index)
        } else {
            None
        }
    }

    #[cfg(feature = "optree-visualisation")]
    pub(crate) fn stats(&self, m: &OpSetMetadata) -> query::stats::OpTreeStats {
        let q = self.internal.search(query::stats::StatsQuery::new(), m);
        q.result()
    }
}

#[derive(Default, Clone, Debug)]
pub(crate) struct FoundOpWithoutPatchLog {
    pub(crate) succ: Vec<usize>,
    pub(crate) pos: usize,
}

#[derive(Default, Clone, Debug)]
pub(crate) struct FoundOpWithPatchLog<'a> {
    pub(crate) before: Option<&'a Op>,
    pub(crate) num_before: usize,
    pub(crate) overwritten: Option<&'a Op>,
    pub(crate) after: Option<&'a Op>,
    pub(crate) succ: Vec<usize>,
    pub(crate) pos: usize,
    pub(crate) index: usize,
    pub(crate) marks: Option<Rc<MarkSet>>,
}

impl<'a> FoundOpWithPatchLog<'a> {
    pub(crate) fn log_patches(
        &self,
        obj: &ObjMeta,
        op: &Op,
        doc: &Automerge,
        patch_log: &mut PatchLog,
    ) {
        if op.insert {
            if op.is_mark() {
                if let OpType::MarkEnd(_) = op.action {
                    let q = doc.ops().search(
                        &obj.id,
                        query::SeekMark::new(op.id.prev(), self.pos, obj.encoding),
                    );
                    for mark in q.marks {
                        let index = mark.start;
                        let len = mark.len();
                        let marks = mark.into_mark_set();
                        patch_log.mark(obj.id, index, len, &marks);
                    }
                }
            } else if obj.typ == ObjType::Text {
                patch_log.splice(obj.id, self.index, op.to_str(), self.marks.clone());
            } else {
                patch_log.insert(
                    obj.id,
                    self.index,
                    op.value().into(),
                    op.id,
                    false,
                    self.marks.clone(),
                );
            }
            return;
        }

        let key: Prop = match op.key {
            Key::Map(i) => doc.ops().m.props[i].clone().into(),
            Key::Seq(_) => self.index.into(),
        };

        if op.is_delete() {
            match (self.before, self.overwritten, self.after) {
                (None, Some(over), None) => match key {
                    Prop::Map(k) => patch_log.delete_map(obj.id, &k),
                    Prop::Seq(index) => {
                        patch_log.delete_seq(obj.id, index, over.width(obj.encoding))
                    }
                },
                (Some(before), Some(_), None) => {
                    let conflict = self.num_before > 1;
                    patch_log.put(
                        obj.id,
                        &key,
                        before.value().into(),
                        before.id,
                        conflict,
                        true,
                    );
                }
                _ => { /* do nothing */ }
            }
        } else if let Some(value) = op.get_increment_value() {
            if self.after.is_none() {
                if let Some(counter) = self.overwritten {
                    if op.overwrites(counter) {
                        patch_log.increment(obj.id, &key, value, op.id);
                    }
                }
            }
        } else {
            let conflict = self.before.is_some();
            //let value = (op.value(), doc.ops().id_to_exid(op.id));
            if op.is_list_op()
                && self.overwritten.is_none()
                && self.before.is_none()
                && self.after.is_none()
            {
                patch_log.insert(obj.id, self.index, op.value().into(), op.id, conflict, None);
            } else if self.after.is_some() {
                if self.before.is_none() {
                    patch_log.flag_conflict(obj.id, &key);
                }
            } else {
                patch_log.put(obj.id, &key, op.value().into(), op.id, conflict, false);
            }
        }
    }
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "optree-visualisation", derive(GetSize))]
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

    pub(crate) fn top_ops<'a>(
        &'a self,
        clock: Option<Clock>,
        meta: &'a OpSetMetadata,
    ) -> TopOps<'a> {
        TopOps::new(OpTreeIter::new(self), clock, meta)
    }

    pub(crate) fn found_op_without_patch_log(
        &self,
        meta: &OpSetMetadata,
        op: &Op,
        mut pos: usize,
    ) -> FoundOpWithoutPatchLog {
        let mut iter = self.iter();
        let mut succ = vec![];
        let mut next = iter.nth(pos);
        while let Some(e) = next {
            if e.elemid_or_key() != op.elemid_or_key() {
                break;
            }

            if meta.lamport_cmp(e.id, op.id) == Ordering::Greater {
                break;
            }

            if op.overwrites(e) {
                succ.push(pos);
            }

            pos += 1;
            next = iter.next();
        }

        FoundOpWithoutPatchLog { pos, succ }
    }

    pub(crate) fn found_op_with_patch_log<'a>(
        &'a self,
        meta: &OpSetMetadata,
        op: &'a Op,
        mut pos: usize,
        index: usize,
        marks: Option<Rc<MarkSet>>,
    ) -> FoundOpWithPatchLog<'a> {
        let mut iter = self.iter();
        let mut found = None;
        let mut before = None;
        let mut num_before = 0;
        let mut overwritten = None;
        let mut after = None;
        let mut succ = vec![];
        let mut next = iter.nth(pos);
        while let Some(e) = next {
            if e.elemid_or_key() != op.elemid_or_key() {
                break;
            }

            if found.is_none() && meta.lamport_cmp(e.id, op.id) == Ordering::Greater {
                found = Some(pos);
            }

            if op.overwrites(e) {
                succ.push(pos);

                if e.visible() {
                    overwritten = Some(e);
                }
            } else if e.visible() {
                if found.is_none() && overwritten.is_none() {
                    before = Some(e);
                    num_before += 1;
                } else {
                    after = Some(e);
                }
            }

            pos += 1;
            next = iter.next();
        }

        pos = found.unwrap_or(pos);

        FoundOpWithPatchLog {
            before,
            num_before,
            after,
            overwritten,
            succ,
            pos,
            index,
            marks,
        }
    }

    pub(crate) fn seek_opid<'a>(
        &'a self,
        opid: OpId,
        encoding: ListEncoding,
        clock: Option<&Clock>,
        meta: &OpSetMetadata,
    ) -> Option<FoundOpId<'a>> {
        let query = self.search(query::OpIdSearch::opid(opid, encoding, clock), meta);
        let pos = query.found()?;
        let mut iter = self.iter();
        let op = iter.nth(pos)?;
        let index = query.index_for(op);
        for e in iter {
            if e.elemid_or_key() != op.elemid_or_key() {
                break;
            }

            if e.visible_at(clock) {
                return Some(FoundOpId {
                    op,
                    index,
                    visible: false,
                });
            }
        }
        Some(FoundOpId {
            op,
            index,
            visible: op.visible_at(clock),
        })
    }

    pub(crate) fn find_op_with_patch_log<'a>(
        &'a self,
        op: &'a Op,
        encoding: ListEncoding,
        meta: &OpSetMetadata,
    ) -> FoundOpWithPatchLog<'a> {
        if let Key::Seq(_) = op.key {
            let query = self.search(query::OpIdSearch::op(op, encoding), meta);
            let pos = query.pos();
            let index = query.index();
            let marks = query.marks(meta);
            self.found_op_with_patch_log(meta, op, pos, index, marks)
        } else {
            let pos = self.binary_search_by(|o| meta.key_cmp(&o.key, &op.key));
            self.found_op_with_patch_log(meta, op, pos, 0, None)
        }
    }

    pub(crate) fn find_op_without_patch_log(
        &self,
        op: &Op,
        meta: &OpSetMetadata,
    ) -> FoundOpWithoutPatchLog {
        if let Key::Seq(_) = op.key {
            let query = self.search(query::SimpleOpIdSearch::op(op), meta);
            let pos = query.pos;
            self.found_op_without_patch_log(meta, op, pos)
        } else {
            let pos = self.binary_search_by(|o| meta.key_cmp(&o.key, &op.key));
            self.found_op_without_patch_log(meta, op, pos)
        }
    }

    pub(crate) fn seek_ops_by_prop<'a>(
        &'a self,
        meta: &'a OpSetMetadata,
        prop: Prop,
        encoding: ListEncoding,
        clock: Option<&Clock>,
    ) -> Option<OpsFound<'a>> {
        match prop {
            Prop::Map(key_name) => {
                let key = Key::Map(meta.props.lookup(&key_name)?);
                let query = self.search(query::Prop::new(key, clock.cloned()), meta);
                Some(OpsFound {
                    ops: query.ops,
                    ops_pos: query.ops_pos,
                    end_pos: query.pos,
                })
            }
            Prop::Seq(index) => {
                let query = self.search(query::Nth::new(index, encoding, clock.cloned()), meta);
                let end_pos = query.pos();
                Some(OpsFound {
                    ops: query.ops,
                    ops_pos: query.ops_pos,
                    end_pos,
                })
            }
        }
    }

    fn binary_search_by<F>(&self, f: F) -> usize
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

    pub(crate) fn search<'a, 'b: 'a, Q>(&'b self, mut query: Q, m: &'a OpSetMetadata) -> Q
    where
        Q: TreeQuery<'a>,
    {
        self.root_node.as_ref().map(|root| {
            match query.query_node_with_metadata(root, m, &self.ops) {
                QueryResult::Descend => root.search(&mut query, m, &self.ops),
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

#[derive(Default, Debug, Clone, PartialEq)]
pub(crate) struct OpsFound<'a> {
    pub(crate) ops: Vec<&'a Op>,
    pub(crate) ops_pos: Vec<usize>,
    pub(crate) end_pos: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FoundOpId<'a> {
    pub(crate) op: &'a Op,
    pub(crate) index: usize,
    pub(crate) visible: bool,
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
