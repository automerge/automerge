use crate::marks::MarkSet;
pub(crate) use crate::op_set::{Op, OpSetData};
use crate::op_tree::node::OpIdx;
use crate::patches::PatchLog;
use crate::{
    clock::Clock,
    query::{self, ChangeVisibility, Index, QueryResult, TreeQuery},
    Automerge,
};
use crate::{
    types::{Key, ListEncoding, ObjMeta, OpId, OpIds, Prop},
    ObjType, OpType,
};
use std::cmp::Ordering;
use std::sync::Arc;
use std::{fmt::Debug, mem};

mod iter;
mod node;

pub(crate) use iter::{OpTreeIter, OpTreeOpIter};
#[allow(unused)]
pub(crate) use node::OpTreeNode;
pub use node::B;

#[derive(Debug, Clone)]
pub(crate) struct OpTree {
    pub(crate) internal: OpTreeInternal,
    pub(crate) objtype: ObjType,
    /// The id of the parent object, root has no parent.
    pub(crate) parent: Option<OpIdx>,
    /// record the last list index and tree position
    /// inserted into the op_set - this allows us to
    /// short circuit the query if the follow op is another
    /// insert or delete at the same spot
    pub(crate) last_insert: Option<LastInsert>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LastInsert {
    pub(crate) pos: usize,
    pub(crate) index: usize,
    pub(crate) width: usize,
    pub(crate) key: Key,
    pub(crate) marks: Option<Arc<MarkSet>>,
}

impl OpTree {
    pub(crate) fn new(objtype: ObjType) -> Self {
        Self {
            internal: OpTreeInternal::new(objtype),
            objtype,
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

    pub(crate) fn add_index(&mut self, osd: &OpSetData) {
        self.internal.has_index = true;
        if let Some(root) = self.internal.root_node.as_mut() {
            root.add_index(osd);
        }
    }

    pub(crate) fn index(&self, encoding: ListEncoding) -> Option<&Index> {
        let node = self.internal.root_node.as_ref()?;
        let index = node.index.as_ref()?;
        if encoding == ListEncoding::List || index.has_never_seen_puts() {
            Some(index)
        } else {
            None
        }
    }
}

#[derive(Default, Clone, Debug)]
pub(crate) struct FoundOpWithoutPatchLog {
    pub(crate) succ: Vec<usize>,
    pub(crate) pos: usize,
}

#[derive(Default, Clone, Debug)]
pub(crate) struct FoundOpWithPatchLog<'a> {
    pub(crate) before: Option<Op<'a>>,
    pub(crate) num_before: usize,
    pub(crate) overwritten: Option<Op<'a>>,
    pub(crate) after: Option<Op<'a>>,
    pub(crate) succ: Vec<usize>,
    pub(crate) pos: usize,
    pub(crate) index: usize,
    pub(crate) marks: Option<Arc<MarkSet>>,
}

impl<'a> FoundOpWithPatchLog<'a> {
    pub(crate) fn log_patches(
        &self,
        obj: &ObjMeta,
        op: Op<'_>,
        pred: &OpIds,
        doc: &Automerge,
        patch_log: &mut PatchLog,
    ) {
        if op.insert() {
            if op.is_mark() {
                if let OpType::MarkEnd(_) = op.action() {
                    let q = doc.ops().search(
                        &obj.id,
                        query::SeekMark::new(
                            op.id().prev(),
                            self.pos,
                            patch_log.text_rep().encoding(obj.typ),
                        ),
                    );
                    for mark in q.finish() {
                        let index = mark.start;
                        let len = mark.len();
                        let marks = mark.into_mark_set();
                        patch_log.mark(obj.id, index, len, &marks);
                    }
                }
            // TODO - move this into patch_log()
            } else if obj.typ == ObjType::Text && !op.action().is_block() {
                patch_log.splice(obj.id, self.index, op.as_str(), self.marks.clone());
            } else {
                patch_log.insert(obj.id, self.index, op.value().into(), *op.id(), false);
            }
            return;
        }

        let key: Prop = match *op.key() {
            Key::Map(i) => doc.ops().osd.props[i].clone().into(),
            Key::Seq(_) => self.index.into(),
        };

        if op.is_delete() {
            match (self.before, self.overwritten, self.after) {
                (None, Some(over), None) => match key {
                    Prop::Map(k) => patch_log.delete_map(obj.id, &k),
                    Prop::Seq(index) => patch_log.delete_seq(
                        obj.id,
                        index,
                        over.width(patch_log.text_rep().encoding(obj.typ)),
                    ),
                },
                (Some(before), Some(_), None) => {
                    let conflict = self.num_before > 1;
                    patch_log.put(
                        obj.id,
                        &key,
                        before.value().into(),
                        *before.id(),
                        conflict,
                        true,
                    );
                }
                _ => { /* do nothing */ }
            }
        } else if let Some(value) = op.get_increment_value() {
            if self.after.is_none() {
                if let Some(counter) = self.overwritten {
                    if pred.overwrites(counter.id()) {
                        patch_log.increment(obj.id, &key, value, *op.id());
                    }
                }
            }
        } else {
            let conflict = self.before.is_some();
            if op.is_list_op()
                && self.overwritten.is_none()
                && self.before.is_none()
                && self.after.is_none()
            {
                patch_log.insert(obj.id, self.index, op.value().into(), *op.id(), conflict);
            } else if self.after.is_some() {
                if self.before.is_none() {
                    patch_log.flag_conflict(obj.id, &key);
                }
            } else {
                patch_log.put(obj.id, &key, op.value().into(), *op.id(), conflict, false);
            }
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct OpTreeInternal {
    pub(crate) root_node: Option<OpTreeNode>,
    pub(crate) has_index: bool,
}

impl OpTreeInternal {
    /// Construct a new, empty, sequence.
    pub(crate) fn new(obj_type: ObjType) -> Self {
        let has_index = obj_type.is_sequence();
        Self {
            root_node: None,
            has_index,
        }
    }

    /// Get the length of the sequence.
    pub(crate) fn len(&self) -> usize {
        self.root_node.as_ref().map_or(0, |n| n.len())
    }

    pub(crate) fn found_op_without_patch_log(
        &self,
        osd: &OpSetData,
        op: Op<'_>,
        pred: &OpIds,
        mut pos: usize,
    ) -> FoundOpWithoutPatchLog {
        let mut iter = self.iter();
        let mut succ = vec![];
        let mut next = iter.nth(pos);
        while let Some(idx) = next {
            let e = idx.as_op(osd);
            if e.elemid_or_key() != op.elemid_or_key() {
                break;
            }

            if e.lamport_cmp(*op.id()) == Ordering::Greater {
                break;
            }

            if pred.overwrites(e.id()) {
                succ.push(pos);
            }

            pos += 1;
            next = iter.next();
        }

        FoundOpWithoutPatchLog { pos, succ }
    }

    pub(crate) fn found_op_with_patch_log<'a>(
        &'a self,
        osd: &'a OpSetData,
        op: Op<'a>,
        pred: &OpIds,
        mut pos: usize,
        index: usize,
        marks: Option<Arc<MarkSet>>,
    ) -> FoundOpWithPatchLog<'a> {
        let mut iter = self.iter();
        let mut found = None;
        let mut before = None;
        let mut num_before = 0;
        let mut overwritten = None;
        let mut after = None;
        let mut succ = vec![];
        let mut next = iter.nth(pos);
        while let Some(idx) = next {
            let e = idx.as_op(osd);
            if e.elemid_or_key() != op.elemid_or_key() {
                break;
            }

            if found.is_none() && e.lamport_cmp(*op.id()) == Ordering::Greater {
                found = Some(pos);
            }

            if pred.overwrites(e.id()) {
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

    pub(crate) fn seek_map_op<'a>(
        &'a self,
        op: Op<'a>,
        clock: Option<&Clock>,
        osd: &'a OpSetData,
    ) -> Option<FoundOpId<'a>> {
        let pos = self.binary_search_by(osd, |o| o.key_cmp(op.key()).then_with(|| o.cmp(&op)));
        let mut iter = self.iter();
        let op2 = iter.nth(pos).map(|idx| idx.as_op(osd))?;
        assert_eq!(op, op2);
        let index = 0;
        for e in iter.map(|idx| idx.as_op(osd)) {
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

    pub(crate) fn seek_idx<'a>(
        &'a self,
        idx: OpIdx,
        encoding: ListEncoding,
        clock: Option<&Clock>,
        osd: &'a OpSetData,
    ) -> Option<FoundOpId<'a>> {
        let op = idx.as_op(osd);
        if let Key::Map(_) = op.key() {
            self.seek_map_op(op, clock, osd)
        } else {
            self.seek_list_opid(*op.id(), encoding, clock, osd)
        }
    }

    pub(crate) fn seek_list_opid<'a>(
        &'a self,
        opid: OpId,
        encoding: ListEncoding,
        clock: Option<&Clock>,
        osd: &'a OpSetData,
    ) -> Option<FoundOpId<'a>> {
        let query = self.search(query::OpIdSearch::opid(opid, encoding, clock), osd);
        let pos = query.found()?;
        let mut iter = self.iter();
        let op = iter.nth(pos).map(|idx| idx.as_op(osd))?;
        let index = query.index_for(op);
        for idx in iter {
            let e = idx.as_op(osd);

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
        op: Op<'a>,
        pred: &OpIds,
        encoding: ListEncoding,
        osd: &'a OpSetData,
    ) -> FoundOpWithPatchLog<'a> {
        if let Key::Seq(_) = *op.key() {
            let query = self.search(query::OpIdSearch::op(op, encoding), osd);
            let pos = query.pos();
            let index = query.index();
            let marks = query.marks(osd);
            self.found_op_with_patch_log(osd, op, pred, pos, index, marks)
        } else {
            let pos = self.binary_search_by(osd, |o| o.key_cmp(op.key()));
            self.found_op_with_patch_log(osd, op, pred, pos, 0, None)
        }
    }

    pub(crate) fn find_op_without_patch_log(
        &self,
        op: Op<'_>,
        pred: &OpIds,
        osd: &OpSetData,
    ) -> FoundOpWithoutPatchLog {
        if let Key::Seq(_) = *op.key() {
            let query = self.search(query::SimpleOpIdSearch::op(op), osd);
            let pos = query.pos;
            self.found_op_without_patch_log(osd, op, pred, pos)
        } else {
            let pos = self.binary_search_by(osd, |o| o.key_cmp(op.key()));
            self.found_op_without_patch_log(osd, op, pred, pos)
        }
    }

    pub(crate) fn seek_ops_by_prop<'a>(
        &'a self,
        osd: &'a OpSetData,
        prop: Prop,
        encoding: ListEncoding,
        clock: Option<&Clock>,
    ) -> Option<OpsFound<'a>> {
        match prop {
            Prop::Map(key_name) => self.seek_ops_by_map_key(osd, key_name, clock),
            Prop::Seq(index) => self.seek_ops_by_index(osd, index, encoding, clock),
        }
    }

    pub(crate) fn seek_ops_by_map_key<'a>(
        &'a self,
        osd: &'a OpSetData,
        key_name: String,
        clock: Option<&Clock>,
    ) -> Option<OpsFound<'a>> {
        let key = Key::Map(osd.props.lookup(&key_name)?);
        let mut pos = self.binary_search_by(osd, |o| o.key_cmp(&key));
        let mut iter = self.iter();
        let mut next = iter.nth(pos);
        let mut ops = vec![];
        let mut ops_pos = vec![];
        while let Some(op) = next {
            let op = op.as_op(osd);
            match op.key_cmp(&key) {
                Ordering::Greater => {
                    break;
                }
                Ordering::Equal if op.visible_at(clock) => {
                    ops.push(op);
                    ops_pos.push(pos);
                }
                _ => {}
            }
            pos += 1;
            next = iter.next();
        }
        Some(OpsFound {
            ops,
            ops_pos,
            end_pos: pos,
        })
    }

    pub(crate) fn seek_ops_by_index<'a>(
        &'a self,
        osd: &'a OpSetData,
        index: usize,
        encoding: ListEncoding,
        clock: Option<&Clock>,
    ) -> Option<OpsFound<'a>> {
        let query = self.search(query::Nth::new(index, encoding, clock.cloned(), osd), osd);
        let end_pos = query.pos();
        Some(OpsFound {
            ops: query.ops,
            ops_pos: query.ops_pos,
            end_pos,
        })
    }

    fn binary_search_by<F>(&self, osd: &OpSetData, f: F) -> usize
    where
        F: Fn(Op<'_>) -> Ordering,
    {
        let mut right = self.len();
        let mut left = 0;
        while left < right {
            let seq = (left + right) / 2;
            if f(self.get(seq).unwrap().as_op(osd)) == Ordering::Less {
                left = seq + 1;
            } else {
                right = seq;
            }
        }
        left
    }

    pub(crate) fn search<'a, 'b: 'a, Q>(&'b self, mut query: Q, osd: &'a OpSetData) -> Q
    where
        Q: TreeQuery<'a>,
    {
        self.root_node.as_ref().map(|root| {
            if let Some(index) = root.index.as_ref() {
                match query.query_node(root, index, osd) {
                    QueryResult::Descend => root.search(&mut query, osd),
                    _ => true,
                }
            } else {
                // the only thing using this branch is rollback and it has a rewrite coming soon
                root.search(&mut query, osd)
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
    pub(crate) fn insert(&mut self, index: usize, element: OpIdx, osd: &OpSetData) {
        assert!(
            index <= self.len(),
            "tried to insert at {} but len is {}",
            index,
            self.len()
        );

        let old_len = self.len();
        if let Some(root) = self.root_node.as_mut() {
            #[cfg(debug_assertions)]
            root.check();

            if root.is_full() {
                let original_len = root.len();
                let new_root = OpTreeNode::new(root.index.is_some());

                // move new_root to root position
                let old_root = mem::replace(root, new_root);

                root.length += old_root.len();
                root.index.clone_from(&old_root.index);
                root.children.push(old_root);
                root.split_child(0, osd);

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
                child.insert_into_non_full_node(insertion_index, element, osd);
                root.index_insert(element.as_op(osd))
            } else {
                root.insert_into_non_full_node(index, element, osd)
            }
        } else {
            let mut root = OpTreeNode::new(self.has_index);
            root.insert_into_non_full_node(index, element, osd);
            self.root_node = Some(root)
        }
        assert_eq!(self.len(), old_len + 1, "{:#?}", self);
    }

    /// Get the `element` at `index` in the sequence.
    pub(crate) fn get(&self, index: usize) -> Option<OpIdx> {
        self.root_node.as_ref().and_then(|n| n.get(index))
    }

    // this replaces get_mut() because it allows the indexes to update correctly
    pub(crate) fn update(&mut self, index: usize, vis: ChangeVisibility<'_>, osd: &OpSetData) {
        if self.len() > index {
            self.root_node.as_mut().unwrap().update(index, vis, osd);
        }
    }

    /// Removes the element at `index` from the sequence.
    ///
    /// # Panics
    ///
    /// Panics if `index` is out of bounds.
    pub(crate) fn remove(&mut self, index: usize, osd: &OpSetData) -> OpIdx {
        if let Some(root) = self.root_node.as_mut() {
            #[cfg(debug_assertions)]
            let len = root.check();
            let old = root.remove(index, osd);

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

/*
impl Default for OpTreeInternal {
    fn default() -> Self {
        Self::new(true)
    }
}
*/

/*
impl PartialEq for OpTreeInternal {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len() && self.iter().zip(other.iter()).all(|(a, b)| a == b)
    }
}
*/

#[derive(Default, Debug, Clone, PartialEq)]
pub(crate) struct OpsFound<'a> {
    pub(crate) ops: Vec<Op<'a>>,
    pub(crate) ops_pos: Vec<usize>,
    pub(crate) end_pos: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FoundOpId<'a> {
    pub(crate) op: Op<'a>,
    pub(crate) index: usize,
    pub(crate) visible: bool,
}

#[cfg(test)]
mod tests {
    use crate::op_set::{OpIdx, OpSetData};
    use crate::types::{OpBuilder, OpId, OpType, ROOT};

    use super::*;

    fn op(osd: &mut OpSetData) -> OpIdx {
        let zero = OpId::new(0, 0);
        let op = OpBuilder {
            id: zero,
            action: OpType::Put(0.into()),
            key: zero.into(),
            insert: false,
        };
        osd.push(ROOT.into(), op)
    }

    #[test]
    fn insert() {
        let mut t: OpTree = OpTree::new(ObjType::List);
        let mut osd = OpSetData::default();
        let d = &mut osd;
        t.internal.insert(0, op(d), d);
        t.internal.insert(1, op(d), d);
        t.internal.insert(0, op(d), d);
        t.internal.insert(0, op(d), d);
        t.internal.insert(0, op(d), d);
        t.internal.insert(3, op(d), d);
        t.internal.insert(4, op(d), d);
    }

    #[test]
    fn insert_book() {
        let mut t: OpTree = OpTree::new(ObjType::List);
        let mut osd = OpSetData::default();

        for i in 0..100 {
            t.internal.insert(i % 2, op(&mut osd), &osd);
        }
    }

    #[test]
    fn insert_book_vec() {
        let mut t: OpTree = OpTree::new(ObjType::List);
        let mut v = Vec::new();

        let mut osd = OpSetData::default();
        for i in 0..100 {
            let idx = op(&mut osd);
            t.internal.insert(i % 3, idx, &osd);
            v.insert(i % 3, idx);

            assert_eq!(v, t.internal.iter().collect::<Vec<_>>())
        }
    }
}
