use crate::iter::TopOps;
pub(crate) use crate::op_set::OpSetMetadata;
use crate::{
    clock::Clock,
    query::{self, ChangeVisibility, Index, QueryResult, TreeQuery},
    Automerge, OpObserver,
};
use crate::{
    types::{Key, ListEncoding, ObjId, ObjMeta, Op, OpId, Prop},
    ObjType, OpType,
};
use std::cmp::Ordering;
use std::{fmt::Debug, mem};

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
    pub(crate) last_insert: Option<LastInsert>,
}

#[derive(Debug, Clone, PartialEq)]
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
}

#[derive(Default, Clone, Debug)]
pub(crate) struct FoundOpWithoutObserver {
    pub(crate) succ: Vec<usize>,
    pub(crate) pos: usize,
}

#[derive(Default, Clone, Debug)]
pub(crate) struct FoundOpWithObserver<'a> {
    pub(crate) before: Option<&'a Op>,
    pub(crate) num_before: usize,
    pub(crate) overwritten: Option<&'a Op>,
    pub(crate) after: Option<&'a Op>,
    pub(crate) succ: Vec<usize>,
    pub(crate) pos: usize,
    pub(crate) index: usize,
}

impl<'a> FoundOpWithObserver<'a> {
    pub(crate) fn observe<Obs: OpObserver>(
        &self,
        obj: &ObjMeta,
        op: &Op,
        doc: &Automerge,
        observer: &mut Obs,
    ) {
        let ex_obj = doc.ops().id_to_exid(obj.id.0);

        if op.insert {
            if op.is_mark() {
                if let OpType::MarkEnd(_) = op.action {
                    let q = doc.ops().search(
                        &obj.id,
                        query::SeekMark::new(op.id.prev(), self.pos, obj.encoding),
                    );
                    observer.mark(doc, ex_obj, q.marks.into_iter());
                }
            } else if obj.typ == ObjType::Text {
                observer.splice_text(doc, ex_obj, self.index, op.to_str());
            } else {
                let value = (op.value(), doc.ops().id_to_exid(op.id));
                observer.insert(doc, ex_obj, self.index, value, false);
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
                    Prop::Map(k) => observer.delete_map(doc, ex_obj, &k),
                    Prop::Seq(index) => {
                        observer.delete_seq(doc, ex_obj, index, over.width(obj.encoding))
                    }
                },
                (Some(before), Some(_), None) => {
                    let value = (before.value(), doc.ops().id_to_exid(before.id));
                    let conflict = self.num_before > 1;
                    observer.expose(doc, ex_obj, key, value, conflict);
                }
                _ => { /* do nothing */ }
            }
        } else if let Some(value) = op.get_increment_value() {
            // only observe this increment if the counter is visible, i.e. the counter's
            if self.after.is_none() {
                if let Some(counter) = self.overwritten {
                    if op.overwrites(counter) {
                        observer.increment(doc, ex_obj, key, (value, doc.ops().id_to_exid(op.id)));
                    }
                }
            }
        } else {
            let conflict = self.before.is_some();
            let value = (op.value(), doc.ops().id_to_exid(op.id));
            if op.is_list_op()
                && self.overwritten.is_none()
                && self.before.is_none()
                && self.after.is_none()
            {
                observer.insert(doc, ex_obj, self.index, value, conflict);
            } else if self.after.is_some() {
                if self.before.is_none() {
                    observer.flag_conflict(doc, ex_obj, key);
                }
            } else {
                observer.put(doc, ex_obj, key, value, conflict);
            }
        }
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

    pub(crate) fn top_ops(&self, clock: Option<Clock>) -> TopOps<'_> {
        TopOps::new(OpTreeIter::new(self), clock)
    }

    pub(crate) fn found_op_without_observer(
        &self,
        meta: &OpSetMetadata,
        op: &Op,
        mut pos: usize,
    ) -> FoundOpWithoutObserver {
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

        FoundOpWithoutObserver { pos, succ }
    }

    pub(crate) fn found_op_with_observer<'a>(
        &'a self,
        meta: &OpSetMetadata,
        op: &'a Op,
        mut pos: usize,
        index: usize,
    ) -> FoundOpWithObserver<'a> {
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

        FoundOpWithObserver {
            before,
            num_before,
            after,
            overwritten,
            succ,
            pos,
            index,
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

            if e.visible() {
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
            visible: op.visible(),
        })
    }

    pub(crate) fn find_op_with_observer<'a>(
        &'a self,
        op: &'a Op,
        encoding: ListEncoding,
        meta: &OpSetMetadata,
    ) -> FoundOpWithObserver<'a> {
        if let Key::Seq(_) = op.key {
            let query = self.search(query::OpIdSearch::op(op, encoding), meta);
            let pos = query.pos();
            let index = query.index();
            self.found_op_with_observer(meta, op, pos, index)
        } else {
            let pos = self.binary_search_by(|o| meta.key_cmp(&o.key, &op.key));
            self.found_op_with_observer(meta, op, pos, 0)
        }
    }

    pub(crate) fn find_op_without_observer(
        &self,
        op: &Op,
        meta: &OpSetMetadata,
    ) -> FoundOpWithoutObserver {
        if let Key::Seq(_) = op.key {
            let query = self.search(query::SimpleOpIdSearch::op(op), meta);
            let pos = query.pos;
            self.found_op_without_observer(meta, op, pos)
        } else {
            let pos = self.binary_search_by(|o| meta.key_cmp(&o.key, &op.key));
            self.found_op_without_observer(meta, op, pos)
        }
    }

    pub(crate) fn seek_ops_by_prop<'a>(
        &'a self,
        meta: &OpSetMetadata,
        prop: Prop,
        encoding: ListEncoding,
        clock: Option<&Clock>,
    ) -> Option<OpsFound<'a>> {
        match prop {
            Prop::Map(key_name) => {
                let key = Key::Map(meta.props.lookup(&key_name)?);
                let pos = self.binary_search_by(|op| meta.key_cmp(&op.key, &key));
                Some(OpsFound::new(pos, self.iter(), key, clock))
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

    pub(crate) fn search<'a, 'b: 'a, Q>(&'b self, mut query: Q, m: &OpSetMetadata) -> Q
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

impl<'a> OpsFound<'a> {
    fn new<T: Iterator<Item = &'a Op>>(
        start_pos: usize,
        mut iter: T,
        key: Key,
        clock: Option<&Clock>,
    ) -> Self {
        let mut found = Self {
            end_pos: start_pos,
            ops: vec![],
            ops_pos: vec![],
        };
        let mut next = iter.nth(start_pos);
        while let Some(op) = next {
            if op.elemid_or_key() == key {
                if op.visible_at(clock) {
                    found.ops.push(op);
                    found.ops_pos.push(found.end_pos);
                }
                found.end_pos += 1;
            } else {
                break;
            }
            next = iter.next();
        }
        found
    }
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
