use crate::clock::Clock;
use crate::error::AutomergeError;
use crate::exid::ExId;
use crate::indexed_cache::IndexedCache;
use crate::iter::{Keys, ListRange, MapRange, TopOps};
use crate::marks::MarkSet;
use crate::op_tree::OpTreeIter;
use crate::op_tree::{
    self, FoundOpId, FoundOpWithPatchLog, FoundOpWithoutPatchLog, LastInsert, OpTree,
    OpTreeInternal, OpsFound,
};
use crate::parents::Parents;
use crate::patches::TextRepresentation;
use crate::query::{ChangeVisibility, TreeQuery};
use crate::text_value::TextValue;
use crate::types::{
    self, ActorId, Export, Exportable, Key, ListEncoding, ObjId, ObjMeta, OpId, OpIds, OpType, Prop,
};
use crate::ObjType;
use fxhash::FxBuildHasher;
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::ops::RangeBounds;
use std::sync::Arc;

mod op;

pub(crate) use op::{Op, OpBuilder, OpDepIdx, OpDepRaw, OpIdx, OpRaw};

pub(crate) type OpSet = OpSetInternal;

#[derive(Debug, Copy, Clone)]
pub(crate) struct OpIdxRange {
    start: u32,
    end: u32,
}

impl OpIdxRange {
    pub(crate) fn len(&self) -> usize {
        (self.end - self.start) as usize
    }
}

#[derive(Debug, Clone)]
pub(crate) struct OpSetInternal {
    /// The map of objects to their type and ops.
    trees: HashMap<ObjId, OpTree, FxBuildHasher>,
    /// The number of operations in the opset.
    length: usize,
    /// Metadata about the operations in this opset.
    pub(crate) osd: OpSetData,
}

impl OpSetInternal {
    pub(crate) fn from_actors(actors: Vec<ActorId>) -> Self {
        let mut trees: HashMap<_, _, _> = Default::default();
        trees.insert(ObjId::root(), OpTree::new(ObjType::Map));
        OpSetInternal {
            trees,
            length: 0,
            osd: OpSetData::from_actors(actors),
        }
    }

    pub(crate) fn new() -> Self {
        let mut trees: HashMap<_, _, _> = Default::default();
        trees.insert(ObjId::root(), OpTree::new(ObjType::Map));
        OpSetInternal {
            trees,
            length: 0,
            osd: OpSetData {
                actors: IndexedCache::new(),
                props: IndexedCache::new(),
                ops: Vec::new(),
                op_deps: Vec::new(),
            },
        }
    }

    pub(crate) fn id_to_exid(&self, id: OpId) -> ExId {
        if id == types::ROOT {
            ExId::Root
        } else {
            ExId::Id(
                id.counter(),
                self.osd.actors.cache[id.actor()].clone(),
                id.actor(),
            )
        }
    }

    pub(crate) fn iter(&self) -> Iter<'_> {
        let mut objs: Vec<_> = self.trees.iter().map(|t| (t.0, t.1.objtype, t.1)).collect();
        objs.sort_by(|a, b| self.osd.lamport_cmp((a.0).0, (b.0).0));
        Iter {
            opset: self,
            trees: objs.into_iter(),
            current: None,
            osd: &self.osd,
        }
    }

    pub(crate) fn iter_obj(&self, obj: &ObjId) -> Option<OpTreeIter<'_>> {
        self.trees.get(obj).map(|t| t.iter())
    }

    /// Iterate over objects in the opset in causal order
    pub(crate) fn iter_objs(&self) -> impl Iterator<Item = (ObjMeta, OpTreeIter<'_>)> + '_ {
        // TODO
        let mut objs: Vec<_> = self
            .trees
            .iter()
            .map(|t| (ObjMeta::new(*t.0, t.1.objtype), t.1))
            .collect();
        objs.sort_by(|a, b| self.osd.lamport_cmp((a.0).id, (b.0).id));
        IterObjs {
            trees: objs.into_iter(),
        }
    }

    pub(crate) fn iter_ops(&self, obj: &ObjId) -> impl Iterator<Item = Op<'_>> {
        self.trees
            .get(obj)
            .map(|o| o.iter())
            .into_iter()
            .flatten()
            .map(|idx| idx.as_op(&self.osd))
    }

    pub(crate) fn parents(
        &self,
        obj: ObjId,
        text_rep: TextRepresentation,
        clock: Option<Clock>,
    ) -> Parents<'_> {
        Parents {
            obj,
            ops: self,
            text_rep,
            clock,
        }
    }

    pub(crate) fn seek_idx(
        &self,
        idx: OpIdx,
        text_rep: TextRepresentation,
        clock: Option<&Clock>,
    ) -> Option<FoundOpId<'_>> {
        let obj = idx.as_op(&self.osd).obj();
        let typ = self.obj_type(obj)?;
        self.trees.get(obj).and_then(|tree| {
            tree.internal
                .seek_idx(idx, text_rep.encoding(typ), clock, &self.osd)
        })
    }

    pub(crate) fn seek_list_opid(
        &self,
        obj: &ObjId,
        id: OpId,
        encoding: ListEncoding,
        clock: Option<&Clock>,
    ) -> Option<FoundOpId<'_>> {
        self.trees
            .get(obj)
            .and_then(|tree| tree.internal.seek_list_opid(id, encoding, clock, &self.osd))
    }

    pub(crate) fn parent_object(
        &self,
        obj: &ObjId,
        text_rep: TextRepresentation,
        clock: Option<&Clock>,
    ) -> Option<Parent> {
        let idx = self.trees.get(obj)?.parent?;
        let found = self.seek_idx(idx, text_rep, clock)?;
        let obj = *found.op.obj();
        let typ = self.obj_type(&obj)?;
        let prop = found.op.map_prop().unwrap_or(Prop::Seq(found.index));
        let visible = found.visible;
        Some(Parent {
            obj,
            prop,
            visible,
            typ,
        })
    }

    pub(crate) fn seek_ops_by_prop<'a>(
        &'a self,
        obj: &ObjId,
        prop: Prop,
        encoding: ListEncoding,
        clock: Option<&Clock>,
    ) -> OpsFound<'a> {
        self.trees
            .get(obj)
            .and_then(|tree| {
                tree.internal
                    .seek_ops_by_prop(&self.osd, prop, encoding, clock)
            })
            .unwrap_or_default()
    }

    pub(crate) fn op_iter<'a>(&'a self, obj: &ObjId) -> Option<OpIter<'a>> {
        self.trees.get(obj).map(|tree| OpIter {
            iter: tree.iter(),
            osd: &self.osd,
        })
    }

    pub(crate) fn top_ops<'a>(&'a self, obj: &ObjId, clock: Option<Clock>) -> TopOps<'a> {
        self.op_iter(obj)
            .map(|iter| TopOps::new(iter, clock))
            .unwrap_or_default()
    }

    pub(crate) fn find_op_with_patch_log<'a>(
        &'a self,
        obj: &ObjMeta,
        encoding: ListEncoding,
        op: Op<'a>,
        pred: &OpIds,
    ) -> FoundOpWithPatchLog<'a> {
        if let Some(tree) = self.trees.get(&obj.id) {
            tree.internal
                .find_op_with_patch_log(op, pred, encoding, &self.osd)
        } else {
            Default::default()
        }
    }

    pub(crate) fn find_op_without_patch_log(
        &self,
        obj: &ObjId,
        op: Op<'_>,
        pred: &OpIds,
    ) -> FoundOpWithoutPatchLog {
        if let Some(tree) = self.trees.get(obj) {
            tree.internal.find_op_without_patch_log(op, pred, &self.osd)
        } else {
            Default::default()
        }
    }

    pub(crate) fn search<'a, 'b: 'a, Q>(&'b self, obj: &ObjId, mut query: Q) -> Q
    where
        Q: TreeQuery<'a>,
    {
        if let Some(tree) = self.trees.get(obj) {
            if query.can_shortcut_search(tree, &self.osd) {
                query
            } else {
                tree.internal.search(query, &self.osd)
            }
        } else {
            query
        }
    }

    /// Add `op` as a successor to each op at `op_indices` in `obj`
    pub(crate) fn add_succ(&mut self, obj: &ObjId, op_indices: &[usize], op: OpIdx) {
        if let Some(tree) = self.trees.get_mut(obj) {
            tree.last_insert = None;
            for i in op_indices {
                if let Some(idx) = tree.internal.get(*i) {
                    let old_vis = idx.as_op(&self.osd).visible();
                    self.osd.add_inc(idx, op);
                    self.osd.add_dep(idx, op);
                    let new_vis = idx.as_op(&self.osd).visible();
                    tree.internal.update(
                        *i,
                        ChangeVisibility {
                            old_vis,
                            new_vis,
                            op: idx.as_op(&self.osd),
                        },
                        &self.osd,
                    );
                }
            }
        }
    }

    pub(crate) fn remove_succ(&mut self, obj: &ObjId, index: usize, op: OpIdx) {
        if let Some(tree) = self.trees.get_mut(obj) {
            tree.last_insert = None;
            if let Some(idx) = tree.internal.get(index) {
                let old_vis = idx.as_op(&self.osd).visible();
                self.osd.remove_inc(idx, op);
                self.osd.remove_dep(idx, op);
                let new_vis = idx.as_op(&self.osd).visible();
                tree.internal.update(
                    index,
                    ChangeVisibility {
                        old_vis,
                        new_vis,
                        op: idx.as_op(&self.osd),
                    },
                    &self.osd,
                );
            }
        }
    }

    pub(crate) fn remove(&mut self, obj: &ObjId, index: usize) {
        // this happens on rollback - be sure to go back to the old state
        let tree = self.trees.get_mut(obj).unwrap();
        self.length -= 1;
        tree.last_insert = None;
        let idx = tree.internal.remove(index, &self.osd);
        let op = idx.as_op(&self.osd);
        if let OpType::Make(_) = op.action() {
            self.trees.remove(&op.id().into());
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.length
    }

    pub(crate) fn hint(
        &mut self,
        obj: &ObjId,
        index: usize,
        pos: usize,
        width: usize,
        key: Key,
        marks: Option<Arc<MarkSet>>,
    ) {
        if let Some(tree) = self.trees.get_mut(obj) {
            tree.last_insert = Some(LastInsert {
                index,
                pos,
                width,
                key,
                marks,
            })
        }
    }

    pub(crate) fn load(&mut self, obj: ObjId, op: OpBuilder) -> OpIdx {
        self.osd.push(obj, op)
    }

    pub(crate) fn load_with_range(
        &mut self,
        obj: ObjId,
        op: OpBuilder,
        range: &mut OpIdxRange,
    ) -> OpIdx {
        let idx = self.osd.push(obj, op);
        range.end += 1;
        assert!(idx.get() >= range.start as usize && idx.get() < range.end as usize);
        idx
    }

    pub(crate) fn add_indexes(&mut self) {
        for (_, tree) in self.trees.iter_mut() {
            if tree.objtype.is_sequence() {
                tree.add_index(&self.osd)
            }
        }
    }

    #[tracing::instrument(skip(self, index))]
    pub(crate) fn insert(&mut self, index: usize, obj: &ObjId, idx: OpIdx) {
        let op = idx.as_op(&self.osd);
        if let OpType::Make(typ) = op.action() {
            self.trees.insert(
                op.id().into(),
                OpTree {
                    internal: OpTreeInternal::new(*typ),
                    objtype: *typ,
                    last_insert: None,
                    parent: Some(idx),
                },
            );
        }

        if let Some(tree) = self.trees.get_mut(obj) {
            tree.last_insert = None;
            tree.internal.insert(index, idx, &self.osd);
            self.length += 1;
        } else {
            tracing::warn!("attempting to insert op for unknown object");
        }
    }

    pub(crate) fn load_idx(&mut self, obj: &ObjId, idx: OpIdx) -> Result<(), AutomergeError> {
        let op = idx.as_op(&self.osd);
        if let OpType::Make(typ) = op.action() {
            self.trees.insert(
                op.id().into(),
                OpTree {
                    internal: OpTreeInternal::new(*typ),
                    objtype: *typ,
                    last_insert: None,
                    parent: Some(idx),
                },
            );
        }

        if let Some(tree) = self.trees.get_mut(obj) {
            tree.last_insert = None;
            tree.internal.insert(tree.len(), idx, &self.osd);
            self.length += 1;
            Ok(())
        } else {
            Err(AutomergeError::NotAnObject)
        }
    }

    pub(crate) fn object_type(&self, id: &ObjId) -> Option<ObjType> {
        self.trees.get(id).map(|tree| tree.objtype)
    }

    pub(crate) fn obj_type(&self, id: &ObjId) -> Option<ObjType> {
        let objtype = self.trees.get(id).map(|tree| tree.objtype)?;
        Some(objtype)
    }

    /// Return a graphviz representation of the opset.
    ///
    /// # Arguments
    ///
    /// * objects: An optional list of object IDs to display, if not specified all objects are
    ///            visualised
    #[cfg(feature = "optree-visualisation")]
    pub(crate) fn visualise(&self, objects: Option<Vec<ObjId>>) -> String {
        use std::borrow::Cow;
        let mut out = Vec::new();
        let trees = if let Some(objects) = objects {
            let mut filtered = self.trees.clone();
            filtered.retain(|k, _| objects.contains(k));
            Cow::Owned(filtered)
        } else {
            Cow::Borrowed(&self.trees)
        };
        let graph = super::visualisation::GraphVisualisation::construct(&trees, &self.osd);
        dot::render(&graph, &mut out).unwrap();
        String::from_utf8_lossy(&out[..]).to_string()
    }

    pub(crate) fn length(
        &self,
        obj: &ObjId,
        encoding: ListEncoding,
        clock: Option<Clock>,
    ) -> usize {
        if let Some(tree) = self.trees.get(obj) {
            match (&clock, tree.index(encoding)) {
                // no clock and a clean index? - use it
                (None, Some(index)) => index.visible_len(encoding),
                // do it the hard way - walk each op
                _ => self
                    .top_ops(obj, clock)
                    .fold(0, |acc, top| acc + top.op.width(encoding)),
            }
        } else {
            0
        }
    }

    pub(crate) fn text(&self, obj: &ObjId, clock: Option<Clock>) -> String {
        self.top_ops(obj, clock)
            .map(|top| top.op.as_str())
            .collect()
    }

    pub(crate) fn keys<'a>(&'a self, obj: &ObjId, clock: Option<Clock>) -> Keys<'a> {
        Keys {
            iter: Some((self.top_ops(obj, clock), self)),
        }
    }

    pub(crate) fn list_range<R: RangeBounds<usize>>(
        &self,
        obj: &ObjId,
        range: R,
        encoding: ListEncoding,
        clock: Option<Clock>,
    ) -> ListRange<'_, R> {
        ListRange::new(self.top_ops(obj, clock.clone()), encoding, range, clock)
    }
    pub(crate) fn map_range<R: RangeBounds<String>>(
        &self,
        obj: &ObjId,
        range: R,
        clock: Option<Clock>,
    ) -> MapRange<'_, R> {
        MapRange::new(self.top_ops(obj, clock.clone()), self, range, clock)
    }

    pub(crate) fn to_string<E: Exportable>(&self, id: E) -> String {
        match id.export() {
            Export::Id(id) => format!("{}@{}", id.counter(), &self.osd.actors[id.actor()]),
            Export::Prop(index) => self.osd.props[index].clone(),
            Export::Special(s) => s,
        }
    }
}

impl Default for OpSetInternal {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> IntoIterator for &'a OpSetInternal {
    type Item = (&'a ObjId, ObjType, Op<'a>);

    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub(crate) struct IterObjs<'a> {
    trees: std::vec::IntoIter<(ObjMeta, &'a op_tree::OpTree)>,
}

impl<'a> Iterator for IterObjs<'a> {
    type Item = (ObjMeta, OpTreeIter<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        self.trees.next().map(|(id, tree)| (id, tree.iter()))
    }
}

#[derive(Clone)]
pub(crate) struct Iter<'a> {
    opset: &'a OpSet,
    trees: std::vec::IntoIter<(&'a ObjId, ObjType, &'a op_tree::OpTree)>,
    current: Option<(&'a ObjId, ObjType, OpTreeIter<'a>)>,
    osd: &'a OpSetData,
}

impl<'a> Iterator for Iter<'a> {
    type Item = (&'a ObjId, ObjType, Op<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((id, typ, tree)) = &mut self.current {
            if let Some(idx) = tree.next() {
                let next = idx.as_op(self.osd);
                return Some((id, *typ, next));
            }
        }

        loop {
            self.current = self.trees.next().map(|o| (o.0, o.1, o.2.iter()));
            if let Some((obj, typ, tree)) = &mut self.current {
                if let Some(idx) = tree.next() {
                    let next = idx.as_op(self.osd);
                    return Some((obj, *typ, next));
                }
            } else {
                return None;
            }
        }
    }
}

impl<'a> ExactSizeIterator for Iter<'a> {
    fn len(&self) -> usize {
        self.opset.len()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct OpSetData {
    pub(crate) actors: IndexedCache<ActorId>,
    pub(crate) props: IndexedCache<String>,
    ops: Vec<OpRaw>,
    op_deps: Vec<OpDepRaw>,
}

impl Default for OpSetData {
    fn default() -> Self {
        Self {
            actors: IndexedCache::new(),
            props: IndexedCache::new(),
            ops: Vec::new(),
            op_deps: Vec::new(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct OpIter<'a> {
    iter: OpTreeIter<'a>,
    pub(crate) osd: &'a OpSetData,
}

impl<'a> Iterator for OpIter<'a> {
    type Item = Op<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|idx| idx.as_op(self.osd))
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.iter.nth(n).map(|idx| idx.as_op(self.osd))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ChangeOpIter<'a> {
    osd: &'a OpSetData,
    range: OpIdxRange,
    current: u32,
    current_back: u32,
}

impl<'a> ChangeOpIter<'a> {
    fn new(osd: &'a OpSetData, range: OpIdxRange) -> Self {
        Self {
            osd,
            current: range.start,
            current_back: range.end,
            range,
        }
    }
}

impl<'a> ExactSizeIterator for ChangeOpIter<'a> {
    fn len(&self) -> usize {
        (self.range.end - self.range.start) as usize
    }
}

impl<'a> Iterator for ChangeOpIter<'a> {
    type Item = Op<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        assert!(self.current >= self.range.start);
        if self.current < self.current_back {
            let idx = OpIdx::new(self.current as usize);
            self.current += 1;
            Some(idx.as_op(self.osd))
        } else {
            None
        }
    }
}

impl<'a> DoubleEndedIterator for ChangeOpIter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        assert!(self.current_back <= self.range.end);
        if self.current_back > self.current {
            self.current_back -= 1;
            let idx = OpIdx::new(self.current_back as usize);
            Some(idx.as_op(self.osd))
        } else {
            None
        }
    }
}

impl OpSetData {
    pub(crate) fn start_range(&self) -> OpIdxRange {
        let len = self.ops.len() as u32;
        OpIdxRange {
            start: len,
            end: len,
        }
    }

    pub(crate) fn get_ops(&self, range: OpIdxRange) -> ChangeOpIter<'_> {
        ChangeOpIter::new(self, range)
    }

    pub(crate) fn add_inc(&mut self, old_op: OpIdx, new_op: OpIdx) {
        if let Some(n) = new_op.as_op(self).get_increment_value() {
            self.ops[old_op.get()].op.increment(n);
        }
    }

    pub(crate) fn remove_inc(&mut self, old_op: OpIdx, new_op: OpIdx) {
        if let Some(n) = new_op.as_op(self).get_increment_value() {
            self.ops[old_op.get()].op.increment(-n);
        }
    }

    pub(crate) fn add_pred(&mut self, pred: OpIdx, succ: OpIdx) {
        let succ_op = &mut self.ops[succ.get()].op;
        let inc = succ_op.get_increment_value();

        if let Some(n) = inc {
            self.ops[pred.get()].op.increment(n);
        }

        self.add_dep(pred, succ);
    }

    pub(crate) fn remove_dep(&mut self, pred: OpIdx, succ: OpIdx) {
        self.remove_succ(pred, succ);
        self.remove_pred(pred, succ);
    }

    fn remove_succ(&mut self, pred: OpIdx, succ: OpIdx) {
        let mut current_succ = self.ops[pred.get()].succ;
        let mut last_succ: Option<OpDepIdx> = None;
        while let Some(current) = current_succ {
            let next_succ = self.op_deps[current.get()].next_succ;
            if self.op_deps[current.get()].succ == succ {
                if let Some(last) = last_succ {
                    self.op_deps[last.get()].next_succ = next_succ;
                } else {
                    self.ops[pred.get()].succ = next_succ;
                }
                self.ops[pred.get()].succ_len -= 1;
                break;
            }
            last_succ = current_succ;
            current_succ = next_succ;
        }
    }
    fn remove_pred(&mut self, pred: OpIdx, succ: OpIdx) {
        let mut current_pred = self.ops[succ.get()].pred;
        let mut last_pred: Option<OpDepIdx> = None;
        while let Some(current) = current_pred {
            let next_pred = self.op_deps[current.get()].next_pred;
            if self.op_deps[current.get()].pred == pred {
                if let Some(last) = last_pred {
                    self.op_deps[last.get()].next_pred = next_pred;
                } else {
                    self.ops[succ.get()].pred = next_pred;
                }
                self.ops[succ.get()].pred_len -= 1;
                break;
            }
            last_pred = current_pred;
            current_pred = next_pred;
        }
    }

    pub(crate) fn add_dep(&mut self, pred: OpIdx, succ: OpIdx) {
        let op_dep_idx = OpDepIdx::new(self.op_deps.len());
        let mut op_dep = OpDepRaw::new(pred, succ);

        self.ops[pred.get()].succ_len += 1;
        self.ops[succ.get()].pred_len += 1;

        let mut last_succ = None;
        let mut next_succ = self.ops[pred.get()].succ;
        while let Some(next) = next_succ {
            let current = &self.op_deps[next.get()];
            if current.succ.as_op(self) > succ.as_op(self) {
                break;
            }
            last_succ = next_succ;
            next_succ = current.next_succ;
        }

        let mut last_pred = None;
        let mut next_pred = self.ops[succ.get()].pred;
        while let Some(next) = next_pred {
            let current = &self.op_deps[next.get()];
            if current.pred.as_op(self) > pred.as_op(self) {
                break;
            }
            last_pred = next_pred;
            next_pred = current.next_pred;
        }

        op_dep.next_succ = next_succ;
        op_dep.next_pred = next_pred;
        op_dep.last_succ = last_succ;
        op_dep.last_pred = last_pred;

        if let Some(last) = last_succ {
            self.op_deps[last.get()].next_succ = Some(op_dep_idx);
        } else {
            self.ops[pred.get()].succ = Some(op_dep_idx);
        }

        if let Some(last) = last_pred {
            self.op_deps[last.get()].next_pred = Some(op_dep_idx);
        } else {
            self.ops[succ.get()].pred = Some(op_dep_idx);
        }

        if let Some(next) = next_succ {
            self.op_deps[next.get()].last_succ = Some(op_dep_idx);
        }

        if let Some(next) = next_pred {
            self.op_deps[next.get()].last_pred = Some(op_dep_idx);
        }

        //log!("opdep idx={} op_dep={:?}", op_dep_idx.get(), op_dep);
        //log!("  idx={} pred={:?}", pred.get(), &self.ops[pred.get()]);
        //log!("  idx={} succ={:?}", succ.get(), &self.ops[succ.get()]);

        self.op_deps.push(op_dep);
    }

    pub(crate) fn push(&mut self, obj: ObjId, op: OpBuilder) -> OpIdx {
        let index = self.ops.len();
        //log!("push idx={:?} op={:?}", index, op);
        let width = TextValue::width(op.to_str()) as u32; // TODO faster
        self.ops.push(OpRaw {
            obj,
            width,
            op,
            pred_len: 0,
            succ_len: 0,
            pred: None,
            succ: None,
        });
        OpIdx::new(index)
    }

    pub(crate) fn from_actors(actors: Vec<ActorId>) -> Self {
        Self {
            props: IndexedCache::new(),
            actors: actors.into_iter().collect(),
            ops: Vec::new(),
            op_deps: Vec::new(),
        }
    }

    pub(crate) fn key_cmp(&self, left: &Key, right: &Key) -> Ordering {
        match (left, right) {
            (Key::Map(a), Key::Map(b)) => self.props[*a].cmp(&self.props[*b]),
            _ => panic!("can only compare map keys"),
        }
    }

    pub(crate) fn lamport_cmp<O: AsRef<OpId>>(&self, left: O, right: O) -> Ordering {
        left.as_ref()
            .lamport_cmp(right.as_ref(), &self.actors.cache)
    }

    pub(crate) fn sorted_opids<I: Iterator<Item = OpId>>(&self, opids: I) -> OpIds {
        OpIds::new(opids, |left, right| self.lamport_cmp(*left, *right))
    }

    /// If `opids` are in ascending lamport timestamp order with respect to the actor IDs in
    /// this `OpSetData` then this returns `Some(OpIds)`, otherwise returns `None`.
    pub(crate) fn try_sorted_opids(&self, opids: Vec<OpId>) -> Option<OpIds> {
        OpIds::new_if_sorted(opids, |a, b| self.lamport_cmp(*a, *b))
    }

    pub(crate) fn import_prop<S: Borrow<str>>(&mut self, key: S) -> usize {
        self.props.cache(key.borrow().to_string())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Parent {
    pub(crate) obj: ObjId,
    pub(crate) typ: ObjType,
    pub(crate) prop: Prop,
    pub(crate) visible: bool,
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::{
        op_set::OpSet,
        op_tree::B,
        types::{Key, ListEncoding, ObjId, ObjMeta, OpBuilder, OpId, OpIds, ROOT},
        ActorId, ScalarValue,
    };

    /// Create an optree in which the only visible ops are on the boundaries of the nodes,
    /// i.e. the visible elements are in the internal nodes. Like so
    ///
    /// ```notrust
    ///
    ///                      .----------------------.
    ///                      | id   |  key  |  succ |
    ///                      | B    |  "a"  |       |
    ///                      | 2B   |  "b"  |       |
    ///                      '----------------------'
    ///                           /      |      \
    ///  ;------------------------.      |       `------------------------------------.
    ///  | id     | op     | succ |      |       | id            | op     | succ      |
    ///  | 0      |set "a" |  1   |      |       | 2B + 1        |set "c" |  2B + 2   |
    ///  | 1      |set "a" |  2   |      |       | 2B + 2        |set "c" |  2B + 3   |
    ///  | 2      |set "a" |  3   |      |       ...
    ///  ...                             |       | 3B            |set "c" |           |
    ///  | B - 1  |set "a" |  B   |      |       '------------------------------------'
    ///  '--------'--------'------'      |
    ///                                  |
    ///                      .-----------------------------.
    ///                      | id         |  key  |  succ  |
    ///                      | B + 1      |  "b"  | B + 2  |
    ///                      | B + 2      |  "b"  | B + 3  |
    ///                      ....
    ///                      | B + (B - 1 |  "b"  |   2B   |
    ///                      '-----------------------------'
    /// ```
    ///
    /// The important point here is that the leaf nodes contain no visible ops for keys "a" and
    /// "b".
    ///
    /// # Returns
    ///
    /// The opset in question and an op which should be inserted at the next position after the
    /// internally visible ops.
    pub(crate) fn optree_with_only_internally_visible_ops() -> (OpSet, OpBuilder, OpIds) {
        let mut set = OpSet::new();
        let actor = set.osd.actors.cache(ActorId::random());
        let a = set.osd.props.cache("a".to_string());
        let b = set.osd.props.cache("b".to_string());
        let c = set.osd.props.cache("c".to_string());

        let mut counter = 0;
        let mut last_idx = None;
        // For each key insert `B` operations with the `pred` and `succ` setup such that the final
        // operation for each key is the only visible op.
        for key in [a, b, c] {
            for iteration in 0..B {
                // Generate a value to insert
                let keystr = set.osd.props.get(key);
                let val = keystr.repeat(iteration + 1);

                let op = OpBuilder {
                    id: OpId::new(counter, actor),
                    action: crate::OpType::Put(ScalarValue::Str(val.into())),
                    key: Key::Map(key),
                    insert: false,
                };
                let idx = set.load(ROOT.into(), op);
                if let Some(pred) = last_idx {
                    set.osd.add_dep(pred, idx);
                }
                set.insert(counter as usize, &ObjId::root(), idx);
                counter += 1;
                last_idx = Some(idx);
            }
            last_idx = None;
        }

        let pred = set
            .osd
            .sorted_opids(std::iter::once(OpId::new(B as u64 - 1, actor)));

        // Now try and create an op which inserts at the next index of 'a'
        let new_op = OpBuilder {
            id: OpId::new(counter, actor),
            action: crate::OpType::Put(ScalarValue::Str("test".into())),
            key: Key::Map(a),
            insert: false,
        };
        (set, new_op, pred)
    }

    #[test]
    fn seek_on_page_boundary() {
        let (mut set, new_op, pred) = optree_with_only_internally_visible_ops();

        let new_op = set.load(ROOT.into(), new_op).as_op(&set.osd);

        let q1 = set.find_op_without_patch_log(&ObjId::root(), new_op, &pred);
        let q2 = set.find_op_with_patch_log(&ObjMeta::root(), ListEncoding::List, new_op, &pred);

        // we've inserted `B - 1` elements for "a", so the index should be `B`
        assert_eq!(q1.pos, B);
        assert_eq!(q2.pos, B);
    }
}
