use crate::clock::Clock;
use crate::exid::ExId;
use crate::indexed_cache::IndexedCache;
use crate::iter::TopOp;
use crate::op_tree::{self, LastInsert, OpTree, OpsFound, SeekOpFound};
use crate::parents::Parents;
use crate::query::TreeQuery;
use crate::types::{
    self, ActorId, Export, Exportable, Key, ListEncoding, ObjId, Op, OpId, OpIds, OpType, Prop,
    TextEncoding,
};
use crate::{ObjType, Value};
use fxhash::FxBuildHasher;
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::ops::RangeBounds;

mod load;
pub(crate) use load::OpSetBuilder;

pub(crate) type OpSet = OpSetInternal;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OpSetInternal {
    /// The map of objects to their type and ops.
    trees: HashMap<ObjId, OpTree, FxBuildHasher>,
    /// The number of operations in the opset.
    length: usize,
    /// Metadata about the operations in this opset.
    pub(crate) m: OpSetMetadata,
    pub(crate) text_encoding: TextEncoding,
}

impl OpSetInternal {
    pub(crate) fn builder() -> OpSetBuilder {
        OpSetBuilder::new()
    }

    pub(crate) fn new() -> Self {
        let mut trees: HashMap<_, _, _> = Default::default();
        trees.insert(ObjId::root(), OpTree::new());
        OpSetInternal {
            trees,
            length: 0,
            m: OpSetMetadata {
                actors: IndexedCache::new(),
                props: IndexedCache::new(),
            },
            text_encoding: Default::default(),
        }
    }

    pub(crate) fn id_to_exid(&self, id: OpId) -> ExId {
        if id == types::ROOT {
            ExId::Root
        } else {
            ExId::Id(
                id.counter(),
                self.m.actors.cache[id.actor()].clone(),
                id.actor(),
            )
        }
    }

    pub(crate) fn iter(&self) -> Iter<'_> {
        let mut objs: Vec<_> = self.trees.iter().map(|t| (t.0, t.1.objtype, t.1)).collect();
        objs.sort_by(|a, b| self.m.lamport_cmp((a.0).0, (b.0).0));
        Iter {
            opset: self,
            trees: objs.into_iter(),
            current: None,
        }
    }

    /// Iterate over objects in the opset in causal order
    pub(crate) fn iter_objs(
        &self,
    ) -> impl Iterator<Item = (&ObjId, ObjType, op_tree::OpTreeIter<'_>)> + '_ {
        let mut objs: Vec<_> = self.trees.iter().map(|t| (t.0, t.1.objtype, t.1)).collect();
        objs.sort_by(|a, b| self.m.lamport_cmp((a.0).0, (b.0).0));
        IterObjs {
            trees: objs.into_iter(),
        }
    }

    pub(crate) fn iter_ops(&self, obj: &ObjId) -> impl Iterator<Item = &Op> {
        self.trees.get(obj).map(|o| o.iter()).into_iter().flatten()
    }

    pub(crate) fn parents(&self, obj: ObjId) -> Parents<'_> {
        Parents { obj, ops: self }
    }

    pub(crate) fn parent_object(&self, obj: &ObjId, clock: Option<&Clock>) -> Option<Parent> {
        let parent = self.trees.get(obj)?.parent?;
        let (_obj_type, encoding) = self.encoding(&parent)?;
        let (op, index, visible) = self
            .trees
            .get(&parent)
            .and_then(|tree| tree.internal.seek_opid(obj.0, encoding, clock, &self.m))
            .unwrap();
        let prop = match op.elemid_or_key() {
            Key::Map(m) => self.m.props.safe_get(m).map(|s| Prop::Map(s.to_string()))?,
            Key::Seq(_) => Prop::Seq(index),
        };
        Some(Parent {
            obj: parent,
            prop,
            visible,
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
                    .seek_ops_by_prop(&self.m, prop, encoding, clock)
            })
            .unwrap_or_default()
    }

    pub(crate) fn top_ops<'a>(
        &'a self,
        obj: &ObjId,
        clock: Option<Clock>,
    ) -> impl Iterator<Item = TopOp<'a>> {
        self.trees
            .get(obj)
            .map(|tree| tree.internal.top_ops(clock))
            .into_iter()
            .flatten()
    }

    pub(crate) fn seek_op_with_observer<'a>(
        &'a self,
        obj: &ObjId,
        op: &'a Op,
        encoding: ListEncoding,
    ) -> SeekOpFound<'a> {
        if let Some(tree) = self.trees.get(obj) {
            tree.internal.seek_op_with_observer(op, encoding, &self.m)
        } else {
            Default::default()
        }
    }

    pub(crate) fn seek_op_simple<'a>(&'a self, obj: &ObjId, op: &'a Op) -> SeekOpFound<'a> {
        if let Some(tree) = self.trees.get(obj) {
            tree.internal.seek_op_simple(op, &self.m)
        } else {
            Default::default()
        }
    }

    pub(crate) fn search<'a, 'b: 'a, Q>(&'b self, obj: &ObjId, mut query: Q) -> Q
    where
        Q: TreeQuery<'a>,
    {
        if let Some(tree) = self.trees.get(obj) {
            if query.can_shortcut_search(tree) {
                query
            } else {
                tree.internal.search(query, &self.m)
            }
        } else {
            query
        }
    }

    pub(crate) fn change_vis<F>(&mut self, obj: &ObjId, index: usize, f: F)
    where
        F: Fn(&mut Op),
    {
        if let Some(tree) = self.trees.get_mut(obj) {
            tree.last_insert = None;
            tree.internal.update(index, f)
        }
    }

    /// Add `op` as a successor to each op at `op_indices` in `obj`
    pub(crate) fn add_succ(&mut self, obj: &ObjId, op_indices: &[usize], op: &Op) {
        if let Some(tree) = self.trees.get_mut(obj) {
            tree.last_insert = None;
            for i in op_indices {
                tree.internal.update(*i, |old_op| {
                    old_op.add_succ(op, |left, right| self.m.lamport_cmp(*left, *right))
                });
            }
        }
    }

    pub(crate) fn remove(&mut self, obj: &ObjId, index: usize) -> Op {
        // this happens on rollback - be sure to go back to the old state
        let tree = self.trees.get_mut(obj).unwrap();
        self.length -= 1;
        tree.last_insert = None;
        let op = tree.internal.remove(index);
        if let OpType::Make(_) = &op.action {
            self.trees.remove(&op.id.into());
        }
        op
    }

    pub(crate) fn len(&self) -> usize {
        self.length
    }

    pub(crate) fn hint(&mut self, obj: &ObjId, index: usize, pos: usize, width: usize, key: Key) {
        if let Some(tree) = self.trees.get_mut(obj) {
            tree.last_insert = Some(LastInsert {
                index,
                pos,
                width,
                key,
            })
        }
    }

    #[tracing::instrument(skip(self, index))]
    pub(crate) fn insert(&mut self, index: usize, obj: &ObjId, element: Op) {
        if let OpType::Make(typ) = element.action {
            self.trees.insert(
                element.id.into(),
                OpTree {
                    internal: Default::default(),
                    objtype: typ,
                    last_insert: None,
                    parent: Some(*obj),
                },
            );
        }

        if let Some(tree) = self.trees.get_mut(obj) {
            tree.last_insert = None;
            tree.internal.insert(index, element);
            self.length += 1;
        } else {
            tracing::warn!("attempting to insert op for unknown object");
        }
    }

    pub(crate) fn object_type(&self, id: &ObjId) -> Option<ObjType> {
        self.trees.get(id).map(|tree| tree.objtype)
    }

    pub(crate) fn encoding(&self, id: &ObjId) -> Option<(ObjType, ListEncoding)> {
        let objtype = self.trees.get(id).map(|tree| tree.objtype)?;
        let encoding = ListEncoding::new(objtype, self.text_encoding);
        Some((objtype, encoding))
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
        let graph = super::visualisation::GraphVisualisation::construct(&trees, &self.m);
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
            .map(|top| top.op.to_str())
            .collect()
    }

    pub(crate) fn keys<'a>(
        &'a self,
        obj: &ObjId,
        clock: Option<Clock>,
    ) -> impl Iterator<Item = String> + 'a {
        self.top_ops(obj, clock)
            .map(|top| self.to_string(top.op.key))
    }

    pub(crate) fn list_range<'a, R: RangeBounds<usize> + 'a>(
        &'a self,
        obj: &ObjId,
        range: R,
        encoding: ListEncoding,
        clock: Option<Clock>,
    ) -> impl Iterator<Item = (usize, Value<'a>, ExId)> + 'a {
        self.top_ops(obj, clock.clone())
            .scan(0, move |state, top| {
                let index = *state;
                //let (value, id) = self.export_value(top.op, None);
                *state += top.op.width(encoding);
                Some((
                    index,
                    top.op.value_at(clock.as_ref()),
                    self.id_to_exid(top.op.id),
                ))
                //Some((index, value, id))
            })
            .filter(move |(index, _, _)| range.contains(index))
    }

    pub(crate) fn to_string<E: Exportable>(&self, id: E) -> String {
        match id.export() {
            Export::Id(id) => format!("{}@{}", id.counter(), &self.m.actors[id.actor()]),
            Export::Prop(index) => self.m.props[index].clone(),
            Export::Special(s) => s,
        }
    }

    //    pub(crate) fn export_value<'a>(&self, op: &'a Op, clock: Option<&Clock>) -> (Value<'a>, ExI
    //        (op.value_at(clock), self.id_to_exid(op.id))
    //    }
}

impl Default for OpSetInternal {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> IntoIterator for &'a OpSetInternal {
    type Item = (&'a ObjId, ObjType, &'a Op);

    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub(crate) struct IterObjs<'a> {
    trees: std::vec::IntoIter<(&'a ObjId, ObjType, &'a op_tree::OpTree)>,
}

impl<'a> Iterator for IterObjs<'a> {
    type Item = (&'a ObjId, ObjType, op_tree::OpTreeIter<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        self.trees
            .next()
            .map(|(id, typ, tree)| (id, typ, tree.iter()))
    }
}

#[derive(Clone)]
pub(crate) struct Iter<'a> {
    opset: &'a OpSet,
    trees: std::vec::IntoIter<(&'a ObjId, ObjType, &'a op_tree::OpTree)>,
    current: Option<(&'a ObjId, ObjType, op_tree::OpTreeIter<'a>)>,
}
impl<'a> Iterator for Iter<'a> {
    type Item = (&'a ObjId, ObjType, &'a Op);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((id, typ, tree)) = &mut self.current {
            if let Some(next) = tree.next() {
                return Some((id, *typ, next));
            }
        }

        loop {
            self.current = self.trees.next().map(|o| (o.0, o.1, o.2.iter()));
            if let Some((obj, typ, tree)) = &mut self.current {
                if let Some(next) = tree.next() {
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

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct OpSetMetadata {
    pub(crate) actors: IndexedCache<ActorId>,
    pub(crate) props: IndexedCache<String>,
}

impl Default for OpSetMetadata {
    fn default() -> Self {
        Self {
            actors: IndexedCache::new(),
            props: IndexedCache::new(),
        }
    }
}

impl OpSetMetadata {
    pub(crate) fn from_actors(actors: Vec<ActorId>) -> Self {
        Self {
            props: IndexedCache::new(),
            actors: actors.into_iter().collect(),
        }
    }

    pub(crate) fn key_cmp(&self, left: &Key, right: &Key) -> Ordering {
        match (left, right) {
            (Key::Map(a), Key::Map(b)) => self.props[*a].cmp(&self.props[*b]),
            _ => panic!("can only compare map keys"),
        }
    }

    pub(crate) fn lamport_cmp(&self, left: OpId, right: OpId) -> Ordering {
        left.lamport_cmp(&right, &self.actors.cache)
    }

    pub(crate) fn sorted_opids<I: Iterator<Item = OpId>>(&self, opids: I) -> OpIds {
        OpIds::new(opids, |left, right| self.lamport_cmp(*left, *right))
    }

    /// If `opids` are in ascending lamport timestamp order with respect to the actor IDs in
    /// this `OpSetMetadata` then this returns `Some(OpIds)`, otherwise returns `None`.
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
    pub(crate) prop: Prop,
    pub(crate) visible: bool,
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::{
        op_set::OpSet,
        op_tree::B,
        types::{Key, ListEncoding, ObjId, Op, OpId},
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
    pub(crate) fn optree_with_only_internally_visible_ops() -> (OpSet, Op) {
        let mut set = OpSet::new();
        let actor = set.m.actors.cache(ActorId::random());
        let a = set.m.props.cache("a".to_string());
        let b = set.m.props.cache("b".to_string());
        let c = set.m.props.cache("c".to_string());

        let mut counter = 0;
        // For each key insert `B` operations with the `pred` and `succ` setup such that the final
        // operation for each key is the only visible op.
        for key in [a, b, c] {
            for iteration in 0..B {
                // Generate a value to insert
                let keystr = set.m.props.get(key);
                let val = keystr.repeat(iteration + 1);

                // Only the last op is visible
                let pred = if iteration == 0 {
                    Default::default()
                } else {
                    set.m
                        .sorted_opids(vec![OpId::new(counter - 1, actor)].into_iter())
                };

                // only the last op is visible
                let succ = if iteration == B - 1 {
                    Default::default()
                } else {
                    set.m
                        .sorted_opids(vec![OpId::new(counter, actor)].into_iter())
                };

                let op = Op {
                    id: OpId::new(counter, actor),
                    action: crate::OpType::Put(ScalarValue::Str(val.into())),
                    key: Key::Map(key),
                    succ,
                    pred,
                    insert: false,
                };
                set.insert(counter as usize, &ObjId::root(), op);
                counter += 1;
            }
        }

        // Now try and create an op which inserts at the next index of 'a'
        let new_op = Op {
            id: OpId::new(counter, actor),
            action: crate::OpType::Put(ScalarValue::Str("test".into())),
            key: Key::Map(a),
            succ: Default::default(),
            pred: set
                .m
                .sorted_opids(std::iter::once(OpId::new(B as u64 - 1, actor))),
            insert: false,
        };
        (set, new_op)
    }

    #[test]
    fn seek_on_page_boundary() {
        let (set, new_op) = optree_with_only_internally_visible_ops();

        let q1 = set.seek_op_simple(&ObjId::root(), &new_op);
        let q2 = set.seek_op_with_observer(&ObjId::root(), &new_op, ListEncoding::List);

        // we've inserted `B - 1` elements for "a", so the index should be `B`
        assert_eq!(q1.pos, B);
        assert_eq!(q2.pos, B);
    }
}
