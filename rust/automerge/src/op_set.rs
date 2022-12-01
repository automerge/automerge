use crate::clock::Clock;
use crate::exid::ExId;
use crate::indexed_cache::IndexedCache;
use crate::op_tree::{self, OpTree};
use crate::parents::Parents;
use crate::query::{self, OpIdVisSearch, TreeQuery};
use crate::types::{self, ActorId, Key, ObjId, Op, OpId, OpIds, OpType, Prop};
use crate::{ObjType, OpObserver};
use fxhash::FxBuildHasher;
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::ops::RangeBounds;

mod load;
pub(crate) use load::{ObservedOpSetBuilder, OpSetBuilder};

pub(crate) type OpSet = OpSetInternal;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OpSetInternal {
    /// The map of objects to their type and ops.
    trees: HashMap<ObjId, OpTree, FxBuildHasher>,
    /// The number of operations in the opset.
    length: usize,
    /// Metadata about the operations in this opset.
    pub(crate) m: OpSetMetadata,
}

impl OpSetInternal {
    pub(crate) fn builder() -> OpSetBuilder {
        OpSetBuilder::new()
    }

    /// Create a builder which passes each operation to `observer`. This will be significantly
    /// slower than `OpSetBuilder`
    pub(crate) fn observed_builder<O: OpObserver>(observer: &mut O) -> ObservedOpSetBuilder<'_, O> {
        ObservedOpSetBuilder::new(observer)
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
        }
    }

    pub(crate) fn id_to_exid(&self, id: OpId) -> ExId {
        if id == types::ROOT {
            ExId::Root
        } else {
            ExId::Id(id.0, self.m.actors.cache[id.1].clone(), id.1)
        }
    }

    pub(crate) fn iter(&self) -> Iter<'_> {
        let mut objs: Vec<_> = self.trees.iter().collect();
        objs.sort_by(|a, b| self.m.lamport_cmp((a.0).0, (b.0).0));
        Iter {
            opset: self,
            trees: objs.into_iter(),
            current: None,
        }
    }

    pub(crate) fn parents(&self, obj: ObjId) -> Parents<'_> {
        Parents { obj, ops: self }
    }

    pub(crate) fn parent_object(&self, obj: &ObjId) -> Option<(ObjId, Key, bool)> {
        let parent = self.trees.get(obj)?.parent?;
        let query = self.search(&parent, OpIdVisSearch::new(obj.0));
        let key = query.key().unwrap();
        let visible = query.visible;
        Some((parent, key, visible))
    }

    pub(crate) fn export_key(&self, obj: ObjId, key: Key) -> Prop {
        match key {
            Key::Map(m) => Prop::Map(self.m.props.get(m).into()),
            Key::Seq(opid) => {
                let i = self
                    .search(&obj, query::ElemIdPos::new(opid))
                    .index()
                    .unwrap();
                Prop::Seq(i)
            }
        }
    }

    pub(crate) fn keys(&self, obj: ObjId) -> Option<query::Keys<'_>> {
        if let Some(tree) = self.trees.get(&obj) {
            tree.internal.keys()
        } else {
            None
        }
    }

    pub(crate) fn keys_at(&self, obj: ObjId, clock: Clock) -> Option<query::KeysAt<'_>> {
        if let Some(tree) = self.trees.get(&obj) {
            tree.internal.keys_at(clock)
        } else {
            None
        }
    }

    pub(crate) fn map_range<R: RangeBounds<String>>(
        &self,
        obj: ObjId,
        range: R,
    ) -> Option<query::MapRange<'_, R>> {
        if let Some(tree) = self.trees.get(&obj) {
            tree.internal.map_range(range, &self.m)
        } else {
            None
        }
    }

    pub(crate) fn map_range_at<R: RangeBounds<String>>(
        &self,
        obj: ObjId,
        range: R,
        clock: Clock,
    ) -> Option<query::MapRangeAt<'_, R>> {
        if let Some(tree) = self.trees.get(&obj) {
            tree.internal.map_range_at(range, &self.m, clock)
        } else {
            None
        }
    }

    pub(crate) fn list_range<R: RangeBounds<usize>>(
        &self,
        obj: ObjId,
        range: R,
    ) -> Option<query::ListRange<'_, R>> {
        if let Some(tree) = self.trees.get(&obj) {
            tree.internal.list_range(range)
        } else {
            None
        }
    }

    pub(crate) fn list_range_at<R: RangeBounds<usize>>(
        &self,
        obj: ObjId,
        range: R,
        clock: Clock,
    ) -> Option<query::ListRangeAt<'_, R>> {
        if let Some(tree) = self.trees.get(&obj) {
            tree.internal.list_range_at(range, clock)
        } else {
            None
        }
    }

    pub(crate) fn search<'a, 'b: 'a, Q>(&'b self, obj: &ObjId, query: Q) -> Q
    where
        Q: TreeQuery<'a>,
    {
        if let Some(tree) = self.trees.get(obj) {
            tree.internal.search(query, &self.m)
        } else {
            query
        }
    }

    pub(crate) fn change_vis<F>(&mut self, obj: &ObjId, index: usize, f: F)
    where
        F: Fn(&mut Op),
    {
        if let Some(tree) = self.trees.get_mut(obj) {
            tree.internal.update(index, f)
        }
    }

    /// Add `op` as a successor to each op at `op_indices` in `obj`
    pub(crate) fn add_succ(&mut self, obj: &ObjId, op_indices: &[usize], op: &Op) {
        if let Some(tree) = self.trees.get_mut(obj) {
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
        let op = tree.internal.remove(index);
        if let OpType::Make(_) = &op.action {
            self.trees.remove(&op.id.into());
        }
        op
    }

    pub(crate) fn len(&self) -> usize {
        self.length
    }

    #[tracing::instrument(skip(self, index))]
    pub(crate) fn insert(&mut self, index: usize, obj: &ObjId, element: Op) {
        if let OpType::Make(typ) = element.action {
            self.trees.insert(
                element.id.into(),
                OpTree {
                    internal: Default::default(),
                    objtype: typ,
                    parent: Some(*obj),
                },
            );
        }

        if let Some(tree) = self.trees.get_mut(obj) {
            //let tree = self.trees.get_mut(&element.obj).unwrap();
            tree.internal.insert(index, element);
            self.length += 1;
        } else {
            tracing::warn!("attempting to insert op for unknown object");
        }
    }

    pub(crate) fn object_type(&self, id: &ObjId) -> Option<ObjType> {
        self.trees.get(id).map(|tree| tree.objtype)
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
}

impl Default for OpSetInternal {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> IntoIterator for &'a OpSetInternal {
    type Item = (&'a ObjId, &'a Op);

    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[derive(Clone)]
pub(crate) struct Iter<'a> {
    opset: &'a OpSet,
    trees: std::vec::IntoIter<(&'a ObjId, &'a op_tree::OpTree)>,
    current: Option<(&'a ObjId, op_tree::OpTreeIter<'a>)>,
}
impl<'a> Iterator for Iter<'a> {
    type Item = (&'a ObjId, &'a Op);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((id, tree)) = &mut self.current {
            if let Some(next) = tree.next() {
                return Some((id, next));
            }
        }

        loop {
            self.current = self.trees.next().map(|o| (o.0, o.1.iter()));
            if let Some((obj, tree)) = &mut self.current {
                if let Some(next) = tree.next() {
                    return Some((obj, next));
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
        match (left, right) {
            (OpId(0, _), OpId(0, _)) => Ordering::Equal,
            (OpId(0, _), OpId(_, _)) => Ordering::Less,
            (OpId(_, _), OpId(0, _)) => Ordering::Greater,
            (OpId(a, x), OpId(b, y)) if a == b => self.actors[x].cmp(&self.actors[y]),
            (OpId(a, _), OpId(b, _)) => a.cmp(&b),
        }
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
