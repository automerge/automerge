use crate::clock::Clock;
use crate::exid::ExId;
use crate::indexed_cache::IndexedCache;
use crate::op_tree::{self, OpTree};
use crate::parents::Parents;
use crate::query::{self, OpIdVisSearch, TreeQuery};
use crate::types::{self, ActorId, Key, ListEncoding, ObjId, Op, OpId, OpIds, OpType, Prop};
use crate::ObjType;
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

    pub(crate) fn parents(&self, obj: ObjId) -> Parents<'_> {
        Parents { obj, ops: self }
    }

    pub(crate) fn parent_object(&self, obj: &ObjId) -> Option<Parent> {
        let parent = self.trees.get(obj)?.parent?;
        let query = self.search(&parent, OpIdVisSearch::new(obj.0));
        let key = query.key().unwrap();
        let visible = query.visible;
        Some(Parent {
            obj: parent,
            key,
            visible,
        })
    }

    pub(crate) fn export_key(&self, obj: ObjId, key: Key, encoding: ListEncoding) -> Option<Prop> {
        match key {
            Key::Map(m) => self.m.props.safe_get(m).map(|s| Prop::Map(s.to_string())),
            Key::Seq(opid) => {
                if opid.is_head() {
                    Some(Prop::Seq(0))
                } else {
                    self.search(&obj, query::ElemIdPos::new(opid, encoding))
                        .index()
                        .map(Prop::Seq)
                }
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

    pub(crate) fn hint(&mut self, obj: &ObjId, index: usize, pos: usize) {
        if let Some(tree) = self.trees.get_mut(obj) {
            tree.last_insert = Some((index, pos))
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

pub(crate) struct Parent {
    pub(crate) obj: ObjId,
    pub(crate) key: Key,
    pub(crate) visible: bool,
}
