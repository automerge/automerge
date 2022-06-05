use crate::clock::Clock;
use crate::exid::ExId;
use crate::indexed_cache::IndexedCache;
use crate::op_tree::{self, OpTree};
use crate::query::{self, OpIdSearch, TreeQuery};
use crate::types::{self, ActorId, Key, ObjId, Op, OpId, OpType, Prop};
use crate::AutomergeError;
use crate::Parents;
use crate::{ObjType, OpObserver};
use fxhash::FxBuildHasher;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::ops::RangeBounds;

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
            trees: objs.into_iter(),
            current: None,
        }
    }

    pub(crate) fn parent(&self, obj: &ObjId) -> Option<(ObjId, Key)> {
        let parent = self.trees.get(obj)?.parent?;
        let key = self.search(&parent, OpIdSearch::new(obj.0)).key().unwrap();
        Some((parent, key))
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

    pub(crate) fn replace<F>(&mut self, obj: &ObjId, index: usize, f: F)
    where
        F: FnMut(&mut Op),
    {
        if let Some(tree) = self.trees.get_mut(obj) {
            tree.internal.update(index, f)
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
        }
    }

    pub(crate) fn insert_op(&mut self, obj: &ObjId, op: Op) -> Op {
        let q = self.search(obj, query::SeekOp::new(&op));

        let succ = q.succ;
        let pos = q.pos;

        for i in succ {
            self.replace(obj, i, |old_op| old_op.add_succ(&op));
        }

        if !op.is_delete() {
            self.insert(pos, obj, op.clone());
        }
        op
    }

    pub(crate) fn insert_op_with_observer<Obs: OpObserver>(
        &mut self,
        obj: &ObjId,
        op: Op,
        observer: &mut Obs,
    ) -> Op {
        let q = self.search(obj, query::SeekOpWithPatch::new(&op));

        let query::SeekOpWithPatch {
            pos,
            succ,
            seen,
            values,
            had_value_before,
            ..
        } = q;

        let ex_obj = self.id_to_exid(obj.0);
        let key = match op.key {
            Key::Map(index) => self.m.props[index].clone().into(),
            Key::Seq(_) => seen.into(),
        };

        if op.insert {
            let value = (op.value(), self.id_to_exid(op.id));
            let parents = self.parents(&ex_obj);
            observer.insert(ex_obj, parents, seen, value);
        } else if op.is_delete() {
            if let Some(winner) = &values.last() {
                let value = (winner.value(), self.id_to_exid(winner.id));
                let conflict = values.len() > 1;
                let parents = self.parents(&ex_obj);
                observer.put(ex_obj, parents, key, value, conflict);
            } else {
                let parents = self.parents(&ex_obj);
                observer.delete(ex_obj, parents, key);
            }
        } else if let Some(value) = op.get_increment_value() {
            // only observe this increment if the counter is visible, i.e. the counter's
            // create op is in the values
            if values.iter().any(|value| op.pred.contains(&value.id)) {
                // we have observed the value
                let parents = self.parents(&ex_obj);
                observer.increment(ex_obj, parents, key, (value, self.id_to_exid(op.id)));
            }
        } else {
            let winner = if let Some(last_value) = values.last() {
                if self.m.lamport_cmp(op.id, last_value.id) == Ordering::Greater {
                    &op
                } else {
                    last_value
                }
            } else {
                &op
            };
            let value = (winner.value(), self.id_to_exid(winner.id));
            if op.is_list_op() && !had_value_before {
                let parents = self.parents(&ex_obj);
                observer.insert(ex_obj, parents, seen, value);
            } else {
                let conflict = !values.is_empty();
                let parents = self.parents(&ex_obj);
                observer.put(ex_obj, parents, key, value, conflict);
            }
        }

        for i in succ {
            self.replace(obj, i, |old_op| old_op.add_succ(&op));
        }

        if !op.is_delete() {
            self.insert(pos, obj, op.clone());
        }

        op
    }

    pub(crate) fn object_type(&self, id: &ObjId) -> Option<ObjType> {
        self.trees.get(id).map(|tree| tree.objtype)
    }

    #[cfg(feature = "optree-visualisation")]
    pub(crate) fn visualise(&self) -> String {
        let mut out = Vec::new();
        let graph = super::visualisation::GraphVisualisation::construct(&self.trees, &self.m);
        dot::render(&graph, &mut out).unwrap();
        String::from_utf8_lossy(&out[..]).to_string()
    }

    pub(crate) fn parent_prop(&self, obj: &ObjId) -> Option<(ObjId, Prop)> {
        self.parent(obj)
            .map(|(id, key)| (id, self.export_key(&id, key)))
    }

    pub(crate) fn parents(&self, obj: &ExId) -> Parents<'_> {
        Parents {
            obj: obj.clone(),
            doc: self,
        }
    }

    pub(crate) fn parent_object<O: AsRef<ExId>>(&self, obj: O) -> Option<(ExId, Prop)> {
        if let Ok(obj) = self.exid_to_obj(obj.as_ref()) {
            if obj == ObjId::root() {
                // root has no parent
                None
            } else {
                self.parent_prop(&obj)
                    .map(|(id, prop)| (self.id_to_exid(id.0), prop))
            }
        } else {
            None
        }
    }

    pub(crate) fn exid_to_obj(&self, id: &ExId) -> Result<ObjId, AutomergeError> {
        match id {
            ExId::Root => Ok(ObjId::root()),
            ExId::Id(ctr, actor, idx) => {
                // do a direct get here b/c this could be foriegn and not be within the array
                // bounds
                if self.m.actors.cache.get(*idx) == Some(actor) {
                    Ok(ObjId(OpId(*ctr, *idx)))
                } else {
                    // FIXME - make a real error
                    let idx = self.m.actors.lookup(actor).ok_or(AutomergeError::Fail)?;
                    Ok(ObjId(OpId(*ctr, idx)))
                }
            }
        }
    }

    pub(crate) fn export_key(&self, obj: &ObjId, key: Key) -> Prop {
        match key {
            Key::Map(m) => Prop::Map(self.m.props.get(m).into()),
            Key::Seq(opid) => {
                let i = self
                    .search(obj, query::ElemIdPos::new(opid))
                    .index()
                    .unwrap();
                Prop::Seq(i)
            }
        }
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

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct OpSetMetadata {
    pub(crate) actors: IndexedCache<ActorId>,
    pub(crate) props: IndexedCache<String>,
}

impl OpSetMetadata {
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
}
