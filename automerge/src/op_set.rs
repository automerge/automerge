use crate::clock::Clock;
use crate::exid::ExId;
use crate::indexed_cache::IndexedCache;
use crate::op_tree::OpTree;
use crate::query::{self, OpIdSearch, TreeQuery};
use crate::types::{self, ActorId, Key, ObjId, Op, OpId, OpType};
use crate::Prop;
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
    pub m: OpSetMetadata,
}

impl OpSetInternal {
    pub fn new() -> Self {
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

    pub fn iter(&self) -> Iter<'_> {
        let mut objs: Vec<_> = self.trees.keys().collect();
        objs.sort_by(|a, b| self.m.lamport_cmp(a.0, b.0));
        Iter {
            inner: self,
            index: 0,
            sub_index: 0,
            objs,
        }
    }

    pub fn parent_object(&self, obj: &ObjId) -> Option<(ObjId, Key)> {
        let parent = self.trees.get(obj)?.parent?;
        let key = self.search(&parent, OpIdSearch::new(obj.0)).key().unwrap();
        Some((parent, key))
    }

    pub fn keys(&self, obj: ObjId) -> Option<query::Keys> {
        if let Some(tree) = self.trees.get(&obj) {
            tree.internal.keys()
        } else {
            None
        }
    }

    pub fn keys_at(&self, obj: ObjId, clock: Clock) -> Option<query::KeysAt> {
        if let Some(tree) = self.trees.get(&obj) {
            tree.internal.keys_at(clock)
        } else {
            None
        }
    }

    pub fn range<R: RangeBounds<String>>(&self, obj: ObjId, range: R) -> Option<query::Range<R>> {
        if let Some(tree) = self.trees.get(&obj) {
            tree.internal.range(range, &self.m)
        } else {
            None
        }
    }

    pub fn range_at<R: RangeBounds<String>>(
        &self,
        obj: ObjId,
        range: R,
        clock: Clock,
    ) -> Option<query::RangeAt<R>> {
        if let Some(tree) = self.trees.get(&obj) {
            tree.internal.range_at(range, &self.m, clock)
        } else {
            None
        }
    }

    pub fn search<'a, 'b: 'a, Q>(&'b self, obj: &ObjId, query: Q) -> Q
    where
        Q: TreeQuery<'a>,
    {
        if let Some(tree) = self.trees.get(obj) {
            tree.internal.search(query, &self.m)
        } else {
            query
        }
    }

    pub fn replace<F>(&mut self, obj: &ObjId, index: usize, f: F)
    where
        F: FnMut(&mut Op),
    {
        if let Some(tree) = self.trees.get_mut(obj) {
            tree.internal.update(index, f)
        }
    }

    pub fn remove(&mut self, obj: &ObjId, index: usize) -> Op {
        // this happens on rollback - be sure to go back to the old state
        let tree = self.trees.get_mut(obj).unwrap();
        self.length -= 1;
        let op = tree.internal.remove(index);
        if let OpType::Make(_) = &op.action {
            self.trees.remove(&op.id.into());
        }
        op
    }

    pub fn len(&self) -> usize {
        self.length
    }

    pub fn insert(&mut self, index: usize, obj: &ObjId, element: Op) {
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
            observer.insert(ex_obj, seen, value);
        } else if op.is_delete() {
            if let Some(winner) = &values.last() {
                let value = (winner.value(), self.id_to_exid(winner.id));
                let conflict = values.len() > 1;
                observer.put(ex_obj, key, value, conflict);
            } else {
                observer.delete(ex_obj, key);
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
                observer.insert(ex_obj, seen, value);
            } else {
                let conflict = !values.is_empty();
                observer.put(ex_obj, key, value, conflict);
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

    pub fn object_type(&self, id: &ObjId) -> Option<ObjType> {
        self.trees.get(id).map(|tree| tree.objtype)
    }

    #[cfg(feature = "optree-visualisation")]
    pub fn visualise(&self) -> String {
        let mut out = Vec::new();
        let graph = super::visualisation::GraphVisualisation::construct(&self.trees, &self.m);
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

pub(crate) struct Iter<'a> {
    inner: &'a OpSetInternal,
    index: usize,
    objs: Vec<&'a ObjId>,
    sub_index: usize,
}

impl<'a> Iterator for Iter<'a> {
    type Item = (&'a ObjId, &'a Op);

    fn next(&mut self) -> Option<Self::Item> {
        let mut result = None;
        for obj in self.objs.iter().skip(self.index) {
            let tree = self.inner.trees.get(obj)?;
            result = tree.internal.get(self.sub_index).map(|op| (*obj, op));
            if result.is_some() {
                self.sub_index += 1;
                break;
            } else {
                self.index += 1;
                self.sub_index = 0;
            }
        }
        result
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct OpSetMetadata {
    pub actors: IndexedCache<ActorId>,
    pub props: IndexedCache<String>,
}

impl OpSetMetadata {
    pub fn key_cmp(&self, left: &Key, right: &Key) -> Ordering {
        match (left, right) {
            (Key::Map(a), Key::Map(b)) => self.props[*a].cmp(&self.props[*b]),
            _ => panic!("can only compare map keys"),
        }
    }

    pub fn lamport_cmp(&self, left: OpId, right: OpId) -> Ordering {
        match (left, right) {
            (OpId(0, _), OpId(0, _)) => Ordering::Equal,
            (OpId(0, _), OpId(_, _)) => Ordering::Less,
            (OpId(_, _), OpId(0, _)) => Ordering::Greater,
            (OpId(a, x), OpId(b, y)) if a == b => self.actors[x].cmp(&self.actors[y]),
            (OpId(a, _), OpId(b, _)) => a.cmp(&b),
        }
    }
}
