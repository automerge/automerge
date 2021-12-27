use crate::op_tree::OpTreeInternal;
use crate::query::TreeQuery;
use crate::{
    external_types::{ExternalElemId, ExternalKey, ExternalObjId, ExternalOpId},
    types::{ElemId, Key, ObjId, OpId},
    ActorId, IndexedCache, Op,
};
use fxhash::FxBuildHasher;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Debug;
use std::rc::Rc;

pub(crate) type OpSet = OpSetInternal<16>;

#[derive(Debug, Clone)]
pub(crate) struct OpSetInternal<const B: usize> {
    trees: HashMap<ObjId, OpTreeInternal<B>, FxBuildHasher>,
    objs: Vec<ObjId>,
    length: usize,
    pub m: Rc<RefCell<OpSetMetadata>>,
}

impl<const B: usize> OpSetInternal<B> {
    pub fn new() -> Self {
        OpSetInternal {
            trees: Default::default(),
            objs: Default::default(),
            length: 0,
            m: Rc::new(RefCell::new(OpSetMetadata {
                actors: IndexedCache::new(),
                props: IndexedCache::new(),
                last_objid: None,
            })),
        }
    }

    pub fn iter(&self) -> Iter<'_, B> {
        Iter {
            inner: self,
            index: 0,
            sub_index: 0,
        }
    }

    pub fn search<Q>(&self, obj: ObjId, query: Q) -> Q
    where
        Q: TreeQuery<B>,
    {
        if let Some(tree) = self.trees.get(&obj) {
            tree.search(query, &*self.m.borrow())
        } else {
            query
        }
    }

    pub fn replace<F>(&mut self, obj: ObjId, index: usize, f: F) -> Option<Op>
    where
        F: FnMut(&mut Op),
    {
        if let Some(tree) = self.trees.get_mut(&obj) {
            tree.replace(index, f)
        } else {
            None
        }
    }

    pub fn remove(&mut self, obj: ObjId, index: usize) -> Op {
        let tree = self.trees.get_mut(&obj).unwrap();
        self.length -= 1;
        let op = tree.remove(index);
        if tree.is_empty() {
            self.trees.remove(&obj);
        }
        op
    }

    pub fn len(&self) -> usize {
        self.length
    }

    pub fn insert(&mut self, index: usize, element: Op) {
        let Self {
            ref mut trees,
            ref mut objs,
            ref mut m,
            ..
        } = self;
        trees
            .entry(element.obj)
            .or_insert_with(|| {
                let pos = objs
                    .binary_search_by(|probe| m.borrow().lamport_cmp(probe, &element.obj))
                    .unwrap_err();
                objs.insert(pos, element.obj);
                Default::default()
            })
            .insert(index, element);
        self.length += 1;
    }

    #[cfg(feature = "optree-visualisation")]
    pub fn visualise(&self) -> String {
        let mut out = Vec::new();
        let meta_ref = self.m.borrow();
        let graph = super::visualisation::GraphVisualisation::construct(&self.trees, &meta_ref);
        dot::render(&graph, &mut out).unwrap();
        String::from_utf8_lossy(&out[..]).to_string()
    }
}

impl<const B: usize> Default for OpSetInternal<B> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a, const B: usize> IntoIterator for &'a OpSetInternal<B> {
    type Item = &'a Op;

    type IntoIter = Iter<'a, B>;

    fn into_iter(self) -> Self::IntoIter {
        Iter {
            inner: self,
            index: 0,
            sub_index: 0,
        }
    }
}

pub(crate) struct Iter<'a, const B: usize> {
    inner: &'a OpSetInternal<B>,
    index: usize,
    sub_index: usize,
}

impl<'a, const B: usize> Iterator for Iter<'a, B> {
    type Item = &'a Op;

    fn next(&mut self) -> Option<Self::Item> {
        let obj = self.inner.objs.get(self.index)?;
        let tree = self.inner.trees.get(obj)?;
        self.sub_index += 1;
        if let Some(op) = tree.get(self.sub_index - 1) {
            Some(op)
        } else {
            self.index += 1;
            self.sub_index = 1;
            // FIXME is it possible that a rolled back transaction could break the iterator by
            // having an empty tree?
            let obj = self.inner.objs.get(self.index)?;
            let tree = self.inner.trees.get(obj)?;
            tree.get(self.sub_index - 1)
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct OpSetMetadata {
    pub actors: IndexedCache<ActorId>,
    pub props: IndexedCache<String>,
    // For the common case of many consecutive operations on the same object we cache the last
    // object we looked up
    last_objid: Option<(ExternalOpId, OpId)>,
}

impl OpSetMetadata {
    pub fn key_cmp(&self, left: &Key, right: &Key) -> Ordering {
        match (left, right) {
            (Key::Map(a), Key::Map(b)) => self.props[*a].cmp(&self.props[*b]),
            _ => panic!("can only compare map keys"),
        }
    }

    pub fn lamport_cmp<S: SuccinctLamport>(&self, left: S, right: S) -> Ordering {
        S::cmp(self, left, right)
    }

    pub fn import_opid(&mut self, ext_opid: &ExternalOpId) -> OpId {
        let actor = self.actors.cache(ext_opid.actor().clone());
        let opid = OpId::new(ext_opid.counter(), actor);
        self.last_objid = Some((ext_opid.clone(), opid));
        opid
    }

    pub(crate) fn export_opid(&self, opid: &OpId) -> Option<ExternalOpId> {
        ExternalOpId::from_internal(opid, self)
    }

    pub(crate) fn export_objid(&self, objid: &ObjId) -> Option<ExternalObjId<'static>> {
        match objid {
            ObjId::Root => Some(ExternalObjId::Root),
            ObjId::Op(op) => self
                .export_opid(op)
                .map(|obj| ExternalObjId::from(obj).into_owned()),
        }
    }

    pub(crate) fn import_objid<'a, A: Into<ExternalObjId<'a>>>(&mut self, ext_objid: A) -> ObjId {
        match ext_objid.into() {
            ExternalObjId::Root => ObjId::Root,
            ExternalObjId::Op(external_op) => {
                if let Some((last_ext, last_int)) = &self.last_objid {
                    if last_ext == external_op.as_ref() {
                        return ObjId::from(*last_int);
                    }
                }
                let op = self.import_opid(&external_op);
                self.last_objid = Some((external_op.into_owned(), op));
                ObjId::Op(op)
            }
        }
    }

    pub(crate) fn export_elemid(&self, elemid: &ElemId) -> Option<ExternalElemId<'static>> {
        match elemid {
            ElemId::Head => Some(ExternalElemId::Head),
            ElemId::Op(op) => self
                .export_opid(op)
                .map(|o| ExternalElemId::from(o).into_owned()),
        }
    }

    pub(crate) fn export_key(&self, key: &Key) -> Option<ExternalKey<'static>> {
        match key {
            Key::Map(key_index) => self
                .map_key(*key_index)
                .map(|s| ExternalKey::Map(s.clone().into())),
            Key::Seq(elemid) => self.export_elemid(elemid).map(|e| e.into()),
        }
    }

    pub(crate) fn map_key(&self, key_index: usize) -> Option<&String> {
        self.props.get_safe(key_index)
    }
}

/// Lamport timestamps which don't contain their actor ID directly and therefore need access to
/// some metadata to compare their actor ID parts
pub(crate) trait SuccinctLamport {
    fn cmp(m: &OpSetMetadata, left: Self, right: Self) -> Ordering;
}

impl SuccinctLamport for OpId {
    fn cmp(m: &OpSetMetadata, left: Self, right: Self) -> Ordering {
        match (left.counter(), right.counter()) {
            (0, 0) => Ordering::Equal,
            (0, _) => Ordering::Less,
            (_, 0) => Ordering::Greater,
            (a, b) if a == b => m.actors[right.actor()].cmp(&m.actors[left.actor()]),
            (a, b) => a.cmp(&b),
        }
    }
}

impl SuccinctLamport for ObjId {
    fn cmp(m: &OpSetMetadata, left: Self, right: Self) -> Ordering {
        match (left, right) {
            (ObjId::Root, ObjId::Root) => Ordering::Equal,
            (ObjId::Root, ObjId::Op(_)) => Ordering::Less,
            (ObjId::Op(_), ObjId::Root) => Ordering::Greater,
            (ObjId::Op(left_op), ObjId::Op(right_op)) => {
                <OpId as SuccinctLamport>::cmp(m, left_op, right_op)
            }
        }
    }
}

impl SuccinctLamport for &ObjId {
    fn cmp(m: &OpSetMetadata, left: Self, right: Self) -> Ordering {
        <ObjId as SuccinctLamport>::cmp(m, *left, *right)
    }
}
