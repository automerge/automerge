use crate::indexed_cache::IndexedCache;
use crate::op_tree::OpTreeInternal;
use crate::query::TreeQuery;
use crate::types::{ActorId, Key, ObjId, Op, OpId, OpType};
use crate::ObjType;
use fxhash::FxBuildHasher;
use std::cmp::Ordering;
use std::collections::HashMap;

pub(crate) type OpSet = OpSetInternal<16>;

#[derive(Debug, Clone)]
pub(crate) struct OpSetInternal<const B: usize> {
    trees: HashMap<ObjId, (ObjType, OpTreeInternal<B>), FxBuildHasher>,
    length: usize,
    pub m: OpSetMetadata,
}

impl<const B: usize> OpSetInternal<B> {
    pub fn new() -> Self {
        let mut trees: HashMap<_, _, _> = Default::default();
        trees.insert(ObjId::root(), (ObjType::Map, Default::default()));
        OpSetInternal {
            trees,
            length: 0,
            m: OpSetMetadata {
                actors: IndexedCache::new(),
                props: IndexedCache::new(),
            },
        }
    }

    pub fn iter(&self) -> Iter<'_, B> {
        let mut objs: Vec<_> = self.trees.keys().collect();
        objs.sort_by(|a, b| self.m.lamport_cmp(a.0, b.0));
        Iter {
            inner: self,
            index: 0,
            sub_index: 0,
            objs,
        }
    }

    pub fn search<Q>(&self, obj: ObjId, query: Q) -> Q
    where
        Q: TreeQuery<B>,
    {
        if let Some((_typ, tree)) = self.trees.get(&obj) {
            tree.search(query, &self.m)
        } else {
            query
        }
    }

    pub fn replace<F>(&mut self, obj: ObjId, index: usize, f: F) -> Option<Op>
    where
        F: FnMut(&mut Op),
    {
        if let Some((_typ, tree)) = self.trees.get_mut(&obj) {
            tree.replace(index, f)
        } else {
            None
        }
    }

    pub fn remove(&mut self, obj: ObjId, index: usize) -> Op {
        // this happens on rollback - be sure to go back to the old state
        let (_typ, tree) = self.trees.get_mut(&obj).unwrap();
        self.length -= 1;
        let op = tree.remove(index);
        if let OpType::Make(_) = &op.action {
            self.trees.remove(&op.id.into());
        }
        op
    }

    pub fn len(&self) -> usize {
        self.length
    }

    pub fn insert(&mut self, index: usize, element: Op) {
        if let OpType::Make(typ) = element.action {
            self.trees
                .insert(element.id.into(), (typ, Default::default()));
        }

        if let Some((_typ, tree)) = self.trees.get_mut(&element.obj) {
            //let tree = self.trees.get_mut(&element.obj).unwrap();
            tree.insert(index, element);
            self.length += 1;
        }
    }

    pub fn object_type(&self, id: &ObjId) -> Option<ObjType> {
        self.trees.get(id).map(|(typ, _)| *typ)
    }

    #[cfg(feature = "optree-visualisation")]
    pub fn visualise(&self) -> String {
        let mut out = Vec::new();
        let graph = super::visualisation::GraphVisualisation::construct(&self.trees, &self.m);
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
        let mut objs: Vec<_> = self.trees.keys().collect();
        objs.sort_by(|a, b| self.m.lamport_cmp(a.0, b.0));
        Iter {
            inner: self,
            index: 0,
            objs,
            sub_index: 0,
        }
    }
}

pub(crate) struct Iter<'a, const B: usize> {
    inner: &'a OpSetInternal<B>,
    index: usize,
    objs: Vec<&'a ObjId>,
    sub_index: usize,
}

impl<'a, const B: usize> Iterator for Iter<'a, B> {
    type Item = &'a Op;

    fn next(&mut self) -> Option<Self::Item> {
        let mut result = None;
        for obj in self.objs.iter().skip(self.index) {
            let (_typ, tree) = self.inner.trees.get(obj)?;
            result = tree.get(self.sub_index);
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

#[derive(Clone, Debug)]
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
