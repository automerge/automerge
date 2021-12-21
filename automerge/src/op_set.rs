use crate::op_tree::OpTreeInternal;
use crate::query::TreeQuery;
use crate::{ActorId, IndexedCache, Key, ObjId, Op, OpId};
use fxhash::FxBuildHasher;
use std::cmp::Ordering;
use std::collections::HashMap;

pub(crate) type OpSet = OpSetInternal<16>;

#[derive(Debug, Clone)]
pub(crate) struct OpSetInternal<const B: usize> {
    trees: HashMap<ObjId, OpTreeInternal<B>, FxBuildHasher>,
    objs: Vec<ObjId>,
    length: usize,
    pub m: OpSetMetadata,
}

impl<const B: usize> OpSetInternal<B> {
    pub fn new() -> Self {
        OpSetInternal {
            trees: Default::default(),
            objs: Default::default(),
            length: 0,
            m: OpSetMetadata {
                actors: IndexedCache::new(),
                props: IndexedCache::new(),
            },
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
            tree.search(query, &self.m)
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
                    .binary_search_by(|probe| m.lamport_cmp(probe.0, element.obj.0))
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
            // FIXME - this one seems backwards to me - why - is values() returning in the wrong order?
            (OpId(a, x), OpId(b, y)) if a == b => self.actors[y].cmp(&self.actors[x]),
            (OpId(a, _), OpId(b, _)) => a.cmp(&b),
        }
    }
}
