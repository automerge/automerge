use std::collections::HashMap;

use fxhash::FxBuildHasher;

use crate::{types::{ObjId, Op}, op_tree::{OpTree, OpTreeIter}, ObjType};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Objects {
    full_fat_optrees: HashMap<ObjId, OpTree, FxBuildHasher>,
    small_objects: SmallObjects,
}

impl Objects {
    pub(crate) fn new(trees: HashMap<ObjId, OpTree, FxBuildHasher>) -> Self {
        Self {
            full_fat_optrees: trees,
            small_objects: SmallObjects{},
        }
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item=(&ObjId, ObjectOps<'_>)> {
        self.full_fat_optrees.iter().map(|(id, tree)| {
            (id, ObjectOps::FullFat(tree.objtype, tree))
        })
    }

    pub(crate) fn get(&self, obj: &ObjId) -> Option<&OpTree> {
        self.full_fat_optrees.get(&obj)
    }

    pub(crate) fn get_mut(&mut self, obj: &ObjId) -> Option<&mut OpTree> {
        self.full_fat_optrees.get_mut(&obj)
    }

    pub(crate) fn remove(&mut self, obj: &ObjId) {
        self.full_fat_optrees.remove(&obj);
    }

    pub(crate) fn insert(&mut self, obj: ObjId, tree: OpTree) {
        self.full_fat_optrees.insert(obj, tree);
    }

    #[cfg(feature = "optree-visualisation")]
    pub(crate) fn trees(&self) -> &HashMap<ObjId, OpTree, FxBuildHasher> {
        &self.full_fat_optrees
    }
}

#[derive(Debug, Clone, PartialEq)]
struct SmallObjects {
}

#[derive(Clone)]
pub(crate) enum ObjectOps<'a> {
    FullFat(ObjType, &'a OpTree),
}

impl<'a> ObjectOps<'a> {
    pub(crate) fn objtype(&self) -> ObjType {
        match self {
            ObjectOps::FullFat(t, _) => *t,
        }
    }

    pub(crate) fn iter(&self) -> ObjIter<'a> {
        match self {
            ObjectOps::FullFat(_, t) => ObjIter::OpTree(t.iter()),
        }
    }
}

struct ObjectsIter<'a> {
    objects: &'a Objects,
    cur: Option<ObjId>,
}

impl<'a> Iterator for ObjectsIter<'a> {
    type Item = ObjectOps<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

#[derive(Clone)]
pub(crate) enum ObjIter<'a> {
    OpTree(OpTreeIter<'a>),
}

impl<'a> Iterator for ObjIter<'a> {
    type Item = &'a Op;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::OpTree(o) => o.next()
        }
    }
}
