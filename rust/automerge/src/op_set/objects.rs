use std::collections::HashMap;

use fxhash::FxBuildHasher;

use crate::{types::ObjId, op_tree::OpTree};

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

    pub(crate) fn iter(&self) -> impl Iterator<Item=(&ObjId, &OpTree)> {
        self.full_fat_optrees.iter()
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
