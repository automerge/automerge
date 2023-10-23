use std::collections::HashMap;

use fxhash::FxBuildHasher;

use super::{OpSet, OpTree};
use crate::op_tree::OpSetData;
use crate::{
    op_tree::OpTreeInternal,
    storage::load::{DocObserver, LoadedObject},
    types::ObjId,
};

/// An opset builder which creates an optree for each object as it finishes loading, inserting the
/// ops using `OpTreeInternal::insert`. This should be faster than using `OpSet::insert_*` but only
/// works because the ops in the document format are in the same order as in the optrees.
pub(crate) struct OpSetBuilder {
    completed_objects: HashMap<ObjId, OpTree, FxBuildHasher>,
}

impl OpSetBuilder {
    pub(crate) fn new() -> OpSetBuilder {
        Self {
            completed_objects: HashMap::default(),
        }
    }
}

impl DocObserver for OpSetBuilder {
    type Output = OpSet;

    fn object_loaded(&mut self, loaded: LoadedObject, osd: &mut OpSetData) {
        let mut internal = OpTreeInternal::new();
        for (index, op) in loaded.ops.into_iter().enumerate() {
            let idx = osd.push(op);
            internal.insert(index, idx, osd);
        }
        let tree = OpTree {
            internal,
            objtype: loaded.obj_type,
            parent: loaded.parent,
            last_insert: None,
        };
        self.completed_objects.insert(loaded.id, tree);
    }

    fn finish(self, osd: super::OpSetData) -> Self::Output {
        let length = self.completed_objects.values().map(|t| t.len()).sum();
        OpSet {
            trees: self.completed_objects,
            length,
            osd,
        }
    }
}
