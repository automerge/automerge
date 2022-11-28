use std::collections::HashMap;

use fxhash::FxBuildHasher;

use super::{OpSet, OpTree};
use crate::{
    op_tree::OpTreeInternal,
    storage::load::{DocObserver, LoadedObject},
    types::{ObjId, Op},
    Automerge, OpObserver,
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

    fn object_loaded(&mut self, loaded: LoadedObject) {
        let mut internal = OpTreeInternal::new();
        for (index, op) in loaded.ops.into_iter().enumerate() {
            internal.insert(index, op);
        }
        let tree = OpTree {
            internal,
            objtype: loaded.obj_type,
            parent: loaded.parent,
        };
        self.completed_objects.insert(loaded.id, tree);
    }

    fn finish(self, metadata: super::OpSetMetadata) -> Self::Output {
        let len = self.completed_objects.values().map(|t| t.len()).sum();
        OpSet {
            trees: self.completed_objects,
            length: len,
            m: metadata,
        }
    }
}

/// A DocObserver which just accumulates ops until the document has finished reconstructing and
/// then inserts all of the ops using `OpSet::insert_op_with_observer`
pub(crate) struct ObservedOpSetBuilder<'a, O: OpObserver> {
    observer: &'a mut O,
    ops: Vec<(ObjId, Op)>,
}

impl<'a, O: OpObserver> ObservedOpSetBuilder<'a, O> {
    pub(crate) fn new(observer: &'a mut O) -> Self {
        Self {
            observer,
            ops: Vec::new(),
        }
    }
}

impl<'a, O: OpObserver> DocObserver for ObservedOpSetBuilder<'a, O> {
    type Output = OpSet;

    fn object_loaded(&mut self, object: LoadedObject) {
        self.ops.reserve(object.ops.len());
        for op in object.ops {
            self.ops.push((object.id, op));
        }
    }

    fn finish(self, _metadata: super::OpSetMetadata) -> Self::Output {
        let mut opset = Automerge::new();
        for (obj, op) in self.ops {
            opset.insert_op_with_observer(&obj, op, self.observer);
        }
        opset.ops
    }
}
