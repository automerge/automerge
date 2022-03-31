use crate::clock::Clock;
use crate::query::TreeQuery;
use crate::types::ObjId;
use crate::types::Op;
use crate::ObjType;
use crate::{query::Keys, query::KeysAt, ObjType};

use crate::op_tree::{OpSetMetadata, OpTreeInternal};

/// Stores the data for an object.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ObjectData {
    /// The type of this object.
    typ: ObjType,
    /// The operations pertaining to this object.
    ops: OpTreeInternal,
    /// The id of the parent object, root has no parent.
    pub parent: Option<ObjId>,
}

impl ObjectData {
    pub fn root() -> Self {
        ObjectData {
            typ: ObjType::Map,
            ops: Default::default(),
            parent: None,
        }
    }

    pub fn new(typ: ObjType, parent: Option<ObjId>) -> Self {
        ObjectData {
            typ,
            ops: Default::default(),
            parent,
        }
    }

    pub fn keys(&self) -> Option<Keys> {
        self.ops.keys()
    }

    pub fn keys_at(&self, clock: Clock) -> Option<KeysAt> {
        self.ops.keys_at(clock)
    }

    pub fn search<Q>(&self, query: Q, metadata: &OpSetMetadata) -> Q
    where
        Q: TreeQuery,
    {
        self.ops.search(query, metadata)
    }

    pub fn replace<F>(&mut self, index: usize, f: F)
    where
        F: FnMut(&mut Op),
    {
        self.ops.replace(index, f)
    }

    pub fn remove(&mut self, index: usize) -> Op {
        self.ops.remove(index)
    }

    pub fn insert(&mut self, index: usize, op: Op) {
        self.ops.insert(index, op)
    }

    pub fn typ(&self) -> ObjType {
        self.typ
    }

    pub fn get(&self, index: usize) -> Option<&Op> {
        self.ops.get(index)
    }
}
