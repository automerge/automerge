use std::ops::RangeBounds;
use std::sync::{Arc, Mutex};

use crate::clock::Clock;
use crate::op_tree::{OpSetMetadata, OpTreeInternal};
use crate::query::{self, TreeQuery};
use crate::types::ObjId;
use crate::types::{Op, OpId};
use crate::Prop;
use crate::{query::Keys, query::KeysAt, ObjType};

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum MapType {
    Map,
    Table,
}

impl From<MapType> for ObjType {
    fn from(m: MapType) -> Self {
        match m {
            MapType::Map => ObjType::Map,
            MapType::Table => ObjType::Table,
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct MapOpsCache {}

impl MapOpsCache {
    fn lookup<'a, Q: TreeQuery<'a>>(&self, query: &mut Q) -> bool {
        query.cache_lookup_map(self)
    }

    fn update<'a, Q: TreeQuery<'a>>(&mut self, query: &Q) {
        query.cache_update_map(self);
        // TODO: fixup the cache (reordering etc.)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum SeqType {
    List,
    Text,
}

impl From<SeqType> for ObjType {
    fn from(s: SeqType) -> Self {
        match s {
            SeqType::List => ObjType::List,
            SeqType::Text => ObjType::Text,
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct SeqOpsCache {
    // last insertion (list index, tree index, whether the last op was an insert, opid to be inserted)
    // TODO: invalidation
    pub(crate) last: Option<(usize, usize, bool, OpId)>,
}

impl SeqOpsCache {
    fn lookup<'a, Q: TreeQuery<'a>>(&self, query: &mut Q) -> bool {
        query.cache_lookup_seq(self)
    }

    fn update<'a, Q: TreeQuery<'a>>(&mut self, query: &Q) {
        query.cache_update_seq(self);
        // TODO: fixup the cache (reordering etc.)
    }
}

/// Stores the data for an object.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ObjectData {
    internal: ObjectDataInternal,
    /// The operations pertaining to this object.
    pub(crate) ops: OpTreeInternal,
    /// The id of the parent object, root has no parent.
    pub parent: Option<ObjId>,
}

#[derive(Debug, Clone)]
pub(crate) enum ObjectDataInternal {
    Map {
        /// The type of this object.
        typ: MapType,
        cache: Arc<Mutex<MapOpsCache>>,
    },
    Seq {
        /// The type of this object.
        typ: SeqType,
        cache: Arc<Mutex<SeqOpsCache>>,
    },
}

impl PartialEq for ObjectDataInternal {
    fn eq(&self, other: &ObjectDataInternal) -> bool {
        match (self, other) {
            (
                ObjectDataInternal::Map {
                    typ: typ1,
                    cache: _,
                },
                ObjectDataInternal::Map {
                    typ: typ2,
                    cache: _,
                },
            ) => typ1 == typ2,
            (
                ObjectDataInternal::Map { typ: _, cache: _ },
                ObjectDataInternal::Seq { typ: _, cache: _ },
            ) => false,
            (
                ObjectDataInternal::Seq { typ: _, cache: _ },
                ObjectDataInternal::Map { typ: _, cache: _ },
            ) => false,
            (
                ObjectDataInternal::Seq {
                    typ: typ1,
                    cache: _,
                },
                ObjectDataInternal::Seq {
                    typ: typ2,
                    cache: _,
                },
            ) => typ1 == typ2,
        }
    }
}

impl ObjectData {
    pub fn root() -> Self {
        ObjectData {
            internal: ObjectDataInternal::Map {
                typ: MapType::Map,
                cache: Default::default(),
            },
            ops: Default::default(),
            parent: None,
        }
    }

    pub fn new(typ: ObjType, parent: Option<ObjId>) -> Self {
        let internal = match typ {
            ObjType::Map => ObjectDataInternal::Map {
                typ: MapType::Map,
                cache: Default::default(),
            },
            ObjType::Table => ObjectDataInternal::Map {
                typ: MapType::Table,
                cache: Default::default(),
            },
            ObjType::List => ObjectDataInternal::Seq {
                typ: SeqType::List,
                cache: Default::default(),
            },
            ObjType::Text => ObjectDataInternal::Seq {
                typ: SeqType::Text,
                cache: Default::default(),
            },
        };
        ObjectData {
            internal,
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

    pub fn range<'a, R: RangeBounds<Prop>>(
        &'a self,
        range: R,
        meta: &'a OpSetMetadata,
    ) -> Option<query::Range<'a, R>> {
        self.ops.range(range, meta)
    }

    pub fn range_at<'a, R: RangeBounds<Prop>>(
        &'a self,
        range: R,
        meta: &'a OpSetMetadata,
        clock: Clock,
    ) -> Option<query::RangeAt<'a, R>> {
        self.ops.range_at(range, meta, clock)
    }

    pub fn search<'a, 'b: 'a, Q>(&'b self, mut query: Q, metadata: &OpSetMetadata) -> Q
    where
        Q: TreeQuery<'a>,
    {
        match self {
            ObjectData {
                ops,
                internal: ObjectDataInternal::Map { cache, .. },
                ..
            } => {
                let mut cache = cache.lock().unwrap();
                if !cache.lookup(&mut query) {
                    query = ops.search(query, metadata);
                }
                cache.update(&query);
                query
            }
            ObjectData {
                ops,
                internal: ObjectDataInternal::Seq { cache, .. },
                ..
            } => {
                let mut cache = cache.lock().unwrap();
                if !cache.lookup(&mut query) {
                    query = ops.search(query, metadata);
                }
                cache.update(&query);
                query
            }
        }
    }

    pub fn update<F>(&mut self, index: usize, f: F)
    where
        F: FnOnce(&mut Op),
    {
        self.ops.update(index, f)
    }

    pub fn remove(&mut self, index: usize) -> Op {
        self.ops.remove(index)
    }

    pub fn insert(&mut self, index: usize, op: Op) {
        self.ops.insert(index, op)
    }

    pub fn typ(&self) -> ObjType {
        match &self.internal {
            ObjectDataInternal::Map { typ, .. } => (*typ).into(),
            ObjectDataInternal::Seq { typ, .. } => (*typ).into(),
        }
    }

    pub fn get(&self, index: usize) -> Option<&Op> {
        self.ops.get(index)
    }
}
