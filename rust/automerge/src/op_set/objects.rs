use std::{
    collections::{BTreeMap, HashMap},
    ops::Range,
};

use fxhash::FxBuildHasher;

use crate::{
    clock::Clock,
    iter::TopOps,
    op_tree::{
        FoundOpId, FoundOpWithPatchLog, FoundOpWithoutPatchLog, OpTree, OpTreeInternal, OpTreeIter,
        OpsFound,
    },
    query::TreeQuery,
    types::{ListEncoding, ObjId, Op, OpId},
    ObjType, Prop,
};

use super::OpSetMetadata;

pub(crate) enum Object<'a> {
    OpTree(&'a OpTree),
}

impl<'a> Object<'a> {
    pub(crate) fn length(
        &self,
        m: &OpSetMetadata,
        encoding: ListEncoding,
        clock: Option<Clock>,
    ) -> usize {
        match self {
            Self::OpTree(tree) => {
                match (&clock, tree.index(encoding)) {
                    // no clock and a clean index? - use it
                    (None, Some(index)) => index.visible_len(encoding),
                    // do it the hard way - walk each op
                    _ => tree
                        .internal
                        .top_ops(clock, m)
                        .fold(0, |acc, top| acc + top.op.width(encoding)),
                }
            }
        }
    }

    pub(crate) fn iter_ops(&self) -> impl Iterator<Item = &'a Op> {
        match self {
            Self::OpTree(tree) => tree.iter(),
        }
    }

    pub(crate) fn seek_opid(
        &self,
        opid: OpId,
        encoding: ListEncoding,
        clock: Option<&Clock>,
        meta: &OpSetMetadata,
    ) -> Option<FoundOpId<'a>> {
        match self {
            Self::OpTree(tree) => tree.internal.seek_opid(opid, encoding, clock, meta),
        }
    }

    pub(crate) fn seek_ops_by_prop(
        &self,
        meta: &'a OpSetMetadata,
        prop: Prop,
        encoding: ListEncoding,
        clock: Option<&Clock>,
    ) -> Option<OpsFound<'a>> {
        match self {
            Self::OpTree(tree) => tree.internal.seek_ops_by_prop(meta, prop, encoding, clock),
        }
    }

    pub(crate) fn find_op_with_patch_log(
        &self,
        op: &'a Op,
        encoding: ListEncoding,
        meta: &OpSetMetadata,
    ) -> FoundOpWithPatchLog<'a> {
        match self {
            Self::OpTree(tree) => tree.internal.find_op_with_patch_log(op, encoding, meta),
        }
    }

    pub(crate) fn find_op_without_patch_log(
        &self,
        op: &Op,
        meta: &OpSetMetadata,
    ) -> FoundOpWithoutPatchLog {
        match self {
            Self::OpTree(tree) => tree.internal.find_op_without_patch_log(op, meta),
        }
    }

    pub(crate) fn top_ops(&self, clock: Option<Clock>, meta: &'a OpSetMetadata) -> TopOps<'a> {
        match self {
            Self::OpTree(tree) => tree.internal.top_ops(clock, meta),
        }
    }

    pub(crate) fn parent(&self) -> Option<ObjId> {
        match self {
            Self::OpTree(tree) => tree.parent,
        }
    }

    pub(crate) fn search<Q>(&self, m: &'a OpSetMetadata, mut query: Q) -> Q
    where
        Q: TreeQuery<'a>,
    {
        match self {
            Self::OpTree(optree) => {
                if query.can_shortcut_search(optree) {
                    query
                } else {
                    optree.internal.search(query, m)
                }
            }
        }
    }

    pub(crate) fn objtype(&self) -> ObjType {
        match self {
            Self::OpTree(tree) => tree.objtype,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Objects {
    full_fat_optrees: HashMap<ObjId, OpTree, FxBuildHasher>,
    small_objects: SmallObjects,
}

impl Objects {
    pub(crate) fn new(trees: HashMap<ObjId, OpTree, FxBuildHasher>) -> Self {
        Self {
            full_fat_optrees: trees,
            small_objects: SmallObjects::new(),
        }
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&ObjId, ObjectOps<'_>)> {
        self.full_fat_optrees
            .iter()
            .map(|(id, tree)| (id, ObjectOps::FullFat(tree.objtype, tree)))
    }

    pub(crate) fn get(&self, obj: &ObjId) -> Option<Object<'_>> {
        self.full_fat_optrees.get(&obj).map(Object::OpTree)
    }

    pub(crate) fn get_mut(&mut self, obj: &ObjId) -> Option<&mut OpTree> {
        if !self.full_fat_optrees.contains_key(&obj) {
            self.full_fat_optrees.insert(obj.clone(), OpTree::new());
            if let Some(tree) = self.promote_small_object(obj) {
                self.full_fat_optrees.insert(obj.clone(), tree);
            }
        }
        self.full_fat_optrees.get_mut(obj)
    }

    pub(crate) fn remove(&mut self, obj: &ObjId) {
        self.full_fat_optrees.remove(&obj);
        self.small_objects.remove(obj);
    }

    pub(crate) fn insert(&mut self, obj: ObjId, tree: OpTree) {
        self.full_fat_optrees.insert(obj, tree);
    }

    #[cfg(feature = "optree-visualisation")]
    pub(crate) fn trees(&self) -> &HashMap<ObjId, OpTree, FxBuildHasher> {
        &self.full_fat_optrees
    }

    fn promote_small_object(&self, obj: &ObjId) -> Option<OpTree> {
        if let Some((small, ops)) = self.small_objects.get(&obj) {
            let mut internal = OpTreeInternal::new();
            for (index, op) in ops.iter().enumerate() {
                internal.insert(index, op.clone());
            }
            let tree = OpTree {
                internal,
                objtype: small.typ,
                parent: small.parent,
                last_insert: None,
            };
            return Some(tree);
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct SmallObject {
    range: Range<usize>,
    typ: ObjType,
    parent: Option<ObjId>,
    removed: bool,
}

#[derive(Debug, Clone, PartialEq)]
struct SmallObjects {
    objs: BTreeMap<ObjId, SmallObject>,
    ops: Vec<Op>,
}

impl SmallObjects {
    fn new() -> SmallObjects {
        SmallObjects {
            objs: BTreeMap::new(),
            ops: Vec::new(),
        }
    }

    fn get(&self, obj: &ObjId) -> Option<(&SmallObject, &[Op])> {
        if let Some(small) = self.objs.get(obj) {
            Some((small, &self.ops[small.range.clone()]))
        } else {
            None
        }
    }

    fn remove(&mut self, obj: &ObjId) {
        if let Some(small) = self.objs.get_mut(obj) {
            small.removed = true;
        }
    }
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
            Self::OpTree(o) => o.next(),
        }
    }
}
