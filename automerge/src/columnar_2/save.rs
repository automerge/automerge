use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    iter::Iterator,
};

use itertools::Itertools;

use crate::{
    columnar_2::{
        rowblock::{
            change_op_columns::{ChangeOp, ChangeOpsColumns},
            doc_change_columns::{ChangeMetadata, DocChangeColumns},
            doc_op_columns::{DocOp, DocOpColumns},
            Key as EncodedKey, PrimVal,
        },
        storage::{Chunk, Document},
    },
    indexed_cache::IndexedCache,
    types::{ActorId, ElemId, Key, ObjId, Op, OpId, OpType},
    Change, ChangeHash,
};

/// # Panics
///
/// * If any of the `heads` are not in `changes`
/// * If any of ops in `ops` reference an actor which is not in `actors`
/// * If any of ops in `ops` reference a property which is not in `props`
/// * If any of the changes reference a dependency index which is not in `changes`
pub(crate) fn save_document<'a, I, O>(
    changes: I,
    ops: O,
    actors: &'a IndexedCache<ActorId>,
    props: &IndexedCache<String>,
    heads: &[ChangeHash],
) -> Vec<u8>
where
    I: Iterator<Item = &'a Change> + Clone + 'a,
    O: Iterator<Item = (&'a ObjId, &'a Op)> + Clone,
{
    let actor_lookup = actors.encode_index();
    let doc_ops = ops.map(|(obj, op)| DocOp {
        id: translate_opid(&op.id, &actor_lookup),
        insert: op.insert,
        object: translate_objid(obj, &actor_lookup),
        key: translate_key(&op.key, props),
        action: op.action.action_index() as usize,
        value: match &op.action {
            OpType::Set(v) => v.into(),
            OpType::Inc(i) => PrimVal::Int(*i),
            _ => PrimVal::Null,
        },
        succ: op
            .succ
            .iter()
            .map(|o| translate_opid(o, &actor_lookup))
            .collect(),
    });
    let mut ops_out = Vec::new();
    let ops_meta = DocOpColumns::encode(doc_ops, &mut ops_out);

    let mut change_out = Vec::new();
    let hash_graph = HashGraph::new(changes.clone(), heads);
    let cols = DocChangeColumns::encode(
        changes.map(|c| hash_graph.construct_change(c, &actor_lookup, actors)),
        &mut change_out,
    );

    let doc = Document {
        actors: actors.sorted().cache,
        heads: heads.to_vec(),
        op_metadata: ops_meta.metadata(),
        op_bytes: Cow::Owned(ops_out),
        change_metadata: cols.metadata(),
        change_bytes: Cow::Owned(change_out),
        head_indices: hash_graph.head_indices,
    };

    let written = doc.write();
    let chunk = Chunk::new_document(&written);
    chunk.write()
}

pub(crate) fn encode_change_ops<'a, O>(
    ops: O,
    change_actor: ActorId,
    actors: &IndexedCache<ActorId>,
    props: &IndexedCache<String>,
) -> (ChangeOpsColumns, Vec<u8>, Vec<ActorId>)
where
    O: Iterator<Item = (&'a ObjId, &'a Op)> + Clone,
{
    let encoded_actors = actor_ids_in_change(ops.clone(), change_actor.clone(), actors);
    let actor_lookup = actors
        .cache
        .iter()
        .map(|a| encoded_actors.iter().position(|r| r == a).unwrap())
        .collect::<Vec<_>>();
    let change_ops = ops.map(|(obj, op)| ChangeOp {
        insert: op.insert,
        obj: translate_objid(obj, &actor_lookup),
        key: translate_key(&op.key, props),
        action: op.action.action_index(),
        val: match &op.action {
            OpType::Set(v) => v.into(),
            OpType::Inc(i) => PrimVal::Int(*i),
            _ => PrimVal::Null,
        },
        pred: op
            .pred
            .iter()
            .map(|o| translate_opid(o, &actor_lookup))
            .collect(),
    });
    let mut out = Vec::new();
    let cols = ChangeOpsColumns::empty().encode(change_ops, &mut out);
    let other_actors = encoded_actors.into_iter().skip(1).collect();
    (cols, out, other_actors)
}

/// When encoding a change chunk we take all the actor IDs referenced by a change and place them in
/// an array. The array has the actor who authored the change as the first element and all
/// remaining actors (i.e. those referenced in object IDs in the target of an operation or in the
/// `pred` of an operation) lexicographically ordered following the change author.
fn actor_ids_in_change<'a, I>(
    ops: I,
    change_actor: ActorId,
    actors: &IndexedCache<ActorId>,
) -> Vec<ActorId>
where
    I: Iterator<Item = (&'a ObjId, &'a Op)> + Clone,
{
    let mut other_ids: Vec<ActorId> = ops
        .flat_map(|(obj, o)| opids_in_operation(&obj, &o, actors))
        .filter(|a| *a != &change_actor)
        .unique()
        .cloned()
        .collect();
    other_ids.sort();
    // Now prepend the change actor
    std::iter::once(change_actor)
        .chain(other_ids.into_iter())
        .collect()
}

fn opids_in_operation<'a>(
    obj: &'a ObjId,
    op: &'a Op,
    actors: &'a IndexedCache<ActorId>,
) -> impl Iterator<Item = &'a ActorId> {
    let obj_actor_id = if obj.is_root() {
        None
    } else {
        Some(actors.get(obj.opid().actor()))
    };
    let pred_ids = op.pred.iter().filter_map(|a| {
        if a.counter() != 0 {
            Some(actors.get(a.actor()))
        } else {
            None
        }
    });
    let key_actor = match &op.key {
        Key::Seq(ElemId(op)) if !op.counter() == 0 => Some(actors.get(op.actor())),
        _ => None,
    };
    obj_actor_id
        .into_iter()
        .chain(key_actor.into_iter())
        .chain(pred_ids)
}

fn translate_key(k: &Key, props: &IndexedCache<String>) -> EncodedKey {
    match k {
        Key::Seq(e) => EncodedKey::Elem(*e),
        Key::Map(idx) => EncodedKey::Prop(props.get(*idx).into()),
    }
}

fn translate_objid(obj: &ObjId, actors: &[usize]) -> ObjId {
    if obj.is_root() {
        *obj
    } else {
        ObjId(translate_opid(&obj.opid(), actors))
    }
}

fn translate_opid(id: &OpId, actors: &[usize]) -> OpId {
    OpId::new(actors[id.actor()], id.counter())
}

fn find_head_indices<'a, I>(changes: I, heads: &[ChangeHash]) -> Vec<u64>
where
    I: Iterator<Item = &'a Change>,
{
    let heads_set: BTreeSet<ChangeHash> = heads.iter().copied().collect();
    let mut head_indices = BTreeMap::new();
    for (index, change) in changes.enumerate() {
        if heads_set.contains(&change.hash()) {
            head_indices.insert(change.hash(), index as u64);
        }
    }
    heads.iter().map(|h| head_indices[h]).collect()
}

struct HashGraph {
    head_indices: Vec<u64>,
    index_by_hash: BTreeMap<ChangeHash, usize>,
}

impl HashGraph {
    fn new<'a, I>(changes: I, heads: &[ChangeHash]) -> Self
    where
        I: Iterator<Item = &'a Change>,
    {
        let heads_set: BTreeSet<ChangeHash> = heads.iter().copied().collect();
        let mut head_indices = BTreeMap::new();
        let mut index_by_hash = BTreeMap::new();
        for (index, change) in changes.enumerate() {
            if heads_set.contains(&change.hash()) {
                head_indices.insert(change.hash(), index as u64);
            }
            index_by_hash.insert(change.hash(), index);
        }
        let head_indices = heads.iter().map(|h| head_indices[h]).collect();
        Self {
            head_indices,
            index_by_hash,
        }
    }

    fn change_index(&self, hash: &ChangeHash) -> usize {
        self.index_by_hash[hash]
    }

    fn construct_change(
        &self,
        c: &Change,
        actor_lookup: &[usize],
        actors: &IndexedCache<ActorId>,
    ) -> ChangeMetadata<'static> {
        ChangeMetadata {
            actor: actor_lookup[actors.lookup(c.actor_id()).unwrap()],
            seq: c.seq(),
            max_op: c.max_op(),
            timestamp: c.timestamp(),
            message: c.message().map(|s| s.into()),
            deps: c
                .deps()
                .iter()
                .map(|d| self.change_index(d) as u64)
                .collect(),
            extra: Cow::Owned(c.extra_bytes().to_vec()),
        }
    }
}
