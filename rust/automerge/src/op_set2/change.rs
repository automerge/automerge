use super::meta::MetaCursor;
use super::op::{AsChangeOp, OpBuilder};
use super::types::{ActionCursor, ActorCursor, ActorIdx};
use crate::change_graph::ChangeGraph;
use crate::storage::change::{ChangeOpsColumns as ChangeOpsColumns2, Verified};
use crate::storage::{Change, ChunkType, Header};
use crate::types::{ActorId, ChangeHash};
use hexane::{BooleanCursor, ColumnCursor, DeltaCursor, RawCursor, StrCursor, UIntCursor};
use std::borrow::Cow;
use std::cmp::Ordering;
use std::io::Write;
use std::marker::PhantomData;
use std::num::NonZero;
use std::ops::Range;

pub(crate) mod batch;
pub(crate) mod collector;

pub(crate) use collector::{BuildChangeMetadata, ChangeCollector, CollectedChanges, OutOfMemory};

pub(crate) trait GetHash {
    fn get_hash(&self, index: usize) -> Option<ChangeHash>;
}

impl GetHash for Vec<crate::Change> {
    fn get_hash(&self, index: usize) -> Option<ChangeHash> {
        Some(self.get(index)?.hash())
    }
}

impl GetHash for ChangeGraph {
    fn get_hash(&self, index: usize) -> Option<ChangeHash> {
        self.index_to_hash(index).copied()
    }
}

pub(crate) fn build_change<T, G>(
    ops: &[T],
    meta: &BuildChangeMetadata<'_>,
    graph: &G,
    actors: &[ActorId],
) -> Change<'static, Verified>
where
    T: AsChangeOp,
    G: GetHash,
{
    let mut mapper = ActorMapper::new(actors);
    build_change_inner(ops, meta, graph, &mut mapper)
}

pub(crate) fn build_change_inner<T, G>(
    ops: &[T],
    meta: &BuildChangeMetadata<'_>,
    graph: &G,
    mapper: &mut ActorMapper<'_>,
) -> Change<'static, Verified>
where
    T: AsChangeOp,
    G: GetHash,
{
    let num_ops = ops.len();
    let mut col_data = Vec::new();

    let actor = mapper.actors[meta.actor].clone();

    let start_op = ops.first().map(T::op_id_ctr).unwrap_or(meta.max_op + 1);

    let ops_meta = write_change_ops(ops, meta.actor, &mut col_data, mapper);

    let other_actors: Vec<_> = mapper.iter().collect();

    let mut data = Vec::with_capacity(col_data.len());
    leb128::write::unsigned(&mut data, meta.deps.len() as u64).unwrap();

    // FIXME missing value here is changes out of order error
    let deps: Vec<_> = meta
        .deps
        .iter()
        .map(|i| graph.get_hash(*i as usize).unwrap())
        .collect();

    for hash in &deps {
        data.write_all(hash.as_bytes()).unwrap();
    }

    length_prefixed_bytes(&actor, &mut data);

    leb128::write::unsigned(&mut data, meta.seq).unwrap();
    leb128::write::unsigned(&mut data, start_op).unwrap();
    leb128::write::signed(&mut data, meta.timestamp).unwrap();

    length_prefixed_bytes(meta.message_str(), &mut data);

    leb128::write::unsigned(&mut data, other_actors.len() as u64).unwrap();

    for actor in other_actors.iter() {
        length_prefixed_bytes(actor, &mut data);
    }

    ops_meta.raw_columns().write(&mut data);

    let ops_data_start = data.len();
    let ops_data = ops_data_start..(ops_data_start + col_data.len());

    data.extend(col_data);
    let extra_bytes = data.len()..(data.len() + meta.extra.len());
    if !meta.extra.is_empty() {
        data.extend(meta.extra.as_ref());
    }

    let header = Header::new(ChunkType::Change, &data);

    let mut bytes = Vec::with_capacity(header.len() + data.len());
    header.write(&mut bytes);
    bytes.extend(data);

    let ops_data = shift_range(ops_data, header.len());
    let extra_bytes = shift_range(extra_bytes, header.len());

    Change {
        bytes: Cow::Owned(bytes),
        header,
        dependencies: deps,
        actor,
        other_actors,
        seq: meta.seq,
        start_op: NonZero::new(start_op).unwrap(),
        timestamp: meta.timestamp,
        message: meta.message.as_ref().map(|s| s.to_string()),
        ops_meta,
        ops_data,
        extra_bytes,
        num_ops,
        _phantom: PhantomData,
    }
}

impl PartialOrd for OpBuilder<'_> {
    fn partial_cmp(&self, other: &OpBuilder<'_>) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OpBuilder<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

#[derive(Default)]
pub(crate) struct ChangeOpsColumns {
    pub(crate) obj_actor: Range<usize>,
    pub(crate) obj_ctr: Range<usize>,
    pub(crate) key_actor: Range<usize>,
    pub(crate) key_ctr: Range<usize>,
    pub(crate) key_str: Range<usize>,
    pub(crate) insert: Range<usize>,
    pub(crate) action: Range<usize>,
    pub(crate) value_meta: Range<usize>,
    pub(crate) value: Range<usize>,
    pub(crate) pred_count: Range<usize>,
    pub(crate) pred_actor: Range<usize>,
    pub(crate) pred_ctr: Range<usize>,
    pub(crate) expand: Range<usize>,
    pub(crate) mark_name: Range<usize>,
}

pub(crate) fn shift_range(range: Range<usize>, by: usize) -> Range<usize> {
    range.start + by..range.end + by
}

pub(crate) fn length_prefixed_bytes<B: AsRef<[u8]>>(b: B, out: &mut Vec<u8>) -> usize {
    let prefix_len = leb128::write::unsigned(out, b.as_ref().len() as u64).unwrap();
    out.write_all(b.as_ref()).unwrap();
    prefix_len + b.as_ref().len()
}

impl<'a> PartialEq for OpBuilder<'a> {
    fn eq(&self, other: &OpBuilder<'a>) -> bool {
        self.id == other.id
    }
}

impl Eq for OpBuilder<'_> {}

fn write_change_ops<T>(
    ops: &[T],
    change_actor: usize,
    data: &mut Vec<u8>,
    mapper: &mut ActorMapper<'_>,
) -> ChangeOpsColumns2
where
    T: AsChangeOp,
{
    if ops.is_empty() {
        mapper.reset();
        return ChangeOpsColumns::default().into();
    }

    mapper.remap_actors(ops, change_actor);

    let remap = move |actor: Option<Cow<'_, ActorIdx>>| {
        actor.map(|a| Cow::Owned(mapper.mapping[usize::from(*a)].unwrap()))
    };

    let obj_actor = ActorCursor::encode(data, ops.iter().map(T::obj_actor).map(&remap), false);
    let obj_ctr = UIntCursor::encode(data, ops.iter().map(T::obj_ctr), false);
    let key_actor = ActorCursor::encode(data, ops.iter().map(T::key_actor).map(&remap), false);
    let key_ctr = DeltaCursor::encode(data, ops.iter().map(T::key_ctr), false);
    let key_str = StrCursor::encode(data, ops.iter().map(T::key_str), false);
    let insert = BooleanCursor::encode(data, ops.iter().map(T::insert), true); // force
    let action = ActionCursor::encode(data, ops.iter().map(T::action), false);
    let value_meta = MetaCursor::encode(data, ops.iter().map(T::value_meta), false);
    let value = RawCursor::encode(data, ops.iter().map(T::value), false);
    let pred_count = UIntCursor::encode(data, ops.iter().map(T::pred_count), false);
    let pred_iter = ops.iter().map(T::pred).flat_map(|id| id.iter());
    let pred_actor_iter = pred_iter.clone().map(T::id_actor).map(&remap);
    let pred_actor = ActorCursor::encode(data, pred_actor_iter, false);
    let pred_ctr_iter = pred_iter.map(T::id_ctr);
    let pred_ctr = DeltaCursor::encode(data, pred_ctr_iter, false);
    let expand = BooleanCursor::encode(data, ops.iter().map(T::expand), false);
    let mark_name = StrCursor::encode(data, ops.iter().map(T::mark_name), false);

    let cols = ChangeOpsColumns {
        obj_actor,
        obj_ctr,
        key_actor,
        key_ctr,
        key_str,
        insert,
        action,
        value_meta,
        value,
        pred_count,
        pred_actor,
        pred_ctr,
        expand,
        mark_name,
    };

    cols.into()
}

// The many small mallocs in the remap_actors
// was causing some memory thrashing with dmalloc/wasm
// this structure allows for the vectors to be allocated
// once and reused (via trucate()) when creating a large number
// of changes (like on load)
#[derive(Debug, PartialEq)]
pub(crate) struct ActorMapper<'a> {
    seen_actors: Vec<bool>,
    pub(crate) mapping: Vec<Option<ActorIdx>>,
    actors: &'a [ActorId],
    other_actors: Vec<usize>,
}

impl<'a> ActorMapper<'a> {
    pub(crate) fn iter(&self) -> impl ExactSizeIterator<Item = ActorId> + '_ {
        self.other_actors.iter().map(|i| self.actors[*i].clone())
    }

    pub(crate) fn new(actors: &'a [ActorId]) -> ActorMapper<'a> {
        let len = actors.len();
        ActorMapper {
            seen_actors: vec![false; len],
            mapping: vec![None; len],
            actors,
            other_actors: vec![],
        }
    }

    pub(crate) fn reset(&mut self) {
        let len = self.actors.len();
        self.seen_actors.truncate(0);
        self.mapping.truncate(0);
        self.other_actors.truncate(0);

        self.seen_actors.resize(len, false);
        self.mapping.resize(len, None);
    }

    pub(crate) fn process_actor(&mut self, actor: usize) {
        self.seen_actors[actor] = true;
    }

    pub(crate) fn process_op<C>(&mut self, op: &C)
    where
        C: AsChangeOp,
    {
        if let Some(actor) = C::obj_actor(op).as_deref() {
            self.process_actor(usize::from(*actor))
        }
        if let Some(actor) = C::key_actor(op).as_deref() {
            self.process_actor(usize::from(*actor));
        }
        for id in C::pred(op) {
            self.process_actor(id.actor());
        }
    }

    pub(crate) fn build_mapping(&mut self, default_actor: Option<usize>) {
        let mut seen_index = 0;

        if let Some(actor) = default_actor {
            self.seen_actors[actor] = false;
            self.mapping[actor] = Some(ActorIdx(0));
            seen_index = 1;
        }

        for (index, seen) in self.seen_actors.iter().enumerate() {
            if *seen {
                self.other_actors.push(index);
                self.mapping[index] = Some(ActorIdx(seen_index));
                seen_index += 1;
            }
        }
    }

    fn remap_actors<C>(&mut self, ops: &[C], change_actor: usize)
    where
        C: AsChangeOp,
    {
        self.reset();

        for op in ops {
            self.process_op(op);
        }

        self.build_mapping(Some(change_actor));
    }
}
