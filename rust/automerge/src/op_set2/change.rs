use super::meta::MetaCursor;
use super::op::{AsChangeOp, OpBuilder};
use super::packer::{BooleanCursor, ColumnCursor, DeltaCursor, RawCursor, StrCursor, UIntCursor};
use super::types::{ActionCursor, ActorCursor, ActorIdx};
use crate::change_graph::ChangeGraph;
use crate::storage::change::{ChangeOpsColumns as ChangeOpsColumns2, Verified};
use crate::storage::{Change, ChunkType, Header};
use crate::types::ActorId;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::io::Write;
use std::marker::PhantomData;
use std::num::NonZero;
use std::ops::Range;

pub(crate) mod batch;
pub(crate) mod collector;

pub(crate) use collector::{BuildChangeMetadata, ChangeCollector, CollectedChanges};

pub(crate) fn build_change<T>(
    ops: &[T],
    meta: &BuildChangeMetadata<'_>,
    graph: &ChangeGraph,
    actors: &[ActorId],
) -> Change<'static, Verified>
where
    T: AsChangeOp,
{
    let value_size: usize = ops.iter().map(T::size_estimate).sum();

    let size_estimate = value_size + 25 * ops.len(); // highest in our beasiary is 23;

    let num_ops = ops.len();
    let mut col_data = Vec::with_capacity(size_estimate);

    let start_op = ops.first().map(T::op_id_ctr).unwrap_or(meta.max_op + 1);

    let (ops_meta, other_actors) = write_change_ops(ops, meta, actors, &mut col_data);

    let mut data = Vec::with_capacity(col_data.len());
    leb128::write::unsigned(&mut data, meta.deps.len() as u64).unwrap();

    // FIXME missing value here is changes out of order error
    let deps: Vec<_> = meta
        .deps
        .iter()
        .map(|i| *graph.index_to_hash(*i as usize).unwrap())
        .collect();

    for hash in &deps {
        data.write_all(hash.as_bytes()).unwrap();
    }

    length_prefixed_bytes(&actors[meta.actor], &mut data);

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

    let actor = actors[meta.actor].clone();

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

fn shift_range(range: Range<usize>, by: usize) -> Range<usize> {
    range.start + by..range.end + by
}

fn length_prefixed_bytes<B: AsRef<[u8]>>(b: B, out: &mut Vec<u8>) -> usize {
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
    meta: &BuildChangeMetadata<'_>,
    actors: &[ActorId],
    data: &mut Vec<u8>,
) -> (ChangeOpsColumns2, Vec<ActorId>)
where
    T: AsChangeOp,
{
    if ops.is_empty() {
        return (ChangeOpsColumns::default().into(), vec![]);
    }

    let (actor_map, actors) = remap_actors(ops, meta, actors);

    let _remap = move |actor: Option<Cow<'_, ActorIdx>>| {
        actor.map(|a| Cow::Owned(actor_map[usize::from(*a)].unwrap()))
    };

    let obj_actor = ActorCursor::encode(data, ops.iter().map(T::obj_actor).map(&_remap), false);
    let obj_ctr = UIntCursor::encode(data, ops.iter().map(T::obj_ctr), false);
    let key_actor = ActorCursor::encode(data, ops.iter().map(T::key_actor).map(&_remap), false);
    let key_ctr = DeltaCursor::encode(data, ops.iter().map(T::key_ctr), false);
    let key_str = StrCursor::encode(data, ops.iter().map(T::key_str), false);
    let insert = BooleanCursor::encode(data, ops.iter().map(T::insert), true); // force
    let action = ActionCursor::encode(data, ops.iter().map(T::action), false);
    let value_meta = MetaCursor::encode(data, ops.iter().map(T::value_meta), false);
    let value = RawCursor::encode(data, ops.iter().map(T::value), false);
    let pred_count = UIntCursor::encode(data, ops.iter().map(T::pred_count), false);
    let pred_iter = ops.iter().map(T::pred).flat_map(|id| id.iter());
    let pred_actor_iter = pred_iter.clone().map(T::id_actor).map(&_remap);
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

    (cols.into(), actors)
}

fn remap_actors<C>(
    ops: &[C],
    meta: &BuildChangeMetadata<'_>,
    actors: &[ActorId],
) -> (Vec<Option<ActorIdx>>, Vec<ActorId>)
where
    C: AsChangeOp,
{
    let mut seen_actors = vec![false; actors.len()];
    let mut mapping = vec![None; actors.len()];
    let mut seen_index = 0;

    for op in ops {
        if let Some(actor) = C::obj_actor(op).as_deref() {
            seen_actors[usize::from(*actor)] = true;
        }
        if let Some(actor) = C::key_actor(op).as_deref() {
            seen_actors[usize::from(*actor)] = true;
        }
        for id in C::pred(op) {
            seen_actors[id.actor()] = true;
        }
    }

    seen_actors[meta.actor] = false;
    mapping[meta.actor] = Some(ActorIdx(seen_index));

    let mut other_actors = Vec::with_capacity(actors.len());
    for (index, seen) in seen_actors.into_iter().enumerate() {
        if seen {
            other_actors.push(actors[index].clone());
            seen_index += 1;
            mapping[index] = Some(ActorIdx(seen_index));
        }
    }

    (mapping, other_actors)
}
