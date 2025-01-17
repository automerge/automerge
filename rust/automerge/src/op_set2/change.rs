use super::meta::{MetaCursor, ValueMeta};
use super::op::{Op, OpBuilder2, OpBuilder3};
use super::packer::{
    BooleanCursor, ColumnCursor, DeltaCursor, Encoder, EncoderState, RawCursor, StrCursor,
    UIntCursor,
};
use super::types::{Action, ActionCursor, ActorCursor, ActorIdx};
use crate::change_graph::ChangeGraph;
use crate::storage::change::{ChangeOpsColumns as ChangeOpsColumns2, Verified};
use crate::storage::{Change, ChunkType, Header};
use crate::types::{ActorId, ChangeHash, OpId};
use fxhash::FxBuildHasher;
use itertools::Itertools;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::io::Write;
use std::marker::PhantomData;
use std::num::NonZero;
use std::ops::Range;

pub(crate) mod collector;

pub(crate) use collector::{ChangeCollector, CollectedChanges, ExtraChangeMetadata};

/*
const OBJ_COL_ID: ColumnId = ColumnId::new(0);
const KEY_COL_ID: ColumnId = ColumnId::new(1);
const INSERT_COL_ID: ColumnId = ColumnId::new(3);
const ACTION_COL_ID: ColumnId = ColumnId::new(4);
const VAL_COL_ID: ColumnId = ColumnId::new(5);
const PRED_COL_ID: ColumnId = ColumnId::new(7);
const EXPAND_COL_ID: ColumnId = ColumnId::new(9);
const MARK_NAME_COL_ID: ColumnId = ColumnId::new(10);
*/
pub(crate) trait AsOpBuilder3<'a> {
    fn get(&self) -> &OpBuilder3<'a>;
}

impl<'a> AsOpBuilder3<'a> for OpBuilder3<'a> {
    fn get(&self) -> &OpBuilder3<'a> {
        self
    }
}

impl<'a> AsOpBuilder3<'a> for Option<OpBuilder3<'a>> {
    fn get(&self) -> &OpBuilder3<'a> {
        self.as_ref().unwrap()
    }
}

#[inline(never)]
pub(crate) fn build_change<'a, T: AsOpBuilder3<'a>>(
    ops: &[T],
    meta: &ExtraChangeMetadata<'_>,
    //hashes: &HashMap<usize, ChangeHash, FxBuildHasher>,
    graph: &ChangeGraph,
    actors: &[ActorId],
) -> Change<'static, Verified> {
    let value_size: usize = ops
        .iter()
        .filter_map(|p| p.get().value.to_raw().map(|s| s.len()))
        .sum();
    let size_estimate = value_size + 25 * ops.len(); // highest in our beasiary is 23;

    let num_ops = ops.len();
    let mut col_data = Vec::with_capacity(size_estimate);

    let start_op = ops
        .first()
        .map(|p| p.get().id.counter())
        .unwrap_or(meta.max_op + 1);

    let (ops_meta, other_actors) = write_change_ops(ops, &meta, actors, &mut col_data);

    let mut data = Vec::with_capacity(col_data.len());
    leb128::write::unsigned(&mut data, meta.deps.len() as u64).unwrap();

    // FIXME missing value here is changes out of order error
    let deps: Vec<_> = meta
        .deps
        .iter()
        //.map(|i| hashes.get(&(*i as usize)).unwrap().clone())
        .map(|i| graph.index_to_hash(*i as usize).unwrap().clone())
        .collect();

    for hash in &deps {
        data.write_all(hash.as_bytes()).unwrap();
    }

    length_prefixed_bytes(&actors[usize::from(meta.actor)], &mut data);

    leb128::write::unsigned(&mut data, meta.seq).unwrap();
    leb128::write::unsigned(&mut data, start_op.into()).unwrap();
    leb128::write::signed(&mut data, meta.timestamp).unwrap();

    length_prefixed_bytes(meta.message_str(), &mut data);

    leb128::write::unsigned(&mut data, other_actors.len() as u64).unwrap();

    for actor in other_actors.iter() {
        length_prefixed_bytes(&actor, &mut data);
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

    let actor = actors[usize::from(meta.actor)].clone();

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

#[derive(Debug, Clone)]
pub(crate) struct ChangeBuilder<'a, T> {
    pub(crate) meta: ChangeMetadata<'a>,
    deps: Vec<T>,
    max_op: u64,
    index: usize,
    len: u64,
    pending_ops: BTreeSet<OpBuilder3<'a>>,
    writer: ChangeWriter<'a>,
}

impl<'a, T> PartialEq for ChangeBuilder<'a, T> {
    fn eq(&self, other: &ChangeBuilder<'a, T>) -> bool {
        self.meta == other.meta
    }
}

impl<'a> PartialOrd for OpBuilder3<'a> {
    fn partial_cmp(&self, other: &OpBuilder3<'a>) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for OpBuilder3<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl<'a> ChangeBuilder<'a, ChangeHash> {
    pub(crate) fn finish(mut self, actors: &[ActorId]) -> Change<'static, Verified> {
        self.flush();
        self.writer.finish(self.meta, self.deps, actors)
    }
}

impl<'a, T> ChangeBuilder<'a, T> {
    pub(crate) fn index(&self) -> usize {
        self.index
    }

    pub(crate) fn max_op(&self) -> u64 {
        self.max_op
    }

    pub(crate) fn seq(&self) -> u64 {
        self.meta.seq
    }

    pub(crate) fn new(meta: ChangeMetadata<'a>, deps: Vec<T>, max_op: u64, index: usize) -> Self {
        ChangeBuilder {
            meta,
            deps,
            max_op,
            len: 0,
            index,
            pending_ops: BTreeSet::default(),
            writer: ChangeWriter::default(),
        }
    }

    #[inline(never)]
    pub(crate) fn finish_where<F>(mut self, actors: &[ActorId], f: F) -> Change<'static, Verified>
    where
        F: Fn(Vec<T>) -> Vec<ChangeHash>,
    {
        if let Some(o) = self.pending_ops.first() {
            if self.len == 0 {
                // documents exist in the wild where start_op is wrong here - not sure why
                // see automerge-battery embark.automerge
                self.meta.start_op = NonZero::new(o.id.counter()).unwrap();
            }
        }
        self.flush();
        //println!("pending_ops={:?} max={} start={} len={}", self.pending_ops, self.max_op, self.meta.start_op, self.len);
        assert!(self.pending_ops.is_empty());
        self.writer.finish(self.meta, f(self.deps), actors)
    }

    #[inline(never)]
    pub(crate) fn flush(&mut self) {
        while let Some(pending) = self.pending_ops.first() {
            if pending.id.counter() != self.meta.start_op.get() + self.len {
                break;
            }
            let pending = self.pending_ops.pop_first().unwrap();
            self.writer.append3(pending);
            self.len += 1;
        }
    }

    fn start_op(&self) -> u64 {
        self.meta.start_op.get()
    }

    fn contains(&self, id: &OpId) -> bool {
        id.actoridx() == self.meta.actor
            && id.counter() >= self.start_op()
            && id.counter() <= self.max_op
    }

    #[inline(never)]
    pub(crate) fn append(&mut self, op: Op<'a>, pred: Vec<OpId>) {
        if self.contains(&op.id) {
            if op.id.counter() == self.meta.start_op.get() + self.len {
                self.writer.append(op, pred);
                self.len += 1;
                self.flush();
            } else {
                self.pending_ops.insert(op.build3(pred));
            }
        }
    }
}

#[derive(Debug, Default, Clone)]
pub(crate) struct ChangeWriter<'a> {
    actors: HashSet<ActorIdx, FxBuildHasher>,
    obj_actor: Encoder<'a, ActorCursor>,
    obj_ctr: Encoder<'a, UIntCursor>,
    key_actor: Encoder<'a, ActorCursor>,
    key_ctr: Encoder<'a, DeltaCursor>,
    key_str: Encoder<'a, StrCursor>,
    insert: Encoder<'a, BooleanCursor>,
    action: Encoder<'a, ActionCursor>,
    value_meta: Encoder<'a, MetaCursor>,
    value: Encoder<'a, RawCursor>,
    pred_count: Encoder<'a, UIntCursor>,
    pred_actor: Encoder<'a, ActorCursor>,
    pred_ctr: Encoder<'a, DeltaCursor>,
    expand: Encoder<'a, BooleanCursor>,
    mark_name: Encoder<'a, StrCursor>,
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

#[derive(Debug, PartialEq, Clone)]
pub(crate) struct ChangeMetadata<'a> {
    pub(crate) actor: ActorIdx,
    pub(crate) seq: u64,
    pub(crate) start_op: NonZero<u64>,
    pub(crate) timestamp: i64,
    pub(crate) message: Option<Cow<'a, str>>,
    pub(crate) extra_bytes: Cow<'a, [u8]>,
}

#[derive(Debug, Clone)]
pub(crate) struct DocChangeMetadata<'a> {
    pub(crate) actor: ActorIdx,
    pub(crate) deps: Vec<u64>,
    pub(crate) seq: u64,
    pub(crate) start_op: NonZero<u64>,
    pub(crate) timestamp: i64,
    pub(crate) message: Option<Cow<'a, str>>,
    pub(crate) extra_bytes: Cow<'a, [u8]>,
}

impl<'a> DocChangeMetadata<'a> {
    /*
        fn x(self, known_hashes: HashMap<usize, ChangeHash, FxBuildHasher>) -> ChangeMetadata<'a> {
            let deps = self
                .deps
                .iter()
                .map(|index| known_hashes.get(&(*index as usize)).copied().unwrap())
                .collect();
            ChangeMetadata {
                actor: self.actor,
                seq: self.seq,
                start_op: self.start_op,
                timestamp: self.timestamp,
                message: self.message,
                extra_bytes: self.extra_bytes,
            }
        }
    */
}

impl<'a> ChangeMetadata<'a> {
    fn message_str(&self) -> &str {
        self.message.as_deref().unwrap_or("")
    }
}

impl<'a> ChangeWriter<'a> {
    fn len(&self) -> usize {
        self.obj_actor.len
    }

    pub(crate) fn finish(
        self,
        meta: ChangeMetadata<'_>,
        deps: Vec<ChangeHash>,
        actors: &[ActorId],
    ) -> Change<'static, Verified> {
        let num_ops = self.len();
        let mut col_data = Vec::new();
        //let actors = change_actors::ChangeActors::new(self.actor.value, ops)?;
        //let cols = ChangeOpsColumns::encode(actors.iter(), &mut col_data);
        let (ops_meta, other_actors) = self.write_change_ops(&meta, actors, &mut col_data);

        let mut data = Vec::with_capacity(col_data.len());
        leb128::write::unsigned(&mut data, deps.len() as u64).unwrap();
        for dep in &deps {
            // FIXME extend_from_slice
            data.write_all(dep.as_bytes()).unwrap();
        }

        length_prefixed_bytes(&actors[usize::from(meta.actor)], &mut data);

        leb128::write::unsigned(&mut data, meta.seq).unwrap();
        leb128::write::unsigned(&mut data, meta.start_op.into()).unwrap();
        leb128::write::signed(&mut data, meta.timestamp).unwrap();

        length_prefixed_bytes(meta.message_str(), &mut data);

        leb128::write::unsigned(&mut data, other_actors.len() as u64).unwrap();

        for actor in other_actors.iter() {
            length_prefixed_bytes(&actor, &mut data);
        }

        ops_meta.raw_columns().write(&mut data);
        let ops_data_start = data.len();
        let ops_data = ops_data_start..(ops_data_start + col_data.len());

        data.extend(col_data);
        let extra_bytes = data.len()..(data.len() + meta.extra_bytes.len());
        if !meta.extra_bytes.is_empty() {
            data.extend(meta.extra_bytes.as_ref());
        }

        let header = Header::new(ChunkType::Change, &data);

        let mut bytes = Vec::with_capacity(header.len() + data.len());
        header.write(&mut bytes);
        bytes.extend(data);

        let ops_data = shift_range(ops_data, header.len());
        let extra_bytes = shift_range(extra_bytes, header.len());

        let actor = actors[usize::from(meta.actor)].clone();

        Change {
            bytes: Cow::Owned(bytes),
            header,
            dependencies: deps,
            actor,
            other_actors,
            seq: meta.seq,
            start_op: meta.start_op,
            timestamp: meta.timestamp,
            message: meta.message.map(|s| s.into_owned()),
            ops_meta,
            ops_data,
            extra_bytes,
            num_ops,
            _phantom: PhantomData,
        }
    }

    #[inline(never)]
    fn write_change_ops(
        mut self,
        meta: &ChangeMetadata<'_>,
        actors: &[ActorId],
        data: &mut Vec<u8>,
    ) -> (ChangeOpsColumns2, Vec<ActorId>) {
        if self.len() == 0 {
            return (ChangeOpsColumns::default().into(), vec![]);
        }

        let (map2, actors) = self.remap_actors(meta, actors);
        let obj_actor = self
            .obj_actor
            //.into_column_data()
            //.and_remap(|v| map.get(&v?).cloned())
            //.write_unless_empty(data);
            .write_and_remap_unless_empty(data, |v| map2.get(&v));
        let obj_ctr = self.obj_ctr.write_unless_empty(data);
        let key_actor = self
            .key_actor
            //.into_column_data()
            //.and_remap(|v| map.get(&v?).cloned())
            //.write_unless_empty(data);
            .write_and_remap_unless_empty(data, |v| map2.get(&v));
        let key_ctr = self.key_ctr.write_unless_empty(data);
        let key_str = self.key_str.write_unless_empty(data);
        //let x = data.len();
        let insert = self.insert.write(data);
        //println!(" X1 INSERT = {:?}", &data[x..]);
        let action = self.action.write_unless_empty(data);
        let value_meta = self.value_meta.write_unless_empty(data);
        let value = self.value.write_unless_empty(data);
        let pred_count = self.pred_count.write_unless_empty(data);
        let pred_actor = self
            .pred_actor
            //.into_column_data()
            //.and_remap(|v| map.get(&v?).cloned())
            //.write_unless_empty(data);
            .write_and_remap_unless_empty(data, |v| map2.get(&v));
        let pred_ctr = self.pred_ctr.write_unless_empty(data);
        let expand = self.expand.write_unless_empty(data);
        let mark_name = self.mark_name.write_unless_empty(data);

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

    pub(crate) fn append(&mut self, op: Op<'a>, pred: Vec<OpId>) {
        self.record_actor(op.obj.actor());
        self.record_actor(op.key.actor());
        self.obj_actor.append(op.obj.actor());
        self.obj_ctr.append(op.obj.counter());
        self.key_actor.append(op.key.actor());
        self.key_ctr.append(op.key.icounter());
        self.key_str.append(op.key.key_str());
        self.insert.append(Some(op.insert));
        self.action.append(Some(op.action));
        self.value_meta.append(Some(ValueMeta::from(&op.value)));
        self.value.append_bytes(op.value.to_raw());
        self.expand.append(Some(op.expand));
        self.mark_name.append(op.mark_name);
        self.pred_count.append(Some(pred.len() as u64));
        for p in pred {
            self.record_actor(Some(p.actoridx()));
            self.pred_actor.append(Some(p.actoridx()));
            self.pred_ctr.append(Some(p.icounter()));
        }
    }

    pub(crate) fn append2(&mut self, op: OpBuilder2) {
        self.record_actor(op.obj.id.actor());
        self.record_actor(op.key.actor());
        self.obj_actor.append(op.obj.id.actor());
        self.obj_ctr.append(op.obj.id.counter());
        self.key_actor.append(op.key.actor());
        self.key_ctr.append(op.key.icounter());
        self.key_str.append(op.key.key_str());
        self.insert.append(Some(op.insert));
        self.action.append(Some(op.action.action()));
        self.value_meta
            .append(Some(ValueMeta::from(op.action.value().as_ref())));
        self.value
            .append_bytes(op.action.to_raw().map(|x| Cow::Owned(x.to_vec())));
        self.expand.append(Some(op.action.expand()));
        self.mark_name
            .append(op.action.mark_name().map(|s| Cow::Owned(s.to_owned())));
        self.pred_count.append(Some(op.pred.len() as u64));
        for p in op.pred {
            self.record_actor(Some(p.actoridx()));
            self.pred_actor.append(Some(p.actoridx()));
            self.pred_ctr.append(Some(p.icounter()));
        }
    }

    pub(crate) fn append3(&mut self, op: OpBuilder3<'a>) {
        self.record_actor(op.obj.actor());
        self.record_actor(op.key.actor());
        self.obj_actor.append(op.obj.actor());
        self.obj_ctr.append(op.obj.counter());
        self.key_actor.append(op.key.actor());
        self.key_ctr.append(op.key.icounter());
        self.key_str.append(op.key.key_str());
        self.insert.append(Some(op.insert));
        self.action.append(Some(op.action));
        self.value_meta.append(Some(ValueMeta::from(&op.value)));
        self.value.append_bytes(op.value.to_raw());
        self.expand.append(Some(op.expand));
        self.mark_name.append(op.mark_name);
        self.pred_count.append(Some(op.pred.len() as u64));
        for p in op.pred {
            self.record_actor(Some(p.actoridx()));
            self.pred_actor.append(Some(p.actoridx()));
            self.pred_ctr.append(Some(p.icounter()));
        }
    }

    #[allow(dead_code)]
    fn record_actor(&mut self, actor: Option<ActorIdx>) {
        if let Some(a) = actor {
            self.actors.insert(a);
        }
    }

    #[inline(never)]
    fn remap_actors(
        &mut self,
        meta: &ChangeMetadata<'_>,
        actors: &[ActorId],
    ) -> (HashMap<ActorIdx, ActorIdx, FxBuildHasher>, Vec<ActorId>) {
        let mut seen_actors = std::mem::take(&mut self.actors);
        let mut mapping = HashMap::default();
        let mut index = 0;

        seen_actors.remove(&meta.actor);
        mapping.insert(meta.actor, ActorIdx(index));

        let mut other_actors = Vec::with_capacity(seen_actors.len());
        for actor in seen_actors.into_iter().sorted() {
            other_actors.push(actors[usize::from(actor)].clone());
            index += 1;
            mapping.insert(actor, ActorIdx(index));
        }

        (mapping, other_actors)
    }
}

fn shift_range(range: Range<usize>, by: usize) -> Range<usize> {
    range.start + by..range.end + by
}

fn length_prefixed_bytes<B: AsRef<[u8]>>(b: B, out: &mut Vec<u8>) -> usize {
    let prefix_len = leb128::write::unsigned(out, b.as_ref().len() as u64).unwrap();
    out.write_all(b.as_ref()).unwrap();
    prefix_len + b.as_ref().len()
}

impl<'a> PartialEq for OpBuilder3<'a> {
    fn eq(&self, other: &OpBuilder3<'a>) -> bool {
        self.id == other.id
    }
}

impl<'a> Eq for OpBuilder3<'a> {}

#[inline(never)]
fn write_change_ops<'a, T: AsOpBuilder3<'a>>(
    ops: &[T],
    meta: &ExtraChangeMetadata<'_>,
    actors: &[ActorId],
    data: &mut Vec<u8>,
) -> (ChangeOpsColumns2, Vec<ActorId>) {
    if ops.len() == 0 {
        return (ChangeOpsColumns::default().into(), vec![]);
    }

    let (actor_map, actors) = remap_actors(ops, meta, actors);

    let _remap = move |actor: Option<Cow<'_, ActorIdx>>| {
        actor.map(|a| Cow::Owned(actor_map[usize::from(*a)].unwrap()))
    };

    let iter = ops.iter().map(|o| o.get());

    let obj_actor =
        encode_column::<ActorCursor, _>(data, iter.clone().map(_obj_actor).map(&_remap));
    let obj_ctr = encode_column::<UIntCursor, _>(data, iter.clone().map(_obj_ctr));
    let key_actor =
        encode_column::<ActorCursor, _>(data, iter.clone().map(_key_actor).map(&_remap));
    let key_ctr = encode_column::<DeltaCursor, _>(data, iter.clone().map(_key_ctr));
    let key_str = encode_column::<StrCursor, _>(data, iter.clone().map(_key_str));
    let insert = force_encode_column::<BooleanCursor, _>(data, iter.clone().map(_insert));
    let action = encode_column::<ActionCursor, _>(data, iter.clone().map(_action));
    let value_meta = encode_column::<MetaCursor, _>(data, iter.clone().map(_value_meta));
    let value = encode_column::<RawCursor, _>(data, iter.clone().map(_value));
    let pred_count = encode_column::<UIntCursor, _>(data, iter.clone().map(_pred_count));
    let pred_actor =
        encode_column::<ActorCursor, _>(data, iter.clone().flat_map(_pred_actor).map(&_remap));
    let pred_ctr = encode_column::<DeltaCursor, _>(data, iter.clone().flat_map(_pred_ctr));
    let expand = encode_column::<BooleanCursor, _>(data, iter.clone().map(_expand));
    let mark_name = encode_column::<StrCursor, _>(data, iter.clone().map(_mark_name));

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

#[inline(never)]
fn remap_actors<'a, T: AsOpBuilder3<'a>>(
    ops: &[T],
    meta: &ExtraChangeMetadata<'_>,
    actors: &[ActorId],
) -> (Vec<Option<ActorIdx>>, Vec<ActorId>) {
    let mut seen_actors = vec![false; actors.len()];
    let mut mapping = vec![None; actors.len()];
    let mut seen_index = 0;

    for op in ops {
        if let Some(actor) = op.get().obj.actor() {
            seen_actors[usize::from(actor)] = true;
        }
        if let Some(actor) = op.get().key.actor() {
            seen_actors[usize::from(actor)] = true;
        }
        for id in &op.get().pred {
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

fn force_encode_column<'a, C, I>(out: &mut Vec<u8>, values: I) -> Range<usize>
where
    C: ColumnCursor,
    I: Iterator<Item = Option<Cow<'a, C::Item>>>,
    C::Item: 'a,
{
    //_encode_column::<C, I>(out, values, true)
    C::encode::<I>(out, values, true)
}

fn encode_column<'a, C, I>(out: &mut Vec<u8>, values: I) -> Range<usize>
where
    C: ColumnCursor,
    I: Iterator<Item = Option<Cow<'a, C::Item>>>,
    C::Item: 'a,
{
    //_encode_column::<C, I>(out, values, false)
    C::encode::<I>(out, values, false)
}

fn _encode_column<'a, C, I>(out: &mut Vec<u8>, values: I, force: bool) -> Range<usize>
where
    C: ColumnCursor,
    I: Iterator<Item = Option<Cow<'a, C::Item>>>,
    C::Item: 'a,
{
    let start = out.len();
    let mut state = C::State::default();
    for v in values {
        state.append(out, v);
    }
    if !force && out.len() == start && state.is_empty() {
        out.truncate(start);
        return start..start;
    }
    state.flush(out);
    let end = out.len();
    start..end
}

fn _obj_actor<'a>(op: &OpBuilder3<'a>) -> Option<Cow<'a, ActorIdx>> {
    op.obj.actor().map(Cow::Owned)
}

fn _obj_ctr<'a>(op: &OpBuilder3<'a>) -> Option<Cow<'a, u64>> {
    op.obj.counter().map(Cow::Owned)
}

fn _key_actor<'a>(op: &OpBuilder3<'a>) -> Option<Cow<'a, ActorIdx>> {
    op.key.actor().map(Cow::Owned)
}

fn _key_ctr<'a>(op: &OpBuilder3<'a>) -> Option<Cow<'a, i64>> {
    op.key.icounter().map(Cow::Owned)
}

fn _key_str<'a>(op: &OpBuilder3<'a>) -> Option<Cow<'a, str>> {
    op.key.key_str()
}

fn _insert<'a>(op: &OpBuilder3<'a>) -> Option<Cow<'a, bool>> {
    Some(Cow::Owned(op.insert))
}

fn _action<'a>(op: &OpBuilder3<'a>) -> Option<Cow<'a, Action>> {
    Some(Cow::Owned(op.action))
}

fn _value_meta<'a>(op: &OpBuilder3<'a>) -> Option<Cow<'a, ValueMeta>> {
    Some(Cow::Owned(ValueMeta::from(&op.value)))
}

fn _value<'a>(op: &OpBuilder3<'a>) -> Option<Cow<'a, [u8]>> {
    op.value.to_raw()
}

fn _pred_count<'a>(op: &OpBuilder3<'a>) -> Option<Cow<'a, u64>> {
    Some(Cow::Owned(op.pred.len() as u64))
}

fn _pred_actor<'a, 'b>(
    op: &'b OpBuilder3<'a>,
) -> impl Iterator<Item = Option<Cow<'static, ActorIdx>>> + 'b {
    op.pred.iter().map(|id| Some(Cow::Owned(id.actoridx())))
}

fn _pred_ctr<'a, 'b>(
    op: &'b OpBuilder3<'a>,
) -> impl Iterator<Item = Option<Cow<'static, i64>>> + 'b {
    op.pred.iter().map(|id| Some(Cow::Owned(id.icounter())))
}

fn _expand<'a>(op: &OpBuilder3<'a>) -> Option<Cow<'a, bool>> {
    Some(Cow::Owned(op.expand))
}

fn _mark_name<'a>(op: &OpBuilder3<'a>) -> Option<Cow<'a, str>> {
    op.mark_name.clone()
}
