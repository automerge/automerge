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
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::marker::PhantomData;
use std::num::NonZero;
use std::ops::Range;

pub(crate) mod collector;

pub(crate) use collector::{ChangeCollector, CollectedChanges, ExtraChangeMetadata};

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
pub(crate) fn build_change<'a, T>(
    ops: &[T],
    meta: &ExtraChangeMetadata<'_>,
    graph: &ChangeGraph,
    actors: &[ActorId],
) -> Change<'static, Verified>
where
    T: CanBeChangeOp,
{
    let value_size: usize = ops
        .iter()
        .map(HasSizeEstimate::get)
        .sum();

    let size_estimate = value_size + 25 * ops.len(); // highest in our beasiary is 23;

    let num_ops = ops.len();
    let mut col_data = Vec::with_capacity(size_estimate);

    let start_op = ops
        .first()
        .map(HasOpIdCtr::get)
        .unwrap_or(meta.max_op + 1);

    let (ops_meta, other_actors) = write_change_ops(ops, &meta, actors, &mut col_data);

    let mut data = Vec::with_capacity(col_data.len());
    leb128::write::unsigned(&mut data, meta.deps.len() as u64).unwrap();

    // FIXME missing value here is changes out of order error
    let deps: Vec<_> = meta
        .deps
        .iter()
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
fn write_change_ops<'a, T>(
    ops: &[T],
    meta: &ExtraChangeMetadata<'_>,
    actors: &[ActorId],
    data: &mut Vec<u8>,
) -> (ChangeOpsColumns2, Vec<ActorId>)
where
    T: CanBeChangeOp,
{
    if ops.len() == 0 {
        return (ChangeOpsColumns::default().into(), vec![]);
    }

    let (actor_map, actors) = remap_actors(ops, meta, actors);

    let _remap = move |actor: Option<Cow<'_, ActorIdx>>| {
        actor.map(|a| Cow::Owned(actor_map[usize::from(*a)].unwrap()))
    };

    //let iter = ops.iter().map(|o| o.get());
    let iter = ops.iter();

    let obj_actor =
        encode_column::<ActorCursor, _>(data, iter.clone().map(HasObjActor::get).map(&_remap));
    let obj_ctr = encode_column::<UIntCursor, _>(data, iter.clone().map(HasObjCtr::get));
    let key_actor =
        encode_column::<ActorCursor, _>(data, iter.clone().map(HasKeyActor::get).map(&_remap));
    let key_ctr = encode_column::<DeltaCursor, _>(data, iter.clone().map(HasKeyCtr::get));
    let key_str = encode_column::<StrCursor, _>(data, iter.clone().map(HasKeyStr::get));
    let insert = force_encode_column::<BooleanCursor, _>(data, iter.clone().map(HasInsert::get));
    let action = encode_column::<ActionCursor, _>(data, iter.clone().map(HasAction::get));
    let value_meta = encode_column::<MetaCursor, _>(data, iter.clone().map(HasValueMeta::get));
    let value = encode_column::<RawCursor, _>(data, iter.clone().map(HasValue::get));
    let pred_count = encode_column::<UIntCursor, _>(data, iter.clone().map(HasPredCount::get));
    //let pred_actor = encode_column::<ActorCursor, _>(data, iter.clone().flat_map(_pred_actor).map(&_remap));
    let pred_actor = encode_column::<ActorCursor, _>(
        data,
        HasPred::iter(ops)
            .map(|id| Some(Cow::Owned(id.actoridx())))
            .map(&_remap),
    );
    //let pred_ctr = encode_column::<DeltaCursor, _>(data, iter.clone().flat_map(_pred_ctr));
    let pred_ctr = encode_column::<DeltaCursor, _>(
        data,
        HasPred::iter(ops).map(|id| Some(Cow::Owned(id.icounter()))),
    );
    //let pred_actor = encode_column::<ActorCursor, _>(data, iter.clone().flat_map(HasPredActor::get).map(&_remap));
    let expand = encode_column::<BooleanCursor, _>(data, iter.clone().map(HasExpand::get));
    let mark_name = encode_column::<StrCursor, _>(data, iter.clone().map(HasMarkName::get));

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
fn remap_actors<'a, T: CanBeChangeOp>(
    ops: &[T],
    meta: &ExtraChangeMetadata<'_>,
    actors: &[ActorId],
) -> (Vec<Option<ActorIdx>>, Vec<ActorId>) {
    let mut seen_actors = vec![false; actors.len()];
    let mut mapping = vec![None; actors.len()];
    let mut seen_index = 0;

    for op in ops {
        if let Some(actor) = HasObjActor::get(op).as_deref() {
            seen_actors[usize::from(*actor)] = true;
        }
        if let Some(actor) = HasKeyActor::get(op).as_deref() {
            seen_actors[usize::from(*actor)] = true;
        }
    }
    for id in HasPred::iter(ops) {
        seen_actors[id.actor()] = true;
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

trait CanBeChangeOp:
    HasObjActor
    + HasObjCtr
    + HasKeyActor
    + HasKeyCtr
    + HasKeyStr
    + HasInsert
    + HasAction
    + HasValue
    + HasValueMeta
    + HasPredCount
    + HasPred
    + HasExpand
    + HasMarkName
    + HasOpIdCtr
    + HasSizeEstimate
{
}

trait HasObjActor {
    fn get<'a>(op: &'a Self) -> Option<Cow<'a, ActorIdx>>;
}

trait HasObjCtr {
    fn get<'a>(op: &'a Self) -> Option<Cow<'a, u64>>;
}

trait HasOpIdCtr {
    fn get(op: &Self) -> u64;
}

trait HasSizeEstimate {
    fn get(op: &Self) -> usize;
}

trait HasKeyActor {
    fn get<'a>(op: &'a Self) -> Option<Cow<'a, ActorIdx>>;
}

trait HasKeyCtr {
    fn get<'a>(op: &'a Self) -> Option<Cow<'a, i64>>;
}

trait HasKeyStr {
    fn get<'a>(op: &'a Self) -> Option<Cow<'a, str>>;
}

trait HasInsert {
    fn get<'a>(op: &'a Self) -> Option<Cow<'a, bool>>;
}

trait HasAction {
    fn get<'a>(op: &'a Self) -> Option<Cow<'a, Action>>;
}

trait HasValue {
    fn get<'a>(op: &'a Self) -> Option<Cow<'a, [u8]>>;
}

trait HasValueMeta {
    fn get<'a>(op: &'a Self) -> Option<Cow<'a, ValueMeta>>;
}

trait HasPredCount {
    fn get<'a>(op: &'a Self) -> Option<Cow<'a, u64>>;
}

trait HasPred {
    type Iter<'a>: Iterator<Item = &'a OpId>
    where
        Self: 'a;
    fn iter(ops: &[Self]) -> Self::Iter<'_>
    where
        Self: Sized;
    fn get(op: &Self) -> &[OpId];
}

trait HasExpand {
    fn get<'a>(op: &'a Self) -> Option<Cow<'a, bool>>;
}

trait HasMarkName {
    fn get<'a>(op: &'a Self) -> Option<Cow<'a, str>>;
}

impl<T: HasObjActor> HasObjActor for Option<T> {
    fn get(op: &Self) -> Option<Cow<'_, ActorIdx>> {
        HasObjActor::get(op.as_ref()?)
    }
}

impl<T: HasObjCtr> HasObjCtr for Option<T> {
    fn get(op: &Self) -> Option<Cow<'_, u64>> {
        HasObjCtr::get(op.as_ref()?)
    }
}

impl<T: HasOpIdCtr> HasOpIdCtr for Option<T> {
    fn get(op: &Self) -> u64 {
       op.as_ref().map(HasOpIdCtr::get).unwrap_or(0)
    }
}

impl<T: HasSizeEstimate> HasSizeEstimate for Option<T> {
    fn get(op: &Self) -> usize {
       op.as_ref().map(HasSizeEstimate::get).unwrap_or(0)
    }
}

impl<T: HasKeyActor> HasKeyActor for Option<T> {
    fn get(op: &Self) -> Option<Cow<'_, ActorIdx>> {
        HasKeyActor::get(op.as_ref()?)
    }
}

impl<T: HasKeyCtr> HasKeyCtr for Option<T> {
    fn get(op: &Self) -> Option<Cow<'_, i64>> {
        HasKeyCtr::get(op.as_ref()?)
    }
}

impl<T: HasKeyStr> HasKeyStr for Option<T> {
    fn get(op: &Self) -> Option<Cow<'_, str>> {
        HasKeyStr::get(op.as_ref()?)
    }
}

impl<T: HasInsert> HasInsert for Option<T> {
    fn get(op: &Self) -> Option<Cow<'_, bool>> {
        HasInsert::get(op.as_ref()?)
    }
}

impl<T: HasAction> HasAction for Option<T> {
    fn get(op: &Self) -> Option<Cow<'_, Action>> {
        HasAction::get(op.as_ref()?)
    }
}

impl<T: HasValue> HasValue for Option<T> {
    fn get(op: &Self) -> Option<Cow<'_, [u8]>> {
        HasValue::get(op.as_ref()?)
    }
}

impl<T: HasValueMeta> HasValueMeta for Option<T> {
    fn get(op: &Self) -> Option<Cow<'_, ValueMeta>> {
        HasValueMeta::get(op.as_ref()?)
    }
}

impl<T: HasPredCount> HasPredCount for Option<T> {
    fn get(op: &Self) -> Option<Cow<'_, u64>> {
        HasPredCount::get(op.as_ref()?)
    }
}

impl<T: HasExpand> HasExpand for Option<T> {
    fn get(op: &Self) -> Option<Cow<'_, bool>> {
        HasExpand::get(op.as_ref()?)
    }
}

impl<T: HasMarkName> HasMarkName for Option<T> {
    fn get(op: &Self) -> Option<Cow<'_, str>> {
        HasMarkName::get(op.as_ref()?)
    }
}

impl<'a> CanBeChangeOp for OpBuilder3<'a> {}

impl<'a> CanBeChangeOp for Option<OpBuilder3<'a>> {}

impl<'a> HasObjActor for OpBuilder3<'a> {
    fn get<'b>(op: &'b OpBuilder3<'a>) -> Option<Cow<'b, ActorIdx>> {
        op.obj.actor().map(Cow::Owned)
    }
}

impl<'a> HasObjCtr for OpBuilder3<'a> {
    fn get<'b>(op: &'b OpBuilder3<'a>) -> Option<Cow<'b, u64>> {
        op.obj.counter().map(Cow::Owned)
    }
}

impl<'a> HasOpIdCtr for OpBuilder3<'a> {
    fn get(op: &OpBuilder3<'a>) -> u64 {
        op.id.counter()
    }
}

impl<'a> HasSizeEstimate for OpBuilder3<'a> {
    fn get(op: &OpBuilder3<'a>) -> usize {
        op.value.to_raw().map(|s| s.len() + 25).unwrap_or(0) // largest in our bestiary was 23
    }
}

impl<'a> HasKeyActor for OpBuilder3<'a> {
    fn get<'b>(op: &'b OpBuilder3<'a>) -> Option<Cow<'b, ActorIdx>> {
        op.key.actor().map(Cow::Owned)
    }
}

impl<'a> HasKeyCtr for OpBuilder3<'a> {
    fn get<'b>(op: &'b OpBuilder3<'a>) -> Option<Cow<'b, i64>> {
        op.key.icounter().map(Cow::Owned)
    }
}

impl<'a> HasKeyStr for OpBuilder3<'a> {
    fn get<'b>(op: &'b OpBuilder3<'a>) -> Option<Cow<'b, str>> {
        op.key.key_str()
    }
}

impl<'a> HasInsert for OpBuilder3<'a> {
    fn get<'b>(op: &'b OpBuilder3<'a>) -> Option<Cow<'b, bool>> {
        Some(Cow::Owned(op.insert))
    }
}

impl<'a> HasExpand for OpBuilder3<'a> {
    fn get<'b>(op: &'b OpBuilder3<'a>) -> Option<Cow<'b, bool>> {
        Some(Cow::Owned(op.expand))
    }
}

impl<'a> HasMarkName for OpBuilder3<'a> {
    fn get<'b>(op: &'b OpBuilder3<'a>) -> Option<Cow<'b, str>> {
        op.mark_name.clone()
    }
}

impl<'a> HasAction for OpBuilder3<'a> {
    fn get<'b>(op: &'b OpBuilder3<'a>) -> Option<Cow<'b, Action>> {
        Some(Cow::Owned(op.action))
    }
}

impl<'a> HasValue for OpBuilder3<'a> {
    fn get<'b>(op: &'b OpBuilder3<'a>) -> Option<Cow<'b, [u8]>> {
        op.value.to_raw()
    }
}

impl<'a> HasValueMeta for OpBuilder3<'a> {
    fn get<'b>(op: &'b OpBuilder3<'a>) -> Option<Cow<'b, ValueMeta>> {
        Some(Cow::Owned(ValueMeta::from(&op.value)))
    }
}

impl<'a> HasPredCount for OpBuilder3<'a> {
    fn get<'b>(op: &'b OpBuilder3<'a>) -> Option<Cow<'b, u64>> {
        Some(Cow::Owned(op.pred.len() as u64))
    }
}

use std::iter::FlatMap;
use std::slice::Iter;

impl<'a> HasPred for OpBuilder3<'a> {
    type Iter<'c> = FlatMap<Iter<'c, OpBuilder3<'a>>, &'c [OpId], fn(&'c OpBuilder3<'a>) -> &'c [OpId]> where Self: 'c;

    fn iter<'b>(ops: &'b [OpBuilder3<'a>]) -> Self::Iter<'b> {
        ops.iter().flat_map(|op| op.pred.as_slice())
    }

    fn get<'b>(op: &'b OpBuilder3<'a>) -> &'b [OpId] {
        op.pred.as_slice()
    }
}

/*
impl<'a> HasPred for Option<OpBuilder3<'a>> {
    type Iter<'c> = FlatMap<Iter<'c, Option<OpBuilder3<'a>>>, &'c [OpId],
        fn(&'c Option<OpBuilder3<'a>>) -> &'c [OpId]> where Self: 'c;
    fn iter<'b>(ops: &'b [Option<OpBuilder3<'a>>]) -> Self::Iter<'b> {
        ops.iter().flat_map(|op| op.as_ref().unwrap().pred.as_slice())
    }
}
*/

impl<'a, T: HasPred> HasPred for Option<T> {
    //type Iter<'c> = FlatMap<Iter<'c, Option<T>>, &'c [OpId], fn(&'c Option<T>) -> &'c [OpId]> where Self: 'c;
    type Iter<'c> = FlatMap<Iter<'c, Self>, &'c [OpId], fn(&'c Self) -> &'c [OpId]> where Self: 'c;

    fn iter<'b>(ops: &'b [Option<T>]) -> Self::Iter<'b> {
        //ops.iter().flat_map(|op| op.as_ref().unwrap().pred.as_slice())
        ops.iter().flat_map(HasPred::get)
    }

    fn get(op: &Option<T>) -> &[OpId] {
        HasPred::get(op.as_ref().unwrap())
    }
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
