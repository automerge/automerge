use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::Write;
use std::marker::PhantomData;
use std::mem::size_of;
use std::num::NonZero;

use super::super::meta::MetaCursor;
use super::super::types::{ActionCursor, ActorCursor, ActorIdx};
use super::{length_prefixed_bytes, shift_range};
use super::{ActorMapper, ChangeOpsColumns};

use hexane::{BooleanCursor, DeltaCursor, Encoder, StrCursor, UIntCursor};

use crate::change_graph::ChangeGraph;
use crate::error::AutomergeError;
use crate::op_set2::change::{write_change_ops, GetHash};
use crate::op_set2::op_set::IndexBuilder;
use crate::storage::bundle::BundleChange;
use crate::storage::change::{Change as StoredChange, Verified};
use crate::storage::load::change_collector::Error;
use crate::storage::{ChunkType, Header};
use crate::{
    change::Change,
    op_set2::{ChangeMetadata, KeyRef, Op, OpBuilder, OpSet, ReadOpError},
    types::{ActorId, ChangeHash, ObjId, OpId},
};

#[derive(Debug, thiserror::Error)]
#[error("out of memory")]
pub(crate) struct OutOfMemory;

pub(crate) struct IndexedChangeCollector<'a> {
    pub(crate) index: &'a mut IndexBuilder,
    pub(crate) collector: ChangeCollector<'a>,
}

impl<'a> IndexedChangeCollector<'a> {
    pub(crate) fn process_ops(&mut self, op_set: &'a OpSet) -> Result<(), ReadOpError> {
        let mut iter = op_set.iter();

        while let Some(op) = iter.try_next()? {
            let op_id = op.id;
            let op_is_counter = op.is_counter();
            let op_succ = op.succ();

            self.process_op(op);

            for id in op_succ {
                self.index.process_succ(op_is_counter, id);
                self.collector.process_succ(op_id, id);
            }
        }
        Ok(())
    }

    pub(crate) fn build_changegraph(self, op_set: &OpSet) -> Result<CollectedChanges, Error> {
        self.collector.build_changegraph(op_set)
    }

    pub(crate) fn process_op(&mut self, op: Op<'a>) {
        let next = Some((op.obj, op.elemid_or_key()));
        let flush = self.collector.last != next;
        if flush {
            self.index.flush();
        }
        self.index.process_op(&op);
        self.collector.process_op_internal(op, flush);
        if flush {
            self.collector.last = next;
        }
    }
}

// TODO: the most memory efficient version of the ChangeCollector
// would consume a change_meta_iter and an op_iter and itself be an iterator
// that emits Change's.  This iterator could then be fed into a
// ChangeGraph::from_iter()
//
// change_iter would only be read from when an op is found with no associated
// change builder.  When a change_builder gets its last op and is emitted
//
// As ChangeBuilders are only allocated as needed and freed when finished then
// no aditional memory would need to be allocated if all ops in the document are
// in change order.  As all objects are in change order and all ops within a
// register are in change order, most ops should fit this pattern.
//
// The worst case scenario for this implementation would be the current memory usage

pub(crate) struct ChangeCollector<'a> {
    mapper: ActorMapper<'a>,
    changes: Vec<BuildChangeMetadata<'a>>,
    pub(crate) builders: Vec<ChangeBuilder<'a>>,
    last: Option<(ObjId, KeyRef<'a>)>,
    preds: HashMap<OpId, Vec<OpId>>,
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) struct BuildChangeMetadata<'a> {
    pub(crate) actor: usize,
    pub(crate) seq: u64,
    pub(crate) max_op: u64,
    pub(crate) timestamp: i64,
    pub(crate) message: Option<Cow<'a, str>>,
    pub(crate) deps: Vec<u64>,
    pub(crate) extra: Cow<'a, [u8]>,
    pub(crate) start_op: u64,
    pub(crate) builder: usize,
}

impl BuildChangeMetadata<'_> {
    pub(crate) fn num_ops(&self) -> usize {
        (1 + self.max_op - self.start_op) as usize
    }

    pub(crate) fn message_str(&self) -> &str {
        self.message.as_deref().unwrap_or_default()
    }
}

#[derive(Debug)]
pub(crate) struct ChangeBuilder<'a> {
    actor: usize,
    seq: u64,
    change: usize,
    start_op: u64,
    encoder: OpEncoderStrategy<'a>,
}

#[derive(Clone, Debug, Default)]
enum OpEncoderStrategy<'a> {
    Ops(VecEncoder<'a>),
    Enc(Box<ProgressiveEncoder<'a>>),
    #[default]
    Null, // this can be removed if we can consume the builder on finnish
}

impl<'a> OpEncoderStrategy<'a> {
    fn new(num_ops: usize) -> Self {
        let ops_size = num_ops * size_of::<Option<OpBuilder<'_>>>();
        let enc_size = size_of::<ProgressiveEncoder<'_>>();
        if ops_size > enc_size {
            Self::Enc(Box::new(ProgressiveEncoder::new(num_ops as u64)))
        } else {
            Self::Ops(VecEncoder::new(num_ops as u64))
        }
    }

    fn try_new(num_ops: usize) -> Result<Self, OutOfMemory> {
        if CAN_OOM {
            // VecEncoder is not ideal for large changes
            // but allocates all its memory up front which allows us to
            // detect OOM errors
            Ok(Self::Ops(VecEncoder::try_new(num_ops as u64)?))
        } else {
            Ok(Self::new(num_ops))
        }
    }

    fn num_ops(&self) -> u64 {
        match self {
            Self::Ops(v) => v.num_ops(),
            Self::Enc(e) => e.num_ops,
            Self::Null => 0,
        }
    }

    fn add(&mut self, index: usize, op: OpBuilder<'a>) {
        match self {
            Self::Ops(v) => v.add(index, op),
            Self::Enc(e) => e.add(index, op),
            Self::Null => (),
        }
    }

    fn into_change_cols(
        self,
        change: &BuildChangeMetadata<'_>,
        mapper: &mut ActorMapper<'_>,
    ) -> Result<ChangeCols, Error> {
        mapper.reset();
        match self {
            Self::Ops(mut v) => v.finish(change, mapper),
            Self::Enc(e) => Ok(e.finish(change, mapper)),
            Self::Null => Err(Error::InvalidState),
        }
    }

    fn finish<G>(
        self,
        change: &BuildChangeMetadata<'_>,
        graph: &G,
        mapper: &mut ActorMapper<'_>,
    ) -> Result<StoredChange<'static, Verified>, Error>
    where
        G: GetHash,
    {
        let cols = self.into_change_cols(change, mapper)?;
        let num_ops = cols.num_ops as usize;
        let start_op = cols.start_op.unwrap_or(change.max_op + 1);
        let ops_meta = cols.meta;
        let col_data = cols.data;
        let actor = cols.actor;
        let other_actors = cols.other_actors;

        let mut data = Vec::with_capacity(col_data.len());

        leb128::write::unsigned(&mut data, change.deps.len() as u64).unwrap();

        // FIXME missing value here is changes out of order error
        let deps: Vec<_> = change
            .deps
            .iter()
            .map(|i| graph.get_hash(*i as usize).unwrap())
            .collect();

        for hash in &deps {
            data.write_all(hash.as_bytes()).unwrap();
        }

        length_prefixed_bytes(&actor, &mut data);

        leb128::write::unsigned(&mut data, change.seq).unwrap();
        leb128::write::unsigned(&mut data, start_op).unwrap();
        leb128::write::signed(&mut data, change.timestamp).unwrap();

        length_prefixed_bytes(change.message_str(), &mut data);

        leb128::write::unsigned(&mut data, other_actors.len() as u64).unwrap();

        for actor in other_actors.iter() {
            length_prefixed_bytes(actor, &mut data);
        }

        ops_meta.raw_columns().write(&mut data);

        let ops_data_start = data.len();
        let ops_data = ops_data_start..(ops_data_start + col_data.len());

        data.extend(col_data);
        let extra_bytes = data.len()..(data.len() + change.extra.len());
        if !change.extra.is_empty() {
            data.extend(change.extra.as_ref());
        }

        let header = Header::new(ChunkType::Change, &data);

        let mut bytes = Vec::with_capacity(header.len() + data.len());
        header.write(&mut bytes);
        bytes.extend(data);

        let ops_data = shift_range(ops_data, header.len());
        let extra_bytes = shift_range(extra_bytes, header.len());

        Ok(StoredChange {
            bytes: Cow::Owned(bytes),
            header,
            dependencies: deps,
            actor,
            other_actors,
            seq: change.seq,
            start_op: NonZero::new(start_op).unwrap(),
            timestamp: change.timestamp,
            message: change.message.as_ref().map(|s| s.to_string()),
            ops_meta,
            ops_data,
            extra_bytes,
            num_ops,
            _phantom: PhantomData,
        })
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct VecEncoder<'a> {
    data: Vec<Option<OpBuilder<'a>>>,
}

impl<'a> VecEncoder<'a> {
    fn new(num_ops: u64) -> Self {
        Self {
            data: vec![None; num_ops as usize],
        }
    }

    fn try_new(num_ops: u64) -> Result<Self, OutOfMemory> {
        let num_ops = num_ops as usize;
        let mut data = Vec::new();
        data.try_reserve(num_ops).map_err(|_| OutOfMemory)?;
        data.extend(std::iter::repeat_n(None, num_ops));
        Ok(Self { data })
    }

    fn num_ops(&self) -> u64 {
        self.data.len() as u64
    }

    fn add(&mut self, index: usize, op: OpBuilder<'a>) {
        assert!(self.data[index].is_none());
        self.data[index] = Some(op);
    }

    fn finish(
        &mut self,
        change: &BuildChangeMetadata<'_>,
        mapper: &mut ActorMapper<'_>,
    ) -> Result<ChangeCols, Error> {
        let start_pos = self.data.iter().position(|op| op.is_some()).unwrap_or(0);
        let ops = &self.data[start_pos..];
        if ops.iter().any(|o| o.is_none()) {
            return Err(Error::MissingOps);
        }

        if let Some(Some(last)) = ops.last() {
            assert_eq!(last.id.counter(), change.max_op);
        }

        let mut data = vec![];
        let meta = write_change_ops(ops, change.actor, &mut data, mapper);
        let actor = mapper.actors[change.actor].clone();
        let other_actors = mapper.iter().collect();

        Ok(ChangeCols {
            actor,
            other_actors,
            start_op: ops
                .first()
                .and_then(|op| op.as_ref())
                .map(|op| op.id.counter()),
            num_ops: ops.len() as u64,
            data,
            meta,
        })
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ProgressiveEncoder<'a> {
    pub(crate) len: usize,
    pub(crate) start_op: Option<u64>,
    pub(crate) num_ops: u64,
    actors: Vec<bool>,
    queue: BTreeMap<usize, OpBuilder<'a>>,
    obj_actor: Encoder<'a, ActorCursor>,
    obj_ctr: Encoder<'a, UIntCursor>,
    key_actor: Encoder<'a, ActorCursor>,
    key_ctr: Encoder<'a, DeltaCursor>,
    key_str: Encoder<'a, StrCursor>,
    insert: Encoder<'a, BooleanCursor>,
    action: Encoder<'a, ActionCursor>,
    value_meta: Encoder<'a, MetaCursor>,
    value: Vec<u8>,
    pred_count: Encoder<'a, UIntCursor>,
    pred_actor: Encoder<'a, ActorCursor>,
    pred_ctr: Encoder<'a, DeltaCursor>,
    expand: Encoder<'a, BooleanCursor>,
    mark_name: Encoder<'a, StrCursor>,
}

impl<'a> ProgressiveEncoder<'a> {
    fn new(num_ops: u64) -> Self {
        ProgressiveEncoder {
            num_ops,
            ..Default::default()
        }
    }

    fn process_actor(&mut self, actor: usize) {
        if actor >= self.actors.len() {
            self.actors.resize(actor + 1, false);
        }
        self.actors[actor] = true;
    }
    fn process_op(&mut self, op: &OpBuilder<'a>) {
        if let Some(actor) = op.obj.actor() {
            self.process_actor(usize::from(actor));
        }
        if let Some(actor) = op.key.actor() {
            self.process_actor(usize::from(actor));
        }
        for id in &op.pred {
            self.process_actor(id.actor());
        }
    }

    fn add(&mut self, index: usize, op: OpBuilder<'a>) {
        self.process_op(&op);
        if index == self.len {
            self.append(op);
            self.len += 1;
            while let Some(op) = self.queue.remove(&self.len) {
                self.append(op);
                self.len += 1;
            }
        } else {
            self.queue.insert(index, op);
        }
    }

    fn append(&mut self, op: OpBuilder<'a>) {
        if self.start_op.is_none() {
            self.start_op = Some(op.id.counter());
        }
        self.obj_actor.append(op.obj.actor());
        self.obj_ctr.append(op.obj.counter());
        self.key_actor.append(op.key.actor());
        self.key_ctr.append(op.key.icounter());
        self.key_str.append(op.key.key_str());
        self.insert.append(op.insert);
        self.action.append(op.action);
        self.value_meta.append(op.value.meta());
        if let Some(bytes) = op.value.as_raw() {
            self.value.extend(&*bytes);
        }
        self.pred_count.append(op.pred.len() as u64);
        for id in op.pred {
            self.pred_actor.append(id.actoridx());
            self.pred_ctr.append(id.icounter());
        }
        self.expand.append(op.expand);
        self.mark_name.append(op.mark_name);
    }

    fn flush(&mut self) {
        let queue = std::mem::take(&mut self.queue);
        for (_index, op) in queue {
            self.append(op);
            self.len += 1;
        }
    }

    pub(crate) fn build_mapping(
        &mut self,
        default: usize,
        m: &mut ActorMapper<'_>,
    ) -> Vec<Option<ActorIdx>> {
        m.other_actors.truncate(0);
        let mut seen_index = 1;
        if default >= self.actors.len() {
            self.actors.resize(default + 1, false);
        }
        let mut mapping = vec![None; self.actors.len()];

        self.actors[default] = false;
        mapping[default] = Some(ActorIdx(0));

        for (index, seen) in self.actors.iter().enumerate() {
            if *seen {
                m.other_actors.push(index);
                mapping[index] = Some(ActorIdx(seen_index));
                seen_index += 1;
            }
        }

        mapping
    }

    pub(crate) fn save_to(
        mut self,
        actor: usize,
        data: &mut Vec<u8>,
        mapper: &mut ActorMapper<'_>,
    ) -> ChangeOpsColumns {
        let mapper = self.build_mapping(actor, mapper);

        let remap = |actor: &ActorIdx| mapper[usize::from(*actor)].as_ref();

        let obj_actor = self.obj_actor.save_to_and_remap_unless_empty(data, &remap);
        let obj_ctr = self.obj_ctr.save_to_unless_empty(data);
        let key_actor = self.key_actor.save_to_and_remap_unless_empty(data, &remap);
        let key_ctr = self.key_ctr.save_to_unless_empty(data);
        let key_str = self.key_str.save_to_unless_empty(data);
        let insert = self.insert.save_to(data);
        let action = self.action.save_to_unless_empty(data);
        let value_meta = self.value_meta.save_to_unless_empty(data);
        let value = {
            let start = data.len();
            data.extend(self.value);
            start..data.len()
        };
        let pred_count = self.pred_count.save_to_unless_empty(data);
        let pred_actor = self.pred_actor.save_to_and_remap_unless_empty(data, &remap);
        let pred_ctr = self.pred_ctr.save_to_unless_empty(data);
        let expand = self.expand.save_to_unless_empty(data);
        let mark_name = self.mark_name.save_to_unless_empty(data);

        ChangeOpsColumns {
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
        }
    }

    fn finish(
        mut self,
        change: &BuildChangeMetadata<'_>,
        mapper: &mut ActorMapper<'_>,
    ) -> ChangeCols {
        self.flush();

        let mut data = vec![];
        let num_ops = self.len as u64;
        let start_op = self.start_op;
        let meta = self.save_to(change.actor, &mut data, mapper).into();
        let actor = mapper.actors[change.actor].clone();
        let other_actors = mapper.iter().collect();

        ChangeCols {
            actor,
            other_actors,
            start_op,
            num_ops,
            data,
            meta,
        }
    }
}

impl<'a> ChangeBuilder<'a> {
    pub(crate) fn finish<G>(
        &mut self,
        change: &BuildChangeMetadata<'_>,
        graph: &G,
        mapper: &mut ActorMapper<'_>,
    ) -> Result<StoredChange<'static, Verified>, Error>
    where
        G: GetHash,
    {
        let encoder = std::mem::take(&mut self.encoder);
        encoder.finish(change, graph, mapper)
    }

    pub(crate) fn max_op(&self) -> u64 {
        self.start_op + self.encoder.num_ops() - 1
    }

    pub(crate) fn add(&mut self, op: OpBuilder<'a>) {
        let counter = op.id.counter();
        let index = (counter - self.start_op) as usize;

        self.encoder.add(index, op);
    }
}

impl<'a> ChangeCollector<'a> {
    pub(crate) fn with_index(self, index: &'a mut IndexBuilder) -> IndexedChangeCollector<'a> {
        IndexedChangeCollector {
            collector: self,
            index,
        }
    }

    pub(crate) fn try_new(
        change_graph: &'a ChangeGraph,
        op_set: &'a OpSet,
    ) -> Result<ChangeCollector<'a>, OutOfMemory> {
        let mut meta = Vec::new();
        meta.try_reserve(change_graph.len())
            .map_err(|_| OutOfMemory)?;
        for c in change_graph.iter() {
            meta.push(c);
        }
        ChangeCollector::try_from_change_meta(meta, &op_set.actors)
    }

    pub(crate) fn process_ops(&mut self, op_set: &'a OpSet) -> Result<(), ReadOpError> {
        let mut iter = op_set.iter();

        while let Some(op) = iter.try_next()? {
            let op_id = op.id;
            let op_succ = op.succ();

            self.process_op(op);

            for id in op_succ {
                self.process_succ(op_id, id);
            }
        }
        Ok(())
    }

    pub(crate) fn from_bundle_changes(
        changes: Vec<BundleChange<'a>>,
        actors: &'a [ActorId],
    ) -> ChangeCollector<'a> {
        let changes = changes.into_iter().map(|c| c.into()).collect();
        Self::from_change_meta(changes, actors)
    }

    pub(crate) fn try_from_change_meta(
        mut changes: Vec<BuildChangeMetadata<'a>>,
        actors: &'a [ActorId],
    ) -> Result<ChangeCollector<'a>, OutOfMemory> {
        let mut builders = Vec::new();
        builders
            .try_reserve(changes.len())
            .map_err(|_| OutOfMemory)?;
        for (index, e) in changes.iter().enumerate() {
            builders.push(ChangeBuilder {
                actor: e.actor,
                seq: e.seq,
                change: index,
                start_op: e.start_op,
                encoder: OpEncoderStrategy::try_new(e.num_ops())?,
            })
        }

        builders.sort_unstable_by(|a, b| a.actor.cmp(&b.actor).then(a.seq.cmp(&b.seq)));

        let mapper = ActorMapper::new(actors);

        builders
            .iter()
            .enumerate()
            .for_each(|(index, b)| changes[b.change].builder = index);

        Ok(ChangeCollector {
            mapper,
            changes,
            builders,
            last: None,
            preds: HashMap::default(),
        })
    }

    pub(crate) fn from_change_meta(
        mut changes: Vec<BuildChangeMetadata<'a>>,
        actors: &'a [ActorId],
    ) -> ChangeCollector<'a> {
        let mut builders: Vec<_> = changes
            .iter()
            .enumerate()
            .map(|(index, e)| ChangeBuilder {
                actor: e.actor,
                seq: e.seq,
                change: index,
                start_op: e.start_op,
                encoder: OpEncoderStrategy::new(e.num_ops()),
            })
            .collect();

        builders.sort_unstable_by(|a, b| a.actor.cmp(&b.actor).then(a.seq.cmp(&b.seq)));

        let mapper = ActorMapper::new(actors);

        builders
            .iter()
            .enumerate()
            .for_each(|(index, b)| changes[b.change].builder = index);

        ChangeCollector {
            mapper,
            changes,
            builders,
            last: None,
            preds: HashMap::default(),
        }
    }

    pub(crate) fn exclude_hashes(
        op_set: &'a OpSet,
        change_graph: &'a ChangeGraph,
        have_deps: &[ChangeHash],
    ) -> Vec<Change> {
        let changes = change_graph.get_build_metadata_clock(have_deps);
        Self::from_build_meta(op_set, change_graph, changes)
    }

    pub(crate) fn exclude_hashes_meta(
        op_set: &'a OpSet,
        change_graph: &'a ChangeGraph,
        have_deps: &[ChangeHash],
    ) -> Vec<ChangeMetadata<'a>> {
        let changes = change_graph.get_build_metadata_clock(have_deps);
        changes
            .into_iter()
            .map(|c| ChangeMetadata {
                actor: Cow::Borrowed(&op_set.actors[c.actor]),
                seq: c.seq,
                start_op: c.start_op,
                max_op: c.max_op,
                timestamp: c.timestamp,
                message: c.message,
                deps: c
                    .deps
                    .iter()
                    .filter_map(|n| change_graph.index_to_hash(*n as usize).cloned())
                    .collect(),
                hash: change_graph.index_to_hash(c.builder).cloned().unwrap(),
                extra: c.extra,
            })
            .collect()
    }

    pub(crate) fn meta_for_hashes<I>(
        op_set: &'a OpSet,
        change_graph: &'a ChangeGraph,
        hashes: I,
    ) -> Result<Vec<ChangeMetadata<'a>>, AutomergeError>
    where
        I: IntoIterator<Item = ChangeHash>,
    {
        let changes = change_graph.get_build_metadata(hashes)?;
        Ok(changes
            .into_iter()
            .map(|c| ChangeMetadata {
                actor: Cow::Borrowed(&op_set.actors[c.actor]),
                seq: c.seq,
                start_op: c.start_op,
                max_op: c.max_op,
                timestamp: c.timestamp,
                message: c.message,
                deps: c
                    .deps
                    .iter()
                    .filter_map(|n| change_graph.index_to_hash(*n as usize).cloned())
                    .collect(),
                hash: change_graph.index_to_hash(c.builder).cloned().unwrap(),
                extra: c.extra,
            })
            .collect())
    }

    pub(crate) fn for_hashes<I>(
        op_set: &'a OpSet,
        change_graph: &'a ChangeGraph,
        hashes: I,
    ) -> Result<Vec<Change>, AutomergeError>
    where
        I: IntoIterator<Item = ChangeHash>,
    {
        let changes = change_graph.get_build_metadata(hashes)?;
        Ok(Self::from_build_meta(op_set, change_graph, changes))
    }

    fn from_build_meta(
        op_set: &'a OpSet,
        change_graph: &'a ChangeGraph,
        changes: Vec<BuildChangeMetadata<'a>>,
    ) -> Vec<Change> {
        let r1 = Self::from_build_meta_inner(op_set, change_graph, changes.clone());
        debug_assert_eq!(
            r1,
            crate::storage::Bundle::for_hashes(op_set, change_graph, r1.iter().map(|c| c.hash()))
                .unwrap()
                .to_changes()
                .unwrap()
        );
        r1
    }

    fn from_build_meta_inner(
        op_set: &'a OpSet,
        change_graph: &'a ChangeGraph,
        changes: Vec<BuildChangeMetadata<'a>>,
    ) -> Vec<Change> {
        let min = changes
            .iter()
            .map(|c| c.start_op as usize)
            .min()
            .unwrap_or(0);
        let max = changes.iter().map(|c| c.max_op as usize).max().unwrap_or(0) + 1;

        let mut collector = Self::from_change_meta(changes, &op_set.actors);

        for op in op_set.iter_ctr_range(min..max) {
            let op_id = op.id;
            let op_succ = op.succ();
            collector.process_op(op);

            for id in op_succ {
                collector.process_succ(op_id, id);
            }
        }

        collector.finish(change_graph).unwrap()
    }

    pub(crate) fn process_succ(&mut self, op_id: OpId, succ_id: OpId) {
        self.preds.entry(succ_id).or_default().push(op_id);
    }

    pub(crate) fn process_op(&mut self, op: Op<'a>) {
        let next = Some((op.obj, op.elemid_or_key()));
        let flush = self.last != next;

        self.process_op_internal(op, flush);

        if flush {
            self.last = next;
        }
    }

    fn process_op_internal(&mut self, op: Op<'a>, flush: bool) {
        if flush {
            self.flush_deletes();
        }

        let pred = self.preds.remove(&op.id).unwrap_or_default();

        let op = op.build(pred);

        self.add(op);
    }

    pub(crate) fn add(&mut self, op: OpBuilder<'a>) {
        if let Some(index) = self.builders_index(op.id) {
            self.builders[index].add(op);
        }
    }

    pub(crate) fn builders_index(&self, id: OpId) -> Option<usize> {
        self.builders
            .binary_search_by(|builder| {
                builder
                    .actor
                    .cmp(&id.actor())
                    .then_with(|| match id.counter() {
                        c if c < builder.start_op => Ordering::Greater,
                        c if c > builder.max_op() => Ordering::Less,
                        _ => Ordering::Equal,
                    })
            })
            .ok()
    }

    pub(crate) fn flush_deletes(&mut self) {
        if let Some((obj, key)) = self.last.take() {
            for (id, pred) in &self.preds {
                let op = Op::del(*id, obj, key.clone());
                let op = op.build(pred.to_vec());
                if let Some(index) = self.builders_index(op.id) {
                    self.builders[index].add(op);
                }
            }
            self.preds.clear();
        }
    }

    pub(crate) fn finish(self, change_graph: &ChangeGraph) -> Result<Vec<Change>, Error> {
        self.finish_inner(change_graph, None)
    }

    fn finish_inner(
        mut self,
        graph: &ChangeGraph,
        index: Option<&mut IndexBuilder>,
    ) -> Result<Vec<Change>, Error> {
        self.flush_deletes();

        if let Some(i) = index {
            i.flush()
        }

        let mut changes = Vec::with_capacity(self.changes.len());

        for change in self.changes.into_iter() {
            let actor = change.actor;

            if actor >= self.mapper.actors.len() {
                return Err(Error::MissingActor);
            }

            let change = self.builders[change.builder].finish(&change, graph, &mut self.mapper)?;

            changes.push(Change::new(change))
        }

        Ok(changes)
    }

    pub(crate) fn unbundle(
        mut self,
        actors: &[ActorId],
        deps: &[ChangeHash],
    ) -> Result<Vec<Change>, Error> {
        let num_actors = actors.len();
        let num_changes = self.changes.len();
        let mut changes = Vec::with_capacity(num_changes);

        for change in self.changes.into_iter() {
            if change.actor >= num_actors {
                return Err(Error::MissingActor);
            }

            let all_deps = BundleDeps::new(num_changes, &changes, deps);
            let change = self.builders[change.builder]
                .finish(&change, &all_deps, &mut self.mapper)
                .unwrap();

            changes.push(Change::from(change));
        }
        Ok(changes)
    }

    pub(crate) fn build_changegraph(mut self, op_set: &OpSet) -> Result<CollectedChanges, Error> {
        self.flush_deletes();

        let num_actors = op_set.actors.len();
        let mut max_ops = vec![0; num_actors];
        let mut seq = vec![0; num_actors];
        let mut changes = Vec::with_capacity(self.changes.len());
        let mut heads = BTreeSet::new();

        for change in self.changes.into_iter() {
            let actor = change.actor;

            if actor >= num_actors {
                return Err(Error::MissingActor);
            }

            if seq[actor] + 1 != change.seq {
                return Err(Error::ChangesOutOfOrder);
            }

            seq[actor] = change.seq;

            let builder = change.builder;
            let max_op = change.max_op;

            if change.start_op == 0 || max_op < max_ops[actor] {
                return Err(Error::IncorrectMaxOp);
            }

            max_ops[actor] = max_op;

            let change = self.builders[builder].finish(&change, &changes, &mut self.mapper)?;

            let hash = change.hash();

            for dep in change.dependencies() {
                heads.remove(dep);
            }

            heads.insert(hash);

            changes.push(Change::from(change));
        }

        Ok(CollectedChanges { changes, heads })
    }
}

pub(crate) struct CollectedChanges {
    pub(crate) changes: Vec<Change>,
    pub(crate) heads: BTreeSet<ChangeHash>,
}

struct BundleDeps<'a> {
    num_changes: usize,
    changes: &'a Vec<Change>,
    deps: &'a [ChangeHash],
}

impl<'a> BundleDeps<'a> {
    fn new(num_changes: usize, changes: &'a Vec<Change>, deps: &'a [ChangeHash]) -> Self {
        Self {
            num_changes,
            changes,
            deps,
        }
    }
}

impl GetHash for BundleDeps<'_> {
    fn get_hash(&self, index: usize) -> Option<ChangeHash> {
        if index >= self.num_changes {
            self.deps.get(index - self.num_changes).copied()
        } else {
            Some(self.changes.get(index)?.hash())
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) struct ChangeCols {
    pub(crate) num_ops: u64,
    pub(crate) start_op: Option<u64>,
    pub(crate) meta: crate::storage::change::ChangeOpsColumns,
    pub(crate) actor: ActorId,
    pub(crate) other_actors: Vec<ActorId>,
    pub(crate) data: Vec<u8>,
}

#[cfg(all(feature = "wasm", target_family = "wasm"))]
const CAN_OOM: bool = true;

#[cfg(not(all(feature = "wasm", target_family = "wasm")))]
const CAN_OOM: bool = false;
