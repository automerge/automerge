use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io::Write;
use std::marker::PhantomData;
use std::mem::size_of;
use std::num::NonZero;

use super::super::types::{Action, ActorIdx};
use super::super::ValueMeta;
use super::{length_prefixed_bytes, shift_range};
use super::{ActorMapper, ChangeOpsColumns};

use crate::actor::ActorTable;
use crate::author::Authors;
use crate::change_graph::ChangeGraph;
use crate::error::AutomergeError;
use crate::op_set2::change::{write_change_ops, GetHash};
use crate::op_set2::op_set::{IndexBuilder, MarkOrderValidator};
use crate::storage::bundle::BundleChange;
use crate::storage::change::{Change as StoredChange, Verified};
use crate::storage::load::change_collector::Error;
use crate::storage::{hash_parts, ChunkType, Header};
use crate::{
    change::Change,
    op_set2::{ChangeMetadata, KeyRef, Op, OpBuilder, OpSet, ReadOpError},
    types::{ActorId, ChangeHash, ObjId, OpId},
};

#[derive(Debug, thiserror::Error)]
#[error("out of memory")]
pub(crate) struct OutOfMemory;

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
    preds: rustc_hash::FxHashMap<OpId, Vec<OpId>>,
    hashing: Option<Box<HashCursor>>,
    /// the first failure while hashing as changes complete, reported when
    /// collecting
    error: Option<Error>,
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
    num_ops: u64,
    received: u64,
    encoder: OpEncoderStrategy<'a>,
}

#[derive(Debug, Default)]
enum OpEncoderStrategy<'a> {
    /// until the change's first op arrives
    #[default]
    Unstarted,
    Ops(VecEncoder<'a>),
    Enc(Box<ProgressiveEncoder<'a>>),
    /// the encoded change after its dependency hashes, and its op count
    Tail(Box<[u8]>, u64),
}

impl<'a> OpEncoderStrategy<'a> {
    fn new(num_ops: usize) -> Self {
        let ops_size = num_ops * size_of::<Option<OpBuilder<'_>>>();
        let enc_size = size_of::<ProgressiveEncoder<'_>>();
        if ops_size > enc_size {
            Self::Enc(Box::default())
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

    fn add(&mut self, index: usize, op: OpBuilder<'a>) {
        match self {
            Self::Ops(v) => v.add(index, op),
            Self::Enc(e) => e.add(index, op),
            // a duplicate op in a malformed document, which fails its hash check
            Self::Tail(..) => (),
            Self::Unstarted => unreachable!("started before its first op"),
        }
    }

    fn tail(
        self,
        change: &BuildChangeMetadata<'_>,
        mapper: &mut ActorMapper<'_>,
        mut data: Vec<u8>,
    ) -> Result<ChangeTail, Error> {
        mapper.reset();
        let cols = match self {
            Self::Ops(mut v) => v.finish(change, mapper)?,
            Self::Enc(e) => e.finish(change, mapper),
            Self::Unstarted | Self::Tail(..) => return Err(Error::InvalidState),
        };
        let num_ops = cols.num_ops as usize;
        let start_op = cols.start_op.unwrap_or(change.max_op + 1);

        data.reserve(cols.data.len() + change.extra.len() + 64);

        length_prefixed_bytes(&cols.actor, &mut data);

        leb128::write::unsigned(&mut data, change.seq).unwrap();
        leb128::write::unsigned(&mut data, start_op).unwrap();
        leb128::write::signed(&mut data, change.timestamp).unwrap();

        length_prefixed_bytes(change.message_str(), &mut data);

        leb128::write::unsigned(&mut data, cols.other_actors.len() as u64).unwrap();

        for actor in cols.other_actors.iter() {
            length_prefixed_bytes(actor, &mut data);
        }

        cols.meta.raw_columns().write(&mut data);

        let ops_data_start = data.len();
        let ops_data = ops_data_start..(ops_data_start + cols.data.len());

        data.extend(cols.data);
        let extra_bytes = data.len()..(data.len() + change.extra.len());
        if !change.extra.is_empty() {
            data.extend(change.extra.as_ref());
        }

        Ok(ChangeTail {
            data,
            actor: cols.actor,
            other_actors: cols.other_actors,
            start_op,
            ops_meta: cols.meta,
            ops_data,
            extra_bytes,
            num_ops,
        })
    }
}

/// A change's encoding after its dependency hashes, which can be written
/// before those hashes are known; `data` starts with whatever prefix it was
/// written after.
struct ChangeTail {
    data: Vec<u8>,
    actor: ActorId,
    other_actors: Vec<ActorId>,
    start_op: u64,
    ops_meta: crate::storage::change::ChangeOpsColumns,
    ops_data: std::ops::Range<usize>,
    extra_bytes: std::ops::Range<usize>,
    num_ops: usize,
}

fn deps_prefix<G: GetHash>(
    change: &BuildChangeMetadata<'_>,
    graph: &G,
) -> Result<(Vec<u8>, Vec<ChangeHash>), Error> {
    let mut deps = Vec::with_capacity(change.deps.len());
    for i in &change.deps {
        deps.push(
            graph
                .get_hash(*i as usize)
                .ok_or(Error::ChangesOutOfOrder)?,
        );
    }
    let mut data = Vec::with_capacity(1 + deps.len() * 32);
    leb128::write::unsigned(&mut data, deps.len() as u64).unwrap();
    for hash in &deps {
        data.write_all(hash.as_bytes()).unwrap();
    }
    Ok((data, deps))
}

fn assemble(
    change: &BuildChangeMetadata<'_>,
    deps: Vec<ChangeHash>,
    tail: ChangeTail,
) -> StoredChange<'static, Verified> {
    let data = tail.data;
    let header = Header::new(ChunkType::Change, &data);

    let mut bytes = Vec::with_capacity(header.len() + data.len());
    header.write(&mut bytes);
    bytes.extend(data);

    let ops_data = shift_range(tail.ops_data, header.len());
    let extra_bytes = shift_range(tail.extra_bytes, header.len());

    StoredChange {
        bytes: Cow::Owned(bytes),
        header,
        dependencies: deps,
        actor: tail.actor,
        other_actors: tail.other_actors,
        seq: change.seq,
        start_op: NonZero::new(tail.start_op).unwrap(),
        timestamp: change.timestamp,
        message: change.message.as_ref().map(|s| s.to_string()),
        ops_meta: tail.ops_meta,
        ops_data,
        extra_bytes,
        num_ops: tail.num_ops,
        _phantom: PhantomData,
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

    /// A duplicate op is dropped; `finish` then finds a slot missing.
    fn add(&mut self, index: usize, op: OpBuilder<'a>) {
        if let Some(slot @ None) = self.data.get_mut(index) {
            *slot = Some(op);
        }
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
            if last.id.counter() != change.max_op {
                return Err(Error::IncorrectMaxOp);
            }
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

#[derive(Debug, Default)]
pub(crate) struct ProgressiveEncoder<'a> {
    pub(crate) len: usize,
    pub(crate) start_op: Option<u64>,
    actors: Vec<bool>,
    /// slot `i` holds op `len + i`; grown only while at least half full
    window: VecDeque<Option<OpBuilder<'a>>>,
    window_filled: usize,
    /// ops past the window, boxed so rebalancing moves pointers
    sparse: BTreeMap<usize, Box<OpBuilder<'a>>>,
    /// ops before `len`, from a malformed document; kept so its hash fails
    stray: Vec<OpBuilder<'a>>,
    obj_actor: hexane::Encoder<'a, Option<ActorIdx>>,
    obj_ctr: hexane::Encoder<'a, Option<u64>>,
    key_actor: hexane::Encoder<'a, Option<ActorIdx>>,
    key_ctr: hexane::DeltaEncoder<'a, Option<i64>>,
    key_str: hexane::Encoder<'a, Option<String>>,
    insert: hexane::Encoder<'a, bool>,
    action: hexane::Encoder<'a, Action>,
    value_meta: hexane::Encoder<'a, ValueMeta>,
    value: Vec<u8>,
    pred_count: hexane::Encoder<'a, u64>,
    pred_actor: hexane::Encoder<'a, ActorIdx>,
    pred_ctr: hexane::DeltaEncoder<'a, i64>,
    expand: hexane::Encoder<'a, bool>,
    mark_name: hexane::Encoder<'a, Option<String>>,
}

impl<'a> ProgressiveEncoder<'a> {
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
        let Some(slot) = index.checked_sub(self.len) else {
            self.stray.push(op);
            return;
        };
        if slot == 0 {
            self.window.pop_front();
            self.append(op);
            self.len += 1;
            while let Some(op) = self.next_queued() {
                self.append(op);
                self.len += 1;
            }
        } else if slot < self.window.len() {
            self.window[slot] = Some(op);
            self.window_filled += 1;
        } else if slot < 2 * (self.window_filled + 1) + 64 {
            self.window.resize_with(slot + 1, || None);
            self.window[slot] = Some(op);
            self.window_filled += 1;
            self.absorb_sparse();
        } else {
            self.sparse.insert(index, Box::new(op));
            let span = index + 1 - self.len;
            if 2 * (self.window_filled + self.sparse.len()) >= span {
                self.window.resize_with(span, || None);
                self.absorb_sparse();
            }
        }
    }

    fn absorb_sparse(&mut self) {
        let end = self.len + self.window.len();
        let beyond = self.sparse.split_off(&end);
        for (i, op) in std::mem::replace(&mut self.sparse, beyond) {
            self.window[i - self.len] = Some(*op);
            self.window_filled += 1;
        }
    }

    fn next_queued(&mut self) -> Option<OpBuilder<'a>> {
        match self.window.front() {
            Some(Some(_)) => {
                self.window_filled -= 1;
                self.window.pop_front().flatten()
            }
            Some(None) => None,
            None => self.sparse.remove(&self.len).map(|op| *op),
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
        self.key_str
            .append_owned(op.key.key_str().map(|s| s.into_owned()));
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
        self.mark_name
            .append_owned(op.mark_name.map(|s| s.into_owned()));
    }

    fn flush(&mut self) {
        let window = std::mem::take(&mut self.window);
        let sparse = std::mem::take(&mut self.sparse);
        let stray = std::mem::take(&mut self.stray);
        self.window_filled = 0;
        for op in window
            .into_iter()
            .flatten()
            .chain(sparse.into_values().map(|op| *op))
            .chain(stray)
        {
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

        let remap_opt = |actor: Option<ActorIdx>| actor.map(|a| mapper[usize::from(a)].unwrap());
        let remap = |a: ActorIdx| mapper[usize::from(a)].unwrap();

        let obj_actor = self
            .obj_actor
            .save_to_unless_and_remap(data, None, &remap_opt);
        let obj_ctr = self.obj_ctr.save_to_unless(data, None);
        let key_actor = self
            .key_actor
            .save_to_unless_and_remap(data, None, &remap_opt);
        let key_ctr = self.key_ctr.save_to_unless(data, None);
        let key_str = self.key_str.save_to_unless(data, None);
        let insert = self.insert.save_to(data);
        let action = self.action.save_to(data);
        let value_meta = self.value_meta.save_to(data);
        let value = {
            let start = data.len();
            data.extend(self.value);
            start..data.len()
        };
        let pred_count = self.pred_count.save_to(data);
        let pred_actor = self.pred_actor.save_to_and_remap(data, &remap);
        let pred_ctr = self.pred_ctr.save_to(data);
        let expand = self.expand.save_to_unless(data, false);
        let mark_name = self.mark_name.save_to_unless(data, None);

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
    fn new(change: usize, meta: &BuildChangeMetadata<'_>) -> Self {
        ChangeBuilder {
            actor: meta.actor,
            seq: meta.seq,
            change,
            start_op: meta.start_op,
            num_ops: meta.num_ops() as u64,
            received: 0,
            encoder: OpEncoderStrategy::Unstarted,
        }
    }

    fn max_op(&self) -> u64 {
        self.start_op + self.num_ops - 1
    }

    fn is_complete(&self) -> bool {
        self.received >= self.num_ops
    }

    fn start(&mut self) -> Result<(), OutOfMemory> {
        if matches!(self.encoder, OpEncoderStrategy::Unstarted) {
            self.encoder = OpEncoderStrategy::try_new(self.num_ops as usize)?;
        }
        Ok(())
    }

    /// Whether `op` was the change's last.
    fn add(&mut self, op: OpBuilder<'a>) -> Result<bool, OutOfMemory> {
        self.start()?;
        let index = (op.id.counter() - self.start_op) as usize;
        self.encoder.add(index, op);
        self.received += 1;
        Ok(self.received == self.num_ops)
    }

    fn take_tail(
        &mut self,
        change: &BuildChangeMetadata<'_>,
        mapper: &mut ActorMapper<'_>,
        prefix: Vec<u8>,
    ) -> Result<ChangeTail, Error> {
        self.start().map_err(|_| Error::OutOfMemory)?;
        std::mem::take(&mut self.encoder).tail(change, mapper, prefix)
    }

    fn encode_tail(
        &mut self,
        change: &BuildChangeMetadata<'_>,
        mapper: &mut ActorMapper<'_>,
    ) -> Result<(), Error> {
        let tail = self.take_tail(change, mapper, Vec::new())?;
        self.encoder = OpEncoderStrategy::Tail(tail.data.into_boxed_slice(), tail.num_ops as u64);
        Ok(())
    }

    pub(crate) fn finish<G: GetHash>(
        &mut self,
        change: &BuildChangeMetadata<'_>,
        graph: &G,
        mapper: &mut ActorMapper<'_>,
    ) -> Result<StoredChange<'static, Verified>, Error> {
        let (prefix, deps) = deps_prefix(change, graph)?;
        let tail = self.take_tail(change, mapper, prefix)?;
        Ok(assemble(change, deps, tail))
    }

    // inlined, it spreads the per-op load loop over more of the i-cache
    #[inline(never)]
    fn hash<G: GetHash>(
        &mut self,
        change: &BuildChangeMetadata<'_>,
        graph: &G,
        mapper: &mut ActorMapper<'_>,
    ) -> Result<(ChangeHash, u64), Error> {
        let (prefix, _) = deps_prefix(change, graph)?;
        match std::mem::take(&mut self.encoder) {
            OpEncoderStrategy::Tail(tail, num_ops) => {
                Ok((hash_parts(ChunkType::Change, &[&prefix, &tail]), num_ops))
            }
            encoder => {
                self.encoder = encoder;
                let tail = self.take_tail(change, mapper, prefix)?;
                let hash = hash_parts(ChunkType::Change, &[&tail.data]);
                Ok((hash, tail.num_ops as u64))
            }
        }
    }
}

/// Hashes changes in change order, each as soon as it and every change
/// before it are complete, so only changes still waiting hold their ops.
struct HashCursor {
    next: usize,
    order: OrderCheck,
    hashes: Vec<ChangeHash>,
    /// a bundle's external deps, indexed after its members
    deps: Vec<ChangeHash>,
    num_ops: Vec<u64>,
    heads: BTreeSet<ChangeHash>,
}

struct OrderCheck {
    max_ops: Vec<u64>,
    seq: Vec<u64>,
    /// a bundle's actors may start anywhere in their seq chains
    continuing: bool,
}

impl OrderCheck {
    fn new(num_actors: usize, continuing: bool) -> Self {
        OrderCheck {
            max_ops: vec![0; num_actors],
            seq: vec![0; num_actors],
            continuing,
        }
    }

    fn check(&mut self, change: &BuildChangeMetadata<'_>) -> Result<(), Error> {
        let actor = change.actor;
        if actor >= self.seq.len() {
            return Err(Error::MissingActor);
        }
        let first = self.continuing && self.seq[actor] == 0 && change.seq > 0;
        if !first && self.seq[actor] + 1 != change.seq {
            return Err(Error::ChangesOutOfOrder);
        }
        self.seq[actor] = change.seq;
        if change.start_op == 0 || change.max_op < self.max_ops[actor] {
            return Err(Error::IncorrectMaxOp);
        }
        self.max_ops[actor] = change.max_op;
        Ok(())
    }
}

impl<'a> ChangeCollector<'a> {
    pub(crate) fn try_new<I>(
        changes: I,
        actors: &'a ActorTable,
    ) -> Result<ChangeCollector<'a>, OutOfMemory>
    where
        I: ExactSizeIterator<Item = BuildChangeMetadata<'a>>,
    {
        let mut meta = Vec::new();
        meta.try_reserve(changes.len()).map_err(|_| OutOfMemory)?;
        meta.extend(changes);
        ChangeCollector::try_from_change_meta(meta, actors)
    }

    pub(crate) fn process_ops(
        &mut self,
        op_set: &'a OpSet,
        mark_order: &mut MarkOrderValidator,
    ) -> Result<(), ReadOpError> {
        let mut iter = op_set.iter();

        while let Some(op) = iter.try_next()? {
            let op_id = op.id;
            let op_succ = op.succ();

            mark_order.process_op(&op);
            self.process_op(op);

            for id in op_succ {
                self.process_succ(op_id, id);
            }
        }
        Ok(())
    }

    pub(crate) fn from_bundle_changes(
        changes: Vec<BundleChange<'a>>,
        actors: &'a ActorTable,
    ) -> ChangeCollector<'a> {
        let changes = changes.into_iter().map(|c| c.into()).collect();
        Self::from_change_meta(changes, actors)
    }

    pub(crate) fn try_from_change_meta(
        mut changes: Vec<BuildChangeMetadata<'a>>,
        actors: &'a ActorTable,
    ) -> Result<ChangeCollector<'a>, OutOfMemory> {
        let mut builders = Vec::new();
        builders
            .try_reserve(changes.len())
            .map_err(|_| OutOfMemory)?;
        for (index, e) in changes.iter().enumerate() {
            builders.push(ChangeBuilder::new(index, e));
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
            preds: Default::default(),
            hashing: None,
            error: None,
        })
    }

    /// Hash each change as soon as it and every change before it are
    /// complete, for [`Self::collect_hashes`].
    pub(crate) fn hashing(self) -> Self {
        self.hashing_with(Vec::new(), false)
    }

    /// [`Self::hashing`] for a bundle's members, which may depend on `deps`
    /// outside it.
    pub(crate) fn hashing_bundle(self, deps: &[ChangeHash]) -> Self {
        self.hashing_with(deps.to_vec(), true)
    }

    fn hashing_with(mut self, deps: Vec<ChangeHash>, continuing: bool) -> Self {
        self.hashing = Some(Box::new(HashCursor {
            next: 0,
            order: OrderCheck::new(self.mapper.actors.len(), continuing),
            hashes: Vec::with_capacity(self.changes.len()),
            deps,
            num_ops: Vec::with_capacity(self.changes.len()),
            heads: BTreeSet::new(),
        }));
        self.advance(false);
        self
    }

    pub(crate) fn from_change_meta(
        changes: Vec<BuildChangeMetadata<'a>>,
        actors: &'a ActorTable,
    ) -> ChangeCollector<'a> {
        Self::try_from_change_meta(changes, actors).unwrap()
    }

    /// Like [`Self::exclude_hashes_meta`] but keyed by a clock that needs no
    /// hashes. Fails if a returned change's dep hashes are not known.
    pub(crate) fn exclude_seq_clock<H: crate::hash_retention::HashRetention>(
        op_set: &'a OpSet,
        change_graph: &'a ChangeGraph<H>,
        clock: crate::clock::SeqClock,
    ) -> Result<Vec<Change>, AutomergeError> {
        let changes = change_graph.get_build_metadata_for_seq_clock(clock);
        Self::from_build_meta(op_set, change_graph, changes)
    }

    pub(crate) fn exclude_hashes_meta<H: crate::hash_retention::HashRetention>(
        op_set: &'a OpSet,
        change_graph: &'a ChangeGraph<H>,
        authors: &'a Authors,
        have_deps: &[ChangeHash],
    ) -> Result<Vec<ChangeMetadata<'a>>, AutomergeError> {
        let changes = change_graph.get_build_metadata_clock(have_deps)?;
        Self::metadata(op_set, change_graph, authors, changes)
    }

    pub(crate) fn meta_for_hashes<I, H: crate::hash_retention::HashRetention>(
        op_set: &'a OpSet,
        change_graph: &'a ChangeGraph<H>,
        authors: &'a Authors,
        hashes: I,
    ) -> Result<Vec<ChangeMetadata<'a>>, AutomergeError>
    where
        I: IntoIterator<Item = ChangeHash>,
    {
        let changes = change_graph.get_build_metadata(hashes)?;
        Self::metadata(op_set, change_graph, authors, changes)
    }

    fn metadata<H: crate::hash_retention::HashRetention>(
        op_set: &'a OpSet,
        change_graph: &'a ChangeGraph<H>,
        authors: &'a Authors,
        changes: Vec<BuildChangeMetadata<'a>>,
    ) -> Result<Vec<ChangeMetadata<'a>>, AutomergeError> {
        changes
            .into_iter()
            .map(|c| {
                Ok(ChangeMetadata {
                    actor: Cow::Borrowed(&op_set.actors[c.actor]),
                    author: authors.get_author_for_actor(c.actor),
                    seq: c.seq,
                    start_op: c.start_op,
                    max_op: c.max_op,
                    timestamp: c.timestamp,
                    message: c.message,
                    deps: c
                        .deps
                        .iter()
                        .map(|n| {
                            change_graph
                                .index_to_hash(*n as usize)
                                .ok_or(AutomergeError::AuditModeRequired)
                        })
                        .collect::<Result<_, _>>()?,
                    hash: change_graph
                        .try_index_to_hash(c.builder)
                        .map_err(AutomergeError::from)?,
                    extra: c.extra,
                })
            })
            .collect()
    }

    pub(crate) fn for_hashes<I, H: crate::hash_retention::HashRetention>(
        op_set: &'a OpSet,
        change_graph: &'a ChangeGraph<H>,
        hashes: I,
    ) -> Result<Vec<Change>, AutomergeError>
    where
        I: IntoIterator<Item = ChangeHash>,
    {
        let changes = change_graph.get_build_metadata(hashes)?;
        Self::from_build_meta(op_set, change_graph, changes)
    }

    fn from_build_meta<H: crate::hash_retention::HashRetention>(
        op_set: &'a OpSet,
        change_graph: &'a ChangeGraph<H>,
        changes: Vec<BuildChangeMetadata<'a>>,
    ) -> Result<Vec<Change>, AutomergeError> {
        // building a change embeds its deps' hashes, which must all be known
        for c in &changes {
            for dep in &c.deps {
                if change_graph.index_to_hash(*dep as usize).is_none() {
                    return Err(AutomergeError::AuditModeRequired);
                }
            }
        }
        let r1 = Self::from_build_meta_inner(op_set, change_graph, changes);
        #[cfg(feature = "slow_path_assertions")]
        {
            // the bundle encoder reorders changes: compare as sets
            let bundle_changes = crate::storage::Bundle::storage_for_hashes(
                op_set,
                change_graph,
                r1.iter().map(|c| c.hash()),
            )
            .unwrap()
            .to_changes()
            .unwrap();
            let r1_hashes: std::collections::HashSet<_> = r1.iter().map(|c| c.hash()).collect();
            let bundle_hashes: std::collections::HashSet<_> =
                bundle_changes.iter().map(|c| c.hash()).collect();
            debug_assert_eq!(r1_hashes, bundle_hashes);
        }
        Ok(r1)
    }

    fn from_build_meta_inner<H: crate::hash_retention::HashRetention>(
        op_set: &'a OpSet,
        change_graph: &'a ChangeGraph<H>,
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
        let Some(index) = self.builders_index(op.id) else {
            return;
        };
        match self.builders[index].add(op) {
            Ok(true) => self.complete(index),
            Ok(false) => (),
            Err(OutOfMemory) => self.fail(Error::OutOfMemory),
        }
    }

    fn fail(&mut self, error: Error) {
        self.error.get_or_insert(error);
    }

    fn complete(&mut self, index: usize) {
        let builder = &mut self.builders[index];
        let Some(cursor) = &self.hashing else {
            return;
        };
        // a change at the cursor is hashed straight from its ops, and a small
        // change's ops take less memory than its encoding
        if builder.change != cursor.next && matches!(builder.encoder, OpEncoderStrategy::Enc(_)) {
            let change = &self.changes[builder.change];
            if let Err(e) = builder.encode_tail(change, &mut self.mapper) {
                self.fail(e);
                return;
            }
        }
        self.advance(false);
    }

    /// Hash changes in order up to the first incomplete one, or, with
    /// `force`, all of them.
    fn advance(&mut self, force: bool) {
        let Some(cursor) = &mut self.hashing else {
            return;
        };
        while self.error.is_none() && cursor.next < self.changes.len() {
            let change = &self.changes[cursor.next];
            let builder = &mut self.builders[change.builder];
            if !force && !builder.is_complete() {
                break;
            }
            let all_deps = MemberHashes {
                num_changes: self.changes.len(),
                hashes: &cursor.hashes,
                deps: &cursor.deps,
            };
            let hashed = cursor
                .order
                .check(change)
                .and_then(|()| builder.hash(change, &all_deps, &mut self.mapper));
            match hashed {
                Ok((hash, num_ops)) => {
                    for dep in &change.deps {
                        if let Some(dep) = cursor.hashes.get(*dep as usize) {
                            cursor.heads.remove(dep);
                        }
                    }
                    cursor.heads.insert(hash);
                    cursor.hashes.push(hash);
                    cursor.num_ops.push(num_ops);
                    cursor.next += 1;
                }
                Err(e) => self.error = Some(e),
            }
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
        let last = self.last.take();
        if self.preds.is_empty() {
            return;
        }
        if let Some((obj, key)) = last {
            let mut preds = std::mem::take(&mut self.preds);
            for (id, mut pred) in preds.drain() {
                // grown by pushes; a queued out-of-order delete would hold the slack
                pred.shrink_to_fit();
                self.add(Op::del(id, obj, key.clone()).build(pred));
            }
            self.preds = preds;
        }
    }

    fn take_error(&mut self) -> Result<(), Error> {
        self.error.take().map_or(Ok(()), Err)
    }

    pub(crate) fn finish<H: crate::hash_retention::HashRetention>(
        self,
        change_graph: &ChangeGraph<H>,
    ) -> Result<Vec<Change>, Error> {
        self.finish_inner(change_graph, None)
    }

    fn finish_inner<H: crate::hash_retention::HashRetention>(
        mut self,
        graph: &ChangeGraph<H>,
        index: Option<&mut IndexBuilder>,
    ) -> Result<Vec<Change>, Error> {
        self.flush_deletes();
        self.take_error()?;

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

    pub(crate) fn decode_bundle(
        mut self,
        actors: &ActorTable,
        deps: &[ChangeHash],
    ) -> Result<Vec<Change>, Error> {
        self.flush_deletes();
        self.take_error()?;
        let num_actors = actors.len();
        let num_changes = self.changes.len();
        let mut changes = Vec::with_capacity(num_changes);

        for change in self.changes.into_iter() {
            if change.actor >= num_actors {
                return Err(Error::MissingActor);
            }

            let all_deps = BundleDeps::new(num_changes, &changes, deps);
            let change =
                self.builders[change.builder].finish(&change, &all_deps, &mut self.mapper)?;

            changes.push(Change::from(change));
        }
        Ok(changes)
    }

    pub(crate) fn process_all_ops(&mut self, op_set: &'a OpSet) -> Result<(), ReadOpError> {
        self.process_all_ops_with(op_set, |_| Ok(()))
    }

    /// [`Self::process_all_ops`], with `external_preds` adding to each row
    /// in turn the preds its succ lists cannot show.
    pub(crate) fn process_all_ops_with<E: From<ReadOpError>>(
        &mut self,
        op_set: &'a OpSet,
        mut external_preds: impl FnMut(&mut Vec<OpId>) -> Result<(), E>,
    ) -> Result<(), E> {
        let mut iter = op_set.iter();
        let mut external = Vec::new();
        while let Some(op) = iter.try_next()? {
            let op_id = op.id;
            let op_succ = op.succ();
            let next = Some((op.obj, op.elemid_or_key()));
            let flush = self.last != next;
            if flush {
                self.flush_deletes();
            }
            // after the flush, which would take them for an elided delete
            external_preds(&mut external)?;
            if !external.is_empty() {
                let pred = self.preds.entry(op_id).or_default();
                pred.append(&mut external);
                pred.sort_unstable();
            }
            self.process_op_internal(op, false);
            if flush {
                self.last = next;
            }
            for id in op_succ {
                self.process_succ(op_id, id);
            }
        }
        Ok(())
    }

    pub(crate) fn collect(mut self, op_set: &OpSet) -> Result<Vec<Change>, Error> {
        self.flush_deletes();
        self.take_error()?;

        let mut order = OrderCheck::new(op_set.actors.len(), false);
        let mut changes: Vec<Change> = Vec::with_capacity(self.changes.len());

        for change in std::mem::take(&mut self.changes) {
            order.check(&change)?;
            let built =
                self.builders[change.builder].finish(&change, &changes, &mut self.mapper)?;
            changes.push(Change::from(built));
        }

        Ok(changes)
    }

    /// Like [`Self::collect`], but holds only each change's hash and op
    /// count. Panics unless [`Self::hashing`] or [`Self::hashing_bundle`]
    /// came before the ops were added.
    pub(crate) fn collect_hashes(mut self) -> Result<CollectedHashes, Error> {
        self.flush_deletes();
        self.advance(true);
        self.take_error()?;
        let cursor = self.hashing.expect("collect_hashes without hashing");
        Ok(CollectedHashes {
            hashes: cursor.hashes,
            num_ops: cursor.num_ops,
            heads: cursor.heads,
        })
    }
}

#[derive(Debug)]
pub(crate) struct CollectedHashes {
    pub(crate) hashes: Vec<ChangeHash>,
    pub(crate) num_ops: Vec<u64>,
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

struct MemberHashes<'a> {
    num_changes: usize,
    hashes: &'a [ChangeHash],
    deps: &'a [ChangeHash],
}

impl GetHash for MemberHashes<'_> {
    fn get_hash(&self, index: usize) -> Option<ChangeHash> {
        if index >= self.num_changes {
            self.deps.get(index - self.num_changes).copied()
        } else {
            self.hashes.get(index).copied()
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
