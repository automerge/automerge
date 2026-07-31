use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::ops::Range;

use hexane::RunDecoder;

use crate::op_set2::change::{length_prefixed_bytes, ActorMapper};
use crate::op_set2::meta::ValueMeta;
use crate::op_set2::op::{Op, OpBuilder};
use crate::op_set2::types::{Action, ActorIdx, KeyRef};
use crate::op_set2::{ReadOpError, ScalarValue};
use crate::storage::change::DEFLATE_MIN_SIZE;
use crate::storage::columns::{compression, ColumnType};
use crate::storage::{RawColumn, RawColumns};
use crate::types::{ChangeHash, ElemId, ObjId, OpId};

use super::{BundleChange, BundleMetadata, BundleStorage, ParseError};

/// Apply the actor remap inline to a nullable actor encoder and write the
/// remapped bytes to `data`, eliding an all-`None` column to an empty range.
fn save_opt_actor_unless_empty(
    enc: hexane::Encoder<'_, Option<ActorIdx>>,
    mapping: &[Option<ActorIdx>],
    data: &mut Vec<u8>,
) -> Range<usize> {
    enc.save_to_unless_and_remap(data, None, |a: Option<ActorIdx>| {
        a.map(|i| mapping[usize::from(i)].unwrap())
    })
}

/// Apply the actor remap inline to a non-null actor encoder and always write
/// the remapped bytes to `data`.  Used for columns where every entry is
/// present (`id_actor`, `pred_actor`, change-level `actor`).
fn save_actor(
    enc: hexane::Encoder<'_, ActorIdx>,
    mapping: &[Option<ActorIdx>],
    data: &mut Vec<u8>,
) -> Range<usize> {
    enc.save_to_and_remap(data, |a: ActorIdx| mapping[usize::from(a)].unwrap())
}

pub(crate) struct BundleBuilder<'a> {
    mapper: ActorMapper<'a>,
    change_writer: BundleChangeWriter<'a>,
    op_writer: BundleOpWriter<'a>,
    builders: Vec<ChangeBuilder>,
    last: Option<(ObjId, KeyRef<'a>)>,
    preds: HashMap<OpId, Vec<OpId>>,
    max_op: u64,
}

impl<'a> BundleBuilder<'a> {
    pub(super) fn from_change_meta(
        mut changes: Vec<BundleMetadata<'a>>,
        mut mapper: ActorMapper<'a>,
    ) -> BundleBuilder<'a> {
        // change[n].builder starts off as NodeIdx which is topo order
        // writing the changes in topo order prevents un-needed hashes in the external buffer
        changes.sort_by(|a, b| a.builder.cmp(&b.builder));

        let mut builders: Vec<_> = changes
            .iter()
            .enumerate()
            .map(|(index, e)| ChangeBuilder {
                actor: e.actor,
                seq: e.seq,
                change: index,
                start_op: e.start_op,
                max_op: e.start_op + e.num_ops() as u64 - 1,
            })
            .collect();

        builders.sort_unstable_by(|a, b| a.actor.cmp(&b.actor).then(a.seq.cmp(&b.seq)));

        builders
            .iter()
            .enumerate()
            .for_each(|(index, b)| changes[b.change].builder = index);

        let mut change_writer = BundleChangeWriter::new(changes.len());
        for c in &changes {
            change_writer.add(c, &mut mapper);
        }

        let op_writer = BundleOpWriter::default();

        BundleBuilder {
            mapper,
            change_writer,
            op_writer,
            builders,
            last: None,
            preds: HashMap::default(),
            max_op: 0,
        }
    }

    pub(crate) fn process_op(&mut self, op: Op<'a>, succ: &[OpId]) {
        let next = Some((op.obj, op.elemid_or_key()));
        let flush = self.last != next;

        self.process_op_internal(op, succ, flush);

        if flush {
            self.last = next;
        }
    }

    fn process_op_internal(&mut self, op: Op<'a>, succ: &[OpId], flush: bool) {
        self.max_op = std::cmp::max(self.max_op, op.id.counter());

        if flush {
            self.flush_deletes();
        }

        let pred = self.preds.remove(&op.id).unwrap_or_default();

        if let Some(index) = self.builders_index(op.id) {
            // a member row carries its in-bundle successors in the succ
            // column; relationships to later, non-member ops are not the
            // bundle's business and are dropped
            let internal_succ: Vec<OpId> = succ
                .iter()
                .copied()
                .filter(|s| self.builders_index(*s).is_some())
                .collect();
            // the RAW key: for an insert that is its anchor (the row
            // the receiver's slot search must locate) — elemid_or_key
            // would give the insert's own element instead
            let target = self.hint_target(&op.key);
            let op = op.build(pred);
            self.op_writer
                .add_with_target(&op, &internal_succ, index, &mut self.mapper, target);
        }
    }

    /// The op's covered seq target: its key elem when that elem is a
    /// doc row (non-head, not a member) — the row the receiver's
    /// manifold will have to locate.
    fn hint_target(&self, key: &crate::op_set2::types::KeyRef<'_>) -> Option<OpId> {
        match key {
            crate::op_set2::types::KeyRef::Seq(e)
                if !e.is_head() && self.builders_index(e.0).is_none() =>
            {
                Some(e.0)
            }
            _ => None,
        }
    }

    pub(crate) fn process_succ(&mut self, op_id: OpId, succ_id: OpId) {
        self.max_op = std::cmp::max(self.max_op, succ_id.counter());
        // only relationships that cross INTO the bundle ride the pred
        // column: an in-bundle target carries the relationship in its
        // succ column instead
        if self.builders_index(op_id).is_none() && self.builders_index(succ_id).is_some() {
            self.preds.entry(succ_id).or_default().push(op_id);
        }
    }

    /// Write the delete ops whose preds crossed into the bundle from
    /// outside. Deletes whose targets are all in-bundle never reach
    /// `preds` — they have no row; their ids live in the targets' succ
    /// column.
    pub(crate) fn flush_deletes(&mut self) {
        if let Some((obj, key)) = self.last.take() {
            let target = self.hint_target(&key);
            // `preds` is a HashMap, whose iteration order is seeded per
            // instance — emitting in that order would make a bundle's
            // bytes depend on which allocation it happened to get rather
            // than on its content. Within a key group document order is
            // by op id, so sort.
            let mut pending: Vec<(OpId, Vec<OpId>)> = self.preds.drain().collect();
            pending.sort_unstable_by_key(|(id, _)| *id);
            for (id, pred) in pending {
                let op = Op::del(id, obj, key.clone());
                let op = op.build(pred);
                if let Some(index) = self.builders_index(op.id) {
                    self.op_writer
                        .add_with_target(&op, &[], index, &mut self.mapper, target);
                }
            }
        }
    }

    /// The covered seq targets referenced by the member ops — the rows
    /// whose covered-rank the hint column carries.
    pub(crate) fn hint_targets(&self) -> rustc_hash::FxHashSet<OpId> {
        self.op_writer.targets.iter().flatten().copied().collect()
    }

    pub(crate) fn is_member(&self, id: OpId) -> bool {
        self.builders_index(id).is_some()
    }

    pub(crate) fn finish_with_ranks(
        mut self,
        ranks: &std::collections::HashMap<OpId, u64>,
    ) -> BundleStorage<'static, crate::storage::change::Verified> {
        self.flush_deletes();

        let mut mapper = self.mapper;

        mapper.build_mapping(None);

        let deps = self.change_writer.external.clone();
        let actors = mapper.iter().collect::<Vec<_>>();

        // Prefix: deps + actors. Identical in both the uncompressed and
        // compressed representations.
        let mut prefix = Vec::new();
        leb128::write::unsigned(&mut prefix, deps.len() as u64).unwrap();
        for hash in &deps {
            prefix.extend(hash.as_bytes());
        }
        leb128::write::unsigned(&mut prefix, actors.len() as u64).unwrap();
        for actor in &actors {
            length_prefixed_bytes(actor, &mut prefix);
        }

        // Column data (uncompressed) and per-column metadata.
        let mut change_data_buf = Vec::new();
        let change_cols = self.change_writer.finish(&mapper, &mut change_data_buf);
        let changes_meta = change_cols.raw_columns();
        let mut ops_data_buf = Vec::new();
        let ops_cols = self
            .op_writer
            .finish_with_ranks(&mapper, &mut ops_data_buf, ranks);
        let ops_meta = ops_cols.raw_columns();

        // ---- Uncompressed assembly (used in-memory for iteration) ----
        let mut data_u = prefix.clone();
        changes_meta.write(&mut data_u);
        let changes_data_start_u = data_u.len();
        data_u.extend_from_slice(&change_data_buf);
        let changes_data_end_u = data_u.len();
        ops_meta.write(&mut data_u);
        let ops_data_start_u = data_u.len();
        data_u.extend_from_slice(&ops_data_buf);
        let ops_data_end_u = data_u.len();

        // No chunk header of its own: these columns are the tail of the
        // bundle chunk, not a nested chunk. Offsets are relative to the
        // start of the column data.
        let bytes_u = data_u;
        let changes_data_u_range = changes_data_start_u..changes_data_end_u;
        let ops_data_u_range = ops_data_start_u..ops_data_end_u;

        // ---- Compressed assembly (used as the on-disk/wire form) ----
        // Per-column DEFLATE above DEFLATE_MIN_SIZE, mirroring Document.
        let mut data_c = prefix;
        let mut compressed_change_data = Vec::new();
        let changes_meta_c = changes_meta.compress(
            &change_data_buf,
            &mut compressed_change_data,
            DEFLATE_MIN_SIZE,
        );
        changes_meta_c.write(&mut data_c);
        data_c.extend_from_slice(&compressed_change_data);
        let mut compressed_ops_data = Vec::new();
        let ops_meta_c =
            ops_meta.compress(&ops_data_buf, &mut compressed_ops_data, DEFLATE_MIN_SIZE);
        ops_meta_c.write(&mut data_c);
        data_c.extend_from_slice(&compressed_ops_data);

        let bytes_c = data_c;

        let storage = BundleStorage {
            bytes: Cow::Owned(bytes_u),
            compressed_bytes: Some(Cow::Owned(bytes_c)),
            ops_meta,
            ops_data: ops_data_u_range,
            deps,
            actors,
            changes_meta,
            changes_data: changes_data_u_range,
            // the builder's caller reads the members back to validate
            // them; that first read fills this
            changes: Default::default(),
            // a bundle being sent is never applied, so its op columns are
            // not loaded here
            frag_ops: Default::default(),
            _phantom: PhantomData,
        };

        storage
    }

    fn builders_index(&self, id: OpId) -> Option<usize> {
        self.builders
            .binary_search_by(|builder| {
                builder
                    .actor
                    .cmp(&id.actor())
                    .then_with(|| match id.counter() {
                        c if c < builder.start_op => Ordering::Greater,
                        c if c > builder.max_op => Ordering::Less,
                        _ => Ordering::Equal,
                    })
            })
            .ok()
    }
}

#[derive(Default)]
pub(crate) struct BundleChangeWriter<'a> {
    len: usize,
    cap: usize,
    seen: HashMap<ChangeHash, usize>,
    external: Vec<ChangeHash>,
    actor: hexane::Encoder<'a, ActorIdx>,
    seq: hexane::DeltaEncoder<'a, i64>,
    /// the member's op count, not its `start_op`: this is the form the
    /// receiving change graph stores, so it copies in as a column
    num_ops: hexane::Encoder<'a, u64>,
    max_op: hexane::DeltaEncoder<'a, i64>,
    timestamp: hexane::DeltaEncoder<'a, i64>,
    message: hexane::Encoder<'a, Option<String>>,
    dep_count: hexane::Encoder<'a, u32>,
    deps: hexane::DeltaEncoder<'a, i64>,
    /// the extra bytes' widths as value metadata, matching the document
    /// format's extra column (and the graph's `extra_bytes_meta`)
    extra_meta: hexane::Encoder<'a, ValueMeta>,
    extra: Vec<u8>,
}

impl<'a> BundleChangeWriter<'a> {
    fn new(cap: usize) -> Self {
        BundleChangeWriter {
            cap,
            ..Default::default()
        }
    }

    fn add(&mut self, change: &BundleMetadata<'a>, mapper: &mut ActorMapper<'_>) {
        assert!(self.len < self.cap);
        mapper.process_actor(change.actor);
        self.len += 1;
        self.actor.append(ActorIdx::from(change.actor));
        self.seq.append(change.seq as i64);
        self.num_ops.append(1 + change.max_op - change.start_op);
        self.max_op.append(change.max_op as i64);
        self.message
            .append_owned(change.message.as_deref().map(str::to_owned));
        self.timestamp.append(change.timestamp);
        self.extra_meta
            .append(ValueMeta::from(change.extra.as_ref()));
        self.extra.extend_from_slice(&change.extra);
        self.dep_count.append(change.deps.len() as u32);
        for d in &change.deps {
            let dep_idx = match d {
                // members are added in topological (member-list) order, so
                // a member's dep index is its position in that list
                super::DepRef::Internal(pos) => *pos as i64,
                super::DepRef::External(h) => {
                    if let Some(i) = self.seen.get(h) {
                        *i as i64
                    } else {
                        let index = self.cap + self.external.len();
                        self.seen.insert(*h, index);
                        self.external.push(*h);
                        index as i64
                    }
                }
            };
            self.deps.append(dep_idx);
        }
    }

    fn finish(self, mapper: &ActorMapper<'_>, data: &mut Vec<u8>) -> BundleChangeColumns {
        let actor = save_actor(self.actor, &mapper.mapping, data);
        let seq = self.seq.save_to(data);
        let num_ops = self.num_ops.save_to(data);
        let max_op = self.max_op.save_to(data);
        let timestamp = self.timestamp.save_to(data);
        let message = self.message.save_to(data);
        let dep_count = self.dep_count.save_to(data);
        let deps = self.deps.save_to(data);
        let extra_meta = self.extra_meta.save_to(data);
        let start = data.len();
        data.extend_from_slice(&self.extra);
        let extra = start..data.len();
        BundleChangeColumns {
            actor,
            seq,
            num_ops,
            max_op,
            timestamp,
            message,
            dep_count,
            deps,
            extra_meta,
            extra,
        }
    }
}

#[derive(Default)]
pub(crate) struct BundleOpWriter<'a> {
    /// per-op covered seq target (key elem) for the hint column
    targets: Vec<Option<OpId>>,
    obj_actor: hexane::Encoder<'a, Option<ActorIdx>>,
    obj_ctr: hexane::Encoder<'a, Option<u64>>,
    key_actor: hexane::Encoder<'a, Option<ActorIdx>>,
    key_ctr: hexane::DeltaEncoder<'a, Option<i64>>,
    key_str: hexane::Encoder<'a, Option<String>>,
    id_actor: hexane::Encoder<'a, ActorIdx>,
    insert: hexane::Encoder<'a, bool>,
    action: hexane::Encoder<'a, Action>,
    value_meta: hexane::Encoder<'a, ValueMeta>,
    value: Vec<u8>,
    pred_count: hexane::Encoder<'a, u32>,
    pred_actor: hexane::Encoder<'a, ActorIdx>,
    pred_ctr: hexane::DeltaEncoder<'a, i64>,
    succ_count: hexane::Encoder<'a, u32>,
    succ_actor: hexane::Encoder<'a, ActorIdx>,
    succ_ctr: hexane::DeltaEncoder<'a, i64>,
    expand: hexane::Encoder<'a, bool>,
    mark_name: hexane::Encoder<'a, Option<String>>,
    /// Each op's counter, in doc order — emitted as the `ID_CTR`
    /// delta-int column, the same encoding a document chunk uses.
    id_ctr_values: Vec<i64>,
}

impl<'a> BundleOpWriter<'a> {
    pub(crate) fn add(
        &mut self,
        op: &OpBuilder<'_>,
        succ: &[OpId],
        _index: usize,
        mapper: &mut ActorMapper<'a>,
    ) {
        self.add_with_target(op, succ, _index, mapper, None)
    }

    /// [`Self::add`], recording the op's covered seq target (its key
    /// elem when that elem is a doc row) for the hint column.
    pub(crate) fn add_with_target(
        &mut self,
        op: &OpBuilder<'_>,
        succ: &[OpId],
        _index: usize,
        mapper: &mut ActorMapper<'a>,
        target: Option<OpId>,
    ) {
        self.targets.push(target);
        mapper.process_op(op);
        self.succ_count.append(succ.len() as u32);
        for s in succ {
            self.succ_actor.append(s.actoridx());
            self.succ_ctr.append(s.icounter());
        }
        self.id_actor.append(op.id.actoridx());
        self.obj_actor.append(op.obj.actor());
        self.obj_ctr.append(op.obj.counter());
        self.key_actor.append(op.key.actor());
        self.key_ctr.append(op.key.icounter());
        self.key_str
            .append_owned(op.key.key_str().map(|s| s.into_owned()));
        self.insert.append(op.insert);
        self.action.append(op.action);
        self.value_meta.append(op.value.meta());
        if let Some(bytes) = op.value.to_raw() {
            self.value.extend_from_slice(&bytes);
        }
        self.pred_count.append(op.pred.len() as u32);
        for p in &op.pred {
            self.pred_actor.append(p.actoridx());
            self.pred_ctr.append(p.icounter());
        }
        self.expand.append(op.expand);
        self.mark_name
            .append_owned(op.mark_name.as_deref().map(str::to_owned));
        self.id_ctr_values.push(op.id.counter() as i64);
    }

    pub(crate) fn finish(self, mapper: &ActorMapper<'a>, data: &mut Vec<u8>) -> BundleOpsColumns {
        self.finish_with_ranks(mapper, data, &Default::default())
    }

    /// [`Self::finish`] with the covered-rank of every recorded target:
    /// each op's hint value is `ranks[target]` — the number of
    /// dep-covered ops preceding the target row in document order.
    pub(crate) fn finish_with_ranks(
        self,
        mapper: &ActorMapper<'a>,
        data: &mut Vec<u8>,
        ranks: &std::collections::HashMap<OpId, u64>,
    ) -> BundleOpsColumns {
        let mut hint_enc = hexane::DeltaEncoder::<Option<i64>>::default();
        for t in &self.targets {
            hint_enc.append(t.and_then(|id| ranks.get(&id)).map(|&r| r as i64));
        }
        let hint = hint_enc.save_to_unless(data, None);
        let obj_actor = save_opt_actor_unless_empty(self.obj_actor, &mapper.mapping, data);
        let obj_ctr = self.obj_ctr.save_to_unless(data, None);
        let key_actor = save_opt_actor_unless_empty(self.key_actor, &mapper.mapping, data);
        let key_ctr = self.key_ctr.save_to_unless(data, None);
        let key_str = self.key_str.save_to_unless(data, None);
        let id_actor = save_actor(self.id_actor, &mapper.mapping, data);
        let insert = self.insert.save_to(data);
        let action = self.action.save_to(data);
        let value_meta = self.value_meta.save_to(data);
        let value_start = data.len();
        data.extend_from_slice(&self.value);
        let value = value_start..data.len();
        // a bundle whose members reference nothing outside — a whole
        // document — has no preds at all; drop the all-zero column
        let pred_count = self.pred_count.save_to_unless(data, 0);
        let pred_actor = save_actor(self.pred_actor, &mapper.mapping, data);
        let pred_ctr = self.pred_ctr.save_to(data);
        let succ_count = self.succ_count.save_to(data);
        let succ_actor = save_actor(self.succ_actor, &mapper.mapping, data);
        let succ_ctr = self.succ_ctr.save_to(data);
        let expand = self.expand.save_to_unless(data, false);
        let mark_name = self.mark_name.save_to_unless(data, None);

        // Op counters in doc order. `add()` appends in doc order, so
        // element k is the counter of the op at doc position k.
        let id_ctr_values = self.id_ctr_values;
        let mut id_ctr_enc = hexane::DeltaEncoder::<Option<i64>>::default();
        for c in &id_ctr_values {
            id_ctr_enc.append(Some(*c));
        }
        let id_ctr = id_ctr_enc.save_to(data);

        BundleOpsColumns {
            id_actor,
            id_ctr,
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
            succ_count,
            succ_actor,
            succ_ctr,
            expand,
            mark_name,
            hint,
        }
    }
}

#[derive(Default)]
pub(crate) struct BundleOpsColumns {
    pub(crate) id_actor: Range<usize>,
    pub(crate) id_ctr: Range<usize>,
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
    pub(crate) succ_count: Range<usize>,
    pub(crate) succ_actor: Range<usize>,
    pub(crate) succ_ctr: Range<usize>,
    pub(crate) expand: Range<usize>,
    pub(crate) mark_name: Range<usize>,
    pub(crate) hint: Range<usize>,
}

#[derive(Default)]
pub(crate) struct BundleChangeColumns {
    actor: Range<usize>,
    seq: Range<usize>,
    max_op: Range<usize>,
    num_ops: Range<usize>,
    timestamp: Range<usize>,
    message: Range<usize>,
    dep_count: Range<usize>,
    deps: Range<usize>,
    extra_meta: Range<usize>,
    extra: Range<usize>,
}

impl BundleChangeColumns {
    fn raw_columns(&self) -> RawColumns<compression::Uncompressed> {
        [
            (change::ACTOR, &self.actor),
            (change::SEQ, &self.seq),
            (change::NUM_OPS, &self.num_ops),
            (change::MAX_OP, &self.max_op),
            (change::TIMESTAMP, &self.timestamp),
            (change::MESSAGE, &self.message),
            (change::DEP_COUNT, &self.dep_count),
            (change::DEPS, &self.deps),
            (change::EXTRA_META, &self.extra_meta),
            (change::EXTRA, &self.extra),
        ]
        .into_iter()
        .filter(|(_, range)| !range.is_empty())
        .map(|(spec, range)| RawColumn::new(spec, range.clone()))
        .collect()
    }
}

impl BundleOpsColumns {
    pub(crate) fn raw_columns(&self) -> RawColumns<compression::Uncompressed> {
        [
            (ops::OBJ_ACTOR, &self.obj_actor),
            (ops::OBJ_CTR, &self.obj_ctr),
            (ops::KEY_ACTOR, &self.key_actor),
            (ops::KEY_CTR, &self.key_ctr),
            (ops::KEY_STR, &self.key_str),
            (ops::ID_ACTOR, &self.id_actor),
            // shares ID_COL_ID with ID_ACTOR, so it belongs here to keep
            // the column list in ascending spec order
            (ops::ID_CTR, &self.id_ctr),
            (ops::INSERT, &self.insert),
            (ops::ACTION, &self.action),
            (ops::VALUE_META, &self.value_meta),
            (ops::VALUE, &self.value),
            (ops::PRED_COUNT, &self.pred_count),
            (ops::PRED_ACTOR, &self.pred_actor),
            (ops::PRED_CTR, &self.pred_ctr),
            (ops::SUCC_COUNT, &self.succ_count),
            (ops::SUCC_ACTOR, &self.succ_actor),
            (ops::SUCC_CTR, &self.succ_ctr),
            (ops::EXPAND, &self.expand),
            (ops::MARK_NAME, &self.mark_name),
            (ops::HINT, &self.hint),
        ]
        .into_iter()
        .filter(|(_, range)| !range.is_empty())
        .map(|(spec, range)| RawColumn::new(spec, range.clone()))
        .collect()
    }
}

#[derive(Debug)]
struct ChangeBuilder {
    actor: usize,
    seq: u64,
    change: usize,
    start_op: u64,
    max_op: u64,
}

/// A bundle's change-metadata columns as raw byte slices.
///
/// The columnar counterpart to [`BundleChangeIterUnverified`]: where the
/// iterator materialises a [`BundleChange`] per member (a `Vec` of deps,
/// an owned message, an owned extra), this hands out the columns so a
/// consumer can decode one column at a time — which is how the document
/// load path builds the same graph state. An absent column is an empty
/// slice, which every decoder reads as all-default.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct BundleChangeCols<'a> {
    pub(crate) actor: &'a [u8],
    pub(crate) seq: &'a [u8],
    pub(crate) num_ops: &'a [u8],
    pub(crate) max_op: &'a [u8],
    pub(crate) timestamp: &'a [u8],
    pub(crate) message: &'a [u8],
    pub(crate) dep_count: &'a [u8],
    pub(crate) deps: &'a [u8],
    pub(crate) extra_meta: &'a [u8],
    pub(crate) extra: &'a [u8],
}

impl<'a> BundleChangeCols<'a> {
    pub(crate) fn try_new(
        columns: &RawColumns<compression::Uncompressed>,
        data: &'a [u8],
    ) -> Result<Self, ParseError> {
        let mut c = Self::default();
        for col in columns.iter() {
            let d = &data[col.data()];
            match col.spec() {
                change::ACTOR => c.actor = d,
                change::SEQ => c.seq = d,
                change::NUM_OPS => c.num_ops = d,
                change::MAX_OP => c.max_op = d,
                change::TIMESTAMP => c.timestamp = d,
                change::MESSAGE => c.message = d,
                change::DEP_COUNT => c.dep_count = d,
                change::DEPS => c.deps = d,
                change::EXTRA_META => c.extra_meta = d,
                change::EXTRA => c.extra = d,
                spec => return Err(ParseError::InvalidChangeColumn(u32::from(spec))),
            }
        }
        Ok(c)
    }

    pub(crate) fn actors(&self) -> hexane::Decoder<'a, Option<ActorIdx>> {
        hexane::decoder::<Option<ActorIdx>>(self.actor)
    }

    pub(crate) fn seqs(&self) -> hexane::DeltaDecoder<'a, Option<i64>> {
        hexane::DeltaDecoder::<Option<i64>>::new(self.seq)
    }

    pub(crate) fn num_ops(&self) -> hexane::Decoder<'a, Option<u64>> {
        hexane::decoder::<Option<u64>>(self.num_ops)
    }

    pub(crate) fn max_ops(&self) -> hexane::DeltaDecoder<'a, Option<i64>> {
        hexane::DeltaDecoder::<Option<i64>>::new(self.max_op)
    }

    pub(crate) fn timestamps(&self) -> hexane::DeltaDecoder<'a, Option<i64>> {
        hexane::DeltaDecoder::<Option<i64>>::new(self.timestamp)
    }

    pub(crate) fn messages(&self) -> hexane::Decoder<'a, Option<String>> {
        hexane::decoder::<Option<String>>(self.message)
    }

    pub(crate) fn dep_counts(&self) -> hexane::Decoder<'a, Option<u64>> {
        hexane::decoder::<Option<u64>>(self.dep_count)
    }

    pub(crate) fn dep_values(&self) -> hexane::DeltaDecoder<'a, Option<i64>> {
        hexane::DeltaDecoder::<Option<i64>>::new(self.deps)
    }

    pub(crate) fn extra_metas(&self) -> hexane::Decoder<'a, Option<ValueMeta>> {
        hexane::decoder::<Option<ValueMeta>>(self.extra_meta)
    }
}

#[derive(Debug)]
pub(crate) struct BundleChangeIterUnverified<'a> {
    inner: Option<BundleChangeIterInner<'a>>,
}

#[derive(Debug)]
struct BundleChangeIterInner<'a> {
    actor: hexane::Decoder<'a, Option<ActorIdx>>,
    seq: hexane::DeltaDecoder<'a, Option<i64>>,
    max_op: hexane::DeltaDecoder<'a, Option<i64>>,
    num_ops: hexane::Decoder<'a, Option<u64>>,
    timestamp: hexane::DeltaDecoder<'a, Option<i64>>,
    message: hexane::Decoder<'a, Option<String>>,
    dep_count: hexane::Decoder<'a, Option<u64>>,
    deps: hexane::DeltaDecoder<'a, Option<i64>>,
    extra_meta: hexane::Decoder<'a, Option<ValueMeta>>,
    extra: &'a [u8],
}

impl<'a> Iterator for BundleChangeIterUnverified<'a> {
    type Item = Result<BundleChange<'a>, ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .as_mut()?
            .try_next()
            .inspect_err(|_| self.inner = None)
            .transpose()
    }
}

impl<'a> BundleChangeIterUnverified<'a> {
    pub(crate) fn try_new(
        columns: &RawColumns<compression::Uncompressed>,
        data: &'a [u8],
    ) -> Result<Self, ParseError> {
        Ok(Self {
            inner: Some(BundleChangeIterInner::try_new(columns, data)?),
        })
    }
}

impl<'a> BundleChangeIterInner<'a> {
    fn try_new(
        columns: &RawColumns<compression::Uncompressed>,
        data: &'a [u8],
    ) -> Result<Self, ParseError> {
        let cols = BundleChangeCols::try_new(columns, data)?;
        Ok(Self {
            actor: cols.actors(),
            seq: cols.seqs(),
            num_ops: cols.num_ops(),
            max_op: cols.max_ops(),
            timestamp: cols.timestamps(),
            message: cols.messages(),
            dep_count: cols.dep_counts(),
            deps: cols.dep_values(),
            extra_meta: cols.extra_metas(),
            extra: cols.extra,
        })
    }

    fn try_next(&mut self) -> Result<Option<BundleChange<'a>>, ParseError> {
        let actor = match self.actor.next().flatten() {
            Some(a) => a.into(),
            None => return Ok(None),
        };
        let seq = self
            .seq
            .next()
            .flatten()
            .ok_or(ReadOpError::MissingValue("seq"))? as u64;
        let num_ops = self
            .num_ops
            .next()
            .flatten()
            .ok_or(ReadOpError::MissingValue("num_ops"))?;
        let max_op = self
            .max_op
            .next()
            .flatten()
            .ok_or(ReadOpError::MissingValue("max_op"))? as u64;
        // the wire carries the op count; a change wants the range's foot
        let start_op = (max_op + 1)
            .checked_sub(num_ops)
            .filter(|s| *s > 0)
            .ok_or(ReadOpError::MissingValue("num_ops"))?;
        let timestamp = self.timestamp.next().flatten().unwrap_or(0);
        let message = self.message.next().flatten().map(Cow::Borrowed);
        let dep_count = self.dep_count.next().flatten().unwrap_or(0) as usize;

        let mut deps = Vec::with_capacity(dep_count);
        for _ in 0..dep_count {
            let dep = self
                .deps
                .next()
                .flatten()
                .ok_or(ReadOpError::MissingValue("dep"))? as u64;
            deps.push(dep);
        }

        let extra_count = self.extra_meta.next().flatten().map_or(0, |m| m.length());
        if extra_count > self.extra.len() {
            return Err(ReadOpError::MissingValue("extra").into());
        }
        let (extra, tail) = self.extra.split_at(extra_count);
        let extra = Cow::Borrowed(extra);
        self.extra = tail;

        Ok(Some(BundleChange {
            actor,
            author: None,
            seq,
            max_op,
            start_op,
            timestamp,
            message,
            deps,
            extra,
        }))
    }
}

pub(crate) struct OpIterUnverified<'a> {
    inner: Option<OpIterInner<'a>>,
}

impl<'a> OpIterUnverified<'a> {
    pub(crate) fn new(columns: &RawColumns<compression::Uncompressed>, data: &'a [u8]) -> Self {
        Self {
            inner: OpIterInner::try_new(columns, data).ok(),
        }
    }
}

struct OpIterInner<'a> {
    obj_actor: hexane::Decoder<'a, Option<ActorIdx>>,
    obj_ctr: hexane::Decoder<'a, Option<u64>>,
    key_actor: hexane::Decoder<'a, Option<ActorIdx>>,
    key_ctr: hexane::DeltaDecoder<'a, Option<i64>>,
    key_str: hexane::Decoder<'a, Option<String>>,
    id_actor: hexane::Decoder<'a, Option<ActorIdx>>,
    /// Doc-order counter values reconstructed at parse time.
    id_ctr: hexane::DeltaDecoder<'a, Option<i64>>,
    insert: hexane::Decoder<'a, bool>,
    action: hexane::Decoder<'a, Option<Action>>,
    meta: hexane::Decoder<'a, Option<ValueMeta>>,
    pred_count: hexane::Decoder<'a, Option<u64>>,
    pred_actor: hexane::Decoder<'a, Option<ActorIdx>>,
    pred_ctr: hexane::DeltaDecoder<'a, Option<i64>>,
    succ_count: hexane::Decoder<'a, Option<u64>>,
    succ_actor: hexane::Decoder<'a, Option<ActorIdx>>,
    succ_ctr: hexane::DeltaDecoder<'a, Option<i64>>,
    expand: hexane::Decoder<'a, bool>,
    mark_name: hexane::Decoder<'a, Option<String>>,
    value: &'a [u8],
}

/// One bundle op row: the op itself (pred = references to ops before
/// the bundle) plus its in-bundle successors from the succ column.
/// `succ` is empty for bundles predating the succ column — those carry
/// every relationship (and every delete) in pred/rows instead.
#[derive(Debug, Clone)]
pub(crate) struct BundleOp<'a> {
    pub(crate) op: OpBuilder<'a>,
    pub(crate) succ: Vec<OpId>,
}

impl<'a> Iterator for OpIterUnverified<'a> {
    type Item = Result<BundleOp<'a>, ParseError>;

    fn next(&mut self) -> Option<Result<BundleOp<'a>, ParseError>> {
        self.inner
            .as_mut()?
            .try_next()
            .inspect_err(|_| self.inner = None)
            .transpose()
    }
}

impl<'a> OpIterInner<'a> {
    fn try_next(&mut self) -> Result<Option<BundleOp<'a>>, ParseError> {
        let id_actor = self.id_actor.next().flatten();
        let id_ctr = self.id_ctr.next().flatten();
        let id = match OpId::try_load(id_actor, id_ctr) {
            Ok(id) => id,
            Err(_) => return Ok(None),
        };

        let obj_actor = self.obj_actor.next().flatten();
        let obj_ctr = self.obj_ctr.next().flatten().map(|v| v as i64);
        let obj = ObjId::try_load(obj_actor, obj_ctr)?;

        let key_str = self.key_str.next().flatten();
        let key_actor = self.key_actor.next().flatten();
        let key_ctr = self.key_ctr.next().flatten();
        let key = KeyRef::try_load(key_str, key_actor, key_ctr)?;

        let action = self
            .action
            .next()
            .flatten()
            .ok_or(ReadOpError::MissingValue("action"))?;
        let insert = self.insert.next().unwrap_or_default();
        let expand = self.expand.next().unwrap_or_default();
        let mark_name = self.mark_name.next().flatten().map(Cow::Borrowed);

        let value_meta = self
            .meta
            .next()
            .flatten()
            .ok_or(ReadOpError::MissingValue("value_meta"))?;
        let (value_raw, tail) = self.value.split_at(value_meta.length());
        self.value = tail;
        let value = ScalarValue::from_raw(value_meta, value_raw)
            .map_err(|_| ReadOpError::MissingValue("value"))?;

        let pred_count = self.pred_count.next().flatten().unwrap_or(0) as usize;
        let mut pred = Vec::with_capacity(pred_count);
        for _ in 0..pred_count {
            let pred_actor = self.pred_actor.next().flatten();
            let pred_ctr = self.pred_ctr.next().flatten();
            pred.push(OpId::try_load(pred_actor, pred_ctr)?);
        }

        let succ_count = self.succ_count.next().flatten().unwrap_or(0) as usize;
        let mut succ = Vec::with_capacity(succ_count);
        for _ in 0..succ_count {
            let succ_actor = self.succ_actor.next().flatten();
            let succ_ctr = self.succ_ctr.next().flatten();
            succ.push(OpId::try_load(succ_actor, succ_ctr)?);
        }

        Ok(Some(BundleOp {
            op: OpBuilder {
                id,
                obj,
                action,
                key,
                value,
                insert,
                expand,
                mark_name,
                pred,
            },
            succ,
        }))
    }

    fn try_new(
        columns: &RawColumns<compression::Uncompressed>,
        data: &'a [u8],
    ) -> Result<Self, ParseError> {
        let mut obj_actor = hexane::decoder::<Option<ActorIdx>>(&[]);
        let mut obj_ctr = hexane::decoder::<Option<u64>>(&[]);
        let mut key_actor = hexane::decoder::<Option<ActorIdx>>(&[]);
        let mut key_ctr = hexane::DeltaDecoder::<Option<i64>>::new(&[]);
        let mut key_str = hexane::decoder::<Option<String>>(&[]);
        let mut id_actor = hexane::decoder::<Option<ActorIdx>>(&[]);
        let mut id_ctr = hexane::DeltaDecoder::<Option<i64>>::new(&[]);
        let mut insert = hexane::decoder::<bool>(&[]);
        let mut action = hexane::decoder::<Option<Action>>(&[]);
        let mut meta = hexane::decoder::<Option<ValueMeta>>(&[]);
        let mut pred_count = hexane::decoder::<Option<u64>>(&[]);
        let mut pred_actor = hexane::decoder::<Option<ActorIdx>>(&[]);
        let mut pred_ctr = hexane::DeltaDecoder::<Option<i64>>::new(&[]);
        let mut succ_count = hexane::decoder::<Option<u64>>(&[]);
        let mut succ_actor = hexane::decoder::<Option<ActorIdx>>(&[]);
        let mut succ_ctr = hexane::DeltaDecoder::<Option<i64>>::new(&[]);
        let mut expand = hexane::decoder::<bool>(&[]);
        let mut mark_name = hexane::decoder::<Option<String>>(&[]);
        let mut value: &[u8] = &[];

        for col in columns.iter() {
            let d = &data[col.data()];
            type C = ColumnType;
            match (col.spec().id(), col.spec().col_type()) {
                (ops::OBJ_COL_ID, C::Actor) => obj_actor = hexane::decoder::<Option<ActorIdx>>(d),
                (ops::OBJ_COL_ID, C::Integer) => obj_ctr = hexane::decoder::<Option<u64>>(d),
                (ops::KEY_COL_ID, C::Actor) => key_actor = hexane::decoder::<Option<ActorIdx>>(d),
                (ops::KEY_COL_ID, C::DeltaInteger) => {
                    key_ctr = hexane::DeltaDecoder::<Option<i64>>::new(d)
                }
                (ops::KEY_COL_ID, C::String) => key_str = hexane::decoder::<Option<String>>(d),
                (ops::ID_COL_ID, C::Actor) => id_actor = hexane::decoder::<Option<ActorIdx>>(d),
                // Both counter encodings are handled at the storage layer.
                (ops::ID_COL_ID, C::DeltaInteger) => {
                    id_ctr = hexane::DeltaDecoder::<Option<i64>>::new(d)
                }
                (ops::INSERT_COL_ID, C::Boolean) => insert = hexane::decoder::<bool>(d),
                (ops::ACTION_COL_ID, C::Integer) => action = hexane::decoder::<Option<Action>>(d),
                (ops::VAL_COL_ID, C::ValueMetadata) => {
                    meta = hexane::decoder::<Option<ValueMeta>>(d)
                }
                (ops::VAL_COL_ID, C::Value) => value = d,
                (ops::PRED_COL_ID, C::Group) => pred_count = hexane::decoder::<Option<u64>>(d),
                (ops::PRED_COL_ID, C::Actor) => pred_actor = hexane::decoder::<Option<ActorIdx>>(d),
                (ops::PRED_COL_ID, C::DeltaInteger) => {
                    pred_ctr = hexane::DeltaDecoder::<Option<i64>>::new(d)
                }
                (ops::HINT_COL_ID, C::DeltaInteger) => {}
                (ops::SUCC_COL_ID, C::Group) => succ_count = hexane::decoder::<Option<u64>>(d),
                (ops::SUCC_COL_ID, C::Actor) => succ_actor = hexane::decoder::<Option<ActorIdx>>(d),
                (ops::SUCC_COL_ID, C::DeltaInteger) => {
                    succ_ctr = hexane::DeltaDecoder::<Option<i64>>::new(d)
                }
                (ops::EXPAND_COL_ID, C::Boolean) => expand = hexane::decoder::<bool>(d),
                (ops::MARK_NAME_COL_ID, C::String) => {
                    mark_name = hexane::decoder::<Option<String>>(d)
                }
                _ => return Err(ParseError::InvalidOpColumn(u32::from(col.spec()))),
            }
        }
        Ok(Self {
            obj_actor,
            obj_ctr,
            key_actor,
            key_ctr,
            key_str,
            id_actor,
            id_ctr,
            insert,
            action,
            meta,
            value,
            pred_count,
            pred_actor,
            pred_ctr,
            succ_count,
            succ_actor,
            succ_ctr,
            expand,
            mark_name,
        })
    }
}

/// A minimally-decoded fragment op for the streaming manifold: no
/// marks, actor indexes already doc-mapped.
#[derive(Debug)]
pub(crate) struct FragOp<'a> {
    pub(crate) id: OpId,
    pub(crate) obj: ObjId,
    pub(crate) key: FragKey<'a>,
    pub(crate) insert: bool,
    pub(crate) action: Action,
    /// external (doc-row) predecessors
    pub(crate) preds: Vec<OpId>,
    /// no in-fragment successor deletes this op (normalized: only an
    /// increment succ on a counter keeps it alive)
    pub(crate) alive: bool,
    /// increments only: the amount this row adds, read from its own
    /// value
    pub(crate) inc: Option<i64>,
    /// in-fragment succ entries this row carries (sub-column width)
    pub(crate) sub_len: usize,
    /// value bytes this row carries
    pub(crate) val_len: usize,
    /// covered-rank position floor for this op's seq target (see
    /// `ops::HINT_COL_ID`)
    pub(crate) hint: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum FragKey<'a> {
    Map(&'a str),
    Seq(ElemId),
}

impl FragKey<'_> {
    pub(crate) fn key_str(&self) -> Option<&str> {
        match self {
            FragKey::Map(s) => Some(s),
            FragKey::Seq(_) => None,
        }
    }

    pub(crate) fn elemid(&self) -> Option<ElemId> {
        match self {
            FragKey::Map(_) => None,
            FragKey::Seq(e) => Some(*e),
        }
    }
}

/// How many of the next `max` items satisfy `pred`, counted run by run
/// so a repeat of ten thousand costs one test.
fn run_len_while<D: hexane::RunDecoder>(
    mut d: D,
    max: usize,
    pred: impl Fn(&D::Item) -> bool,
) -> usize {
    let mut n = 0;
    while n < max {
        match d.next_run_max(max - n) {
            Some(run) if pred(&run.value) => n += run.count,
            _ => break,
        }
    }
    n
}

// `MakeTable` is deliberately absent: it has no `ObjType`, so the
// manifold's `ObjType::try_from` never records it in `obj_info` either
fn makes_no_object(a: &Option<Action>) -> bool {
    !matches!(
        a,
        Some(Action::MakeMap) | Some(Action::MakeList) | Some(Action::MakeText)
    )
}

fn is_zero_count(c: &Option<u64>) -> bool {
    c.unwrap_or(0) == 0
}

fn last_true_offset(mut d: hexane::Decoder<'_, bool>, max: usize) -> Option<usize> {
    let mut seen = 0;
    let mut last = None;
    while seen < max {
        let Some(run) = d.next_run_max(max - seen) else {
            break;
        };
        if run.value {
            last = Some(seen + run.count - 1);
        }
        seen += run.count;
    }
    last
}

/// A run of clean inserts, taken without decoding: its last row's id
/// (the manifold registers it as a candidate) and the value bytes the
/// run carries.
pub(crate) struct CleanRun {
    pub(crate) last_id: OpId,
    pub(crate) val_bytes: usize,
}

/// What a bulk tail skip resolved: see [`FragOps::skip_tail`].
pub(crate) struct TailRun {
    /// succ entries the skipped rows carry, in total
    pub(crate) sub: usize,
    /// value bytes the skipped rows carry, in total
    pub(crate) val: usize,
    /// offset (from the run's first row) and id of its last insert
    pub(crate) last_insert: Option<(usize, OpId)>,
}

/// Long-lived forward-only streaming reader over a fragment's op
/// columns — the fragment-side counterpart of the manifold's document
/// iterators. Only what the manifold consults is decoded: no marks, and
/// of the value column only an increment's own amount (the rest of it
/// is stepped over by width). Run-level peeks power the tail fast path.
#[derive(Clone)]
pub(crate) struct FragOps<'a> {
    pub(crate) pos: usize,
    pub(crate) len: usize,
    /// succ entries and value bytes the whole fragment holds — parse
    /// rejects columns that disagree with the per-row counts, so these
    /// are the sums, already taken
    succ_entries: usize,
    value_bytes: usize,
    actor_map: &'a [usize],
    id_actor: hexane::Decoder<'a, Option<ActorIdx>>,
    id_ctr: hexane::DeltaDecoder<'a, Option<i64>>,
    obj_actor: hexane::Decoder<'a, Option<ActorIdx>>,
    obj_ctr: hexane::Decoder<'a, Option<u64>>,
    key_actor: hexane::Decoder<'a, Option<ActorIdx>>,
    key_ctr: hexane::DeltaDecoder<'a, Option<i64>>,
    key_str: hexane::Decoder<'a, Option<String>>,
    insert: hexane::Decoder<'a, bool>,
    action: hexane::Decoder<'a, Option<Action>>,
    pred_count: hexane::Decoder<'a, Option<u64>>,
    pred_actor: hexane::Decoder<'a, Option<ActorIdx>>,
    pred_ctr: hexane::DeltaDecoder<'a, Option<i64>>,
    succ_count: hexane::Decoder<'a, Option<u64>>,
    succ_actor: hexane::Decoder<'a, Option<ActorIdx>>,
    succ_ctr: hexane::DeltaDecoder<'a, Option<i64>>,
    value_meta: hexane::Decoder<'a, Option<ValueMeta>>,
    /// the raw value column, and how far into it the walk has read
    value: &'a [u8],
    val_pos: usize,
    /// the fragment's `inc` index and the walk's position in it. Keyed
    /// by succ position: an entry is `Some` exactly when that successor
    /// is an increment. Only a counter with successors ever reads it, so
    /// the position is carried as a count and the column is seeked when
    /// one turns up — a fragment without counters never touches it.
    inc: &'a hexane::Column<Option<i64>>,
    sub_pos: usize,
    hint: hexane::DeltaDecoder<'a, Option<i64>>,
    /// raw (unmapped) obj actor of the last-read op, for same-obj run
    /// peeks against the raw column
    cur_obj_raw: (Option<ActorIdx>, Option<u64>),
    /// elided columns decode as empty but mean "all default": the run
    /// peeks must treat them as unbounded default runs
    pred_absent: bool,
    succ_absent: bool,
    obj_absent: bool,
}

impl<'a> FragOps<'a> {
    pub(crate) fn new(
        columns: &RawColumns<compression::Uncompressed>,
        data: &'a [u8],
        len: usize,
        actor_map: &'a [usize],
        succ_entries: usize,
        value_bytes: usize,
        inc: &'a hexane::Column<Option<i64>>,
    ) -> Self {
        let mut s = FragOps {
            pos: 0,
            len,
            succ_entries,
            value_bytes,
            actor_map,
            id_actor: hexane::decoder::<Option<ActorIdx>>(&[]),
            id_ctr: hexane::DeltaDecoder::<Option<i64>>::new(&[]),
            obj_actor: hexane::decoder::<Option<ActorIdx>>(&[]),
            obj_ctr: hexane::decoder::<Option<u64>>(&[]),
            key_actor: hexane::decoder::<Option<ActorIdx>>(&[]),
            key_ctr: hexane::DeltaDecoder::<Option<i64>>::new(&[]),
            key_str: hexane::decoder::<Option<String>>(&[]),
            insert: hexane::decoder::<bool>(&[]),
            action: hexane::decoder::<Option<Action>>(&[]),
            pred_count: hexane::decoder::<Option<u64>>(&[]),
            pred_actor: hexane::decoder::<Option<ActorIdx>>(&[]),
            pred_ctr: hexane::DeltaDecoder::<Option<i64>>::new(&[]),
            succ_count: hexane::decoder::<Option<u64>>(&[]),
            succ_actor: hexane::decoder::<Option<ActorIdx>>(&[]),
            succ_ctr: hexane::DeltaDecoder::<Option<i64>>::new(&[]),
            value_meta: hexane::decoder::<Option<ValueMeta>>(&[]),
            value: &[],
            val_pos: 0,
            inc,
            sub_pos: 0,
            hint: hexane::DeltaDecoder::<Option<i64>>::new(&[]),
            cur_obj_raw: (None, None),
            pred_absent: true,
            succ_absent: true,
            obj_absent: true,
        };
        for col in columns.iter() {
            let d = &data[col.data()];
            type C = ColumnType;
            match (col.spec().id(), col.spec().col_type()) {
                (ops::OBJ_COL_ID, C::Actor) => {
                    s.obj_actor = hexane::decoder::<Option<ActorIdx>>(d);
                    s.obj_absent = d.is_empty();
                }
                (ops::OBJ_COL_ID, C::Integer) => s.obj_ctr = hexane::decoder::<Option<u64>>(d),
                (ops::KEY_COL_ID, C::Actor) => s.key_actor = hexane::decoder::<Option<ActorIdx>>(d),
                (ops::KEY_COL_ID, C::DeltaInteger) => {
                    s.key_ctr = hexane::DeltaDecoder::<Option<i64>>::new(d)
                }
                (ops::KEY_COL_ID, C::String) => s.key_str = hexane::decoder::<Option<String>>(d),
                (ops::ID_COL_ID, C::Actor) => s.id_actor = hexane::decoder::<Option<ActorIdx>>(d),
                (ops::ID_COL_ID, C::DeltaInteger) => {
                    s.id_ctr = hexane::DeltaDecoder::<Option<i64>>::new(d)
                }
                (ops::INSERT_COL_ID, C::Boolean) => s.insert = hexane::decoder::<bool>(d),
                (ops::ACTION_COL_ID, C::Integer) => s.action = hexane::decoder::<Option<Action>>(d),
                (ops::PRED_COL_ID, C::Group) => {
                    s.pred_count = hexane::decoder::<Option<u64>>(d);
                    s.pred_absent = d.is_empty();
                }
                (ops::PRED_COL_ID, C::Actor) => {
                    s.pred_actor = hexane::decoder::<Option<ActorIdx>>(d)
                }
                (ops::PRED_COL_ID, C::DeltaInteger) => {
                    s.pred_ctr = hexane::DeltaDecoder::<Option<i64>>::new(d)
                }
                (ops::SUCC_COL_ID, C::Group) => {
                    s.succ_count = hexane::decoder::<Option<u64>>(d);
                    s.succ_absent = d.is_empty();
                }
                (ops::SUCC_COL_ID, C::Actor) => {
                    s.succ_actor = hexane::decoder::<Option<ActorIdx>>(d)
                }
                (ops::SUCC_COL_ID, C::DeltaInteger) => {
                    s.succ_ctr = hexane::DeltaDecoder::<Option<i64>>::new(d)
                }
                (ops::VAL_COL_ID, C::ValueMetadata) => {
                    s.value_meta = hexane::decoder::<Option<ValueMeta>>(d)
                }
                (ops::VAL_COL_ID, C::Value) => s.value = d,
                (ops::HINT_COL_ID, C::DeltaInteger) => {
                    s.hint = hexane::DeltaDecoder::<Option<i64>>::new(d)
                }
                _ => {}
            }
        }
        s
    }

    /// Decode the next op (minimal fields), doc-mapping every actor.
    pub(crate) fn next_op(&mut self) -> FragOp<'a> {
        debug_assert!(self.pos < self.len, "read past the end of the fragment");
        self.pos += 1;

        let actor = self.id_actor.next().flatten().expect("id actor");
        let ctr = self.id_ctr.next().flatten().expect("id ctr");
        let id = OpId::new(ctr as u64, self.actor_map[usize::from(actor)]);

        let oa = self.obj_actor.next().flatten();
        let oc = self.obj_ctr.next().flatten();
        self.cur_obj_raw = (oa, oc);
        let obj = match (oa, oc) {
            (Some(a), Some(c)) if c > 0 => ObjId(OpId::new(c, self.actor_map[usize::from(a)])),
            _ => ObjId::root(),
        };

        let ks = self.key_str.next().flatten();
        let ka = self.key_actor.next().flatten();
        let kc = self.key_ctr.next().flatten();
        let key = match ks {
            Some(sv) => FragKey::Map(sv),
            None => {
                let e = match (ka, kc) {
                    (None, Some(0)) | (None, None) => ElemId(OpId::new(0, 0)),
                    (Some(a), Some(c)) => {
                        ElemId(OpId::new(c as u64, self.actor_map[usize::from(a)]))
                    }
                    _ => panic!("invalid elem key"),
                };
                FragKey::Seq(e)
            }
        };

        let insert = self.insert.next().unwrap_or_default();
        let action = self.action.next().flatten().expect("action");

        let n_pred = self.pred_count.next().flatten().unwrap_or(0) as usize;
        let mut preds = Vec::with_capacity(n_pred);
        for _ in 0..n_pred {
            let pa = self.pred_actor.next().flatten().expect("pred actor");
            let pc = self.pred_ctr.next().flatten().expect("pred ctr");
            preds.push(OpId::new(pc as u64, self.actor_map[usize::from(pa)]));
        }

        // the row's own value says what it is: a counter, and — for an
        // increment — how much it adds
        let vm = self.value_meta.next().flatten();
        let val_len = vm.map_or(0, |m| m.length());
        let is_counter =
            vm.is_some_and(|m| m.type_code() == crate::op_set2::meta::ValueType::Counter);
        // only an increment's value is ever read; the rest just move the
        // cursor past their bytes
        let inc = (action == Action::Increment).then(|| {
            let raw = &self.value[self.val_pos..self.val_pos + val_len];
            match ScalarValue::from_raw(vm.expect("an increment has a value"), raw)
                .expect("validated value")
            {
                ScalarValue::Int(i) => i,
                ScalarValue::Uint(u) => u as i64,
                _ => 0,
            }
        });
        self.val_pos += val_len;

        let n_succ = self.succ_count.next().flatten().unwrap_or(0) as usize;
        self.succ_actor.advance_by(n_succ);
        self.succ_ctr.advance_by(n_succ);
        let sub = self.sub_pos;
        self.sub_pos += n_succ;
        // a row with successors dies unless it is a counter and every one
        // of them is an increment, which the `inc` index says outright —
        // and nothing else reads it, so a non-counter never seeks it
        let alive = n_succ == 0
            || (is_counter && self.inc.iter_range(sub..sub + n_succ).all(|v| v.is_some()));

        let hint = self.hint.next().flatten().map(|h| h as u64);

        FragOp {
            id,
            obj,
            key,
            insert,
            action,
            preds,
            alive,
            inc,
            sub_len: n_succ,
            val_len,
            hint,
        }
    }

    /// How many upcoming ops are a *clean run*: same object, insert
    /// rows, no preds and no succ. The manifold takes such a run
    /// wholesale — one position push, no per-op scope work.
    pub(crate) fn clean_insert_run(&self) -> usize {
        let mut n = self.len - self.pos;
        if n == 0 {
            return 0;
        }
        n = n.min(run_len_while(self.insert.clone(), n, |i| *i));
        if n == 0 {
            return 0;
        }
        // only plain Set inserts: Make ops must register obj_info and
        // Mark/Increment rows carry semantics the skip would drop
        n = n.min(run_len_while(self.action.clone(), n, |a| {
            *a == Some(Action::Set)
        }));
        if n == 0 {
            return 0;
        }
        if !self.pred_absent {
            n = n.min(run_len_while(self.pred_count.clone(), n, is_zero_count));
        }
        if n == 0 {
            return 0;
        }
        if !self.succ_absent {
            n = n.min(run_len_while(self.succ_count.clone(), n, is_zero_count));
        }
        if n == 0 {
            return 0;
        }
        n.min(self.same_obj_run(n))
    }

    /// How many upcoming ops carry no external preds (bounded).
    pub(crate) fn pred_free_run(&self) -> usize {
        if self.pred_absent {
            return self.len - self.pos;
        }
        run_len_while(self.pred_count.clone(), self.len - self.pos, is_zero_count)
    }

    /// How many upcoming ops still belong to the last-read op's object
    /// (bounded by `max`).
    pub(crate) fn same_obj_run(&self, max: usize) -> usize {
        if self.obj_absent {
            // every op is in the root object
            return max.min(self.len - self.pos);
        }
        // same object ⇔ both obj columns keep repeating their value
        let mut n = 0;
        let mut ctr = self.obj_ctr.clone();
        while n < max {
            match ctr.next_run_max(max - n) {
                Some(run) if run.value == self.cur_obj_raw.1 => n += run.count,
                _ => break,
            }
        }
        let mut m = 0;
        let mut act = self.obj_actor.clone();
        while m < n {
            match act.next_run_max(n - m) {
                Some(run) if run.value == self.cur_obj_raw.0 => m += run.count,
                _ => break,
            }
        }
        m
    }

    /// How many upcoming ops of a tail run need no per-op attention at
    /// all: the manifold's blank mode reads nothing but the row count,
    /// the succ/value widths and the object-creating rows, so a run
    /// stops only at a `Make*`.
    pub(crate) fn make_free_run(&self, max: usize) -> usize {
        run_len_while(self.action.clone(), max, makes_no_object)
    }

    /// Consume every remaining row, reading nothing.
    ///
    /// The caller is finishing the fragment — no column is read after
    /// this — so the decoders are left where they stand rather than
    /// wound forward through rows nobody will look at. Returns the
    /// extents the copy range ends at, which are the columns' own
    /// lengths.
    pub(crate) fn consume_rest(&mut self) -> (usize, usize) {
        self.pos = self.len;
        (self.succ_entries, self.value_bytes)
    }

    /// Skip `n` tail ops wholesale, advancing every column in step.
    /// Unlike [`skip_clean`](Self::skip_clean) the rows may carry succ
    /// (in-fragment deletes and overwrites), so the succ sub-columns
    /// advance by the run's total. Preds must be absent — the caller
    /// proves that with [`pred_free_run`](Self::pred_free_run) before
    /// entering the tail.
    ///
    /// Returns the run's total succ entries and value bytes (the
    /// manifold's copy-range widths), plus the offset and id of its
    /// last insert row.
    pub(crate) fn skip_tail(&mut self, n: usize) -> TailRun {
        debug_assert!(n > 0 && self.pos + n <= self.len);

        // Single pass over the id columns. Cloning a decoder and calling
        // `nth` reads the run twice — once on the clone, once on the
        // skip — so instead advance the real decoders to `off`, take the
        // value there, and carry on to the end of the run. Measurably
        // faster than two O(runs) traversals.
        let last_insert = match last_true_offset(self.insert.clone(), n) {
            Some(off) => {
                let a = self.id_actor.nth(off).flatten().expect("id actor");
                let c = self.id_ctr.nth(off).flatten().expect("id ctr");
                let rest = n - off - 1;
                self.id_actor.advance_by(rest);
                self.id_ctr.advance_by(rest);
                let id = OpId::new(c as u64, self.actor_map[usize::from(a)]);
                Some((off, id))
            }
            None => {
                self.id_actor.advance_by(n);
                self.id_ctr.advance_by(n);
                None
            }
        };
        self.obj_actor.advance_by(n);
        self.obj_ctr.advance_by(n);
        self.key_actor.advance_by(n);
        self.key_ctr.advance_by(n);
        self.key_str.advance_by(n);
        self.insert.advance_by(n);
        self.action.advance_by(n);
        self.pred_count.advance_by(n);
        self.hint.advance_by(n);

        // succ and value widths ride into the copy range, so both are
        // summed run by run rather than row by row
        let mut sub = 0usize;
        let mut m = 0usize;
        while m < n {
            match self.succ_count.next_run_max(n - m) {
                Some(run) => {
                    sub += run.value.unwrap_or(0) as usize * run.count;
                    m += run.count;
                }
                None => break, // elided column: no successors
            }
        }
        self.succ_actor.advance_by(sub);
        self.succ_ctr.advance_by(sub);

        let mut val = 0usize;
        let mut m = 0usize;
        while m < n {
            match self.value_meta.next_run_max(n - m) {
                Some(run) => {
                    val += run.value.map_or(0, |v| v.length()) * run.count;
                    m += run.count;
                }
                None => break, // elided column: no bytes
            }
        }

        self.pos += n;
        self.val_pos += val;
        self.sub_pos += sub;
        TailRun {
            sub,
            val,
            last_insert,
        }
    }

    /// Skip `n` ops known to have zero preds and zero succ (a clean
    /// run), advancing every column in step. Returns the id of the
    /// last skipped op.
    pub(crate) fn skip_clean(&mut self, n: usize) -> CleanRun {
        debug_assert!(n > 0 && self.pos + n <= self.len);
        let last_actor = self.id_actor.nth(n - 1).flatten().expect("id actor");
        let last_ctr = self.id_ctr.nth(n - 1).flatten().expect("id ctr");
        self.obj_actor.advance_by(n);
        self.obj_ctr.advance_by(n);
        self.key_actor.advance_by(n);
        self.key_ctr.advance_by(n);
        self.key_str.advance_by(n);
        self.insert.advance_by(n);
        self.action.advance_by(n);
        // pred/succ counts are all zero in a clean run: the group
        // sub-columns do not advance
        self.pred_count.advance_by(n);
        self.succ_count.advance_by(n);
        self.hint.advance_by(n);
        // value bytes ride along in the copy ranges: sum the skipped
        // rows' meta lengths run by run
        let mut vbytes = 0usize;
        let mut m = 0usize;
        while m < n {
            match self.value_meta.next_run_max(n - m) {
                Some(run) => {
                    vbytes += run.value.map_or(0, |v| v.length()) * run.count;
                    m += run.count;
                }
                None => break, // elided column: no bytes
            }
        }
        self.pos += n;
        self.val_pos += vbytes;
        CleanRun {
            last_id: OpId::new(last_ctr as u64, self.actor_map[usize::from(last_actor)]),
            val_bytes: vbytes,
        }
    }
}

#[rustfmt::skip]
pub(crate) mod ops {
    use crate::storage::{columns::ColumnId, ColumnSpec};

    pub(super) const OBJ_COL_ID:            ColumnId = ColumnId::new(0);
    pub(super) const KEY_COL_ID:            ColumnId = ColumnId::new(1);
    pub(super) const ID_COL_ID:             ColumnId = ColumnId::new(2);
    pub(super) const INSERT_COL_ID:         ColumnId = ColumnId::new(3);
    pub(super) const ACTION_COL_ID:         ColumnId = ColumnId::new(4);
    pub(super) const VAL_COL_ID:            ColumnId = ColumnId::new(5);
    pub(super) const PRED_COL_ID:           ColumnId = ColumnId::new(7);
    /// In-bundle successors of each op, mirroring the document format's
    /// succ group. Only relationships between two bundle members are
    /// stored here; the pred column holds only references to ops from
    /// before the bundle.
    pub(super) const SUCC_COL_ID:           ColumnId = ColumnId::new(8);
    pub(super) const EXPAND_COL_ID:         ColumnId = ColumnId::new(9);
    pub(super) const MARK_NAME_COL_ID:      ColumnId = ColumnId::new(10);
    /// Per-op position hint: the rank of the op's key-elem row among
    /// the ops covered by the fragment's dependency clock — a sound
    /// lower bound on that row's position in any document the fragment
    /// can apply to, and identical no matter when (or from what doc
    /// state) the fragment is generated. Null for ops without a
    /// covered seq target.
    pub(super) const HINT_COL_ID:           ColumnId = ColumnId::new(12);

    pub(super) const ID_ACTOR:   ColumnSpec = ColumnSpec::new_actor(ID_COL_ID);
    /// Doc-order op counters, the same encoding a document chunk uses.
    pub(super) const ID_CTR:     ColumnSpec = ColumnSpec::new_delta(ID_COL_ID);
    pub(crate) const HINT:       ColumnSpec = ColumnSpec::new_delta(HINT_COL_ID);
    pub(super) const OBJ_ACTOR:  ColumnSpec = ColumnSpec::new_actor(OBJ_COL_ID);
    pub(super) const OBJ_CTR:    ColumnSpec = ColumnSpec::new_integer(OBJ_COL_ID);
    pub(super) const KEY_ACTOR:  ColumnSpec = ColumnSpec::new_actor(KEY_COL_ID);
    pub(super) const KEY_CTR:    ColumnSpec = ColumnSpec::new_delta(KEY_COL_ID);
    pub(super) const KEY_STR:    ColumnSpec = ColumnSpec::new_string(KEY_COL_ID);
    pub(crate) const PRED_COUNT: ColumnSpec = ColumnSpec::new_group(PRED_COL_ID);
    pub(crate) const PRED_ACTOR: ColumnSpec = ColumnSpec::new_actor(PRED_COL_ID);
    pub(crate) const PRED_CTR:   ColumnSpec = ColumnSpec::new_delta(PRED_COL_ID);
    pub(super) const SUCC_COUNT: ColumnSpec = ColumnSpec::new_group(SUCC_COL_ID);
    pub(super) const SUCC_ACTOR: ColumnSpec = ColumnSpec::new_actor(SUCC_COL_ID);
    pub(super) const SUCC_CTR:   ColumnSpec = ColumnSpec::new_delta(SUCC_COL_ID);
    pub(super) const INSERT:     ColumnSpec = ColumnSpec::new_boolean(INSERT_COL_ID);
    pub(super) const ACTION:     ColumnSpec = ColumnSpec::new_integer(ACTION_COL_ID);
    pub(super) const VALUE_META: ColumnSpec = ColumnSpec::new_value_metadata(VAL_COL_ID);
    pub(super) const VALUE:      ColumnSpec = ColumnSpec::new_value(VAL_COL_ID);
    pub(super) const MARK_NAME:  ColumnSpec = ColumnSpec::new_string(MARK_NAME_COL_ID);
    pub(super) const EXPAND:     ColumnSpec = ColumnSpec::new_boolean(EXPAND_COL_ID);
}

#[rustfmt::skip]
pub(crate) mod change {
    use crate::storage::{columns::ColumnId, ColumnSpec};

    pub(super) const ACTOR_COL_ID:           ColumnId = ColumnId::new(0);
    pub(super) const SEQ_COL_ID:             ColumnId = ColumnId::new(0);
    pub(super) const NUM_OPS_COL_ID:         ColumnId = ColumnId::new(1);
    pub(super) const MAX_OP_COL_ID:          ColumnId = ColumnId::new(2);
    pub(super) const TIME_COL_ID:            ColumnId = ColumnId::new(3);
    pub(super) const MESSAGE_COL_ID:         ColumnId = ColumnId::new(4);
    pub(super) const DEPS_COL_ID:            ColumnId = ColumnId::new(5);
    pub(super) const EXTRA_COL_ID:           ColumnId = ColumnId::new(6);

    pub(super) const ACTOR:       ColumnSpec = ColumnSpec::new_actor(ACTOR_COL_ID);
    pub(super) const SEQ:         ColumnSpec = ColumnSpec::new_delta(SEQ_COL_ID);
    pub(super) const NUM_OPS:     ColumnSpec = ColumnSpec::new_integer(NUM_OPS_COL_ID);
    pub(super) const MAX_OP:      ColumnSpec = ColumnSpec::new_delta(MAX_OP_COL_ID);
    pub(super) const TIMESTAMP:   ColumnSpec = ColumnSpec::new_delta(TIME_COL_ID);
    pub(super) const MESSAGE:     ColumnSpec = ColumnSpec::new_string(MESSAGE_COL_ID);
    pub(super) const DEP_COUNT:   ColumnSpec = ColumnSpec::new_group(DEPS_COL_ID);
    pub(super) const DEPS:        ColumnSpec = ColumnSpec::new_delta(DEPS_COL_ID);
    pub(super) const EXTRA_META:  ColumnSpec = ColumnSpec::new_value_metadata(EXTRA_COL_ID);
    pub(super) const EXTRA:       ColumnSpec = ColumnSpec::new_value(EXTRA_COL_ID);
}
