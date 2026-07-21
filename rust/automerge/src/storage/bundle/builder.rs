use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::ops::Range;

use crate::op_set2::change::{length_prefixed_bytes, shift_range, ActorMapper};
use crate::op_set2::meta::ValueMeta;
use crate::op_set2::op::{Op, OpBuilder};
use crate::op_set2::types::{Action, ActorIdx, KeyRef};
use crate::op_set2::{ReadOpError, ScalarValue};
use crate::storage::change::DEFLATE_MIN_SIZE;
use crate::storage::columns::{compression, ColumnType};
use crate::storage::{ChunkType, Header, RawColumn, RawColumns};
use crate::types::{ChangeHash, ElemId, ObjId, OpId};

use super::{Bundle, BundleChange, BundleMetadata, BundleStorage, ParseError};

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
            for (id, pred) in &self.preds {
                let op = Op::del(*id, obj, key.clone());
                let op = op.build(pred.to_vec());
                if let Some(index) = self.builders_index(op.id) {
                    self.op_writer
                        .add_with_target(&op, &[], index, &mut self.mapper, target);
                }
            }
            self.preds.clear();
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
    ) -> Bundle {
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
        // builders are sorted by (actor, seq) — the canonical member
        // order the inverse column is keyed by
        let members: Vec<(usize, u64, u64)> = self
            .builders
            .iter()
            .map(|b| (b.actor, b.start_op, b.max_op))
            .collect();
        let (ops_cols, id_ctr) =
            self.op_writer
                .finish_with_ranks(&mapper, &mut ops_data_buf, &members, ranks);
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

        let header_u = Header::new(ChunkType::Bundle, &data_u);
        let mut bytes_u = Vec::with_capacity(header_u.len() + data_u.len());
        header_u.write(&mut bytes_u);
        bytes_u.extend(data_u);

        let changes_data_u_range =
            shift_range(changes_data_start_u..changes_data_end_u, header_u.len());
        let ops_data_u_range = shift_range(ops_data_start_u..ops_data_end_u, header_u.len());

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

        let header_c = Header::new(ChunkType::Bundle, &data_c);
        let mut bytes_c = Vec::with_capacity(header_c.len() + data_c.len());
        header_c.write(&mut bytes_c);
        bytes_c.extend(data_c);

        let storage = BundleStorage {
            bytes: Cow::Owned(bytes_u),
            compressed_bytes: Some(Cow::Owned(bytes_c)),
            header: header_u,
            ops_meta,
            ops_data: ops_data_u_range,
            deps,
            actors,
            changes_meta,
            changes_data: changes_data_u_range,
            id_ctr,
            _phantom: PhantomData,
        };

        Bundle { storage }
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
    start_op: hexane::DeltaEncoder<'a, i64>,
    max_op: hexane::DeltaEncoder<'a, i64>,
    timestamp: hexane::DeltaEncoder<'a, i64>,
    message: hexane::Encoder<'a, Option<String>>,
    dep_count: hexane::Encoder<'a, u32>,
    deps: hexane::DeltaEncoder<'a, i64>,
    extra_count: hexane::Encoder<'a, u32>,
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
        self.start_op.append(change.start_op as i64);
        self.max_op.append(change.max_op as i64);
        self.message
            .append_owned(change.message.as_deref().map(str::to_owned));
        self.timestamp.append(change.timestamp);
        self.extra_count.append(change.extra.len() as u32);
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
        let start_op = self.start_op.save_to(data);
        let max_op = self.max_op.save_to(data);
        let timestamp = self.timestamp.save_to(data);
        let message = self.message.save_to(data);
        let dep_count = self.dep_count.save_to(data);
        let deps = self.deps.save_to(data);
        let extra_count = self.extra_count.save_to(data);
        let start = data.len();
        data.extend_from_slice(&self.extra);
        let extra = start..data.len();
        BundleChangeColumns {
            actor,
            seq,
            start_op,
            max_op,
            timestamp,
            message,
            dep_count,
            deps,
            extra_count,
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
    /// `(actor, counter, doc_pos)` for each op as we process it. At
    /// `finish` time these are sorted by `(actor, counter)` and the
    /// `doc_pos` values are emitted as a delta-int column.
    inverse_positions: Vec<(usize, u64, u32)>,
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
        let doc_pos = self.id_actor.len() as u32;
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
        self.inverse_positions
            .push((op.id.actor(), op.id.counter(), doc_pos));
    }

    /// `members` is the canonical (actor, start_op, max_op) list sorted
    /// by (actor, seq) — the inverse column carries one entry per op in
    /// these ranges, null for ops with no row.
    pub(crate) fn finish(
        self,
        mapper: &ActorMapper<'a>,
        data: &mut Vec<u8>,
        members: &[(usize, u64, u64)],
    ) -> (BundleOpsColumns, Vec<i64>) {
        self.finish_with_ranks(mapper, data, members, &Default::default())
    }

    /// [`Self::finish`] with the covered-rank of every recorded target:
    /// each op's hint value is `ranks[target]` — the number of
    /// dep-covered ops preceding the target row in document order.
    pub(crate) fn finish_with_ranks(
        mut self,
        mapper: &ActorMapper<'a>,
        data: &mut Vec<u8>,
        members: &[(usize, u64, u64)],
        ranks: &std::collections::HashMap<OpId, u64>,
    ) -> (BundleOpsColumns, Vec<i64>) {
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
        let pred_count = self.pred_count.save_to(data);
        let pred_actor = save_actor(self.pred_actor, &mapper.mapping, data);
        let pred_ctr = self.pred_ctr.save_to(data);
        let succ_count = self.succ_count.save_to(data);
        let succ_actor = save_actor(self.succ_actor, &mapper.mapping, data);
        let succ_ctr = self.succ_ctr.save_to(data);
        let expand = self.expand.save_to_unless(data, false);
        let mark_name = self.mark_name.save_to_unless(data, None);

        // Capture doc-order counters before sorting `inverse_positions`.
        // `add()` populates this Vec in doc order, so element k is the
        // counter of the op at doc position k.
        let id_ctr_values: Vec<i64> = self
            .inverse_positions
            .iter()
            .map(|(_, counter, _)| *counter as i64)
            .collect();

        // Emit one inverse entry per member op in canonical
        // (actor, counter) order: the op's doc position, or null for
        // ops with no row (deletes whose targets are all in-bundle —
        // they live in the succ column). Readers walk the change
        // metadata in (actor, seq) order to materialise the canonical
        // (actor, counter) for each `k`, then place non-null entries'
        // counters at their doc position. For editing-style workloads
        // where each new op lands close to its predecessor, the deltas
        // are almost all `+1`s — far more compressible than the
        // doc-order counter sequence.
        self.inverse_positions
            .sort_unstable_by_key(|(a, c, _)| (*a, *c));
        let mut id_ctr_inverse_enc = hexane::DeltaEncoder::<Option<i64>>::default();
        let mut row = self.inverse_positions.iter().peekable();
        for (actor, start_op, max_op) in members {
            for ctr in *start_op..=*max_op {
                match row.peek() {
                    Some((a, c, doc_pos)) if a == actor && *c == ctr => {
                        id_ctr_inverse_enc.append(Some(*doc_pos as i64));
                        row.next();
                    }
                    _ => id_ctr_inverse_enc.append(None),
                }
            }
        }
        debug_assert!(row.next().is_none(), "row op outside member op ranges");
        let id_ctr_inverse = id_ctr_inverse_enc.save_to(data);

        (
            BundleOpsColumns {
                id_actor,
                id_ctr_inverse,
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
            },
            id_ctr_values,
        )
    }
}

#[derive(Default)]
pub(crate) struct BundleOpsColumns {
    pub(crate) id_actor: Range<usize>,
    pub(crate) id_ctr_inverse: Range<usize>,
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
    start_op: Range<usize>,
    timestamp: Range<usize>,
    message: Range<usize>,
    dep_count: Range<usize>,
    deps: Range<usize>,
    extra_count: Range<usize>,
    extra: Range<usize>,
}

impl BundleChangeColumns {
    fn raw_columns(&self) -> RawColumns<compression::Uncompressed> {
        [
            (change::ACTOR, &self.actor),
            (change::SEQ, &self.seq),
            (change::START_OP, &self.start_op),
            (change::MAX_OP, &self.max_op),
            (change::TIMESTAMP, &self.timestamp),
            (change::MESSAGE, &self.message),
            (change::DEP_COUNT, &self.dep_count),
            (change::DEPS, &self.deps),
            (change::EXTRA_COUNT, &self.extra_count),
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
            (ops::ID_CTR_INVERSE, &self.id_ctr_inverse),
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

#[derive(Debug)]
pub struct BundleChangeIter<'a>(BundleChangeIterUnverified<'a>);

impl<'a> BundleChangeIter<'a> {
    // this will panic if passed unverified bytes
    pub(crate) fn new_from_verified(
        columns: &RawColumns<compression::Uncompressed>,
        data: &'a [u8],
    ) -> Self {
        Self(BundleChangeIterUnverified::try_new(columns, data).unwrap())
    }
}

impl<'a> Iterator for BundleChangeIter<'a> {
    type Item = BundleChange<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().transpose().unwrap()
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
    start_op: hexane::DeltaDecoder<'a, Option<i64>>,
    timestamp: hexane::DeltaDecoder<'a, Option<i64>>,
    message: hexane::Decoder<'a, Option<String>>,
    dep_count: hexane::Decoder<'a, Option<u64>>,
    deps: hexane::DeltaDecoder<'a, Option<i64>>,
    extra_count: hexane::Decoder<'a, Option<u64>>,
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
    pub(crate) fn new(columns: &RawColumns<compression::Uncompressed>, data: &'a [u8]) -> Self {
        Self {
            inner: BundleChangeIterInner::try_new(columns, data).ok(),
        }
    }

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
        let mut actor = hexane::decoder::<Option<ActorIdx>>(&[]);
        let mut seq = hexane::DeltaDecoder::<Option<i64>>::new(&[]);
        let mut max_op = hexane::DeltaDecoder::<Option<i64>>::new(&[]);
        let mut start_op = hexane::DeltaDecoder::<Option<i64>>::new(&[]);
        let mut timestamp = hexane::DeltaDecoder::<Option<i64>>::new(&[]);
        let mut message = hexane::decoder::<Option<String>>(&[]);
        let mut dep_count = hexane::decoder::<Option<u64>>(&[]);
        let mut deps = hexane::DeltaDecoder::<Option<i64>>::new(&[]);
        let mut extra_count = hexane::decoder::<Option<u64>>(&[]);
        let mut extra: &[u8] = &[];

        for col in columns.iter() {
            let d = &data[col.data()];
            match col.spec() {
                change::ACTOR => actor = hexane::decoder::<Option<ActorIdx>>(d),
                change::SEQ => seq = hexane::DeltaDecoder::<Option<i64>>::new(d),
                change::START_OP => start_op = hexane::DeltaDecoder::<Option<i64>>::new(d),
                change::MAX_OP => max_op = hexane::DeltaDecoder::<Option<i64>>::new(d),
                change::TIMESTAMP => timestamp = hexane::DeltaDecoder::<Option<i64>>::new(d),
                change::MESSAGE => message = hexane::decoder::<Option<String>>(d),
                change::DEP_COUNT => dep_count = hexane::decoder::<Option<u64>>(d),
                change::DEPS => deps = hexane::DeltaDecoder::<Option<i64>>::new(d),
                change::EXTRA_COUNT => extra_count = hexane::decoder::<Option<u64>>(d),
                change::EXTRA => extra = d,
                spec => return Err(ParseError::InvalidChangeColumn(u32::from(spec))),
            }
        }
        Ok(Self {
            actor,
            seq,
            start_op,
            max_op,
            timestamp,
            message,
            dep_count,
            deps,
            extra_count,
            extra,
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
        let start_op = self
            .start_op
            .next()
            .flatten()
            .ok_or(ReadOpError::MissingValue("start_op"))? as u64;
        let max_op = self
            .max_op
            .next()
            .flatten()
            .ok_or(ReadOpError::MissingValue("max_op"))? as u64;
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

        let extra_count = self.extra_count.next().flatten().unwrap_or(0) as usize;
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
    pub(crate) fn new(
        columns: &RawColumns<compression::Uncompressed>,
        data: &'a [u8],
        id_ctr_values: &'a [i64],
    ) -> Self {
        Self {
            inner: OpIterInner::try_new(columns, data, id_ctr_values).ok(),
        }
    }

    pub(crate) fn try_new(
        columns: &RawColumns<compression::Uncompressed>,
        data: &'a [u8],
        id_ctr_values: &'a [i64],
    ) -> Result<Self, ParseError> {
        Ok(Self {
            inner: Some(OpIterInner::try_new(columns, data, id_ctr_values)?),
        })
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
    id_ctr: std::slice::Iter<'a, i64>,
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

pub(crate) struct OpIter<'a> {
    iter: OpIterUnverified<'a>,
}

impl<'a> OpIter<'a> {
    pub(crate) fn new(
        columns: &RawColumns<compression::Uncompressed>,
        data: &'a [u8],
        id_ctr_values: &'a [i64],
    ) -> Self {
        Self {
            iter: OpIterUnverified::new(columns, data, id_ctr_values),
        }
    }
}

impl<'a> Iterator for OpIter<'a> {
    type Item = BundleOp<'a>;

    fn next(&mut self) -> Option<BundleOp<'a>> {
        self.iter.next().map(|v| v.unwrap())
    }
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
        let id_ctr = self.id_ctr.next().copied();
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
        id_ctr_values: &'a [i64],
    ) -> Result<Self, ParseError> {
        let mut obj_actor = hexane::decoder::<Option<ActorIdx>>(&[]);
        let mut obj_ctr = hexane::decoder::<Option<u64>>(&[]);
        let mut key_actor = hexane::decoder::<Option<ActorIdx>>(&[]);
        let mut key_ctr = hexane::DeltaDecoder::<Option<i64>>::new(&[]);
        let mut key_str = hexane::decoder::<Option<String>>(&[]);
        let mut id_actor = hexane::decoder::<Option<ActorIdx>>(&[]);
        let id_ctr = id_ctr_values.iter();
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
                (ops::ID_CTR_INVERSE_COL_ID, C::DeltaInteger) => {}
                (ops::ID_COL_ID, C::DeltaInteger) => {}
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

/// Prepass facts the streaming manifold needs about *rare* rows,
/// gathered from the action/value columns so the main pass never
/// touches values at all: each increment's value (its succ entry on
/// the target carries `Some(v)`), and which rows are counter-valued
/// puts (only a counter is kept alive by increment successors). Ids
/// come out doc-mapped.
#[derive(Debug, Default)]
pub(crate) struct FragMeta {
    pub(crate) increments: HashMap<OpId, i64>,
    pub(crate) counters: std::collections::HashSet<OpId>,
}

pub(crate) fn frag_prepass(
    columns: &RawColumns<compression::Uncompressed>,
    data: &[u8],
    id_ctr: &[i64],
    actor_map: &[usize],
) -> FragMeta {
    let mut action = hexane::decoder::<Option<Action>>(&[]);
    let mut meta = hexane::decoder::<Option<ValueMeta>>(&[]);
    let mut id_actor = hexane::decoder::<Option<ActorIdx>>(&[]);
    let mut value: &[u8] = &[];
    for col in columns.iter() {
        let d = &data[col.data()];
        match (col.spec().id(), col.spec().col_type()) {
            (ops::ACTION_COL_ID, ColumnType::Integer) => {
                action = hexane::decoder::<Option<Action>>(d)
            }
            (ops::VAL_COL_ID, ColumnType::ValueMetadata) => {
                meta = hexane::decoder::<Option<ValueMeta>>(d)
            }
            (ops::VAL_COL_ID, ColumnType::Value) => value = d,
            (ops::ID_COL_ID, ColumnType::Actor) => {
                id_actor = hexane::decoder::<Option<ActorIdx>>(d)
            }
            _ => {}
        }
    }

    let mut out = FragMeta::default();
    let mut offset = 0usize;
    for ctr in id_ctr.iter() {
        let a = action.next().flatten().expect("validated action");
        let m = meta.next().flatten().expect("validated value meta");
        let actor = id_actor.next().flatten().expect("validated id actor");
        if a == Action::Increment {
            let id = OpId::new(*ctr as u64, actor_map[usize::from(actor)]);
            let raw = &value[offset..offset + m.length()];
            let v = ScalarValue::from_raw(m, raw).expect("validated value");
            let inc = match v {
                ScalarValue::Int(i) => i,
                ScalarValue::Uint(u) => u as i64,
                _ => 0,
            };
            out.increments.insert(id, inc);
        } else if m.type_code() == crate::op_set2::meta::ValueType::Counter {
            let id = OpId::new(*ctr as u64, actor_map[usize::from(actor)]);
            out.counters.insert(id);
        }
        offset += m.length();
    }
    out
}

/// A minimally-decoded fragment op for the streaming manifold: no
/// value, no marks, actor indexes already doc-mapped.
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
    /// increments only: the value carried by the succ entry on the
    /// target
    pub(crate) inc: Option<i64>,
    pub(crate) is_counter: bool,
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

fn skip_rle<D: hexane::RunDecoder>(d: &mut D, mut n: usize) {
    // CLAUDE - isnt this just advance_by(n)
    while n > 0 {
        // an elided column decodes as empty: nothing to advance
        let Some(run) = d.next_run_max(n) else { break };
        n -= run.count;
    }
}

fn skip_delta(d: &mut hexane::DeltaDecoder<'_, Option<i64>>, mut n: usize) {
    // CLAUDE - isnt this just advance_by(n)
    while n > 0 {
        let Some(run) = d.next_delta_run_max(n) else {
            break;
        };
        n -= run.count;
    }
}

fn run_len_zero_count(mut d: hexane::Decoder<'_, Option<u64>>, max: usize) -> usize {
    use hexane::RunDecoder;
    let mut n = 0;
    while n < max {
        match d.next_run_max(max - n) {
            Some(run) if run.value.unwrap_or(0) == 0 => n += run.count,
            _ => break,
        }
    }
    n
}

fn run_len_set(mut d: hexane::Decoder<'_, Option<Action>>, max: usize) -> usize {
    use hexane::RunDecoder;
    let mut n = 0;
    while n < max {
        match d.next_run_max(max - n) {
            Some(run) if run.value == Some(Action::Set) => n += run.count,
            _ => break,
        }
    }
    n
}

fn run_len_true(mut d: hexane::Decoder<'_, bool>, max: usize) -> usize {
    use hexane::RunDecoder;
    let mut n = 0;
    while n < max {
        match d.next_run_max(max - n) {
            Some(run) if run.value => n += run.count,
            _ => break,
        }
    }
    n
}

/// Long-lived forward-only streaming reader over a fragment's op
/// columns — the fragment-side counterpart of the manifold's document
/// iterators. Only the columns the manifold consults are decoded (no
/// values, no marks) and run-level peeks power the tail fast path.
#[derive(Clone)]
pub(crate) struct FragOps<'a> {
    pub(crate) pos: usize,
    pub(crate) len: usize,
    actor_map: &'a [usize],
    id_actor: hexane::Decoder<'a, Option<ActorIdx>>,
    id_ctr: &'a [i64],
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
        id_ctr: &'a [i64],
        actor_map: &'a [usize],
    ) -> Self {
        let mut s = FragOps {
            pos: 0,
            len: id_ctr.len(),
            actor_map,
            id_actor: hexane::decoder::<Option<ActorIdx>>(&[]),
            id_ctr,
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
                (ops::HINT_COL_ID, C::DeltaInteger) => {
                    s.hint = hexane::DeltaDecoder::<Option<i64>>::new(d)
                }
                _ => {}
            }
        }
        s
    }

    /// Decode the next op (minimal fields), doc-mapping every actor.
    pub(crate) fn next_op(&mut self, meta: &FragMeta) -> FragOp<'a> {
        debug_assert!(self.pos < self.len);
        let r = self.pos;
        self.pos += 1;

        let actor = self.id_actor.next().flatten().expect("id actor");
        let id = OpId::new(self.id_ctr[r] as u64, self.actor_map[usize::from(actor)]);

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

        let is_counter = meta.counters.contains(&id);
        let n_succ = self.succ_count.next().flatten().unwrap_or(0) as usize;
        let mut alive = true;
        for _ in 0..n_succ {
            let sa = self.succ_actor.next().flatten().expect("succ actor");
            let sc = self.succ_ctr.next().flatten().expect("succ ctr");
            let sm = OpId::new(sc as u64, self.actor_map[usize::from(sa)]);
            if !(is_counter && meta.increments.contains_key(&sm)) {
                alive = false;
            }
        }

        let inc = meta.increments.get(&id).copied();
        let hint = self.hint.next().flatten().map(|h| h as u64);
        let val_len = self.value_meta.next().flatten().map_or(0, |m| m.length());

        FragOp {
            id,
            obj,
            key,
            insert,
            action,
            preds,
            alive,
            inc,
            is_counter,
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
        n = n.min(run_len_true(self.insert.clone(), n));
        if n == 0 {
            return 0;
        }
        // only plain Set inserts: Make ops must register obj_info and
        // Mark/Increment rows carry semantics the skip would drop
        n = n.min(run_len_set(self.action.clone(), n));
        if n == 0 {
            return 0;
        }
        if !self.pred_absent {
            n = n.min(run_len_zero_count(self.pred_count.clone(), n));
        }
        if n == 0 {
            return 0;
        }
        if !self.succ_absent {
            n = n.min(run_len_zero_count(self.succ_count.clone(), n));
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
        run_len_zero_count(self.pred_count.clone(), self.len - self.pos)
    }

    /// How many upcoming ops still belong to the last-read op's object
    /// (bounded by `max`).
    pub(crate) fn same_obj_run(&self, max: usize) -> usize {
        use hexane::RunDecoder;
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

    /// Skip `n` ops known to have zero preds and zero succ (a clean
    /// run), advancing every column in step. Returns the id of the
    /// last skipped op.
    pub(crate) fn skip_clean(&mut self, n: usize) -> (OpId, usize) {
        use hexane::RunDecoder;
        debug_assert!(n > 0 && self.pos + n <= self.len);
        let last_actor = self.id_actor.nth(n - 1).flatten().expect("id actor");
        skip_rle(&mut self.obj_actor, n);
        skip_rle(&mut self.obj_ctr, n);
        skip_rle(&mut self.key_actor, n);
        skip_delta(&mut self.key_ctr, n);
        skip_rle(&mut self.key_str, n);
        skip_rle(&mut self.insert, n);
        skip_rle(&mut self.action, n);
        // pred/succ counts are all zero in a clean run: the group
        // sub-columns do not advance
        skip_rle(&mut self.pred_count, n);
        skip_rle(&mut self.succ_count, n);
        skip_delta(&mut self.hint, n);
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
        (
            OpId::new(
                self.id_ctr[self.pos - 1] as u64,
                self.actor_map[usize::from(last_actor)],
            ),
            vbytes,
        )
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
    /// Inverse permutation of doc positions. For each op in canonical
    /// `(actor, counter)` order, stores its doc-order index as a
    /// delta-int. Readers reconstruct each op's `counter` from this
    /// column plus the change metadata — no separate `ID_CTR` column on
    /// the wire.
    pub(super) const ID_CTR_INVERSE_COL_ID: ColumnId = ColumnId::new(11);
    /// Per-op position hint: the rank of the op's key-elem row among
    /// the ops covered by the fragment's dependency clock — a sound
    /// lower bound on that row's position in any document the fragment
    /// can apply to, and identical no matter when (or from what doc
    /// state) the fragment is generated. Null for ops without a
    /// covered seq target.
    pub(super) const HINT_COL_ID:           ColumnId = ColumnId::new(12);

    pub(super) const ID_ACTOR:   ColumnSpec = ColumnSpec::new_actor(ID_COL_ID);
    pub(super) const ID_CTR_INVERSE: ColumnSpec = ColumnSpec::new_delta(ID_CTR_INVERSE_COL_ID);
    pub(super) const HINT:       ColumnSpec = ColumnSpec::new_delta(HINT_COL_ID);
    pub(super) const OBJ_ACTOR:  ColumnSpec = ColumnSpec::new_actor(OBJ_COL_ID);
    pub(super) const OBJ_CTR:    ColumnSpec = ColumnSpec::new_integer(OBJ_COL_ID);
    pub(super) const KEY_ACTOR:  ColumnSpec = ColumnSpec::new_actor(KEY_COL_ID);
    pub(super) const KEY_CTR:    ColumnSpec = ColumnSpec::new_delta(KEY_COL_ID);
    pub(super) const KEY_STR:    ColumnSpec = ColumnSpec::new_string(KEY_COL_ID);
    pub(super) const PRED_COUNT: ColumnSpec = ColumnSpec::new_group(PRED_COL_ID);
    pub(super) const PRED_ACTOR: ColumnSpec = ColumnSpec::new_actor(PRED_COL_ID);
    pub(super) const PRED_CTR:   ColumnSpec = ColumnSpec::new_delta(PRED_COL_ID);
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
    pub(super) const START_OP_COL_ID:        ColumnId = ColumnId::new(1);
    pub(super) const MAX_OP_COL_ID:          ColumnId = ColumnId::new(2);
    pub(super) const TIME_COL_ID:            ColumnId = ColumnId::new(3);
    pub(super) const MESSAGE_COL_ID:         ColumnId = ColumnId::new(4);
    pub(super) const DEPS_COL_ID:            ColumnId = ColumnId::new(5);
    pub(super) const EXTRA_COL_ID:           ColumnId = ColumnId::new(6);

    pub(super) const ACTOR:       ColumnSpec = ColumnSpec::new_actor(ACTOR_COL_ID);
    pub(super) const SEQ:         ColumnSpec = ColumnSpec::new_delta(SEQ_COL_ID);
    pub(super) const START_OP:    ColumnSpec = ColumnSpec::new_delta(START_OP_COL_ID);
    pub(super) const MAX_OP:      ColumnSpec = ColumnSpec::new_delta(MAX_OP_COL_ID);
    pub(super) const TIMESTAMP:   ColumnSpec = ColumnSpec::new_delta(TIME_COL_ID);
    pub(super) const MESSAGE:     ColumnSpec = ColumnSpec::new_string(MESSAGE_COL_ID);
    pub(super) const DEP_COUNT:   ColumnSpec = ColumnSpec::new_group(DEPS_COL_ID);
    pub(super) const DEPS:        ColumnSpec = ColumnSpec::new_delta(DEPS_COL_ID);
    pub(super) const EXTRA_COUNT: ColumnSpec = ColumnSpec::new_group(EXTRA_COL_ID);
    pub(super) const EXTRA:       ColumnSpec = ColumnSpec::new_value(EXTRA_COL_ID);
}
