use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::ops::Range;

use crate::op_set2::change::{length_prefixed_bytes, shift_range, ActorMapper};
use crate::op_set2::op::{Op, OpBuilder};
use crate::op_set2::types::{ActionCursor, ActorCursor, ActorIdx, KeyRef, MetaCursor};
use crate::op_set2::{ReadOpError, ScalarValue};
use crate::storage::change::DEFLATE_MIN_SIZE;
use crate::storage::columns::{compression, ColumnType};
use crate::storage::{ChunkType, Header, RawColumn, RawColumns};
use crate::types::{ChangeHash, ObjId, OpId};

use hexane::{
    BooleanCursor, ColumnCursor, CursorIter, DeltaCursor, Encoder, RawCursor, StrCursor, UIntCursor,
};

use super::{Bundle, BundleChange, BundleMetadata, BundleStorage, ParseError};

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

    pub(crate) fn process_op(&mut self, op: Op<'a>) {
        let next = Some((op.obj, op.elemid_or_key()));
        let flush = self.last != next;

        self.process_op_internal(op, flush);

        if flush {
            self.last = next;
        }
    }

    fn process_op_internal(&mut self, op: Op<'a>, flush: bool) {
        self.max_op = std::cmp::max(self.max_op, op.id.counter());

        if flush {
            self.flush_deletes();
        }

        let pred = self.preds.remove(&op.id).unwrap_or_default();

        let op = op.build(pred);

        if let Some(index) = self.builders_index(op.id) {
            self.op_writer.add(op, index, &mut self.mapper);
        }
    }

    pub(crate) fn process_succ(&mut self, op_id: OpId, succ_id: OpId) {
        self.max_op = std::cmp::max(self.max_op, succ_id.counter());
        self.preds.entry(succ_id).or_default().push(op_id);
    }

    pub(crate) fn flush_deletes(&mut self) {
        if let Some((obj, key)) = self.last.take() {
            for (id, pred) in &self.preds {
                let op = Op::del(*id, obj, key.clone());
                let op = op.build(pred.to_vec());
                if let Some(index) = self.builders_index(op.id) {
                    self.op_writer.add(op, index, &mut self.mapper);
                }
            }
            self.preds.clear();
        }
    }

    pub(crate) fn finish(mut self) -> Bundle {
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
        let (ops_cols, id_ctr) = self.op_writer.finish(&mapper, &mut ops_data_buf);
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
    actor: Encoder<'a, ActorCursor>,
    seq: Encoder<'a, DeltaCursor>,
    start_op: Encoder<'a, DeltaCursor>,
    max_op: Encoder<'a, DeltaCursor>,
    timestamp: Encoder<'a, DeltaCursor>,
    message: Encoder<'a, StrCursor>,
    dep_count: Encoder<'a, UIntCursor>,
    deps: Encoder<'a, DeltaCursor>,
    extra_count: Encoder<'a, UIntCursor>,
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
        self.seen.insert(change.hash, self.len);
        self.len += 1;
        self.actor.append(ActorIdx::from(change.actor));
        self.seq.append(change.seq as i64);
        self.start_op.append(change.start_op as i64);
        self.max_op.append(change.max_op as i64);
        self.message.append(change.message.clone());
        self.timestamp.append(change.timestamp);
        self.extra_count.append(change.extra.len() as u64);
        self.extra.extend_from_slice(&change.extra);
        self.dep_count.append(change.deps.len() as u64);
        for d in &change.deps {
            if let Some(i) = self.seen.get(d) {
                self.deps.append(*i as i64);
            } else {
                let index = self.cap + self.external.len();
                self.seen.insert(*d, index);
                self.external.push(*d);
                self.deps.append(index as i64);
            }
        }
    }

    fn finish(self, mapper: &ActorMapper<'_>, data: &mut Vec<u8>) -> BundleChangeColumns {
        let remap = move |a: &ActorIdx| mapper.mapping[usize::from(*a)].as_ref();
        let actor = self.actor.save_to_and_remap(data, remap);
        let seq = self.seq.save_to(data);
        let start_op = self.start_op.save_to(data);
        let max_op = self.max_op.save_to(data);
        let timestamp = self.timestamp.save_to(data);
        let message = self.message.save_to(data);
        let dep_count = self.dep_count.save_to(data);
        let deps = self.deps.save_to_unless_empty(data);
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
    obj_actor: Encoder<'a, ActorCursor>,
    obj_ctr: Encoder<'a, DeltaCursor>,
    key_actor: Encoder<'a, ActorCursor>,
    key_ctr: Encoder<'a, DeltaCursor>,
    key_str: Encoder<'a, StrCursor>,
    id_actor: Encoder<'a, ActorCursor>,
    insert: Encoder<'a, BooleanCursor>,
    action: Encoder<'a, ActionCursor>,
    value_meta: Encoder<'a, MetaCursor>,
    value: Encoder<'a, RawCursor>,
    pred_count: Encoder<'a, UIntCursor>,
    pred_actor: Encoder<'a, ActorCursor>,
    pred_ctr: Encoder<'a, DeltaCursor>,
    expand: Encoder<'a, BooleanCursor>,
    mark_name: Encoder<'a, StrCursor>,
    /// `(actor, counter, doc_pos)` for each op as we process it. At
    /// `finish` time these are sorted by `(actor, counter)` and the
    /// `doc_pos` values are emitted as a delta-int column. Readers
    /// reconstruct each op's counter from this column plus the change
    /// metadata, so we don't need an explicit `id_ctr` column on the wire.
    inverse_positions: Vec<(usize, u64, u32)>,
}

impl<'a> BundleOpWriter<'a> {
    fn add(&mut self, op: OpBuilder<'a>, _index: usize, mapper: &mut ActorMapper<'a>) {
        mapper.process_op(&op);
        let doc_pos = self.id_actor.len as u32;
        self.id_actor.append(op.id.actoridx());
        self.obj_actor.append(op.obj.actor());
        self.obj_ctr.append(op.obj.icounter());
        self.key_actor.append(op.key.actor());
        self.key_ctr.append(op.key.icounter());
        self.key_str.append(op.key.key_str());
        self.insert.append(op.insert);
        self.action.append(op.action);
        self.value_meta.append(op.value.meta());
        self.value.append(op.value.to_raw());
        self.pred_count.append(op.pred.len() as u64);
        for p in &op.pred {
            self.pred_actor.append(p.actoridx());
            self.pred_ctr.append(p.icounter());
        }
        self.expand.append(op.expand);
        self.mark_name.append(op.mark_name);
        self.inverse_positions
            .push((op.id.actor(), op.id.counter(), doc_pos));
    }

    fn finish(
        mut self,
        mapper: &ActorMapper<'a>,
        data: &mut Vec<u8>,
    ) -> (BundleOpsColumns, Vec<i64>) {
        let remap = move |a: &ActorIdx| mapper.mapping[usize::from(*a)].as_ref();
        let obj_actor = self.obj_actor.save_to_and_remap_unless_empty(data, remap);
        let obj_ctr = self.obj_ctr.save_to_unless_empty(data);
        let key_actor = self.key_actor.save_to_and_remap_unless_empty(data, remap);
        let key_ctr = self.key_ctr.save_to_unless_empty(data);
        let key_str = self.key_str.save_to_unless_empty(data);
        let id_actor = self.id_actor.save_to_and_remap(data, remap);
        let insert = self.insert.save_to(data);
        let action = self.action.save_to_unless_empty(data);
        let value_meta = self.value_meta.save_to_unless_empty(data);
        let value = self.value.save_to_unless_empty(data);
        let pred_count = self.pred_count.save_to_unless_empty(data);
        let pred_actor = self.pred_actor.save_to_and_remap_unless_empty(data, remap);
        let pred_ctr = self.pred_ctr.save_to_unless_empty(data);
        let expand = self.expand.save_to_unless_empty(data);
        let mark_name = self.mark_name.save_to_unless_empty(data);

        // Capture doc-order counters before sorting `inverse_positions`.
        // `add()` populates this Vec in doc order, so element k is the
        // counter of the op at doc position k.
        let id_ctr_values: Vec<i64> = self
            .inverse_positions
            .iter()
            .map(|(_, counter, _)| *counter as i64)
            .collect();

        // Sort by canonical (actor, counter) order and emit each op's
        // doc_pos as a delta-int — the inverse permutation. Readers walk
        // the change metadata in (actor, seq) order to materialise the
        // canonical (actor, counter) for each `k`, then look up
        // `doc_pos = inverse[k]` to place that op's id in the doc-order
        // column. For editing-style workloads where each new op lands
        // close to its predecessor, the deltas are almost all `+1`s —
        // far more compressible than the doc-order counter sequence.
        self.inverse_positions
            .sort_unstable_by_key(|(a, c, _)| (*a, *c));
        let mut id_ctr_inverse_enc: Encoder<'_, DeltaCursor> = Encoder::default();
        for (_, _, doc_pos) in &self.inverse_positions {
            id_ctr_inverse_enc.append(*doc_pos as i64);
        }
        let id_ctr_inverse = id_ctr_inverse_enc.save_to_unless_empty(data);

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
                expand,
                mark_name,
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
    pub(crate) expand: Range<usize>,
    pub(crate) mark_name: Range<usize>,
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
    fn raw_columns(&self) -> RawColumns<compression::Uncompressed> {
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
            (ops::MARK_NAME, &self.mark_name),
            (ops::EXPAND, &self.expand),
            (ops::ID_CTR_INVERSE, &self.id_ctr_inverse),
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
    actor: CursorIter<'a, ActorCursor>,
    seq: CursorIter<'a, DeltaCursor>,
    max_op: CursorIter<'a, DeltaCursor>,
    start_op: CursorIter<'a, DeltaCursor>,
    timestamp: CursorIter<'a, DeltaCursor>,
    message: CursorIter<'a, StrCursor>,
    dep_count: CursorIter<'a, UIntCursor>,
    deps: CursorIter<'a, DeltaCursor>,
    extra_count: CursorIter<'a, UIntCursor>,
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
        let mut actor = ActorCursor::iter(&[]);
        let mut seq = DeltaCursor::iter(&[]);
        let mut max_op = DeltaCursor::iter(&[]);
        let mut start_op = DeltaCursor::iter(&[]);
        let mut timestamp = DeltaCursor::iter(&[]);
        let mut message = StrCursor::iter(&[]);
        let mut dep_count = UIntCursor::iter(&[]);
        let mut deps = DeltaCursor::iter(&[]);
        let mut extra_count = UIntCursor::iter(&[]);
        let mut extra: &[u8] = &[];

        for col in columns.iter() {
            let d = &data[col.data()];
            match col.spec() {
                change::ACTOR => actor = ActorCursor::iter(d),
                change::SEQ => seq = DeltaCursor::iter(d),
                change::START_OP => start_op = DeltaCursor::iter(d),
                change::MAX_OP => max_op = DeltaCursor::iter(d),
                change::TIMESTAMP => timestamp = DeltaCursor::iter(d),
                change::MESSAGE => message = StrCursor::iter(d),
                change::DEP_COUNT => dep_count = UIntCursor::iter(d),
                change::DEPS => deps = DeltaCursor::iter(d),
                change::EXTRA_COUNT => extra_count = UIntCursor::iter(d),
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
        let actor = match self.actor.next().transpose()?.flatten().as_deref().copied() {
            Some(actor) => actor.into(),
            None => return Ok(None),
        };

        let seq = self
            .seq
            .next()
            .transpose()?
            .flatten()
            .as_deref()
            .copied()
            .ok_or(ReadOpError::MissingValue("seq"))? as u64;
        let start_op = self
            .start_op
            .next()
            .transpose()?
            .flatten()
            .as_deref()
            .copied()
            .ok_or(ReadOpError::MissingValue("start_op"))? as u64;
        let max_op = self
            .max_op
            .next()
            .transpose()?
            .flatten()
            .as_deref()
            .copied()
            .ok_or(ReadOpError::MissingValue("max_op"))? as u64;
        let timestamp = self
            .timestamp
            .next()
            .transpose()?
            .flatten()
            .as_deref()
            .copied()
            .unwrap_or(0);
        let message = self.message.next().transpose()?.flatten();
        let dep_count = self
            .dep_count
            .next()
            .transpose()?
            .flatten()
            .as_deref()
            .copied()
            .unwrap_or(0) as usize;
        let mut deps = vec![];
        for _ in 0..dep_count {
            let dep = self
                .deps
                .next()
                .transpose()?
                .flatten()
                .as_deref()
                .copied()
                .ok_or(ReadOpError::MissingValue("dep"))? as u64;
            deps.push(dep);
        }
        let extra_count = self
            .extra_count
            .next()
            .transpose()?
            .flatten()
            .as_deref()
            .copied()
            .unwrap_or(0) as usize;
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
    obj_actor: CursorIter<'a, ActorCursor>,
    obj_ctr: CursorIter<'a, DeltaCursor>,
    key_actor: CursorIter<'a, ActorCursor>,
    key_ctr: CursorIter<'a, DeltaCursor>,
    key_str: CursorIter<'a, StrCursor>,
    id_actor: CursorIter<'a, ActorCursor>,
    /// Doc-order counter values, reconstructed at parse time from the
    /// wire's `ID_CTR_INVERSE` column. Read directly as `i64`s instead
    /// of going through a re-encoded columnar form.
    id_ctr: std::slice::Iter<'a, i64>,
    insert: CursorIter<'a, BooleanCursor>,
    action: CursorIter<'a, ActionCursor>,
    meta: CursorIter<'a, MetaCursor>,
    value: &'a [u8],
    pred_count: CursorIter<'a, UIntCursor>,
    pred_actor: CursorIter<'a, ActorCursor>,
    pred_ctr: CursorIter<'a, DeltaCursor>,
    expand: CursorIter<'a, BooleanCursor>,
    mark_name: CursorIter<'a, StrCursor>,
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
    type Item = OpBuilder<'a>;

    fn next(&mut self) -> Option<OpBuilder<'a>> {
        self.iter.next().map(|v| v.unwrap())
    }
}

impl<'a> Iterator for OpIterUnverified<'a> {
    type Item = Result<OpBuilder<'a>, ParseError>;

    fn next(&mut self) -> Option<Result<OpBuilder<'a>, ParseError>> {
        self.inner
            .as_mut()?
            .try_next()
            .inspect_err(|_| self.inner = None)
            .transpose()
    }
}

impl<'a> OpIterInner<'a> {
    fn try_next(&mut self) -> Result<Option<OpBuilder<'a>>, ParseError> {
        let id_actor = self.id_actor.next().transpose()?.flatten();
        let id_ctr = self.id_ctr.next().map(std::borrow::Cow::Borrowed);
        let id = match OpId::try_load(id_actor, id_ctr) {
            Ok(id) => id,
            Err(_) => return Ok(None),
        };
        let obj_actor = self.obj_actor.next().transpose()?.flatten();
        let obj_ctr = self.obj_ctr.next().transpose()?.flatten();
        let obj = ObjId::try_load_i(obj_actor, obj_ctr)?;

        let key_str = self.key_str.next().transpose()?.flatten();
        let key_actor = self.key_actor.next().transpose()?.flatten();
        let key_ctr = self.key_ctr.next().transpose()?.flatten();
        let key = KeyRef::try_load(key_str, key_actor, key_ctr)?;

        let action = *self
            .action
            .next()
            .transpose()?
            .flatten()
            .ok_or(ReadOpError::MissingValue("action"))?;

        let insert = self
            .insert
            .next()
            .transpose()?
            .flatten()
            .as_deref()
            .copied()
            .unwrap_or_default();

        let expand = self
            .expand
            .next()
            .transpose()?
            .flatten()
            .as_deref()
            .copied()
            .unwrap_or_default();

        let mark_name = self.mark_name.next().transpose()?.flatten();

        let value_meta = self.meta.next().transpose()?.flatten();
        let value_meta = value_meta.ok_or(ReadOpError::MissingValue("value_meta"))?;
        let (value_raw, tail) = self.value.split_at(value_meta.length());
        self.value = tail;
        let value = ScalarValue::from_raw(*value_meta, value_raw)
            .map_err(|_| ReadOpError::MissingValue("value"))?;

        let pred_count = self
            .pred_count
            .next()
            .transpose()?
            .flatten()
            .as_deref()
            .copied()
            .unwrap_or(0) as usize;
        let mut pred = Vec::with_capacity(pred_count);

        for _ in 0..pred_count {
            let pred_actor = self.pred_actor.next().transpose()?.flatten();
            let pred_ctr = self.pred_ctr.next().transpose()?.flatten();
            pred.push(OpId::try_load(pred_actor, pred_ctr)?);
        }

        Ok(Some(OpBuilder {
            id,
            obj,
            action,
            key,
            value,
            insert,
            expand,
            mark_name,
            pred,
        }))
    }

    fn try_new(
        columns: &RawColumns<compression::Uncompressed>,
        data: &'a [u8],
        id_ctr_values: &'a [i64],
    ) -> Result<Self, ParseError> {
        let mut obj_actor = ActorCursor::iter(&[]);
        let mut obj_ctr = DeltaCursor::iter(&[]);
        let mut key_actor = ActorCursor::iter(&[]);
        let mut key_ctr = DeltaCursor::iter(&[]);
        let mut key_str = StrCursor::iter(&[]);
        let mut id_actor = ActorCursor::iter(&[]);
        // id_ctr is not stored on the wire — it's reconstructed in
        // `BundleStorage::parse_following_header` from `ID_CTR_INVERSE`
        // plus the change metadata. We iterate over the reconstructed
        // values directly as a slice, no columnar round-trip needed.
        let id_ctr = id_ctr_values.iter();
        let mut insert = BooleanCursor::iter(&[]);
        let mut action = ActionCursor::iter(&[]);
        let mut meta = MetaCursor::iter(&[]);
        let mut value: &[u8] = &[]; //RawCursor::iter(&[]);
        let mut pred_count = UIntCursor::iter(&[]);
        let mut pred_actor = ActorCursor::iter(&[]);
        let mut pred_ctr = DeltaCursor::iter(&[]);
        let mut expand = BooleanCursor::iter(&[]);
        let mut mark_name = StrCursor::iter(&[]);

        for col in columns.iter() {
            let d = &data[col.data()];
            type C = ColumnType;
            match (col.spec().id(), col.spec().col_type()) {
                (ops::OBJ_COL_ID, C::Actor) => obj_actor = ActorCursor::iter(d),
                (ops::OBJ_COL_ID, C::DeltaInteger) => obj_ctr = DeltaCursor::iter(d),
                (ops::KEY_COL_ID, C::Actor) => key_actor = ActorCursor::iter(d),
                (ops::KEY_COL_ID, C::DeltaInteger) => key_ctr = DeltaCursor::iter(d),
                (ops::KEY_COL_ID, C::String) => key_str = StrCursor::iter(d),
                (ops::ID_COL_ID, C::Actor) => id_actor = ActorCursor::iter(d),
                (ops::INSERT_COL_ID, C::Boolean) => insert = BooleanCursor::iter(d),
                (ops::ACTION_COL_ID, C::Integer) => action = ActionCursor::iter(d),
                (ops::VAL_COL_ID, C::ValueMetadata) => meta = MetaCursor::iter(d),
                (ops::VAL_COL_ID, C::Value) => value = d,
                (ops::PRED_COL_ID, C::Group) => pred_count = UIntCursor::iter(d),
                (ops::PRED_COL_ID, C::Actor) => pred_actor = ActorCursor::iter(d),
                (ops::PRED_COL_ID, C::DeltaInteger) => pred_ctr = DeltaCursor::iter(d),
                (ops::EXPAND_COL_ID, C::Boolean) => expand = BooleanCursor::iter(d),
                (ops::MARK_NAME_COL_ID, C::String) => mark_name = StrCursor::iter(d),
                // `ID_CTR_INVERSE` and the legacy `ID_CTR` are both
                // handled at the storage layer: it picks whichever is
                // present and feeds the resulting doc-order counters in
                // via `id_ctr_values`.
                (ops::ID_CTR_INVERSE_COL_ID, C::DeltaInteger) => {}
                (ops::ID_COL_ID, C::DeltaInteger) => {}
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
            expand,
            mark_name,
        })
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
    pub(super) const EXPAND_COL_ID:         ColumnId = ColumnId::new(9);
    pub(super) const MARK_NAME_COL_ID:      ColumnId = ColumnId::new(10);
    /// Inverse permutation of doc positions. For each op in canonical
    /// `(actor, counter)` order, stores its doc-order index as a
    /// delta-int. Readers reconstruct each op's `counter` from this
    /// column plus the change metadata — no separate `ID_CTR` column on
    /// the wire.
    pub(super) const ID_CTR_INVERSE_COL_ID: ColumnId = ColumnId::new(11);

    pub(super) const ID_ACTOR:   ColumnSpec = ColumnSpec::new_actor(ID_COL_ID);
    pub(super) const ID_CTR_INVERSE: ColumnSpec = ColumnSpec::new_delta(ID_CTR_INVERSE_COL_ID);
    pub(super) const OBJ_ACTOR:  ColumnSpec = ColumnSpec::new_actor(OBJ_COL_ID);
    pub(super) const OBJ_CTR:    ColumnSpec = ColumnSpec::new_delta(OBJ_COL_ID);
    pub(super) const KEY_ACTOR:  ColumnSpec = ColumnSpec::new_actor(KEY_COL_ID);
    pub(super) const KEY_CTR:    ColumnSpec = ColumnSpec::new_delta(KEY_COL_ID);
    pub(super) const KEY_STR:    ColumnSpec = ColumnSpec::new_string(KEY_COL_ID);
    pub(super) const PRED_COUNT: ColumnSpec = ColumnSpec::new_group(PRED_COL_ID);
    pub(super) const PRED_ACTOR: ColumnSpec = ColumnSpec::new_actor(PRED_COL_ID);
    pub(super) const PRED_CTR:   ColumnSpec = ColumnSpec::new_delta(PRED_COL_ID);
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
