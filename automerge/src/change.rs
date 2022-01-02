use crate::automerge::Transaction;
use crate::columnar::{
    ChangeEncoder, ChangeIterator, ColumnEncoder, DepsIterator, DocChange, DocOp, DocOpEncoder,
    DocOpIterator, OperationIterator, COLUMN_TYPE_DEFLATE,
};
use crate::decoding;
use crate::decoding::{Decodable, InvalidChangeError};
use crate::encoding::{Encodable, DEFLATE_MIN_SIZE};
use crate::error::AutomergeError;
use crate::indexed_cache::IndexedCache;
use crate::legacy as amp;
use crate::types;
use crate::types::{ActorId, ElemId, Key, ObjId, Op, OpId, OpType};
use core::ops::Range;
use flate2::{
    bufread::{DeflateDecoder, DeflateEncoder},
    Compression,
};
use itertools::Itertools;
use sha2::Digest;
use sha2::Sha256;
use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::fmt::Debug;
use std::io::{Read, Write};
use tracing::instrument;

const MAGIC_BYTES: [u8; 4] = [0x85, 0x6f, 0x4a, 0x83];
const PREAMBLE_BYTES: usize = 8;
const HEADER_BYTES: usize = PREAMBLE_BYTES + 1;

const HASH_BYTES: usize = 32;
const BLOCK_TYPE_DOC: u8 = 0;
const BLOCK_TYPE_CHANGE: u8 = 1;
const BLOCK_TYPE_DEFLATE: u8 = 2;
const CHUNK_START: usize = 8;
const HASH_RANGE: Range<usize> = 4..8;

fn get_heads(changes: &[amp::Change]) -> HashSet<amp::ChangeHash> {
    changes.iter().fold(HashSet::new(), |mut acc, c| {
        if let Some(h) = c.hash {
            acc.insert(h);
        }
        for dep in &c.deps {
            acc.remove(dep);
        }
        acc
    })
}

pub(crate) fn encode_document(
    changes: &[amp::Change],
    doc_ops: &[Op],
    actors_index: &IndexedCache<ActorId>,
    props: &[String],
) -> Result<Vec<u8>, AutomergeError> {
    let mut bytes: Vec<u8> = Vec::new();

    let heads = get_heads(changes);

    let actors_map = actors_index.encode_index();
    let actors = actors_index.sorted();

    /*
    // this assumes that all actor_ids referenced are seen in changes.actor_id which is true
    // so long as we have a full history
    let mut actors: Vec<_> = changes
        .iter()
        .map(|c| &c.actor)
        .unique()
        .sorted()
        .cloned()
        .collect();
    */

    let (change_bytes, change_info) = ChangeEncoder::encode_changes(changes, &actors);

    //let doc_ops = group_doc_ops(changes, &actors);

    let (ops_bytes, ops_info) = DocOpEncoder::encode_doc_ops(doc_ops, &actors_map, props);

    bytes.extend(&MAGIC_BYTES);
    bytes.extend(vec![0, 0, 0, 0]); // we dont know the hash yet so fill in a fake
    bytes.push(BLOCK_TYPE_DOC);

    let mut chunk = Vec::new();

    actors.len().encode(&mut chunk)?;

    for a in actors.into_iter() {
        a.to_bytes().encode(&mut chunk)?;
    }

    heads.len().encode(&mut chunk)?;
    for head in heads.iter().sorted() {
        chunk.write_all(&head.0).unwrap();
    }

    chunk.extend(change_info);
    chunk.extend(ops_info);

    chunk.extend(change_bytes);
    chunk.extend(ops_bytes);

    leb128::write::unsigned(&mut bytes, chunk.len() as u64).unwrap();

    bytes.extend(&chunk);

    let hash_result = Sha256::digest(&bytes[CHUNK_START..bytes.len()]);

    bytes.splice(HASH_RANGE, hash_result[0..4].iter().copied());

    Ok(bytes)
}

/// When encoding a change we take all the actor IDs referenced by a change and place them in an
/// array. The array has the actor who authored the change as the first element and all remaining
/// actors (i.e. those referenced in object IDs in the target of an operation or in the `pred` of
/// an operation) lexicographically ordered following the change author.
fn actor_ids_in_change(change: &amp::Change) -> Vec<amp::ActorId> {
    let mut other_ids: Vec<&amp::ActorId> = change
        .operations
        .iter()
        .flat_map(opids_in_operation)
        .filter(|a| *a != &change.actor_id)
        .unique()
        .collect();
    other_ids.sort();
    // Now prepend the change actor
    std::iter::once(&change.actor_id)
        .chain(other_ids.into_iter())
        .cloned()
        .collect()
}

fn opids_in_operation(op: &amp::Op) -> impl Iterator<Item = &amp::ActorId> {
    let obj_actor_id = match &op.obj {
        amp::ObjectId::Root => None,
        amp::ObjectId::Id(opid) => Some(opid.actor()),
    };
    let pred_ids = op.pred.iter().map(amp::OpId::actor);
    let key_actor = match &op.key {
        amp::Key::Seq(amp::ElementId::Id(i)) => Some(i.actor()),
        _ => None,
    };
    obj_actor_id
        .into_iter()
        .chain(key_actor.into_iter())
        .chain(pred_ids)
}

impl From<amp::Change> for Change {
    fn from(value: amp::Change) -> Self {
        encode(&value)
    }
}

impl From<&amp::Change> for Change {
    fn from(value: &amp::Change) -> Self {
        encode(value)
    }
}

fn encode(change: &amp::Change) -> Change {
    let mut deps = change.deps.clone();
    deps.sort_unstable();

    let mut chunk = encode_chunk(change, &deps);

    let mut bytes = Vec::with_capacity(MAGIC_BYTES.len() + 4 + chunk.bytes.len());

    bytes.extend(&MAGIC_BYTES);

    bytes.extend(vec![0, 0, 0, 0]); // we dont know the hash yet so fill in a fake

    bytes.push(BLOCK_TYPE_CHANGE);

    leb128::write::unsigned(&mut bytes, chunk.bytes.len() as u64).unwrap();

    let body_start = bytes.len();

    increment_range(&mut chunk.body, bytes.len());
    increment_range(&mut chunk.message, bytes.len());
    increment_range(&mut chunk.extra_bytes, bytes.len());
    increment_range_map(&mut chunk.ops, bytes.len());

    bytes.extend(&chunk.bytes);

    let hash_result = Sha256::digest(&bytes[CHUNK_START..bytes.len()]);
    let hash: amp::ChangeHash = hash_result[..].try_into().unwrap();

    bytes.splice(HASH_RANGE, hash_result[0..4].iter().copied());

    // any time I make changes to the encoder decoder its a good idea
    // to run it through a round trip to detect errors the tests might not
    // catch
    // let c0 = Change::from_bytes(bytes.clone()).unwrap();
    // std::assert_eq!(c1, c0);
    // perhaps we should add something like this to the test suite

    let bytes = ChangeBytes::Uncompressed(bytes);

    Change {
        bytes,
        body_start,
        hash,
        seq: change.seq,
        start_op: change.start_op,
        time: change.time,
        actors: chunk.actors,
        message: chunk.message,
        deps,
        ops: chunk.ops,
        extra_bytes: chunk.extra_bytes,
    }
}

struct ChunkIntermediate {
    bytes: Vec<u8>,
    body: Range<usize>,
    actors: Vec<ActorId>,
    message: Range<usize>,
    ops: HashMap<u32, Range<usize>>,
    extra_bytes: Range<usize>,
}

fn encode_chunk(change: &amp::Change, deps: &[amp::ChangeHash]) -> ChunkIntermediate {
    let mut bytes = Vec::new();

    // All these unwraps are okay because we're writing to an in memory buffer so io erros should
    // not happen

    // encode deps
    deps.len().encode(&mut bytes).unwrap();
    for hash in deps.iter() {
        bytes.write_all(&hash.0).unwrap();
    }

    let actors = actor_ids_in_change(change);
    change.actor_id.to_bytes().encode(&mut bytes).unwrap();

    // encode seq, start_op, time, message
    change.seq.encode(&mut bytes).unwrap();
    change.start_op.encode(&mut bytes).unwrap();
    change.time.encode(&mut bytes).unwrap();
    let message = bytes.len() + 1;
    change.message.encode(&mut bytes).unwrap();
    let message = message..bytes.len();

    // encode ops into a side buffer - collect all other actors
    let (ops_buf, mut ops) = ColumnEncoder::encode_ops(&change.operations, &actors);

    // encode all other actors
    actors[1..].encode(&mut bytes).unwrap();

    // now we know how many bytes ops are offset by so we can adjust the ranges
    increment_range_map(&mut ops, bytes.len());

    // write out the ops

    bytes.write_all(&ops_buf).unwrap();

    // write out the extra bytes
    let extra_bytes = bytes.len()..(bytes.len() + change.extra_bytes.len());
    bytes.write_all(&change.extra_bytes).unwrap();
    let body = 0..bytes.len();

    ChunkIntermediate {
        bytes,
        body,
        actors,
        message,
        ops,
        extra_bytes,
    }
}

#[derive(PartialEq, Debug, Clone)]
enum ChangeBytes {
    Compressed {
        compressed: Vec<u8>,
        uncompressed: Vec<u8>,
    },
    Uncompressed(Vec<u8>),
}

impl ChangeBytes {
    fn uncompressed(&self) -> &[u8] {
        match self {
            ChangeBytes::Compressed { uncompressed, .. } => &uncompressed[..],
            ChangeBytes::Uncompressed(b) => &b[..],
        }
    }

    fn compress(&mut self, body_start: usize) {
        match self {
            ChangeBytes::Compressed { .. } => {}
            ChangeBytes::Uncompressed(uncompressed) => {
                if uncompressed.len() > DEFLATE_MIN_SIZE {
                    let mut result = Vec::with_capacity(uncompressed.len());
                    result.extend(&uncompressed[0..8]);
                    result.push(BLOCK_TYPE_DEFLATE);
                    let mut deflater =
                        DeflateEncoder::new(&uncompressed[body_start..], Compression::default());
                    let mut deflated = Vec::new();
                    let deflated_len = deflater.read_to_end(&mut deflated).unwrap();
                    leb128::write::unsigned(&mut result, deflated_len as u64).unwrap();
                    result.extend(&deflated[..]);
                    *self = ChangeBytes::Compressed {
                        compressed: result,
                        uncompressed: std::mem::take(uncompressed),
                    }
                }
            }
        }
    }

    fn raw(&self) -> &[u8] {
        match self {
            ChangeBytes::Compressed { compressed, .. } => &compressed[..],
            ChangeBytes::Uncompressed(b) => &b[..],
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct Change {
    bytes: ChangeBytes,
    body_start: usize,
    pub hash: amp::ChangeHash,
    pub seq: u64,
    pub start_op: u64,
    pub time: i64,
    message: Range<usize>,
    actors: Vec<ActorId>,
    pub deps: Vec<amp::ChangeHash>,
    ops: HashMap<u32, Range<usize>>,
    extra_bytes: Range<usize>,
}

impl Change {
    pub fn actor_id(&self) -> &ActorId {
        &self.actors[0]
    }

    #[instrument(level = "debug", skip(bytes))]
    pub fn load_document(bytes: &[u8]) -> Result<Vec<Change>, AutomergeError> {
        load_blocks(bytes)
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Result<Change, decoding::Error> {
        decode_change(bytes)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        // TODO - this could be a lot more efficient
        self.iter_ops().count()
    }

    pub fn max_op(&self) -> u64 {
        self.start_op + (self.len() as u64) - 1
    }

    fn message(&self) -> Option<String> {
        let m = &self.bytes.uncompressed()[self.message.clone()];
        if m.is_empty() {
            None
        } else {
            std::str::from_utf8(m).map(ToString::to_string).ok()
        }
    }

    pub fn decode(&self) -> amp::Change {
        amp::Change {
            start_op: self.start_op,
            seq: self.seq,
            time: self.time,
            hash: Some(self.hash),
            message: self.message(),
            actor_id: self.actors[0].clone(),
            deps: self.deps.clone(),
            operations: self
                .iter_ops()
                .map(|op| amp::Op {
                    action: op.action.clone(),
                    obj: op.obj.clone(),
                    key: op.key.clone(),
                    pred: op.pred.clone(),
                    insert: op.insert,
                })
                .collect(),
            extra_bytes: self.extra_bytes().into(),
        }
    }

    pub(crate) fn iter_ops(&self) -> OperationIterator {
        OperationIterator::new(self.bytes.uncompressed(), self.actors.as_slice(), &self.ops)
    }

    pub fn extra_bytes(&self) -> &[u8] {
        &self.bytes.uncompressed()[self.extra_bytes.clone()]
    }

    pub fn compress(&mut self) {
        self.bytes.compress(self.body_start);
    }

    pub fn raw_bytes(&self) -> &[u8] {
        self.bytes.raw()
    }
}

fn read_leb128(bytes: &mut &[u8]) -> Result<(usize, usize), decoding::Error> {
    let mut buf = &bytes[..];
    let val = leb128::read::unsigned(&mut buf)? as usize;
    let leb128_bytes = bytes.len() - buf.len();
    Ok((val, leb128_bytes))
}

fn read_slice<T: Decodable + Debug>(
    bytes: &[u8],
    cursor: &mut Range<usize>,
) -> Result<T, decoding::Error> {
    let mut view = &bytes[cursor.clone()];
    let init_len = view.len();
    let val = T::decode::<&[u8]>(&mut view).ok_or(decoding::Error::NoDecodedValue);
    let bytes_read = init_len - view.len();
    *cursor = (cursor.start + bytes_read)..cursor.end;
    val
}

fn slice_bytes(bytes: &[u8], cursor: &mut Range<usize>) -> Result<Range<usize>, decoding::Error> {
    let (val, len) = read_leb128(&mut &bytes[cursor.clone()])?;
    let start = cursor.start + len;
    let end = start + val;
    *cursor = end..cursor.end;
    Ok(start..end)
}

fn increment_range(range: &mut Range<usize>, len: usize) {
    range.end += len;
    range.start += len;
}

fn increment_range_map(ranges: &mut HashMap<u32, Range<usize>>, len: usize) {
    for range in ranges.values_mut() {
        increment_range(range, len);
    }
}

fn export_objid(id: &ObjId, actors: &IndexedCache<ActorId>) -> amp::ObjectId {
    if id == &ObjId::root() {
        amp::ObjectId::Root
    } else {
        export_opid(&id.0, actors).into()
    }
}

fn export_elemid(id: &ElemId, actors: &IndexedCache<ActorId>) -> amp::ElementId {
    if id == &types::HEAD {
        amp::ElementId::Head
    } else {
        export_opid(&id.0, actors).into()
    }
}

fn export_opid(id: &OpId, actors: &IndexedCache<ActorId>) -> amp::OpId {
    amp::OpId(id.0, actors.get(id.1).clone())
}

fn export_op(op: &Op, actors: &IndexedCache<ActorId>, props: &IndexedCache<String>) -> amp::Op {
    let action = op.action.clone();
    let key = match &op.key {
        Key::Map(n) => amp::Key::Map(props.get(*n).clone().into()),
        Key::Seq(id) => amp::Key::Seq(export_elemid(id, actors)),
    };
    let obj = export_objid(&op.obj, actors);
    let pred = op.pred.iter().map(|id| export_opid(id, actors)).collect();
    amp::Op {
        action,
        obj,
        insert: op.insert,
        pred,
        key,
    }
}

pub(crate) fn export_change(
    change: &Transaction,
    actors: &IndexedCache<ActorId>,
    props: &IndexedCache<String>,
) -> Change {
    amp::Change {
        actor_id: actors.get(change.actor).clone(),
        seq: change.seq,
        start_op: change.start_op,
        time: change.time,
        deps: change.deps.clone(),
        message: change.message.clone(),
        hash: change.hash,
        operations: change
            .operations
            .iter()
            .map(|op| export_op(op, actors, props))
            .collect(),
        extra_bytes: change.extra_bytes.clone(),
    }
    .into()
}

pub fn decode_change(bytes: Vec<u8>) -> Result<Change, decoding::Error> {
    let (chunktype, body) = decode_header_without_hash(&bytes)?;
    let bytes = if chunktype == BLOCK_TYPE_DEFLATE {
        decompress_chunk(0..PREAMBLE_BYTES, body, bytes)?
    } else {
        ChangeBytes::Uncompressed(bytes)
    };

    let (chunktype, hash, body) = decode_header(bytes.uncompressed())?;

    if chunktype != BLOCK_TYPE_CHANGE {
        return Err(decoding::Error::WrongType {
            expected_one_of: vec![BLOCK_TYPE_CHANGE],
            found: chunktype,
        });
    }

    let body_start = body.start;
    let mut cursor = body;

    let deps = decode_hashes(bytes.uncompressed(), &mut cursor)?;

    let actor =
        ActorId::from(&bytes.uncompressed()[slice_bytes(bytes.uncompressed(), &mut cursor)?]);
    let seq = read_slice(bytes.uncompressed(), &mut cursor)?;
    let start_op = read_slice(bytes.uncompressed(), &mut cursor)?;
    let time = read_slice(bytes.uncompressed(), &mut cursor)?;
    let message = slice_bytes(bytes.uncompressed(), &mut cursor)?;

    let actors = decode_actors(bytes.uncompressed(), &mut cursor, Some(actor))?;

    let ops_info = decode_column_info(bytes.uncompressed(), &mut cursor, false)?;
    let ops = decode_columns(&mut cursor, &ops_info);

    Ok(Change {
        bytes,
        body_start,
        hash,
        seq,
        start_op,
        time,
        actors,
        message,
        deps,
        ops,
        extra_bytes: cursor,
    })
}

fn decompress_chunk(
    preamble: Range<usize>,
    body: Range<usize>,
    compressed: Vec<u8>,
) -> Result<ChangeBytes, decoding::Error> {
    let mut decoder = DeflateDecoder::new(&compressed[body]);
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed)?;
    let mut result = Vec::with_capacity(decompressed.len() + preamble.len());
    result.extend(&compressed[preamble]);
    result.push(BLOCK_TYPE_CHANGE);
    leb128::write::unsigned::<Vec<u8>>(&mut result, decompressed.len() as u64).unwrap();
    result.extend(decompressed);
    Ok(ChangeBytes::Compressed {
        uncompressed: result,
        compressed,
    })
}

fn decode_hashes(
    bytes: &[u8],
    cursor: &mut Range<usize>,
) -> Result<Vec<amp::ChangeHash>, decoding::Error> {
    let num_hashes = read_slice(bytes, cursor)?;
    let mut hashes = Vec::with_capacity(num_hashes);
    for _ in 0..num_hashes {
        let hash = cursor.start..(cursor.start + HASH_BYTES);
        *cursor = hash.end..cursor.end;
        hashes.push(
            bytes
                .get(hash)
                .ok_or(decoding::Error::NotEnoughBytes)?
                .try_into()
                .map_err(InvalidChangeError::from)?,
        );
    }
    Ok(hashes)
}

fn decode_actors(
    bytes: &[u8],
    cursor: &mut Range<usize>,
    first: Option<ActorId>,
) -> Result<Vec<ActorId>, decoding::Error> {
    let num_actors: usize = read_slice(bytes, cursor)?;
    let mut actors = Vec::with_capacity(num_actors + 1);
    if let Some(actor) = first {
        actors.push(actor);
    }
    for _ in 0..num_actors {
        actors.push(ActorId::from(
            bytes
                .get(slice_bytes(bytes, cursor)?)
                .ok_or(decoding::Error::NotEnoughBytes)?,
        ));
    }
    Ok(actors)
}

fn decode_column_info(
    bytes: &[u8],
    cursor: &mut Range<usize>,
    allow_compressed_column: bool,
) -> Result<Vec<(u32, usize)>, decoding::Error> {
    let num_columns = read_slice(bytes, cursor)?;
    let mut columns = Vec::with_capacity(num_columns);
    let mut last_id = 0;
    for _ in 0..num_columns {
        let id: u32 = read_slice(bytes, cursor)?;
        if (id & !COLUMN_TYPE_DEFLATE) <= (last_id & !COLUMN_TYPE_DEFLATE) {
            return Err(decoding::Error::ColumnsNotInAscendingOrder {
                last: last_id,
                found: id,
            });
        }
        if id & COLUMN_TYPE_DEFLATE != 0 && !allow_compressed_column {
            return Err(decoding::Error::ChangeContainedCompressedColumns);
        }
        last_id = id;
        let length = read_slice(bytes, cursor)?;
        columns.push((id, length));
    }
    Ok(columns)
}

fn decode_columns(
    cursor: &mut Range<usize>,
    columns: &[(u32, usize)],
) -> HashMap<u32, Range<usize>> {
    let mut ops = HashMap::new();
    for (id, length) in columns {
        let start = cursor.start;
        let end = start + length;
        *cursor = end..cursor.end;
        ops.insert(*id, start..end);
    }
    ops
}

fn decode_header(bytes: &[u8]) -> Result<(u8, amp::ChangeHash, Range<usize>), decoding::Error> {
    let (chunktype, body) = decode_header_without_hash(bytes)?;

    let calculated_hash = Sha256::digest(&bytes[PREAMBLE_BYTES..]);

    let checksum = &bytes[4..8];
    if checksum != &calculated_hash[0..4] {
        return Err(decoding::Error::InvalidChecksum {
            found: checksum.try_into().unwrap(),
            calculated: calculated_hash[0..4].try_into().unwrap(),
        });
    }

    let hash = calculated_hash[..]
        .try_into()
        .map_err(InvalidChangeError::from)?;

    Ok((chunktype, hash, body))
}

fn decode_header_without_hash(bytes: &[u8]) -> Result<(u8, Range<usize>), decoding::Error> {
    if bytes.len() <= HEADER_BYTES {
        return Err(decoding::Error::NotEnoughBytes);
    }

    if bytes[0..4] != MAGIC_BYTES {
        return Err(decoding::Error::WrongMagicBytes);
    }

    let (val, len) = read_leb128(&mut &bytes[HEADER_BYTES..])?;
    let body = (HEADER_BYTES + len)..(HEADER_BYTES + len + val);
    if bytes.len() != body.end {
        return Err(decoding::Error::WrongByteLength {
            expected: body.end,
            found: bytes.len(),
        });
    }

    let chunktype = bytes[PREAMBLE_BYTES];

    Ok((chunktype, body))
}

fn load_blocks(bytes: &[u8]) -> Result<Vec<Change>, AutomergeError> {
    let mut changes = Vec::new();
    for slice in split_blocks(bytes)? {
        decode_block(slice, &mut changes)?;
    }
    Ok(changes)
}

fn split_blocks(bytes: &[u8]) -> Result<Vec<&[u8]>, decoding::Error> {
    // split off all valid blocks - ignore the rest if its corrupted or truncated
    let mut blocks = Vec::new();
    let mut cursor = bytes;
    while let Some(block) = pop_block(cursor)? {
        blocks.push(&cursor[block.clone()]);
        if cursor.len() <= block.end {
            break;
        }
        cursor = &cursor[block.end..];
    }
    Ok(blocks)
}

fn pop_block(bytes: &[u8]) -> Result<Option<Range<usize>>, decoding::Error> {
    if bytes.len() < 4 || bytes[0..4] != MAGIC_BYTES {
        // not reporting error here - file got corrupted?
        return Ok(None);
    }
    let (val, len) = read_leb128(
        &mut bytes
            .get(HEADER_BYTES..)
            .ok_or(decoding::Error::NotEnoughBytes)?,
    )?;
    // val is arbitrary so it could overflow
    let end = (HEADER_BYTES + len)
        .checked_add(val)
        .ok_or(decoding::Error::Overflow)?;
    if end > bytes.len() {
        // not reporting error here - file got truncated?
        return Ok(None);
    }
    Ok(Some(0..end))
}

fn decode_block(bytes: &[u8], changes: &mut Vec<Change>) -> Result<(), decoding::Error> {
    match bytes[PREAMBLE_BYTES] {
        BLOCK_TYPE_DOC => {
            changes.extend(decode_document(bytes)?);
            Ok(())
        }
        BLOCK_TYPE_CHANGE | BLOCK_TYPE_DEFLATE => {
            changes.push(decode_change(bytes.to_vec())?);
            Ok(())
        }
        found => Err(decoding::Error::WrongType {
            expected_one_of: vec![BLOCK_TYPE_DOC, BLOCK_TYPE_CHANGE, BLOCK_TYPE_DEFLATE],
            found,
        }),
    }
}

fn decode_document(bytes: &[u8]) -> Result<Vec<Change>, decoding::Error> {
    let (chunktype, _hash, mut cursor) = decode_header(bytes)?;

    // chunktype == 0 is a document, chunktype = 1 is a change
    if chunktype > 0 {
        return Err(decoding::Error::WrongType {
            expected_one_of: vec![0],
            found: chunktype,
        });
    }

    let actors = decode_actors(bytes, &mut cursor, None)?;

    let heads = decode_hashes(bytes, &mut cursor)?;

    let changes_info = decode_column_info(bytes, &mut cursor, true)?;
    let ops_info = decode_column_info(bytes, &mut cursor, true)?;

    let changes_data = decode_columns(&mut cursor, &changes_info);
    let mut doc_changes = ChangeIterator::new(bytes, &changes_data).collect::<Vec<_>>();
    let doc_changes_deps = DepsIterator::new(bytes, &changes_data);

    let doc_changes_len = doc_changes.len();

    let ops_data = decode_columns(&mut cursor, &ops_info);
    let doc_ops: Vec<_> = DocOpIterator::new(bytes, &actors, &ops_data).collect();

    group_doc_change_and_doc_ops(&mut doc_changes, doc_ops, &actors)?;

    let uncompressed_changes =
        doc_changes_to_uncompressed_changes(doc_changes.into_iter(), &actors);

    let changes = compress_doc_changes(uncompressed_changes, doc_changes_deps, doc_changes_len)
        .ok_or(decoding::Error::NoDocChanges)?;

    let mut calculated_heads = HashSet::new();
    for change in &changes {
        for dep in &change.deps {
            calculated_heads.remove(dep);
        }
        calculated_heads.insert(change.hash);
    }

    if calculated_heads != heads.into_iter().collect::<HashSet<_>>() {
        return Err(decoding::Error::MismatchedHeads);
    }

    Ok(changes)
}

fn compress_doc_changes(
    uncompressed_changes: impl Iterator<Item = amp::Change>,
    doc_changes_deps: impl Iterator<Item = Vec<usize>>,
    num_changes: usize,
) -> Option<Vec<Change>> {
    let mut changes: Vec<Change> = Vec::with_capacity(num_changes);

    // fill out the hashes as we go
    for (deps, mut uncompressed_change) in doc_changes_deps.zip_eq(uncompressed_changes) {
        for idx in deps {
            uncompressed_change.deps.push(changes.get(idx)?.hash);
        }
        changes.push(uncompressed_change.into());
    }

    Some(changes)
}

fn group_doc_change_and_doc_ops(
    changes: &mut [DocChange],
    mut ops: Vec<DocOp>,
    actors: &[ActorId],
) -> Result<(), decoding::Error> {
    let mut changes_by_actor: HashMap<usize, Vec<usize>> = HashMap::new();

    for (i, change) in changes.iter().enumerate() {
        let actor_change_index = changes_by_actor.entry(change.actor).or_default();
        if change.seq != (actor_change_index.len() + 1) as u64 {
            return Err(decoding::Error::ChangeDecompressFailed(
                "Doc Seq Invalid".into(),
            ));
        }
        if change.actor >= actors.len() {
            return Err(decoding::Error::ChangeDecompressFailed(
                "Doc Actor Invalid".into(),
            ));
        }
        actor_change_index.push(i);
    }

    let mut op_by_id = HashMap::new();
    ops.iter().enumerate().for_each(|(i, op)| {
        op_by_id.insert((op.ctr, op.actor), i);
    });

    for i in 0..ops.len() {
        let op = ops[i].clone(); // this is safe - avoid borrow checker issues
                                 //let id = (op.ctr, op.actor);
                                 //op_by_id.insert(id, i);
        for succ in &op.succ {
            if let Some(index) = op_by_id.get(succ) {
                ops[*index].pred.push((op.ctr, op.actor));
            } else {
                let key = if op.insert {
                    amp::OpId(op.ctr, actors[op.actor].clone()).into()
                } else {
                    op.key.clone()
                };
                let del = DocOp {
                    actor: succ.1,
                    ctr: succ.0,
                    action: OpType::Del,
                    obj: op.obj.clone(),
                    key,
                    succ: Vec::new(),
                    pred: vec![(op.ctr, op.actor)],
                    insert: false,
                };
                op_by_id.insert(*succ, ops.len());
                ops.push(del);
            }
        }
    }

    for op in ops {
        // binary search for our change
        let actor_change_index = changes_by_actor.entry(op.actor).or_default();
        let mut left = 0;
        let mut right = actor_change_index.len();
        while left < right {
            let seq = (left + right) / 2;
            if changes[actor_change_index[seq]].max_op < op.ctr {
                left = seq + 1;
            } else {
                right = seq;
            }
        }
        if left >= actor_change_index.len() {
            return Err(decoding::Error::ChangeDecompressFailed(
                "Doc MaxOp Invalid".into(),
            ));
        }
        changes[actor_change_index[left]].ops.push(op);
    }

    changes
        .iter_mut()
        .for_each(|change| change.ops.sort_unstable());

    Ok(())
}

fn doc_changes_to_uncompressed_changes<'a>(
    changes: impl Iterator<Item = DocChange> + 'a,
    actors: &'a [ActorId],
) -> impl Iterator<Item = amp::Change> + 'a {
    changes.map(move |change| amp::Change {
        // we've already confirmed that all change.actor's are valid
        actor_id: actors[change.actor].clone(),
        seq: change.seq,
        time: change.time,
        start_op: change.max_op - change.ops.len() as u64 + 1,
        hash: None,
        message: change.message,
        operations: change
            .ops
            .into_iter()
            .map(|op| amp::Op {
                action: op.action.clone(),
                insert: op.insert,
                key: op.key,
                obj: op.obj,
                // we've already confirmed that all op.actor's are valid
                pred: pred_into(op.pred.into_iter(), actors),
            })
            .collect(),
        deps: Vec::new(),
        extra_bytes: change.extra_bytes,
    })
}

fn pred_into(
    pred: impl Iterator<Item = (u64, usize)>,
    actors: &[ActorId],
) -> amp::SortedVec<amp::OpId> {
    pred.map(|(ctr, actor)| amp::OpId(ctr, actors[actor].clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::legacy as amp;
    #[test]
    fn mismatched_head_repro_one() {
        let op_json = serde_json::json!({
            "ops": [
                {
                    "action": "del",
                    "obj": "1@1485eebc689d47efbf8b892e81653eb3",
                    "elemId": "3164@0dcdf83d9594477199f80ccd25e87053",
                    "pred": [
                        "3164@0dcdf83d9594477199f80ccd25e87053"
                    ],
                    "insert": false
                },
            ],
            "actor": "e63cf5ed1f0a4fb28b2c5bc6793b9272",
            "hash": "e7fd5c02c8fdd2cdc3071ce898a5839bf36229678af3b940f347da541d147ae2",
            "seq": 1,
            "startOp": 3179,
            "time": 1634146652,
            "message": null,
            "deps": [
                "2603cded00f91e525507fc9e030e77f9253b239d90264ee343753efa99e3fec1"
            ]
        });

        let change: amp::Change = serde_json::from_value(op_json).unwrap();
        let expected_hash: super::amp::ChangeHash =
            "4dff4665d658a28bb6dcace8764eb35fa8e48e0a255e70b6b8cbf8e8456e5c50"
                .parse()
                .unwrap();
        let encoded: super::Change = change.into();
        assert_eq!(encoded.hash, expected_hash);
    }
}
