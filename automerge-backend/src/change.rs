//use crate::columnar;
use crate::columnar::{
    ChangeEncoder, ChangeIterator, ColumnEncoder, DocChange, DocOp, DocOpEncoder, DocOpIterator,
    OperationIterator, COLUMN_TYPE_DEFLATE,
};
use crate::encoding::DEFLATE_MIN_SIZE;
use crate::encoding::{Decodable, Encodable};
use crate::error::{AutomergeError, InvalidChangeError};
use automerge_protocol as amp;
use core::fmt::Debug;
use flate2::{
    bufread::{DeflateDecoder, DeflateEncoder},
    Compression,
};
use itertools::Itertools;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::convert::TryInto;
use std::io::Read;
use std::io::Write;
use std::ops::Range;
use std::str;

const HASH_BYTES: usize = 32;
const BLOCK_TYPE_DOC: u8 = 0;
const BLOCK_TYPE_CHANGE: u8 = 1;
const BLOCK_TYPE_DEFLATE: u8 = 2;
const CHUNK_START: usize = 8;
const HASH_RANGE: Range<usize> = 4..8;

impl From<amp::UncompressedChange> for Change {
    fn from(value: amp::UncompressedChange) -> Self {
        encode(&value)
    }
}

impl From<&amp::UncompressedChange> for Change {
    fn from(value: &amp::UncompressedChange) -> Self {
        encode(value)
    }
}

fn encode(uncompressed_change: &amp::UncompressedChange) -> Change {
    let mut bytes: Vec<u8> = Vec::new();
    let mut hasher = Sha256::new();

    let mut deps = uncompressed_change.deps.clone();
    deps.sort_unstable();

    let mut chunk = encode_chunk(uncompressed_change, &deps);

    bytes.extend(&MAGIC_BYTES);

    bytes.extend(vec![0, 0, 0, 0]); // we dont know the hash yet so fill in a fake

    bytes.push(BLOCK_TYPE_CHANGE);

    leb128::write::unsigned(&mut bytes, chunk.bytes.len() as u64).unwrap();

    increment_range(&mut chunk.body, bytes.len());
    increment_range(&mut chunk.message, bytes.len());
    increment_range(&mut chunk.extra_bytes, bytes.len());
    increment_range_map(&mut chunk.ops, bytes.len());

    bytes.extend(&chunk.bytes);

    hasher.input(&bytes[CHUNK_START..bytes.len()]);
    let hash_result = hasher.result();
    let hash: amp::ChangeHash = hash_result[..].try_into().unwrap();

    bytes.splice(HASH_RANGE, hash_result[0..4].iter().cloned());

    // any time I make changes to the encoder decoder its a good idea
    // to run it through a round trip to detect errors the tests might not
    // catch
    // let c0 = Change::from_bytes(bytes.clone()).unwrap();
    // std::assert_eq!(c1, c0);
    // perhaps we should add something like this to the test suite

    let bytes = if bytes.len() > DEFLATE_MIN_SIZE {
        let mut result = Vec::with_capacity(bytes.len());
        result.extend(&bytes[0..8]);
        result.push(BLOCK_TYPE_DEFLATE);
        let mut deflater = DeflateEncoder::new(&chunk.bytes[..], Compression::best());
        let mut deflated = Vec::new();
        let deflated_len = deflater.read_to_end(&mut deflated).unwrap();
        leb128::write::unsigned(&mut result, deflated_len as u64).unwrap();
        result.extend(&deflated[..]);
        ChangeBytes::Compressed {
            compressed: result,
            uncompressed: bytes,
        }
    } else {
        ChangeBytes::Uncompressed(bytes)
    };

    Change {
        bytes,
        hash,
        seq: uncompressed_change.seq,
        start_op: uncompressed_change.start_op,
        time: uncompressed_change.time,
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
    actors: Vec<amp::ActorId>,
    message: Range<usize>,
    ops: HashMap<u32, Range<usize>>,
    extra_bytes: Range<usize>,
}

fn encode_chunk(
    uncompressed_change: &amp::UncompressedChange,
    deps: &[amp::ChangeHash],
) -> ChunkIntermediate {
    let mut bytes = Vec::new();

    // All these unwraps are okay because we're writing to an in memory buffer so io erros should
    // not happen

    // encode deps
    deps.len().encode(&mut bytes).unwrap();
    for hash in deps.iter() {
        bytes.write_all(&hash.0).unwrap();
    }

    // encode first actor
    let mut actors = vec![uncompressed_change.actor_id.clone()];
    uncompressed_change
        .actor_id
        .to_bytes()
        .encode(&mut bytes)
        .unwrap();

    // encode seq, start_op, time, message
    uncompressed_change.seq.encode(&mut bytes).unwrap();
    uncompressed_change.start_op.encode(&mut bytes).unwrap();
    uncompressed_change.time.encode(&mut bytes).unwrap();
    let message = bytes.len() + 1;
    uncompressed_change.message.encode(&mut bytes).unwrap();
    let message = message..bytes.len();

    // encode ops into a side buffer - collect all other actors
    let (ops_buf, mut ops) =
        ColumnEncoder::encode_ops(uncompressed_change.operations.iter(), &mut actors);

    // encode all other actors
    actors[1..].encode(&mut bytes).unwrap();

    // now we know how many bytes ops are offset by so we can adjust the ranges
    increment_range_map(&mut ops, bytes.len());

    // write out the ops

    bytes.write_all(&ops_buf).unwrap();

    // write out the extra bytes
    let extra_bytes = bytes.len()..(bytes.len() + uncompressed_change.extra_bytes.len());
    bytes.write_all(&uncompressed_change.extra_bytes).unwrap();
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
    pub hash: amp::ChangeHash,
    pub seq: u64,
    pub start_op: u64,
    pub time: i64,
    message: Range<usize>,
    actors: Vec<amp::ActorId>,
    pub deps: Vec<amp::ChangeHash>,
    ops: HashMap<u32, Range<usize>>,
    extra_bytes: Range<usize>,
}

impl Change {
    pub fn actor_id(&self) -> &amp::ActorId {
        &self.actors[0]
    }

    pub fn load_document(bytes: &[u8]) -> Result<Vec<Change>, AutomergeError> {
        load_blocks(bytes)
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Result<Change, AutomergeError> {
        decode_change(bytes)
    }

    pub fn max_op(&self) -> u64 {
        // TODO - this could be a lot more efficient
        let len = self.iter_ops().count();
        self.start_op + (len as u64) - 1
    }

    fn message(&self) -> Option<String> {
        let m = &self.bytes.uncompressed()[self.message.clone()];
        if m.is_empty() {
            None
        } else {
            str::from_utf8(&m).map(|s| s.to_string()).ok()
        }
    }

    pub fn decode(&self) -> amp::UncompressedChange {
        amp::UncompressedChange {
            start_op: self.start_op,
            seq: self.seq,
            time: self.time,
            hash: Some(self.hash),
            message: self.message(),
            actor_id: self.actors[0].clone(),
            deps: self.deps.clone(),
            operations: self.iter_ops().collect(),
            extra_bytes: self.extra_bytes().into(),
        }
    }

    pub fn iter_ops(&self) -> OperationIterator {
        OperationIterator::new(&self.bytes.uncompressed(), &self.actors, &self.ops)
    }

    pub fn extra_bytes(&self) -> &[u8] {
        &self.bytes.uncompressed()[self.extra_bytes.clone()]
    }

    pub fn raw_bytes(&self) -> &[u8] {
        self.bytes.raw()
    }

    pub fn checksum(&self) -> u32 {
        let bytes = self.bytes.uncompressed();
        u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]])
    }
}

impl From<&Change> for amp::UncompressedChange {
    fn from(change: &Change) -> amp::UncompressedChange {
        change.decode()
    }
}

impl TryFrom<&[u8]> for Change {
    type Error = AutomergeError;
    fn try_from(bytes: &[u8]) -> Result<Self, AutomergeError> {
        Change::from_bytes(bytes.to_vec())
    }
}

fn read_leb128(bytes: &mut &[u8]) -> Result<(usize, usize), AutomergeError> {
    let mut buf = &bytes[..];
    let val = leb128::read::unsigned(&mut buf)? as usize;
    let leb128_bytes = bytes.len() - buf.len();
    Ok((val, leb128_bytes))
}

fn read_slice<T: Decodable + Debug>(
    bytes: &[u8],
    cursor: &mut Range<usize>,
) -> Result<T, AutomergeError> {
    let mut view = &bytes[cursor.clone()];
    let init_len = view.len();
    let val = T::decode::<&[u8]>(&mut view).ok_or(AutomergeError::EncodingError);
    let bytes_read = init_len - view.len();
    *cursor = (cursor.start + bytes_read)..cursor.end;
    val
}

fn slice_bytes(bytes: &[u8], cursor: &mut Range<usize>) -> Result<Range<usize>, AutomergeError> {
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
        increment_range(range, len)
    }
}

#[allow(dead_code)]
pub(crate) struct Document {
    pub bytes: Vec<u8>,
    pub hash: amp::ChangeHash,
    pub seq: u64,
    pub start_op: u64,
    pub time: i64,
    body: Range<usize>,
    message: Range<usize>,
    actors: Vec<amp::ActorId>,
    pub deps: Vec<amp::ChangeHash>,
    ops: HashMap<u32, Range<usize>>,
    extra_bytes: Range<usize>,
}

fn decode_header(bytes: &[u8]) -> Result<(u8, amp::ChangeHash, Range<usize>), AutomergeError> {
    let (chunktype, body) = decode_header_without_hash(bytes)?;

    let mut hasher = Sha256::new();
    hasher.input(&bytes[PREAMBLE_BYTES..]);
    let calculated_hash = hasher.result();

    let checksum = &bytes[4..8];
    if checksum != &calculated_hash[0..4] {
        return Err(AutomergeError::InvalidChecksum {
            found: checksum.try_into().unwrap(),
            calculated: calculated_hash[0..4].try_into().unwrap(),
        });
    }

    let hash = calculated_hash[..]
        .try_into()
        .map_err(InvalidChangeError::from)?;

    Ok((chunktype, hash, body))
}

fn decode_header_without_hash(bytes: &[u8]) -> Result<(u8, Range<usize>), AutomergeError> {
    if bytes.len() <= HEADER_BYTES {
        return Err(AutomergeError::EncodingError);
    }

    if bytes[0..4] != MAGIC_BYTES {
        return Err(AutomergeError::EncodingError);
    }

    let (val, len) = read_leb128(&mut &bytes[HEADER_BYTES..])?;
    let body = (HEADER_BYTES + len)..(HEADER_BYTES + len + val);
    if bytes.len() != body.end {
        return Err(AutomergeError::EncodingError);
    }

    let chunktype = bytes[PREAMBLE_BYTES];

    Ok((chunktype, body))
}

fn decode_hashes(
    bytes: &[u8],
    cursor: &mut Range<usize>,
) -> Result<Vec<amp::ChangeHash>, AutomergeError> {
    let num_hashes = read_slice(bytes, cursor)?;
    let mut hashes = Vec::with_capacity(num_hashes);
    for _ in 0..num_hashes {
        let hash = cursor.start..(cursor.start + HASH_BYTES);
        *cursor = hash.end..cursor.end;
        hashes.push(bytes[hash].try_into().map_err(InvalidChangeError::from)?);
    }
    Ok(hashes)
}

fn decode_actors(
    bytes: &[u8],
    cursor: &mut Range<usize>,
    first: Option<amp::ActorId>,
) -> Result<Vec<amp::ActorId>, AutomergeError> {
    let num_actors: usize = read_slice(bytes, cursor)?;
    let mut actors = Vec::with_capacity(num_actors + 1);
    if let Some(actor) = first {
        actors.push(actor)
    }
    for _ in 0..num_actors {
        actors.push(amp::ActorId::from(&bytes[slice_bytes(bytes, cursor)?]));
    }
    Ok(actors)
}

fn decode_column_info(
    bytes: &[u8],
    cursor: &mut Range<usize>,
    allow_compressed_column: bool,
) -> Result<Vec<(u32, usize)>, AutomergeError> {
    let num_columns = read_slice(bytes, cursor)?;
    let mut columns = Vec::with_capacity(num_columns);
    let mut last_id = 0;
    for _ in 0..num_columns {
        let id: u32 = read_slice(bytes, cursor)?;
        if id <= last_id {
            return Err(AutomergeError::EncodingError);
        }
        if id & COLUMN_TYPE_DEFLATE != 0 && !allow_compressed_column {
            return Err(AutomergeError::ChangeContainedCompressedColumns);
        }
        last_id = id;
        let length = read_slice(bytes, cursor)?;
        columns.push((id, length));
    }
    Ok(columns)
}

fn decode_columns(
    cursor: &mut Range<usize>,
    columns: Vec<(u32, usize)>,
) -> HashMap<u32, Range<usize>> {
    let mut ops = HashMap::new();
    for (id, length) in columns.iter() {
        let start = cursor.start;
        let end = start + length;
        *cursor = end..cursor.end;
        ops.insert(*id, start..end);
    }
    ops
}

fn decode_block(bytes: &[u8], changes: &mut Vec<Change>) -> Result<(), AutomergeError> {
    match bytes[PREAMBLE_BYTES] {
        BLOCK_TYPE_DOC => {
            changes.extend(decode_document(bytes)?);
            Ok(())
        }
        BLOCK_TYPE_CHANGE | BLOCK_TYPE_DEFLATE => {
            changes.push(decode_change(bytes.to_vec())?);
            Ok(())
        }
        _ => Err(AutomergeError::EncodingError),
    }
}

fn decode_change(bytes: Vec<u8>) -> Result<Change, AutomergeError> {
    let (chunktype, body) = decode_header_without_hash(&bytes)?;
    let bytes = if chunktype == BLOCK_TYPE_DEFLATE {
        decompress_chunk(&bytes[..PREAMBLE_BYTES], &bytes[body])?
    } else {
        ChangeBytes::Uncompressed(bytes)
    };

    let (chunktype, hash, body) = decode_header(&bytes.uncompressed())?;

    if chunktype != BLOCK_TYPE_CHANGE {
        return Err(AutomergeError::EncodingError);
    }

    let mut cursor = body;

    let deps = decode_hashes(&bytes.uncompressed(), &mut cursor)?;

    let actor =
        amp::ActorId::from(&bytes.uncompressed()[slice_bytes(&bytes.uncompressed(), &mut cursor)?]);
    let seq = read_slice(&bytes.uncompressed(), &mut cursor)?;
    let start_op = read_slice(&bytes.uncompressed(), &mut cursor)?;
    let time = read_slice(&bytes.uncompressed(), &mut cursor)?;
    let message = slice_bytes(&bytes.uncompressed(), &mut cursor)?;

    let actors = decode_actors(&bytes.uncompressed(), &mut cursor, Some(actor))?;

    let ops_info = decode_column_info(&bytes.uncompressed(), &mut cursor, false)?;
    let ops = decode_columns(&mut cursor, ops_info);

    Ok(Change {
        bytes,
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

fn decompress_chunk(preamble: &[u8], buf: &[u8]) -> Result<ChangeBytes, AutomergeError> {
    let mut decoder = DeflateDecoder::new(buf);
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed)?;
    let mut result = Vec::with_capacity(decompressed.len() + preamble.len());
    result.extend(preamble);
    result.push(BLOCK_TYPE_CHANGE);
    leb128::write::unsigned::<Vec<u8>>(&mut result, decompressed.len() as u64).unwrap();
    result.extend(decompressed);
    Ok(ChangeBytes::Compressed {
        uncompressed: result,
        compressed: buf.into(),
    })
}

//
// group all the ops together with the appropriate change and reconstitute the del ops
// mutates the arguments - returns nothing
//

fn group_doc_change_and_doc_ops(
    changes: &mut [DocChange],
    ops: &mut Vec<DocOp>,
    actors: &[amp::ActorId],
) -> Result<(), AutomergeError> {
    let mut change_actors = HashMap::new();
    let mut actor_max = HashMap::new();

    for (i, change) in changes.iter().enumerate() {
        if change.seq != *actor_max.get(&change.actor).unwrap_or(&1) {
            return Err(AutomergeError::ChangeDecompressError(
                "Doc Seq Invalid".into(),
            ));
        }
        if change.actor >= actors.len() {
            return Err(AutomergeError::ChangeDecompressError(
                "Doc Actor Invalid".into(),
            ));
        }
        change_actors.insert((change.actor, change.seq), i);
        actor_max.insert(change.actor, change.seq + 1);
    }

    let mut op_by_id = HashMap::new();
    ops.iter().enumerate().for_each(|(i, op)| {
        op_by_id.insert((op.ctr, op.actor), i);
    });
    for i in 0..ops.len() {
        let op = ops[i].clone(); // this is safe - avoid borrow checker issues
                                 //let id = (op.ctr, op.actor);
                                 //op_by_id.insert(id, i);
        for succ in op.succ.iter() {
            if let Some(index) = op_by_id.get(&succ) {
                ops[*index].pred.push((op.ctr, op.actor))
            } else {
                let key = if op.insert {
                    amp::OpId(op.ctr, actors[op.actor].clone()).into()
                } else {
                    op.key.clone()
                };
                let del = DocOp {
                    actor: succ.1,
                    ctr: succ.0,
                    action: amp::OpType::Del,
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

    'outer: for op in ops.iter() {
        let max_seq = *actor_max
            .get(&op.actor)
            .ok_or_else(|| AutomergeError::ChangeDecompressError("Doc Op.Actor Invalid".into()))?;
        for seq in 1..max_seq {
            // this is safe - invalid seq would have thrown an error earlier
            let idx: usize = *change_actors.get(&(op.actor, seq)).unwrap();
            // this is safe since I build the array above ^^
            let change = &mut changes[idx];
            if op.ctr <= change.max_op {
                change.ops.push(op.clone());
                continue 'outer;
            }
        }
        return Err(AutomergeError::ChangeDecompressError(
            "Doc MaxOp Invalid".into(),
        ));
    }

    changes
        .iter_mut()
        .for_each(|change| change.ops.sort_unstable());

    Ok(())
}

fn pred_into(pred: &[(u64, usize)], actors: &[amp::ActorId]) -> Vec<amp::OpId> {
    pred.iter()
        .map(|(ctr, actor)| amp::OpId(*ctr, actors[*actor].clone()))
        .collect()
}

fn doc_changes_to_uncompressed_changes(
    changes: &[DocChange],
    actors: &[amp::ActorId],
) -> Vec<amp::UncompressedChange> {
    changes
        .iter()
        .map(|change| amp::UncompressedChange {
            // we've already confirmed that all change.actor's are valid
            actor_id: actors[change.actor].clone(),
            seq: change.seq,
            time: change.time,
            start_op: change.max_op - change.ops.len() as u64 + 1,
            hash: None,
            message: change.message.clone(),
            operations: change
                .ops
                .iter()
                .map(|op| amp::Op {
                    action: op.action.clone(),
                    insert: op.insert,
                    key: op.key.clone(),
                    obj: op.obj.clone(),
                    // we've already confirmed that all op.actor's are valid
                    pred: pred_into(&op.pred, actors),
                })
                .collect(),
            deps: Vec::new(),
            extra_bytes: change.extra_bytes.clone(),
        })
        .collect()
}

fn load_blocks(bytes: &[u8]) -> Result<Vec<Change>, AutomergeError> {
    let mut changes = Vec::new();
    for slice in split_blocks(bytes).into_iter() {
        decode_block(slice, &mut changes)?;
    }
    Ok(changes)
}

fn split_blocks(bytes: &[u8]) -> Vec<&[u8]> {
    // split off all valid blocks - ignore the rest if its corrupted or truncated
    let mut blocks = Vec::new();
    let mut cursor = bytes;
    while let Some(block) = pop_block(cursor) {
        blocks.push(&cursor[block.clone()]);
        if cursor.len() <= block.end {
            break;
        }
        cursor = &cursor[block.end..];
    }
    blocks
}

fn pop_block(bytes: &[u8]) -> Option<Range<usize>> {
    if bytes.len() < 4 || bytes[0..4] != MAGIC_BYTES {
        // not reporting error here - file got corrupted?
        return None;
    }
    let (val, len) = read_leb128(&mut &bytes[HEADER_BYTES..]).unwrap();
    let end = HEADER_BYTES + len + val;
    if end > bytes.len() {
        // not reporting error here - file got truncated?
        return None;
    }
    Some(0..end)
}

fn decode_document(bytes: &[u8]) -> Result<Vec<Change>, AutomergeError> {
    let (chunktype, _hash, mut cursor) = decode_header(&bytes)?;

    if chunktype > 0 {
        return Err(AutomergeError::EncodingError);
    }

    let actors = decode_actors(&bytes, &mut cursor, None)?;
    // FIXME
    // I should calculate the deads generated on decode and confirm they match these
    let _heads = decode_hashes(&bytes, &mut cursor)?;

    let changes_info = decode_column_info(&bytes, &mut cursor, true)?;
    let ops_info = decode_column_info(&bytes, &mut cursor, true)?;

    let changes_data = decode_columns(&mut cursor, changes_info);
    let mut doc_changes: Vec<_> = ChangeIterator::new(&bytes, &changes_data).collect();

    let ops_data = decode_columns(&mut cursor, ops_info);
    let mut doc_ops: Vec<_> = DocOpIterator::new(&bytes, &actors, &ops_data).collect();

    group_doc_change_and_doc_ops(&mut doc_changes, &mut doc_ops, &actors)?;

    let mut uncompressed_changes = doc_changes_to_uncompressed_changes(&doc_changes, &actors);

    compress_doc_changes(&mut uncompressed_changes, &doc_changes)
        .ok_or(AutomergeError::EncodingError)
}

fn compress_doc_changes(
    uncompressed_changes: &mut [amp::UncompressedChange],
    doc_changes: &[DocChange],
) -> Option<Vec<Change>> {
    let mut changes: Vec<Change> = Vec::with_capacity(doc_changes.len());

    // fill out the hashes as we go

    for i in 0..doc_changes.len() {
        let deps = &mut uncompressed_changes.get_mut(i)?.deps;
        for idx in doc_changes.get(i)?.deps.iter() {
            deps.push(changes.get(*idx)?.hash)
        }
        changes.push(uncompressed_changes.get(i)?.into());
    }

    Some(changes)
}

fn group_doc_ops(changes: &[amp::UncompressedChange], actors: &[amp::ActorId]) -> Vec<DocOp> {
    let mut by_obj_id = HashMap::<amp::ObjectId, HashMap<amp::Key, HashMap<amp::OpId, _>>>::new();
    let mut by_ref = HashMap::<amp::ObjectId, HashMap<amp::Key, Vec<amp::OpId>>>::new();
    let mut is_seq = HashSet::<amp::ObjectId>::new();
    let mut ops = Vec::new();

    for change in changes {
        for (i, op) in change.operations.iter().enumerate() {
            let opid = amp::OpId(change.start_op + i as u64, change.actor_id.clone());
            let objid = op.obj.clone();
            if let amp::OpType::Make(amp::ObjType::Sequence(_)) = op.action {
                is_seq.insert(opid.clone().into());
            }

            let key = if !op.insert {
                op.key.clone()
            } else {
                by_ref
                    .entry(objid.clone())
                    .or_default()
                    .entry(op.key.clone())
                    .or_default()
                    .push(opid.clone());
                opid.clone().into()
            };

            by_obj_id
                .entry(objid.clone())
                .or_default()
                .entry(key.clone())
                .or_default()
                .insert(
                    opid.clone(),
                    DocOp {
                        actor: actors.iter().position(|a| a == &opid.1).unwrap(),
                        ctr: opid.0,
                        action: op.action.clone(),
                        obj: op.obj.clone(),
                        key: op.key.clone(),
                        succ: Vec::new(),
                        pred: Vec::new(),
                        insert: op.insert,
                    },
                );

            for pred in &op.pred {
                by_obj_id
                    .entry(objid.clone())
                    .or_default()
                    .entry(key.clone())
                    .or_default()
                    .get_mut(pred)
                    .unwrap()
                    .succ
                    .push((opid.0, actors.iter().position(|a| a == &opid.1).unwrap()));
            }
        }
    }

    for objid in by_obj_id.keys().sorted() {
        let mut keys = Vec::new();
        if is_seq.contains(objid) {
            let mut stack = vec![amp::ElementId::Head];
            while let Some(key) = stack.pop() {
                if key != amp::ElementId::Head {
                    keys.push(amp::Key::Seq(key.clone()))
                }
                for opid in by_ref
                    .entry(objid.clone())
                    .or_default()
                    .entry(key.into())
                    .or_default()
                    .iter()
                    .sorted()
                {
                    stack.push(opid.into())
                }
            }
        } else {
            keys = by_obj_id
                .get(objid)
                .map(|d| d.keys().sorted().cloned().collect())
                .unwrap_or_default()
        }

        for key in keys {
            if let Some(key_ops) = by_obj_id.get(objid).and_then(|d| d.get(&key)) {
                for opid in key_ops.keys().sorted() {
                    let op = key_ops.get(opid).unwrap();
                    if op.action != amp::OpType::Del {
                        ops.push(op.clone());
                    }
                }
            }
        }
    }

    ops
}

fn get_heads(changes: &[amp::UncompressedChange]) -> HashSet<amp::ChangeHash> {
    changes.iter().fold(HashSet::new(), |mut acc, c| {
        if let Some(hash) = c.hash {
            acc.insert(hash);
        }
        for dep in c.deps.iter() {
            acc.remove(&dep);
        }
        acc
    })
}

pub(crate) fn encode_document(
    changes: Vec<amp::UncompressedChange>,
) -> Result<Vec<u8>, AutomergeError> {
    let mut bytes: Vec<u8> = Vec::new();
    let mut hasher = Sha256::new();

    let heads = get_heads(&changes);

    // this assumes that all actor_ids referenced are seen in changes.actor_id which is true
    // so long as we have a full history
    let mut actors: Vec<_> = changes
        .iter()
        .map(|c| &c.actor_id)
        .unique()
        .sorted()
        .cloned()
        .collect();

    let (change_bytes, change_info) = ChangeEncoder::encode_changes(&changes, &actors);

    let doc_ops = group_doc_ops(&changes, &actors);

    let (ops_bytes, ops_info) = DocOpEncoder::encode_doc_ops(&doc_ops, &mut actors);

    bytes.extend(&MAGIC_BYTES);
    bytes.extend(vec![0, 0, 0, 0]); // we dont know the hash yet so fill in a fake
    bytes.push(BLOCK_TYPE_DOC);

    let mut chunk = Vec::new();

    actors.len().encode(&mut chunk)?;

    for a in actors.iter() {
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

    hasher.input(&bytes[CHUNK_START..bytes.len()]);
    let hash_result = hasher.result();
    //let hash: amp::ChangeHash = hash_result[..].try_into().unwrap();

    bytes.splice(HASH_RANGE, hash_result[0..4].iter().cloned());

    Ok(bytes)
}

pub(crate) const MAGIC_BYTES: [u8; 4] = [0x85, 0x6f, 0x4a, 0x83];
pub(crate) const PREAMBLE_BYTES: usize = 8;
pub(crate) const HEADER_BYTES: usize = PREAMBLE_BYTES + 1;

#[cfg(test)]
mod tests {
    use super::*;
    use amp::{ActorId, UncompressedChange};
    use std::str::FromStr;

    #[test]
    fn test_empty_change() {
        let change1 = amp::UncompressedChange {
            start_op: 1,
            seq: 2,
            time: 1234,
            message: None,
            hash: None,
            actor_id: amp::ActorId::from_str("deadbeefdeadbeef").unwrap(),
            deps: vec![],
            operations: vec![],
            extra_bytes: vec![1, 1, 1],
        };
        let bin1: Change = change1.clone().try_into().unwrap();
        let change2 = bin1.decode();
        let bin2 = Change::try_from(change2.clone()).unwrap();
        assert_eq!(bin1, bin2);
        assert_eq!(change1, change2);
    }

    #[test]
    fn test_complex_change() {
        let actor1 = amp::ActorId::from_str("deadbeefdeadbeef").unwrap();
        let actor2 = amp::ActorId::from_str("feeddefaff").unwrap();
        let actor3 = amp::ActorId::from_str("00101010fafafafa").unwrap();
        let opid1 = amp::OpId::new(102, &actor1);
        let opid2 = amp::OpId::new(391, &actor1);
        let opid3 = amp::OpId::new(299, &actor2);
        let opid4 = amp::OpId::new(762, &actor3);
        let opid5 = amp::OpId::new(100_203, &actor2);
        let obj1 = amp::ObjectId::Id(opid1.clone());
        let obj2 = amp::ObjectId::Root;
        let obj3 = amp::ObjectId::Id(opid4.clone());
        let key1 = amp::Key::Map("field1".into());
        let key2 = amp::Key::Map("field2".into());
        let key3 = amp::Key::Map("field3".into());
        let head = amp::Key::head();
        let keyseq1 = amp::Key::from(&opid1);
        let keyseq2 = amp::Key::from(&opid2);
        let insert = false;
        let change1 = amp::UncompressedChange {
            start_op: 123,
            seq: 29291,
            time: 12_341_231,
            message: Some("This is my message".into()),
            hash: None,
            actor_id: actor1,
            deps: vec![],
            operations: vec![
                amp::Op {
                    action: amp::OpType::Set(amp::ScalarValue::F64(10.0)),
                    key: key1,
                    obj: obj1.clone(),
                    insert,
                    pred: vec![opid1.clone(), opid2.clone()],
                },
                amp::Op {
                    action: amp::OpType::Set(amp::ScalarValue::Counter(-11)),
                    key: key2.clone(),
                    obj: obj1.clone(),
                    insert,
                    pred: vec![opid1.clone(), opid2.clone()],
                },
                amp::Op {
                    action: amp::OpType::Set(amp::ScalarValue::Timestamp(20)),
                    key: key3,
                    obj: obj1,
                    insert,
                    pred: vec![opid1.clone(), opid2],
                },
                amp::Op {
                    action: amp::OpType::Set(amp::ScalarValue::Str("some value".into())),
                    key: key2.clone(),
                    obj: obj2.clone(),
                    insert,
                    pred: vec![opid3.clone(), opid4.clone()],
                },
                amp::Op {
                    action: amp::OpType::Make(amp::ObjType::map()),
                    key: key2.clone(),
                    obj: obj2.clone(),
                    insert,
                    pred: vec![opid3.clone(), opid4.clone()],
                },
                amp::Op {
                    action: amp::OpType::Set(amp::ScalarValue::Str("val1".into())),
                    key: head.clone(),
                    obj: obj3.clone(),
                    insert: true,
                    pred: vec![opid3, opid4.clone()],
                },
                amp::Op {
                    action: amp::OpType::Set(amp::ScalarValue::Str("val2".into())),
                    key: head,
                    obj: obj3.clone(),
                    insert: true,
                    pred: vec![opid4.clone(), opid5.clone()],
                },
                amp::Op {
                    action: amp::OpType::Inc(10),
                    key: key2,
                    obj: obj2,
                    insert,
                    pred: vec![opid1.clone(), opid5.clone()],
                },
                amp::Op {
                    action: amp::OpType::Del,
                    obj: obj3.clone(),
                    key: keyseq1,
                    insert: true,
                    pred: vec![opid4.clone(), opid5.clone()],
                },
                amp::Op {
                    action: amp::OpType::Del,
                    obj: obj3.clone(),
                    key: keyseq2,
                    insert: true,
                    pred: vec![opid4, opid5],
                },
                amp::Op {
                    action: amp::OpType::Set(amp::ScalarValue::Cursor(opid1)),
                    obj: obj3,
                    key: "somekey".into(),
                    insert: false,
                    pred: Vec::new(),
                },
            ],
            extra_bytes: vec![1, 2, 3],
        };
        let bin1 = Change::from(change1.clone());
        let change2 = bin1.decode();
        let bin2 = Change::from(change2.clone());
        assert_eq!(change1, change2);
        assert_eq!(bin1, bin2);
    }

    #[test]
    fn test_encode_decode_document() {
        let actor = amp::ActorId::random();
        let mut backend = crate::Backend::init();
        let change1 = amp::UncompressedChange {
            start_op: 1,
            seq: 1,
            time: 0,
            message: None,
            hash: None,
            actor_id: actor.clone(),
            deps: Vec::new(),
            operations: vec![
                amp::Op {
                    action: amp::OpType::Set("somevalue".into()),
                    obj: amp::ObjectId::Root,
                    key: "somekey".into(),
                    insert: false,
                    pred: Vec::new(),
                },
                amp::Op {
                    action: amp::OpType::Make(amp::ObjType::list()),
                    obj: amp::ObjectId::Root,
                    key: "somelist".into(),
                    insert: false,
                    pred: Vec::new(),
                },
                amp::Op {
                    action: amp::OpType::Set(amp::ScalarValue::Str("elem".into())),
                    obj: actor.op_id_at(2).into(),
                    key: amp::ElementId::Head.into(),
                    insert: true,
                    pred: Vec::new(),
                },
                amp::Op {
                    action: amp::OpType::Set(amp::ScalarValue::Cursor(actor.op_id_at(3))),
                    obj: amp::ObjectId::Root,
                    key: "cursor".into(),
                    insert: false,
                    pred: Vec::new(),
                },
            ],
            extra_bytes: vec![1, 2, 3],
        };
        let binchange1: Change = Change::try_from(change1.clone()).unwrap();
        backend.apply_changes(vec![binchange1.clone()]).unwrap();

        let change2 = amp::UncompressedChange {
            start_op: 5,
            seq: 2,
            time: 0,
            message: None,
            hash: None,
            actor_id: change1.actor_id,
            deps: vec![binchange1.hash],
            operations: vec![amp::Op {
                action: amp::OpType::Set("someothervalue".into()),
                obj: amp::ObjectId::Root,
                key: "someotherkey".into(),
                insert: false,
                pred: Vec::new(),
            }],
            extra_bytes: vec![],
        };
        let binchange2: Change = Change::try_from(change2).unwrap();
        backend.apply_changes(vec![binchange2]).unwrap();

        let changes = backend.get_changes(&[]);
        let encoded = backend.save().unwrap();
        let loaded_changes = Change::load_document(&encoded).unwrap();
        let decoded_loaded: Vec<amp::UncompressedChange> = loaded_changes
            .clone()
            .into_iter()
            .map(|c| c.decode())
            .collect();
        let decoded_preload: Vec<amp::UncompressedChange> =
            changes.clone().into_iter().map(|c| c.decode()).collect();
        assert_eq!(decoded_loaded, decoded_preload);
        assert_eq!(
            loaded_changes,
            changes.into_iter().cloned().collect::<Vec<Change>>()
        );
    }

    #[test_env_log::test]
    fn test_encode_decode_document_large_enough_for_compression() {
        let actor = amp::ActorId::random();
        let mut backend = crate::Backend::init();
        let mut change1 = amp::UncompressedChange {
            start_op: 1,
            seq: 1,
            time: 0,
            message: None,
            hash: None,
            actor_id: actor.clone(),
            deps: Vec::new(),
            operations: vec![amp::Op {
                action: amp::OpType::Make(amp::ObjType::list()),
                obj: amp::ObjectId::Root,
                key: "somelist".into(),
                insert: false,
                pred: Vec::new(),
            }],
            extra_bytes: vec![1, 2, 3],
        };
        let mut last_elem_id: amp::Key = amp::ElementId::Head.into();
        for i in 0..256 {
            change1.operations.push(amp::Op {
                action: amp::OpType::Set(format!("value {}", i).as_str().into()),
                obj: actor.op_id_at(1).into(),
                key: last_elem_id,
                insert: true,
                pred: Vec::new(),
            });
            last_elem_id = actor.op_id_at(i + 2).into();
        }
        let binchange1: Change = Change::try_from(change1).unwrap();
        backend.apply_changes(vec![binchange1]).unwrap();

        let changes = backend.get_changes(&[]);
        let encoded = backend.save().unwrap();
        let loaded_changes = Change::load_document(&encoded).unwrap();
        let decoded_loaded: Vec<amp::UncompressedChange> = loaded_changes
            .clone()
            .into_iter()
            .map(|c| c.decode())
            .collect();
        let decoded_preload: Vec<amp::UncompressedChange> =
            changes.clone().into_iter().map(|c| c.decode()).collect();
        assert_eq!(decoded_loaded[0].operations.len(), 257);
        assert_eq!(decoded_loaded, decoded_preload);
        assert_eq!(
            loaded_changes,
            changes.into_iter().cloned().collect::<Vec<Change>>()
        );
    }

    #[test]
    fn test_invalid_document_checksum() {
        let change = UncompressedChange {
            operations: Vec::new(),
            actor_id: ActorId::random(),
            hash: None,
            seq: 1,
            start_op: 1,
            time: 0,
            message: None,
            deps: Vec::new(),
            extra_bytes: Vec::new(),
        };
        let mut doc = encode_document(vec![change]).unwrap();
        let hash = doc[4..8].try_into().unwrap();
        doc[4] = 0;
        doc[5] = 0;
        doc[6] = 0;
        doc[7] = 1;
        let decode_result = decode_document(&doc);
        assert_eq!(
            Err(AutomergeError::InvalidChecksum {
                found: [0, 0, 0, 1],
                calculated: hash,
            }),
            decode_result
        )
    }
}
