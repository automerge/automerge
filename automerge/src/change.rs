use crate::columnar::ColumnEncoder;
use crate::columnar::OperationIterator;
use crate::columnar::{ChangeEncoder, DocOpEncoder};
use crate::decoding;
use crate::decoding::Decodable;
use crate::encoding::{Encodable, DEFLATE_MIN_SIZE};
use crate::expanded_op::ExpandedOpIterator;
use crate::{AutomergeError, ElemId, IndexedCache, Key, ObjId, Op, OpId, Transaction, HEAD, ROOT};
use automerge_protocol as amp;
use core::ops::Range;
use flate2::{bufread::DeflateEncoder, Compression};
use itertools::Itertools;
use sha2::Digest;
use sha2::Sha256;
use std::collections::HashMap;
use std::collections::HashSet;
use std::convert::TryInto;
use std::fmt::Debug;
use std::io::Read;
use std::io::Write;
use std::str;
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
    actors_index: &IndexedCache<amp::ActorId>,
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

impl From<amp::Change> for EncodedChange {
    fn from(value: amp::Change) -> Self {
        encode(&value)
    }
}

impl From<&amp::Change> for EncodedChange {
    fn from(value: &amp::Change) -> Self {
        encode(value)
    }
}

fn encode(change: &amp::Change) -> EncodedChange {
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

    EncodedChange {
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
    actors: Vec<amp::ActorId>,
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

    // encode first actor
    let mut actors = vec![change.actor_id.clone()];
    change.actor_id.to_bytes().encode(&mut bytes).unwrap();

    // encode seq, start_op, time, message
    change.seq.encode(&mut bytes).unwrap();
    change.start_op.encode(&mut bytes).unwrap();
    change.time.encode(&mut bytes).unwrap();
    let message = bytes.len() + 1;
    change.message.encode(&mut bytes).unwrap();
    let message = message..bytes.len();

    let expanded_ops =
        ExpandedOpIterator::new(&change.operations, change.start_op, change.actor_id.clone());

    // encode ops into a side buffer - collect all other actors
    let (ops_buf, mut ops) = ColumnEncoder::encode_ops(expanded_ops, &mut actors);

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
pub struct EncodedChange {
    bytes: ChangeBytes,
    body_start: usize,
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

impl EncodedChange {
    pub fn actor_id(&self) -> &amp::ActorId {
        &self.actors[0]
    }

    #[instrument(level = "debug", skip(bytes))]
    pub fn load_document(bytes: &[u8]) -> Result<Vec<EncodedChange>, AutomergeError> {
        unimplemented!()
        //load_blocks(bytes)
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Result<EncodedChange, decoding::Error> {
        unimplemented!()
        //decode_change(bytes)
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
            str::from_utf8(m).map(ToString::to_string).ok()
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
                    action: op.action.into(),
                    obj: op.obj.clone().into_owned(),
                    key: op.key.into_owned(),
                    pred: op.pred.into_owned(),
                    insert: op.insert,
                })
                .collect(),
            extra_bytes: self.extra_bytes().into(),
        }
    }

    pub fn iter_ops(&self) -> OperationIterator {
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

/*
impl TryFrom<&[u8]> for EncodedChange {
    type Error = decoding::Error;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        EncodedChange::from_bytes(bytes.to_vec())
    }
}
*/

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

fn export_objid(id: &ObjId, actors: &IndexedCache<amp::ActorId>) -> amp::ObjectId {
    if id == &ROOT {
        amp::ObjectId::Root
    } else {
        export_opid(&id.0, actors).into()
    }
}

fn export_elemid(id: &ElemId, actors: &IndexedCache<amp::ActorId>) -> amp::ElementId {
    if id == &HEAD {
        amp::ElementId::Head
    } else {
        export_opid(&id.0, actors).into()
    }
}

fn export_opid(id: &OpId, actors: &IndexedCache<amp::ActorId>) -> amp::OpId {
    amp::OpId(id.0, actors.get(id.1).clone())
}

fn export_op(
    op: &Op,
    actors: &IndexedCache<amp::ActorId>,
    props: &IndexedCache<String>,
) -> amp::Op {
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
    actors: &IndexedCache<amp::ActorId>,
    props: &IndexedCache<String>,
) -> EncodedChange {
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
