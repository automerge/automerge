use crate::columnar;
use crate::columnar::{
    ColumnEncoder, KeyIterator, ObjIterator, OperationIterator, PredIterator, ValueIterator,
};
use crate::encoding::{Decodable, Encodable};
use crate::error::{AutomergeError, InvalidChangeError};
use automerge_protocol as amp;
use core::fmt::Debug;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::io::Write;
use std::ops::Range;
use std::str;

const HASH_BYTES: usize = 32;
const CHUNK_TYPE: u8 = 1;
const CHUNK_START: usize = 8;
const HASH_RANGE: Range<usize> = 4..8;

impl From<amp::UncompressedChange> for Change {
    fn from(value: amp::UncompressedChange) -> Self {
        encode(&value)
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

    bytes.push(CHUNK_TYPE);

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

    Change {
        bytes,
        hash,
        body: chunk.body,
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
    actors: Vec<amp::ActorID>,
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
    let mut actors = Vec::new();
    actors.push(uncompressed_change.actor_id.clone());
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
    let extra_bytes = bytes.len()..bytes.len() + uncompressed_change.extra_bytes.len();
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
pub struct Change {
    pub bytes: Vec<u8>,
    pub hash: amp::ChangeHash,
    pub seq: u64,
    pub start_op: u64,
    pub time: i64,
    body: Range<usize>,
    message: Range<usize>,
    actors: Vec<amp::ActorID>,
    pub deps: Vec<amp::ChangeHash>,
    ops: HashMap<u32, Range<usize>>,
    extra_bytes: Range<usize>,
}

impl Change {
    pub fn actor_id(&self) -> &amp::ActorID {
        &self.actors[0]
    }

    pub fn parse(bytes: &[u8]) -> Result<Vec<Change>, AutomergeError> {
        let mut changes = Vec::new();
        let mut cursor = &bytes[..];
        while !cursor.is_empty() {
            let (val, len) = read_leb128(&mut &cursor[HEADER_BYTES..])?;
            let (data, rest) = cursor.split_at(HEADER_BYTES + val + len);
            changes.push(Self::from_bytes(data.to_vec())?);
            cursor = rest;
        }
        Ok(changes)
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Result<Change, AutomergeError> {
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

        if chunktype == 0 {
            return Err(AutomergeError::EncodingError); // Format not implemented
        }

        if chunktype > 1 {
            return Err(AutomergeError::EncodingError);
        }

        let mut hasher = Sha256::new();
        hasher.input(&bytes[PREAMBLE_BYTES..]);
        let hash = hasher.result()[..]
            .try_into()
            .map_err(InvalidChangeError::from)?;

        let mut cursor = body.clone();
        let mut deps = Vec::new();
        let num_deps = read_slice(&bytes, &mut cursor)?;
        for _ in 0..num_deps {
            let hash = cursor.start..(cursor.start + HASH_BYTES);
            cursor = hash.end..cursor.end;
            //let hash = slice_n_bytes(bytes, HASH_BYTES)?;
            deps.push(bytes[hash].try_into().map_err(InvalidChangeError::from)?);
        }
        let actor = amp::ActorID::from(&bytes[slice_bytes(&bytes, &mut cursor)?]);
        let seq = read_slice(&bytes, &mut cursor)?;
        let start_op = read_slice(&bytes, &mut cursor)?;
        let time = read_slice(&bytes, &mut cursor)?;
        let message = slice_bytes(&bytes, &mut cursor)?;
        let num_actors = read_slice(&bytes, &mut cursor)?;
        let mut actors = vec![actor];
        for _ in 0..num_actors {
            actors.push(amp::ActorID::from(
                &bytes[slice_bytes(&bytes, &mut cursor)?],
            ));
        }

        let num_columns = read_slice(&bytes, &mut cursor)?;
        let mut columns = Vec::with_capacity(num_columns);
        let mut last_id = 0;
        for _ in 0..num_columns {
            let id: u32 = read_slice(&bytes, &mut cursor)?;
            if id <= last_id {
                return Err(AutomergeError::EncodingError);
            }
            last_id = id;
            let length = read_slice(&bytes, &mut cursor)?;
            columns.push((id, length));
        }

        let mut ops = HashMap::new();
        for (id, length) in columns.iter() {
            let start = cursor.start;
            let end = start + length;
            cursor = end..cursor.end;
            ops.insert(*id, start..end);
        }

        Ok(Change {
            bytes,
            hash,
            body,
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

    pub fn max_op(&self) -> u64 {
        // TODO - this could be a lot more efficient
        let len = self.iter_ops().count();
        self.start_op + (len as u64) - 1
    }

    fn message(&self) -> Option<String> {
        let m = &self.bytes[self.message.clone()];
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
            message: self.message(),
            actor_id: self.actors[0].clone(),
            deps: self.deps.clone(),
            operations: self.iter_ops().collect(),
            extra_bytes: self.extra_bytes().into(),
        }
    }

    fn col_iter<'a, T>(&'a self, col_id: u32) -> T
    where
        T: From<&'a [u8]>,
    {
        self.ops
            .get(&col_id)
            .map(|r| T::from(&self.bytes[r.clone()]))
            .unwrap_or_else(|| T::from(&[] as &[u8]))
    }

    pub fn iter_ops(&self) -> OperationIterator {
        OperationIterator {
            objs: ObjIterator {
                actors: &self.actors,
                actor: self.col_iter(columnar::COL_OBJ_ACTOR),
                ctr: self.col_iter(columnar::COL_OBJ_CTR),
            },
            keys: KeyIterator {
                actors: &self.actors,
                actor: self.col_iter(columnar::COL_KEY_ACTOR),
                ctr: self.col_iter(columnar::COL_KEY_CTR),
                str: self.col_iter(columnar::COL_KEY_STR),
            },
            value: ValueIterator {
                val_len: self.col_iter(columnar::COL_VAL_LEN),
                val_raw: self.col_iter(columnar::COL_VAL_RAW),
            },
            pred: PredIterator {
                actors: &self.actors,
                pred_num: self.col_iter(columnar::COL_PRED_NUM),
                pred_actor: self.col_iter(columnar::COL_PRED_ACTOR),
                pred_ctr: self.col_iter(columnar::COL_PRED_CTR),
            },
            insert: self.col_iter(columnar::COL_INSERT),
            action: self.col_iter(columnar::COL_ACTION),
        }
    }

    pub fn extra_bytes(&self) -> &[u8] {
        &self.bytes[self.extra_bytes.clone()]
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
    let view = &bytes[cursor.clone()];
    let mut reader = &view[..];
    let val = T::decode::<&[u8]>(&mut reader).ok_or(AutomergeError::EncodingError);
    let len = view.len() - reader.len();
    *cursor = (cursor.start + len)..cursor.end;
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

const MAGIC_BYTES: [u8; 4] = [0x85, 0x6f, 0x4a, 0x83];
const PREAMBLE_BYTES: usize = 8;
const HEADER_BYTES: usize = PREAMBLE_BYTES + 1;

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_empty_change() {
        let change1 = amp::UncompressedChange {
            start_op: 1,
            seq: 2,
            time: 1234,
            message: None,
            actor_id: amp::ActorID::from_str("deadbeefdeadbeef").unwrap(),
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
        let actor1 = amp::ActorID::from_str("deadbeefdeadbeef").unwrap();
        let actor2 = amp::ActorID::from_str("feeddefaff").unwrap();
        let actor3 = amp::ActorID::from_str("00101010fafafafa").unwrap();
        let opid1 = amp::OpID::new(102, &actor1);
        let opid2 = amp::OpID::new(391, &actor1);
        let opid3 = amp::OpID::new(299, &actor2);
        let opid4 = amp::OpID::new(762, &actor3);
        let opid5 = amp::OpID::new(100_203, &actor2);
        let obj1 = amp::ObjectID::ID(opid1.clone());
        let obj2 = amp::ObjectID::Root;
        let obj3 = amp::ObjectID::ID(opid4.clone());
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
                    pred: vec![opid1, opid5.clone()],
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
                    obj: obj3,
                    key: keyseq2,
                    insert: true,
                    pred: vec![opid4, opid5],
                },
            ],
            extra_bytes: vec![1, 2, 3],
        };
        let bin1 = Change::try_from(change1.clone()).unwrap();
        let change2 = bin1.decode();
        let bin2 = Change::try_from(change2.clone()).unwrap();
        assert_eq!(bin1, bin2);
        assert_eq!(change1, change2);
    }
}
