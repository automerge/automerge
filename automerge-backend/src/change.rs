use crate::columnar::{
    ColumnEncoder, KeyIterator, ObjIterator, OperationIterator, PredIterator, ValueIterator,
};
use crate::encoding::{Decodable, Encodable};
use crate::error::AutomergeError;
use automerge_protocol::{ActorID, ChangeHash, Operation};
use core::fmt::Debug;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::io;
use std::io::Write;
use std::ops::Range;
use std::str;

const HASH_BYTES: usize = 32;
const CHUNK_TYPE: u8 = 1;

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct Change {
    #[serde(rename = "ops")]
    pub operations: Vec<Operation>,
    #[serde(rename = "actor")]
    pub actor_id: ActorID,
    //pub hash: ChangeHash,
    pub seq: u64,
    #[serde(rename = "startOp")]
    pub start_op: u64,
    pub time: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    pub deps: Vec<ChangeHash>,
}

impl Change {
    pub fn max_op(&self) -> u64 {
        self.start_op + (self.operations.len() as u64) - 1
    }

    pub fn encode(&self) -> BinChange {
        let mut buf = Vec::new();
        let mut hasher = Sha256::new();

        let chunk = self.encode_chunk();

        hasher.input(&chunk);

        buf.extend(&MAGIC_BYTES);
        buf.extend(&hasher.result()[0..4]);
        buf.extend(&chunk);

        // possible optimization here - i can assemble the metadata without having to parse
        // the generated object
        // ---
        // unwrap :: we generated this binchange so there's no chance of bad format
        // ---

        BinChange::from_bytes(buf).unwrap()
    }

    fn encode_chunk(&self) -> Vec<u8> {
        let mut chunk = vec![CHUNK_TYPE]; // chunk type is always 1
                                          // unwrap - io errors cant happen when writing to an in memory vec
        let data = self.encode_chunk_body().unwrap();
        leb128::write::unsigned(&mut chunk, data.len() as u64).unwrap();
        chunk.extend(&data);
        chunk
    }

    fn encode_chunk_body(&self) -> io::Result<Vec<u8>> {
        let mut buf = Vec::new();
        let mut actors = Vec::new();

        actors.push(self.actor_id.clone());

        self.actor_id.to_bytes().encode(&mut buf)?;
        self.seq.encode(&mut buf)?;
        self.start_op.encode(&mut buf)?;
        self.time.encode(&mut buf)?;
        self.message.encode(&mut buf)?;

        let ops_buf = ColumnEncoder::encode_ops(&self.operations, &mut actors);

        actors[1..].encode(&mut buf)?;

        let mut deps = self.deps.clone();
        deps.sort_unstable();
        deps.len().encode(&mut buf)?;
        for hash in deps.iter() {
            buf.write_all(&hash.0)?;
        }

        buf.write_all(&ops_buf)?;

        Ok(buf)
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct BinChange {
    pub bytes: Vec<u8>,
    pub hash: ChangeHash,
    pub seq: u64,
    pub start_op: u64,
    pub time: i64,
    body: Range<usize>,
    message: Range<usize>,
    actors: Vec<ActorID>,
    pub deps: Vec<ChangeHash>,
    ops: HashMap<u32, Range<usize>>,
}

impl BinChange {
    pub fn actor_id(&self) -> ActorID {
        self.actors[0].clone()
    }

    pub fn extract(bytes: &[u8]) -> Result<Vec<BinChange>, AutomergeError> {
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

    pub fn from_bytes(bytes: Vec<u8>) -> Result<BinChange, AutomergeError> {
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
        let hash = hasher.result()[..].try_into()?;

        let mut cursor = body.clone();
        let actor = ActorID::from_bytes(&bytes[slice_bytes(&bytes, &mut cursor)?]);
        let seq = read_slice(&bytes, &mut cursor)?;
        let start_op = read_slice(&bytes, &mut cursor)?;
        let time = read_slice(&bytes, &mut cursor)?;
        let message = slice_bytes(&bytes, &mut cursor)?;
        let num_actors = read_slice(&bytes, &mut cursor)?;
        let mut actors = vec![actor];
        for _ in 0..num_actors {
            let actor = ActorID::from_bytes(&bytes[slice_bytes(&bytes, &mut cursor)?]);
            actors.push(actor);
        }
        let mut deps = Vec::new();
        let num_deps = read_slice(&bytes, &mut cursor)?;
        for _ in 0..num_deps {
            let hash = cursor.start..(cursor.start + HASH_BYTES);
            cursor = hash.end..cursor.end;
            //let hash = slice_n_bytes(bytes, HASH_BYTES)?;
            deps.push(bytes[hash].try_into()?);
        }
        let mut ops = HashMap::new();
        let mut last_id = 0;
        while !bytes[cursor.clone()].is_empty() {
            let id = read_slice(&bytes, &mut cursor)?;
            if id < last_id {
                return Err(AutomergeError::EncodingError);
            }
            last_id = id;
            let column = slice_bytes(&bytes, &mut cursor)?;
            ops.insert(id, column);
        }

        Ok(BinChange {
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
        })
    }

    pub fn max_op(&self) -> u64 {
        // FIXME - this is crazy inefficent
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

    pub fn decode(&self) -> Change {
        Change {
            start_op: self.start_op,
            seq: self.seq,
            time: self.time,
            message: self.message(),
            actor_id: self.actors[0].clone(),
            deps: self.deps.clone(),
            operations: self.iter_ops().collect(),
        }
    }

    fn col_iter<'a, T>(&'a self, col_id: u32) -> T
    where
        T: From<&'a [u8]>,
    {
        let empty = 0..0;
        let range = self.ops.get(&col_id).unwrap_or(&empty);
        let buf = &self.bytes[range.clone()];
        T::from(&buf)
    }

    pub fn iter_ops(&self) -> OperationIterator {
        OperationIterator {
            objs: ObjIterator {
                actors: &self.actors,
                actor: self.col_iter(COL_OBJ_ACTOR),
                ctr: self.col_iter(COL_OBJ_CTR),
            },
            chld: ObjIterator {
                actors: &self.actors,
                actor: self.col_iter(COL_CHILD_ACTOR),
                ctr: self.col_iter(COL_CHILD_CTR),
            },
            keys: KeyIterator {
                actors: &self.actors,
                actor: self.col_iter(COL_KEY_ACTOR),
                ctr: self.col_iter(COL_KEY_CTR),
                str: self.col_iter(COL_KEY_STR),
            },
            value: ValueIterator {
                val_len: self.col_iter(COL_VAL_LEN),
                val_raw: self.col_iter(COL_VAL_RAW),
            },
            pred: PredIterator {
                actors: &self.actors,
                pred_num: self.col_iter(COL_PRED_NUM),
                pred_actor: self.col_iter(COL_PRED_ACTOR),
                pred_ctr: self.col_iter(COL_PRED_CTR),
            },
            insert: self.col_iter(COL_INSERT),
            action: self.col_iter(COL_ACTION),
        }
    }
}

impl From<&Change> for BinChange {
    fn from(change: &Change) -> BinChange {
        change.encode()
    }
}

impl From<Change> for BinChange {
    fn from(change: Change) -> BinChange {
        change.encode()
    }
}

impl From<&BinChange> for Change {
    fn from(change: &BinChange) -> Change {
        change.decode()
    }
}

impl TryFrom<&[u8]> for BinChange {
    type Error = AutomergeError;
    fn try_from(bytes: &[u8]) -> Result<Self, AutomergeError> {
        BinChange::from_bytes(bytes.to_vec())
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

/*
const CHUNK_TYPE: u8 = 1;

const VALUE_TYPE_NULL: usize = 0;
const VALUE_TYPE_FALSE: usize = 1;
const VALUE_TYPE_TRUE: usize = 2;
const VALUE_TYPE_LEB128_UINT: usize = 3;
const VALUE_TYPE_LEB128_INT: usize = 4;
const VALUE_TYPE_IEEE754: usize = 5;
const VALUE_TYPE_UTF8: usize = 6;
const VALUE_TYPE_BYTES: usize = 7;
const VALUE_TYPE_COUNTER: usize = 8;
const VALUE_TYPE_TIMESTAMP: usize = 9;
const VALUE_TYPE_MIN_UNKNOWN: usize = 10;
const VALUE_TYPE_MAX_UNKNOWN: usize = 15;
*/

const COLUMN_TYPE_GROUP_CARD: u32 = 0;
const COLUMN_TYPE_ACTOR_ID: u32 = 1;
const COLUMN_TYPE_INT_RLE: u32 = 2;
const COLUMN_TYPE_INT_DELTA: u32 = 3;
const COLUMN_TYPE_BOOLEAN: u32 = 4;
const COLUMN_TYPE_STRING_RLE: u32 = 5;
const COLUMN_TYPE_VALUE_LEN: u32 = 6;
const COLUMN_TYPE_VALUE_RAW: u32 = 7;
/*

#[derive(PartialEq, Debug, Clone, Copy)]
#[repr(u32)]
enum Action {
    Set,
    Del,
    Inc,
    Link,
    MakeMap,
    MakeList,
    MakeText,
    MakeTable,
}
const ACTIONS: [Action; 8] = [
    Action::Set,
    Action::Del,
    Action::Inc,
    Action::Link,
    Action::MakeMap,
    Action::MakeList,
    Action::MakeText,
    Action::MakeTable,
];

impl Decodable for Action {
    fn decode<R>(bytes: &mut R) -> Option<Self>
    where
        R: Read,
    {
        let num = usize::decode::<R>(bytes)?;
        ACTIONS.get(num).cloned()
    }
}
*/

const COL_OBJ_ACTOR: u32 = COLUMN_TYPE_ACTOR_ID;
const COL_OBJ_CTR: u32 = COLUMN_TYPE_INT_RLE;
const COL_KEY_ACTOR: u32 = 1 << 3 | COLUMN_TYPE_ACTOR_ID;
const COL_KEY_CTR: u32 = 1 << 3 | COLUMN_TYPE_INT_DELTA;
const COL_KEY_STR: u32 = 1 << 3 | COLUMN_TYPE_STRING_RLE;
//const COL_ID_ACTOR : u32 = 2 << 3 | COLUMN_TYPE_ACTOR_ID;
//const COL_ID_CTR : u32 = 2 << 3 | COLUMN_TYPE_INT_DELTA;
const COL_INSERT: u32 = 3 << 3 | COLUMN_TYPE_BOOLEAN;
const COL_ACTION: u32 = 4 << 3 | COLUMN_TYPE_INT_RLE;
const COL_VAL_LEN: u32 = 5 << 3 | COLUMN_TYPE_VALUE_LEN;
const COL_VAL_RAW: u32 = 5 << 3 | COLUMN_TYPE_VALUE_RAW;
const COL_CHILD_ACTOR: u32 = 6 << 3 | COLUMN_TYPE_ACTOR_ID;
const COL_CHILD_CTR: u32 = 6 << 3 | COLUMN_TYPE_INT_DELTA;
const COL_PRED_NUM: u32 = 7 << 3 | COLUMN_TYPE_GROUP_CARD;
const COL_PRED_ACTOR: u32 = 7 << 3 | COLUMN_TYPE_ACTOR_ID;
const COL_PRED_CTR: u32 = 7 << 3 | COLUMN_TYPE_INT_DELTA;
//const COL_SUCC_NUM : u32 = 8 << 3 | COLUMN_TYPE_GROUP_CARD;
//const COL_SUCC_ACTOR : u32 = 8 << 3 | COLUMN_TYPE_ACTOR_ID;
//const COL_SUCC_CTR : u32 = 8 << 3 | COLUMN_TYPE_INT_DELTA;

const MAGIC_BYTES: [u8; 4] = [0x85, 0x6f, 0x4a, 0x83];
const PREAMBLE_BYTES: usize = 8;
const HEADER_BYTES: usize = PREAMBLE_BYTES + 1;

#[cfg(test)]
mod tests {
    use super::*;
    use automerge_protocol::{Key, ObjType, ObjectID, OpID, OpType, Value};

    #[test]
    fn test_empty_change() {
        let change1 = Change {
            start_op: 1,
            seq: 2,
            time: 1234,
            message: None,
            actor_id: ActorID("deadbeefdeadbeef".into()),
            deps: vec![],
            operations: vec![],
        };
        let bin1 = change1.encode();
        let change2 = bin1.decode();
        let bin2 = change2.encode();
        assert_eq!(bin1, bin2);
        assert_eq!(change1, change2);
    }

    #[test]
    fn test_complex_change() -> Result<(), AutomergeError> {
        let actor1 = ActorID("deadbeefdeadbeef".into());
        let actor2 = ActorID("feeddefaff".into());
        let actor3 = ActorID("00101010fafafafa".into());
        let opid1 = OpID(102, actor1.0.clone());
        let opid2 = OpID(391, actor1.0.clone());
        let opid3 = OpID(299, actor2.0.clone());
        let opid4 = OpID(762, actor3.0);
        let opid5 = OpID(100_203, actor2.0);
        let obj1 = ObjectID::ID(opid1.clone());
        let obj2 = ObjectID::Root;
        let obj3 = ObjectID::ID(opid4.clone());
        let key1 = Key::Map("field1".into());
        let key2 = Key::Map("field2".into());
        let key3 = Key::Map("field3".into());
        let head = Key::head();
        let keyseq1 = Key::from(&opid1);
        let keyseq2 = Key::from(&opid2);
        let insert = false;
        let change1 = Change {
            start_op: 123,
            seq: 29291,
            time: 12_341_231,
            message: Some("This is my message".into()),
            actor_id: actor1,
            deps: vec![],
            operations: vec![
                Operation {
                    action: OpType::Set(Value::F64(10.0)),
                    key: key1.clone(),
                    obj: obj1.clone(),
                    insert,
                    pred: vec![opid1.clone(), opid2.clone()],
                },
                Operation {
                    action: OpType::Set(Value::Counter(-11)),
                    key: key2.clone(),
                    obj: obj1.clone(),
                    insert,
                    pred: vec![opid1.clone(), opid2.clone()],
                },
                Operation {
                    action: OpType::Set(Value::Timestamp(20)),
                    key: key3,
                    obj: obj1.clone(),
                    insert,
                    pred: vec![opid1.clone(), opid2],
                },
                Operation {
                    action: OpType::Set(Value::Str("some value".into())),
                    key: key2.clone(),
                    obj: obj2.clone(),
                    insert,
                    pred: vec![opid3.clone(), opid4.clone()],
                },
                Operation {
                    action: OpType::Make(ObjType::List),
                    key: key2.clone(),
                    obj: obj2.clone(),
                    insert,
                    pred: vec![opid3.clone(), opid4.clone()],
                },
                Operation {
                    action: OpType::Set(Value::Str("val1".into())),
                    key: head.clone(),
                    obj: obj3.clone(),
                    insert: true,
                    pred: vec![opid3.clone(), opid4.clone()],
                },
                Operation {
                    action: OpType::Set(Value::Str("val2".into())),
                    key: head,
                    obj: obj3.clone(),
                    insert: true,
                    pred: vec![opid4.clone(), opid5.clone()],
                },
                Operation {
                    action: OpType::Inc(10),
                    key: key2,
                    obj: obj2,
                    insert,
                    pred: vec![opid1.clone(), opid5.clone()],
                },
                Operation {
                    action: OpType::Link(obj3.clone()),
                    obj: obj1,
                    key: key1,
                    insert,
                    pred: vec![opid1, opid3],
                },
                Operation {
                    action: OpType::Del,
                    obj: obj3.clone(),
                    key: keyseq1,
                    insert: true,
                    pred: vec![opid4.clone(), opid5.clone()],
                },
                Operation {
                    action: OpType::Del,
                    obj: obj3,
                    key: keyseq2,
                    insert: true,
                    pred: vec![opid4, opid5],
                },
            ],
        };
        let bin1 = change1.encode();
        let change2 = bin1.decode();
        let bin2 = change2.encode();
        assert_eq!(bin1, bin2);
        assert_eq!(change1, change2);
        Ok(())
    }
}
