use crate::columnar;
use crate::columnar::{
    ColumnEncoder, KeyIterator, ObjIterator, OperationIterator, PredIterator, ValueIterator,
};
use crate::encoding::{Decodable, Encodable};
use crate::error::AutomergeError;
use crate::op::Operation;
use automerge_protocol as amp;
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
pub struct UnencodedChange {
    #[serde(rename = "ops")]
    pub operations: Vec<Operation>,
    #[serde(rename = "actor")]
    pub actor_id: amp::ActorID,
    //pub hash: amp::ChangeHash,
    pub seq: u64,
    #[serde(rename = "startOp")]
    pub start_op: u64,
    pub time: i64,
    pub message: Option<String>,
    pub deps: Vec<amp::ChangeHash>,
}

impl UnencodedChange {
    pub fn max_op(&self) -> u64 {
        self.start_op + (self.operations.len() as u64) - 1
    }

    pub fn encode(&self) -> Change {
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

        Change::from_bytes(buf).unwrap()
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
        let mut deps = self.deps.clone();
        deps.sort_unstable();
        deps.len().encode(&mut buf)?;
        for hash in deps.iter() {
            buf.write_all(&hash.0)?;
        }

        self.actor_id.to_bytes().encode(&mut buf)?;
        self.seq.encode(&mut buf)?;
        self.start_op.encode(&mut buf)?;
        self.time.encode(&mut buf)?;
        self.message.encode(&mut buf)?;

        let mut actors = Vec::new();
        actors.push(self.actor_id.clone());
        let ops_buf = ColumnEncoder::encode_ops(&self.operations, &mut actors);
        actors[1..].encode(&mut buf)?;

        buf.write_all(&ops_buf)?;

        Ok(buf)
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
        let hash = hasher.result()[..].try_into()?;

        let mut cursor = body.clone();
        let mut deps = Vec::new();
        let num_deps = read_slice(&bytes, &mut cursor)?;
        for _ in 0..num_deps {
            let hash = cursor.start..(cursor.start + HASH_BYTES);
            cursor = hash.end..cursor.end;
            //let hash = slice_n_bytes(bytes, HASH_BYTES)?;
            deps.push(bytes[hash].try_into()?);
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

    pub fn decode(&self) -> UnencodedChange {
        UnencodedChange {
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
            chld: ObjIterator {
                actors: &self.actors,
                actor: self.col_iter(columnar::COL_CHILD_ACTOR),
                ctr: self.col_iter(columnar::COL_CHILD_CTR),
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
}

impl From<&UnencodedChange> for Change {
    fn from(change: &UnencodedChange) -> Change {
        change.encode()
    }
}

impl From<UnencodedChange> for Change {
    fn from(change: UnencodedChange) -> Change {
        change.encode()
    }
}

impl From<&Change> for UnencodedChange {
    fn from(change: &Change) -> UnencodedChange {
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

const MAGIC_BYTES: [u8; 4] = [0x85, 0x6f, 0x4a, 0x83];
const PREAMBLE_BYTES: usize = 8;
const HEADER_BYTES: usize = PREAMBLE_BYTES + 1;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::op_type::OpType;
    use std::str::FromStr;

    #[test]
    fn test_empty_change() {
        let change1 = UnencodedChange {
            start_op: 1,
            seq: 2,
            time: 1234,
            message: None,
            actor_id: amp::ActorID::from_str("deadbeefdeadbeef").unwrap(),
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
        let change1 = UnencodedChange {
            start_op: 123,
            seq: 29291,
            time: 12_341_231,
            message: Some("This is my message".into()),
            actor_id: actor1,
            deps: vec![],
            operations: vec![
                Operation {
                    action: OpType::Set(amp::ScalarValue::F64(10.0)),
                    key: key1.clone(),
                    obj: obj1.clone(),
                    insert,
                    pred: vec![opid1.clone(), opid2.clone()],
                },
                Operation {
                    action: OpType::Set(amp::ScalarValue::Counter(-11)),
                    key: key2.clone(),
                    obj: obj1.clone(),
                    insert,
                    pred: vec![opid1.clone(), opid2.clone()],
                },
                Operation {
                    action: OpType::Set(amp::ScalarValue::Timestamp(20)),
                    key: key3,
                    obj: obj1.clone(),
                    insert,
                    pred: vec![opid1.clone(), opid2],
                },
                Operation {
                    action: OpType::Set(amp::ScalarValue::Str("some value".into())),
                    key: key2.clone(),
                    obj: obj2.clone(),
                    insert,
                    pred: vec![opid3.clone(), opid4.clone()],
                },
                Operation {
                    action: OpType::Make(amp::ObjType::list()),
                    key: key2.clone(),
                    obj: obj2.clone(),
                    insert,
                    pred: vec![opid3.clone(), opid4.clone()],
                },
                Operation {
                    action: OpType::Set(amp::ScalarValue::Str("val1".into())),
                    key: head.clone(),
                    obj: obj3.clone(),
                    insert: true,
                    pred: vec![opid3.clone(), opid4.clone()],
                },
                Operation {
                    action: OpType::Set(amp::ScalarValue::Str("val2".into())),
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
