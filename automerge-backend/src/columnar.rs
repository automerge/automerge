use crate::encoding::{BooleanDecoder, Decodable, Decoder, DeltaDecoder, RLEDecoder};
use crate::encoding::{BooleanEncoder, ColData, DeltaEncoder, Encodable, RLEEncoder};
use crate::error::AutomergeError;
use automerge_protocol::{
    ActorID, Change, ChangeHash, ElementID, Key, ObjType, ObjectID, OpID, OpType, Operation, Value,
};
use core::fmt::Debug;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::convert::TryInto;
use std::io;
use std::io::{Read, Write};
use std::rc::Rc;
use std::str;

const HASH_BYTES: usize = 32;

pub(crate) fn bin_to_changes(bindata: &[u8]) -> Result<Vec<Change>, AutomergeError> {
    BinaryContainer::from(&bindata)?
        .iter()
        .map(|bin| bin.to_change())
        .collect()
}

// FIXME - really I need a bin change constructor
pub(crate) fn change_to_change(change: Change) -> Result<Rc<Change>, AutomergeError> {
    let bin_change = changes_to_bin(&[&change])?;
    let mut changes = bin_to_changes(&bin_change[0])?;
    Ok(Rc::new(changes.remove(0)))
}

pub(crate) fn changes_to_bin(changes: &[&Change]) -> Result<Vec<Vec<u8>>, AutomergeError> {
    let mut bins = Vec::new();
    for c in changes {
        let bin = change_to_bin(c)?;
        bins.push(bin)
    }
    Ok(bins)
}

pub(crate) fn changes_to_one_bin(changes: &[&Change]) -> Result<Vec<u8>, AutomergeError> {
    let mut data = Vec::new();
    for c in changes {
        let bin = change_to_bin(c)?;
        data.extend(bin)
    }
    Ok(data)
}

fn change_to_bin(change: &Change) -> Result<Vec<u8>, AutomergeError> {
    let mut buf = Vec::new();
    let mut hasher = Sha256::new();

    let chunk = encode_chunk(&change)?;

    hasher.input(&chunk);

    buf.extend(&MAGIC_BYTES);
    buf.extend(&hasher.result()[0..4]);
    buf.extend(&chunk);
    Ok(buf)
}

fn encode_chunk(change: &Change) -> Result<Vec<u8>, AutomergeError> {
    let mut chunk = vec![CHUNK_TYPE]; // chunk type is always 1
    let data = encode(change)?;
    leb128::write::unsigned(&mut chunk, data.len() as u64)?;
    chunk.extend(&data);
    Ok(chunk)
}

fn encode<V: Encodable>(val: &V) -> Result<Vec<u8>, AutomergeError> {
    let mut actor_ids = Vec::new();
    Ok(val.encode_with_actors_to_vec(&mut actor_ids)?)
}

impl Encodable for Change {
    fn encode_with_actors<R: Write>(
        &self,
        buf: &mut R,
        actors: &mut Vec<ActorID>,
    ) -> io::Result<usize> {
        let mut len = 0;

        actors.push(self.actor_id.clone());

        len += self.actor_id.to_bytes().encode(buf)?;
        len += self.seq.encode(buf)?;
        len += self.start_op.encode(buf)?;
        len += self.time.encode(buf)?;
        len += self.message.encode(buf)?;

        //        let deps_buf = self.deps.encode_with_actors_to_vec(actors)?;

        let ops_buf = ColumnEncoder::encode_ops(&self.operations, actors);

        len += actors[1..].encode(buf)?;

        let mut deps = self.deps.clone();
        deps.sort_unstable();
        len += deps.len().encode(buf)?;
        for hash in deps.iter() {
            len += buf.write(&hash.0)?;
        }

        buf.write_all(&ops_buf)?;
        len += ops_buf.len();

        Ok(len)
    }
}

/*
impl Encodable for Clock {
    fn encode_with_actors<R: Write>(
        &self,
        buf: &mut R,
        actors: &mut Vec<ActorID>,
    ) -> io::Result<usize> {
        let mut len = 0;
        self.0.len().encode(buf)?;
        let mut keys: Vec<&ActorID> = self.0.keys().collect();
        keys.sort_unstable();
        for actor in keys.iter() {
            let val = self.get(actor);
            len += actor.encode_with_actors(buf, actors)?;
            len += val.encode(buf)?;
        }
        Ok(len)
    }
}
*/

impl Encodable for Action {
    fn encode<R: Write>(&self, buf: &mut R) -> io::Result<usize> {
        (*self as u32).encode(buf)
    }
}

impl Encodable for [ActorID] {
    fn encode<R: Write>(&self, buf: &mut R) -> io::Result<usize> {
        let mut len = self.len().encode(buf)?;
        for i in self {
            len += i.to_bytes().encode(buf)?;
        }
        Ok(len)
    }
}

fn map_string(actor: &str, actors: &mut Vec<ActorID>) -> usize {
    let a = ActorID(actor.to_string());
    map_actor(&a, actors)
}

fn map_actor(actor: &ActorID, actors: &mut Vec<ActorID>) -> usize {
    if let Some(pos) = actors.iter().position(|a| a == actor) {
        pos
    } else {
        actors.push(actor.clone());
        actors.len() - 1
    }
}

impl Encodable for ActorID {
    fn encode_with_actors<R: Write>(
        &self,
        buf: &mut R,
        actors: &mut Vec<ActorID>,
    ) -> io::Result<usize> {
        map_actor(self, actors).encode(buf)
    }
}

impl Encodable for Vec<u8> {
    fn encode<R: Write>(&self, buf: &mut R) -> io::Result<usize> {
        self.as_slice().encode(buf)
    }
}

impl Encodable for &[u8] {
    fn encode<R: Write>(&self, buf: &mut R) -> io::Result<usize> {
        let head = self.len().encode(buf)?;
        buf.write_all(self)?;
        Ok(head + self.len())
    }
}

fn read_slice<T: Decodable + Debug>(buf: &mut &[u8]) -> Result<T, AutomergeError> {
    T::decode::<&[u8]>(buf).ok_or(AutomergeError::ChangeBadFormat)
}

fn slice_bytes<'a>(bytes: &mut &'a [u8]) -> Result<&'a [u8], AutomergeError> {
    let mut buf = &bytes[..];
    let len = leb128::read::unsigned(&mut buf)? as usize;
    let result = &buf[0..len];
    *bytes = &buf[len..];
    Ok(result)
}

fn slice_n_bytes<'a>(bytes: &mut &'a [u8], n: usize) -> Result<&'a [u8], AutomergeError> {
    let result = &bytes[0..n];
    *bytes = &bytes[n..];
    Ok(result)
}

fn slice_bytes_len(bytes: &[u8]) -> Result<(&[u8], usize), AutomergeError> {
    let mut view = &bytes[..];
    let len = leb128::read::unsigned(&mut view)? as usize;
    let len_bytes = bytes.len() - view.len();
    Ok((&view[0..len], len + len_bytes))
}

impl From<leb128::read::Error> for AutomergeError {
    fn from(_err: leb128::read::Error) -> Self {
        AutomergeError::ChangeBadFormat
    }
}

impl From<std::io::Error> for AutomergeError {
    fn from(_err: std::io::Error) -> Self {
        AutomergeError::EncodeFailed
    }
}

#[derive(Debug, Clone)]
struct BinaryContainer<'a> {
    magic: &'a [u8],
    checksum: &'a [u8],
    hash: ChangeHash,
    body: &'a [u8],
    chunktype: u8,
    chunk: BinaryChange<'a>,
    len: usize,
}

#[derive(Debug, Clone)]
struct BinaryChange<'a> {
    all: &'a [u8],
    seq: u64,
    start_op: u64,
    time: i64,
    message: &'a [u8],
    actors: Vec<&'a [u8]>,
    deps: Vec<&'a [u8]>,
    ops: HashMap<u32, &'a [u8]>,
}

impl<'a> BinaryChange<'a> {
    fn from(bytes: &'a [u8]) -> Result<BinaryChange<'a>, AutomergeError> {
        let bytes = &mut &bytes[..];
        let all = &bytes[0..];
        let actor = slice_bytes(bytes)?;
        let seq = read_slice(bytes)?;
        let start_op = read_slice(bytes)?;
        let time = read_slice(bytes)?;
        let message = slice_bytes(bytes)?;
        let num_actors = read_slice(bytes)?;
        let mut actors = vec![&actor[..]];
        for _ in 0..num_actors {
            let actor = slice_bytes(bytes)?;
            actors.push(actor);
        }
        let mut deps = Vec::new();
        let num_deps = read_slice(bytes)?;
        for _ in 0..num_deps {
            let hash = slice_n_bytes(bytes, HASH_BYTES)?;
            deps.push(hash);
        }
        let mut ops = HashMap::new();
        let mut last_id = 0;
        while !bytes.is_empty() {
            let id = read_slice(bytes)?;
            if id < last_id {
                return Err(AutomergeError::ChangeBadFormat);
            }
            last_id = id;
            let column = slice_bytes(bytes)?;
            ops.insert(id, column);
        }
        Ok(BinaryChange {
            all,
            seq,
            start_op,
            time,
            actors,
            message,
            deps,
            ops,
        })
    }

    fn gen_deps(&self) -> Vec<ChangeHash> {
        // TODO Add error propagation
        self.deps.iter().map(|&v| v.try_into().unwrap()).collect()
    }

    fn message(&self) -> Option<String> {
        if self.message.is_empty() {
            None
        } else {
            str::from_utf8(&self.message).map(|s| s.to_string()).ok()
        }
    }

    fn to_change(&self, hash: ChangeHash) -> Result<Change, AutomergeError> {
        let change = Change {
            start_op: self.start_op,
            seq: self.seq,
            hash,
            time: self.time,
            message: self.message(),
            actor_id: ActorID::from_bytes(self.actors[0]),
            deps: self.gen_deps(),
            operations: self.iter_ops().collect(),
        };
        Ok(change)
    }

    fn col_iter<T>(&self, col_id: u32) -> T
    where
        T: From<&'a [u8]>,
    {
        let empty = &self.all[0..0];
        let buf = self.ops.get(&col_id).unwrap_or(&empty);
        T::from(&buf)
    }

    fn iter_ops(&self) -> OperationIterator {
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

struct OperationIterator<'a> {
    action: RLEDecoder<'a, Action>,
    objs: ObjIterator<'a>,
    chld: ObjIterator<'a>,
    keys: KeyIterator<'a>,
    insert: BooleanDecoder<'a>,
    value: ValueIterator<'a>,
    pred: PredIterator<'a>,
}

struct ObjIterator<'a> {
    actors: &'a Vec<&'a [u8]>,
    actor: RLEDecoder<'a, usize>,
    ctr: RLEDecoder<'a, u64>,
}

struct PredIterator<'a> {
    actors: &'a Vec<&'a [u8]>,
    pred_num: RLEDecoder<'a, usize>,
    pred_actor: RLEDecoder<'a, usize>,
    pred_ctr: DeltaDecoder<'a>,
}

struct KeyIterator<'a> {
    actors: &'a Vec<&'a [u8]>,
    actor: RLEDecoder<'a, usize>,
    ctr: DeltaDecoder<'a>,
    str: RLEDecoder<'a, String>,
}

struct ValueIterator<'a> {
    val_len: RLEDecoder<'a, usize>,
    val_raw: Decoder<'a>,
}

impl<'a> Iterator for PredIterator<'a> {
    type Item = Vec<OpID>;
    fn next(&mut self) -> Option<Vec<OpID>> {
        let num = self.pred_num.next()??;
        let mut p = Vec::with_capacity(num);
        for _ in 0..num {
            let actor = self.pred_actor.next()??;
            let ctr = self.pred_ctr.next()??;
            let actor_bytes = self.actors.get(actor)?;
            let actor_id = ActorID::from_bytes(actor_bytes);
            let op_id = OpID::new(ctr, &actor_id);
            p.push(op_id)
        }
        Some(p)
    }
}

impl<'a> Iterator for ValueIterator<'a> {
    type Item = Value;
    fn next(&mut self) -> Option<Value> {
        let val_type = self.val_len.next()??;
        match val_type {
            VALUE_TYPE_NULL => Some(Value::Null),
            VALUE_TYPE_FALSE => Some(Value::Boolean(false)),
            VALUE_TYPE_TRUE => Some(Value::Boolean(true)),
            v if v % 16 == VALUE_TYPE_COUNTER => {
                let len = v >> 4;
                let val = self.val_raw.read().ok()?;
                if len != self.val_raw.last_read {
                    return None;
                }
                Some(Value::Counter(val))
            }
            v if v % 16 == VALUE_TYPE_TIMESTAMP => {
                let len = v >> 4;
                let val = self.val_raw.read().ok()?;
                if len != self.val_raw.last_read {
                    return None;
                }
                Some(Value::Timestamp(val))
            }
            v if v % 16 == VALUE_TYPE_LEB128_UINT => {
                let len = v >> 4;
                let val = self.val_raw.read().ok()?;
                if len != self.val_raw.last_read {
                    return None;
                }
                Some(Value::Uint(val))
            }
            v if v % 16 == VALUE_TYPE_LEB128_INT => {
                let len = v >> 4;
                let val = self.val_raw.read().ok()?;
                if len != self.val_raw.last_read {
                    return None;
                }
                Some(Value::Int(val))
            }
            v if v % 16 == VALUE_TYPE_UTF8 => {
                let len = v >> 4;
                let data = self.val_raw.read_bytes(len).ok()?;
                let s = str::from_utf8(&data).ok()?;
                Some(Value::Str(s.to_string()))
            }
            v if v % 16 == VALUE_TYPE_BYTES => {
                let len = v >> 4;
                let _data = self.val_raw.read_bytes(len).ok()?;
                unimplemented!()
                //Some((Value::Bytes(data))
            }
            v if v % 16 >= VALUE_TYPE_MIN_UNKNOWN && v % 16 <= VALUE_TYPE_MAX_UNKNOWN => {
                let len = v >> 4;
                let _data = self.val_raw.read_bytes(len).ok()?;
                unimplemented!()
                //Some((Value::Bytes(data))
            }
            v if v % 16 == VALUE_TYPE_IEEE754 => {
                let len = v >> 4;
                if len == 4 {
                    // confirm only 4 bytes read
                    let num: f32 = self.val_raw.read().ok()?;
                    Some(Value::F32(num))
                } else if len == 8 {
                    // confirm only 8 bytes read
                    let num = self.val_raw.read().ok()?;
                    Some(Value::F64(num))
                } else {
                    // bad size of float
                    None
                }
            }
            _ => {
                // unknown command
                None
            }
        }
    }
}

impl<'a> Iterator for KeyIterator<'a> {
    type Item = Key;
    fn next(&mut self) -> Option<Key> {
        match (self.actor.next()?, self.ctr.next()?, self.str.next()?) {
            (None, None, Some(string)) => Some(Key::Map(string)),
            (Some(0), Some(0), None) => Some(Key::head()),
            (Some(actor), Some(ctr), None) => {
                let actor_bytes = self.actors.get(actor)?;
                let actor_id = ActorID::from_bytes(actor_bytes);
                Some(OpID(ctr, actor_id.0).into())
            }
            _ => None,
        }
    }
}

impl<'a> Iterator for ObjIterator<'a> {
    type Item = ObjectID;
    fn next(&mut self) -> Option<ObjectID> {
        if let (Some(actor), Some(ctr)) = (self.actor.next()?, self.ctr.next()?) {
            let actor_id = ActorID::from_bytes(self.actors.get(actor)?);
            Some(ObjectID::ID(OpID(ctr, actor_id.0)))
        } else {
            Some(ObjectID::Root)
        }
    }
}

impl<'a> Iterator for OperationIterator<'a> {
    type Item = Operation;
    fn next(&mut self) -> Option<Operation> {
        let action = self.action.next()??;
        let insert = self.insert.next()?;
        let obj = self.objs.next()?;
        let key = self.keys.next()?;
        let pred = self.pred.next()?;
        let value = self.value.next()?;
        let child = self.chld.next()?;
        let action = match action {
            Action::Set => OpType::Set(value),
            Action::MakeList => OpType::Make(ObjType::List),
            Action::MakeText => OpType::Make(ObjType::Text),
            Action::MakeMap => OpType::Make(ObjType::Map),
            Action::MakeTable => OpType::Make(ObjType::Table),
            Action::Del => OpType::Del,
            Action::Inc => OpType::Inc(value.to_i64()?),
            Action::Link => OpType::Link(child),
        };
        Some(Operation {
            action,
            obj,
            key,
            pred,
            insert,
        })
    }
}

impl<'a> BinaryContainer<'a> {
    fn from(mut bytes: &'a [u8]) -> Result<Vec<BinaryContainer<'a>>, AutomergeError> {
        let mut changes = Vec::new();
        while !bytes.is_empty() {
            let change = Self::parse_single(bytes)?;
            bytes = &bytes[change.len..];
            changes.push(change);
        }
        Ok(changes)
    }

    fn parse_single(bytes: &'a [u8]) -> Result<BinaryContainer<'a>, AutomergeError> {
        if bytes.len() < 8 {
            return Err(AutomergeError::ChangeBadFormat);
        }
        let (header, rest) = &bytes.split_at(8);
        let (magic, checksum) = &header.split_at(4);
        if magic != &MAGIC_BYTES {
            return Err(AutomergeError::ChangeBadFormat);
        }
        let (chunk_data, chunk_len) = slice_bytes_len(&rest[1..])?;
        let body = &rest[0..(chunk_len + 1)]; // +1 for chunktype
        let chunktype = body[0];
        let len = body.len() + header.len();

        let mut hasher = Sha256::new();
        hasher.input(&body);
        let hash = hasher.result()[..]
            .try_into()
            .map_err(|_| AutomergeError::DecodeFailed)?;

        Ok(BinaryContainer {
            magic,
            checksum,
            hash,
            chunktype,
            body,
            chunk: BinaryChange::from(chunk_data)?,
            len,
        })
    }

    fn is_valid(&self) -> bool {
        &self.hash.0[0..4] == self.checksum
    }

    fn to_change(&self) -> Result<Change, AutomergeError> {
        if !self.is_valid() {
            return Err(AutomergeError::InvalidChange);
        }
        self.chunk.to_change(self.hash)
    }
}

struct ValEncoder {
    len: RLEEncoder<usize>,
    raw: Vec<u8>,
}

impl ValEncoder {
    fn new() -> ValEncoder {
        ValEncoder {
            len: RLEEncoder::new(),
            raw: Vec::new(),
        }
    }

    fn append_value(&mut self, val: &Value) {
        match val {
            Value::Null => self.len.append_value(VALUE_TYPE_NULL),
            Value::Boolean(true) => self.len.append_value(VALUE_TYPE_TRUE),
            Value::Boolean(false) => self.len.append_value(VALUE_TYPE_FALSE),
            Value::Str(s) => {
                let bytes = s.as_bytes();
                let len = bytes.len();
                self.raw.extend(bytes);
                self.len.append_value(len << 4 | VALUE_TYPE_UTF8)
            }
            /*
            Value::Bytes(bytes) => {
                let len = bytes.len();
                self.raw.extend(bytes);
                self.len.append_value(len << 4 | VALUE_TYPE_BYTES)
            },
            */
            Value::Counter(count) => {
                let len = count.encode(&mut self.raw).unwrap();
                self.len.append_value(len << 4 | VALUE_TYPE_COUNTER)
            }
            Value::Timestamp(time) => {
                let len = time.encode(&mut self.raw).unwrap();
                self.len.append_value(len << 4 | VALUE_TYPE_TIMESTAMP)
            }
            Value::Int(n) => {
                let len = n.encode(&mut self.raw).unwrap();
                self.len.append_value(len << 4 | VALUE_TYPE_LEB128_INT)
            }
            Value::Uint(n) => {
                let len = n.encode(&mut self.raw).unwrap();
                self.len.append_value(len << 4 | VALUE_TYPE_LEB128_UINT)
            }
            Value::F32(n) => {
                let len = (*n).encode(&mut self.raw).unwrap();
                self.len.append_value(len << 4 | VALUE_TYPE_IEEE754)
            }
            Value::F64(n) => {
                let len = (*n).encode(&mut self.raw).unwrap();
                self.len.append_value(len << 4 | VALUE_TYPE_IEEE754)
            } /*
              Value::Unknown(num,bytes) => {
                  let len = bytes.len();
                  self.raw.extend(bytes);
                  self.len.append_value(len << 4 | num)
              },
              */
        }
    }

    fn append_null(&mut self) {
        self.len.append_value(VALUE_TYPE_NULL)
    }

    fn finish(self) -> Vec<ColData> {
        vec![
            self.len.finish(COL_VAL_LEN),
            ColData {
                col: COL_VAL_RAW,
                data: self.raw,
            },
        ]
    }
}

struct KeyEncoder {
    actor: RLEEncoder<usize>,
    ctr: DeltaEncoder,
    str: RLEEncoder<String>,
}

impl KeyEncoder {
    fn new() -> KeyEncoder {
        KeyEncoder {
            actor: RLEEncoder::new(),
            ctr: DeltaEncoder::new(),
            str: RLEEncoder::new(),
        }
    }

    fn append(&mut self, key: &Key, actors: &mut Vec<ActorID>) {
        match &key {
            Key::Map(s) => {
                self.actor.append_null();
                self.ctr.append_null();
                self.str.append_value(s.clone());
            }
            Key::Seq(ElementID::Head) => {
                self.actor.append_value(0);
                self.ctr.append_value(0);
                self.str.append_null();
            }
            Key::Seq(ElementID::ID(OpID(ctr, actor))) => {
                self.actor.append_value(map_string(&actor, actors));
                self.ctr.append_value(*ctr);
                self.str.append_null();
            }
        }
    }

    fn finish(self) -> Vec<ColData> {
        vec![
            self.actor.finish(COL_KEY_ACTOR),
            self.ctr.finish(COL_KEY_CTR),
            self.str.finish(COL_KEY_STR),
        ]
    }
}

struct PredEncoder {
    num: RLEEncoder<usize>,
    actor: RLEEncoder<usize>,
    ctr: DeltaEncoder,
}

impl PredEncoder {
    fn new() -> PredEncoder {
        PredEncoder {
            num: RLEEncoder::new(),
            actor: RLEEncoder::new(),
            ctr: DeltaEncoder::new(),
        }
    }

    fn append(&mut self, pred: &[OpID], actors: &mut Vec<ActorID>) {
        self.num.append_value(pred.len());
        for p in pred.iter() {
            self.ctr.append_value(p.0);
            self.actor.append_value(map_string(&p.1, actors));
        }
    }

    fn finish(self) -> Vec<ColData> {
        vec![
            self.num.finish(COL_PRED_NUM),
            self.actor.finish(COL_PRED_ACTOR),
            self.ctr.finish(COL_PRED_CTR),
        ]
    }
}

struct ObjEncoder {
    actor: RLEEncoder<usize>,
    ctr: RLEEncoder<u64>,
}

impl ObjEncoder {
    fn new() -> ObjEncoder {
        ObjEncoder {
            actor: RLEEncoder::new(),
            ctr: RLEEncoder::new(),
        }
    }

    fn append(&mut self, obj: &ObjectID, actors: &mut Vec<ActorID>) {
        match obj {
            ObjectID::Root => {
                self.actor.append_null();
                self.ctr.append_null();
            }
            ObjectID::ID(OpID(ctr, actor)) => {
                self.actor.append_value(map_string(&actor, actors));
                self.ctr.append_value(*ctr);
            }
        }
    }

    fn finish(self) -> Vec<ColData> {
        vec![
            self.actor.finish(COL_OBJ_ACTOR),
            self.ctr.finish(COL_OBJ_CTR),
        ]
    }
}

struct ChildEncoder {
    actor: RLEEncoder<usize>,
    ctr: DeltaEncoder,
}

impl ChildEncoder {
    fn new() -> ChildEncoder {
        ChildEncoder {
            actor: RLEEncoder::new(),
            ctr: DeltaEncoder::new(),
        }
    }

    fn append_null(&mut self) {
        self.actor.append_null();
        self.ctr.append_null();
    }

    fn append(&mut self, obj: &ObjectID, actors: &mut Vec<ActorID>) {
        match obj {
            ObjectID::Root => self.append_null(),
            ObjectID::ID(OpID(ctr, actor)) => {
                self.actor.append_value(map_string(&actor, actors));
                self.ctr.append_value(*ctr);
            }
        }
    }

    fn finish(self) -> Vec<ColData> {
        vec![
            self.actor.finish(COL_CHILD_ACTOR),
            self.ctr.finish(COL_CHILD_CTR),
        ]
    }
}

struct ColumnEncoder {
    obj: ObjEncoder,
    key: KeyEncoder,
    insert: BooleanEncoder,
    action: RLEEncoder<Action>,
    val: ValEncoder,
    chld: ChildEncoder,
    pred: PredEncoder,
}

impl ColumnEncoder {
    fn encode_ops(ops: &[Operation], actors: &mut Vec<ActorID>) -> Vec<u8> {
        let mut e = Self::new();
        e.encode(ops, actors);
        e.finish()
    }

    fn new() -> ColumnEncoder {
        ColumnEncoder {
            obj: ObjEncoder::new(),
            key: KeyEncoder::new(),
            insert: BooleanEncoder::new(),
            action: RLEEncoder::new(),
            val: ValEncoder::new(),
            chld: ChildEncoder::new(),
            pred: PredEncoder::new(),
        }
    }

    fn encode(&mut self, ops: &[Operation], actors: &mut Vec<ActorID>) {
        for op in ops {
            self.append(op, actors)
        }
    }

    fn append(&mut self, op: &Operation, actors: &mut Vec<ActorID>) {
        self.obj.append(&op.obj, actors);
        self.key.append(&op.key, actors);
        self.insert.append(op.insert);
        self.pred.append(&op.pred, actors);
        let action = match &op.action {
            OpType::Set(value) => {
                self.val.append_value(value);
                self.chld.append_null();
                Action::Set
            }
            OpType::Inc(val) => {
                self.val.append_value(&Value::Int(*val));
                self.chld.append_null();
                Action::Inc
            }
            OpType::Del => {
                self.val.append_null();
                self.chld.append_null();
                Action::Del
            }
            OpType::Link(child) => {
                self.val.append_null();
                self.chld.append(child, actors);
                Action::Link
            }
            OpType::Make(kind) => {
                self.val.append_null();
                self.chld.append_null();
                match kind {
                    ObjType::List => Action::MakeList,
                    ObjType::Map => Action::MakeMap,
                    ObjType::Table => Action::MakeTable,
                    ObjType::Text => Action::MakeText,
                }
            }
        };
        self.action.append_value(action);
    }

    fn finish(self) -> Vec<u8> {
        let mut coldata = Vec::new();
        coldata.push(self.insert.finish(COL_INSERT));
        coldata.push(self.action.finish(COL_ACTION));
        coldata.extend(self.obj.finish());
        coldata.extend(self.key.finish());
        coldata.extend(self.val.finish());
        coldata.extend(self.chld.finish());
        coldata.extend(self.pred.finish());
        coldata.sort_by(|a, b| a.col.cmp(&b.col));

        let mut result = Vec::new();
        for d in coldata.iter() {
            d.encode(&mut result).ok();
        }
        result
    }
}

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

const COLUMN_TYPE_GROUP_CARD: u32 = 0;
const COLUMN_TYPE_ACTOR_ID: u32 = 1;
const COLUMN_TYPE_INT_RLE: u32 = 2;
const COLUMN_TYPE_INT_DELTA: u32 = 3;
const COLUMN_TYPE_BOOLEAN: u32 = 4;
const COLUMN_TYPE_STRING_RLE: u32 = 5;
const COLUMN_TYPE_VALUE_LEN: u32 = 6;
const COLUMN_TYPE_VALUE_RAW: u32 = 7;

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

#[cfg(test)]
mod tests {
    use super::*;
    //use std::str::FromStr;

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
            hash: ChangeHash::zero(),
        };
        let bin1 = change_to_bin(&change1).unwrap();
        let changes2 = bin_to_changes(&bin1).unwrap();
        let bin2 = change_to_bin(&changes2[0]).unwrap();
        assert_eq!(bin1, bin2);
        assert_eq!(vec![change1], changes2);
    }

    #[test]
    fn test_complex_change() -> Result<(), AutomergeError> {
        let actor1 = ActorID("deadbeefdeadbeef".into());
        let actor2 = ActorID("feeddefaff".into());
        let actor3 = ActorID("00101010fafafafa".into());
        let opid1 = OpID(102, actor1.0.clone());
        let opid2 = OpID(391, actor1.0.clone());
        let opid3 = OpID(299, actor2.0.clone());
        let opid4 = OpID(762, actor3.0.clone());
        let opid5 = OpID(100203, actor2.0.clone());
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
            hash: ChangeHash::zero(),
            start_op: 123,
            seq: 29291,
            time: 12341231,
            message: Some("This is my message".into()),
            actor_id: actor1.clone(),
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
                    key: key3.clone(),
                    obj: obj1.clone(),
                    insert,
                    pred: vec![opid1.clone(), opid2.clone()],
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
                    key: head.clone(),
                    obj: obj3.clone(),
                    insert: true,
                    pred: vec![opid4.clone(), opid5.clone()],
                },
                Operation {
                    action: OpType::Inc(10),
                    key: key2.clone(),
                    obj: obj2.clone(),
                    insert,
                    pred: vec![opid1.clone(), opid5.clone()],
                },
                Operation {
                    action: OpType::Link(obj3.clone()),
                    obj: obj1.clone(),
                    key: key1.clone(),
                    insert,
                    pred: vec![opid1.clone(), opid3.clone()],
                },
                Operation {
                    action: OpType::Del,
                    obj: obj3.clone(),
                    key: keyseq1.clone(),
                    insert: true,
                    pred: vec![opid4.clone(), opid5.clone()],
                },
                Operation {
                    action: OpType::Del,
                    obj: obj3.clone(),
                    key: keyseq2.clone(),
                    insert: true,
                    pred: vec![opid4.clone(), opid5.clone()],
                },
            ],
        };
        let bin1 = change_to_bin(&change1)?;
        let changes2 = bin_to_changes(&bin1)?;
        let bin2 = change_to_bin(&changes2[0])?;
        assert_eq!(bin1, bin2);
        assert_eq!(vec![change1], changes2);
        Ok(())
    }
}
