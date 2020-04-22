use crate::encoding::{BooleanDecoder, Decodable, Decoder, DeltaDecoder, RLEDecoder};
use crate::encoding::{BooleanEncoder, DeltaEncoder, Encodable, RLEEncoder, ColData };
use crate::error::AutomergeError;
use crate::protocol::{
    ActorID, Change, Clock, ElementID, Key, ObjType, ObjectID, OpID, OpType, Operation,
    PrimitiveValue,
};
use sha2::{Digest, Sha256};
use core::fmt::Debug;
use std::collections::HashMap;
use std::io;
use std::io::{Read, Write};
use std::str;

pub(crate) fn bin_to_changes(bindata: &[u8]) -> Result<Vec<Change>,AutomergeError> {
    BinaryContainer::from(&bindata)?.iter().map(|bin| bin.to_change()).collect()
}

pub (crate) fn changes_to_bin(changes: &[&Change]) -> Result<Vec<Vec<u8>>, AutomergeError> {
    let mut bins = Vec::new();
    for c in changes {
        let bin = change_to_bin(c)?;
        bins.push(bin)
    }
    Ok(bins)
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
    leb128::write::unsigned(&mut chunk, data.len() as u64)
        .map_err(|_| AutomergeError::EncodeFailed)?;
    chunk.extend(&data);
    Ok(chunk)
}

fn encode<V: Encodable>(val: &V) -> Result<Vec<u8>, AutomergeError> {
    let mut actor_ids = Vec::new();
    val.encode_with_actors_to_vec(&mut actor_ids)
        .map_err(|_| AutomergeError::EncodeFailed)
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

        let deps_buf = self.deps.encode_with_actors_to_vec(actors)?;

        let ops_buf = ColumnEncoder::encode_ops(&self.operations, actors);

        len += actors[1..].encode(buf)?;

        buf.write_all(&deps_buf)?;
        len += deps_buf.len();

        buf.write_all(&ops_buf)?;
        len += ops_buf.len();

        Ok(len)
    }
}

impl Encodable for Clock {
    fn encode_with_actors<R: Write>(
        &self,
        buf: &mut R,
        actors: &mut Vec<ActorID>,
    ) -> io::Result<usize> {
        let mut len = 0;
        self.0.len().encode(buf)?;
        for (actor, val) in self.0.iter() {
            len += actor.encode_with_actors(buf, actors)?;
            len += val.encode(buf)?;
        }
        Ok(len)
    }
}

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

fn read_slice<T: Decodable + Debug>(buf: &mut &[u8]) -> Result<T,AutomergeError> {
    T::decode::<&[u8]>(buf).ok_or(AutomergeError::ChangeBadFormat)
}

fn slice_bytes<'a>(bytes: &mut &'a[u8]) -> Result<&'a [u8],AutomergeError>
{
    let mut buf = &bytes[..];
    let len = leb128::read::unsigned(&mut buf).map_err(|_| AutomergeError::ChangeBadFormat)? as usize;
    let result = &buf[0..len];
    *bytes = &buf[len..];
    Ok(result)
}

fn slice_bytes_len<'a>(bytes: &'a [u8]) -> Result<(&'a [u8], usize),AutomergeError> {
    let mut view = &bytes[..];
    let len = leb128::read::unsigned(&mut view).map_err(|_| AutomergeError::ChangeBadFormat)? as usize;
    let len_bytes = bytes.len() - view.len();
    Ok((&view[0..len], len + len_bytes))
}

#[derive(Debug, Clone)]
struct BinaryContainer<'a> {
    magic: &'a [u8],
    checksum: &'a [u8],
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
    deps: Vec<(usize,u64)>,
    ops: HashMap<u32,&'a [u8]>,
}

impl <'a> BinaryChange<'a> {
    fn from(bytes: &'a [u8]) -> Result<BinaryChange<'a>,AutomergeError> {
        let bytes = &mut &bytes[..];
        let all = &bytes[0..];
        let actor = slice_bytes(bytes)?;
        let seq = read_slice(bytes)?;
        let start_op = read_slice(bytes)?;
        let time = read_slice(bytes)?;
        let message = slice_bytes(bytes)?;
        let num_actors = read_slice(bytes)?;
        let mut actors = vec![ &actor[..] ];
        for _ in 0..num_actors {
            let actor = slice_bytes(bytes)?;
            actors.push(actor);
        }
        let mut deps = Vec::new();
        let num_deps = read_slice(bytes)?;
        for _ in 0..num_deps {
            let actor: usize = read_slice(bytes)?;
            let val: u64 = read_slice(bytes)?;
            deps.push((actor,val));
        }
        let mut ops = HashMap::new();
        while !bytes.is_empty() {
            let id = read_slice(bytes)?;
            let column = slice_bytes(bytes)?;
            ops.insert(id,column);
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

    fn gen_deps(&self) -> Option<Clock> {
        let mut deps = Clock::empty();
        for (id,val) in self.deps.iter() {
            deps.set(&ActorID::from_bytes(self.actors.get(*id)?),*val)
        }
        Some(deps)
    }

    fn message(&self) -> Option<String> {
        if self.message.len() == 0 {
            None
        } else {
            str::from_utf8(&self.message).map(|s| s.to_string()).ok()
        }
    }

    fn to_change(&self) -> Result<Change,AutomergeError> {
        let change = Change {
            start_op: self.start_op,
            seq: self.seq,
            time: self.time,
            message: self.message(),
            actor_id: ActorID::from_bytes(self.actors[0]),
            deps: self.gen_deps().ok_or_else(|| err("failed to gen deps"))?,
            operations: self.iter_ops().collect(),
        };
        Ok(change)
    }

    fn col_iter<T>(&self, col_id: u32) -> T
        where T: From<&'a [u8]>,
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
    action: RLEDecoder<'a,Action>,
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

impl <'a> Iterator for PredIterator<'a> {
    type Item = Vec<OpID>;
    fn next(&mut self) -> Option<Vec<OpID>> {
        let num = self.pred_num.next()??;
        let mut p = Vec::with_capacity(num);
        for _ in 0..num {
            let actor = self.pred_actor.next()??;
            let ctr = self.pred_ctr.next()??;
            let actor_bytes = self.actors.get(actor)?;
            let actor_id = ActorID::from_bytes(actor_bytes);
            let op_id = OpID::new(ctr,&actor_id);
            p.push(op_id)
        }
        Some(p)
    }
}

impl <'a> Iterator for ValueIterator<'a> {
    type Item = PrimitiveValue;
    fn next(&mut self) -> Option<PrimitiveValue> {
        let val_type = self.val_len.next()??;
        match val_type {
            VALUE_TYPE_NULL => Some(PrimitiveValue::Null),
            VALUE_TYPE_FALSE => Some(PrimitiveValue::Boolean(false)),
            VALUE_TYPE_TRUE => Some(PrimitiveValue::Boolean(true)),
            v if v % 16 == VALUE_TYPE_COUNTER => {
                let _len = v >> 4; // FIXME - should confirm this length
                let val = self.val_raw.read::<i64>("").ok()?;
                Some(PrimitiveValue::Counter(val))
            }
            v if v % 16 == VALUE_TYPE_TIMESTAMP => {
                let _len = v >> 4; // FIXME - should confirm this length
                let val = self.val_raw.read::<i64>("").ok()?;
                Some(PrimitiveValue::Timestamp(val))
            }
            v if v % 16 == VALUE_TYPE_LEB128_UINT => {
                let _len = v >> 4; // FIXME - should confirm this length
                let val = self.val_raw.read::<u64>("").ok()?;
                Some(PrimitiveValue::Number(val as f64))
            }
            v if v % 16 == VALUE_TYPE_LEB128_INT => {
                let _len = v >> 4; // FIXME - should confirm this length
                let val = self.val_raw.read::<i64>("").ok()?;
                Some(PrimitiveValue::Number(val as f64))
            }
            v if v % 16 == VALUE_TYPE_UTF8 => {
                let len = v >> 4;
                let data = self.val_raw.read_bytes(len, "raw_val_utf8").ok()?;
                let s = str::from_utf8(&data).ok()?;
                Some(PrimitiveValue::Str(s.to_string()))
            }
            v if v % 16 == VALUE_TYPE_BYTES => {
                let len = v >> 4;
                let _data = self.val_raw.read_bytes(len, "raw_val_bytes").ok()?;
                unimplemented!()
                //Some((PrimitiveValue::Bytes(data))
            }
            v if v % 16 >= VALUE_TYPE_MIN_UNKNOWN && v % 16 <= VALUE_TYPE_MAX_UNKNOWN => {
                let len = v >> 4;
                let _data = self.val_raw.read_bytes(len, "raw_unknown").ok()?;
                unimplemented!()
                //Some((PrimitiveValue::Bytes(data))
            }
            v if v % 16 == VALUE_TYPE_IEEE754 => {
                let len = v >> 4;
                if len == 4 {
                    // confirm only 4 bytes read
                    let num: f32 = self.val_raw.read("f32").ok()?;
                    Some(PrimitiveValue::Number(num as f64))
                } else if len == 8 {
                    // confirm only 8 bytes read
                    let num = self.val_raw.read("f64").ok()?;
                    Some(PrimitiveValue::Number(num))
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

impl <'a> Iterator for KeyIterator<'a> {
    type Item = Key;
    fn next(&mut self) -> Option<Key> {
        match (self.actor.next()?, self.ctr.next()?, self.str.next()?) {
            (None,None,Some(string)) => Some(Key(string)),
            (Some(0),Some(0),None) => Some(Key("_head".to_string())),
            (Some(actor),Some(ctr),None) => {
                let actor_bytes = self.actors.get(actor)?;
                let actor_id = ActorID::from_bytes(actor_bytes);
                let op_id = OpID::new(ctr,&actor_id);
                Some(op_id.to_key())
            },
            _ => None
        }
    }
}

impl <'a> Iterator for ObjIterator<'a> {
    type Item = ObjectID;
    fn next(&mut self) -> Option<ObjectID> {
        if let (Some(actor),Some(ctr)) = (self.actor.next()?, self.ctr.next()? ) {
            let actor_id = ActorID::from_bytes(self.actors.get(actor)?);
            Some(ObjectID::ID(OpID(ctr, actor_id.0)))
        } else {
            Some(ObjectID::Root)
        }
    }
}

impl <'a> Iterator for OperationIterator<'a> {
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

impl <'a> BinaryContainer<'a> {

    fn from(mut bytes: &'a [u8]) -> Result<Vec<BinaryContainer<'a>>,AutomergeError> {
        let mut changes = Vec::new();
        while !bytes.is_empty() {
            let change = Self::parse_single(bytes)?;
            bytes = &bytes[change.len .. ];
            changes.push(change);
        }
        Ok(changes)
    }

    fn parse_single(bytes: &'a [u8]) -> Result<BinaryContainer<'a>,AutomergeError> {
        if bytes.len() < 8 {
            return Err(AutomergeError::ChangeBadFormat)
        }
        let (header,rest) = &bytes.split_at(8);
        let (magic,checksum) = &header.split_at(4);
        if magic != &MAGIC_BYTES {
            return Err(AutomergeError::ChangeBadFormat)
        }
        let (chunk_data,chunk_len) = slice_bytes_len(&rest[1..])?;
        let body = &rest[0..(chunk_len + 1)]; // +1 for chunktype
        let chunktype = body[0];
        let len = body.len() + header.len();
        Ok(BinaryContainer {
            magic,
            checksum,
            chunktype,
            body,
            chunk: BinaryChange::from(chunk_data)?,
            len,
        })
    }

    fn is_valid(&self) -> bool {
        let mut hasher = Sha256::new();
        hasher.input(&self.body);
        &hasher.result()[0..4] == self.checksum
    }

    fn to_change(&self) -> Result<Change,AutomergeError> {
        if !self.is_valid() {
            return Err(AutomergeError::InvalidChange)
        }
        self.chunk.to_change()
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

    fn append_value(&mut self, val: &PrimitiveValue) {
        match val {
            PrimitiveValue::Null => self.len.append_value(VALUE_TYPE_NULL),
            PrimitiveValue::Boolean(true) => self.len.append_value(VALUE_TYPE_TRUE),
            PrimitiveValue::Boolean(false) => self.len.append_value(VALUE_TYPE_FALSE),
            PrimitiveValue::Str(s) => {
                let bytes = s.as_bytes();
                let len = bytes.len();
                self.raw.extend(bytes);
                self.len.append_value(len << 4 | VALUE_TYPE_UTF8)
            }
            /*
            PrimitiveValue::Bytes(bytes) => {
                let len = bytes.len();
                self.raw.extend(bytes);
                self.len.append_value(len << 4 | VALUE_TYPE_BYTES)
            },
            */
            PrimitiveValue::Counter(count) => {
                let len = count.encode(&mut self.raw).unwrap();
                self.len.append_value(len << 4 | VALUE_TYPE_COUNTER)
            }
            PrimitiveValue::Timestamp(time) => {
                let len = time.encode(&mut self.raw).unwrap();
                self.len.append_value(len << 4 | VALUE_TYPE_TIMESTAMP)
            }
            PrimitiveValue::Number(n) => {
                if *n < 0.0 {
                    let len = (*n as i64).encode(&mut self.raw).unwrap();
                    self.len.append_value(len << 4 | VALUE_TYPE_LEB128_INT)
                } else {
                    let len = (*n as u64).encode(&mut self.raw).unwrap();
                    self.len.append_value(len << 4 | VALUE_TYPE_LEB128_UINT)
                }
            } /*
              PrimitiveValue::Int(n) => { }
              PrimitiveValue::UInt(n) => { }
              PrimitiveValue::F32(n) => { }
              PrimitiveValue::F64(n) => { }
              PrimitiveValue::Unknown(num,bytes) => {
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
        vec![self.len.finish(COL_VAL_LEN), ColData { col: COL_VAL_RAW, data: self.raw } ]
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
        match key.as_element_id().ok() {
            None => {
                self.actor.append_null();
                self.ctr.append_null();
                self.str.append_value(key.0.clone());
            }
            Some(ElementID::Head) => {
                self.actor.append_value(0);
                self.ctr.append_value(0);
                self.str.append_null();
            }
            Some(ElementID::ID(OpID(ctr, actor))) => {
                self.actor.append_value(map_string(&actor, actors));
                self.ctr.append_value(ctr);
                self.str.append_null();
            }
        }
    }

    fn finish(self) -> Vec<ColData> {
        vec![ self.actor.finish(COL_KEY_ACTOR), self.ctr.finish(COL_KEY_CTR), self.str.finish(COL_KEY_STR) ]
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
        vec![ self.num.finish(COL_PRED_NUM), self.actor.finish(COL_PRED_ACTOR), self.ctr.finish(COL_PRED_CTR) ]
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
        vec![ self.actor.finish(COL_OBJ_ACTOR), self.ctr.finish(COL_OBJ_CTR) ]
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
        vec![ self.actor.finish(COL_CHILD_ACTOR), self.ctr.finish(COL_CHILD_CTR) ]
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
        e.encode(ops,actors);
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
            },
            OpType::Inc(val) => {
                // FIXME - should be int or uint
                self.val.append_value(&PrimitiveValue::Number(*val as f64));
                self.chld.append_null();
                Action::Inc
            },
            OpType::Del => {
                self.val.append_null();
                self.chld.append_null();
                Action::Del
            },
            OpType::Link(child) => {
                self.val.append_null();
                self.chld.append(child, actors);
                Action::Link
            },
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
        coldata.sort_by(|a,b| a.col.cmp(&b.col));

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

/*
#[derive(PartialEq, Debug, Clone, Copy)]
enum Column {
    ObjActor,
    ObjCtr,
    KeyActor,
    KeyCtr,
    KeyStr,
    IdActor,
    IdCtr,
    Insert,
    Action,
    ValLen,
    ValRaw,
    ChildActor,
    ChildCtr,
    PredNum,
    PredActor,
    PredCtr,
    SuccNum,
    SuccActor,
    SuccCtr,
}
*/

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

const COL_OBJ_ACTOR : u32 = COLUMN_TYPE_ACTOR_ID;
const COL_OBJ_CTR : u32 = COLUMN_TYPE_INT_RLE;
const COL_KEY_ACTOR : u32 = 1 << 3 | COLUMN_TYPE_ACTOR_ID;
const COL_KEY_CTR : u32 = 1 << 3 | COLUMN_TYPE_INT_DELTA;
const COL_KEY_STR : u32 = 1 << 3 | COLUMN_TYPE_STRING_RLE;
//const COL_ID_ACTOR : u32 = 2 << 3 | COLUMN_TYPE_ACTOR_ID;
//const COL_ID_CTR : u32 = 2 << 3 | COLUMN_TYPE_INT_DELTA;
const COL_INSERT : u32 = 3 << 3 | COLUMN_TYPE_BOOLEAN;
const COL_ACTION : u32 = 4 << 3 | COLUMN_TYPE_INT_RLE;
const COL_VAL_LEN : u32 = 5 << 3 | COLUMN_TYPE_VALUE_LEN;
const COL_VAL_RAW : u32 = 5 << 3 | COLUMN_TYPE_VALUE_RAW;
const COL_CHILD_ACTOR : u32 = 6 << 3 | COLUMN_TYPE_ACTOR_ID;
const COL_CHILD_CTR : u32 = 6 << 3 | COLUMN_TYPE_INT_DELTA;
const COL_PRED_NUM : u32 = 7 << 3 | COLUMN_TYPE_GROUP_CARD;
const COL_PRED_ACTOR : u32 = 7 << 3 | COLUMN_TYPE_ACTOR_ID;
const COL_PRED_CTR : u32 = 7 << 3 | COLUMN_TYPE_INT_DELTA;
//const COL_SUCC_NUM : u32 = 8 << 3 | COLUMN_TYPE_GROUP_CARD;
//const COL_SUCC_ACTOR : u32 = 8 << 3 | COLUMN_TYPE_ACTOR_ID;
//const COL_SUCC_CTR : u32 = 8 << 3 | COLUMN_TYPE_INT_DELTA;

const MAGIC_BYTES: [u8; 4] = [0x85, 0x6f, 0x4a, 0x83];

fn err(s: &str) -> AutomergeError {
    AutomergeError::ChangeDecompressError(s.to_string())
}
