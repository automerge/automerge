use crate::encoding::{BooleanDecoder, Decodable, Decoder, DeltaDecoder, RLEDecoder};
use crate::encoding::{BooleanEncoder, ColData, DeltaEncoder, Encodable, RLEEncoder};
use automerge_protocol::{
    ActorID, ElementID, Key, ObjType, ObjectID, OpID, OpType, Operation, Value,
};
use core::fmt::Debug;
use std::io;
use std::io::{Read, Write};
use std::str;

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

pub struct OperationIterator<'a> {
    pub(crate) action: RLEDecoder<'a, Action>,
    pub(crate) objs: ObjIterator<'a>,
    pub(crate) chld: ObjIterator<'a>,
    pub(crate) keys: KeyIterator<'a>,
    pub(crate) insert: BooleanDecoder<'a>,
    pub(crate) value: ValueIterator<'a>,
    pub(crate) pred: PredIterator<'a>,
}

pub struct ObjIterator<'a> {
    //actors: &'a Vec<&'a [u8]>,
    pub(crate) actors: &'a Vec<ActorID>,
    pub(crate) actor: RLEDecoder<'a, usize>,
    pub(crate) ctr: RLEDecoder<'a, u64>,
}

pub struct PredIterator<'a> {
    pub(crate) actors: &'a Vec<ActorID>,
    pub(crate) pred_num: RLEDecoder<'a, usize>,
    pub(crate) pred_actor: RLEDecoder<'a, usize>,
    pub(crate) pred_ctr: DeltaDecoder<'a>,
}

pub struct KeyIterator<'a> {
    pub(crate) actors: &'a Vec<ActorID>,
    pub(crate) actor: RLEDecoder<'a, usize>,
    pub(crate) ctr: DeltaDecoder<'a>,
    pub(crate) str: RLEDecoder<'a, String>,
}

pub struct ValueIterator<'a> {
    pub(crate) val_len: RLEDecoder<'a, usize>,
    pub(crate) val_raw: Decoder<'a>,
}

impl<'a> Iterator for PredIterator<'a> {
    type Item = Vec<OpID>;
    fn next(&mut self) -> Option<Vec<OpID>> {
        let num = self.pred_num.next()??;
        let mut p = Vec::with_capacity(num);
        for _ in 0..num {
            let actor = self.pred_actor.next()??;
            let ctr = self.pred_ctr.next()??;
            let actor_id = self.actors.get(actor)?.clone();
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
                let actor_id = self.actors.get(actor)?.clone();
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
            let actor_id = self.actors.get(actor)?.clone();
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

pub(crate) struct ColumnEncoder {
    obj: ObjEncoder,
    key: KeyEncoder,
    insert: BooleanEncoder,
    action: RLEEncoder<Action>,
    val: ValEncoder,
    chld: ChildEncoder,
    pred: PredEncoder,
}

impl ColumnEncoder {
    pub fn encode_ops(ops: &[Operation], actors: &mut Vec<ActorID>) -> Vec<u8> {
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

pub(crate) const COLUMN_TYPE_GROUP_CARD: u32 = 0;
pub(crate) const COLUMN_TYPE_ACTOR_ID: u32 = 1;
pub(crate) const COLUMN_TYPE_INT_RLE: u32 = 2;
pub(crate) const COLUMN_TYPE_INT_DELTA: u32 = 3;
pub(crate) const COLUMN_TYPE_BOOLEAN: u32 = 4;
pub(crate) const COLUMN_TYPE_STRING_RLE: u32 = 5;
pub(crate) const COLUMN_TYPE_VALUE_LEN: u32 = 6;
pub(crate) const COLUMN_TYPE_VALUE_RAW: u32 = 7;

#[derive(PartialEq, Debug, Clone, Copy)]
#[repr(u32)]
pub(crate) enum Action {
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

pub(crate) const COL_OBJ_ACTOR: u32 = COLUMN_TYPE_ACTOR_ID;
pub(crate) const COL_OBJ_CTR: u32 = COLUMN_TYPE_INT_RLE;
pub(crate) const COL_KEY_ACTOR: u32 = 1 << 3 | COLUMN_TYPE_ACTOR_ID;
pub(crate) const COL_KEY_CTR: u32 = 1 << 3 | COLUMN_TYPE_INT_DELTA;
pub(crate) const COL_KEY_STR: u32 = 1 << 3 | COLUMN_TYPE_STRING_RLE;
//pub(crate) const COL_ID_ACTOR : u32 = 2 << 3 | COLUMN_TYPE_ACTOR_ID;
//pub(crate) const COL_ID_CTR : u32 = 2 << 3 | COLUMN_TYPE_INT_DELTA;
pub(crate) const COL_INSERT: u32 = 3 << 3 | COLUMN_TYPE_BOOLEAN;
pub(crate) const COL_ACTION: u32 = 4 << 3 | COLUMN_TYPE_INT_RLE;
pub(crate) const COL_VAL_LEN: u32 = 5 << 3 | COLUMN_TYPE_VALUE_LEN;
pub(crate) const COL_VAL_RAW: u32 = 5 << 3 | COLUMN_TYPE_VALUE_RAW;
pub(crate) const COL_CHILD_ACTOR: u32 = 6 << 3 | COLUMN_TYPE_ACTOR_ID;
pub(crate) const COL_CHILD_CTR: u32 = 6 << 3 | COLUMN_TYPE_INT_DELTA;
pub(crate) const COL_PRED_NUM: u32 = 7 << 3 | COLUMN_TYPE_GROUP_CARD;
pub(crate) const COL_PRED_ACTOR: u32 = 7 << 3 | COLUMN_TYPE_ACTOR_ID;
pub(crate) const COL_PRED_CTR: u32 = 7 << 3 | COLUMN_TYPE_INT_DELTA;
//pub(crate) const COL_SUCC_NUM : u32 = 8 << 3 | COLUMN_TYPE_GROUP_CARD;
//pub(crate) const COL_SUCC_ACTOR : u32 = 8 << 3 | COLUMN_TYPE_ACTOR_ID;
//pub(crate) const COL_SUCC_CTR : u32 = 8 << 3 | COLUMN_TYPE_INT_DELTA;
