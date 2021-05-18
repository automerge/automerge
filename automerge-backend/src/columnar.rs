use core::fmt::Debug;
use std::{
    borrow::Cow,
    cmp::Ordering,
    collections::HashMap,
    io,
    io::{Read, Write},
    ops::Range,
    str,
};

use automerge_protocol as amp;
use flate2::bufread::DeflateDecoder;

use crate::{
    decoding::{BooleanDecoder, Decodable, Decoder, DeltaDecoder, RleDecoder},
    encoding::{BooleanEncoder, ColData, DeltaEncoder, Encodable, RleEncoder},
};

impl Encodable for Action {
    fn encode<R: Write>(&self, buf: &mut R) -> io::Result<usize> {
        (*self as u32).encode(buf)
    }
}

impl Encodable for [amp::ActorId] {
    fn encode<R: Write>(&self, buf: &mut R) -> io::Result<usize> {
        let mut len = self.len().encode(buf)?;
        for i in self {
            len += i.to_bytes().encode(buf)?;
        }
        Ok(len)
    }
}

fn map_actor(actor: &amp::ActorId, actors: &mut Vec<amp::ActorId>) -> usize {
    if let Some(pos) = actors.iter().position(|a| a == actor) {
        pos
    } else {
        actors.push(actor.clone());
        actors.len() - 1
    }
}

impl Encodable for amp::ActorId {
    fn encode_with_actors<R: Write>(
        &self,
        buf: &mut R,
        actors: &mut Vec<amp::ActorId>,
    ) -> io::Result<usize> {
        map_actor(self, actors).encode(buf)
    }

    fn encode<R: Write>(&self, _buf: &mut R) -> io::Result<usize> {
        // we instead encode actors as their position on a sequence
        Ok(0)
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
    pub(crate) action: RleDecoder<'a, Action>,
    pub(crate) objs: ObjIterator<'a>,
    pub(crate) keys: KeyIterator<'a>,
    pub(crate) insert: BooleanDecoder<'a>,
    pub(crate) value: ValueIterator<'a>,
    pub(crate) pred: PredIterator<'a>,
}

impl<'a> OperationIterator<'a> {
    pub(crate) fn new(
        bytes: &'a [u8],
        actors: &'a [amp::ActorId],
        ops: &'a HashMap<u32, Range<usize>>,
    ) -> OperationIterator<'a> {
        OperationIterator {
            objs: ObjIterator {
                actors,
                actor: col_iter(bytes, ops, COL_OBJ_ACTOR),
                ctr: col_iter(bytes, ops, COL_OBJ_CTR),
            },
            keys: KeyIterator {
                actors,
                actor: col_iter(bytes, ops, COL_KEY_ACTOR),
                ctr: col_iter(bytes, ops, COL_KEY_CTR),
                str: col_iter(bytes, ops, COL_KEY_STR),
            },
            value: ValueIterator {
                val_len: col_iter(bytes, ops, COL_VAL_LEN),
                val_raw: col_iter(bytes, ops, COL_VAL_RAW),
                actors,
                actor: col_iter(bytes, ops, COL_REF_ACTOR),
                ctr: col_iter(bytes, ops, COL_REF_CTR),
            },
            pred: PredIterator {
                actors,
                pred_num: col_iter(bytes, ops, COL_PRED_NUM),
                pred_actor: col_iter(bytes, ops, COL_PRED_ACTOR),
                pred_ctr: col_iter(bytes, ops, COL_PRED_CTR),
            },
            insert: col_iter(bytes, ops, COL_INSERT),
            action: col_iter(bytes, ops, COL_ACTION),
        }
    }
}

impl<'a> Iterator for OperationIterator<'a> {
    type Item = amp::Op;
    fn next(&mut self) -> Option<amp::Op> {
        let action = self.action.next()??;
        let insert = self.insert.next()?;
        let obj = self.objs.next()?;
        let key = self.keys.next()?;
        let pred = self.pred.next()?;
        let value = self.value.next()?;
        let action = match action {
            Action::Set => amp::OpType::Set(value),
            Action::MakeList => amp::OpType::Make(amp::ObjType::list()),
            Action::MakeText => amp::OpType::Make(amp::ObjType::text()),
            Action::MakeMap => amp::OpType::Make(amp::ObjType::map()),
            Action::MakeTable => amp::OpType::Make(amp::ObjType::table()),
            Action::Del => amp::OpType::Del,
            Action::Inc => amp::OpType::Inc(value.to_i64()?),
        };
        Some(amp::Op {
            action,
            obj,
            key,
            pred,
            insert,
        })
    }
}

pub(crate) struct DocOpIterator<'a> {
    pub(crate) actor: RleDecoder<'a, usize>,
    pub(crate) ctr: DeltaDecoder<'a>,
    pub(crate) action: RleDecoder<'a, Action>,
    pub(crate) objs: ObjIterator<'a>,
    pub(crate) keys: KeyIterator<'a>,
    pub(crate) insert: BooleanDecoder<'a>,
    pub(crate) value: ValueIterator<'a>,
    pub(crate) succ: SuccIterator<'a>,
}

impl<'a> Iterator for DocOpIterator<'a> {
    type Item = DocOp;
    fn next(&mut self) -> Option<DocOp> {
        let action = self.action.next()??;
        let actor = self.actor.next()??;
        let ctr = self.ctr.next()??;
        let insert = self.insert.next()?;
        let obj = self.objs.next()?;
        let key = self.keys.next()?;
        let succ = self.succ.next()?;
        let value = self.value.next()?;
        let action = match action {
            Action::Set => amp::OpType::Set(value),
            Action::MakeList => amp::OpType::Make(amp::ObjType::list()),
            Action::MakeText => amp::OpType::Make(amp::ObjType::text()),
            Action::MakeMap => amp::OpType::Make(amp::ObjType::map()),
            Action::MakeTable => amp::OpType::Make(amp::ObjType::table()),
            Action::Del => amp::OpType::Del,
            Action::Inc => amp::OpType::Inc(value.to_i64()?),
        };
        Some(DocOp {
            actor,
            ctr,
            action,
            obj,
            key,
            succ,
            pred: Vec::new(),
            insert,
        })
    }
}

impl<'a> DocOpIterator<'a> {
    pub(crate) fn new(
        bytes: &'a [u8],
        actors: &'a [amp::ActorId],
        ops: &'a HashMap<u32, Range<usize>>,
    ) -> DocOpIterator<'a> {
        DocOpIterator {
            actor: col_iter(bytes, ops, COL_ID_ACTOR),
            ctr: col_iter(bytes, ops, COL_ID_CTR),
            objs: ObjIterator {
                actors,
                actor: col_iter(bytes, ops, COL_OBJ_ACTOR),
                ctr: col_iter(bytes, ops, COL_OBJ_CTR),
            },
            keys: KeyIterator {
                actors,
                actor: col_iter(bytes, ops, COL_KEY_ACTOR),
                ctr: col_iter(bytes, ops, COL_KEY_CTR),
                str: col_iter(bytes, ops, COL_KEY_STR),
            },
            value: ValueIterator {
                val_len: col_iter(bytes, ops, COL_VAL_LEN),
                val_raw: col_iter(bytes, ops, COL_VAL_RAW),
                actors,
                actor: col_iter(bytes, ops, COL_REF_ACTOR),
                ctr: col_iter(bytes, ops, COL_REF_CTR),
            },
            succ: SuccIterator {
                succ_num: col_iter(bytes, ops, COL_SUCC_NUM),
                succ_actor: col_iter(bytes, ops, COL_SUCC_ACTOR),
                succ_ctr: col_iter(bytes, ops, COL_SUCC_CTR),
            },
            insert: col_iter(bytes, ops, COL_INSERT),
            action: col_iter(bytes, ops, COL_ACTION),
        }
    }
}

pub(crate) struct ChangeIterator<'a> {
    pub(crate) actor: RleDecoder<'a, usize>,
    pub(crate) seq: DeltaDecoder<'a>,
    pub(crate) max_op: DeltaDecoder<'a>,
    pub(crate) time: DeltaDecoder<'a>,
    pub(crate) message: RleDecoder<'a, String>,
    pub(crate) deps: DepsIterator<'a>,
    pub(crate) extra: ExtraIterator<'a>,
}

impl<'a> ChangeIterator<'a> {
    pub(crate) fn new(bytes: &'a [u8], ops: &'a HashMap<u32, Range<usize>>) -> ChangeIterator<'a> {
        ChangeIterator {
            actor: col_iter(bytes, ops, DOC_ACTOR),
            seq: col_iter(bytes, ops, DOC_SEQ),
            max_op: col_iter(bytes, ops, DOC_MAX_OP),
            time: col_iter(bytes, ops, DOC_TIME),
            message: col_iter(bytes, ops, DOC_MESSAGE),
            deps: DepsIterator {
                num: col_iter(bytes, ops, DOC_DEPS_NUM),
                dep: col_iter(bytes, ops, DOC_DEPS_INDEX),
            },
            extra: ExtraIterator {
                len: col_iter(bytes, ops, DOC_EXTRA_LEN),
                extra: col_iter(bytes, ops, DOC_EXTRA_RAW),
            },
        }
    }
}

impl<'a> Iterator for ChangeIterator<'a> {
    type Item = DocChange;
    fn next(&mut self) -> Option<DocChange> {
        let actor = self.actor.next()??;
        let seq = self.seq.next()??;
        let max_op = self.max_op.next()??;
        let time = self.time.next()?? as i64;
        let message = self.message.next()?;
        let deps = self.deps.next()?;
        let extra_bytes = self.extra.next().unwrap_or_else(Vec::new);
        Some(DocChange {
            actor,
            seq,
            max_op,
            time,
            message,
            deps,
            extra_bytes,
            ops: Vec::new(),
        })
    }
}

pub struct ObjIterator<'a> {
    //actors: &'a Vec<&'a [u8]>,
    pub(crate) actors: &'a [amp::ActorId],
    pub(crate) actor: RleDecoder<'a, usize>,
    pub(crate) ctr: RleDecoder<'a, u64>,
}

pub struct DepsIterator<'a> {
    pub(crate) num: RleDecoder<'a, usize>,
    pub(crate) dep: DeltaDecoder<'a>,
}

pub struct ExtraIterator<'a> {
    pub(crate) len: RleDecoder<'a, usize>,
    pub(crate) extra: Decoder<'a>,
}

pub struct PredIterator<'a> {
    pub(crate) actors: &'a [amp::ActorId],
    pub(crate) pred_num: RleDecoder<'a, usize>,
    pub(crate) pred_actor: RleDecoder<'a, usize>,
    pub(crate) pred_ctr: DeltaDecoder<'a>,
}

pub struct SuccIterator<'a> {
    pub(crate) succ_num: RleDecoder<'a, usize>,
    pub(crate) succ_actor: RleDecoder<'a, usize>,
    pub(crate) succ_ctr: DeltaDecoder<'a>,
}

pub struct KeyIterator<'a> {
    pub(crate) actors: &'a [amp::ActorId],
    pub(crate) actor: RleDecoder<'a, usize>,
    pub(crate) ctr: DeltaDecoder<'a>,
    pub(crate) str: RleDecoder<'a, String>,
}

pub struct ValueIterator<'a> {
    pub(crate) actors: &'a [amp::ActorId],
    pub(crate) val_len: RleDecoder<'a, usize>,
    pub(crate) val_raw: Decoder<'a>,
    pub(crate) actor: RleDecoder<'a, usize>,
    pub(crate) ctr: RleDecoder<'a, u64>,
}

impl<'a> Iterator for DepsIterator<'a> {
    type Item = Vec<usize>;
    fn next(&mut self) -> Option<Vec<usize>> {
        let num = self.num.next()??;
        // I bet there's something simple like `self.dep.take(num).collect()`
        let mut p = Vec::with_capacity(num);
        for _ in 0..num {
            let dep = self.dep.next()??;
            p.push(dep as usize);
        }
        Some(p)
    }
}

impl<'a> Iterator for ExtraIterator<'a> {
    type Item = Vec<u8>;
    fn next(&mut self) -> Option<Vec<u8>> {
        let v = self.len.next()??;
        // if v % 16 == VALUE_TYPE_BYTES => { // this should be bytes
        let len = v >> 4;
        self.extra.read_bytes(len).ok().map(|s| s.to_vec())
    }
}

impl<'a> Iterator for PredIterator<'a> {
    type Item = Vec<amp::OpId>;
    fn next(&mut self) -> Option<Vec<amp::OpId>> {
        let num = self.pred_num.next()??;
        let mut p = Vec::with_capacity(num);
        for _ in 0..num {
            let actor = self.pred_actor.next()??;
            let ctr = self.pred_ctr.next()??;
            let actor_id = self.actors.get(actor)?.clone();
            let op_id = amp::OpId::new(ctr, &actor_id);
            p.push(op_id)
        }
        Some(p)
    }
}

impl<'a> Iterator for SuccIterator<'a> {
    type Item = Vec<(u64, usize)>;
    fn next(&mut self) -> Option<Vec<(u64, usize)>> {
        let num = self.succ_num.next()??;
        let mut p = Vec::with_capacity(num);
        for _ in 0..num {
            let actor = self.succ_actor.next()??;
            let ctr = self.succ_ctr.next()??;
            p.push((ctr, actor))
        }
        Some(p)
    }
}

impl<'a> Iterator for ValueIterator<'a> {
    type Item = amp::ScalarValue;
    fn next(&mut self) -> Option<amp::ScalarValue> {
        let val_type = self.val_len.next()??;
        let actor = self.actor.next()?;
        let ctr = self.ctr.next()?;
        match val_type {
            VALUE_TYPE_NULL => Some(amp::ScalarValue::Null),
            VALUE_TYPE_FALSE => Some(amp::ScalarValue::Boolean(false)),
            VALUE_TYPE_TRUE => Some(amp::ScalarValue::Boolean(true)),
            v if v % 16 == VALUE_TYPE_COUNTER => {
                let len = v >> 4;
                let val = self.val_raw.read().ok()?;
                if len != self.val_raw.last_read {
                    return None;
                }
                Some(amp::ScalarValue::Counter(val))
            }
            v if v % 16 == VALUE_TYPE_TIMESTAMP => {
                let len = v >> 4;
                let val = self.val_raw.read().ok()?;
                if len != self.val_raw.last_read {
                    return None;
                }
                Some(amp::ScalarValue::Timestamp(val))
            }
            v if v % 16 == VALUE_TYPE_LEB128_UINT => {
                let len = v >> 4;
                let val = self.val_raw.read().ok()?;
                if len != self.val_raw.last_read {
                    return None;
                }
                Some(amp::ScalarValue::Uint(val))
            }
            v if v % 16 == VALUE_TYPE_LEB128_INT => {
                let len = v >> 4;
                let val = self.val_raw.read().ok()?;
                if len != self.val_raw.last_read {
                    return None;
                }
                Some(amp::ScalarValue::Int(val))
            }
            v if v % 16 == VALUE_TYPE_UTF8 => {
                let len = v >> 4;
                let data = self.val_raw.read_bytes(len).ok()?;
                let s = str::from_utf8(data).ok()?;
                Some(amp::ScalarValue::Str(s.to_string()))
            }
            v if v % 16 == VALUE_TYPE_BYTES => {
                let len = v >> 4;
                let _data = self.val_raw.read_bytes(len).ok()?;
                unimplemented!()
                //Some((amp::Value::Bytes(data))
            }
            v if v % 16 >= VALUE_TYPE_MIN_UNKNOWN && v % 16 <= VALUE_TYPE_MAX_UNKNOWN => {
                let len = v >> 4;
                let _data = self.val_raw.read_bytes(len).ok()?;
                unimplemented!()
                //Some((amp::Value::Bytes(data))
            }
            v if v % 16 == VALUE_TYPE_IEEE754 => {
                let len = v >> 4;
                if len == 4 {
                    // confirm only 4 bytes read
                    let num: f32 = self.val_raw.read().ok()?;
                    Some(amp::ScalarValue::F32(num))
                } else if len == 8 {
                    // confirm only 8 bytes read
                    let num = self.val_raw.read().ok()?;
                    Some(amp::ScalarValue::F64(num))
                } else {
                    // bad size of float
                    None
                }
            }
            v if v % 16 == VALUE_TYPE_CURSOR => {
                if let (Some(actor), Some(ctr)) = (actor, ctr) {
                    let actor_id = self.actors.get(actor)?;
                    Some(amp::ScalarValue::Cursor(amp::OpId(ctr, actor_id.clone())))
                } else {
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
    type Item = amp::Key;
    fn next(&mut self) -> Option<amp::Key> {
        match (self.actor.next()?, self.ctr.next()?, self.str.next()?) {
            (None, None, Some(string)) => Some(amp::Key::Map(string)),
            (None, Some(0), None) => Some(amp::Key::head()),
            (Some(actor), Some(ctr), None) => {
                let actor_id = self.actors.get(actor)?;
                Some(amp::OpId::new(ctr, actor_id).into())
            }
            _ => None,
        }
    }
}

impl<'a> Iterator for ObjIterator<'a> {
    type Item = amp::ObjectId;
    fn next(&mut self) -> Option<amp::ObjectId> {
        if let (Some(actor), Some(ctr)) = (self.actor.next()?, self.ctr.next()?) {
            let actor_id = self.actors.get(actor)?;
            Some(amp::ObjectId::Id(amp::OpId::new(ctr, actor_id)))
        } else {
            Some(amp::ObjectId::Root)
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub(crate) struct DocChange {
    pub actor: usize,
    pub seq: u64,
    pub max_op: u64,
    pub time: i64,
    pub message: Option<String>,
    pub deps: Vec<usize>,
    pub extra_bytes: Vec<u8>,
    pub ops: Vec<DocOp>,
}

#[derive(Debug, Clone)]
pub(crate) struct DocOp {
    pub actor: usize,
    pub ctr: u64,
    pub action: amp::OpType,
    pub obj: amp::ObjectId,
    pub key: amp::Key,
    pub succ: Vec<(u64, usize)>,
    pub pred: Vec<(u64, usize)>,
    pub insert: bool,
}

impl Ord for DocOp {
    fn cmp(&self, other: &Self) -> Ordering {
        self.ctr.cmp(&other.ctr)
    }
}

impl PartialOrd for DocOp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for DocOp {
    fn eq(&self, other: &Self) -> bool {
        self.ctr == other.ctr
    }
}

impl Eq for DocOp {}

struct ValEncoder {
    len: RleEncoder<usize>,
    ref_actor: RleEncoder<usize>,
    ref_counter: RleEncoder<u64>,
    raw: Vec<u8>,
}

impl ValEncoder {
    fn new() -> ValEncoder {
        ValEncoder {
            len: RleEncoder::new(),
            raw: Vec::new(),
            ref_actor: RleEncoder::new(),
            ref_counter: RleEncoder::new(),
        }
    }

    fn append_value(&mut self, val: &amp::ScalarValue, actors: &mut Vec<amp::ActorId>) {
        // It may seem weird to have two consecutive matches on the same value. The reason is so
        // that we don't have to repeat the `append_null` calls on ref_actor and ref_counter in
        // every arm of the next match
        if !matches!(val, amp::ScalarValue::Cursor(_)) {
            self.ref_actor.append_null();
            self.ref_counter.append_null();
        }
        match val {
            amp::ScalarValue::Null => self.len.append_value(VALUE_TYPE_NULL),
            amp::ScalarValue::Boolean(true) => self.len.append_value(VALUE_TYPE_TRUE),
            amp::ScalarValue::Boolean(false) => self.len.append_value(VALUE_TYPE_FALSE),
            amp::ScalarValue::Str(s) | amp::ScalarValue::Bytes(s) => {
                let bytes = s.as_bytes();
                let len = bytes.len();
                self.raw.extend(bytes);
                self.len.append_value(len << 4 | VALUE_TYPE_UTF8)
            }
            amp::ScalarValue::Counter(count) => {
                let len = count.encode(&mut self.raw).unwrap();
                self.len.append_value(len << 4 | VALUE_TYPE_COUNTER)
            }
            amp::ScalarValue::Timestamp(time) => {
                let len = time.encode(&mut self.raw).unwrap();
                self.len.append_value(len << 4 | VALUE_TYPE_TIMESTAMP)
            }
            amp::ScalarValue::Int(n) => {
                let len = n.encode(&mut self.raw).unwrap();
                self.len.append_value(len << 4 | VALUE_TYPE_LEB128_INT)
            }
            amp::ScalarValue::Uint(n) => {
                let len = n.encode(&mut self.raw).unwrap();
                self.len.append_value(len << 4 | VALUE_TYPE_LEB128_UINT)
            }
            amp::ScalarValue::F32(n) => {
                let len = (*n).encode(&mut self.raw).unwrap();
                self.len.append_value(len << 4 | VALUE_TYPE_IEEE754)
            }
            amp::ScalarValue::F64(n) => {
                let len = (*n).encode(&mut self.raw).unwrap();
                self.len.append_value(len << 4 | VALUE_TYPE_IEEE754)
            }
            amp::ScalarValue::Cursor(opid) => {
                // the cursor opid are encoded in DocOpEncoder::encode and ColumnEncoder::encode
                self.len.append_value(VALUE_TYPE_CURSOR);
                let actor_index = map_actor(&opid.1, actors);
                self.ref_actor.append_value(actor_index);
                self.ref_counter.append_value(opid.0);
            }
        }
    }

    fn append_null(&mut self) {
        self.ref_counter.append_null();
        self.ref_actor.append_null();
        self.len.append_value(VALUE_TYPE_NULL)
    }

    fn finish(self) -> Vec<ColData> {
        vec![
            self.ref_counter.finish(COL_REF_CTR),
            self.ref_actor.finish(COL_REF_ACTOR),
            self.len.finish(COL_VAL_LEN),
            ColData::new(COL_VAL_RAW, self.raw),
        ]
    }
}

struct KeyEncoder {
    actor: RleEncoder<usize>,
    ctr: DeltaEncoder,
    str: RleEncoder<String>,
}

impl KeyEncoder {
    fn new() -> KeyEncoder {
        KeyEncoder {
            actor: RleEncoder::new(),
            ctr: DeltaEncoder::new(),
            str: RleEncoder::new(),
        }
    }

    fn append(&mut self, key: &amp::Key, actors: &mut Vec<amp::ActorId>) {
        match &key {
            amp::Key::Map(s) => {
                self.actor.append_null();
                self.ctr.append_null();
                self.str.append_value(s.clone());
            }
            amp::Key::Seq(amp::ElementId::Head) => {
                self.actor.append_null();
                self.ctr.append_value(0);
                self.str.append_null();
            }
            amp::Key::Seq(amp::ElementId::Id(amp::OpId(ctr, actor))) => {
                self.actor.append_value(map_actor(actor, actors));
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

struct SuccEncoder {
    num: RleEncoder<usize>,
    actor: RleEncoder<usize>,
    ctr: DeltaEncoder,
}

impl SuccEncoder {
    fn new() -> SuccEncoder {
        SuccEncoder {
            num: RleEncoder::new(),
            actor: RleEncoder::new(),
            ctr: DeltaEncoder::new(),
        }
    }

    fn append(&mut self, succ: &[(u64, usize)]) {
        self.num.append_value(succ.len());
        for s in succ.iter() {
            self.ctr.append_value(s.0);
            self.actor.append_value(s.1);
        }
    }

    fn finish(self) -> Vec<ColData> {
        vec![
            self.num.finish(COL_SUCC_NUM),
            self.actor.finish(COL_SUCC_ACTOR),
            self.ctr.finish(COL_SUCC_CTR),
        ]
    }
}

struct PredEncoder {
    num: RleEncoder<usize>,
    actor: RleEncoder<usize>,
    ctr: DeltaEncoder,
}

impl PredEncoder {
    fn new() -> PredEncoder {
        PredEncoder {
            num: RleEncoder::new(),
            actor: RleEncoder::new(),
            ctr: DeltaEncoder::new(),
        }
    }

    fn append(&mut self, pred: &[amp::OpId], actors: &mut Vec<amp::ActorId>) {
        self.num.append_value(pred.len());
        for p in pred.iter() {
            self.ctr.append_value(p.0);
            self.actor.append_value(map_actor(&p.1, actors));
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
    actor: RleEncoder<usize>,
    ctr: RleEncoder<u64>,
}

impl ObjEncoder {
    fn new() -> ObjEncoder {
        ObjEncoder {
            actor: RleEncoder::new(),
            ctr: RleEncoder::new(),
        }
    }

    fn append(&mut self, obj: &amp::ObjectId, actors: &mut Vec<amp::ActorId>) {
        match obj {
            amp::ObjectId::Root => {
                self.actor.append_null();
                self.ctr.append_null();
            }
            amp::ObjectId::Id(amp::OpId(ctr, actor)) => {
                self.actor.append_value(map_actor(actor, actors));
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

pub(crate) struct ChangeEncoder {
    actor: RleEncoder<usize>,
    seq: DeltaEncoder,
    max_op: DeltaEncoder,
    time: DeltaEncoder,
    message: RleEncoder<Option<String>>,
    deps_num: RleEncoder<usize>,
    deps_index: DeltaEncoder,
    extra_len: RleEncoder<usize>,
    extra_raw: Vec<u8>,
}

impl ChangeEncoder {
    pub fn encode_changes<'a, 'b, I>(changes: I, actors: &'a [amp::ActorId]) -> (Vec<u8>, Vec<u8>)
    where
        I: IntoIterator<Item = &'b amp::UncompressedChange>,
    {
        let mut e = Self::new();
        e.encode(changes, actors);
        e.finish()
    }

    fn new() -> ChangeEncoder {
        ChangeEncoder {
            actor: RleEncoder::new(),
            seq: DeltaEncoder::new(),
            max_op: DeltaEncoder::new(),
            time: DeltaEncoder::new(),
            message: RleEncoder::new(),
            deps_num: RleEncoder::new(),
            deps_index: DeltaEncoder::new(),
            extra_len: RleEncoder::new(),
            extra_raw: Vec::new(),
        }
    }

    fn encode<'a, 'b, 'c, I>(&'a mut self, changes: I, actors: &'b [amp::ActorId])
    where
        I: IntoIterator<Item = &'c amp::UncompressedChange>,
    {
        let mut index_by_hash: HashMap<amp::ChangeHash, usize> = HashMap::new();
        for (index, change) in changes.into_iter().enumerate() {
            if let Some(hash) = change.hash {
                index_by_hash.insert(hash, index);
            }
            self.actor
                .append_value(actors.iter().position(|a| a == &change.actor_id).unwrap());
            self.seq.append_value(change.seq);
            self.max_op
                .append_value(change.start_op + change.operations.len() as u64 - 1);
            self.time.append_value(change.time as u64);
            self.message.append_value(change.message.clone());
            self.deps_num.append_value(change.deps.len());
            for dep in &change.deps {
                if let Some(dep_index) = index_by_hash.get(dep) {
                    self.deps_index.append_value(*dep_index as u64)
                } else {
                    // FIXME This relies on the changes being in causal order, which they may not
                    // be, we could probably do something cleverer like accumulate the values to
                    // write and the dependency tree in an intermediate value, then write it to the
                    // encoder in a second pass over the intermediates
                    panic!("Missing dependency for hash: {:?}", dep);
                }
            }
            self.extra_len
                .append_value(change.extra_bytes.len() << 4 | VALUE_TYPE_BYTES);
            self.extra_raw.extend(&change.extra_bytes);
        }
    }

    fn finish(self) -> (Vec<u8>, Vec<u8>) {
        let mut coldata = vec![
            self.actor.finish(DOC_ACTOR),
            self.seq.finish(DOC_SEQ),
            self.max_op.finish(DOC_MAX_OP),
            self.time.finish(DOC_TIME),
            self.message.finish(DOC_MESSAGE),
            self.deps_num.finish(DOC_DEPS_NUM),
            self.deps_index.finish(DOC_DEPS_INDEX),
            self.extra_len.finish(DOC_EXTRA_LEN),
            ColData::new(DOC_EXTRA_RAW, self.extra_raw),
        ];
        coldata.sort_by(|a, b| a.col.cmp(&b.col));

        let mut data = Vec::new();
        let mut info = Vec::new();
        coldata
            .iter()
            .filter(|&d| !d.data.is_empty())
            .count()
            .encode(&mut info)
            .ok();
        for d in &mut coldata {
            d.deflate();
            d.encode_col_len(&mut info).ok();
        }
        for d in &coldata {
            data.write_all(d.data.as_slice()).ok();
        }
        (data, info)
    }
}

pub(crate) struct DocOpEncoder {
    actor: RleEncoder<usize>,
    ctr: DeltaEncoder,
    obj: ObjEncoder,
    key: KeyEncoder,
    insert: BooleanEncoder,
    action: RleEncoder<Action>,
    val: ValEncoder,
    succ: SuccEncoder,
}

// FIXME - actors should not be mut here

impl DocOpEncoder {
    pub(crate) fn encode_doc_ops<'a, 'b, I>(
        ops: I,
        actors: &'a mut Vec<amp::ActorId>,
    ) -> (Vec<u8>, Vec<u8>)
    where
        I: IntoIterator<Item = &'b DocOp>,
    {
        let mut e = Self::new();
        e.encode(ops, actors);
        e.finish()
    }

    fn new() -> DocOpEncoder {
        DocOpEncoder {
            actor: RleEncoder::new(),
            ctr: DeltaEncoder::new(),
            obj: ObjEncoder::new(),
            key: KeyEncoder::new(),
            insert: BooleanEncoder::new(),
            action: RleEncoder::new(),
            val: ValEncoder::new(),
            succ: SuccEncoder::new(),
        }
    }

    fn encode<'a, 'b, 'c, I>(&'a mut self, ops: I, actors: &'b mut Vec<amp::ActorId>)
    where
        I: IntoIterator<Item = &'c DocOp>,
    {
        for op in ops {
            self.actor.append_value(op.actor);
            self.ctr.append_value(op.ctr);
            self.obj.append(&op.obj, actors);
            self.key.append(&op.key, actors);
            self.insert.append(op.insert);
            self.succ.append(&op.succ);
            let action = match &op.action {
                amp::OpType::Set(value) => {
                    self.val.append_value(value, actors);
                    Action::Set
                }
                amp::OpType::Inc(val) => {
                    self.val.append_value(&amp::ScalarValue::Int(*val), actors);
                    Action::Inc
                }
                amp::OpType::Del => {
                    // FIXME throw error
                    self.val.append_null();
                    Action::Del
                }
                amp::OpType::Make(kind) => {
                    self.val.append_null();
                    match kind {
                        amp::ObjType::Sequence(amp::SequenceType::List) => Action::MakeList,
                        amp::ObjType::Map(amp::MapType::Map) => Action::MakeMap,
                        amp::ObjType::Map(amp::MapType::Table) => Action::MakeTable,
                        amp::ObjType::Sequence(amp::SequenceType::Text) => Action::MakeText,
                    }
                }
            };
            self.action.append_value(action);
        }
    }

    fn finish(self) -> (Vec<u8>, Vec<u8>) {
        let mut coldata = vec![
            self.actor.finish(COL_ID_ACTOR),
            self.ctr.finish(COL_ID_CTR),
            self.insert.finish(COL_INSERT),
            self.action.finish(COL_ACTION),
        ];
        coldata.extend(self.obj.finish());
        coldata.extend(self.key.finish());
        coldata.extend(self.val.finish());
        coldata.extend(self.succ.finish());
        coldata.sort_by(|a, b| a.col.cmp(&b.col));

        let mut info = Vec::new();
        let mut data = Vec::new();
        coldata
            .iter()
            .filter(|&d| !d.data.is_empty())
            .count()
            .encode(&mut info)
            .ok();
        for d in &mut coldata {
            d.deflate();
            d.encode_col_len(&mut info).ok();
        }
        for d in &coldata {
            data.write_all(d.data.as_slice()).ok();
        }
        (data, info)
    }
}

//pub(crate) encode_cols(a) -> (Vec<u8>, HashMap<u32, Range<usize>>) { }

pub(crate) struct ColumnEncoder {
    obj: ObjEncoder,
    key: KeyEncoder,
    insert: BooleanEncoder,
    action: RleEncoder<Action>,
    val: ValEncoder,
    pred: PredEncoder,
}

impl ColumnEncoder {
    pub fn encode_ops<'a, 'b, I>(
        ops: I,
        actors: &'a mut Vec<amp::ActorId>,
    ) -> (Vec<u8>, HashMap<u32, Range<usize>>)
    where
        I: IntoIterator<Item = &'b amp::Op>,
    {
        let mut e = Self::new();
        e.encode(ops, actors);
        e.finish()
    }

    fn new() -> ColumnEncoder {
        ColumnEncoder {
            obj: ObjEncoder::new(),
            key: KeyEncoder::new(),
            insert: BooleanEncoder::new(),
            action: RleEncoder::new(),
            val: ValEncoder::new(),
            pred: PredEncoder::new(),
        }
    }

    fn encode<'a, 'b, 'c, I>(&'a mut self, ops: I, actors: &'b mut Vec<amp::ActorId>)
    where
        I: IntoIterator<Item = &'c amp::Op>,
    {
        for op in ops {
            self.append(op, actors)
        }
    }

    fn append(&mut self, op: &amp::Op, actors: &mut Vec<amp::ActorId>) {
        self.obj.append(&op.obj, actors);
        self.key.append(&op.key, actors);
        self.insert.append(op.insert);
        self.pred.append(&op.pred, actors);
        let action = match &op.action {
            amp::OpType::Set(value) => {
                self.val.append_value(value, actors);
                Action::Set
            }
            amp::OpType::Inc(val) => {
                self.val.append_value(&amp::ScalarValue::Int(*val), actors);
                Action::Inc
            }
            amp::OpType::Del => {
                self.val.append_null();
                Action::Del
            }
            amp::OpType::Make(kind) => {
                self.val.append_null();
                match kind {
                    amp::ObjType::Sequence(amp::SequenceType::List) => Action::MakeList,
                    amp::ObjType::Map(amp::MapType::Map) => Action::MakeMap,
                    amp::ObjType::Map(amp::MapType::Table) => Action::MakeTable,
                    amp::ObjType::Sequence(amp::SequenceType::Text) => Action::MakeText,
                }
            }
        };
        self.action.append_value(action);
    }

    fn finish(self) -> (Vec<u8>, HashMap<u32, Range<usize>>) {
        let mut coldata = vec![
            self.insert.finish(COL_INSERT),
            self.action.finish(COL_ACTION),
        ];
        coldata.extend(self.obj.finish());
        coldata.extend(self.key.finish());
        coldata.extend(self.val.finish());
        coldata.extend(self.pred.finish());
        coldata.sort_by(|a, b| a.col.cmp(&b.col));

        let mut data = Vec::new();
        let mut rangemap = HashMap::new();
        coldata
            .iter()
            .filter(|&d| !d.data.is_empty())
            .count()
            .encode(&mut data)
            .ok();
        for d in &mut coldata {
            d.encode_col_len(&mut data).ok();
        }
        for d in &coldata {
            let begin = data.len();
            data.write_all(d.data.as_slice()).ok();
            if !d.data.is_empty() {
                rangemap.insert(d.col, begin..data.len());
            }
        }
        (data, rangemap)
    }
}

fn col_iter<'a, T>(bytes: &'a [u8], ops: &'a HashMap<u32, Range<usize>>, col_id: u32) -> T
where
    T: From<Cow<'a, [u8]>>,
{
    let bytes = if let Some(r) = ops.get(&col_id) {
        Cow::Borrowed(&bytes[r.clone()])
    } else if let Some(r) = ops.get(&(col_id | COLUMN_TYPE_DEFLATE)) {
        let mut decoder = DeflateDecoder::new(&bytes[r.clone()]);
        let mut inflated = Vec::new();
        //TODO this could throw if the compression is corrupt, we should propagate the error rather
        //than unwrapping
        decoder.read_to_end(&mut inflated).unwrap();
        Cow::Owned(inflated)
    } else {
        Cow::from(&[] as &[u8])
    };
    T::from(bytes)
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
const VALUE_TYPE_CURSOR: usize = 10;
const VALUE_TYPE_MIN_UNKNOWN: usize = 11;
const VALUE_TYPE_MAX_UNKNOWN: usize = 15;

pub(crate) const COLUMN_TYPE_GROUP_CARD: u32 = 0;
pub(crate) const COLUMN_TYPE_ACTOR_ID: u32 = 1;
pub(crate) const COLUMN_TYPE_INT_RLE: u32 = 2;
pub(crate) const COLUMN_TYPE_INT_DELTA: u32 = 3;
pub(crate) const COLUMN_TYPE_BOOLEAN: u32 = 4;
pub(crate) const COLUMN_TYPE_STRING_RLE: u32 = 5;
pub(crate) const COLUMN_TYPE_VALUE_LEN: u32 = 6;
pub(crate) const COLUMN_TYPE_VALUE_RAW: u32 = 7;
pub(crate) const COLUMN_TYPE_DEFLATE: u32 = 8;

#[derive(PartialEq, Debug, Clone, Copy)]
#[repr(u32)]
pub(crate) enum Action {
    MakeMap,
    Set,
    MakeList,
    Del,
    MakeText,
    Inc,
    MakeTable,
}
const ACTIONS: [Action; 7] = [
    Action::MakeMap,
    Action::Set,
    Action::MakeList,
    Action::Del,
    Action::MakeText,
    Action::Inc,
    Action::MakeTable,
];

impl Decodable for Action {
    fn decode<R>(bytes: &mut R) -> Option<Self>
    where
        R: Read,
    {
        let num = usize::decode::<R>(bytes)?;
        ACTIONS.get(num).copied()
    }
}

const COL_OBJ_ACTOR: u32 = COLUMN_TYPE_ACTOR_ID;
const COL_OBJ_CTR: u32 = COLUMN_TYPE_INT_RLE;
const COL_KEY_ACTOR: u32 = 1 << 4 | COLUMN_TYPE_ACTOR_ID;
const COL_KEY_CTR: u32 = 1 << 4 | COLUMN_TYPE_INT_DELTA;
const COL_KEY_STR: u32 = 1 << 4 | COLUMN_TYPE_STRING_RLE;
const COL_ID_ACTOR: u32 = 2 << 4 | COLUMN_TYPE_ACTOR_ID;
const COL_ID_CTR: u32 = 2 << 4 | COLUMN_TYPE_INT_DELTA;
const COL_INSERT: u32 = 3 << 4 | COLUMN_TYPE_BOOLEAN;
const COL_ACTION: u32 = 4 << 4 | COLUMN_TYPE_INT_RLE;
const COL_VAL_LEN: u32 = 5 << 4 | COLUMN_TYPE_VALUE_LEN;
const COL_VAL_RAW: u32 = 5 << 4 | COLUMN_TYPE_VALUE_RAW;
const COL_PRED_NUM: u32 = 7 << 4 | COLUMN_TYPE_GROUP_CARD;
const COL_PRED_ACTOR: u32 = 7 << 4 | COLUMN_TYPE_ACTOR_ID;
const COL_PRED_CTR: u32 = 7 << 4 | COLUMN_TYPE_INT_DELTA;
const COL_SUCC_NUM: u32 = 8 << 4 | COLUMN_TYPE_GROUP_CARD;
const COL_SUCC_ACTOR: u32 = 8 << 4 | COLUMN_TYPE_ACTOR_ID;
const COL_SUCC_CTR: u32 = 8 << 4 | COLUMN_TYPE_INT_DELTA;
const COL_REF_CTR: u32 = 6 << 4 | COLUMN_TYPE_INT_RLE;
const COL_REF_ACTOR: u32 = 6 << 4 | COLUMN_TYPE_ACTOR_ID;

const DOC_ACTOR: u32 = /* 0 << 4 */ COLUMN_TYPE_ACTOR_ID;
const DOC_SEQ: u32 = /* 0 << 4 */ COLUMN_TYPE_INT_DELTA;
const DOC_MAX_OP: u32 = 1 << 4 | COLUMN_TYPE_INT_DELTA;
const DOC_TIME: u32 = 2 << 4 | COLUMN_TYPE_INT_DELTA;
const DOC_MESSAGE: u32 = 3 << 4 | COLUMN_TYPE_STRING_RLE;
const DOC_DEPS_NUM: u32 = 4 << 4 | COLUMN_TYPE_GROUP_CARD;
const DOC_DEPS_INDEX: u32 = 4 << 4 | COLUMN_TYPE_INT_DELTA;
const DOC_EXTRA_LEN: u32 = 5 << 4 | COLUMN_TYPE_VALUE_LEN;
const DOC_EXTRA_RAW: u32 = 5 << 4 | COLUMN_TYPE_VALUE_RAW;

/*
const DOCUMENT_COLUMNS = {
  actor:     0 << 3 | COLUMN_TYPE.ACTOR_ID,
  seq:       0 << 3 | COLUMN_TYPE.INT_DELTA,
  maxOp:     1 << 3 | COLUMN_TYPE.INT_DELTA,
  time:      2 << 3 | COLUMN_TYPE.INT_DELTA,
  message:   3 << 3 | COLUMN_TYPE.STRING_RLE,
  depsNum:   4 << 3 | COLUMN_TYPE.GROUP_CARD,
  depsIndex: 4 << 3 | COLUMN_TYPE.INT_DELTA,
  extraLen:  5 << 3 | COLUMN_TYPE.VALUE_LEN,
  extraRaw:  5 << 3 | COLUMN_TYPE.VALUE_RAW
}
*/

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rle_encoder_for_strings_from_key() {
        // this seems like a strange case but checks that we write nulls into the encoder as usize and read them out the same.
        // if we don't then the 64 nulls gets interpreted as -64 and causes the rle decoder to never read the next values.
        let ops = vec![
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some("a".to_owned()),
        ];
        let mut encoder = RleEncoder::new();
        for op in &ops {
            if let Some(v) = op {
                encoder.append_value(v.clone())
            } else {
                encoder.append_null()
            }
        }
        let encoded = encoder.finish(0).data;

        assert_eq!(encoded, vec![0, 64, 127, 1, 97]);

        let decoder: RleDecoder<String> = RleDecoder::from(Cow::from(&encoded[..]));

        let decoded = decoder.take(ops.len()).collect::<Vec<_>>();
        assert_eq!(decoded, ops);
    }
}
