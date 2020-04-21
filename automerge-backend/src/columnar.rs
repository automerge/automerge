use crate::encoding::{BooleanDecoder, Decodable, Decoder, DeltaDecoder, RLEDecoder};
use crate::error::AutomergeError;
use crate::protocol::{
    ActorID, Change, Clock, DataType, Key, ObjType, ObjectID, OpID, OpType, Operation,
    PrimitiveValue,
};
use sha2::{Digest, Sha256};
use std::str;

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

#[derive(PartialEq, Debug, Clone, Copy)]
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

impl Decodable for Action {
    fn decode(bytes: &[u8]) -> Option<(Self, usize)> {
        let (num, offset) = u32::decode(bytes)?;
        match num {
            i if i == 0 => Some((Action::Set, offset)),
            i if i == 1 => Some((Action::Del, offset)),
            i if i == 2 => Some((Action::Inc, offset)),
            i if i == 3 => Some((Action::Link, offset)),
            i if i == 4 => Some((Action::MakeMap, offset)),
            i if i == 5 => Some((Action::MakeList, offset)),
            i if i == 6 => Some((Action::MakeText, offset)),
            i if i == 7 => Some((Action::MakeTable, offset)),
            _ => None,
        }
    }
}

impl Decodable for Column {
    fn decode(bytes: &[u8]) -> Option<(Self, usize)> {
        let (s, offset) = u32::decode(bytes)?;
        match s {
            i if i == COLUMN_TYPE_ACTOR_ID => Some((Self::ObjActor, offset)),
            i if i == COLUMN_TYPE_INT_RLE => Some((Self::ObjCtr, offset)),
            i if i == 1 << 3 | COLUMN_TYPE_ACTOR_ID => Some((Self::KeyActor, offset)),
            i if i == 1 << 3 | COLUMN_TYPE_INT_DELTA => Some((Self::KeyCtr, offset)),
            i if i == 1 << 3 | COLUMN_TYPE_STRING_RLE => Some((Self::KeyStr, offset)),
            i if i == 2 << 3 | COLUMN_TYPE_ACTOR_ID => Some((Self::IdActor, offset)),
            i if i == 2 << 3 | COLUMN_TYPE_INT_DELTA => Some((Self::IdCtr, offset)),
            i if i == 3 << 3 | COLUMN_TYPE_BOOLEAN => Some((Self::Insert, offset)),
            i if i == 4 << 3 | COLUMN_TYPE_INT_RLE => Some((Self::Action, offset)),
            i if i == 5 << 3 | COLUMN_TYPE_VALUE_LEN => Some((Self::ValLen, offset)),
            i if i == 5 << 3 | COLUMN_TYPE_VALUE_RAW => Some((Self::ValRaw, offset)),
            i if i == 6 << 3 | COLUMN_TYPE_ACTOR_ID => Some((Self::ChildActor, offset)),
            i if i == 6 << 3 | COLUMN_TYPE_INT_DELTA => Some((Self::ChildCtr, offset)),
            i if i == 7 << 3 | COLUMN_TYPE_GROUP_CARD => Some((Self::PredNum, offset)),
            i if i == 7 << 3 | COLUMN_TYPE_ACTOR_ID => Some((Self::PredActor, offset)),
            i if i == 7 << 3 | COLUMN_TYPE_INT_DELTA => Some((Self::PredCtr, offset)),
            i if i == 8 << 3 | COLUMN_TYPE_GROUP_CARD => Some((Self::SuccNum, offset)),
            i if i == 8 << 3 | COLUMN_TYPE_ACTOR_ID => Some((Self::SuccActor, offset)),
            i if i == 8 << 3 | COLUMN_TYPE_INT_DELTA => Some((Self::SuccCtr, offset)),
            _ => None,
        }
    }
}

const MAGIC_BYTES: [u8; 4] = [0x85, 0x6f, 0x4a, 0x83];

fn err(s: &str) -> AutomergeError {
    AutomergeError::ChangeDecompressError(s.to_string())
}

pub(crate) struct ChangeDecoder {
    blocks: Vec<Vec<u8>>,
}

impl ChangeDecoder {
    pub fn new(blocks: Vec<Vec<u8>>) -> Self {
        ChangeDecoder { blocks }
    }

    pub fn decode(&mut self) -> Result<Vec<Change>, AutomergeError> {
        let mut changes = Vec::new();
        for block in &self.blocks {
            let mut bytes: &[u8] = block.as_slice();
            while !bytes.is_empty() {
                let (change, rest) = self.decode_change(&bytes)?;
                changes.push(change);
                bytes = rest;
            }
        }
        Ok(changes)
    }

    fn decode_change<'a>(&self, bytes: &'a [u8]) -> Result<(Change, &'a [u8]), AutomergeError> {
        let mut decoder = Decoder::new(bytes);
        let mut hasher = Sha256::new();

        let header = decoder.read_bytes(MAGIC_BYTES.len(), "magic_bytes")?;

        if header != MAGIC_BYTES {
            return Err(err("bad magic_bytes"));
        }

        let expected_hash = decoder.read_bytes(32, "expected_hash")?;
        let hash_offset = decoder.offset;
        let chunk_type = decoder.read::<u8>("chunk_type")?;
        let chunk_leng = decoder.read::<usize>("chunk_length")?;
        let chunk_data = decoder.read_bytes(chunk_leng, "chunk_data")?;

        hasher.input(&bytes[hash_offset..decoder.offset]);

        let rest = decoder.rest();

        if hasher.result()[..] != expected_hash[..] {
            return Err(err("hash_failed"));
        }

        if chunk_type != 1 {
            return Err(err("chunk_type not 1"));
        }

        let chunk = self.decode_chunk(&chunk_data)?;

        Ok((chunk, rest))
    }

    fn decode_chunk(&self, bytes: &[u8]) -> Result<Change, AutomergeError> {
        let mut decoder = Decoder::new(bytes);
        let actor_id: ActorID = decoder.read("actorid")?;
        let seq = decoder.read("seq")?;
        let start_op = decoder.read("start_op")?;
        let time = decoder.read::<i64>("time")? as u128;

        let message = decoder.read("message")?;

        let num_actors: i64 = decoder.read("num_actors")?;
        let mut actors: Vec<ActorID> = Vec::new();
        actors.push(actor_id.clone());
        for _ in 0..num_actors {
            let actor = decoder.read("another_actor")?;
            actors.push(actor);
        }
        let num_deps: i64 = decoder.read("num_deps")?;
        let mut deps = Clock::empty();
        for _ in 0..num_deps {
            let dep_actor: usize = decoder.read("dep_actor")?;
            let dep_val: u64 = decoder.read("dep_val")?;
            deps.set(&actors[dep_actor], dep_val);
        }
        Ok(Change {
            start_op,
            message,
            actor_id,
            seq,
            deps,
            time,
            operations: self.decode_ops(decoder.rest(), actors)?,
        })
    }

    fn decode_ops(
        &self,
        bytes: &[u8],
        actors: Vec<ActorID>,
    ) -> Result<Vec<Operation>, AutomergeError> {
        let mut columns = Columns::new(bytes, actors)?;
        columns.ops().ok_or_else(|| err("failed to parse ops"))
    }
}

struct Columns<'a> {
    pub actors: Vec<ActorID>,
    pub obj_actor: RLEDecoder<'a, usize>,
    pub obj_ctr: RLEDecoder<'a, u64>,
    pub key_actor: RLEDecoder<'a, usize>,
    pub key_ctr: DeltaDecoder<'a>,
    pub key_str: RLEDecoder<'a, String>,
    pub id_actor: RLEDecoder<'a, usize>,
    pub id_ctr: DeltaDecoder<'a>,
    pub insert: BooleanDecoder<'a>,
    pub action: Decoder<'a>,
    pub val_len: RLEDecoder<'a, usize>,
    pub val_raw: Decoder<'a>,
    pub chld_actor: RLEDecoder<'a, usize>,
    pub chld_ctr: DeltaDecoder<'a>,
    pub pred_num: RLEDecoder<'a, usize>,
    pub pred_actor: RLEDecoder<'a, usize>,
    pub pred_ctr: DeltaDecoder<'a>,
    pub succ_num: RLEDecoder<'a, usize>,
    pub succ_actor: RLEDecoder<'a, usize>,
    pub succ_ctr: DeltaDecoder<'a>,
}

impl<'a> Columns<'a> {
    pub fn new(bytes: &[u8], actors: Vec<ActorID>) -> Result<Columns, AutomergeError> {
        let blank = Decoder::new(&bytes[0..0]);

        let mut c = Columns {
            actors,
            obj_actor: blank.iter(),
            obj_ctr: blank.iter(),
            key_actor: blank.iter(),
            key_ctr: blank.iter(),
            key_str: blank.iter(),
            id_actor: blank.iter(),
            id_ctr: blank.iter(),
            insert: blank.iter(),
            action: blank.clone(),
            val_len: blank.iter(),
            val_raw: blank.clone(),
            chld_actor: blank.iter(),
            chld_ctr: blank.iter(),
            pred_num: blank.iter(),
            pred_actor: blank.iter(),
            pred_ctr: blank.iter(),
            succ_num: blank.iter(),
            succ_actor: blank.iter(),
            succ_ctr: blank.iter(),
        };

        let mut decoder = Decoder::new(bytes);

        while !decoder.done() {
            let column_id: Column = decoder.read("column_id")?;
            let len: usize = decoder.read("column_len")?;
            let data = decoder.read_bytes(len, "column_data")?;
            match column_id {
                Column::ObjActor => c.obj_actor = Decoder::new(data).iter(),
                Column::ObjCtr => c.obj_ctr = Decoder::new(data).iter(),
                Column::KeyActor => c.key_actor = Decoder::new(data).iter(),
                Column::KeyCtr => c.key_ctr = Decoder::new(data).iter(),
                Column::KeyStr => c.key_str = Decoder::new(data).iter(),
                Column::IdActor => c.id_actor = Decoder::new(data).iter(),
                Column::IdCtr => c.id_ctr = Decoder::new(data).iter(),
                Column::Insert => c.insert = Decoder::new(data).iter(),
                Column::Action => c.action = Decoder::new(data),
                Column::ValLen => c.val_len = Decoder::new(data).iter(),
                Column::ValRaw => c.val_raw = Decoder::new(data),
                Column::ChildActor => c.chld_actor = Decoder::new(data).iter(),
                Column::ChildCtr => c.chld_ctr = Decoder::new(data).iter(),
                Column::PredNum => c.pred_num = Decoder::new(data).iter(),
                Column::PredActor => c.pred_actor = Decoder::new(data).iter(),
                Column::PredCtr => c.pred_ctr = Decoder::new(data).iter(),
                Column::SuccNum => c.succ_num = Decoder::new(data).iter(),
                Column::SuccActor => c.succ_actor = Decoder::new(data).iter(),
                Column::SuccCtr => c.succ_ctr = Decoder::new(data).iter(),
            }
        }

        Ok(c)
    }

    fn lookup_actor(&self, a: usize) -> Option<ActorID> {
        self.actors.get(a).cloned()
    }

    fn insert(&mut self) -> Option<bool> {
        self.insert.next()
    }

    // FIXME - this could be an iterator (zip).map()
    fn obj(&mut self) -> Option<ObjectID> {
        let actor = self.obj_actor.next()?;
        let ctr = self.obj_ctr.next()?;
        match (actor, ctr) {
            (None, None) => Some(ObjectID::Root),
            (Some(a), Some(c)) => Some(self.op_id(a, c)?.to_object_id()),
            _ => None,
        }
    }

    // FIXME - this could be an iterator (zip).map()
    fn child(&mut self) -> Option<Option<ObjectID>> {
        let actor = self.chld_actor.next()?;
        let ctr = self.chld_ctr.next()?;
        match (actor, ctr) {
            (None, None) => Some(None),
            (Some(a), Some(c)) => Some(Some(self.op_id(a, c)?.to_object_id())),
            _ => None,
        }
    }

    fn op_id(&self, act: usize, ctr: u64) -> Option<OpID> {
        self.lookup_actor(act).map(|actor| OpID::ID(ctr, actor.0))
    }

    // FIXME - this could be an iterator (zip).map()
    fn pred(&mut self) -> Option<Vec<OpID>> {
        let num = self.pred_num.next()??;
        let mut p = Vec::with_capacity(num);
        for _ in 0..num {
            let actor = self.pred_actor.next()??;
            let ctr = self.pred_ctr.next()??;
            p.push(self.op_id(actor, ctr)?);
        }
        Some(p)
    }

    fn _succ(&mut self) -> Option<Vec<OpID>> {
        let num = self.succ_num.next()??;
        let mut p = Vec::with_capacity(num);
        for _ in 0..num {
            let actor = self.succ_actor.next()??;
            let ctr = self.succ_ctr.next()??;
            p.push(self.op_id(actor, ctr)?);
        }
        Some(p)
    }

    // FIXME - this could be an iterator (zip).map()
    // FIXME - need to get Key::Map(String) and Key::Seq(ElementID)
    pub fn key(&mut self, insert: bool) -> Option<Key> {
        let actor = self.key_actor.next()?;
        let ctr = self.key_ctr.next()?;
        let name = self.key_str.next()?;
        match (actor, ctr, name, insert) {
            (Some(0), Some(0), None, true) => Some(Key("_head".to_string())),
            (Some(a), Some(c), None, _) => Some(self.op_id(a, c)?.to_key()),
            (None, None, Some(n), _) => Some(Key(n)),
            _ => None,
        }
    }

    // FIXME - need to combine Prim and DataType
    pub fn value(&mut self) -> Option<(PrimitiveValue, DataType)> {
        let val_type = self.val_len.next()??;
        let dt = DataType::Undefined;
        match val_type {
            VALUE_TYPE_NULL => Some((PrimitiveValue::Null, dt)),
            VALUE_TYPE_FALSE => Some((PrimitiveValue::Boolean(false), dt)),
            VALUE_TYPE_TRUE => Some((PrimitiveValue::Boolean(true), dt)),
            v if v % 16 == VALUE_TYPE_COUNTER => {
                let _len = v >> 4; // FIXME - should confirm this length
                let val = self.val_raw.read::<i64>("").ok()?;
                Some((PrimitiveValue::Number(val as f64), DataType::Counter))
            }
            v if v % 16 == VALUE_TYPE_TIMESTAMP => {
                let _len = v >> 4; // FIXME - should confirm this length
                let val = self.val_raw.read::<i64>("").ok()?;
                Some((PrimitiveValue::Number(val as f64), DataType::Timestamp))
            }
            v if v % 16 == VALUE_TYPE_LEB128_UINT => {
                let _len = v >> 4; // FIXME - should confirm this length
                let val = self.val_raw.read::<u64>("").ok()?;
                Some((PrimitiveValue::Number(val as f64), dt))
            }
            v if v % 16 == VALUE_TYPE_LEB128_INT => {
                let _len = v >> 4; // FIXME - should confirm this length
                let val = self.val_raw.read::<i64>("").ok()?;
                Some((PrimitiveValue::Number(val as f64), dt))
            }
            v if v % 16 == VALUE_TYPE_UTF8 => {
                let len = v >> 4;
                let data = self.val_raw.read_bytes(len, "raw_val_utf8").ok()?;
                let s = str::from_utf8(&data).ok()?;
                Some((PrimitiveValue::Str(s.to_string()), dt))
            }
            v if v % 16 == VALUE_TYPE_BYTES => {
                let len = v >> 4;
                let _data = self.val_raw.read_bytes(len, "raw_val_bytes").ok()?;
                unimplemented!()
                //Some((PrimitiveValue::Bytes(data),dt))
            }
            v if v % 16 >= VALUE_TYPE_MIN_UNKNOWN && v % 16 <= VALUE_TYPE_MAX_UNKNOWN => {
                let len = v >> 4;
                let _data = self.val_raw.read_bytes(len, "raw_unknown").ok()?;
                unimplemented!()
                //Some((PrimitiveValue::Bytes(data),dt))
            }
            v if v % 16 == VALUE_TYPE_IEEE754 => {
                let len = v >> 4;
                if len == 4 {
                    // confirm only 4 bytes read
                    let num: f32 = self.val_raw.read("f32").ok()?;
                    Some((PrimitiveValue::Number(num as f64), dt))
                } else if len == 8 {
                    // confirm only 8 bytes read
                    let num = self.val_raw.read("f64").ok()?;
                    Some((PrimitiveValue::Number(num), dt))
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

    pub fn ops(&mut self) -> Option<Vec<Operation>> {
        let mut ops = Vec::new();
        for action in self.action.iter::<RLEDecoder<Action>>().scan(0, |_, a| a) {
            ops.push(self.op(action)?);
        }
        Some(ops)
    }

    fn op(&mut self, action: Action) -> Option<Operation> {
        let insert = self.insert()?;
        let obj = self.obj()?;
        let key = self.key(insert)?;
        let pred = self.pred()?;
        //        let _succ =  self._succ()?;
        let (primitive, datatype) = self.value()?;
        let child = self.child()?;
        let action = match action {
            Action::Set => OpType::Set(primitive, datatype),
            Action::MakeList => OpType::Make(ObjType::List),
            Action::MakeText => OpType::Make(ObjType::Text),
            Action::MakeMap => OpType::Make(ObjType::Map),
            Action::MakeTable => OpType::Make(ObjType::Table),
            Action::Del => OpType::Del,
            Action::Inc => {
                if let PrimitiveValue::Number(f) = primitive {
                    OpType::Inc(f)
                } else {
                    return None;
                }
            }
            Action::Link => OpType::Link(child?),
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
