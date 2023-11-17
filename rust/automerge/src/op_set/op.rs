use crate::clock::Clock;
use crate::exid::ExId;
use crate::op_set::OpSetData;
use crate::text_value::TextValue;
use crate::types::{self, ActorId, ElemId, Key, ListEncoding, ObjId, OpId, OpIds, OpType};
use crate::value::{Counter, ScalarValue, Value};
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::HashSet;

#[derive(Copy, Clone, Debug, PartialEq)]
pub(crate) struct OpIdx(u32);

impl OpIdx {
    pub(crate) fn new(index: usize) -> Self {
        Self(index as u32)
    }

    pub(crate) fn get(&self) -> usize {
        self.0 as usize
    }

    pub(crate) fn as_op2(self, osd: &OpSetData) -> Op<'_> {
        Op::new(self.0 as usize, osd)
    }
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct Op<'a> {
    idx: usize,
    osd: &'a OpSetData,
}

// lamport compare with PartialEq! =D
impl<'a> PartialEq for Op<'a> {
    fn eq(&self, other: &Self) -> bool {
        (std::ptr::eq(self.osd, other.osd) && self.idx == other.idx) || self.op() == other.op()
    }
}

impl<'a> PartialOrd for Op<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let c1 = self.id().counter();
        let c2 = other.id().counter();
        Some(c1.cmp(&c2).then_with(|| self.actor().cmp(other.actor())))
    }
}

impl<'a> Op<'a> {
    pub(crate) fn new(idx: usize, osd: &'a OpSetData) -> Self {
        Self { idx, osd }
    }

    pub(crate) fn actor(&self) -> &ActorId {
        &self.osd.actors[self.op().id.actor()]
    }

    pub(crate) fn obj(&self) -> &'a ObjId {
        &self.op_plus().obj
    }

    fn op_plus(&self) -> &'a OpPlus {
        &self.osd.ops[self.idx]
    }

    fn op(&self) -> &'a OpBuilder {
        &self.osd.ops[self.idx].op
    }

    pub(crate) fn action(&self) -> &'a OpType {
        &self.op().action
    }

    pub(crate) fn key(&self) -> &'a Key {
        &self.op().key
    }

    pub(crate) fn id(&self) -> &'a OpId {
        &self.op().id
    }

    pub(crate) fn is_noop(&self, action: &OpType) -> bool {
        self.op().is_noop(action)
    }

    pub(crate) fn visible_at(&self, clock: Option<&Clock>) -> bool {
        self.op().visible_at(clock)
    }

    pub(crate) fn visible_or_mark(&self, clock: Option<&Clock>) -> bool {
        self.op().visible_or_mark(clock)
    }

    pub(crate) fn visible(&self) -> bool {
        self.op().visible()
    }

    pub(crate) fn overwrites(&self, other: Op<'_>) -> bool {
        self.op().overwrites(other.op())
    }

    pub(crate) fn elemid_or_key(&self) -> Key {
        self.op().elemid_or_key()
    }

    pub(crate) fn is_counter(&self) -> bool {
        self.op().is_counter()
    }

    pub(crate) fn is_delete(&self) -> bool {
        self.op().is_delete()
    }

    pub(crate) fn is_list_op(&self) -> bool {
        self.op().is_list_op()
    }

    pub(crate) fn is_mark(&self) -> bool {
        self.op().is_mark()
    }

    pub(crate) fn as_str(&self) -> &'a str {
        self.op().to_str()
    }

    pub(crate) fn width(&self, encoding: ListEncoding) -> usize {
        if encoding == ListEncoding::List {
            1
        } else {
            self.op_plus().width as usize
        }
    }

    pub(crate) fn insert(&self) -> bool {
        self.op().insert
    }

    pub(crate) fn elemid(&self) -> Option<ElemId> {
        self.op().elemid()
    }

    pub(crate) fn value(&self) -> Value<'a> {
        self.op().value()
    }

    pub(crate) fn value_at(&self, clock: Option<&Clock>) -> Value<'a> {
        self.op().value_at(clock)
    }

    pub(crate) fn scalar_value(&self) -> Option<&ScalarValue> {
        self.op().scalar_value()
    }

    pub(crate) fn tagged_value(&self, clock: Option<&Clock>) -> (Value<'a>, ExId) {
        (self.op().value_at(clock), self.exid())
    }

    pub(crate) fn predates(&self, clock: &Clock) -> bool {
        self.op().predates(clock)
    }

    pub(crate) fn was_deleted_before(&self, clock: &Clock) -> bool {
        self.op().was_deleted_before(clock)
    }

    pub(crate) fn exid(&self) -> ExId {
        let id = self.op().id;
        if id == types::ROOT {
            ExId::Root
        } else {
            ExId::Id(
                id.counter(),
                self.osd.actors.cache[id.actor()].clone(),
                id.actor(),
            )
        }
    }

    pub(crate) fn get_increment_value(&self) -> Option<i64> {
        self.op().get_increment_value()
    }

    pub(crate) fn lamport_cmp(&self, id: OpId) -> Ordering {
        self.osd.lamport_cmp(self.op().id, id)
    }

    pub(crate) fn key_cmp(&self, other: &Key) -> Ordering {
        self.osd.key_cmp(&self.op().key, other)
    }

    /*
        pub(crate) fn succ2(&self) -> impl Iterator<Item = Op<'a>> {
          todo!()
        }
    */

    pub(crate) fn succ(&self) -> &OpIds {
        &self.op().succ
    }

    pub(crate) fn pred(&self) -> impl Iterator<Item = &OpId> + ExactSizeIterator {
        self.op().pred.iter()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OpBuilder {
    pub(crate) id: OpId,
    pub(crate) action: OpType,
    pub(crate) key: Key,
    pub(crate) succ: OpIds,
    pub(crate) pred: OpIds,
    pub(crate) insert: bool,
}

pub(crate) enum SuccIter<'a> {
    Counter(HashSet<&'a OpId>, std::slice::Iter<'a, OpId>),
    NonCounter(std::slice::Iter<'a, OpId>),
}

impl<'a> Iterator for SuccIter<'a> {
    type Item = &'a OpId;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Counter(set, iter) => {
                for i in iter {
                    if !set.contains(i) {
                        return Some(i);
                    }
                }
                None
            }
            Self::NonCounter(iter) => iter.next(),
        }
    }
}

impl OpBuilder {
    pub(crate) fn succ_iter(&self) -> SuccIter<'_> {
        if let OpType::Put(ScalarValue::Counter(c)) = &self.action {
            let set = c
                .increments
                .iter()
                .map(|(id, _)| id)
                .collect::<HashSet<_>>();
            SuccIter::Counter(set, self.succ.iter())
        } else {
            SuccIter::NonCounter(self.succ.iter())
        }
    }

    pub(crate) fn increment(&mut self, n: i64, id: OpId) {
        if let OpType::Put(ScalarValue::Counter(c)) = &mut self.action {
            c.current += n;
            c.increments.push((id, n));
        }
    }

    pub(crate) fn remove_succ(&mut self, opid: &OpId, action: &OpType) {
        self.succ.retain(|id| id != opid);
        if let OpType::Put(ScalarValue::Counter(Counter {
            current,
            increments,
            ..
        })) = &mut self.action
        {
            if let OpType::Increment(n) = action {
                *current -= *n;
                increments.retain(|(id, _)| id != opid);
            }
        }
    }

    pub(crate) fn width(&self, encoding: ListEncoding) -> usize {
        match encoding {
            ListEncoding::List => 1,
            ListEncoding::Text => TextValue::width(self.to_str()),
        }
    }

    pub(crate) fn to_str(&self) -> &str {
        self.action.to_str()
    }

    pub(crate) fn visible(&self) -> bool {
        if self.is_inc() || self.is_mark() {
            false
        } else if self.is_counter() {
            self.succ.len() <= self.incs()
        } else {
            self.succ.is_empty()
        }
    }

    pub(crate) fn visible_at(&self, clock: Option<&Clock>) -> bool {
        if let Some(clock) = clock {
            if self.is_inc() || self.is_mark() {
                false
            } else {
                clock.covers(&self.id) && !self.succ_iter().any(|i| clock.covers(i))
            }
        } else {
            self.visible()
        }
    }

    pub(crate) fn visible_or_mark(&self, clock: Option<&Clock>) -> bool {
        if self.is_inc() {
            false
        } else if let Some(clock) = clock {
            clock.covers(&self.id) && !self.succ_iter().any(|i| clock.covers(i))
        } else if self.is_counter() {
            self.succ.len() <= self.incs()
        } else {
            self.succ.is_empty()
        }
    }

    pub(crate) fn incs(&self) -> usize {
        if let OpType::Put(ScalarValue::Counter(Counter { increments, .. })) = &self.action {
            increments.len()
        } else {
            0
        }
    }

    pub(crate) fn is_delete(&self) -> bool {
        matches!(&self.action, OpType::Delete)
    }

    pub(crate) fn is_inc(&self) -> bool {
        matches!(&self.action, OpType::Increment(_))
    }

    pub(crate) fn is_counter(&self) -> bool {
        matches!(&self.action, OpType::Put(ScalarValue::Counter(_)))
    }

    pub(crate) fn is_mark(&self) -> bool {
        self.action.is_mark()
    }

    pub(crate) fn is_noop(&self, action: &OpType) -> bool {
        matches!((&self.action, action), (OpType::Put(n), OpType::Put(m)) if n == m)
    }

    pub(crate) fn is_list_op(&self) -> bool {
        matches!(&self.key, Key::Seq(_))
    }

    pub(crate) fn overwrites(&self, other: &OpBuilder) -> bool {
        self.pred.iter().any(|i| i == &other.id)
    }

    pub(crate) fn elemid(&self) -> Option<ElemId> {
        if self.insert {
            Some(ElemId(self.id))
        } else if let Key::Seq(e) = self.key {
            Some(e)
        } else {
            None
        }
    }

    pub(crate) fn elemid_or_key(&self) -> Key {
        if self.insert {
            Key::Seq(ElemId(self.id))
        } else {
            self.key
        }
    }

    pub(crate) fn get_increment_value(&self) -> Option<i64> {
        if let OpType::Increment(i) = self.action {
            Some(i)
        } else {
            None
        }
    }

    pub(crate) fn value_at(&self, clock: Option<&Clock>) -> Value<'_> {
        if let Some(clock) = clock {
            if let OpType::Put(ScalarValue::Counter(c)) = &self.action {
                return Value::counter(c.value_at(clock));
            }
        }
        self.value()
    }

    pub(crate) fn scalar_value(&self) -> Option<&ScalarValue> {
        match &self.action {
            OpType::Put(scalar) => Some(scalar),
            _ => None,
        }
    }

    pub(crate) fn value(&self) -> Value<'_> {
        match &self.action {
            OpType::Make(obj_type) => Value::Object(*obj_type),
            OpType::Put(scalar) => Value::Scalar(Cow::Borrowed(scalar)),
            OpType::MarkBegin(_, mark) => {
                Value::Scalar(Cow::Owned(format!("markBegin={}", mark.value).into()))
            }
            OpType::MarkEnd(_) => Value::Scalar(Cow::Owned("markEnd".into())),
            _ => panic!("cant convert op into a value - {:?}", self),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn dump(&self) -> String {
        match &self.action {
            OpType::Put(value) if self.insert => format!("i:{}", value),
            OpType::Put(value) => format!("s:{}", value),
            OpType::Make(obj) => format!("make{}", obj),
            OpType::Increment(val) => format!("inc:{}", val),
            OpType::Delete => "del".to_string(),
            OpType::MarkBegin(_, _) => "markBegin".to_string(),
            OpType::MarkEnd(_) => "markEnd".to_string(),
        }
    }

    pub(crate) fn was_deleted_before(&self, clock: &Clock) -> bool {
        self.succ_iter().any(|i| clock.covers(i))
    }

    pub(crate) fn predates(&self, clock: &Clock) -> bool {
        clock.covers(&self.id)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct OpLinkIdx(u32);

/*
#[derive(Clone, Debug)]
pub(crate) struct OpLink {
    op: OpIdx,
    next: Option<OpLinkIdx>,
}
*/

#[derive(Clone, Debug)]
pub(crate) struct OpPlus {
    pub(crate) obj: ObjId,
    pub(crate) width: u32,
    //pub(crate) pred: Option<OpLinkIdx>,
    //pub(crate) succ: Option<OpLinkIdx>,
    pub(crate) op: OpBuilder,
}
