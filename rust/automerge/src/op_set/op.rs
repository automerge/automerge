use crate::clock::Clock;
use crate::exid::ExId;
use crate::op_set::OpSetData;
use crate::types::{self, ActorId, ElemId, Key, ListEncoding, ObjId, OpId, OpType, Prop};
use crate::value::{ScalarValue, Value};
use std::borrow::Cow;
use std::cmp::Ordering;

#[derive(Copy, Clone, Debug, PartialEq)]
pub(crate) struct OpIdx(u32);

impl OpIdx {
    pub(crate) fn new(index: usize) -> Self {
        Self(index as u32)
    }

    pub(crate) fn get(&self) -> usize {
        self.0 as usize
    }

    pub(crate) fn as_op(self, osd: &OpSetData) -> Op<'_> {
        Op {
            idx: self.get(),
            osd,
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct Op<'a> {
    idx: usize,
    osd: &'a OpSetData,
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct OpDep<'a> {
    idx: usize,
    osd: &'a OpSetData,
}

impl<'a> OpDep<'a> {
    fn raw(&self) -> &OpDepRaw {
        &self.osd.op_deps[self.idx]
    }

    /*
        pub(crate) fn idx(&self) -> OpDepIdx {
            OpDepIdx::new(self.idx)
        }
    */

    pub(crate) fn pred(&self) -> Op<'a> {
        self.raw().pred.as_op(self.osd)
    }

    pub(crate) fn succ(&self) -> Op<'a> {
        self.raw().succ.as_op(self.osd)
    }

    /*
        pub(crate) fn next_pred(&self) -> Option<OpDep<'a>> {
            self.raw()
                .next_pred
                .as_ref()
                .map(|next| next.as_opdep(self.osd))
        }

        pub(crate) fn next_succ(&self) -> Option<OpDep<'a>> {
            self.raw()
                .next_succ
                .as_ref()
                .map(|next| next.as_opdep(self.osd))
        }

        pub(crate) fn last_pred(&self) -> Option<OpDep<'a>> {
            self.raw()
                .last_pred
                .as_ref()
                .map(|next| next.as_opdep(self.osd))
        }

        pub(crate) fn last_succ(&self) -> Option<OpDep<'a>> {
            self.raw()
                .last_succ
                .as_ref()
                .map(|next| next.as_opdep(self.osd))
        }
    */
}

impl<'a> PartialEq for Op<'a> {
    fn eq(&self, other: &Self) -> bool {
        (std::ptr::eq(self.osd, other.osd) && self.idx == other.idx) || self.op() == other.op()
    }
}

impl<'a> Eq for Op<'a> {}

impl<'a> PartialOrd for Op<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for Op<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        let c1 = self.id().counter();
        let c2 = other.id().counter();
        c1.cmp(&c2).then_with(|| self.actor().cmp(other.actor()))
    }
}

impl<'a> Op<'a> {
    pub(crate) fn actor(&self) -> &'a ActorId {
        &self.osd.actors[self.op().id.actor()]
    }

    pub(crate) fn succ_iter(&self) -> impl Iterator<Item = Op<'a>> {
        self.succ().filter(|op| !op.is_inc())
    }

    pub(crate) fn osd(&self) -> &'a OpSetData {
        self.osd
    }

    pub(crate) fn obj(&self) -> &'a ObjId {
        &self.raw().obj
    }

    fn raw(&self) -> &'a OpRaw {
        &self.osd.ops[self.idx]
    }

    pub(crate) fn idx(&self) -> OpIdx {
        OpIdx::new(self.idx)
    }

    fn op(&self) -> &'a OpBuilder {
        &self.osd.ops[self.idx].op
    }

    pub(crate) fn map_prop(&self) -> Option<Prop> {
        if let Key::Map(m) = &self.op().key {
            Some(Prop::Map(String::from(self.osd.props.safe_get(*m)?)))
        } else {
            None
        }
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

    pub(crate) fn is_inc(&self) -> bool {
        matches!(&self.op().action, OpType::Increment(_))
    }

    pub(crate) fn visible_at(&self, clock: Option<&Clock>) -> bool {
        if let Some(clock) = clock {
            if self.is_inc() || self.is_mark() {
                false
            } else {
                clock.covers(&self.op().id) && !self.succ().any(|i| clock.covers(i.id()))
            }
        } else {
            self.visible()
        }
    }

    pub(crate) fn visible_or_mark(&self, clock: Option<&Clock>) -> bool {
        if self.is_inc() {
            false
        } else if let Some(clock) = clock {
            clock.covers(&self.op().id) && self.succ().all(|o| o.is_inc() || !clock.covers(o.id()))
        } else if self.is_counter() {
            self.succ().all(|op| op.is_inc())
        } else {
            self.succ().len() == 0
        }
    }

    pub(crate) fn visible(&self) -> bool {
        if self.is_inc() || self.is_mark() {
            false
        } else if self.is_counter() {
            self.succ().all(|op| op.is_inc())
        } else {
            self.succ().len() == 0
        }
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
            self.raw().width as usize
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

    pub(crate) fn inc_at(&self, clock: &Clock) -> i64 {
        self.succ()
            .filter_map(|o| {
                if clock.covers(o.id()) {
                    o.op().get_increment_value()
                } else {
                    None
                }
            })
            .sum()
    }

    pub(crate) fn value_at(&self, clock: Option<&Clock>) -> Value<'a> {
        if let Some(clock) = clock {
            if let OpType::Put(ScalarValue::Counter(c)) = &self.op().action {
                return Value::counter(c.start + self.inc_at(clock));
            }
        }
        self.value()
    }

    pub(crate) fn tagged_value(&self, clock: Option<&Clock>) -> (Value<'a>, ExId) {
        (self.value_at(clock), self.exid())
    }

    pub(crate) fn predates(&self, clock: &Clock) -> bool {
        self.op().predates(clock)
    }

    pub(crate) fn was_deleted_before(&self, clock: &Clock) -> bool {
        self.succ_iter().any(|op| clock.covers(op.id()))
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

    pub(crate) fn succ_idx(&self) -> SuccIdxIter<'a> {
        SuccIdxIter {
            next: self.raw().succ,
            len: self.raw().succ_len as usize,
            offset: 0,
            osd: self.osd,
        }
    }

    pub(crate) fn succ(&self) -> impl ExactSizeIterator<Item = Op<'a>> {
        self.succ_idx().map(|idx| idx.as_opdep(self.osd).succ())
    }

    pub(crate) fn pred_idx(&self) -> PredIdxIter<'a> {
        PredIdxIter {
            next: self.raw().pred,
            len: self.raw().pred_len as usize,
            offset: 0,
            osd: self.osd,
        }
    }

    pub(crate) fn pred(&self) -> impl ExactSizeIterator<Item = Op<'a>> {
        self.pred_idx().map(|idx| idx.as_opdep(self.osd).pred())
    }

    pub(crate) fn block_id(&self) -> Option<OpId> {
        if self.action().is_block() {
            if self.insert() {
                return Some(*self.id());
            } else if let Key::Seq(ElemId(id)) = self.key() {
                return Some(*id);
            }
        }
        None
    }

    pub(crate) fn visible_block(&self) -> Option<OpId> {
        if self.visible() {
            self.block_id()
        } else {
            None
        }
    }

    pub(crate) fn is_put(&self) -> bool {
        matches!(&self.action(), OpType::Put(_))
    }
}

pub(crate) struct PredIdxIter<'a> {
    next: Option<OpDepIdx>,
    len: usize,
    offset: usize,
    osd: &'a OpSetData,
}

pub(crate) struct SuccIdxIter<'a> {
    next: Option<OpDepIdx>,
    len: usize,
    offset: usize,
    osd: &'a OpSetData,
}

impl<'a> Iterator for PredIdxIter<'a> {
    type Item = OpDepIdx;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(idx) = self.next {
            self.next = self.osd.op_deps[idx.get()].next_pred;
            self.offset += 1;
            assert!(self.offset <= self.len);
            Some(idx)
        } else {
            None
        }
    }
}

impl<'a> ExactSizeIterator for PredIdxIter<'a> {
    fn len(&self) -> usize {
        self.len - self.offset
    }
}

impl<'a> ExactSizeIterator for SuccIdxIter<'a> {
    fn len(&self) -> usize {
        self.len - self.offset
    }
}

impl<'a> Iterator for SuccIdxIter<'a> {
    type Item = OpDepIdx;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(idx) = self.next {
            self.next = self.osd.op_deps[idx.get()].next_succ;
            self.offset += 1;
            assert!(self.offset <= self.len);
            Some(idx)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OpBuilder {
    pub(crate) id: OpId,
    pub(crate) action: OpType,
    pub(crate) key: Key,
    pub(crate) insert: bool,
}

impl OpBuilder {
    pub(crate) fn increment(&mut self, n: i64) {
        if let OpType::Put(ScalarValue::Counter(c)) = &mut self.action {
            c.current += n;
        }
    }

    pub(crate) fn to_str(&self) -> &str {
        self.action.to_str()
    }

    pub(crate) fn is_delete(&self) -> bool {
        matches!(&self.action, OpType::Delete)
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

    /*
        pub(crate) fn was_deleted_before(&self, clock: &Clock) -> bool {
            self.succ_iter().any(|i| clock.covers(i))
        }
    */

    pub(crate) fn predates(&self, clock: &Clock) -> bool {
        clock.covers(&self.id)
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct OpDepIdx(u32);

impl OpDepIdx {
    pub(crate) fn new(index: usize) -> Self {
        Self(index as u32)
    }

    pub(crate) fn get(&self) -> usize {
        self.0 as usize
    }

    pub(crate) fn as_opdep(self, osd: &OpSetData) -> OpDep<'_> {
        OpDep {
            idx: self.get(),
            osd,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct OpDepRaw {
    pub(crate) pred: OpIdx,
    pub(crate) succ: OpIdx,
    pub(crate) next_pred: Option<OpDepIdx>,
    pub(crate) next_succ: Option<OpDepIdx>,
    pub(crate) last_pred: Option<OpDepIdx>,
    pub(crate) last_succ: Option<OpDepIdx>,
}

impl OpDepRaw {
    pub(crate) fn new(pred: OpIdx, succ: OpIdx) -> Self {
        Self {
            pred,
            succ,
            next_pred: None,
            next_succ: None,
            last_pred: None,
            last_succ: None,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct OpRaw {
    pub(crate) obj: ObjId,
    pub(crate) width: u32,
    pub(crate) pred_len: u32,
    pub(crate) succ_len: u32,
    pub(crate) pred: Option<OpDepIdx>,
    pub(crate) succ: Option<OpDepIdx>,
    pub(crate) op: OpBuilder,
}
