use super::Slab;
use crate::aggregate::{Acc, Agg};
use crate::leb128::{lebsize, ulebsize};
use crate::pack::Packable;
use crate::Cow;

use std::fmt::Debug;
use std::ops::Range;

#[derive(Clone, PartialEq)]
pub enum WriteOp<'a> {
    LitHead(i64),
    UInt(u64),
    UIntAcc(u64, Agg),
    BoolRun(u64, bool),
    Int(i64),
    Bytes(Cow<'a, [u8]>),
    Raw(Cow<'a, [u8]>),
    Cpy(&'a [u8], Range<usize>, Acc, Option<bool>),
}

impl<'a> From<i64> for WriteOp<'a> {
    fn from(n: i64) -> WriteOp<'static> {
        WriteOp::Int(n)
    }
}

impl<'a> From<u64> for WriteOp<'a> {
    fn from(n: u64) -> WriteOp<'static> {
        WriteOp::UIntAcc(n, Agg::from(n))
    }
}

impl<'a> From<u32> for WriteOp<'a> {
    fn from(n: u32) -> WriteOp<'static> {
        WriteOp::UIntAcc(n as u64, Agg::from(n))
    }
}

impl<'a> From<usize> for WriteOp<'a> {
    fn from(n: usize) -> WriteOp<'static> {
        WriteOp::UIntAcc(n as u64, Agg::from(n))
    }
}

impl<'a> From<Cow<'a, str>> for WriteOp<'a> {
    fn from(s: Cow<'a, str>) -> WriteOp<'a> {
        match s {
            Cow::Owned(s) => WriteOp::Bytes(Cow::from(s.into_bytes())),
            Cow::Borrowed(s) => WriteOp::Bytes(Cow::from(s.as_bytes())),
        }
    }
}

/*
impl<'a,T: Packable> From<Cow<'a,T>> for WriteOp<'a> {
    fn from(s: Cow<'a,T>) -> WriteOp<'a> {
        match s {
          Cow::Owned(s) => WriteOp::Bytes(Cow::from(s.into_bytes())),
          Cow::Borrowed(s) => WriteOp::Bytes(Cow::from(s.as_bytes()))
        }
    }
}
*/

impl<'a> From<Cow<'a, [u8]>> for WriteOp<'a> {
    fn from(bytes: Cow<'a, [u8]>) -> WriteOp<'a> {
        WriteOp::Bytes(bytes)
    }
}

impl<'a> From<bool> for WriteOp<'a> {
    fn from(_bool: bool) -> WriteOp<'a> {
        panic!()
    }
}

impl<'a> Debug for WriteOp<'a> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        let mut s = fmt.debug_struct("WriteOp");
        match self {
            Self::UInt(a) => s.field("uint", a),
            Self::UIntAcc(a, b) => s.field("acc_uint", a).field("acc", b),
            Self::BoolRun(a, b) => s.field("bool_run", a).field("bool", b),
            Self::Int(a) => s.field("int", a),
            Self::LitHead(a) => s.field("lit_head", a),
            Self::Bytes(a) => s.field("bytes", &a.len()),
            Self::Raw(a) => s.field("raw", &a.len()),
            Self::Cpy(_a, b, _c, _) => s.field("import", b),
        }
        .finish()
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum WriteAction<'a> {
    Op(WriteOp<'a>),
    Pair(i64, WriteOp<'a>),
    Slab(usize, Acc, i64, usize),
    SlabHead,
}

impl<'a> WriteOp<'a> {
    fn acc(&self) -> Acc {
        match self {
            Self::Cpy(_, _, acc, _) => *acc,
            Self::UIntAcc(_, agg) => *agg * 1,
            Self::BoolRun(c, b) if *b => Acc::from(*c),
            _ => Acc::new(),
        }
    }

    fn agg(&self) -> Agg {
        match self {
            Self::UIntAcc(_, agg) => *agg,
            _ => Agg::default(),
        }
    }

    fn abs(&self) -> i64 {
        match self {
            Self::Int(i) => *i,
            _ => 0,
        }
    }

    fn width(&self) -> usize {
        match self {
            Self::UInt(i) => ulebsize(*i) as usize,
            Self::UIntAcc(i, _) => ulebsize(*i) as usize,
            Self::BoolRun(i, _) => ulebsize(*i) as usize,
            Self::Int(i) => lebsize(*i) as usize,
            Self::LitHead(i) => lebsize(*i) as usize,
            Self::Bytes(b) => ulebsize(b.len() as u64) as usize + b.len(),
            Self::Raw(b) => b.len(),
            Self::Cpy(_, r, _, _) => r.end - r.start,
        }
    }

    fn bool_value(&self) -> Option<bool> {
        match self {
            Self::BoolRun(_, v) => Some(*v),
            Self::Cpy(_, _, _, v) => *v,
            _ => None,
        }
    }

    fn copy_width(&self) -> usize {
        match self {
            Self::Cpy(_, _, _, _) => self.width(),
            _ => 0,
        }
    }

    fn write(self, buff: &mut Vec<u8>) {
        //let start = buff.len();
        match self {
            Self::UInt(i) => {
                leb128::write::unsigned(buff, i).unwrap();
                //println!("write uint {} {:?}",i, &buff[start..]);
            }
            Self::UIntAcc(i, _) => {
                leb128::write::unsigned(buff, i).unwrap();
                //println!("write acc uint {} {:?}",i, &buff[start..]);
            }
            Self::BoolRun(i, _) => {
                leb128::write::unsigned(buff, i).unwrap();
                //println!("write acc uint {} {:?}",i, &buff[start..]);
            }
            Self::Int(i) => {
                leb128::write::signed(buff, i).unwrap();
                //println!("write int {} {:?}",i, &buff[start..]);
            }
            Self::Bytes(b) => {
                leb128::write::unsigned(buff, b.len() as u64).unwrap();
                buff.extend_from_slice(&b);
                //println!("write bytes {:?}",&buff[start..]);
            }
            Self::Raw(b) => {
                buff.extend_from_slice(b.as_ref());
                //println!("write raw {:?}", &buff[start..]);
            }
            Self::LitHead(n) => {
                leb128::write::signed(buff, -n).unwrap();
            }
            Self::Cpy(s, r, _, _) => {
                buff.extend_from_slice(&s[r]);
                //println!("write import ({:?} bytes)",buff[start..].len());
            }
        }
    }
}

impl<'a> WriteAction<'a> {
    fn lithead(i: i64) -> Self {
        WriteAction::Op(WriteOp::LitHead(i))
    }

    fn acc(&self) -> Acc {
        match self {
            Self::Op(op) => op.acc(),
            Self::Pair(count, op) => op.agg() * *count as usize,
            _ => Acc::new(),
        }
    }

    fn abs(&self) -> i64 {
        match self {
            Self::Pair(count, WriteOp::Int(value)) => count * value,
            _ => 0,
        }
    }

    /*
        fn width(&self) -> usize {
            match self {
                Self::Op(op) => op.width(),
                Self::Pair(op1, op2) => op1.width() + op2.width(),
                _ => 0,
            }
        }
    */

    fn copy_width(&self) -> usize {
        match self {
            Self::Op(op) => op.copy_width(),
            _ => 0,
        }
    }

    fn bool_value(&self) -> Option<bool> {
        match self {
            Self::Op(op) => op.bool_value(),
            _ => None,
        }
    }

    fn write(self, buff: &mut Vec<u8>) {
        match self {
            Self::Op(op) => op.write(buff),
            Self::Pair(count, op2) => {
                leb128::write::signed(buff, count).unwrap();
                op2.write(buff)
            }
            Self::Slab(_, _, _, _) => {}
            Self::SlabHead => {}
        }
    }
}

#[derive(Debug, Clone)]
pub struct SlabWriter<'a> {
    actions: Vec<WriteAction<'a>>,
    width: usize,
    items: usize,
    acc: Acc,
    bools: u64,
    abs: i64,
    init_abs: i64,
    lit_items: usize,
    lit_head: usize,
    slab_head: usize,
    num_slabs: usize,
    max: usize,
}

impl<'a> Default for SlabWriter<'a> {
    fn default() -> Self {
        Self::new(usize::MAX, 0)
    }
}

impl<'a> SlabWriter<'a> {
    pub fn new(max: usize, cap: usize) -> Self {
        let mut actions = Vec::with_capacity(cap);
        actions.push(WriteAction::SlabHead);
        SlabWriter {
            max,
            width: 0,
            acc: Acc::new(),
            abs: 0,
            init_abs: 0,
            bools: 0,
            lit_items: 0,
            lit_head: 0,
            slab_head: 0,
            num_slabs: 0,
            items: 0,
            actions,
        }
    }

    pub fn set_init_abs(&mut self, abs: i64) {
        self.init_abs = abs;
    }

    pub fn set_abs(&mut self, abs: i64) {
        self.abs = abs;
    }

    fn push_lit(&mut self, op: WriteOp<'a>, lit: usize, items: usize) {
        let mut width = op.width();
        if width == 0 {
            return;
        }
        if self.lit_items == 0 {
            // this is the first item for the lit run
            // add the width of the lit run header
            //
            // we could make another entry here for self.lit_items == 127, etc
            // for complete correctness
            //
            width += 1;
        }
        self.check_copy_overflow(op.copy_width());
        self.abs += op.abs();
        self.acc += op.acc();
        self.width += width;
        self.items += items;
        if self.lit_items == 0 && lit > 0 {
            self.lit_head = self.actions.len();
            self.actions.push(WriteAction::lithead(0));
        }
        self.lit_items += lit;
        self.actions.push(WriteAction::Op(op));
        if items > lit {
            // copy contains non lit run elements at the end
            self.close_lit()
        }
        self.check_max();
    }

    fn push(&mut self, action: WriteAction<'a>, items: usize, width: usize) {
        //assert_eq!(width, action.width());
        if width == 0 {
            return;
        }
        self.check_copy_overflow(action.copy_width());
        self.check_bool_state(action.bool_value());
        self.abs += action.abs();
        self.acc += action.acc();
        self.width += width;
        self.items += items;
        self.close_lit();
        self.actions.push(action);
        self.check_max();
    }

    fn check_bool_state(&mut self, val: Option<bool>) {
        // we cant count items b/c we might
        // have copied over a zero run of false's
        if self.width == 0 && val == Some(true) {
            let op = WriteOp::BoolRun(0, false);
            let width = op.width();
            self.push(WriteAction::Op(op), 0, width);
        }
    }

    fn close_lit(&mut self) {
        if self.lit_items > 0 {
            assert!(self.lit_items > 0);
            assert_eq!(
                self.actions.get(self.lit_head),
                Some(&WriteAction::lithead(0))
            );
            self.actions[self.lit_head] = WriteAction::lithead(self.lit_items as i64);

            self.lit_items = 0;
        }
    }

    fn check_max(&mut self) {
        if self.width >= self.max {
            self.close_lit();
            self.close_slab();
            self.width = 0;
            self.acc = Acc::new();
            self.bools = 0;
            self.items = 0;
        }
    }

    fn close_slab(&mut self) {
        assert_eq!(
            self.actions.get(self.slab_head),
            Some(&WriteAction::SlabHead)
        );
        self.actions[self.slab_head] =
            WriteAction::Slab(self.items, self.acc, self.abs, self.width);
        self.num_slabs += 1;
        self.slab_head = self.actions.len();
        self.actions.push(WriteAction::SlabHead);
    }

    fn check_copy_overflow(&mut self, copy: usize) {
        if self.width + copy > self.max && self.width > 0 {
            self.close_lit();
            self.close_slab();
            self.width = 0;
            self.acc = Acc::new();
            self.bools = 0;
            self.items = 0;
        }
    }

    pub fn write(mut self, out: &mut Vec<u8>) {
        self.close_lit();
        for action in self.actions {
            action.write(out)
        }
    }

    pub fn finish(mut self) -> Vec<Slab> {
        self.close_lit();
        if self.items > 0 {
            self.close_slab();
        }
        self.actions.pop();
        let mut result = Vec::with_capacity(self.num_slabs);
        let mut buffer = vec![];
        let mut len = 0;
        let mut acc = Acc::new();
        let mut abs = self.init_abs;
        let mut next_abs = 0;
        let mut width = 0;
        for action in self.actions {
            match action {
                WriteAction::Slab(next_len, next_acc, next_next_abs, next_width) => {
                    if !buffer.is_empty() {
                        assert_eq!(width, buffer.len());
                        let data = std::mem::take(&mut buffer);
                        result.push(Slab::new(data, len, acc, abs));
                        abs = next_abs;
                    }
                    buffer = Vec::with_capacity(next_width);
                    acc = next_acc;
                    len = next_len;
                    width = next_width;
                    next_abs = next_next_abs;
                }
                action => action.write(&mut buffer),
            }
        }
        result.push(Slab::new(buffer, len, acc, abs));
        assert_eq!(self.num_slabs, result.len());
        result
    }

    pub fn copy(
        &mut self,
        slab: &'a [u8],
        range: Range<usize>,
        lit: usize,
        size: usize,
        acc: Acc,
        bool_state: Option<bool>,
    ) {
        if !range.is_empty() {
            let op = WriteOp::Cpy(slab, range, acc, bool_state);
            if lit > 0 {
                self.push_lit(op, lit, size)
            } else {
                let width = op.width();
                self.push(WriteAction::Op(op), size, width)
            }
        }
    }

    pub fn flush_lit_run<P: Packable + ?Sized>(&mut self, run: &[Cow<'a, P>]) {
        for value in run.iter() {
            self.push_lit(P::pack(value.clone()), 1, 1);
        }
    }

    pub fn flush_bool_run(&mut self, count: usize, value: bool) {
        let op = WriteOp::BoolRun(count as u64, value);
        let width = op.width();
        self.push(WriteAction::Op(op), count, width);
    }

    pub fn flush_run<P: Packable + ?Sized>(&mut self, count: i64, value: Cow<'a, P>) {
        //let value_op = value.into();
        let value_op = P::pack(value);
        let width = lebsize(count) as usize + value_op.width();
        self.push(WriteAction::Pair(count, value_op), count as usize, width);
    }

    pub fn flush_bytes(&mut self, data: Cow<'a, [u8]>) {
        let items = data.len();
        self.push(WriteAction::Op(WriteOp::Raw(data)), items, items);
    }

    pub fn flush_null(&mut self, count: usize) {
        //let null_op = WriteOp::Int(0);
        let count_op = WriteOp::UInt(count as u64);
        let width = 1 + count_op.width();
        self.push(WriteAction::Pair(0, count_op), count, width);
    }
}
