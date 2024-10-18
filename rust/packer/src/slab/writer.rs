use super::Slab;
use crate::leb128::{lebsize, ulebsize};

use std::fmt::Debug;
use std::ops::Range;

#[derive(Clone, PartialEq)]
pub enum WriteOp<'a> {
    UInt(u64),
    GroupUInt(u64, usize),
    BoolRun(u64, bool),
    Int(i64),
    Bytes(&'a [u8]),
    Import(&'a Slab, Range<usize>, usize, Option<bool>),
}

impl<'a> From<i64> for WriteOp<'a> {
    fn from(n: i64) -> WriteOp<'static> {
        WriteOp::Int(n)
    }
}

impl<'a> From<u64> for WriteOp<'a> {
    fn from(n: u64) -> WriteOp<'static> {
        WriteOp::GroupUInt(n, n as usize)
    }
}

impl<'a> From<u32> for WriteOp<'a> {
    fn from(n: u32) -> WriteOp<'static> {
        WriteOp::GroupUInt(n as u64, n as usize)
    }
}

impl<'a> From<usize> for WriteOp<'a> {
    fn from(n: usize) -> WriteOp<'static> {
        WriteOp::GroupUInt(n as u64, n)
    }
}

impl<'a> From<&'a str> for WriteOp<'a> {
    fn from(s: &'a str) -> WriteOp<'a> {
        WriteOp::Bytes(s.as_bytes())
    }
}

impl<'a> From<&'a [u8]> for WriteOp<'a> {
    fn from(bytes: &'a [u8]) -> WriteOp<'a> {
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
            Self::GroupUInt(a, b) => s.field("group_uint", a).field("group", b),
            Self::BoolRun(a, b) => s.field("bool_run", a).field("bool", b),
            Self::Int(a) => s.field("int", a),
            Self::Bytes(a) => s.field("bytes", &a.len()),
            Self::Import(_a, b, _c, _) => s.field("import", b),
        }
        .finish()
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum WriteAction<'a> {
    Op(WriteOp<'a>),
    LitHead(i64),
    Lit(WriteOp<'a>),
    Pair(WriteOp<'a>, WriteOp<'a>),
    Raw(&'a [u8]),
    Slab(usize, usize, i64, usize),
    SlabHead,
}

impl<'a> WriteOp<'a> {
    fn group(&self) -> usize {
        match self {
            Self::Import(_, _, g, _) => *g,
            Self::GroupUInt(_, g) => *g,
            Self::BoolRun(c, b) if *b => *c as usize,
            _ => 0,
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
            Self::GroupUInt(i, _) => ulebsize(*i) as usize,
            Self::BoolRun(i, _) => ulebsize(*i) as usize,
            Self::Int(i) => lebsize(*i) as usize,
            Self::Bytes(b) => ulebsize(b.len() as u64) as usize + b.len(),
            Self::Import(_, r, _, _) => r.end - r.start,
        }
    }

    fn bool_value(&self) -> Option<bool> {
        match self {
            Self::BoolRun(_, v) => Some(*v),
            Self::Import(_, _, _, v) => *v,
            _ => None,
        }
    }

    fn copy_width(&self) -> usize {
        match self {
            Self::Import(_, _, _, _) => self.width(),
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
            Self::GroupUInt(i, _) => {
                leb128::write::unsigned(buff, i).unwrap();
                //println!("write group uint {} {:?}",i, &buff[start..]);
            }
            Self::BoolRun(i, _) => {
                leb128::write::unsigned(buff, i).unwrap();
                //println!("write group uint {} {:?}",i, &buff[start..]);
            }
            Self::Int(i) => {
                leb128::write::signed(buff, i).unwrap();
                //println!("write int {} {:?}",i, &buff[start..]);
            }
            Self::Bytes(b) => {
                leb128::write::unsigned(buff, b.len() as u64).unwrap();
                buff.extend(b);
                //println!("write bytes {:?}",&buff[start..]);
            }
            Self::Import(s, r, _, _) => {
                buff.extend(&s[r]);
                //println!("write import ({:?} bytes)",buff[start..].len());
            }
        }
    }
}

impl<'a> WriteAction<'a> {
    fn group(&self) -> usize {
        match self {
            Self::Op(op) => op.group(),
            Self::Pair(WriteOp::Int(count), op) => *count as usize * op.group(),
            //Self::Raw(_) => 0,
            _ => 0,
        }
    }

    fn abs(&self) -> i64 {
        match self {
            Self::Pair(WriteOp::Int(count), WriteOp::Int(value)) => count * value,
            //Self::Lit(_) => 0,
            _ => 0,
        }
    }

    // FIXME dont need this
    fn width(&self) -> usize {
        match self {
            Self::Op(op) => op.width(),
            Self::Pair(count, op) => count.width() + op.width(),
            Self::Raw(data) => data.len(),
            _ => 0,
        }
    }

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
        //let start = buff.len();
        match self {
            Self::Op(op) => op.write(buff),
            Self::Pair(op1, op2) => {
                op1.write(buff);
                op2.write(buff)
            }
            Self::Raw(b) => {
                buff.extend(b);
                //println!("write raw {:?}", &buff[start..]);
            }
            Self::LitHead(n) => {
                leb128::write::signed(buff, -n).unwrap();
            }
            Self::Lit(op) => op.write(buff),
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
    group: usize,
    bools: u64,
    abs: i64,
    init_abs: i64,
    lit_items: usize,
    lit_head: usize,
    slab_head: usize,
    num_slabs: usize,
    max: usize,
}

impl<'a> SlabWriter<'a> {
    pub fn new(max: usize, cap: usize) -> Self {
        let mut actions = Vec::with_capacity(cap);
        actions.push(WriteAction::SlabHead);
        SlabWriter {
            max,
            width: 0,
            group: 0,
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
        self.group += op.group();
        self.width += width;
        self.items += items;
        if self.lit_items == 0 && lit > 0 {
            self.lit_head = self.actions.len();
            self.actions.push(WriteAction::LitHead(0));
        }
        self.lit_items += lit;
        self.actions.push(WriteAction::Lit(op));
        if items > lit {
            // copy contains non lit run elements at the end
            self.close_lit()
        }
        self.check_max();
    }

    fn push(&mut self, action: WriteAction<'a>, items: usize, width: usize) {
        assert_eq!(width, action.width());
        if width == 0 {
            return;
        }
        self.check_copy_overflow(action.copy_width());
        self.check_bool_state(action.bool_value());
        self.abs += action.abs();
        self.group += action.group();
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
                Some(&WriteAction::LitHead(0))
            );
            self.actions[self.lit_head] = WriteAction::LitHead(self.lit_items as i64);

            self.lit_items = 0;
        }
    }

    fn check_max(&mut self) {
        if self.width >= self.max {
            self.close_lit();
            self.close_slab();
            self.width = 0;
            self.group = 0;
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
            WriteAction::Slab(self.items, self.group, self.abs, self.width);
        self.num_slabs += 1;
        self.slab_head = self.actions.len();
        self.actions.push(WriteAction::SlabHead);
    }

    fn check_copy_overflow(&mut self, copy: usize) {
        if self.width + copy > self.max && self.width > 0 {
            self.close_lit();
            self.close_slab();
            self.width = 0;
            self.group = 0;
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
        let mut group = 0;
        let mut abs = self.init_abs;
        let mut next_abs = 0;
        let mut width = 0;
        for action in self.actions {
            match action {
                WriteAction::Slab(next_len, next_group, next_next_abs, next_width) => {
                    if !buffer.is_empty() {
                        assert_eq!(width, buffer.len());
                        let data = std::mem::take(&mut buffer);
                        result.push(Slab::new(data, len, group, abs));
                        abs = next_abs;
                    }
                    buffer = Vec::with_capacity(next_width);
                    group = next_group;
                    len = next_len;
                    width = next_width;
                    next_abs = next_next_abs;
                }
                action => action.write(&mut buffer),
            }
        }
        result.push(Slab::new(buffer, len, group, abs));
        assert_eq!(self.num_slabs, result.len());
        result
    }

    // TODO:
    // only difference with this vs flush_before is doing nothing when size == 0
    // skipping this on size zero is needed on write/merge operations
    // but being able to write something with size == 0 is needed for the first element of
    // boolean sets - likely these 2 and flush_after could all get turned into one nice method
    pub fn flush_before2(
        &mut self,
        slab: &'a Slab,
        range: Range<usize>,
        lit: usize,
        size: usize,
        group: usize,
    ) {
        if size > 0 {
            let op = WriteOp::Import(slab, range, group, None);
            if lit > 0 {
                self.push_lit(op, lit, size)
            } else {
                let width = op.width();
                self.push(WriteAction::Op(op), size, width)
            }
        }
    }

    pub fn flush_before(
        &mut self,
        slab: &'a Slab,
        range: Range<usize>,
        lit: usize,
        size: usize,
        group: usize,
    ) {
        let op = WriteOp::Import(slab, range, group, None);
        if lit > 0 {
            self.push_lit(op, lit, size)
        } else {
            let width = op.width();
            self.push(WriteAction::Op(op), size, width)
        }
    }

    pub fn flush_after(
        &mut self,
        slab: &'a Slab,
        index: usize,
        lit: usize,
        size: usize,
        group: usize,
        bool_state: Option<bool>,
    ) {
        let range = index..slab.byte_len();
        let op = WriteOp::Import(slab, range, group, bool_state);
        if lit > 0 {
            self.push_lit(op, lit, size)
        } else {
            let width = op.width();
            self.push(WriteAction::Op(op), size, width)
        }
    }

    pub fn flush_lit_run<W: Debug + Copy + Into<WriteOp<'a>>>(&mut self, run: &[W]) {
        for value in run.iter() {
            self.push_lit((*value).into(), 1, 1);
        }
    }

    pub fn flush_bool_run(&mut self, count: usize, value: bool) {
        let op = WriteOp::BoolRun(count as u64, value);
        let width = op.width();
        self.push(WriteAction::Op(op), count, width);
    }

    pub fn flush_run<W: Debug + Into<WriteOp<'a>>>(&mut self, count: i64, value: W) {
        let value_op = value.into();
        let count_op = WriteOp::Int(count);
        let width = count_op.width() + value_op.width();
        self.push(WriteAction::Pair(count_op, value_op), count as usize, width);
    }

    pub fn flush_bytes(&mut self, data: &'a [u8], count: usize) {
        self.push(WriteAction::Raw(data), count, data.len());
    }

    pub fn flush_null(&mut self, count: usize) {
        let null_op = WriteOp::Int(0);
        let count_op = WriteOp::UInt(count as u64);
        let width = null_op.width() + count_op.width();
        self.push(WriteAction::Pair(null_op, count_op), count, width);
    }
}
