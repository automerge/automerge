use super::{Slab, SlabTree};
use crate::aggregate::Acc;
use crate::columndata::ColumnData;
use crate::cursor::ColumnCursor;
use crate::encoder::Writer;
use crate::leb128::{lebsize, ulebsize};
use crate::pack::Packable;
use crate::Cow;

use std::fmt::Debug;
use std::ops::Range;

#[derive(PartialEq)]
pub enum WriteOp<'a, P: Packable + ?Sized> {
    Value(Cow<'a, P>),
    Cpy(&'a [u8], Range<usize>, Acc, Option<bool>),
}

impl<P: Packable + ?Sized> Clone for WriteOp<'_, P> {
    fn clone(&self) -> Self {
        match self {
            Self::Value(c) => Self::Value(c.clone()),
            Self::Cpy(a, b, c, d) => Self::Cpy(a, b.clone(), *c, *d),
        }
    }
}

impl<P: Packable + ?Sized> Debug for WriteOp<'_, P> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        let mut s = fmt.debug_struct("WriteOp");
        match self {
            Self::Value(a) => s.field("value", a),
            Self::Cpy(_a, b, _c, _) => s.field("import", b),
        }
        .finish()
    }
}

#[derive(Debug, PartialEq)]
pub enum WriteAction<'a, P: Packable + ?Sized> {
    Op(WriteOp<'a, P>),
    LitHead(i64),
    BoolRun(u64, bool),
    Run(i64, Cow<'a, P>),
    NullRun(u64),
    Raw(Cow<'a, [u8]>),
    Slab(usize, Acc, i64, usize),
    SlabHead,
}

impl<P: Packable + ?Sized> Clone for WriteAction<'_, P> {
    fn clone(&self) -> Self {
        match self {
            Self::Op(op) => Self::Op(op.clone()),
            Self::Run(n, value) => Self::Run(*n, value.clone()),
            Self::Raw(bytes) => Self::Raw(bytes.clone()),
            Self::LitHead(n) => Self::LitHead(*n),
            Self::NullRun(n) => Self::NullRun(*n),
            Self::BoolRun(a, b) => Self::BoolRun(*a, *b),
            Self::Slab(a, b, c, d) => Self::Slab(*a, *b, *c, *d),
            Self::SlabHead => Self::SlabHead,
        }
    }
}

impl<P: Packable + ?Sized> WriteOp<'_, P> {
    fn acc(&self) -> Acc {
        match self {
            Self::Value(v) => P::agg(v) * 1,
            Self::Cpy(_, _, acc, _) => *acc,
        }
    }

    fn abs(&self) -> i64 {
        match self {
            Self::Value(v) => P::abs(v),
            _ => 0,
        }
    }

    fn width(&self) -> usize {
        match self {
            Self::Value(v) => P::width(v),
            Self::Cpy(_, r, _, _) => r.end - r.start,
        }
    }

    fn bool_value(&self) -> Option<bool> {
        match self {
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

    fn write_and_remap<'b, F>(self, buff: &mut Vec<u8>, f: &F)
    where
        F: Fn(&P) -> Option<&'b P>,
        P: 'b,
    {
        match self {
            Self::Value(value) => {
                let v = f(&value).unwrap_or(&value);
                P::pack(v, buff)
            }
            Self::Cpy(s, r, _, _) => {
                buff.extend_from_slice(&s[r]);
            }
        }
    }
    fn write(self, buff: &mut Vec<u8>) {
        //let start = buff.len();
        match self {
            Self::Value(v) => {
                P::pack(&v, buff)
                //println!("write value {} {:?}",v, &buff[start..]);
            }
            Self::Cpy(s, r, _, _) => {
                buff.extend_from_slice(&s[r]);
                //println!("write import ({:?} bytes)",buff[start..].len());
            }
        }
    }
}

impl<P: Packable + ?Sized> WriteAction<'_, P> {
    fn acc(&self) -> Acc {
        match self {
            Self::Op(op) => op.acc(),
            Self::BoolRun(c, b) if *b => Acc::from(*c),
            Self::Run(count, value) => P::agg(value) * *count as usize,
            _ => Acc::new(),
        }
    }

    fn abs(&self) -> i64 {
        match self {
            Self::Run(count, value) => P::abs(value) * count,
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
            Self::BoolRun(_, v) => Some(*v),
            _ => None,
        }
    }

    fn write_and_remap<'b, F>(self, buff: &mut Vec<u8>, f: &F)
    where
        F: Fn(&P) -> Option<&'b P>,
        P: 'b,
    {
        match self {
            Self::Op(op) => op.write_and_remap(buff, f),
            Self::Run(count, value) => {
                leb128::write::signed(buff, count).unwrap();
                let v = f(&value).unwrap_or(&value);
                P::pack(v, buff);
            }
            _ => self.write(buff),
        }
    }

    fn write(self, buff: &mut Vec<u8>) {
        match self {
            Self::Op(op) => op.write(buff),
            Self::LitHead(n) => {
                leb128::write::signed(buff, -n).unwrap();
            }
            Self::Raw(b) => buff.extend_from_slice(b.as_ref()),
            Self::BoolRun(i, _) => {
                leb128::write::unsigned(buff, i).unwrap();
            }
            Self::NullRun(count) => {
                buff.push(0);
                leb128::write::unsigned(buff, count).unwrap();
            }
            Self::Run(count, value) => {
                leb128::write::signed(buff, count).unwrap();
                P::pack(&value, buff);
                //op.write(buff);
            }
            Self::Slab(_, _, _, _) => {}
            Self::SlabHead => {}
        }
    }
}

#[derive(Debug)]
pub struct SlabWriter<'a, P: Packable + ?Sized> {
    actions: Vec<WriteAction<'a, P>>,
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
    locked: bool,
}

impl<P: Packable + ?Sized> Clone for SlabWriter<'_, P> {
    fn clone(&self) -> Self {
        Self {
            actions: self.actions.clone(),
            width: self.width,
            items: self.items,
            acc: self.acc,
            bools: self.bools,
            abs: self.abs,
            init_abs: self.init_abs,
            lit_items: self.lit_items,
            lit_head: self.lit_head,
            slab_head: self.slab_head,
            num_slabs: self.num_slabs,
            max: self.max,
            locked: self.locked,
        }
    }
}

impl<'a, P: Packable + ?Sized> Writer<'a, P> for SlabWriter<'a, P> {
    /*
      fn flush_null(&mut self, count: usize) {}
      fn flush_lit_run(&mut self, run: &[Cow<'_,P>]) {}
      fn flush_run(&mut self, count: i64, value: Cow<'_,P>) {}
      fn flush_bool_run(&mut self, count: usize, value: bool) {}
      fn flush_bytes(&mut self, bytes: Cow<'_,[u8]>) {}
    */

    fn flush_lit_run(&mut self, run: &[Cow<'a, P>]) {
        for value in run.iter() {
            self.push_lit(WriteOp::Value(value.clone()), 1, 1);
        }
    }

    fn flush_bool_run(&mut self, count: usize, value: bool) {
        let action = WriteAction::BoolRun(count as u64, value);
        let width = ulebsize(count as u64) as usize;
        self.push(action, count, width);
    }

    fn flush_run(&mut self, count: i64, value: Cow<'a, P>) {
        let value_width = P::width(&value);
        let width = lebsize(count) as usize + value_width;
        self.push(WriteAction::Run(count, value), count as usize, width);
    }

    fn flush_bytes(&mut self, data: Cow<'a, [u8]>) {
        let items = data.len();
        self.push(WriteAction::Raw(data), items, items);
    }

    fn flush_null(&mut self, count: usize) {
        let width = 1 + ulebsize(count as u64) as usize;
        self.push(WriteAction::NullRun(count as u64), count, width);
    }
}

impl<'a, P: Packable + ?Sized> SlabWriter<'a, P> {
    pub fn new(max: usize, locked: bool) -> Self {
        let actions = vec![WriteAction::SlabHead];
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
            locked,
            actions,
        }
    }

    pub fn set_init_abs(&mut self, abs: i64) {
        self.init_abs = abs;
    }

    pub fn is_locked(&self) -> bool {
        self.locked
    }

    pub fn unlock(&mut self) {
        self.locked = false;
    }

    pub fn set_abs(&mut self, abs: i64) {
        self.abs = abs;
    }

    fn push_lit(&mut self, op: WriteOp<'a, P>, lit: usize, items: usize) {
        let mut width = op.width();
        if width == 0 {
            return;
        }

        self.check_copy_overflow(op.copy_width());

        width += header_size(self.lit_items + lit) - header_size(self.lit_items);

        self.abs += op.abs();
        self.acc += op.acc();
        self.width += width;
        self.items += items;
        if self.lit_items == 0 && lit > 0 {
            self.lit_head = self.actions.len();
            self.actions.push(WriteAction::LitHead(0));
        }
        self.lit_items += lit;
        self.actions.push(WriteAction::Op(op));
        if items > lit {
            // copy contains non lit run elements at the end
            self.close_lit()
        }
        self.check_max();
    }

    fn push(&mut self, action: WriteAction<'a, P>, items: usize, width: usize) {
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
            let action = WriteAction::BoolRun(0, false);
            let width = 1;
            self.push(action, 0, width);
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
        if self.width >= self.max && !self.locked {
            self.close_lit();
            self.close_slab();
            self.width = 0;
            self.acc = Acc::new();
            self.bools = 0;
            self.items = 0;
        }
    }

    pub(crate) fn manual_slab_break(&mut self) {
        if self.width > 0 {
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
        if self.width + copy > self.max && self.width > 0 && !self.locked {
            self.close_lit();
            self.close_slab();
            self.width = 0;
            self.acc = Acc::new();
            self.bools = 0;
            self.items = 0;
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.actions.len() <= 1 // first action is slab_head
    }

    pub fn write(mut self, out: &mut Vec<u8>) {
        self.close_lit();
        for action in self.actions {
            action.write(out)
        }
    }

    pub fn write_and_remap<'b, F>(mut self, out: &mut Vec<u8>, f: F)
    where
        F: Fn(&P) -> Option<&'b P>,
        P: 'b,
    {
        self.close_lit();
        for action in self.actions {
            action.write_and_remap(out, &f)
        }
    }

    pub fn into_column<C: ColumnCursor>(self, length: usize) -> ColumnData<C> {
        let mut slabs = self.finish();
        C::compute_min_max(&mut slabs);
        ColumnData::init(length, SlabTree::load(slabs))
    }

    pub fn finish(mut self) -> Vec<Slab> {
        self.close_lit();
        if self.items > 0 {
            self.close_slab();
        }
        if self.num_slabs == 0 {
            return vec![];
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
                        debug_assert_eq!(width, buffer.len());
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
                action => {
                    action.write(&mut buffer);
                }
            }
        }
        debug_assert_eq!(width, buffer.len());
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
}

fn header_size(lit: usize) -> usize {
    //lit == 0 || lit == 64 || lit == 8192
    if lit == 0 {
        0
    } else {
        lebsize(-(lit as i64)) as usize
    }
}
