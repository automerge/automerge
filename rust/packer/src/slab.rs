use super::cursor::{ColumnCursor, RunIter, ScanMeta};
use super::leb128::{lebsize, ulebsize};
use super::pack::PackError;
use std::ops::{Add, AddAssign, Sub, SubAssign};

pub(crate) mod tree;

pub(crate) use super::columndata::normalize_range;
pub(crate) use tree::{HasWeight, SpanTree, SpanTreeIter};

pub type SlabTree = SpanTree<Slab>;
pub(crate) type Iter<'a> = SpanTreeIter<'a, Slab>;

use std::fmt::Debug;
use std::ops::{Index, Range};
use std::sync::Arc;

#[derive(Debug, PartialEq, Clone)]
pub enum Slab {
    External(ReadOnlySlab),
    Owned(OwnedSlab),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReadOnlySlab {
    data: Arc<Vec<u8>>,
    range: Range<usize>,
    len: usize,
    group: usize,
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct OwnedSlab {
    data: Arc<Vec<u8>>,
    len: usize,
    group: usize,
    abs: i64,
}

impl Index<Range<usize>> for Slab {
    type Output = [u8];

    fn index(&self, index: Range<usize>) -> &Self::Output {
        match self {
            Self::External(ReadOnlySlab { data, range, .. }) => {
                // FIXME possible to go past range.end
                &data[range.start + index.start..range.start + index.end]
            }
            Self::Owned(OwnedSlab { data, .. }) => &data[index],
        }
    }
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

#[derive(Clone)]
pub enum WriteOp<'a> {
    UInt(u64),
    GroupUInt(u64, usize),
    BoolRun(u64, bool),
    Int(i64),
    Bytes(&'a [u8]),
    Import(&'a Slab, Range<usize>, usize, Option<bool>),
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

#[derive(Debug, Clone)]
pub enum WriteAction<'a> {
    Op(WriteOp<'a>),
    Pair(WriteOp<'a>, WriteOp<'a>),
    Raw(&'a [u8]),
    Run(i64, Vec<WriteOp<'a>>),
    End(usize, usize, i64),
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
            //Self::Run(_, _) => 0, // already added in
            //Self::End(_, _, _) => 0,
            _ => 0,
        }
    }

    fn abs(&self) -> i64 {
        match self {
            Self::Pair(WriteOp::Int(count), WriteOp::Int(value)) => count * value,
            Self::Run(_, _) => 0,
            _ => 0,
        }
    }

    fn width(&self) -> usize {
        match self {
            Self::Op(op) => op.width(),
            Self::Pair(count, op) => count.width() + op.width(),
            Self::Raw(data) => data.len(),
            Self::Run(_, _) => 0, // already added in
            Self::End(_, _, _) => 0,
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
            Self::Run(n, b) => {
                leb128::write::signed(buff, -n).unwrap();
                //println!("write lit run of {:?} {:?}", n, &buff[start..]);
                for item in b {
                    item.write(buff);
                }
            }
            Self::End(_, _, _) => {}
        }
    }
}

#[derive(Debug, Clone)]
pub struct SlabWriter<'a> {
    actions: Vec<WriteAction<'a>>,
    lit: Vec<WriteOp<'a>>,
    width: usize,
    items: usize,
    group: usize,
    bools: u64,
    abs: i64,
    init_abs: i64,
    lit_items: usize,
    max: usize,
}

impl<'a> SlabWriter<'a> {
    pub fn new(max: usize) -> Self {
        SlabWriter {
            max,
            width: 0,
            group: 0,
            abs: 0,
            init_abs: 0,
            bools: 0,
            lit_items: 0,
            items: 0,
            actions: vec![],
            lit: vec![],
        }
    }

    pub fn set_init_abs(&mut self, abs: i64) {
        self.init_abs = abs;
    }

    pub fn set_abs(&mut self, abs: i64) {
        self.abs = abs;
    }

    fn push_lit(&mut self, action: WriteOp<'a>, lit: usize, items: usize) {
        let mut width = action.width();
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
        self.check_copy_overflow(action.copy_width());
        self.abs += action.abs();
        self.group += action.group();
        self.width += width;
        self.items += items;
        self.lit_items += lit;
        self.lit.push(action);
        if items > lit {
            self.close_lit()
        }
        self.check_max();
    }

    fn push(&mut self, action: WriteAction<'a>, items: usize) {
        let width = action.width();
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
            self.push(WriteAction::Op(WriteOp::BoolRun(0, false)), 0);
        }
    }

    fn close_lit(&mut self) {
        if !self.lit.is_empty() {
            let next = std::mem::take(&mut self.lit);
            self.actions
                .push(WriteAction::Run(self.lit_items as i64, next));
            self.lit_items = 0;
        }
    }

    fn check_max(&mut self) {
        if self.width >= self.max {
            self.close_lit();
            self.actions
                .push(WriteAction::End(self.items, self.group, self.abs));
            self.width = 0;
            self.group = 0;
            self.bools = 0;
            self.items = 0;
        }
    }

    fn check_copy_overflow(&mut self, copy: usize) {
        if self.width + copy > self.max && self.width > 0 {
            self.close_lit();
            self.actions
                .push(WriteAction::End(self.items, self.group, self.abs));
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
            self.actions
                .push(WriteAction::End(self.items, self.group, self.abs));
        }
        let mut result = vec![];
        let mut buffer = vec![];
        let mut abs = self.init_abs;
        for action in self.actions {
            match action {
                WriteAction::End(len, group, next_abs) => {
                    let data = std::mem::take(&mut buffer);
                    let data = Arc::new(data);
                    result.push(Slab::Owned(OwnedSlab {
                        data,
                        len,
                        group,
                        abs,
                    }));
                    abs = next_abs;
                }
                action => action.write(&mut buffer),
            }
        }
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
            if lit > 0 {
                self.push_lit(WriteOp::Import(slab, range, group, None), lit, size)
            } else {
                self.push(
                    WriteAction::Op(WriteOp::Import(slab, range, group, None)),
                    size,
                )
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
        if lit > 0 {
            self.push_lit(WriteOp::Import(slab, range, group, None), lit, size)
        } else {
            self.push(
                WriteAction::Op(WriteOp::Import(slab, range, group, None)),
                size,
            )
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
        if lit > 0 {
            self.push_lit(WriteOp::Import(slab, range, group, bool_state), lit, size)
        } else {
            self.push(
                WriteAction::Op(WriteOp::Import(slab, range, group, bool_state)),
                size,
            )
        }
    }

    pub fn flush_lit_run<W: Debug + Copy + Into<WriteOp<'a>>>(&mut self, run: &[W]) {
        for value in run.iter() {
            self.push_lit((*value).into(), 1, 1);
        }
    }

    pub fn flush_bool_run(&mut self, count: usize, value: bool) {
        self.push(
            WriteAction::Op(WriteOp::BoolRun(count as u64, value)),
            count,
        );
    }

    pub fn flush_run<W: Debug + Into<WriteOp<'a>>>(&mut self, count: i64, value: W) {
        self.push(
            WriteAction::Pair(WriteOp::Int(count), value.into()),
            count as usize,
        );
    }

    pub fn flush_bytes(&mut self, data: &'a [u8], count: usize) {
        self.push(WriteAction::Raw(data), count);
    }

    pub fn flush_null(&mut self, count: usize) {
        self.push(
            WriteAction::Pair(WriteOp::Int(0), WriteOp::UInt(count as u64)),
            count,
        );
    }
}

impl Default for Slab {
    fn default() -> Self {
        Self::Owned(OwnedSlab::default())
    }
}

impl Slab {
    pub fn abs(&self) -> i64 {
        match self {
            Self::External(ReadOnlySlab { .. }) => 0,
            Self::Owned(OwnedSlab { abs, .. }) => *abs,
        }
    }

    pub fn run_iter<C: ColumnCursor>(&self) -> RunIter<'_, C> {
        RunIter {
            slab: self.as_slice(),
            cursor: C::new(self),
            weight_left: self.weight(),
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::External(ReadOnlySlab { data, range, .. }) => &data[range.clone()],
            Self::Owned(OwnedSlab { data, .. }) => data,
        }
    }

    pub fn external<C: ColumnCursor>(
        data: Arc<Vec<u8>>,
        range: Range<usize>,
        m: &ScanMeta,
    ) -> Result<Self, PackError> {
        let index = C::scan(&data.as_ref()[range.clone()], m)?;
        Ok(Slab::External(ReadOnlySlab {
            data,
            range,
            len: index.index(),
            group: index.group(),
        }))
    }

    pub fn byte_len(&self) -> usize {
        match self {
            Self::External(ReadOnlySlab { data, range, .. }) => data[range.clone()].len(),
            Self::Owned(OwnedSlab { data, .. }) => data.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        match self {
            Self::External(ReadOnlySlab { len, .. }) => *len,
            Self::Owned(OwnedSlab { len, .. }) => *len,
        }
    }

    pub fn group(&self) -> usize {
        match self {
            Self::External(ReadOnlySlab { group, .. }) => *group,
            Self::Owned(OwnedSlab { group, .. }) => *group,
        }
    }
}

#[derive(Debug, PartialEq, Clone, Copy, Default)]
pub struct SlabWeight {
    pub(crate) pos: usize,
    pub(crate) group: usize,
}

impl HasWeight for Slab {
    type Weight = SlabWeight;

    fn weight(&self) -> SlabWeight {
        SlabWeight {
            pos: self.len(),
            group: self.group(),
        }
    }
}

impl Add for SlabWeight {
    type Output = SlabWeight;

    fn add(self, b: Self) -> Self {
        Self {
            pos: self.pos + b.pos,
            group: self.group + b.group,
        }
    }
}

impl AddAssign<SlabWeight> for SlabWeight {
    fn add_assign(&mut self, other: Self) {
        self.pos += other.pos;
        self.group += other.group;
    }
}
impl SubAssign<SlabWeight> for SlabWeight {
    fn sub_assign(&mut self, other: Self) {
        self.pos -= other.pos;
        self.group = self.group.saturating_sub(other.group);
        //self.group = 0; // FIXME
    }
}

impl Sub for SlabWeight {
    type Output = SlabWeight;
    fn sub(self, b: Self) -> Self {
        Self {
            pos: self.pos - b.pos,
            group: self.group - b.group,
        }
    }
}
