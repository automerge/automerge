use super::{Action, ActorIdx, ColumnCursor, PackError, Packable, Run, ValueMeta};
use crate::columnar::encoding::leb128::{lebsize, ulebsize};

use std::borrow::Borrow;
use std::fmt::Debug;
use std::ops::{Index, Range};
use std::sync::Arc;

#[derive(Debug, PartialEq, Clone)]
pub(crate) enum Slab {
    External(ReadOnlySlab),
    Owned(OwnedSlab),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ReadOnlySlab {
    data: Arc<Vec<u8>>,
    range: Range<usize>,
    len: usize,
}

#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct OwnedSlab {
    data: Vec<u8>,
    len: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct WritableSlab<'a> {
    data: Vec<u8>,
    done: Vec<Slab>,
    writer: SlabWriter<'a>,
    len: usize,
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

impl<'a> Into<WriteOp<'a>> for i64 {
    fn into(self) -> WriteOp<'static> {
        WriteOp::Int(self)
    }
}

impl<'a> Into<WriteOp<'a>> for u64 {
    fn into(self) -> WriteOp<'static> {
        WriteOp::UInt(self)
    }
}

impl<'a> Into<WriteOp<'a>> for ActorIdx {
    fn into(self) -> WriteOp<'static> {
        WriteOp::UInt(u64::from(self))
    }
}

impl<'a> Into<WriteOp<'a>> for Action {
    fn into(self) -> WriteOp<'static> {
        WriteOp::UInt(u64::from(self))
    }
}

impl<'a> Into<WriteOp<'a>> for usize {
    fn into(self) -> WriteOp<'static> {
        WriteOp::UInt(self as u64)
    }
}

impl<'a> Into<WriteOp<'a>> for &'a str {
    fn into(self) -> WriteOp<'a> {
        WriteOp::Bytes(self.as_bytes())
    }
}

impl<'a> Into<WriteOp<'a>> for &'a [u8] {
    fn into(self) -> WriteOp<'a> {
        WriteOp::Bytes(self)
    }
}

impl<'a> Into<WriteOp<'a>> for bool {
    fn into(self) -> WriteOp<'a> {
        panic!()
    }
}

#[derive(Debug, Clone)]
pub(crate) enum WriteOp<'a> {
    UInt(u64),
    Int(i64),
    Bytes(&'a [u8]),
    Import(&'a Slab, Range<usize>),
}

#[derive(Debug, Clone)]
pub(crate) enum WriteAction<'a> {
    Op(WriteOp<'a>),
    Pair(WriteOp<'a>, WriteOp<'a>),
    Raw(&'a [u8]),
    Run(i64, Vec<WriteOp<'a>>),
    End(usize),
}

impl<'a> WriteOp<'a> {
    fn width(&self) -> usize {
        match self {
            Self::UInt(i) => ulebsize(*i) as usize,
            Self::Int(i) => lebsize(*i) as usize,
            Self::Bytes(b) => ulebsize(b.len() as u64) as usize + b.len(),
            Self::Import(_, r) => r.end - r.start,
        }
    }

    fn copy_width(&self) -> usize {
        match self {
            Self::Import(_, _) => self.width(),
            _ => 0,
        }
    }

    fn write(self, buff: &mut Vec<u8>) {
        match self {
            Self::UInt(i) => {
                leb128::write::unsigned(buff, i).unwrap();
            }
            Self::Int(i) => {
                leb128::write::signed(buff, i).unwrap();
            }
            Self::Bytes(b) => {
                leb128::write::unsigned(buff, b.len() as u64).unwrap();
                buff.extend(b);
            }
            Self::Import(s, r) => {
                buff.extend(&s[r]);
            }
        }
    }
}

impl<'a> WriteAction<'a> {
    fn width(&self) -> usize {
        match self {
            Self::Op(op) => op.width(),
            Self::Pair(op1, op2) => op1.width() + op2.width(),
            Self::Raw(data) => data.len(),
            Self::Run(_, _) => 0, // already added in
            Self::End(_) => 0,
        }
    }

    fn copy_width(&self) -> usize {
        match self {
            Self::Op(op) => op.copy_width(),
            _ => 0,
        }
    }

    fn write(self, buff: &mut Vec<u8>) {
        match self {
            Self::Op(op) => op.write(buff),
            Self::Pair(op1, op2) => {
                op1.write(buff);
                op2.write(buff)
            }
            Self::Raw(b) => buff.extend(b),
            Self::Run(n, b) => {
                leb128::write::signed(buff, -1 * n).unwrap();
                for item in b {
                    item.write(buff);
                }
            }
            Self::End(_) => {}
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SlabWriter<'a> {
    actions: Vec<WriteAction<'a>>,
    lit: Vec<WriteOp<'a>>,
    width: usize,
    items: usize,
    lit_items: usize,
    max: usize,
}

impl<'a> SlabWriter<'a> {
    pub(crate) fn new(max: usize) -> Self {
        SlabWriter {
            max,
            width: 0,
            lit_items: 0,
            items: 0,
            actions: vec![],
            lit: vec![],
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.items
    }

    fn push_lit(&mut self, action: WriteOp<'a>, lit: usize, items: usize, force: bool) {
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
        self.width += width;
        self.items += items;
        self.close_lit();
        self.actions.push(action);
        self.check_max();
    }

    fn close_lit(&mut self) {
        if !self.lit.is_empty() {
            let mut next = Vec::new();
            std::mem::swap(&mut next, &mut self.lit);
            self.actions
                .push(WriteAction::Run(self.lit_items as i64, next));
            self.lit_items = 0;
        }
    }

    fn check_max(&mut self) {
        if self.width >= self.max {
            self.close_lit();
            self.actions.push(WriteAction::End(self.items));
            self.width = 0;
            self.items = 0;
        }
    }

    fn check_copy_overflow(&mut self, copy: usize) {
        if self.width + copy >= self.max {
            self.close_lit();
            self.actions.push(WriteAction::End(self.items));
            self.width = 0;
            self.items = 0;
        }
    }

    pub(crate) fn write(mut self, out: &mut Vec<u8>) {
        self.close_lit();
        for action in self.actions {
            action.write(out)
        }
    }

    pub(crate) fn finish(mut self) -> Vec<Slab> {
        self.close_lit();
        if self.items > 0 {
            self.actions.push(WriteAction::End(self.items));
        }
        let mut result = vec![];
        let mut slab = OwnedSlab {
            data: vec![],
            len: 0,
        };
        for action in self.actions {
            match action {
                WriteAction::End(n) => {
                    slab.len = n;
                    let mut next = OwnedSlab::default();
                    std::mem::swap(&mut next, &mut slab);
                    result.push(Slab::Owned(next));
                }
                action => action.write(&mut slab.data),
            }
        }
        result
    }

    // TODO:
    // only difference with this vs flush_before is doing nothing when size == 0
    // skipping this on size zero is needed on write/merge operations
    // but being able to write something with size == 0 is needed for the first element of
    // boolean sets - likely these 2 and flush_after could all get turned into one nice method
    pub(crate) fn flush_before2(
        &mut self,
        slab: &'a Slab,
        range: Range<usize>,
        lit: usize,
        size: usize,
    ) {
        if size > 0 {
            if lit > 0 {
                self.push_lit(WriteOp::Import(slab, range), lit, size, false)
            } else {
                self.push(WriteAction::Op(WriteOp::Import(slab, range)), size)
            }
        }
    }

    pub(crate) fn flush_before(
        &mut self,
        slab: &'a Slab,
        range: Range<usize>,
        lit: usize,
        size: usize,
    ) {
        if lit > 0 {
            self.push_lit(WriteOp::Import(slab, range), lit, size, false)
        } else {
            self.push(WriteAction::Op(WriteOp::Import(slab, range)), size)
        }
    }

    pub(crate) fn flush_after(&mut self, slab: &'a Slab, index: usize, lit: usize, size: usize) {
        let range = index..slab.byte_len();
        if lit > 0 {
            self.push_lit(WriteOp::Import(slab, range), lit, size, false)
        } else {
            self.push(WriteAction::Op(WriteOp::Import(slab, range)), size)
        }
    }

    pub(crate) fn flush_lit_run<W: Debug + Copy + Into<WriteOp<'a>>>(&mut self, run: &[W]) {
        for (i, value) in run.iter().enumerate() {
            self.push_lit((*value).into(), 1, 1, i == 0);
        }
    }

    pub(crate) fn flush_bool_run(&mut self, count: usize) {
        self.push(WriteAction::Op(WriteOp::UInt(count as u64)), count as usize);
    }

    pub(crate) fn flush_run<W: Debug + Into<WriteOp<'a>>>(&mut self, count: i64, value: W) {
        self.push(
            WriteAction::Pair(WriteOp::Int(count), value.into()),
            count as usize,
        );
    }

    pub(crate) fn flush_bytes(&mut self, data: &'a [u8], count: usize) {
        self.push(WriteAction::Raw(data), count as usize);
    }

    pub(crate) fn flush_null(&mut self, count: usize) {
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

#[derive(Copy, Clone, Debug)]
pub(crate) struct SlabIter<'a, C: ColumnCursor> {
    slab: &'a Slab,
    cursor: C,
    state: Option<Run<'a, C::Item>>,
}

impl<'a, C: ColumnCursor> Iterator for SlabIter<'a, C> {
    type Item = Option<<C::Item as Packable>::Unpacked<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut state = None;
        std::mem::swap(&mut state, &mut self.state);
        match state {
            Some(run) => {
                let (value, next_state) = self.cursor.pop(run);
                self.state = next_state;
                Some(value)
            }
            None => {
                if let Some((run, cursor)) = self.cursor.next(self.slab.as_ref()) {
                    self.cursor = cursor;
                    let (value, next_state) = self.cursor.pop(run);
                    self.state = next_state;
                    Some(value)
                } else {
                    self.state = None;
                    None
                }
            }
        }
    }
}

impl Slab {
    pub(crate) fn iter<'a, C: ColumnCursor>(&'a self) -> SlabIter<'a, C> {
        SlabIter {
            slab: self,
            cursor: C::default(),
            state: None,
        }
    }

    pub(crate) fn as_ref(&self) -> &[u8] {
        match self {
            Self::External(ReadOnlySlab { data, range, .. }) => &data[range.clone()],
            Self::Owned(OwnedSlab { data, .. }) => &data,
        }
    }

    pub(crate) fn external<C: ColumnCursor>(
        data: Arc<Vec<u8>>,
        range: Range<usize>,
    ) -> Result<Self, PackError> {
        let index = C::scan(&data.as_ref()[range.clone()])?;
        Ok(Slab::External(ReadOnlySlab {
            data,
            range,
            len: index.index(),
        }))
    }

    pub(crate) fn byte_len(&self) -> usize {
        match self {
            Self::External(ReadOnlySlab { data, range, .. }) => data[range.clone()].len(),
            Self::Owned(OwnedSlab { data, .. }) => data.len(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::External(ReadOnlySlab { len, .. }) => *len,
            Self::Owned(OwnedSlab { len, .. }) => *len,
        }
    }
}
