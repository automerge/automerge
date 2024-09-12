use super::{
    columns::{RunStep, ScanMeta, Seek},
    Action, ActorIdx, ColumnCursor, PackError, Packable, Run,
};
use crate::columnar::encoding::leb128::{lebsize, ulebsize};

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
    group: usize,
}

#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct OwnedSlab {
    data: Arc<Vec<u8>>,
    len: usize,
    group: usize,
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
        WriteOp::UInt(n)
    }
}

impl<'a> From<ActorIdx> for WriteOp<'a> {
    fn from(n: ActorIdx) -> WriteOp<'static> {
        WriteOp::UInt(u64::from(n))
    }
}

impl<'a> From<Action> for WriteOp<'a> {
    fn from(a: Action) -> WriteOp<'static> {
        WriteOp::UInt(u64::from(a))
    }
}

impl<'a> From<usize> for WriteOp<'a> {
    fn from(n: usize) -> WriteOp<'static> {
        WriteOp::UInt(n as u64)
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

#[derive(Debug, Clone)]
pub(crate) enum WriteOp<'a> {
    UInt(u64),
    GroupUInt(u64, usize),
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
    End(usize, usize),
}

impl<'a> WriteOp<'a> {
    fn group(&self) -> usize {
        match self {
            Self::GroupUInt(_, g) => *g,
            _ => 0,
        }
    }

    fn width(&self) -> usize {
        match self {
            Self::UInt(i) => ulebsize(*i) as usize,
            Self::GroupUInt(i, _) => ulebsize(*i) as usize,
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
        //let start = buff.len();
        match self {
            Self::UInt(i) => {
                leb128::write::unsigned(buff, i).unwrap();
                //log!("write uint {} {:?}",i, &buff[start..]);
            }
            Self::GroupUInt(i, _) => {
                leb128::write::unsigned(buff, i).unwrap();
                //log!("write group uint {} {:?}",i, &buff[start..]);
            }
            Self::Int(i) => {
                leb128::write::signed(buff, i).unwrap();
                //log!("write int {} {:?}",i, &buff[start..]);
            }
            Self::Bytes(b) => {
                leb128::write::unsigned(buff, b.len() as u64).unwrap();
                buff.extend(b);
                //log!("write bytes {:?}",&buff[start..]);
            }
            Self::Import(s, r) => {
                buff.extend(&s[r]);
                //log!("write import {:?}",&buff[start..]);
            }
        }
    }
}

impl<'a> WriteAction<'a> {
    fn group(&self) -> usize {
        match self {
            Self::Op(op) => op.group(),
            Self::Pair(op1, op2) => op1.group() + op2.group(),
            Self::Raw(_) => 0,
            Self::Run(_, _) => 0, // already added in
            Self::End(_, _) => 0,
        }
    }

    fn width(&self) -> usize {
        match self {
            Self::Op(op) => op.width(),
            Self::Pair(op1, op2) => op1.width() + op2.width(),
            Self::Raw(data) => data.len(),
            Self::Run(_, _) => 0, // already added in
            Self::End(_, _) => 0,
        }
    }

    fn copy_width(&self) -> usize {
        match self {
            Self::Op(op) => op.copy_width(),
            _ => 0,
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
                //log!("write raw {:?}", &buff[start..]);
            }
            Self::Run(n, b) => {
                leb128::write::signed(buff, -n).unwrap();
                //log!("write lit run of {:?} {:?}", n, &buff[start..]);
                for item in b {
                    item.write(buff);
                }
            }
            Self::End(_, _) => {}
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SlabWriter<'a> {
    actions: Vec<WriteAction<'a>>,
    lit: Vec<WriteOp<'a>>,
    width: usize,
    items: usize,
    group: usize,
    lit_items: usize,
    max: usize,
}

impl<'a> SlabWriter<'a> {
    pub(crate) fn new(max: usize) -> Self {
        SlabWriter {
            max,
            width: 0,
            group: 0,
            lit_items: 0,
            items: 0,
            actions: vec![],
            lit: vec![],
        }
    }

    //pub(crate) fn len(&self) -> usize {
    //    self.items
    //}

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
        self.group += action.group();
        self.width += width;
        self.items += items;
        self.close_lit();
        self.actions.push(action);
        self.check_max();
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
            self.actions.push(WriteAction::End(self.items, self.group));
            self.width = 0;
            self.group = 0;
            self.items = 0;
        }
    }

    fn check_copy_overflow(&mut self, copy: usize) {
        if self.width + copy > self.max {
            self.close_lit();
            self.actions.push(WriteAction::End(self.items, self.group));
            self.width = 0;
            self.group = 0;
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
            self.actions.push(WriteAction::End(self.items, self.group));
        }
        let mut result = vec![];
        let mut buffer = vec![];
        for action in self.actions {
            match action {
                WriteAction::End(len, group) => {
                    let data = Arc::new(std::mem::take(&mut buffer));
                    result.push(Slab::Owned(OwnedSlab { data, len, group }));
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
    pub(crate) fn flush_before2(
        &mut self,
        slab: &'a Slab,
        range: Range<usize>,
        lit: usize,
        size: usize,
    ) {
        if size > 0 {
            if lit > 0 {
                self.push_lit(WriteOp::Import(slab, range), lit, size)
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
            self.push_lit(WriteOp::Import(slab, range), lit, size)
        } else {
            self.push(WriteAction::Op(WriteOp::Import(slab, range)), size)
        }
    }

    pub(crate) fn flush_after(
        &mut self,
        slab: &'a Slab,
        index: usize,
        lit: usize,
        size: usize,
        _group: usize, // FIXME!!
    ) {
        let range = index..slab.byte_len();
        if lit > 0 {
            self.push_lit(WriteOp::Import(slab, range), lit, size)
        } else {
            self.push(WriteAction::Op(WriteOp::Import(slab, range)), size)
        }
    }

    pub(crate) fn flush_lit_run<W: Debug + Copy + Into<WriteOp<'a>>>(&mut self, run: &[W]) {
        for value in run.iter() {
            self.push_lit((*value).into(), 1, 1);
        }
    }

    pub(crate) fn flush_bool_run(&mut self, count: usize) {
        self.push(WriteAction::Op(WriteOp::UInt(count as u64)), count);
    }

    pub(crate) fn flush_run<W: Debug + Into<WriteOp<'a>>>(&mut self, count: i64, value: W) {
        self.push(
            WriteAction::Pair(WriteOp::Int(count), value.into()),
            count as usize,
        );
    }

    pub(crate) fn flush_bytes(&mut self, data: &'a [u8], count: usize) {
        self.push(WriteAction::Raw(data), count);
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

#[derive(Debug)]
pub(crate) struct SlabIter<'a, C: ColumnCursor> {
    slab: &'a Slab,
    pub(crate) cursor: C,
    state: Option<IterState<'a, C::Item>>,
    last_group: usize,
}

impl<'a, C: ColumnCursor> Copy for SlabIter<'a, C> {}

impl<'a, C: ColumnCursor> std::clone::Clone for SlabIter<'a, C> {
    fn clone(&self) -> Self {
        *self
    }
}

#[derive(Debug)]
enum IterState<'a, I: Packable + ?Sized> {
    Popped(Option<I::Unpacked<'a>>, Option<Run<'a, I>>),
    AtStartOfRun(Run<'a, I>),
    InRun(Run<'a, I>),
}

impl<'a, I: Packable + ?Sized> Copy for IterState<'a, I> {}
impl<'a, I: Packable + ?Sized> std::clone::Clone for IterState<'a, I> {
    fn clone(&self) -> Self {
        *self
    }
}

//impl<'a, C: ColumnCursor> SlabIter<'a, C> {
impl<'a, I: Packable + ?Sized> IterState<'a, I> {
    fn group(&self) -> usize {
        match self {
            Self::Popped(None, Some(run)) => run.group(),
            Self::Popped(Some(val), Some(run)) => I::group(*val) + run.group(),
            Self::Popped(Some(val), None) => I::group(*val),
            Self::AtStartOfRun(run) => run.group(),
            Self::InRun(run) => run.group(),
            _ => 0,
        }
    }

    fn state_length(&self) -> usize {
        match self {
            Self::Popped(_, Some(run)) => run.count + 1,
            Self::Popped(_, None) => 1,
            Self::AtStartOfRun(run) => run.count,
            Self::InRun(run) => run.count,
        }
    }
}

impl<'a, C: ColumnCursor> SlabIter<'a, C> {
    pub(crate) fn next_run(&mut self) -> Option<Run<'a, C::Item>> {
        if let Some((run, cursor)) = self.cursor.next(self.slab.as_ref()) {
            self.cursor = cursor;
            self.last_group = item_group::<C::Item>(&run.value) * run.count;
            Some(run)
        } else {
            None
        }
    }

    pub(crate) fn pos(&self) -> usize {
        self.cursor.index() - self.state.as_ref().map(|s| s.state_length()).unwrap_or(0)
    }

    pub(crate) fn len(&self) -> usize {
        self.slab.len()
    }

    pub(crate) fn group(&self) -> usize {
        // FIXME
        let state_group = self.state.as_ref().map(|s| s.group()).unwrap_or(0);
        self.cursor.group() - state_group - self.last_group
    }

    pub(crate) fn max_group(&self) -> usize {
        self.slab.group()
    }

    pub(crate) fn seek<S: Seek<C::Item>>(&mut self, seek: &mut S) -> bool {
        match seek.process_slab(self.slab) {
            RunStep::Skip => {
                return false;
            }
            RunStep::Done => {
                return true;
            }
            _ => (),
        }
        loop {
            match self.state.take() {
                Some(IterState::AtStartOfRun(run)) => match seek.process_run(&run) {
                    RunStep::Skip => {
                        self.state = None;
                    }
                    RunStep::Process => {
                        let (value, next_state) = self.cursor.pop(run);
                        self.state = Some(IterState::Popped(value, next_state));
                    }
                    RunStep::Done => {
                        return true;
                    }
                },
                Some(IterState::InRun(run)) => {
                    let (value, next_state) = self.cursor.pop(run);
                    self.state = Some(IterState::Popped(value, next_state));
                }
                Some(IterState::Popped(elem, run)) => {
                    seek.process_element(elem);
                    if seek.done() {
                        self.state = Some(IterState::Popped(elem, run));
                        return true;
                    }
                    if let Some(run) = run {
                        let (value, next_state) = self.cursor.pop(run);
                        self.state = Some(IterState::Popped(value, next_state));
                    } else {
                        self.state = None
                    }
                }
                None => {
                    if let Some((run, cursor)) = self.cursor.next(self.slab.as_ref()) {
                        //if let Some(run) = self.next_run() {
                        self.cursor = cursor;
                        self.state = Some(IterState::AtStartOfRun(run));
                    } else {
                        return false;
                    }
                }
            }
        }
    }
}

fn item_group<P: Packable + ?Sized>(item: &Option<P::Unpacked<'_>>) -> usize {
    match item {
        Some(i) => P::group(*i),
        None => 0,
    }
}

impl<'a, C: ColumnCursor> Iterator for SlabIter<'a, C> {
    type Item = Option<<C::Item as Packable>::Unpacked<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut state = None;
        std::mem::swap(&mut state, &mut self.state);
        match state {
            Some(IterState::Popped(value, next_run)) => {
                self.state = next_run.map(IterState::InRun);
                self.last_group = item_group::<C::Item>(&value);
                Some(value)
            }
            Some(IterState::InRun(run) | IterState::AtStartOfRun(run)) => {
                let (value, next_state) = self.cursor.pop(run);
                self.state = next_state.map(IterState::InRun);
                self.last_group = item_group::<C::Item>(&value);
                Some(value)
            }
            None => {
                if let Some((run, cursor)) = self.cursor.next(self.slab.as_ref()) {
                    //if let Some(run) = self.next_run() {
                    self.cursor = cursor;
                    let (value, next_state) = self.cursor.pop(run);
                    self.state = next_state.map(IterState::InRun);
                    self.last_group = item_group::<C::Item>(&value);
                    Some(value)
                } else {
                    self.state = None;
                    self.last_group = 0;
                    None
                }
            }
        }
    }
}

impl Slab {
    pub(crate) fn iter<C: ColumnCursor>(&self) -> SlabIter<'_, C> {
        SlabIter {
            slab: self,
            cursor: C::default(),
            state: None,
            last_group: 0,
        }
    }

    pub(crate) fn as_ref(&self) -> &[u8] {
        match self {
            Self::External(ReadOnlySlab { data, range, .. }) => &data[range.clone()],
            Self::Owned(OwnedSlab { data, .. }) => data,
        }
    }

    pub(crate) fn external<C: ColumnCursor>(
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

    pub(crate) fn group(&self) -> usize {
        match self {
            Self::External(ReadOnlySlab { group, .. }) => *group,
            Self::Owned(OwnedSlab { group, .. }) => *group,
        }
    }
}
