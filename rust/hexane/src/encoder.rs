use super::aggregate::Acc;
use super::boolean::BooleanState;
use super::columndata::ColumnData;
use super::cursor::{ColumnCursor, Run};
use super::delta::DeltaState;
use super::pack::{MaybePackable, Packable};
use super::rle::RleState;
use super::slab::{Slab, SlabWriter};
use crate::Cow;
use std::marker::PhantomData;
use std::ops::Range;

use std::fmt::Debug;

pub trait Writer<'a, P: Packable + ?Sized> {
    fn flush_null(&mut self, count: usize);
    fn flush_lit_run(&mut self, run: &[Cow<'a, P>]);
    fn flush_run(&mut self, count: i64, value: Cow<'a, P>);
    fn flush_bool_run(&mut self, count: usize, value: bool);
    fn flush_bytes(&mut self, bytes: Cow<'a, [u8]>);
}

impl<'a, P: Packable + ?Sized> Writer<'a, P> for Vec<u8> {
    fn flush_null(&mut self, count: usize) {
        self.push(0);
        leb128::write::unsigned(self, count as u64).unwrap();
    }
    fn flush_lit_run(&mut self, run: &[Cow<'a, P>]) {
        let len = run.len() as i64;
        leb128::write::signed(self, -len).unwrap();
        for value in run {
            P::pack(value, self);
        }
    }
    fn flush_run(&mut self, count: i64, value: Cow<'a, P>) {
        leb128::write::signed(self, count).unwrap();
        P::pack(&value, self);
    }
    fn flush_bool_run(&mut self, count: usize, _value: bool) {
        leb128::write::unsigned(self, count as u64).unwrap();
    }

    fn flush_bytes(&mut self, bytes: Cow<'a, [u8]>) {
        self.extend_from_slice(bytes.as_ref())
    }
}

pub trait EncoderState<'a, P: Packable + ?Sized + 'a>: Debug + Default + Clone {
    fn is_empty(&self) -> bool;

    fn append<W: Writer<'a, P>>(&mut self, writer: &mut W, value: Option<Cow<'a, P>>) -> usize {
        self.append_chunk(writer, Run { count: 1, value })
    }

    fn append_first_chunk(
        &mut self,
        writer: &mut SlabWriter<'a, P>,
        chunk: Run<'a, P>,
        _slab: &Slab,
    ) -> bool {
        self.append_chunk(writer, chunk);
        true
    }

    fn copy_slab<C: ColumnCursor<State<'a> = Self, Item = P>>(
        &mut self,
        writer: &mut SlabWriter<'a, P>,
        slab: &'a Slab,
    ) {
        let mut size = slab.len();

        let mut c0 = None;
        let mut last = None;
        for (run, c) in slab.run_iter::<C>().with_cursor() {
            if c0.is_none() {
                size -= run.count;
                if self.append_first_chunk(writer, run, slab) {
                    c0 = Some(c);
                }
            } else {
                last = Some((run, c));
            }
        }

        if c0.is_none() || last.is_none() {
            return;
        }

        let c0 = c0.unwrap();
        let (run1, c1) = last.unwrap();

        size -= run1.count;

        if size == 0 {
            self.append_chunk(writer, run1);
            return;
        }

        self.flush(writer);

        *self = C::copy_between(slab.as_slice(), writer, c0, c1, run1, size);
    }

    fn append_chunk<W: Writer<'a, P>>(&mut self, writer: &mut W, chunk: Run<'a, P>) -> usize;
    fn flush<W: Writer<'a, P>>(&mut self, writer: &mut W);
}

impl<'a> EncoderState<'a, bool> for BooleanState {
    fn is_empty(&self) -> bool {
        !self.value || self.count == 0
    }

    fn append_chunk<W: Writer<'a, bool>>(&mut self, writer: &mut W, run: Run<'a, bool>) -> usize {
        let item = *run.value.unwrap_or_default();
        if self.value == item {
            self.count += run.count;
        } else {
            if self.count > 0 || !self.flushed {
                writer.flush_bool_run(self.count, self.value);
                self.flushed = true;
            }
            self.value = item;
            self.count = run.count;
        }
        run.count
    }

    fn flush<W: Writer<'a, bool>>(&mut self, writer: &mut W) {
        let state = std::mem::take(self);
        writer.flush_bool_run(state.count, state.value);
        self.flushed = true;
    }

    fn copy_slab<C: ColumnCursor<State<'a> = Self, Item = bool>>(
        &mut self,
        writer: &mut SlabWriter<'a, bool>,
        slab: &'a Slab,
    ) {
        let mut cursor = C::empty();
        let mut start = 0;
        let mut first_count = 0;
        let data = slab.as_slice();
        while let Ok(Some(run)) = cursor.try_next(data) {
            if run.count > 0 {
                first_count = run.count;
                self.append_chunk(writer, run);
                start = cursor.offset();
                break;
            }
        }

        let mut end = 0;
        while let Ok(Some(run)) = cursor.try_next(data) {
            assert!(run.count != 0);
            if cursor.index() == slab.len() {
                let size = slab.len() - first_count - run.count;
                self.flush(writer);
                writer.copy(data, start..end, 0, size, Acc::new(), None);
                self.append_chunk(writer, run);
                break;
            }
            end = cursor.offset();
        }
    }
}

impl<'a> EncoderState<'a, i64> for DeltaState<'a> {
    fn is_empty(&self) -> bool {
        self.rle.is_empty()
    }

    fn append<W: Writer<'a, i64>>(&mut self, writer: &mut W, value: Option<Cow<'a, i64>>) -> usize {
        let value = value.map(|i| Cow::Owned(*i - self.abs));
        self.append_chunk(writer, Run { count: 1, value })
    }

    fn append_first_chunk(
        &mut self,
        writer: &mut SlabWriter<'a, i64>,
        run: Run<'a, i64>,
        slab: &Slab,
    ) -> bool {
        if let Some(v) = &run.value {
            let delta = self.abs - slab.abs();
            self.append_chunk(
                writer,
                Run {
                    count: 1,
                    value: Some(Cow::Owned(**v - delta)),
                },
            );
            if let Some(r) = run.pop() {
                self.append_chunk(writer, r);
            }
            true
        } else {
            self.append_chunk(writer, run);
            false
        }
    }

    fn append_chunk<W: Writer<'a, i64>>(&mut self, writer: &mut W, run: Run<'a, i64>) -> usize {
        self.abs += run.delta();
        self.rle.append_chunk(writer, run)
    }

    fn flush<W: Writer<'a, i64>>(&mut self, writer: &mut W) {
        self.rle.flush(writer)
    }

    fn copy_slab<C>(&mut self, writer: &mut SlabWriter<'a, i64>, slab: &'a Slab)
    where
        C: ColumnCursor<State<'a> = Self, Item = i64>,
    {
        let mut cursor = C::new(slab);
        let data = slab.as_slice();
        while let Some(run) = cursor.next(data) {
            match &run.value {
                None => {
                    self.append_chunk(writer, run);
                }
                Some(value) => {
                    let first = **value + slab.abs();
                    self.append(writer, Some(Cow::Owned(first)));
                    if let Some(r) = run.pop() {
                        self.append_chunk(writer, r);
                    }
                    self.abs = slab.abs() + run.delta();
                    break;
                }
            }
        }
        // FIXME - would be faster to use a copy here
        while let Some(run) = cursor.next(data) {
            self.append_chunk(writer, run);
        }
    }
}

impl<'a> EncoderState<'a, [u8]> for () {
    fn is_empty(&self) -> bool {
        true
    }

    fn append_chunk<W: Writer<'a, [u8]>>(&mut self, writer: &mut W, run: Run<'a, [u8]>) -> usize {
        let mut len = 0;
        for _ in 0..run.count {
            if let Some(i) = run.value.clone() {
                len += i.len();
                writer.flush_bytes(i);
            }
        }
        len
    }

    fn copy_slab<C: ColumnCursor<State<'a> = Self, Item = [u8]>>(
        &mut self,
        writer: &mut SlabWriter<'a, [u8]>,
        slab: &'a Slab,
    ) {
        let len = slab.len();
        writer.copy(slab.as_slice(), 0..len, 0, len, Acc::new(), None);
    }

    fn flush<W: Writer<'a, [u8]>>(&mut self, _writer: &mut W) {}
}

impl<'a, P: Packable + ?Sized> EncoderState<'a, P> for RleState<'a, P> {
    fn is_empty(&self) -> bool {
        match self {
            RleState::Empty => true,
            RleState::LoneValue(None) => true,
            RleState::Run { value, .. } if value.is_none() => true,
            _ => false,
        }
    }

    fn append_chunk<W: Writer<'a, P>>(&mut self, writer: &mut W, chunk: Run<'a, P>) -> usize {
        let count = chunk.count;
        let state = std::mem::take(self);
        let new_state = match state {
            RleState::Empty => RleState::from(chunk),
            RleState::LoneValue(value) => match (value, chunk.value) {
                (a, b) if a == b => RleState::from(Run {
                    count: count + 1,
                    value: b,
                }),
                (Some(a), Some(b)) if chunk.count == 1 => RleState::lit_run(a, b),
                (a, b) => {
                    flush_run::<P, W>(writer, 1, a);
                    RleState::from(Run { count, value: b })
                }
            },
            RleState::Run { count, value } if chunk.value == value => {
                RleState::from(chunk.plus(count))
            }
            RleState::Run { count, value } => {
                flush_run::<P, W>(writer, count, value);
                RleState::from(chunk)
            }
            RleState::LitRun { mut run, current } => {
                match (current, chunk.value) {
                    (a, Some(b)) if a == b => {
                        // the end of the lit run merges with the next
                        writer.flush_lit_run(&run);
                        RleState::from(Run {
                            count: count + 1,
                            value: Some(b),
                        })
                    }
                    (a, Some(b)) if chunk.count == 1 => {
                        // its single and different - addit to the lit run
                        run.push(a);
                        RleState::LitRun { run, current: b }
                    }
                    (a, b) => {
                        // flush this lit run (current and all) - next run replaces it
                        run.push(a);
                        writer.flush_lit_run(&run);
                        RleState::from(Run { count, value: b })
                    }
                }
            }
        };
        *self = new_state;
        count
    }

    fn flush<W: Writer<'a, P>>(&mut self, writer: &mut W) {
        match std::mem::take(self) {
            RleState::Empty => (),
            RleState::LoneValue(Some(value)) => writer.flush_lit_run(&[value]),
            RleState::LoneValue(None) => writer.flush_null(1),
            RleState::Run {
                count,
                value: Some(v),
            } => writer.flush_run(count as i64, v),
            RleState::Run { count, value: None } => writer.flush_null(count),
            RleState::LitRun { mut run, current } => {
                run.push(current);
                writer.flush_lit_run(&run);
            }
        }
    }
}

fn flush_run<'a, P: ?Sized + Packable, W: Writer<'a, P>>(
    writer: &mut W,
    num: usize,
    value: Option<Cow<'a, P>>,
) {
    if let Some(v) = value {
        if num == 1 {
            writer.flush_lit_run(&[v]);
        } else {
            writer.flush_run(num as i64, v);
        }
    } else {
        writer.flush_null(num);
    }
}

#[derive(Debug)]
pub struct Encoder<'a, C: ColumnCursor>
where
    C::Item: 'a,
{
    pub len: usize,
    pub state: C::State<'a>,
    pub writer: SlabWriter<'a, C::Item>,
    _phantom: PhantomData<C>,
}

impl<C: ColumnCursor> Default for Encoder<'_, C> {
    fn default() -> Self {
        Self {
            len: 0,
            state: C::State::default(),
            writer: SlabWriter::new(C::slab_size(), true),
            _phantom: PhantomData,
        }
    }
}

impl<C: ColumnCursor> Clone for Encoder<'_, C> {
    fn clone(&self) -> Self {
        Self {
            len: self.len,
            state: self.state.clone(),
            writer: self.writer.clone(),
            _phantom: self._phantom,
        }
    }
}

impl<'a, C: ColumnCursor> Encoder<'a, C>
where
    C::Item: 'a,
{
    pub fn append<M: MaybePackable<'a, C::Item>>(&mut self, value: M) -> usize {
        self.append_item(value.maybe_packable())
    }

    pub fn append_item(&mut self, value: Option<Cow<'a, C::Item>>) -> usize {
        let items = self.state.append(&mut self.writer, value);
        self.len += items;
        items
    }

    pub fn extend<I: Iterator<Item = Option<Cow<'a, C::Item>>>>(&mut self, iter: I)
    where
        <C as ColumnCursor>::Item: 'a,
    {
        for value in iter {
            self.append_item(value);
        }
    }

    pub fn append_bytes(&mut self, bytes: Option<Cow<'a, [u8]>>) {
        // this doesn't update len
        // current len is only used in ChangeOps and append_bytes is not
        if let Some(bytes) = bytes {
            self.writer.flush_bytes(bytes);
        }
    }

    #[inline(never)]
    pub(crate) fn append_chunk(&mut self, run: Run<'a, C::Item>) -> usize {
        self.len += run.count;
        self.state.append_chunk(&mut self.writer, run)
    }

    pub fn new(locked: bool) -> Self {
        Self {
            len: 0,
            state: C::State::default(),
            writer: SlabWriter::new(C::slab_size(), locked),
            _phantom: PhantomData,
        }
    }

    pub fn with_capacity(_cap: usize, locked: bool) -> Self {
        Self {
            len: 0,
            state: C::State::default(),
            writer: SlabWriter::new(C::slab_size(), locked),
            _phantom: PhantomData,
        }
    }

    pub fn init(writer: SlabWriter<'a, C::Item>, state: C::State<'a>) -> Self {
        Self {
            len: 0,
            state,
            writer,
            _phantom: PhantomData,
        }
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
        // this doesn't update len
        // current len is only used in ChangeOps and append_bytes is not
        self.writer.copy(slab, range, lit, size, acc, bool_state)
    }

    pub fn flush(&mut self) {
        self.state.flush(&mut self.writer);
    }

    #[inline(never)]
    pub fn finish(mut self) -> Vec<Slab> {
        self.state.flush(&mut self.writer);
        self.writer.finish()
    }

    pub fn save_to(mut self, out: &mut Vec<u8>) -> Range<usize> {
        // theres a save_to bug w unlocked encoders on slab boundaries but its currently not needed anywhere
        assert!(self.writer.is_locked());
        self.state.flush(&mut self.writer);
        let start = out.len();
        if self.len > 0 {
            self.writer.write(out);
        }
        let end = out.len();
        start..end
    }

    fn is_empty(&self) -> bool {
        self.writer.is_empty() && self.state.is_empty()
    }

    pub fn save_to_unless_empty(self, out: &mut Vec<u8>) -> Range<usize> {
        let mut _tmp: Vec<u8> = vec![];
        #[cfg(debug_assertions)]
        self.clone()
            .into_column_data()
            .save_to_unless_empty(&mut _tmp);
        let range = if !self.is_empty() {
            self.save_to(out)
        } else {
            out.len()..out.len()
        };
        debug_assert_eq!(&_tmp, &out[range.clone()]);
        range
    }

    pub fn save_to_and_remap_unless_empty<'b, F>(self, out: &mut Vec<u8>, f: F) -> Range<usize>
    where
        F: Fn(&C::Item) -> Option<&'b C::Item>,
        C::Item: 'b,
    {
        if !self.is_empty() {
            self.save_to_and_remap(out, f)
        } else {
            out.len()..out.len()
        }
    }

    pub fn save_to_and_remap<'b, F>(mut self, out: &mut Vec<u8>, f: F) -> Range<usize>
    where
        F: Fn(&C::Item) -> Option<&'b C::Item>,
        C::Item: 'b,
    {
        self.state.flush(&mut self.writer);
        let start = out.len();
        self.writer.write_and_remap(out, f);
        let end = out.len();
        start..end
    }

    pub fn into_column_data(mut self) -> ColumnData<C> {
        self.state.flush(&mut self.writer);
        self.writer.into_column(self.len)
    }

    pub fn copy_slab(&mut self, slab: &'a Slab) {
        self.state.copy_slab::<C>(&mut self.writer, slab);
    }
}

#[derive(Debug)]
pub struct SpliceEncoder<'a, C: ColumnCursor>
where
    C::Item: 'a,
{
    pub encoder: Encoder<'a, C>,
    pub slab: &'a Slab,
    pub acc: Acc,
    pub post: C::PostState<'a>,
    pub deleted: usize,
    pub overflow: usize,
    pub cursor: C,
}

impl<'a, C: ColumnCursor> SpliceEncoder<'a, C> {
    pub fn append_item(&mut self, v: Option<Cow<'a, C::Item>>) -> usize {
        self.encoder.append_item(v)
    }

    pub fn append<M: MaybePackable<'a, C::Item>>(&mut self, v: M) -> usize {
        self.encoder.append(v)
    }

    #[inline(never)]
    pub fn finish(mut self) -> Vec<Slab> {
        if let Some(cursor) =
            C::finalize_state(self.slab, &mut self.encoder, self.post, self.cursor)
        {
            C::finish(self.slab, &mut self.encoder.writer, cursor)
        }
        self.encoder.writer.finish()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::super::boolean::BooleanCursor;
    use super::super::rle::UIntCursor;
    use super::*;

    #[test]
    fn test_empty_bool() {
        let encoder = Encoder::<BooleanCursor>::new(true); // locked
        let mut data = vec![];
        let range = encoder.save_to(&mut data);
        assert_eq!(range, 0..0);
    }

    #[test]
    fn test_encoding_large_lit_runs() {
        for i in 0..10_000 {
            let mut encoder = Encoder::<UIntCursor>::new(true); // locked
            for j in 0..i {
                encoder.append(Some(Cow::Owned(j)));
            }
            let col1 = encoder.into_column_data();
            if i % 100 == 0 {
                let col2 = UIntCursor::load(&col1.save()).unwrap();
                assert_eq!(col1.to_vec(), col2.to_vec());
            }
        }
    }
}
