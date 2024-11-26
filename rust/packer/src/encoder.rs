use super::aggregate::Acc;
use super::boolean::BooleanState;
use super::columndata::ColumnData;
use super::cursor::{ColumnCursor, Run};
use super::delta::DeltaState;
use super::pack::{MaybePackable2, Packable};
use super::rle::RleState;
use super::slab::{Slab, SlabWriter};
use crate::Cow;
use std::marker::PhantomData;
use std::ops::Range;

use std::fmt::Debug;

pub trait EncoderState<'a, P: Packable + ?Sized + 'a>: Debug + Default + Clone {
    fn append(&mut self, writer: &mut SlabWriter<'a>, value: Option<Cow<'a, P>>) -> usize {
        self.append_chunk(writer, Run { count: 1, value })
    }

    fn append_first_chunk(
        &mut self,
        writer: &mut SlabWriter<'a>,
        chunk: Run<'a, P>,
        _slab: &Slab,
    ) -> bool {
        self.append_chunk(writer, chunk);
        true
    }

    fn write<C: ColumnCursor<State<'a> = Self, Item = P>>(
        &mut self,
        writer: &mut SlabWriter<'a>,
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

    fn append_chunk(&mut self, writer: &mut SlabWriter<'a>, chunk: Run<'a, P>) -> usize;
    fn flush(&mut self, writer: &mut SlabWriter<'a>);
}

impl<'a> EncoderState<'a, bool> for BooleanState {
    fn append_chunk(&mut self, writer: &mut SlabWriter<'a>, run: Run<'a, bool>) -> usize {
        let item = *run.value.unwrap_or_default();
        if self.value == item {
            self.count += run.count;
        } else {
            if self.count > 0 {
                writer.flush_bool_run(self.count, self.value);
            }
            self.value = item;
            self.count = run.count;
        }
        run.count
    }

    fn flush(&mut self, writer: &mut SlabWriter<'a>) {
        let state = std::mem::take(self);
        writer.flush_bool_run(state.count, state.value);
    }
}

impl<'a> EncoderState<'a, i64> for DeltaState<'a> {
    fn append(&mut self, writer: &mut SlabWriter<'a>, value: Option<Cow<'a, i64>>) -> usize {
        let value = value.map(|i| Cow::Owned(*i - self.abs));
        self.append_chunk(writer, Run { count: 1, value })
    }

    fn append_first_chunk(
        &mut self,
        writer: &mut SlabWriter<'a>,
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

    fn append_chunk(&mut self, writer: &mut SlabWriter<'a>, run: Run<'a, i64>) -> usize {
        self.abs += run.delta();
        self.rle.append_chunk(writer, run)
    }

    fn flush(&mut self, writer: &mut SlabWriter<'a>) {
        self.rle.flush(writer)
    }
}

impl<'a> EncoderState<'a, [u8]> for () {
    fn append_chunk(&mut self, writer: &mut SlabWriter<'a>, run: Run<'a, [u8]>) -> usize {
        let mut len = 0;
        for _ in 0..run.count {
            if let Some(i) = run.value.clone() {
                len += i.len();
                writer.flush_bytes(i);
            }
        }
        len
    }

    fn write<C: ColumnCursor<State<'a> = Self, Item = [u8]>>(
        &mut self,
        writer: &mut SlabWriter<'a>,
        slab: &'a Slab,
    ) {
        let len = slab.len();
        writer.copy(slab.as_slice(), 0..len, 0, len, Acc::new(), None);
    }

    fn flush(&mut self, _writer: &mut SlabWriter<'a>) {}
}

impl<'a, P: Packable + ?Sized> EncoderState<'a, P> for RleState<'a, P> {
    fn append_chunk(&mut self, writer: &mut SlabWriter<'a>, chunk: Run<'a, P>) -> usize {
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
                    flush_run::<P>(writer, 1, a);
                    RleState::from(Run { count, value: b })
                }
            },
            RleState::Run { count, value } if chunk.value == value => {
                RleState::from(chunk.plus(count))
            }
            RleState::Run { count, value } => {
                flush_run::<P>(writer, count, value);
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

    fn flush(&mut self, writer: &mut SlabWriter<'a>) {
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

fn flush_run<'a, P: ?Sized + Packable>(
    writer: &mut SlabWriter<'a>,
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

#[derive(Debug, Default)]
pub struct Encoder<'a, C: ColumnCursor>
where
    C::Item: 'a,
{
    pub state: C::State<'a>,
    pub writer: SlabWriter<'a>,
    _phantom: PhantomData<C>,
}

impl<'a, C: ColumnCursor> Clone for Encoder<'a, C> {
    fn clone(&self) -> Self {
        Self {
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
    pub fn append<M: MaybePackable2<'a, C::Item>>(&mut self, value: M) -> usize {
        self.state.append(&mut self.writer, value.maybe_packable2())
    }

    pub fn append_item(&mut self, value: Option<Cow<'a, C::Item>>) -> usize {
        self.state.append(&mut self.writer, value)
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
        if let Some(bytes) = bytes {
            self.writer.flush_bytes(bytes);
        }
    }

    pub(crate) fn append_chunk(&mut self, run: Run<'a, C::Item>) -> usize {
        self.state.append_chunk(&mut self.writer, run)
    }

    pub fn new() -> Self {
        Self {
            state: C::State::default(),
            //writer: SlabWriter::new(C::slab_size(), 0),
            writer: SlabWriter::new(usize::MAX, 0),
            _phantom: PhantomData,
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            state: C::State::default(),
            writer: SlabWriter::new(usize::MAX, cap),
            _phantom: PhantomData,
        }
    }

    pub fn init(writer: SlabWriter<'a>, state: C::State<'a>) -> Self {
        Self {
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
        self.writer.copy(slab, range, lit, size, acc, bool_state)
    }

    pub fn flush(&mut self) {
        self.state.flush(&mut self.writer);
    }

    pub fn finish(mut self) -> Vec<Slab> {
        self.state.flush(&mut self.writer);
        self.writer.finish()
    }

    pub fn into_column_data(self) -> ColumnData<C> {
        let mut slabs = self.finish();
        C::compute_min_max(&mut slabs); // this should be handled by slabwriter.finish
        let mut col = ColumnData::default();
        col.len = slabs.iter().map(|s| s.len()).sum();
        col.slabs.splice(0..1, slabs);
        #[cfg(debug_assertions)]
        {
            col.debug = col.to_vec();
        }
        col
    }

    pub fn write(&mut self, slab: &'a Slab) {
        self.state.write::<C>(&mut self.writer, slab);
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

    pub fn append<M: MaybePackable2<'a, C::Item>>(&mut self, v: M) -> usize {
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
