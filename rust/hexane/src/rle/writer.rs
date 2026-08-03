//! The splice writer: encode a rebuilt region into a byte buffer, cutting
//! new slabs at the segment budget as it goes.
//!
//! This is the cut-and-overflow logic of
//! `build_splice_buf` lifted into a
//! type that can be *driven* rather than only called. Two callers want
//! it:
//!
//! * a one-shot splice — push the inserted values, finish with the
//!   postfix (see `build_splice_buf_v2`, held to byte-identity with the
//!   original in `writer_matches_v1`);
//! * a cursor making several edits in one slab — push the inserted
//!   values *and* the runs it carries through between them, then finish
//!   once. That is what makes N edits in a slab cost one partition, one
//!   buffer and one byte splice.
//!
//! The writer owns its buffer and its encoder state, so it can live
//! across calls next to a `&mut Column`; the state holds owned values
//! ([`RleState::into_owned`]), which is a register move for every `Copy`
//! column and a clone per retained run for the string ones.

use crate::rle::state::{FlushState, OwnedPostfix, RleState};
use crate::rle::{RleTail, Slab};
use crate::{AsColumnRef, Codec, RleValue};
use std::ops::Range;

/// What a finished rebuild hands back: the bytes that replace
/// `range` in the slab, the slab's new metadata, and any slabs the
/// rebuild spilled into.
#[derive(Debug, Default)]
pub(crate) struct Rebuilt {
    pub(crate) bytes: Vec<u8>,
    pub(crate) range: Range<usize>,
    pub(crate) len: usize,
    pub(crate) segments: usize,
    pub(crate) rewrite: Option<crate::rle::state::RewriteHeader>,
    pub(crate) overflow: Vec<Slab>,
    pub(crate) wpos: crate::rle::state::WPos,
}

/// Encodes a rebuilt region, cutting slabs at the budget.
pub(crate) struct SpliceWriter<T: RleValue, C: Codec> {
    state: RleState<'static, T, T, C>,
    f: FlushState,
    buf: Vec<u8>,
    max_segments: usize,
    /// budget for the chunk in hand — halves after the first cut, so a
    /// spill leaves room for later growth rather than filling to the brim
    target: usize,
    /// segments standing before the chunk in hand
    starting_segments: usize,
    /// segments of the slab before the rebuilt region, which stay put
    prefix_segments: usize,
    /// items of the slab before the rebuilt region — they stay in the
    /// edited slab, so its length counts them
    head_len: usize,
    overflowed: bool,
    /// items emitted into the chunk in hand
    inserted: usize,
    out: Rebuilt,
}

impl<T: RleValue, C: Codec> SpliceWriter<T, C> {
    /// An idle writer, holding nothing but its buffer.
    ///
    /// A cursor builds one of these and [`reset`](Self::reset)s it for
    /// each slab it edits, rather than constructing a writer per slab: the
    /// struct is a few hundred bytes, and on a one-edit cursor that
    /// construction is a measurable share of the whole operation. It also
    /// means the encode buffer is allocated once for the cursor's life.
    pub(crate) fn empty() -> Self {
        SpliceWriter {
            state: RleState::empty(),
            f: FlushState::default(),
            buf: Vec::new(),
            max_segments: 0,
            target: 0,
            starting_segments: 0,
            prefix_segments: 0,
            head_len: 0,
            overflowed: false,
            inserted: 0,
            out: Rebuilt::default(),
        }
    }

    /// Open a rebuild whose encoder state and segment count come from a
    /// partition, replacing `range` of the slab's bytes.
    ///
    /// Every field is written, so nothing carries over from the last slab
    /// this writer rebuilt.
    pub(crate) fn reset(
        &mut self,
        state: RleState<'static, T, T, C>,
        prefix_segments: usize,
        head_len: usize,
        range: Range<usize>,
        max_segments: usize,
    ) {
        self.state = state;
        self.f = FlushState::default();
        self.buf.clear();
        self.max_segments = max_segments;
        self.target = max_segments;
        self.starting_segments = prefix_segments;
        self.prefix_segments = prefix_segments;
        self.head_len = head_len;
        self.overflowed = false;
        self.inserted = 0;
        self.out = Rebuilt {
            range,
            ..Default::default()
        };
    }

    /// Take back the buffer a finished rebuild wrote into, so the next one
    /// reuses the allocation.
    pub(crate) fn recycle(&mut self, mut bytes: Vec<u8>) {
        if bytes.capacity() > self.buf.capacity() {
            bytes.clear();
            self.buf = bytes;
        }
    }

    /// Append `count` copies of `value`, cutting a slab first if the
    /// chunk in hand has reached its budget.
    pub(crate) fn push(&mut self, value: impl AsColumnRef<T>, count: usize) {
        if self.starting_segments + self.f.segments + self.state.pending_segments() >= self.target {
            self.cut();
        }
        self.inserted += count;
        let value = T::to_owned(value.as_column_ref());
        self.f += self.state.append_n(&mut self.buf, value, count);
    }

    /// The rebuilt region's end, known only when the rebuild closes.
    pub(crate) fn set_range_end(&mut self, end: usize) {
        self.out.range.end = end;
    }

    fn cut(&mut self) {
        self.f += self.state.flush(&mut self.buf);
        if !self.overflowed {
            self.overflowed = true;
            self.target = self.max_segments / 2;
            self.out.wpos = self.f.wpos;
            self.out.bytes = std::mem::take(&mut self.buf);
            self.out.len = self.head_len + self.inserted;
            self.out.segments = self.prefix_segments + self.f.segments;
            self.out.rewrite = self.f.rewrite;
        } else {
            let tail = self.f.wpos.as_tail(0, self.buf.len());
            self.out.overflow.push(Slab {
                data: std::mem::take(&mut self.buf),
                len: self.inserted,
                segments: self.f.segments,
                tail,
            });
        }
        self.state = RleState::empty();
        self.f = FlushState::default();
        self.inserted = 0;
        self.starting_segments = 0;
    }

    /// Close the rebuild, carrying the slab's remaining bytes through as
    /// the postfix.
    ///
    /// `head_len` is the item count of the slab's untouched head (the
    /// items before `range.start`), `tail_count` the item count the
    /// postfix stands for, and `data_len` the slab's current byte length.
    /// `tail_bytes` is the slab's bytes from `range.end` to its end —
    /// the region the postfix stands for, carried through unchanged. In
    /// the spill case it moves onto the last new slab (which is why the
    /// replaced range then covers the whole tail); when the rebuild fits
    /// in one slab it stays where it is and this is unused.
    pub(crate) fn finish(
        &mut self,
        postfix: Option<OwnedPostfix<T>>,
        tail_count: usize,
        tail_bytes: &[u8],
        data_len: usize,
        slab_tail: RleTail,
    ) -> Rebuilt {
        let postfix_segments = postfix.as_ref().map(|p| p.segments()).unwrap_or(0);
        let postfix_pending = postfix.as_ref().map(|p| p.pending()).unwrap_or(0);
        let mut postfix = postfix;

        if !self.overflowed {
            let pending = self.state.pending_segments() + postfix_pending;
            if self.prefix_segments + pending + self.f.segments + postfix_segments
                <= self.max_segments
            {
                // it all fits in the one slab
                self.f += self
                    .state
                    .flush_postfix_owned(&mut self.buf, postfix.take())
                    .0;
                self.out.bytes = std::mem::take(&mut self.buf);
                self.out.wpos = self.f.wpos;
                self.out.rewrite = self.f.rewrite;
                self.out.len = self.head_len + self.inserted + tail_count;
                self.out.segments = self.prefix_segments + self.f.segments + postfix_segments;
                return std::mem::take(&mut self.out);
            }
            // spill: the edited slab keeps what is built so far
            self.f += self.state.flush(&mut self.buf);
            self.out.bytes = std::mem::take(&mut self.buf);
            self.out.wpos = self.f.wpos;
            self.out.rewrite = self.f.rewrite;
            self.out.len = self.head_len + self.inserted;
            self.out.segments = self.prefix_segments + self.f.segments;
            self.f = FlushState::default();
            self.inserted = 0;
        }

        // if the postfix will not fit on the chunk in hand, close it
        let pending = self.state.pending_segments() + postfix_pending;
        if pending + self.f.segments + postfix_segments > self.max_segments {
            self.f += self.state.flush(&mut self.buf);
            let tail = self.f.wpos.as_tail(0, self.buf.len());
            self.out.overflow.push(Slab {
                data: std::mem::take(&mut self.buf),
                len: self.inserted,
                segments: self.f.segments,
                tail,
            });
            self.state = RleState::empty();
            self.f = FlushState::default();
            self.inserted = 0;
        }

        self.f += self
            .state
            .flush_postfix_owned(&mut self.buf, postfix.take())
            .0;

        // the postfix moved onto the last new slab, so the whole tail of
        // the original is replaced
        self.out.range.end = data_len;

        let len = self.inserted + tail_count;
        let segments = self.f.segments + postfix_segments;
        let tail = self
            .f
            .wpos
            .merge(0, self.buf.len(), tail_bytes.len(), slab_tail);
        self.buf.extend_from_slice(tail_bytes);
        self.out.overflow.push(Slab {
            data: std::mem::take(&mut self.buf),
            len,
            segments,
            tail,
        });
        std::mem::take(&mut self.out)
    }
}

impl Rebuilt {
    /// Write the rebuild back into the slab it came from, returning the
    /// slabs it spilled into. Mirrors what `splice_slab` does with a
    /// `SpliceBuf`, which is the only other consumer of this shape.
    /// Returns the spilled slabs and the encode buffer, so the caller can
    /// reuse the allocation for the next slab it edits.
    // T is only read by the debug-build slab validation below
    #[cfg_attr(not(debug_assertions), allow(clippy::extra_unused_type_parameters))]
    pub(crate) fn apply<T: RleValue, C: Codec>(&mut self, slab: &mut Slab) -> (Vec<Slab>, Vec<u8>) {
        let range = self.range.clone();
        let mut prefix = range.start as i64;
        let middle = self.bytes.len();
        let postfix = slab.data.len() - range.end;

        crate::column::splice_bytes(&mut slab.data, range.start, range.len(), &self.bytes);

        if let Some(rw) = self.rewrite {
            prefix += C::rewrite_lit_header(&mut slab.data, rw.pos, rw.count);
        }

        slab.tail = self.wpos.merge(prefix as usize, middle, postfix, slab.tail);
        slab.len = self.len;
        slab.segments = self.segments;

        #[cfg(debug_assertions)]
        crate::rle::validate_rle_slab::<T, C>(slab);
        #[cfg(debug_assertions)]
        for s in &self.overflow {
            crate::rle::validate_rle_slab::<T, C>(s);
        }

        let mut bytes = std::mem::take(&mut self.bytes);
        bytes.clear();
        (std::mem::take(&mut self.overflow), bytes)
    }
}
