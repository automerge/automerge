use crate::rle::state::{FlushState, OwnedPostfix, RleState};
use crate::rle::{RleTail, Slab};
use crate::{AsColumnRef, Codec, RleValue};
use std::ops::Range;

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

pub(crate) struct SpliceWriter<T: RleValue, C: Codec> {
    state: RleState<'static, T, T, C>,
    f: FlushState,
    buf: Vec<u8>,
    max_segments: usize,
    target: usize,
    starting_segments: usize,
    prefix_segments: usize,
    head_len: usize,
    overflowed: bool,
    inserted: usize,
    out: Rebuilt,
}

impl<T: RleValue, C: Codec> SpliceWriter<T, C> {
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

    pub(crate) fn recycle(&mut self, mut bytes: Vec<u8>) {
        if bytes.capacity() > self.buf.capacity() {
            bytes.clear();
            self.buf = bytes;
        }
    }

    pub(crate) fn push(&mut self, value: impl AsColumnRef<T>, count: usize) {
        if self.starting_segments + self.f.segments + self.state.pending_segments() >= self.target {
            self.cut();
        }
        self.inserted += count;
        let value = T::to_owned(value.as_column_ref());
        self.f += self.state.append_n(&mut self.buf, value, count);
    }

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
