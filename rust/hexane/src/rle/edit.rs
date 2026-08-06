//! Progressive editing of a single RLE slab.
//!
//! [`RleEdit`] is the slab half of a column cursor: it opens a rebuild at
//! one item, takes any mix of *pass* (carry the slab's own items
//! through), *insert* and *delete* moving forward, and closes by carrying
//! the untouched tail wholesale.
//!
//! One splice is `reset` + `delete` + `insert` + `close`. What a cursor
//! adds is staying open across several of them, so N edits landing in one
//! slab cost one partition, one buffer, one byte splice and one index
//! update rather than N of each — the shape real workloads have (94% of
//! the splices in an automerge fragment chain re-open the slab the
//! previous one just rebuilt).
//!
//! The cursor owns its encoder state, where a one-shot splice borrows the
//! slab, because it lives alongside a `&mut Column`. For a `Copy` value
//! that is a register move; string columns pay a clone per run carried.

use crate::edit::{EditSlab, SlabEdit};
use crate::encoding::RunDecoder;
use crate::rle::splice::{find_partition_at, postfix_from_run, Postfix};
use crate::rle::writer::SpliceWriter;
use crate::rle::{RleDecoder, RleDecoderState, RleTail, Slab};
use crate::{AsColumnRef, Codec, RleValue};

/// A rebuild in progress within one slab.
///
/// Positions are item offsets into the slab as it was when the edit
/// opened. `read` is how far the cursor has consumed the original; the
/// items emitted so far are `written`. They differ by the net edit.
pub struct RleEdit<T: RleValue, C: Codec> {
    /// the shared encoder — the same one a one-shot splice drives
    w: SpliceWriter<T, C>,
    /// Where reading stopped — every walk starts here and `close`
    /// describes the tail from here, so nothing is read twice. The same
    /// reader the cursor carries, so a rebuild picks up where the cursor
    /// stands rather than re-finding it.
    dec: RleDecoderState,
    /// Complete wire segments behind the reader. The writer needs it to
    /// describe the tail it carries; a decoder has no use for it, which
    /// is why it is counted here rather than inside one.
    read_segments: usize,
    read: usize,
    written: usize,
}

impl<T: RleValue, C: Codec> Default for RleEdit<T, C> {
    fn default() -> Self {
        RleEdit {
            w: SpliceWriter::empty(),
            dec: RleDecoderState::default(),
            read_segments: 0,
            read: 0,
            written: 0,
        }
    }
}

impl<T: RleValue, C: Codec> RleEdit<T, C> {
    /// An idle rebuild pointed at item `at`, for tests with no cursor to
    /// hold one across slabs.
    #[cfg(test)]
    pub(crate) fn open(slab: &Slab, at: usize) -> Self {
        let mut e = Self::default();
        e.reset(slab, at, usize::MAX);
        e
    }
}

impl<T: RleValue, C: Codec> RleEdit<T, C> {
    /// Move the read cursor `n` items forward, pushing them into the
    /// rebuild when `emit` — the difference between carrying items and
    /// dropping them. Returns what it did not take.
    ///
    /// Reading starts where the last walk stopped, so nothing is decoded
    /// twice. `budget` caps the pushes an emitting walk makes; a dropping
    /// one has to consume what it was asked for and passes none. `f` sees
    /// every run either way, which is how a wrapper keeping an
    /// accumulator — a delta column's running value — stays in step.
    fn walk<F>(&mut self, slab: &Slab, n: usize, emit: bool, budget: usize, f: &mut F) -> usize
    where
        F: FnMut(T::Get<'_>, usize),
    {
        let mut dec = RleDecoder::<T, C>::resume(&slab.data, &self.dec);
        let mut want = n;
        let mut pushes = 0usize;
        while want > 0 {
            if emit && pushes == budget {
                break;
            }
            // bounded by what is still wanted, so the decoder keeps the
            // rest of a run it only partly gave up — the position the
            // suspend below records
            let Some(run) = dec.next_run_max(want) else {
                debug_assert!(false, "walk ran past the end of the slab");
                break;
            };
            f(run.value, run.count);
            if emit {
                self.w.push(T::to_owned(run.value), run.count);
                self.written += run.count;
                pushes += 1;
            }
            self.read += run.count;
            want -= run.count;
            // a literal is one segment per value; a repeat or null run is
            // one segment however many items it holds, so it counts only
            // once the decoder has given up the last of them
            if dec.is_literal() || dec.remaining == 0 {
                self.read_segments += 1;
            }
        }
        self.dec = dec.suspend();
        want
    }

    /// The tail at the read cursor: what `close` carries through, and
    /// where its bytes start. Reads one run — the one the tail begins
    /// with, whose value the encoder may have to merge into what it just
    /// wrote. Everything after it stays bytes.
    fn tail<'s>(
        dec: &RleDecoderState,
        read_segments: usize,
        slab: &'s Slab,
    ) -> (Option<Postfix<'s, T>>, usize) {
        let mut dec = RleDecoder::<T, C>::resume(&slab.data, dec);
        // the run in hand is what the reader left of it, so there is no
        // consumed part to step over first
        let Some(run) = dec.next_run() else {
            // read to the end of the slab leaves no tail to carry
            return (None, slab.data.len());
        };
        let is_lit = dec.is_literal() && run.count == 1;
        let (p, end) = postfix_from_run(slab, &mut dec, 0, run, is_lit, run.count, read_segments);
        (Some(p), end)
    }
}

impl<T: RleValue, C: Codec> EditSlab for RleEdit<T, C> {
    type Tail = RleTail;
    type Value = T;
    type State = RleDecoderState;

    fn reset(&mut self, slab: &Slab, at: usize, max_segments: usize) {
        // one walk, both halves: the run the cut lands in for the writer
        // to re-emit, and a reader standing at the cut
        let p = find_partition_at::<T, C>(slab, at);
        self.w.reset(
            p.state.into_owned(),
            p.segments,
            at,
            p.byte_start..p.byte_start,
            max_segments,
        );
        self.dec = p.read_state;
        self.read_segments = p.read_segments;
        self.read = at;
        self.written = 0;
    }

    /// Carry `n` of the slab's items through the rebuild unchanged,
    /// spending at most `runs` pushes. Returns what it declined to carry;
    /// closing the rebuild is the only sensible answer to that.
    ///
    /// The budget is in runs because that is what the work is: carrying a
    /// 10,000-item run costs one push, ten literals cost ten.
    fn pass<F>(&mut self, slab: &Slab, n: usize, runs: usize, f: &mut F) -> usize
    where
        F: FnMut(T::Get<'_>, usize),
    {
        self.walk(slab, n, true, runs, f)
    }

    /// Insert `n` copies of `value` at the cursor.
    fn insert<V: AsColumnRef<T>>(&mut self, value: V, n: usize) {
        if n == 0 {
            return;
        }
        self.w.push(value, n);
        self.written += n;
    }

    /// Drop `n` of the slab's items at the cursor. Returns the count the
    /// slab could not supply, for the caller to cascade into the ones
    /// that follow.
    fn delete<F>(&mut self, slab: &Slab, n: usize, f: &mut F)
    where
        F: FnMut(T::Get<'_>, usize),
    {
        debug_assert!(self.read + n <= slab.len, "delete past the end of the slab");
        self.walk(slab, n, false, usize::MAX, f);
    }

    fn read_state(&self) -> &RleDecoderState {
        &self.dec
    }

    /// Finish: carry the slab's remaining bytes through wholesale, write
    /// the rebuilt region back, and return any slabs the rebuild spilled
    /// into (to be inserted after this one).
    fn close(&mut self, slab: &mut Slab) -> Vec<Slab> {
        // Only the tail's first run is read, from the point the cursor
        // stopped at — a close never re-reads what it already walked.
        let (tail, byte_end) = Self::tail(&self.dec, self.read_segments, slab);
        let postfix = tail.map(|p| p.into_owned());
        let tail_count = slab.len - self.read;
        let data_len = slab.data.len();
        let slab_tail = slab.tail;
        let w = &mut self.w;
        w.set_range_end(byte_end);
        // the tail borrow ends when `finish` returns, before `apply`
        // takes the slab mutably — no copy of the carried region
        let mut rebuilt = w.finish(
            postfix,
            tail_count,
            &slab.data[byte_end..],
            data_len,
            slab_tail,
        );
        let (spilled, bytes) = rebuilt.apply::<T, C>(slab);
        self.w.recycle(bytes);
        spilled
    }
}

/// The slab-level operations a column cursor drives. Implemented here
/// for the RLE encoding; an encoding without an implementation simply
/// has no cursor (nothing outside this module requires one yet).
impl<T, C> SlabEdit for crate::rle::RleEncoding<T, C>
where
    T: RleValue,
    C: Codec,
    T: crate::ColumnValueRef<Encoding<C> = Self>,
{
    type Edit = RleEdit<T, C>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::Leb128;
    use crate::encoding::EncoderApi;
    use crate::Encoder;

    fn slab_of(vals: &[u64]) -> Slab {
        Encoder::<u64>::encode_slab(vals.iter().copied())
    }

    fn values(slab: &Slab) -> Vec<u64> {
        RleDecoder::<u64, Leb128>::new(&slab.data).collect()
    }

    /// open → pass → insert → close, against the same edit expressed as
    /// a plain `Vec` splice.
    fn check(input: &[u64], script: impl Fn(&mut RleEdit<u64, Leb128>, &Slab), want: Vec<u64>) {
        let mut slab = slab_of(input);
        let orig = slab.clone();
        let mut e = RleEdit::<u64, Leb128>::open(&orig, 0);
        script(&mut e, &orig);
        e.close(&mut slab);
        assert_eq!(values(&slab), want, "values");
        assert_eq!(slab.len, want.len(), "len");
        // the encoding must be canonical: re-encoding the same values
        // from scratch produces the same bytes
        let fresh = slab_of(&want);
        assert_eq!(slab.data, fresh.data, "bytes");
        assert_eq!(slab.segments, fresh.segments, "segments");
    }

    #[test]
    fn insert_at_front() {
        check(&[1, 1, 2, 3], |e, _s| e.insert(9u64, 1), {
            let mut v = vec![9u64];
            v.extend([1, 1, 2, 3]);
            v
        });
    }

    #[test]
    fn pass_then_insert() {
        check(
            &[1, 1, 2, 3],
            |e, s| {
                e.pass(s, 2, usize::MAX, &mut |_, _| {});
                e.insert(9u64, 1);
            },
            vec![1, 1, 9, 2, 3],
        );
    }

    #[test]
    fn two_inserts_one_pass() {
        check(
            &[1, 1, 1, 1, 1, 1],
            |e, s| {
                e.pass(s, 1, usize::MAX, &mut |_, _| {});
                e.insert(7u64, 1);
                e.pass(s, 2, usize::MAX, &mut |_, _| {});
                e.insert(8u64, 2);
            },
            vec![1, 7, 1, 1, 8, 8, 1, 1, 1],
        );
    }

    #[test]
    fn delete_then_insert() {
        check(
            &[1, 2, 3, 4, 5],
            |e, s| {
                e.pass(s, 1, usize::MAX, &mut |_, _| {});
                e.delete(s, 2, &mut |_, _| {});
                e.insert(9u64, 1);
            },
            vec![1, 9, 4, 5],
        );
    }

    #[test]
    fn extends_a_run() {
        check(
            &[5, 5, 5, 5],
            |e, s| {
                e.pass(s, 2, usize::MAX, &mut |_, _| {});
                e.insert(5u64, 3);
            },
            vec![5, 5, 5, 5, 5, 5, 5],
        );
    }

    #[test]
    fn append_at_end() {
        let mut slab = slab_of(&[1, 2, 3]);
        let orig = slab.clone();
        let mut e = RleEdit::<u64, Leb128>::open(&orig, 3);
        e.insert(4u64, 1);
        e.close(&mut slab);
        assert_eq!(values(&slab), vec![1, 2, 3, 4]);
    }

    #[test]
    fn delete_only() {
        check(
            &[1, 2, 3, 4],
            |e, s| {
                e.pass(s, 1, usize::MAX, &mut |_, _| {});
                e.delete(s, 2, &mut |_, _| {});
            },
            vec![1, 4],
        );
    }

    /// Every single edit must land byte-identically to the splice that
    /// expresses the same thing — that is the oracle for the whole
    /// design, since `splice_slab` is what the column uses today.
    #[test]
    fn matches_splice_slab() {
        use crate::rle::splice::splice_slab;
        let input: Vec<u64> = vec![1, 1, 1, 2, 3, 3, 4, 5, 5, 5, 6, 7, 8, 8];
        for at in 0..=input.len() {
            for del in 0..=(input.len() - at).min(3) {
                for ins in [vec![], vec![9u64], vec![9, 9], vec![3, 3, 3]] {
                    let mut a = slab_of(&input);
                    let orig = a.clone();
                    let mut e = RleEdit::<u64, Leb128>::open(&orig, at);
                    e.delete(&orig, del, &mut |_, _| {});
                    for v in &ins {
                        e.insert(*v, 1);
                    }
                    e.close(&mut a);

                    let mut b = slab_of(&input);
                    let overflow = splice_slab::<u64, u64, Leb128>(
                        &mut b,
                        at,
                        del,
                        ins.iter().map(|v| (*v, 1)),
                        usize::MAX,
                    );
                    assert!(overflow.is_empty());
                    assert_eq!(
                        values(&a),
                        values(&b),
                        "values at={at} del={del} ins={ins:?}"
                    );
                    assert_eq!(a.data, b.data, "bytes at={at} del={del} ins={ins:?}");
                    assert_eq!(a.len, b.len, "len at={at} del={del} ins={ins:?}");
                    assert_eq!(
                        a.segments, b.segments,
                        "segments at={at} del={del} ins={ins:?}"
                    );
                }
            }
        }
    }
}
