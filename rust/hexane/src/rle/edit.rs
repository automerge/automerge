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

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::len_zero, clippy::assign_op_pattern)]

use crate::edit::{EditSlab, SlabEdit};
#[cfg(verus_keep_ghost)]
use crate::rle::dec::*;
use crate::rle::dec::{partition_at, postfix_at, Dec, RleDecoderState};
use crate::rle::lift::*;
use crate::rle::model::*;
#[cfg(verus_keep_ghost)]
use crate::rle::postfix::*;
use crate::rle::state::*;
#[cfg(verus_keep_ghost)]
use crate::rle::writer::*;
use crate::rle::writer::{add, Rebuilt, Splice};
use crate::rle::{RleTail, Slab};
use crate::{AsColumnRef, Codec, RleValue};
#[allow(unused_imports)]
use vstd::prelude::*;

verus! {

/// A rebuild in progress within one slab, verified with Verus: the
/// values written so far ([`vals`](Edit::vals)), and how far the slab
/// has been read ([`read`](Edit::read)). Closing it leaves the slab, and
/// any it spills into, the canonical encoding of the values written then
/// the slab's from where reading stopped.
///
/// `M` writes owned values; each call reads the slab through a
/// borrowed-value model `L` that [`Lift`]s to it.
pub(crate) struct Edit<M: WriteOps<'static>> {
    /// the shared encoder — the same one a one-shot splice drives
    w: Splice<'static, M>,
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
    /// the slab's bytes
    data: Ghost<Seq<u8>>,
}

impl<M: WriteOps<'static>> Edit<M> {
    /// The slab's bytes.
    pub(crate) closed spec fn data(&self) -> Seq<u8> {
        self.data@
    }

    /// The values written: the slab's before the edit, then what the
    /// edit carried through and inserted.
    pub(crate) closed spec fn vals(&self) -> Seq<VOf<'static, M>> {
        self.w.vals()
    }

    /// How many of the slab's values have been read.
    pub(crate) closed spec fn read(&self) -> int {
        self.read as int
    }

    /// Room for `n` more values.
    pub(crate) closed spec fn room(&self, n: nat) -> bool {
        self.w.room(n)
    }

    /// The slab's bytes before the edit, and the count in its literal
    /// header there.
    pub(crate) closed spec fn pre(&self) -> Seq<u8> {
        self.w.pre()
    }

    pub(crate) closed spec fn h0(&self) -> i64 {
        self.w.h0()
    }

    closed spec fn core(&self) -> bool {
        &&& valid_slab::<M::R>(self.data@) && self.data@.len() < i64::MAX
        &&& self.w.inv()
        &&& self.w.pre().len() <= self.data@.len()
        &&& self.w.pre() == self.data@.take(self.w.pre().len() as int)
        &&& self.read <= slab_values::<M::R>(self.data@).len()
    }

    /// Open, between walks.
    pub(crate) closed spec fn inv(&self) -> bool {
        &&& self.core()
        &&& state_ok::<M::R>(self.data@, self.dec) && state_pos::<M::R>(self.data@, self.dec) == self.read
        &&& self.w.pre().len() <= sbyte(self.dec)
        &&& self.read_segments == rsegs::<M::R>(self.data@, self.dec)
    }

    /// Open, walking with the reader `d`.
    pub(crate) closed spec fn walking<'s, O: RleOps<'s>>(&self, d: Dec<'s, O>) -> bool {
        &&& self.core()
        &&& d.inv() && d.data() == self.data@ && d.pos() == self.read
        &&& self.w.pre().len() <= sbyte(d.dstate())
        &&& self.read_segments == d.dsegs()
    }

    pub(crate) fn new() -> (r: Self) {
        Edit {
            w: Splice::empty(),
            dec: RleDecoderState::start(),
            read_segments: 0,
            read: 0,
            written: 0,
            data: Ghost(Seq::empty()),
        }
    }

    /// The reader's position.
    pub(crate) fn dec(&self) -> &RleDecoderState {
        &self.dec
    }

    /// Open a rebuild of the valid slab `data` at item `at`. One walk,
    /// both halves: the run the cut lands in for the writer to re-emit,
    /// and a reader standing at the cut.
    pub(crate) fn reset<'s, L: Lift<'s, W = M>>(&mut self, data: &'s [u8], at: usize, max_segments: usize)
        requires
            valid_slab::<L::R>(data@), data@.len() < i64::MAX,
            at <= slab_values::<L::R>(data@).len(),
        ensures
            final(self).inv(), final(self).data() == data@, final(self).read() == at,
            final(self).vals() == slab_values::<M::R>(data@).take(at as int),
    {
        let p = partition_at::<L>(data, at);
        let ghost bs = p.byte_start as int;
        let ghost pre = data@.take(bs);
        let ghost (gs, ws, h0) = choose|gs: Seq<Group<VOf<'s, L>>>, ws: Seq<VOf<'s, L>>, h0: i64|
            #[trigger] rel::<L>(p.state, pre, Seq::empty(), 0, gs, ws, h0)
            && values_of::<L>(p.state, gs, ws) == slab_values::<L::R>(data@).take(at as int)
            && acc::<L>(p.state, gs, ws) == p.segments;
        let state = own_state::<L>(p.state);
        let ghost vals = slab_values::<M::R>(data@).take(at as int);
        proof {
            lemma_lift_rel::<L>(p.state, state, pre, Seq::empty(), 0, gs, ws, h0);
            lemma_lift_slab::<L>(data@);
            assert(lift_seq::<L>(slab_values::<L::R>(data@).take(at as int)) =~= vals);
        }
        self.w.reset(state, p.segments, at, p.byte_start, max_segments, Ghost(pre), Ghost(h0), Ghost(vals));
        self.dec = p.read_state;
        self.read_segments = p.read_segments;
        self.read = at;
        self.written = 0;
        self.data = Ghost(data@);
        proof {
            lemma_lift_state::<L>(data@, p.read_state);
            lemma_slab::<M::R>(data@);
        }
    }

    /// Start walking: a reader where reading stopped.
    pub(crate) fn walk_begin<'s, L: Lift<'s, W = M>>(&self, data: &'s [u8]) -> (d: Dec<'s, L::R>)
        requires self.inv(), data@ == self.data(),
        ensures self.walking(d),
    {
        proof {
            lemma_unlift_slab::<L>(data@);
            lemma_lift_state::<L>(data@, self.dec);
        }
        let d = Dec::resume(data, &self.dec);
        proof {
            d.lemma_dstate();
        }
        d
    }

    /// Read the next run, of at most `want` values, carrying it through
    /// the rebuild if `emit`, else dropping it. `None` at the end of the
    /// slab.
    pub(crate) fn walk_step<'s, L: Lift<'s, W = M>>(&mut self, d: &mut Dec<'s, L::R>, want: usize, emit: bool) -> (r: Option<crate::Run<VOf<'s, L>>>)
        requires old(self).walking(*old(d)), want >= 1, emit ==> old(self).room(want as nat),
        ensures
            final(self).walking(*final(d)), final(self).data() == old(self).data(),
            final(self).pre() == old(self).pre(), final(self).h0() == old(self).h0(),
            match r {
                Some(run) => {
                    &&& 1 <= run.count <= want
                    &&& final(self).read() == old(self).read() + run.count
                    &&& slab_values::<L::R>(old(self).data()).subrange(old(self).read(), final(self).read())
                        == copies(run.count as nat, run.value)
                    &&& final(self).vals() == old(self).vals()
                        + if emit { copies(run.count as nat, L::lift(run.value)) } else { Seq::empty() }
                },
                None => {
                    &&& final(self).read() == old(self).read() && final(self).vals() == old(self).vals()
                    &&& old(self).read() == slab_values::<M::R>(old(self).data()).len()
                },
            },
    {
        let ghost d0 = *d;
        let ghost data = self.data@;
        proof {
            lemma_unlift_slab::<L>(data);
            lemma_lift_slab::<L>(data);
            d0.lemma_dstate();
            lemma_slab::<L::R>(data);
        }
        let run = match d.next_run_max(want) {
            Some(run) => run,
            None => {
                proof {
                    d.lemma_dstate();
                    lemma_sbyte_end::<L::R>(data, d.dstate());
                }
                return None;
            },
        };
        let ghost lv = slab_values::<L::R>(data);
        proof {
            assert(lv.subrange(self.read as int, self.read + run.count) =~= copies(run.count as nat, run.value));
            d.lemma_dstate();
            lemma_sbyte_mono::<L::R>(data, d0.dstate(), d.dstate());
            lemma_rsegs_le::<L::R>(data, d.dstate());
        }
        if emit {
            proof {
                L::axiom_lift(run.value);
                L::axiom_lift_codec(0, 0);
                if <L::R as RleOps<'s>>::is_null(run.value) {
                    assert(lv[self.read as int] == run.value);
                    lemma_slab_null::<L::R>(data, self.read as int);
                }
            }
            proof {
                self.w.lemma_room_le(run.count as nat, want as nat);
            }
            self.w.push_cow(L::own(L::cow_ref(run.value)), run.count);
            self.written = add(self.written, run.count);
        }
        self.read = self.read + run.count;
        // a literal is one segment per value; a repeat or null run is
        // one segment however many items it holds, so it counts only
        // once the decoder has given up the last of them
        if d.is_literal() || d.left() == 0 {
            self.read_segments = self.read_segments + 1;
        }
        Some(run)
    }

    /// Stop walking: keep where reading stopped.
    pub(crate) fn walk_end<'s, L: Lift<'s, W = M>>(&mut self, d: Dec<'s, L::R>)
        requires old(self).walking(d),
        ensures
            final(self).inv(), final(self).data() == old(self).data(), final(self).read() == old(self).read(),
            final(self).vals() == old(self).vals(),
    {
        self.dec = d.suspend();
        proof {
            d.lemma_dstate();
            lemma_unlift_slab::<L>(self.data@);
            lemma_lift_state::<L>(self.data@, self.dec);
        }
    }

    /// Insert `n` copies of `value` at the cursor.
    pub(crate) fn insert_cow(&mut self, value: M::Cow, n: usize)
        requires
            old(self).inv(), old(self).room(n as nat),
            <M::R as RleOps<'static>>::is_null(M::val(value)) ==> <M::R as RleOps<'static>>::nullable(),
        ensures
            final(self).inv(), final(self).data() == old(self).data(), final(self).read() == old(self).read(),
            final(self).vals() == old(self).vals() + copies(n as nat, M::val(value)),
    {
        if n == 0 {
            assert(old(self).vals() + copies(0, M::val(value)) =~= old(self).vals());
            return;
        }
        self.w.push_cow(value, n);
        self.written = add(self.written, n);
    }

    /// Room to close: for the slab's values not yet read.
    pub(crate) closed spec fn room_close(&self) -> bool {
        self.w.room((slab_values::<M::R>(self.data@).len() - self.read) as nat)
    }

    /// Close: carry the slab's values from where reading stopped (the
    /// first run as values, the rest as bytes) and finish. Only the
    /// first run after the cursor is read — a close never re-reads what
    /// it already walked.
    pub(crate) fn close_rebuild<'s, L: Lift<'s, W = M>>(&mut self, data: &'s [u8], slab_segments: usize, slab_len: usize, slab_tail: RleTail) -> (r: Rebuilt)
        requires
            old(self).inv(), data@ == old(self).data(), old(self).room_close(),
            slab_len == slab_values::<M::R>(data@).len(),
            slab_segments == groups_segments(slab_groups::<M::R>(data@)),
        ensures
            old(self).pre() == data@.take(old(self).pre().len() as int),
            r.h0@ == old(self).h0(),
            rebuilt_ok::<M>(r, old(self).pre(), old(self).h0(), data@,
                old(self).vals() + slab_values::<M::R>(data@).skip(old(self).read())),
    {
        proof {
            lemma_unlift_slab::<L>(data@);
            lemma_lift_state::<L>(data@, self.dec);
        }
        let (p, end) = postfix_at::<L::R>(data, &self.dec);
        proof {
            lemma_lift_slab::<L>(data@);
            lemma_lift_groups::<L>(slab_groups::<L::R>(data@));
            lemma_lift_postfix::<L>(data@, self.read as int, p, end as int);
            lemma_rest_bound::<M::R>(data@, self.read as int, plift::<L>(p), end as int);
        }
        let postfix_segments = match &p {
            Some(q) => slab_segments - self.read_segments - q.pending(),
            None => 0,
        };
        let ghost lp = p;
        let postfix = own_postfix::<L>(p);
        let tail_count = slab_len - self.read;
        proof {
            lemma_lift_postfix::<L>(data@, self.read as int, lp, end as int);
            lemma_plen_bound::<M::R>(data@, self.read as int, plift::<L>(lp), end as int);
            lemma_pmap::<M>(postfix);
            self.w.lemma_room_le(plen_of(postfix), (slab_values::<M::R>(data@).len() - self.read) as nat);
            self.w.lemma_room_le(tail_count as nat, (slab_values::<M::R>(data@).len() - self.read) as nat);
        }
        self.w.set_range_end(end);
        let tail = &data[end..];
        assert(tail@ == data@.skip(end as int));
        let r = self.w.finish(postfix, postfix_segments, tail_count, tail, data.len(), slab_tail);
        proof {
            let ghost w0 = old(self).w;
            assert(postfix_ok::<M::R>(data@, self.read as int, pmap::<M>(postfix), end as int));
        }
        r
    }
}

} // verus!

/// A rebuild in progress within one slab.
///
/// Positions are item offsets into the slab as it was when the edit
/// opened. The verified core is [`Edit`]; this drives it with the
/// slab's bytes, and keeps the slab metadata.
pub struct RleEdit<T: RleValue, C: Codec> {
    e: Edit<WOps<'static, T, T, C>>,
}

impl<T: RleValue, C: Codec> Default for RleEdit<T, C> {
    fn default() -> Self {
        RleEdit { e: Edit::new() }
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
        let mut d = self.e.walk_begin::<WOps<'_, T, T, C>>(&slab.data);
        let mut want = n;
        let mut pushes = 0usize;
        while want > 0 {
            if emit && pushes == budget {
                break;
            }
            // bounded by what is still wanted, so the decoder keeps the
            // rest of a run it only partly gave up — the position the
            // suspend below records
            let Some(run) = self.e.walk_step::<WOps<'_, T, T, C>>(&mut d, want, emit) else {
                debug_assert!(false, "walk ran past the end of the slab");
                break;
            };
            f(run.value, run.count);
            if emit {
                pushes += 1;
            }
            want -= run.count;
        }
        self.e.walk_end::<WOps<'_, T, T, C>>(d);
        want
    }
}

impl<T: RleValue, C: Codec> EditSlab for RleEdit<T, C> {
    type Tail = RleTail;
    type Value = T;
    type State = RleDecoderState;

    fn reset(&mut self, slab: &Slab, at: usize, max_segments: usize) {
        self.e
            .reset::<WOps<'_, T, T, C>>(&slab.data, at, max_segments);
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
        let value = T::to_owned(value.as_column_ref());
        self.e.insert_cow(RleCow::Owned(value), n);
    }

    /// Drop `n` of the slab's items at the cursor. Returns the count the
    /// slab could not supply, for the caller to cascade into the ones
    /// that follow.
    fn delete<F>(&mut self, slab: &Slab, n: usize, f: &mut F)
    where
        F: FnMut(T::Get<'_>, usize),
    {
        debug_assert!(n <= slab.len, "delete past the end of the slab");
        self.walk(slab, n, false, usize::MAX, f);
    }

    fn read_state(&self) -> &RleDecoderState {
        self.e.dec()
    }

    /// Finish: carry the slab's remaining bytes through wholesale, write
    /// the rebuilt region back, and return any slabs the rebuild spilled
    /// into (to be inserted after this one).
    fn close(&mut self, slab: &mut Slab) -> Vec<Slab> {
        // verified with Verus: the rebuild (`Edit::close_rebuild`) leaves
        // `rebuilt_ok`, and writing it back (`Rebuilt::apply_bytes`) gives
        // the slab those bytes
        let mut rebuilt = self.e.close_rebuild::<WOps<'_, T, T, C>>(
            &slab.data,
            slab.segments,
            slab.len,
            slab.tail,
        );
        let (spilled, bytes) = rebuilt.apply::<T, C>(slab);
        self.e.w.recycle(bytes);
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
    use crate::rle::RleDecoder;
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
