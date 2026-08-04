//! Progressive editing: a forward-only cursor over a
//! [`Column`], and the contract an encoding implements so
//! its slabs can be rebuilt in place.
//!
//! Nothing here is specific to an encoding — [`RleEdit`](crate::rle::edit::RleEdit)
//! and [`BoolEdit`](crate::bool::BoolEdit) are the implementations.

use crate::column::{Column, ColumnRef, WeightFn};
use crate::encoding::ColumnEncoding;
use crate::index::ColumnIndex;
use crate::{AsColumnRef, Codec, ColumnValueRef, Run};
use std::ops::Range;

/// An encoding whose slabs a [`Column`] cursor can edit.
pub trait SlabEdit: crate::encoding::ColumnEncoding {
    type Edit: EditSlab<Tail = Self::Tail, Value = Self::Value, State = Self::State>;

    /// How many runs a cursor carries through an open rebuild before
    /// closing it instead — the trade between re-encoding what it carries
    /// and paying for a write-back.
    ///
    /// It belongs to the encoding because the two sides differ: carrying
    /// an RLE run costs a decode and a re-encode against a cheap byte
    /// splice, where a boolean run is arithmetic against the same.
    const CARRY_BUDGET: usize = RUN_BUDGET;
}

/// A rebuild in progress on one slab. A cursor keeps one for its whole
/// life and resets it onto each slab it touches.
pub trait EditSlab: Default {
    type Tail: Copy + std::fmt::Debug + Default;
    type Value: crate::ColumnValueRef;
    /// The reader position, shared with [`ColumnEncoding::State`]: a
    /// rebuild carries one, and hands it out for reads while it is open.
    type State: Clone;

    /// Point the rebuild at item `at` of `slab`.
    fn reset(&mut self, slab: &crate::column::Slab<Self::Tail>, at: usize, max_segments: usize);

    /// Carry `n` items through the rebuild, spending at most `runs`
    /// pushes; returns the items left uncarried.
    ///
    /// `f` sees every run passed or dropped, which is how a wrapper
    /// keeping an accumulator — a delta column's running value — stays in
    /// step without reading the slab again.
    fn pass<F>(
        &mut self,
        slab: &crate::column::Slab<Self::Tail>,
        n: usize,
        runs: usize,
        f: &mut F,
    ) -> usize
    where
        F: FnMut(<Self::Value as crate::ColumnValueRef>::Get<'_>, usize);

    fn insert<V: AsColumnRef<Self::Value>>(&mut self, value: V, n: usize);

    /// Drop `n` items. The cursor never asks for more than the slab
    /// holds — it closes at the boundary and goes on into the next.
    fn delete<F>(&mut self, slab: &crate::column::Slab<Self::Tail>, n: usize, f: &mut F)
    where
        F: FnMut(<Self::Value as crate::ColumnValueRef>::Get<'_>, usize);

    /// The rebuild's own reader, which stands at the cursor.
    fn read_state(&self) -> &Self::State;

    /// Write the rebuild back, returning the slabs it spilled into.
    fn close(
        &mut self,
        slab: &mut crate::column::Slab<Self::Tail>,
    ) -> Vec<crate::column::Slab<Self::Tail>>;
}

// ── Progressive editing ─────────────────────────────────────────────────────

/// The encoding a cursor drives, and the rebuild it owns.
type Enc<T, C> = <T as ColumnValueRef>::Encoding<C>;
type EditOf<T, C> = <Enc<T, C> as SlabEdit>::Edit;
type StateOf<T, C> = <Enc<T, C> as crate::encoding::ColumnEncoding>::State;

/// A cursor's reader, and the offset in the current slab it stands at.
///
/// `at` trails the cursor's `in_slab` — the reader only catches up when
/// something reads — so the two are not the same number and the gap is
/// the work a read still owes.
struct Reader<S> {
    state: S,
    at: usize,
}

/// The default carry budget, in runs — an encoding states its own as
/// [`SlabEdit::CARRY_BUDGET`].
///
/// Carrying costs a decode and a push per run; closing costs a partition,
/// a byte splice and an index update, and leaves the skipped bytes where
/// they are. In runs rather than items because that is what the work is:
/// a long span of one value is carried for nothing, the same span of
/// distinct values is better memmoved.
///
/// 8 is the knee of a sweep over `edit_vs_splice`. Flushing sooner loses
/// everywhere (2 runs costs 1.5× on the string shapes); past 8 nothing
/// moves, since `est_runs` has already dispatched the long spans. Never
/// flushing costs 1.3× to 4.9×.
pub(crate) const RUN_BUDGET: usize = 8;

/// A forward-only read/write cursor over a [`Column`].
///
/// ```ignore
/// let mut e = col.edit_at(0);
/// e.seek(10).delete(5).insert(value).seek(100).insert_runs(runs);
/// e.finish();
/// ```
///
/// Seeks take positions in the column as it was when the cursor opened,
/// so a caller holding a list of edits applies them in the order it has
/// them. It keeps one slab open at a time, so edits landing in the slab
/// it already stands in cost nothing extra — no re-locate, no second
/// buffer, no second byte splice, no second index update. That is the
/// shape real workloads have: in an automerge fragment chain 94% of
/// splices land in the slab the previous one just rebuilt.
pub struct Edit<'a, T, C, WF, Idx>
where
    T: ColumnValueRef,
    C: Codec,
    T::Encoding<C>: SlabEdit,
    WF: WeightFn<T, C>,
    Idx: ColumnIndex<WF::Weight>,
{
    col: &'a mut Column<T, C, WF, Idx>,
    /// index of the slab the cursor is standing in
    slab: usize,
    /// item offset of the cursor within that slab, in the slab's
    /// *original* (pre-edit) coordinates
    in_slab: usize,
    /// absolute cursor position in the column's *original* coordinates —
    /// how many of its own items the cursor has consumed. This is what
    /// callers seek in: every caller (splice lists, merges, succ writes)
    /// holds pre-edit positions, so making them convert would just move
    /// the drift bookkeeping outside.
    orig: usize,
    /// the matching position in the column as edited so far, needed to
    /// re-find the cursor's slab after a write-back
    out: usize,
    /// the rebuild, reset onto each slab this cursor edits — one of
    /// these lives for the whole cursor, holding the encode buffer, so a
    /// slab session costs a few field writes rather than building a few
    /// hundred bytes of encoder
    edit: EditOf<T, C>,
    /// whether `edit` is pointed at the current slab
    dirty: bool,
    /// Original items after the cursor, maintained as it moves.
    ///
    /// The tail beyond the cursor is untouched, so this is both how many
    /// original items are left and how many current ones — which is why
    /// it can answer "is this the end of the column?" while a rebuild is
    /// open and `col.total_len` is stale.
    remaining: usize,
    /// slabs this cursor has rebuilt — what the batching is meant to
    /// keep down, and what a no-op `replace` must not increment
    rebuilds: usize,
    /// the reader for the current slab, at `in_slab` in that slab's
    /// original bytes — valid clean *and* dirty, since a rebuild leaves
    /// the slab's bytes alone until it closes.
    ///
    /// The reader trails `in_slab` and only catches up when something
    /// reads, so a caller that just moves — a merge copying ranges —
    /// never decodes at all.
    ///
    /// `None` until a read asks for one, and again after every jump
    /// through the index. Constructing it eagerly would be free work for
    /// the `edit()`-then-`seek()` chain, which is most of them, and no
    /// saving for the rest: `state_at(slab, k)` is `state_advance` from
    /// the slab's start, which is where an eager reader would stand.
    dec: Option<Reader<StateOf<T, C>>>,
}

impl<'a, T, C, WF, Idx> Edit<'a, T, C, WF, Idx>
where
    T: ColumnValueRef,
    C: Codec,
    WF: WeightFn<T, C>,
    Idx: ColumnIndex<WF::Weight>,
{
    pub(crate) fn new(col: &'a mut Column<T, C, WF, Idx>, at: usize) -> Self {
        assert!(at <= col.len(), "edit_at out of bounds");
        if col.slabs.is_empty() {
            let empty = <T::Encoding<C> as crate::encoding::ColumnEncoding>::empty_slab();
            col.index.splice(0..0, [WF::compute(&empty)]);
            col.slabs.push(empty);
        }
        // the front of the column needs no lookup — and `col.edit()`
        // followed by a seek is the common chain, which would otherwise
        // pay for positioning twice
        let (slab, in_slab) = if at == 0 {
            (0, 0)
        } else if let Some(hit) = col.resume_at(at) {
            hit
        } else {
            col.find_slab(at)
        };
        let (slab, in_slab) = if slab >= col.slabs.len() {
            (col.slabs.len() - 1, col.slabs[col.slabs.len() - 1].len)
        } else {
            (slab, in_slab)
        };
        let remaining = col.total_len - at.min(col.total_len);
        Edit {
            col,
            slab,
            in_slab,
            orig: at,
            out: at,
            remaining,
            edit: EditOf::<T, C>::default(),
            dirty: false,
            rebuilds: 0,
            dec: None,
        }
    }

    /// The cursor's position in the column's original coordinates — the
    /// coordinate system [`seek`](Self::seek) takes.
    pub fn pos(&self) -> usize {
        self.orig
    }

    /// The cursor's position in the column as edited so far.
    pub fn out_pos(&self) -> usize {
        self.out
    }

    /// Original items still ahead of the cursor.
    pub(crate) fn remaining(&self) -> usize {
        self.remaining
    }

    /// The column under the cursor. Only meaningful while no rebuild is
    /// open — the slab metadata is stale until one closes.
    pub(crate) fn column(&self) -> &Column<T, C, WF, Idx> {
        self.col
    }

    /// How many slabs this cursor has rebuilt. One per slab *touched*,
    /// however many edits landed in it — which is the whole point.
    #[doc(hidden)]
    pub fn rebuilds(&self) -> usize {
        self.rebuilds
    }

    /// Move forward to original position `to`, carrying everything in
    /// between through unchanged.
    ///
    /// Forward-only: every caller is already ascending, and it is what
    /// lets the cursor keep one slab open.
    pub fn seek(&mut self, to: usize) -> &mut Self {
        assert!(to >= self.orig, "Edit::seek is forward-only");
        let n = to - self.orig;
        self.advance(n)
    }

    /// Move forward `n` items from wherever the cursor is.
    pub fn advance(&mut self, n: usize) -> &mut Self {
        self.advance_with(n, |_, _| {});
        self
    }

    /// [`advance`](Self::advance), reporting to `f` every run it carries
    /// through an open rebuild. Returns how many items it did *not*
    /// report: a clean cursor jumps whole slabs through the index without
    /// decoding them, which is what makes a long seek cheap and what a
    /// caller keeping an accumulator has to make up for from the column
    /// itself.
    pub(crate) fn advance_with<F>(&mut self, n: usize, mut f: F) -> usize
    where
        F: FnMut(T::Get<'_>, usize),
    {
        if self.dirty && self.est_runs(n) >= Enc::<T, C>::CARRY_BUDGET {
            self.close();
        }
        let mut want = n;
        let mut jumped = 0;
        while want > 0 {
            if !self.dirty {
                jumped += self.jump(want);
                break;
            }
            let avail = self.orig_len() - self.in_slab;
            let take = want.min(avail);
            let left = self.step(take, &mut f);
            want -= take - left;
            if left > 0 {
                // the carry budget ran out; the loop jumps the rest
                self.close();
            } else if take == avail && !self.next_slab() {
                break;
            }
        }
        jumped
    }

    /// Move `n` items with no rebuild open. The slabs in between are
    /// never visited — stepping through them would make a long seek
    /// O(slabs) — so the new position is looked up once.
    ///
    /// A move that stays inside the current slab skips the lookup and
    /// carries the reader along instead: the index answers in items and
    /// would leave the reader stranded, which is what makes a
    /// peek/advance/peek caller re-decode the slab on every read.
    fn jump(&mut self, n: usize) -> usize {
        let step = n.min(self.remaining);
        // strictly inside: landing exactly on the slab's end would rest
        // the cursor where no value can be read, which `locate_out`
        // resolves by moving on to the next slab
        if step < self.orig_len() - self.in_slab {
            self.in_slab += step;
            self.orig += step;
            self.out += step;
            self.remaining -= step;
            return step;
        }
        self.orig += step;
        self.out += step;
        self.remaining -= step;
        self.locate_out();
        step
    }

    /// Insert one `value` at the cursor.
    pub fn insert(&mut self, value: impl AsColumnRef<T>) -> &mut Self {
        self.insert_run(value, 1)
    }

    /// Insert `n` copies of `value` at the cursor.
    pub fn insert_run(&mut self, value: impl AsColumnRef<T>, n: usize) -> &mut Self {
        if n == 0 {
            return self;
        }
        self.open();
        self.edit.insert(value, n);
        self.out += n;
        self
    }

    /// Insert a stream of runs at the cursor.
    pub fn insert_runs<V: AsColumnRef<T>>(
        &mut self,
        runs: impl Iterator<Item = Run<V>>,
    ) -> &mut Self {
        for run in runs {
            self.insert_run(run.value, run.count);
        }
        self
    }

    /// The value at the cursor, without consuming it.
    ///
    /// The reader is at the cursor either way, so a rebuild being open
    /// changes nothing: it writes into its own buffer and leaves the
    /// slab's bytes alone until it closes.
    pub fn peek(&mut self) -> Option<T::Get<'_>> {
        if self.remaining == 0 {
            return None;
        }
        if self.dirty {
            let slab = &self.col.slabs[self.slab];
            return Enc::<T, C>::state_peek(slab, self.edit.read_state());
        }
        self.load_reader();
        let slab = &self.col.slabs[self.slab];
        let r = self.dec.as_ref().expect("reader loaded");
        Enc::<T, C>::state_peek(slab, &r.state)
    }

    /// Read the value at the cursor and replace it with what `f` returns:
    /// the shape of an in-place update — bumping a succ count, clearing a
    /// visibility bit.
    pub fn replace<V: AsColumnRef<T>>(&mut self, f: impl FnOnce(T::Get<'_>) -> V) -> &mut Self {
        let Some(cur) = self.peek() else {
            return self;
        };
        // own it so the borrow of the slab ends before anything is
        // written — free for every `Copy` column
        let cur = T::to_owned(cur);
        let next = f(cur.as_column_ref());
        if T::eq(cur.as_column_ref(), next.as_column_ref()) {
            // a rebuild would produce identical bytes, and callers that
            // scan a column replacing only some rows hit this constantly
            return self.advance(1);
        }
        self.delete(1);
        self.insert(next);
        self
    }

    /// Delete `n` items at the cursor.
    pub fn delete(&mut self, n: usize) -> &mut Self {
        self.delete_with(n, |_, _| {}, |_| {})
    }

    /// [`delete`](Self::delete), reporting every run it drops to `f` —
    /// the values are gone from the column but a delta column's running
    /// value still moves over them.
    pub(crate) fn delete_with<F, G>(&mut self, n: usize, mut f: F, mut g: G) -> &mut Self
    where
        F: FnMut(T::Get<'_>, usize),
        G: FnMut(&WF::Weight),
    {
        // a delete off the end of the column takes what is there;
        // counting the rest would move the cursor past items that never
        // existed, and a later `seek` would look like it went backwards
        let mut left = n.min(self.remaining);
        let mut dropped = false;
        while left > 0 {
            let avail = self.orig_len() - self.in_slab;
            if left < avail {
                self.open();
                self.edit.delete(&self.col.slabs[self.slab], left, &mut f);
                self.took(left);
                return self;
            }
            // a whole slab, and no rebuild reading it: drop it outright.
            // The column always keeps one slab, so the last one is
            // emptied through the rebuild below instead.
            if self.in_slab == 0 && !self.dirty && self.col.slabs.len() > 1 {
                g(&WF::compute(&self.col.slabs[self.slab]));
                self.col.slabs.remove(self.slab);
                self.col.index.splice(self.slab..self.slab + 1, []);
                self.col.total_len -= avail;
                self.orig += avail;
                self.remaining -= avail;
                left -= avail;
                dropped = true;
                self.locate_out();
                self.stamp();
                continue;
            }
            // the delete reaches this slab's end: finish it here and go on
            self.open();
            self.edit.delete(&self.col.slabs[self.slab], avail, &mut f);
            self.took(avail);
            left -= avail;
            // `close` re-locates, which is what carries the cursor into
            // the next slab
            self.close();
        }
        if dropped {
            // dropping slabs can leave undersized neighbours meeting
            let range = self.col.try_merge_range(self.slab..self.slab + 1);
            self.reconcile(range);
            self.locate_out();
            self.stamp();
        }
        self
    }

    /// Account for `n` items the cursor has consumed.
    fn took(&mut self, n: usize) {
        self.in_slab += n;
        self.orig += n;
        self.remaining -= n;
    }

    /// Bring the reader to `in_slab`: forward from where it stands, or
    /// from the slab's start if the cursor arrived through the index.
    /// Paid per read, over the ground since the last one.
    fn load_reader(&mut self) {
        let slab = &self.col.slabs[self.slab];
        match self.dec.as_mut() {
            Some(r) => {
                debug_assert!(r.at <= self.in_slab, "the reader ran past the cursor");
                if r.at < self.in_slab {
                    Enc::<T, C>::state_advance(slab, &mut r.state, self.in_slab - r.at);
                    r.at = self.in_slab;
                }
            }
            None => {
                self.dec = Some(Reader {
                    state: Enc::<T, C>::state_at(slab, self.in_slab),
                    at: self.in_slab,
                })
            }
        }
    }

    /// Close the open slab, if any, and reconcile the column. Ends a
    /// chain; also runs on drop.
    pub fn finish(&mut self) {
        self.close();
    }

    // ── internals ───────────────────────────────────────────────────

    /// The current slab's item count in its original coordinates —
    /// `slab.len` is only updated when the rebuild closes.
    fn orig_len(&self) -> usize {
        self.col.slabs[self.slab].len
    }

    /// Move `n` items forward inside the current slab, returning the ones
    /// the open rebuild declined to carry (see [`RUN_BUDGET`]).
    fn step<F>(&mut self, n: usize, f: &mut F) -> usize
    where
        F: FnMut(T::Get<'_>, usize),
    {
        if n == 0 {
            return 0;
        }
        let mut left = 0;
        if self.dirty {
            // dirty: the items have to be carried through the rebuild
            let budget = Enc::<T, C>::CARRY_BUDGET;
            left = self.edit.pass(&self.col.slabs[self.slab], n, budget, f);
        }
        // clean: nothing to do but move — no decode, no write
        let done = n - left;
        self.in_slab += done;
        self.orig += done;
        self.out += done;
        self.remaining -= done;
        left
    }

    /// What carrying `n` items would cost in encoder pushes, from the
    /// slab's own density — `segments` and `len` are already on it, so
    /// this decides without decoding anything.
    ///
    /// Deciding here rather than mid-carry is what makes it free; the
    /// budget passed to `pass` is only a guard for slabs whose runs are
    /// unevenly distributed.
    fn est_runs(&self, n: usize) -> usize {
        let s = &self.col.slabs[self.slab];
        if s.len == 0 {
            return 0;
        }
        n.saturating_mul(s.segments) / s.len
    }

    /// Point `slab`/`in_slab` at the cursor's position, looked up through
    /// the index. Only valid with no rebuild open: `out` is a position in
    /// the column as it stands, which is only true once the slab it
    /// touched has been written back.
    /// Record a write: bump the column's change token and leave the
    /// cursor where the next edit can pick it up. Every path that moves
    /// slabs must call this, or a stale cursor outlives the layout it
    /// describes.
    fn stamp(&mut self) {
        self.col.counter += 1;
        self.col.last = crate::column::LastEdit {
            counter: self.col.counter,
            pos: self.out,
            slab: self.slab,
            in_slab: self.in_slab,
        };
    }

    fn locate_out(&mut self) {
        let at_end = self.out >= self.col.total_len;
        let (si, off) = if at_end {
            let last = self.col.slabs.len() - 1;
            (last, self.col.slabs[last].len)
        } else {
            self.col.find_slab(self.out)
        };
        self.slab = si.min(self.col.slabs.len() - 1);
        self.in_slab = off;
        // the write-back moved bytes and the index answered in items:
        // whatever the reader was pointing at is gone
        self.dec = None;
    }

    /// Close the current slab and step to the next. Returns false at the
    /// end of the column.
    fn next_slab(&mut self) -> bool {
        if self.dirty {
            // `close` re-locates: the write-back can merge, split or drop
            // slabs, so the absolute cursor is the only way back — and it
            // has to happen even when this was the last slab, or a
            // following insert would land through a stale offset
            self.close();
            return self.remaining > 0;
        }
        if self.slab + 1 >= self.col.slabs.len() {
            return false;
        }
        self.slab += 1;
        self.in_slab = 0;
        self.dec = Some(Reader {
            state: Enc::<T, C>::state_at(&self.col.slabs[self.slab], 0),
            at: 0,
        });
        true
    }

    /// Begin a rebuild of the current slab if one is not already open.
    fn open(&mut self) {
        if self.dirty {
            return;
        }
        let slab = &self.col.slabs[self.slab];
        let max = self.col.max_segments;
        // the rebuild derives its own reader from the partition it has to
        // walk anyway, so opening costs one pass over the slab
        self.edit.reset(slab, self.in_slab, max);
        self.dirty = true;
    }

    /// Finish the open rebuild: write it back, coalesce neighbours and
    /// update the index — each once, however many edits the slab took.
    fn close(&mut self) {
        if !self.dirty {
            return;
        }
        self.dirty = false;
        self.rebuilds += 1;
        let si = self.slab;
        let removed = self.col.slabs[si].len;
        // `edit` and `col` are separate fields, so the rebuild closes in
        // place against the slab it belongs to — nothing is moved
        let spilled = self.edit.close(&mut self.col.slabs[si]);
        let spill = spilled.len();
        if spill > 0 {
            self.col.slabs.splice(si + 1..si + 1, spilled);
        }
        let range = si..si + 1 + spill;
        let kept: usize = self.col.slabs[range.clone()].iter().map(|s| s.len).sum();
        self.col.total_len = self.col.total_len + kept - removed;
        let range = self.col.try_merge_range(range);
        self.reconcile(range);
        // the write-back can merge, split or drop slabs, so `slab` and
        // `in_slab` mean nothing until they are looked up again. Doing it
        // here rather than at each call site is what lets everything
        // downstream trust them — a caller that skipped it would read
        // through a stale offset.
        self.locate_out();
        self.stamp();
    }

    /// Bring the slab index back in step with `slabs` over `range`.
    fn reconcile(&mut self, range: Range<usize>) {
        if self.col.index.len() == self.col.slabs.len() && range.len() == 1 {
            let w = WF::compute(&self.col.slabs[range.start]);
            self.col.index.update_slab(range.start, w);
        } else {
            let new: Vec<WF::Weight> = self.col.slabs[range.clone()]
                .iter()
                .map(WF::compute)
                .collect();
            let old_end = range.end + self.col.index.len() - self.col.slabs.len();
            self.col.index.splice(range.start..old_end, new);
            debug_assert_eq!(self.col.index.len(), self.col.slabs.len());
        }
    }
}

impl<T, C, WF, Idx> Drop for Edit<'_, T, C, WF, Idx>
where
    T: ColumnValueRef,
    C: Codec,
    T::Encoding<C>: SlabEdit,
    WF: WeightFn<T, C>,
    Idx: ColumnIndex<WF::Weight>,
{
    fn drop(&mut self) {
        // finishing on drop keeps chains honest: the last call in a
        // chain does not have to be `finish()`
        if self.dirty {
            self.close();
        }
    }
}
