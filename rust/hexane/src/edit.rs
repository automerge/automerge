//! Progressive editing: a forward-only cursor over a
//! [`Column`], and the contract an encoding implements so
//! its slabs can be rebuilt in place.

use crate::column::{Column, WeightFn};
use crate::column_core::Parts;
use crate::copy::Ranged;
use crate::cursor::{Cursor, Feeding, Inserting, Slabs, Visiting, WeightOf};
use crate::encoding::ColumnEncoding;
use crate::index::ColumnIndex;
use crate::{AsColumnRef, Codec, ColumnValueRef, Run};
use vstd::prelude::Ghost;

/// An encoding whose slabs a [`Column`] cursor can edit.
// `Model`'s bound is the crate's own verified contract, which nothing
// outside can name or implement — a seal, like `ColumnValueRef`'s
#[allow(private_bounds)]
pub trait SlabEdit: crate::encoding::ColumnEncoding {
    type Edit: EditSlab<Tail = Self::Tail, Value = Self::Value, State = Self::State>;

    /// The slabs as the verified cursor sees them: what a slab holds, and
    /// the rebuild, merge and reader operations specified against it.
    /// The cursor is proved for any model, and each encoding's model is
    /// proved against its format. An associated type rather than a
    /// blanket impl so that each encoding can have its own.
    #[doc(hidden)]
    type Model: crate::cursor::Slabs<Tail = Self::Tail, State = Self::State, Rebuild = Self::Edit>;

    /// How many runs a cursor carries through an open rebuild before
    /// closing it instead — the trade between re-encoding what it carries
    /// and paying for a write-back.
    const CARRY_BUDGET: usize = RUN_BUDGET;
}

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

/// The encoding a cursor drives.
type Enc<T, C> = <T as ColumnValueRef>::Encoding<C>;

/// The encoding's slabs, as the cursor sees them.
type Model<T, C> = <Enc<T, C> as SlabEdit>::Model;

/// The column's weight function, for the cursor.
type G<T, C, WF> = WeightOf<T, C, WF>;

/// The default carry budget, in runs — an encoding states its own as
/// [`SlabEdit::CARRY_BUDGET`].
///
/// Carrying costs a decode and a push per run; closing costs a partition,
/// a byte splice and an index update, and leaves the skipped bytes where
/// they are. In runs rather than items because that is what the work is:
/// a long span of one value is carried for nothing, the same span of
/// distinct values is better memmoved.
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
///
/// The cursor's state and every move it makes are verified with Verus
/// (`crate::cursor`): each operation keeps the column well-formed and
/// does exactly what it says to its values, given the encodings' slab
/// operations. This struct holds the column's parts while it edits them
/// (taken out of its verified core, and put back when it is dropped),
/// and the cursor's state.
pub struct Edit<'a, T, C, WF, Idx>
where
    T: ColumnValueRef,
    C: Codec,
    T::Encoding<C>: SlabEdit,
    WF: WeightFn<T, C>,
    Idx: ColumnIndex<WF::Weight>,
{
    col: &'a mut Column<T, C, WF, Idx>,
    /// the column's parts while the cursor has them (`None` once handed
    /// back)
    parts: Option<Parts<crate::column::TailOf<T, C>, Idx>>,
    max_segments: usize,
    cur: Cursor<Model<T, C>>,
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
        // verified (`ColumnCore::take_parts`): a column, with room for
        // three more items, as the rebuilds count; the core holds no items
        // until they are put back
        let max_segments = col.core.max_segments();
        let mut p = col.core.take_parts();
        let cur = Cursor::new::<WF::Weight, Idx, G<T, C, WF>>(
            &mut p.slabs,
            &mut p.index,
            p.total_len,
            Ghost::assume_new(),
            at,
        );
        Edit {
            col,
            parts: Some(p),
            max_segments,
            cur,
        }
    }

    /// The column's parts, which the cursor holds until it is dropped.
    #[inline(always)]
    fn p_ref(&self) -> &Parts<crate::column::TailOf<T, C>, Idx> {
        self.parts
            .as_ref()
            .expect("the edit cursor holds the parts")
    }

    /// The slabs as the cursor has left them: those of the column it
    /// edits, with any open rebuild not yet written back.
    pub(crate) fn slabs(&self) -> &[crate::column::Slab<crate::column::TailOf<T, C>>] {
        &self.p_ref().slabs
    }

    /// The column's parts as the cursor has left them: its slabs, their
    /// index and its item count (see [`Self::slabs`]).
    pub(crate) fn parts(
        &self,
    ) -> (
        &[crate::column::Slab<crate::column::TailOf<T, C>>],
        &Idx,
        usize,
    ) {
        let p = self.p_ref();
        (&p.slabs, &p.index, p.total_len)
    }

    /// The cursor's position in the column's original coordinates — the
    /// coordinate system [`seek`](Self::seek) takes.
    pub fn pos(&self) -> usize {
        self.cur.orig
    }

    /// The cursor's position in the column as edited so far.
    pub fn out_pos(&self) -> usize {
        self.cur.out
    }

    /// Original items still ahead of the cursor.
    pub(crate) fn remaining(&self) -> usize {
        self.cur.remaining
    }

    /// How many slabs this cursor has rebuilt. One per slab *touched*,
    /// however many edits landed in it — which is the whole point.
    #[doc(hidden)]
    pub fn rebuilds(&self) -> usize {
        self.cur.rebuilds
    }

    /// Move forward to original position `to`, carrying everything in
    /// between through unchanged.
    ///
    /// Forward-only: every caller is already ascending, and it is what
    /// lets the cursor keep one slab open.
    pub fn seek(&mut self, to: usize) -> &mut Self {
        assert!(to >= self.cur.orig, "Edit::seek is forward-only");
        let n = to - self.cur.orig;
        self.advance(n)
    }

    /// Move forward `n` items from wherever the cursor is.
    pub fn advance(&mut self, n: usize) -> &mut Self {
        self.advance_with(n, |_, _| {});
        self
    }

    /// [`advance`](Self::advance), reporting every run it carries to `f`;
    /// returns how many items it jumped through the index without reading.
    pub(crate) fn advance_with<F>(&mut self, n: usize, mut f: F) -> usize
    where
        F: FnMut(T::Get<'_>, usize),
    {
        let max_segments = self.max_segments;
        let p = self
            .parts
            .as_mut()
            .expect("the edit cursor holds the parts");
        self.cur.advance_with::<WF::Weight, Idx, G<T, C, WF>, _>(
            &mut p.slabs,
            &mut p.index,
            &mut p.total_len,
            max_segments,
            &mut p.counter,
            n,
            &mut Visiting::<Enc<T, C>, F>::new(&mut f),
        )
    }

    /// Insert one `value` at the cursor.
    pub fn insert(&mut self, value: impl AsColumnRef<T>) -> &mut Self {
        self.insert_run(value, 1)
    }

    /// Insert `n` copies of `value` at the cursor.
    pub fn insert_run(&mut self, value: impl AsColumnRef<T>, n: usize) -> &mut Self {
        let p = self
            .parts
            .as_ref()
            .expect("the edit cursor holds the parts");
        self.cur.insert(
            &p.slabs,
            self.max_segments,
            Inserting::<Enc<T, C>, _>::new(value),
            n,
        );
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
        if self.cur.remaining == 0 {
            return None;
        }
        let p = self
            .parts
            .as_ref()
            .expect("the edit cursor holds the parts");
        if self.cur.dirty {
            let slab = &p.slabs[self.cur.slab];
            return Enc::<T, C>::state_peek(
                slab,
                <Model<T, C> as Slabs>::read_state(&self.cur.edit),
            );
        }
        // verified: the reader is then at the cursor (`Cursor::load_reader`)
        self.cur.load_reader(&p.slabs);
        let slab = &p.slabs[self.cur.slab];
        let r = self.cur.dec.as_ref().expect("reader loaded");
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
    /// value still moves over them — and the weight of every slab it
    /// drops whole, unread, to `g`.
    pub(crate) fn delete_with<F, H>(&mut self, n: usize, mut f: F, mut g: H) -> &mut Self
    where
        F: FnMut(T::Get<'_>, usize),
        H: FnMut(&WF::Weight),
    {
        let max_segments = self.max_segments;
        let p = self
            .parts
            .as_mut()
            .expect("the edit cursor holds the parts");
        self.cur.delete_with::<WF::Weight, Idx, G<T, C, WF>, _, H>(
            &mut p.slabs,
            &mut p.index,
            &mut p.total_len,
            max_segments,
            &mut p.counter,
            n,
            &mut Visiting::<Enc<T, C>, F>::new(&mut f),
            &mut g,
        );
        self
    }

    /// Close the open slab, if any, and reconcile the column. Ends a
    /// chain; also runs on drop.
    pub fn finish(&mut self) {
        if self.cur.dirty {
            self.close();
        }
    }

    /// Finish the open rebuild: write it back, coalesce neighbours and
    /// update the index — each once, however many edits the slab took.
    fn close(&mut self) {
        let max_segments = self.max_segments;
        let p = self
            .parts
            .as_mut()
            .expect("the edit cursor holds the parts");
        self.cur.close::<WF::Weight, Idx, G<T, C, WF>>(
            &mut p.slabs,
            &mut p.index,
            &mut p.total_len,
            max_segments,
            &mut p.counter,
        );
    }
}

impl<T, C, WF, Idx> Column<T, C, WF, Idx>
where
    T: ColumnValueRef,
    C: Codec,
    WF: WeightFn<T, C>,
    Idx: ColumnIndex<WF::Weight>,
{
    /// Delete `del` items at `at` and insert `runs` there: one cursor
    /// opened, deleting, inserting and closed, the whole chain verified
    /// (`ColumnCore::splice`, `cursor::splice_feed`). The caller checks
    /// the range.
    pub(crate) fn splice_feed<V: AsColumnRef<T>>(
        &mut self,
        at: usize,
        del: usize,
        runs: impl Iterator<Item = (V, usize)>,
    ) {
        let mut f = |_: T::Get<'_>, _: usize| {};
        let mut g = |_: &WF::Weight| {};
        self.core.splice(
            at,
            del,
            &mut Visiting::<Enc<T, C>, _>::new(&mut f),
            &mut g,
            &mut Feeding::<Enc<T, C>, _>::new(runs),
        );
    }

    /// `copy_ranges` directly: apply the normalized `splices` to this
    /// column, copying from `src`, with one cursor (verified,
    /// `ColumnCore::copy_direct`).
    pub(crate) fn copy_direct_from(&mut self, src: &Self, splices: &[crate::Splice])
    where
        for<'b> T::Get<'b>: AsColumnRef<T>,
    {
        let mut f = |_: T::Get<'_>, _: usize| {};
        let mut g = |_: &WF::Weight| {};
        self.core.copy_direct(
            splices,
            &mut Visiting::<Enc<T, C>, _>::new(&mut f),
            &mut g,
            &mut Ranged::<Enc<T, C>, _>::new(src.iter().runs()),
        );
    }

    /// `copy_ranges` inverted: this column holds the source; keep the
    /// normalized `splices`' ranges of it and put `dst`'s items back
    /// between them (verified, `ColumnCore::copy_invert`).
    pub(crate) fn copy_invert_from(&mut self, dst: &Self, splices: &[crate::Splice])
    where
        for<'b> T::Get<'b>: AsColumnRef<T>,
    {
        let mut f = |_: T::Get<'_>, _: usize| {};
        let mut g = |_: &WF::Weight| {};
        self.core.copy_invert(
            splices,
            dst.len(),
            &mut Visiting::<Enc<T, C>, _>::new(&mut f),
            &mut g,
            &mut Ranged::<Enc<T, C>, _>::new(dst.iter().runs()),
        );
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
        if self.cur.dirty {
            self.close();
        }
        // hand the column its parts back: the cursor's steps keep them a
        // column (verified), the chain of them here is glue
        // (`ColumnCore::restore`'s precondition)
        if let Some(p) = self.parts.take() {
            self.col.core.restore(p);
        }
    }
}
