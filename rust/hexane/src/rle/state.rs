//! RLE encoding state machine for cursor-aware splice.
//!
//! [`State`] is verified with Verus (pass 1: a writer starting from
//! empty): each append keeps the bytes written the canonical encoding of
//! the values appended, with the pending run held back, and a flush
//! writes that run.

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::len_zero, clippy::assign_op_pattern)]

use crate::rle::model::*;
#[cfg(test)]
use crate::Leb128;
use crate::{AsColumnRef, Codec, RleValue};

use std::marker::PhantomData;
#[allow(unused_imports)]
use vstd::prelude::*;

/// A Cow-like type for RLE values. Holds either an owned value from the
/// splice iterator (`V`) or a borrowed value decoded from the slab (`T::Get<'a>`).
///
/// Derefs to `T::Get<'_>` via `as_ref()`, and implements `AsColumnRef<T>`
/// so it can be used directly with `T::pack()`.
#[derive(Debug)]
pub enum RleCow<'a, T: RleValue, V: AsColumnRef<T>> {
    Owned(V),
    Ref(T::Get<'a>),
}

impl<'a, T: RleValue, V: AsColumnRef<T> + Copy> Copy for RleCow<'a, T, V> {}

impl<'a, T: RleValue, V: AsColumnRef<T>> Clone for RleCow<'a, T, V> {
    fn clone(&self) -> Self {
        match self {
            Self::Owned(v) => Self::Owned(v.clone()),
            Self::Ref(g) => Self::Ref(*g),
        }
    }
}

impl<'a, T: RleValue, V: AsColumnRef<T>> RleCow<'a, T, V> {
    /// Get as `T::Get<'_>` — the Ref arm reborrows at the shorter
    /// lifetime via [`ColumnValueRef::shorten`], no `unsafe` needed.
    ///
    /// [`ColumnValueRef::shorten`]: crate::ColumnValueRef::shorten
    #[inline]
    pub fn get(&self) -> T::Get<'_> {
        match self {
            Self::Owned(v) => v.as_column_ref(),
            Self::Ref(g) => T::shorten(g),
        }
    }

    #[inline]
    pub fn pack<C: Codec>(&self, buf: &mut Vec<u8>) -> usize {
        let pos = buf.len();
        match self {
            Self::Owned(v) => T::pack::<C>(v.as_column_ref(), buf),
            Self::Ref(g) => T::pack::<C>(*g, buf),
        };
        buf.len() - pos
    }

    #[inline]
    pub fn is_null(&self) -> bool {
        match self {
            Self::Owned(v) => T::is_null(v.as_column_ref()),
            Self::Ref(g) => T::is_null(*g),
        }
    }
}

impl<'a, T: RleValue, V: AsColumnRef<T>> PartialEq for RleCow<'a, T, V> {
    fn eq(&self, other: &Self) -> bool {
        self.get() == other.get()
    }
}

impl<'a, T: RleValue, V: AsColumnRef<T>> From<V> for RleCow<'a, T, V> {
    fn from(v: V) -> Self {
        Self::Owned(v)
    }
}

/// [`WriteOps`] for column type `T`, inserted values `V` and codec `C`:
/// the trusted bridge from the verified writer to the value type's and
/// codec's own code.
pub struct WOps<T, V, C>(PhantomData<(T, V, C)>);

// Outside `verus!`, so Verus takes it on trust; its spec items exist only
// when Verus compiles the crate.
impl<'a, T: RleValue, V: AsColumnRef<T>, C: Codec> WriteOps<'a> for WOps<T, V, C> {
    type R = Ops<T, C>;
    type Cow = RleCow<'a, T, V>;

    #[cfg(verus_keep_ghost)]
    fn val(_c: Self::Cow) -> <Self::R as RleOps<'a>>::V {
        unimplemented!()
    }

    #[inline(always)]
    fn cow_ref(v: <Self::R as RleOps<'a>>::V) -> Self::Cow {
        RleCow::Ref(v)
    }

    #[inline(always)]
    fn cow_eq(a: &Self::Cow, b: &Self::Cow) -> bool {
        a == b
    }

    #[inline(always)]
    fn cow_is_null(c: &Self::Cow) -> bool {
        c.is_null()
    }

    #[inline(always)]
    fn cow_pack(c: &Self::Cow, buf: &mut Vec<u8>) -> usize {
        c.pack::<C>(buf)
    }

    #[inline(always)]
    fn put_signed(n: i64, buf: &mut Vec<u8>) {
        buf.extend(C::encode_signed(n));
    }

    #[inline(always)]
    fn put_unsigned(n: u64, buf: &mut Vec<u8>) {
        buf.extend(C::encode_unsigned(n));
    }

    #[inline(always)]
    fn rewrite_lit_header(buf: &mut Vec<u8>, pos: usize, total: usize) -> i64 {
        C::rewrite_lit_header(buf, pos, total)
    }
}

/// The writer for column type `T`, inserted values `V` and codec `C`.
#[cfg(test)]
pub(crate) type RleState<'a, T, V, C = Leb128> = State<'a, WOps<T, V, C>>;

verus! {

/// Panics: a repeat run longer than `i64::MAX` has no encoding.  Trusted
/// only in that a panic does not return.
#[verifier::external_body]
#[cold]
#[inline(never)]
fn run_too_long(count: usize)
    ensures false,
{
    panic!("a run of {count} equal values is not encodable")
}

/// Panics: a literal run of more than `i64::MAX` values has no encoding
/// (nor the memory to hold one).  Trusted only in that a panic does not
/// return.
#[verifier::external_body]
#[cold]
#[inline(never)]
fn lit_too_long()
    ensures false,
{
    panic!("a literal run of more than i64::MAX values is not encodable")
}

/// A new repeat run of `n` values: panics if it has no encoding.
#[inline(always)]
fn check_run(n: usize)
    ensures n <= i64::MAX,
{
    if n as u64 > i64::MAX as u64 {
        run_too_long(n);
    }
}

/// Returned when flushing a Lit whose header is not in our buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct RewriteHeader {
    pub count: usize,
    pub pos: usize,
}

impl RewriteHeader {
    fn new(count: usize, pos: usize) -> (r: Self)
        ensures r == (RewriteHeader { count, pos }),
    {
        Self { count, pos }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct FlushState {
    pub segments: usize,
    pub rewrite: Option<RewriteHeader>,
    pub wpos: WPos,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WPos {
    Run {
        pos: usize,
        local: bool,
    },
    Lit {
        pos: usize,
        bytes: usize,
        local: bool,
    },
}

impl WPos {
    fn lit(pos: usize, bytes: usize, local: bool) -> (r: Self)
        ensures r == (WPos::Lit { pos, bytes, local }),
    {
        WPos::Lit { pos, bytes, local }
    }

    // truncated lit run
    fn trunc(count: usize, pos: usize, bytes: usize, local: bool) -> Self {
        if count == 0 {
            WPos::Run { pos, local }
        } else {
            WPos::Lit { pos, bytes, local }
        }
    }

    fn run(pos: usize, local: bool) -> (r: Self)
        ensures r == (WPos::Run { pos, local }),
    {
        WPos::Run { pos, local }
    }
}

impl FlushState {
    /// Nothing flushed.
    pub(crate) fn none() -> (f: Self)
        ensures f.rewrite is None, f.segments == 0, f.wpos == (WPos::Run { pos: 0, local: true }),
    {
        FlushState { segments: 0, rewrite: None, wpos: WPos::Run { pos: 0, local: true } }
    }

    fn new(segments: usize, wpos: WPos) -> (f: Self)
        ensures f.rewrite is None, f.segments == segments, f.wpos == wpos,
    {
        Self { segments, rewrite: None, wpos }
    }

    fn rewrite(segments: usize, r: RewriteHeader, wpos: WPos) -> (f: Self)
        ensures f.rewrite == Some(r), f.segments == segments, f.wpos == wpos,
    {
        Self { segments, rewrite: Some(r), wpos }
    }
}

/// The values of a writer's value model.
#[allow(dead_code)]
pub type VOf<'a, W> = <<W as WriteOps<'a>>::R as RleOps<'a>>::V;

/// Encoding state for RLE splice operations.
///
/// `'a` is the lifetime of the slab being read. The state borrows values
/// (e.g. `&'a str` for String columns) from the slab. All borrowed data
/// must be written to the output buffer before the state is dropped and
/// the slab is mutated.
///
/// `W` supplies the values (`W::Cow`) and the codec (`WOps<T, V, C>` for
/// column type `T`, inserted values `V` and codec `C`).
pub(crate) enum State<'a, W: WriteOps<'a>> {
    Empty,
    Lone(W::Cow),
    Run(usize, W::Cow),
    /// `count`: items already written to buf (after header at `header_pos`).
    /// `local`: items written by THIS state machine (== count when header is ours).
    /// `current`: latest value, not yet written.
    Lit {
        count: usize,
        local: usize,
        header_pos: usize,
        /// Width of the last value written locally; 0 until one is.
        bytes: usize,
        current: W::Cow,
    },
    Null(usize),
}

// ── The writer's contract ───────────────────────────────────────────────────
//
// A state stands for a pending group.  The bytes before it — `pre`, the
// slab's bytes before the edit point, then the bytes written since
// `base` in the buffer — are the groups already emitted, plus, for a
// literal in progress, its header and the values written so far (`ws`).
// A literal in progress either has its header in the buffer (a `-1`
// placeholder, rewritten when it closes), or — opened from a slab —
// in `pre`, with the slab's count `h0`; closing it then returns a
// `RewriteHeader` for the caller to apply to the slab.  Each step keeps
// this, appending to the values.

/// The pending group, given the literal values already written.
pub(crate) open spec fn pend_group<'a, W: WriteOps<'a>>(s: State<'a, W>, ws: Seq<VOf<'a, W>>) -> Option<Group<VOf<'a, W>>> {
    match s {
        State::Empty => None,
        State::Lone(c) => Some(Group::Lit { values: seq![W::val(c)] }),
        State::Run(n, c) => Some(Group::Run { count: n as nat, value: W::val(c) }),
        State::Null(n) => Some(Group::Null { count: n as nat }),
        State::Lit { current, .. } => Some(Group::Lit { values: ws.push(W::val(current)) }),
    }
}

/// The emitted groups, then the pending one.
pub(crate) open spec fn all_groups<'a, W: WriteOps<'a>>(s: State<'a, W>, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>) -> Seq<Group<VOf<'a, W>>> {
    match pend_group(s, ws) {
        Some(g) => gs.push(g),
        None => gs,
    }
}

/// The values written so far, pending ones included.
pub(crate) open spec fn values_of<'a, W: WriteOps<'a>>(s: State<'a, W>, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>) -> Seq<VOf<'a, W>> {
    groups_values(all_groups(s, gs, ws), <W::R as RleOps<'a>>::null())
}

/// A literal in progress whose header is in the slab before the edit.
pub(crate) open spec fn pre_lit<'a, W: WriteOps<'a>>(s: State<'a, W>) -> bool {
    s is Lit && s->Lit_local < s->Lit_count
}

/// The `-1` placeholder header of a literal the writer opened.
pub(crate) open spec fn ph<'a, W: WriteOps<'a>>() -> Seq<u8> {
    <W::R as RleOps<'a>>::enc_signed(-1i64)
}

/// State `s`, with the slab's bytes `pre` before the edit point and the
/// bytes `buf` written from `base`: emitted groups `gs`, then the
/// pending group with literal values `ws` written.  `h0` is the slab's
/// header count of a literal opened from it.
pub(crate) open spec fn rel<'a, W: WriteOps<'a>>(s: State<'a, W>, pre: Seq<u8>, buf: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64) -> bool {
    &&& 0 <= base <= buf.len()
    &&& canonical::<W::R>(all_groups(s, gs, ws))
    &&& match s {
        State::Empty => gs.len() == 0 && ws.len() == 0 && pre.len() == 0 && buf.skip(base).len() == 0,
        State::Lit { count, local, header_pos, .. } => {
            &&& count == ws.len() && 1 <= count < usize::MAX && local <= count
            &&& if local == count {
                &&& pre + buf.skip(base) == groups_bytes::<W::R>(gs) + ph::<W>() + encs::<W::R>(ws)
                &&& pre.len() <= groups_bytes::<W::R>(gs).len()
                &&& header_pos == base + groups_bytes::<W::R>(gs).len() - pre.len()
            } else {
                &&& pre == groups_bytes::<W::R>(gs) + <W::R as RleOps<'a>>::enc_signed(h0)
                    + encs::<W::R>(ws.take(count - local))
                &&& buf.skip(base) == encs::<W::R>(ws.skip(count - local))
                &&& header_pos == groups_bytes::<W::R>(gs).len()
            }
        },
        _ => ws.len() == 0 && pre + buf.skip(base) == groups_bytes::<W::R>(gs),
    }
}

/// `pre` with a deferred header rewrite applied.
pub(crate) open spec fn apply<'a, W: WriteOps<'a>>(rw: Option<RewriteHeader>, pre: Seq<u8>, h0: i64) -> Seq<u8> {
    match rw {
        None => pre,
        Some(r) => pre.take(r.pos as int) + <W::R as RleOps<'a>>::enc_signed(-(r.count as int) as i64)
            + pre.skip(r.pos + <W::R as RleOps<'a>>::enc_signed(h0).len()),
    }
}

/// What the state's operations need of it, whatever it stands for.
pub(crate) open spec fn wf<'a, W: WriteOps<'a>>(s: State<'a, W>, buf: Seq<u8>) -> bool {
    match s {
        State::Empty => true,
        State::Lone(c) => !<W::R as RleOps<'a>>::is_null(W::val(c)),
        State::Run(n, c) => 2 <= n <= i64::MAX && !<W::R as RleOps<'a>>::is_null(W::val(c)),
        State::Null(n) => n >= 1,
        State::Lit { count, local, header_pos, current, .. } => {
            &&& !<W::R as RleOps<'a>>::is_null(W::val(current))
            &&& 1 <= count && local <= count && count + 1 <= i64::MAX && count < usize::MAX
            &&& local == count ==> header_pos <= buf.len() && ph::<W>().is_prefix_of(buf.skip(header_pos as int))
        },
    }
}

/// The segments a state has accounted for: the groups emitted, and the
/// values of a literal opened from the slab that were in it before the
/// edit. A literal's values written since are counted when it closes.
pub(crate) open spec fn acc<'a, W: WriteOps<'a>>(s: State<'a, W>, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>) -> int {
    groups_segments(gs) + match s {
        State::Lit { count, local, .. } => count - local,
        _ => 0int,
    }
}

/// From `(s0, b0)` to `(s1, b1)` the writer appended `added`, returning
/// the header rewrite `rw` for the slab and `seg` more segments
/// accounted for.  The slab's header count `h0` carries over, so steps
/// compose.
pub(crate) open spec fn step<'a, W: WriteOps<'a>>(s0: State<'a, W>, b0: Seq<u8>, s1: State<'a, W>, b1: Seq<u8>, rw: Option<RewriteHeader>, added: Seq<VOf<'a, W>>, seg: int) -> bool {
    forall|pre: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64| #[trigger] rel(s0, pre, b0, base, gs, ws, h0)
        ==> exists|gs2: Seq<Group<VOf<'a, W>>>, ws2: Seq<VOf<'a, W>>| #[trigger] rel(s1, apply::<W>(rw, pre, h0), b1, base, gs2, ws2, h0)
            && values_of(s1, gs2, ws2) == values_of(s0, gs, ws) + added
            && acc(s1, gs2, ws2) == acc(s0, gs, ws) + seg
}

/// Flushing `(s0, b0)` wrote `b1` and returned the rewrite `rw`: the
/// canonical groups of its values.
pub(crate) open spec fn flushed<'a, W: WriteOps<'a>>(s0: State<'a, W>, b0: Seq<u8>, b1: Seq<u8>, rw: Option<RewriteHeader>, seg: int) -> bool {
    forall|pre: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64| #[trigger] rel(s0, pre, b0, base, gs, ws, h0)
        ==> 0 <= base <= b1.len() && apply::<W>(rw, pre, h0) + b1.skip(base) == groups_bytes::<W::R>(all_groups(s0, gs, ws))
            && canonical::<W::R>(all_groups(s0, gs, ws))
            && groups_segments(all_groups(s0, gs, ws)) == acc(s0, gs, ws) + seg
}

/// The bytes a flush onto `lit` literal values that follow in the slab
/// writes before them: the groups, then one literal header covering the
/// pending values and those `lit`, then the pending values.
pub(crate) open spec fn lit_flush_bytes<'a, W: WriteOps<'a>>(s: State<'a, W>, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, lit: nat) -> Seq<u8> {
    match s {
        State::Lit { current, .. } => groups_bytes::<W::R>(gs)
            + <W::R as RleOps<'a>>::enc_signed(-((ws.len() + 1 + lit) as int) as i64)
            + encs::<W::R>(ws.push(W::val(current))),
        State::Lone(c) => groups_bytes::<W::R>(gs)
            + <W::R as RleOps<'a>>::enc_signed(-((1 + lit) as int) as i64)
            + <W::R as RleOps<'a>>::enc(W::val(c)),
        _ => groups_bytes::<W::R>(all_groups(s, gs, ws)) + <W::R as RleOps<'a>>::enc_signed(-(lit as int) as i64),
    }
}

/// The segments a flush onto following literal values writes: the
/// groups, then the pending values (the following ones are the slab's).
pub(crate) open spec fn lit_flush_segs<'a, W: WriteOps<'a>>(s: State<'a, W>, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>) -> int {
    match s {
        State::Lit { .. } => (groups_segments(gs) + ws.len() + 1) as int,
        State::Lone(_) => (groups_segments(gs) + 1) as int,
        _ => groups_segments(all_groups(s, gs, ws)) as int,
    }
}

/// Flushing `(s0, b0)` onto `lit` following literal values wrote `b1`
/// and returned the rewrite `rw`, accounting `seg` more segments.
pub(crate) open spec fn flushed_lit<'a, W: WriteOps<'a>>(s0: State<'a, W>, b0: Seq<u8>, b1: Seq<u8>, rw: Option<RewriteHeader>, lit: nat, seg: int) -> bool {
    forall|pre: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64| #[trigger] rel(s0, pre, b0, base, gs, ws, h0)
        ==> 0 <= base <= b1.len() && apply::<W>(rw, pre, h0) + b1.skip(base) == lit_flush_bytes(s0, gs, ws, lit)
            && lit_flush_segs(s0, gs, ws) == acc(s0, gs, ws) + seg
}

/// A step's rewrite closes the slab's literal, which no step reopens: of
/// the rewrites a run of steps returns, at most one is `Some`.
pub(crate) open spec fn rw_ok<'a, W: WriteOps<'a>>(s0: State<'a, W>, s1: State<'a, W>, rw: Option<RewriteHeader>) -> bool {
    &&& rw_from(s0, rw)
    &&& rw is Some ==> !pre_lit(s1)
    &&& pre_lit(s1) ==> pre_lit(s0) && s1->Lit_header_pos == s0->Lit_header_pos
}

/// A rewrite comes from closing the slab's literal: it is that
/// literal's header, given a count a header can hold.
pub(crate) open spec fn rw_from<'a, W: WriteOps<'a>>(s0: State<'a, W>, rw: Option<RewriteHeader>) -> bool {
    rw is Some ==> pre_lit(s0) && rw->Some_0.pos == s0->Lit_header_pos && 1 <= rw->Some_0.count <= i64::MAX
}

/// The pending group ends with `v`: a null run if `v` is null, else a
/// repeat run or literal of non-null `v`.
pub(crate) open spec fn last_is<'a, W: WriteOps<'a>>(s: State<'a, W>, v: VOf<'a, W>) -> bool {
    match s {
        State::Empty => false,
        State::Null(_) => <W::R as RleOps<'a>>::is_null(v),
        State::Lone(c) => W::val(c) == v && !<W::R as RleOps<'a>>::is_null(v),
        State::Run(_, c) => W::val(c) == v && !<W::R as RleOps<'a>>::is_null(v),
        State::Lit { current, .. } => W::val(current) == v && !<W::R as RleOps<'a>>::is_null(v),
    }
}

/// A flush onto `lit` literal values that follow fits one header: the
/// pending literal (or lone value) and those values.
pub(crate) open spec fn lit_fits<'a, W: WriteOps<'a>>(s: State<'a, W>, lit: nat) -> bool {
    (s is Lit || s is Lone) ==> s.plen() + lit <= i64::MAX
}

/// Where `w` places a group, in an output whose bytes before the buffer
/// are `plen` long and which takes the buffer from `base`.
pub(crate) open spec fn wstart(w: WPos, plen: int, base: int) -> int {
    match w {
        WPos::Run { pos, local } => if local { plen + pos - base } else { pos as int },
        WPos::Lit { pos, local, .. } => if local { plen + pos - base } else { pos as int },
    }
}

/// `w` names the group `g` starting at `at`: its kind, and for a literal
/// the width of its last value.
pub(crate) open spec fn wpos_names<'a, W: WriteOps<'a>>(w: WPos, g: Group<VOf<'a, W>>, at: int, plen: int, base: int) -> bool {
    &&& wstart(w, plen, base) == at
    &&& match w {
        WPos::Run { .. } => !(g is Lit),
        WPos::Lit { bytes, .. } => g is Lit && bytes == <W::R as RleOps<'a>>::enc(g->values.last()).len(),
    }
}

/// Flushing `(s0, b0)` into `b1` with the rewrite `rw` left `w` naming the
/// last group written.
pub(crate) open spec fn flushed_tail<'a, W: WriteOps<'a>>(s0: State<'a, W>, b0: Seq<u8>, b1: Seq<u8>, rw: Option<RewriteHeader>, w: WPos) -> bool {
    forall|pre: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64| #[trigger] rel(s0, pre, b0, base, gs, ws, h0)
        && !(s0 is Empty)
        ==> wpos_names::<W>(w, all_groups(s0, gs, ws).last(), groups_bytes::<W::R>(gs).len() as int,
            apply::<W>(rw, pre, h0).len() as int, base)
}

/// Flushing `(s0, b0)` onto following literal values left `w` naming
/// where the literal they join starts.
pub(crate) open spec fn flushed_lit_tail<'a, W: WriteOps<'a>>(s0: State<'a, W>, b0: Seq<u8>, b1: Seq<u8>, rw: Option<RewriteHeader>, w: WPos) -> bool {
    forall|pre: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64| #[trigger] rel(s0, pre, b0, base, gs, ws, h0)
        ==> w is Lit && wstart(w, apply::<W>(rw, pre, h0).len() as int, base) == match s0 {
            State::Lit { .. } | State::Lone(_) => groups_bytes::<W::R>(gs).len() as int,
            _ => groups_bytes::<W::R>(all_groups(s0, gs, ws)).len() as int,
        }
}

/// Where the bytes a state may still change start: its literal's
/// header, if the writer put it in the buffer; else the buffer's end.
pub(crate) open spec fn wlo<'a, W: WriteOps<'a>>(s: State<'a, W>, buf: Seq<u8>) -> int {
    match s {
        State::Lit { count, local, header_pos, .. } => if local == count { header_pos as int } else { buf.len() as int },
        _ => buf.len() as int,
    }
}

/// `b1` keeps `b0`'s bytes before `lo`: a writer leaves alone what it
/// did not write.
pub(crate) open spec fn keeps(b0: Seq<u8>, b1: Seq<u8>, lo: int) -> bool {
    &&& lo <= b1.len()
    &&& forall|i: int| 0 <= i < lo ==> #[trigger] b1[i] == b0[i]
}

/// `n` copies of `v`.
pub(crate) open spec fn copies<V>(n: nat, v: V) -> Seq<V> {
    Seq::new(n, |_i| v)
}

// ── Lemmas ──────────────────────────────────────────────────────────────────

pub(crate) proof fn lemma_push<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>, g: Group<O::V>)
    ensures
        groups_segments(gs.push(g)) == groups_segments(gs) + group_segments(g),
        groups_values(gs.push(g), O::null()) == groups_values(gs, O::null()) + group_values(g, O::null()),
        groups_bytes::<O>(gs.push(g)) == groups_bytes::<O>(gs) + group_bytes::<O>(g),
        gs.push(g).drop_last() == gs, gs.push(g).last() == g,
{
    assert(gs.push(g).drop_last() =~= gs);
}

/// Canonical groups with one more: the parts.
pub(crate) proof fn lemma_canonical_split<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>, g: Group<O::V>)
    requires canonical::<O>(gs.push(g)),
    ensures
        canonical::<O>(gs), group_ok::<O>(g),
        gs.len() > 0 ==> adjacent_ok(gs.last(), g),
{
    let g2 = gs.push(g);
    assert(group_ok::<O>(g2[gs.len() as int]));
    if gs.len() > 0 {
        assert(adjacent_ok(g2[gs.len() - 1], g2[gs.len() as int]));
    }
    assert forall|i: int| 0 <= i < gs.len() implies group_ok::<O>(#[trigger] gs[i]) by {
        assert(g2[i] == gs[i]);
    }
    assert forall|i: int| 0 < i < gs.len() implies adjacent_ok(gs[i - 1], #[trigger] gs[i]) by {
        assert(g2[i] == gs[i] && g2[i - 1] == gs[i - 1]);
    }
}

/// Canonical groups with one more that may follow them.
pub(crate) proof fn lemma_canonical_join<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>, g: Group<O::V>)
    requires
        canonical::<O>(gs), group_ok::<O>(g),
        gs.len() > 0 ==> adjacent_ok(gs.last(), g),
    ensures canonical::<O>(gs.push(g)),
{
    let g2 = gs.push(g);
    assert forall|i: int| 0 <= i < g2.len() implies group_ok::<O>(#[trigger] g2[i]) by {
        if i < gs.len() {
            assert(g2[i] == gs[i]);
        }
    }
    assert forall|i: int| 0 < i < g2.len() implies adjacent_ok(g2[i - 1], #[trigger] g2[i]) by {
        assert(g2[i - 1] == gs[i - 1]);
        if i < gs.len() {
            assert(g2[i] == gs[i]);
        }
    }
}

pub(crate) proof fn lemma_copies<V>(a: nat, b: nat, v: V)
    ensures copies(a, v) + copies(b, v) == copies(a + b, v),
{
    assert(copies(a, v) + copies(b, v) =~= copies(a + b, v));
}

pub(crate) proof fn lemma_copies_push<V>(ws: Seq<V>, v: V, n: nat)
    ensures ws.push(v) + copies(n, v) == ws + copies(n + 1, v),
{
    assert(ws.push(v) + copies(n, v) =~= ws + copies(n + 1, v));
}

/// A placeholder header rewritten in place in the buffer.
pub(crate) proof fn lemma_rewrite<'a, O: RleOps<'a>>(b0: Seq<u8>, base: int, a: Seq<u8>, mid: Seq<u8>, total: int, b1: Seq<u8>)
    requires
        0 <= base <= b0.len(), b0.skip(base) == a + O::enc_signed(-1i64) + mid,
        b1 == b0.take(base + a.len()) + O::enc_signed(total as i64) + b0.skip(base + a.len() + O::enc_signed(-1i64).len()),
    ensures b1.skip(base) == a + O::enc_signed(total as i64) + mid, base <= b1.len(),
{
    let hp = base + a.len();
    let h = O::enc_signed(-1i64);
    assert(b0 =~= b0.take(base) + b0.skip(base));
    assert(b0.take(hp) =~= b0.take(base) + a);
    assert(b0.skip(hp + h.len()) =~= mid);
    assert(b1 =~= b0.take(base) + (a + O::enc_signed(total as i64) + mid));
    assert(b1.skip(base) =~= a + O::enc_signed(total as i64) + mid);
}

/// The buffer's share of bytes that start with `pre`.
pub(crate) proof fn lemma_local(pre: Seq<u8>, m: Seq<u8>, g: Seq<u8>, x: Seq<u8>)
    requires pre + m == g + x, pre.len() <= g.len(),
    ensures m == g.skip(pre.len() as int) + x, pre == g.take(pre.len() as int),
{
    assert(pre =~= (pre + m).take(pre.len() as int));
    assert(m =~= (pre + m).skip(pre.len() as int));
    assert((g + x).skip(pre.len() as int) =~= g.skip(pre.len() as int) + x);
    assert((g + x).take(pre.len() as int) =~= g.take(pre.len() as int));
}

/// The group a run of `n` copies of `v` is written as.
pub(crate) open spec fn run_group<V>(n: nat, v: V) -> Group<V> {
    if n == 1 { Group::Lit { values: seq![v] } } else { Group::Run { count: n, value: v } }
}

pub(crate) proof fn lemma_lone_bytes<'a, O: RleOps<'a>>(v: O::V)
    ensures
        group_bytes::<O>(Group::Lit { values: seq![v] }) == O::enc_signed(-1i64) + O::enc(v),
        group_values(Group::Lit { values: seq![v] }, O::null()) == copies(1, v),
        encs::<O>(seq![v]) == O::enc(v),
{
    reveal_with_fuel(encs, 2);
    assert(seq![v].drop_last() =~= Seq::<O::V>::empty());
    assert(encs::<O>(seq![v]) =~= O::enc(v));
    assert(seq![v] =~= copies(1, v));
}

pub(crate) proof fn lemma_encs_push<'a, O: RleOps<'a>>(ws: Seq<O::V>, c: O::V)
    ensures encs::<O>(ws.push(c)) == encs::<O>(ws) + O::enc(c),
{
    assert(ws.push(c).drop_last() =~= ws);
}

/// A related state is well formed.
pub(crate) proof fn lemma_rel_wf<'a, W: WriteOps<'a>>(s: State<'a, W>, pre: Seq<u8>, buf: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64)
    requires rel(s, pre, buf, base, gs, ws, h0),
    ensures wf(s, buf),
{
    match s {
        State::Empty => {},
        _ => {
            let g = pend_group(s, ws)->Some_0;
            lemma_canonical_split::<W::R>(gs, g);
            if let State::Lit { count, local, header_pos, current, .. } = s {
                assert(group_ok::<W::R>(g));
                let w = ws.push(W::val(current));
                assert(w[ws.len() as int] == W::val(current));
                if local == count {
                    let gb = groups_bytes::<W::R>(gs);
                    let m = buf.skip(base);
                    let x = ph::<W>() + encs::<W::R>(ws);
                    assert(gb + ph::<W>() + encs::<W::R>(ws) =~= gb + x);
                    lemma_local(pre, m, gb, x);
                    let q = gb.len() - pre.len();
                    assert(buf.skip(header_pos as int) =~= m.skip(q));
                    assert(m.skip(q) =~= x);
                    assert(x.take(ph::<W>().len() as int) =~= ph::<W>());
                }
            }
        },
    }
}

/// Emitting a whole group.
pub(crate) proof fn lemma_emit<'a, O: RleOps<'a>>(pre: Seq<u8>, b0: Seq<u8>, base: int, gs: Seq<Group<O::V>>, g: Group<O::V>, b1: Seq<u8>)
    requires 0 <= base <= b0.len(), pre + b0.skip(base) == groups_bytes::<O>(gs), b1 == b0 + group_bytes::<O>(g),
    ensures pre + b1.skip(base) == groups_bytes::<O>(gs.push(g)), base <= b1.len(),
{
    lemma_push::<O>(gs, g);
    assert(b1.skip(base) =~= b0.skip(base) + group_bytes::<O>(g));
    assert(pre + b1.skip(base) =~= (pre + b0.skip(base)) + group_bytes::<O>(g));
}

/// Flushing a buffered literal: its value, then the real header.
pub(crate) proof fn lemma_flush_lit<'a, W: WriteOps<'a>>(b0: Seq<u8>, base: int, a: Seq<u8>, ws: Seq<VOf<'a, W>>, cur: VOf<'a, W>, b1: Seq<u8>, b2: Seq<u8>)
    requires
        0 <= base <= b0.len(),
        b0.skip(base) == a + ph::<W>() + encs::<W::R>(ws),
        b1 == b0 + <W::R as RleOps<'a>>::enc(cur),
        b2 == b1.take(base + a.len())
            + <W::R as RleOps<'a>>::enc_signed(-((ws.len() + 1) as int) as i64)
            + b1.skip(base + a.len() + ph::<W>().len()),
    ensures b2.skip(base) == a + group_bytes::<W::R>(Group::Lit { values: ws.push(cur) }), base <= b2.len(),
{
    let w2 = ws.push(cur);
    lemma_encs_push::<W::R>(ws, cur);
    assert(b1.skip(base) =~= b0.skip(base) + <W::R as RleOps<'a>>::enc(cur));
    assert(b1.skip(base) =~= a + ph::<W>() + encs::<W::R>(w2));
    lemma_rewrite::<W::R>(b1, base, a, encs::<W::R>(w2), -((ws.len() + 1) as int), b2);
}

/// Closing a literal opened from the slab: the slab's header rewritten.
pub(crate) proof fn lemma_close_pre<'a, W: WriteOps<'a>>(pre: Seq<u8>, m: Seq<u8>, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, k0: int, h0: i64, total: usize, hp: usize)
    requires
        0 <= k0 <= ws.len(), hp == groups_bytes::<W::R>(gs).len(),
        pre == groups_bytes::<W::R>(gs) + <W::R as RleOps<'a>>::enc_signed(h0) + encs::<W::R>(ws.take(k0)),
        m == encs::<W::R>(ws.skip(k0)),
    ensures
        apply::<W>(Some(RewriteHeader { count: total, pos: hp }), pre, h0) + m
            == groups_bytes::<W::R>(gs) + <W::R as RleOps<'a>>::enc_signed(-(total as int) as i64) + encs::<W::R>(ws),
        total == ws.len() ==> apply::<W>(Some(RewriteHeader { count: total, pos: hp }), pre, h0) + m
            == groups_bytes::<W::R>(gs.push(Group::Lit { values: ws })),
{
    let g = groups_bytes::<W::R>(gs);
    let h = <W::R as RleOps<'a>>::enc_signed(h0);
    assert(pre.take(g.len() as int) =~= g);
    assert(pre.skip((g.len() + h.len()) as int) =~= encs::<W::R>(ws.take(k0)));
    assert(ws =~= ws.take(k0) + ws.skip(k0));
    lemma_encs_concat::<W::R>(ws.take(k0), ws.skip(k0));
    lemma_push::<W::R>(gs, Group::Lit { values: ws });
}

// ── The steps ───────────────────────────────────────────────────────────────

/// Three groups' values.
pub(crate) proof fn lemma_vals2<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>, a: Group<O::V>, b: Group<O::V>)
    ensures
        groups_values(gs.push(a).push(b), O::null())
            == groups_values(gs, O::null()) + group_values(a, O::null()) + group_values(b, O::null()),
{
    lemma_push::<O>(gs, a);
    lemma_push::<O>(gs.push(a), b);
}

/// The pending group as a run of `n` copies of `v`, once written.
pub(crate) proof fn lemma_run_group<'a, O: RleOps<'a>>(n: nat, v: O::V)
    ensures group_values(run_group(n, v), O::null()) == copies(n, v),
{
    if n == 1 {
        assert(seq![v] =~= copies(1, v));
    }
}

/// A literal's values, all but the last: still a literal that may
/// follow what the whole one followed.
pub(crate) proof fn lemma_lit_prefix<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>, ws: Seq<O::V>, cur: O::V)
    requires canonical::<O>(gs.push(Group::Lit { values: ws.push(cur) })), ws.len() >= 1,
    ensures
        canonical::<O>(gs.push(Group::Lit { values: ws })),
        ws.last() != cur,
        !O::is_null(cur),
{
    let w = ws.push(cur);
    lemma_canonical_split::<O>(gs, Group::Lit { values: w });
    assert(w[ws.len() as int] == cur && w[ws.len() - 1] == ws.last());
    assert forall|i: int| 0 <= i < ws.len() implies !O::is_null(#[trigger] ws[i]) by {
        assert(w[i] == ws[i]);
    }
    assert forall|i: int| 0 < i < ws.len() implies ws[i - 1] != #[trigger] ws[i] by {
        assert(w[i] == ws[i] && w[i - 1] == ws[i - 1]);
    }
    assert(w[0] == ws[0]);
    lemma_canonical_join::<O>(gs, Group::Lit { values: ws });
}

/// A literal grown by one more value, different from its last.
pub(crate) proof fn lemma_lit_grow<'a, O: RleOps<'a>>(gs: Seq<Group<O::V>>, ws: Seq<O::V>, v: O::V)
    requires
        canonical::<O>(gs.push(Group::Lit { values: ws })), ws.len() >= 1,
        ws.last() != v, !O::is_null(v), ws.len() + 1 <= i64::MAX,
    ensures canonical::<O>(gs.push(Group::Lit { values: ws.push(v) })),
{
    let w = ws.push(v);
    lemma_canonical_split::<O>(gs, Group::Lit { values: ws });
    assert forall|i: int| 0 <= i < w.len() implies !O::is_null(#[trigger] w[i]) by {
        if i < ws.len() {
            assert(w[i] == ws[i]);
        }
    }
    assert forall|i: int| 0 < i < w.len() implies w[i - 1] != #[trigger] w[i] by {
        assert(w[i - 1] == ws[i - 1]);
        if i < ws.len() {
            assert(w[i] == ws[i]);
        }
    }
    assert(w[0] == ws[0]);
    lemma_canonical_join::<O>(gs, Group::Lit { values: w });
}

/// A state that keeps the pending group's kind and grows it.
#[verifier::spinoff_prover]
pub(crate) proof fn lemma_step_grow<'a, W: WriteOps<'a>>(s0: State<'a, W>, b0: Seq<u8>, s1: State<'a, W>, added: Seq<VOf<'a, W>>)
    requires
        !(s0 is Lit), !(s1 is Lit), !(s0 is Empty), !(s1 is Empty),
        forall|ws: Seq<VOf<'a, W>>| ws.len() == 0 ==> {
            &&& #[trigger] pend_group(s1, ws) is Some
            &&& group_ok::<W::R>(pend_group(s1, ws)->Some_0)
            &&& group_values(pend_group(s1, ws)->Some_0, <W::R as RleOps<'a>>::null())
                == group_values(pend_group(s0, ws)->Some_0, <W::R as RleOps<'a>>::null()) + added
            &&& forall|g: Group<VOf<'a, W>>| adjacent_ok(g, pend_group(s0, ws)->Some_0) ==> #[trigger] adjacent_ok(g, pend_group(s1, ws)->Some_0)
        },
    ensures step(s0, b0, s1, b0, None, added, 0),
{
    assert forall|pre: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64| #[trigger] rel(s0, pre, b0, base, gs, ws, h0)
        implies exists|gs2: Seq<Group<VOf<'a, W>>>, ws2: Seq<VOf<'a, W>>| #[trigger] rel(s1, apply::<W>(None, pre, h0), b0, base, gs2, ws2, h0)
            && values_of(s1, gs2, ws2) == values_of(s0, gs, ws) + added
            && acc(s1, gs2, ws2) == acc(s0, gs, ws) + 0 by {
        let g0 = pend_group(s0, ws)->Some_0;
        let g1 = pend_group(s1, ws)->Some_0;
        assert(pend_group(s1, ws) is Some);
        lemma_canonical_split::<W::R>(gs, g0);
        if gs.len() > 0 {
            assert(adjacent_ok(gs.last(), g1));
        }
        lemma_canonical_join::<W::R>(gs, g1);
        lemma_push::<W::R>(gs, g0);
        lemma_push::<W::R>(gs, g1);
        assert(rel(s1, apply::<W>(None, pre, h0), b0, base, gs, ws, h0));
    }
}

/// Emitting the pending group (not a literal in progress) and starting
/// another.
pub(crate) proof fn lemma_step_emit<'a, W: WriteOps<'a>>(s0: State<'a, W>, b0: Seq<u8>, g0: Group<VOf<'a, W>>, s1: State<'a, W>, b1: Seq<u8>, added: Seq<VOf<'a, W>>)
    requires
        !(s0 is Lit), !(s0 is Empty), !(s1 is Lit), !(s1 is Empty),
        pend_group(s0, Seq::empty()) == Some(g0),
        b1 == b0 + group_bytes::<W::R>(g0),
        group_ok::<W::R>(pend_group(s1, Seq::empty())->Some_0),
        adjacent_ok(g0, pend_group(s1, Seq::empty())->Some_0),
        group_values(pend_group(s1, Seq::empty())->Some_0, <W::R as RleOps<'a>>::null()) == added,
    ensures step(s0, b0, s1, b1, None, added, 1),
{
    assert forall|pre: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64| #[trigger] rel(s0, pre, b0, base, gs, ws, h0)
        implies exists|gs2: Seq<Group<VOf<'a, W>>>, ws2: Seq<VOf<'a, W>>| #[trigger] rel(s1, apply::<W>(None, pre, h0), b1, base, gs2, ws2, h0)
            && values_of(s1, gs2, ws2) == values_of(s0, gs, ws) + added
            && acc(s1, gs2, ws2) == acc(s0, gs, ws) + 1 by {
        assert(ws =~= Seq::<VOf<'a, W>>::empty());
        let g1 = pend_group(s1, ws)->Some_0;
        lemma_emit::<W::R>(pre, b0, base, gs, g0, b1);
        lemma_push::<W::R>(gs, g0);
        lemma_canonical_join::<W::R>(gs.push(g0), g1);
        lemma_vals2::<W::R>(gs, g0, g1);
        assert(rel(s1, apply::<W>(None, pre, h0), b1, base, gs.push(g0), ws, h0));
        assert(values_of(s1, gs.push(g0), ws) =~= values_of(s0, gs, ws) + added);
    }
}

/// From empty: the first run.
pub(crate) proof fn lemma_step_empty<'a, W: WriteOps<'a>>(s0: State<'a, W>, b0: Seq<u8>, s1: State<'a, W>, added: Seq<VOf<'a, W>>)
    requires
        s0 is Empty, !(s1 is Lit), !(s1 is Empty),
        group_ok::<W::R>(pend_group(s1, Seq::empty())->Some_0),
        group_values(pend_group(s1, Seq::empty())->Some_0, <W::R as RleOps<'a>>::null()) == added,
    ensures step(s0, b0, s1, b0, None, added, 0),
{
    assert forall|pre: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64| #[trigger] rel(s0, pre, b0, base, gs, ws, h0)
        implies exists|gs2: Seq<Group<VOf<'a, W>>>, ws2: Seq<VOf<'a, W>>| #[trigger] rel(s1, apply::<W>(None, pre, h0), b0, base, gs2, ws2, h0)
            && values_of(s1, gs2, ws2) == values_of(s0, gs, ws) + added
            && acc(s1, gs2, ws2) == acc(s0, gs, ws) + 0 by {
        let g1 = pend_group(s1, ws)->Some_0;
        lemma_canonical_join::<W::R>(gs, g1);
        lemma_push::<W::R>(gs, g1);
        assert(groups_bytes::<W::R>(gs) =~= Seq::<u8>::empty());
        assert(pre + b0.skip(base) =~= Seq::<u8>::empty());
        assert(rel(s1, apply::<W>(None, pre, h0), b0, base, gs, ws, h0));
        assert(values_of(s0, gs, ws) =~= Seq::<VOf<'a, W>>::empty());
        assert(values_of(s1, gs, ws) =~= values_of(s0, gs, ws) + added);
    }
}

/// A literal in progress takes one more value.
#[verifier::spinoff_prover]
pub(crate) proof fn lemma_step_lit_push<'a, W: WriteOps<'a>>(s0: State<'a, W>, b0: Seq<u8>, s1: State<'a, W>, b1: Seq<u8>, v: VOf<'a, W>)
    requires
        s0 is Lit, s1 is Lit, wf(s0, b0),
        s1->Lit_count == s0->Lit_count + 1, s1->Lit_local == s0->Lit_local + 1,
        s1->Lit_header_pos == s0->Lit_header_pos, W::val(s1->Lit_current) == v,
        v != W::val(s0->Lit_current), !<W::R as RleOps<'a>>::is_null(v),
        s0->Lit_count + 2 <= i64::MAX, s0->Lit_count + 2 < usize::MAX,
        b1 == b0 + <W::R as RleOps<'a>>::enc(W::val(s0->Lit_current)),
    ensures step(s0, b0, s1, b1, None, copies(1, v), 0), wf(s1, b1),
{
    let c = W::val(s0->Lit_current);
    let hp = s0->Lit_header_pos as int;
    assert forall|pre: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64| #[trigger] rel(s0, pre, b0, base, gs, ws, h0)
        implies exists|gs2: Seq<Group<VOf<'a, W>>>, ws2: Seq<VOf<'a, W>>| #[trigger] rel(s1, apply::<W>(None, pre, h0), b1, base, gs2, ws2, h0)
            && values_of(s1, gs2, ws2) == values_of(s0, gs, ws) + copies(1, v)
            && acc(s1, gs2, ws2) == acc(s0, gs, ws) + 0 by {
        let ws2 = ws.push(c);
        assert(ws2.last() == c);
        lemma_lit_grow::<W::R>(gs, ws2, v);
        lemma_encs_push::<W::R>(ws, c);
        assert(b1.skip(base) =~= b0.skip(base) + <W::R as RleOps<'a>>::enc(c));
        if s0->Lit_local == s0->Lit_count {
            let gb = groups_bytes::<W::R>(gs);
            assert(pre + b0.skip(base) == gb + ph::<W>() + encs::<W::R>(ws));
            assert(pre + b1.skip(base) =~= (pre + b0.skip(base)) + <W::R as RleOps<'a>>::enc(c));
            assert(encs::<W::R>(ws2) == encs::<W::R>(ws) + <W::R as RleOps<'a>>::enc(c));
            assert(pre + b1.skip(base) =~= gb + ph::<W>() + encs::<W::R>(ws2));
        } else {
            let k0 = s0->Lit_count - s0->Lit_local;
            assert(ws2.take(k0) =~= ws.take(k0));
            assert(ws2.skip(k0) =~= ws.skip(k0).push(c));
            lemma_encs_push::<W::R>(ws.skip(k0), c);
        }
        assert(all_groups(s1, gs, ws2) == gs.push(Group::Lit { values: ws2.push(v) }));
        assert(rel(s1, apply::<W>(None, pre, h0), b1, base, gs, ws2, h0));
        lemma_push::<W::R>(gs, Group::Lit { values: ws2.push(v) });
        lemma_push::<W::R>(gs, Group::Lit { values: ws2 });
        assert(seq![v] =~= copies(1, v));
        assert(values_of(s1, gs, ws2) =~= values_of(s0, gs, ws) + copies(1, v));
    }
    if s1->Lit_local == s1->Lit_count {
        let h = ph::<W>();
        assert(b1.skip(hp) =~= b0.skip(hp) + <W::R as RleOps<'a>>::enc(c));
        assert(b1.skip(hp).take(h.len() as int) =~= b0.skip(hp).take(h.len() as int));
    }
}

/// A lone value meets a different one: a literal begins in the buffer.
pub(crate) proof fn lemma_step_lone_lit<'a, W: WriteOps<'a>>(s0: State<'a, W>, b0: Seq<u8>, s1: State<'a, W>, b1: Seq<u8>, v: VOf<'a, W>)
    requires
        s0 is Lone, s1 is Lit, wf(s0, b0),
        s1->Lit_count == 1, s1->Lit_local == 1, s1->Lit_header_pos == b0.len(), W::val(s1->Lit_current) == v,
        v != W::val(s0->Lone_0), !<W::R as RleOps<'a>>::is_null(v),
        b1 == b0 + group_bytes::<W::R>(Group::Lit { values: seq![W::val(s0->Lone_0)] }),
    ensures step(s0, b0, s1, b1, None, copies(1, v), 0), wf(s1, b1),
{
    let p = W::val(s0->Lone_0);
    let h = ph::<W>();
    lemma_lone_bytes::<W::R>(p);
    assert forall|pre: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64| #[trigger] rel(s0, pre, b0, base, gs, ws, h0)
        implies exists|gs2: Seq<Group<VOf<'a, W>>>, ws2: Seq<VOf<'a, W>>| #[trigger] rel(s1, apply::<W>(None, pre, h0), b1, base, gs2, ws2, h0)
            && values_of(s1, gs2, ws2) == values_of(s0, gs, ws) + copies(1, v)
            && acc(s1, gs2, ws2) == acc(s0, gs, ws) + 0 by {
        let ws2 = seq![p];
        assert(ws2.last() == p);
        lemma_lit_grow::<W::R>(gs, ws2, v);
        assert(b1.skip(base) =~= b0.skip(base) + h + <W::R as RleOps<'a>>::enc(p));
        assert(pre + b1.skip(base) =~= groups_bytes::<W::R>(gs) + h + encs::<W::R>(ws2));
        assert((pre + b0.skip(base)).len() == pre.len() + b0.len() - base);
        assert(all_groups(s1, gs, ws2) == gs.push(Group::Lit { values: ws2.push(v) }));
        assert(rel(s1, apply::<W>(None, pre, h0), b1, base, gs, ws2, h0));
        lemma_push::<W::R>(gs, Group::Lit { values: ws2.push(v) });
        lemma_push::<W::R>(gs, Group::Lit { values: ws2 });
        assert(seq![v] =~= copies(1, v));
        assert(values_of(s1, gs, ws2) =~= values_of(s0, gs, ws) + copies(1, v));
    }
    assert(b1.skip(b0.len() as int) =~= h + <W::R as RleOps<'a>>::enc(p));
    assert((h + <W::R as RleOps<'a>>::enc(p)).take(h.len() as int) =~= h);
}

/// The bytes after closing a literal of values `lv` (its header in the
/// buffer at `pre`'s end of `gs`'s bytes, or in `pre`): those of
/// `gs.push(Lit(lv))`.
#[verifier::spinoff_prover]
#[verifier::rlimit(60)]
pub(crate) proof fn lemma_close_bytes<'a, W: WriteOps<'a>>(s0: State<'a, W>, pre: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64, b0: Seq<u8>, mid: Seq<u8>, lv: Seq<VOf<'a, W>>, b2: Seq<u8>, rw: Option<RewriteHeader>, total: usize)
    requires
        total >= lv.len(), total <= i64::MAX,
        rel(s0, pre, b0, base, gs, ws, h0), s0 is Lit,
        lv == ws || lv == ws.push(W::val(s0->Lit_current)),
        lv == ws ==> mid == Seq::<u8>::empty(),
        lv != ws ==> mid == <W::R as RleOps<'a>>::enc(W::val(s0->Lit_current)),
        s0->Lit_local == s0->Lit_count ==> rw is None && b2 == (b0 + mid).take(s0->Lit_header_pos as int)
            + <W::R as RleOps<'a>>::enc_signed(-(total as int) as i64)
            + (b0 + mid).skip(s0->Lit_header_pos + ph::<W>().len()),
        s0->Lit_local < s0->Lit_count ==> b2 == b0 + mid
            && rw == Some(RewriteHeader { count: total, pos: s0->Lit_header_pos }),
    ensures
        apply::<W>(rw, pre, h0) + b2.skip(base)
            == groups_bytes::<W::R>(gs) + <W::R as RleOps<'a>>::enc_signed(-(total as int) as i64) + encs::<W::R>(lv),
        total == lv.len() ==> apply::<W>(rw, pre, h0) + b2.skip(base) == groups_bytes::<W::R>(gs.push(Group::Lit { values: lv })),
        base <= b2.len(),
{
    hide(canonical);
    let c = W::val(s0->Lit_current);
    let gb = groups_bytes::<W::R>(gs);
    lemma_push::<W::R>(gs, Group::Lit { values: lv });
    if lv != ws {
        lemma_encs_push::<W::R>(ws, c);
    } else {
        assert(b0 + mid =~= b0);
    }
    if s0->Lit_local == s0->Lit_count {
        let x = ph::<W>() + encs::<W::R>(ws);
        assert(gb + ph::<W>() + encs::<W::R>(ws) =~= gb + x);
        lemma_local(pre, b0.skip(base), gb, x);
        let a = gb.skip(pre.len() as int);
        let b1 = b0 + mid;
        assert(b1.skip(base) =~= b0.skip(base) + mid);
        if lv == ws {
            assert(encs::<W::R>(lv) + Seq::<u8>::empty() =~= encs::<W::R>(lv));
        }
        assert(encs::<W::R>(lv) == encs::<W::R>(ws) + mid);
        assert(b1.skip(base) =~= a + ph::<W>() + encs::<W::R>(lv));
        lemma_rewrite::<W::R>(b1, base, a, encs::<W::R>(lv), -(total as int), b2);
        assert(apply::<W>(rw, pre, h0) + b2.skip(base) =~= gb + <W::R as RleOps<'a>>::enc_signed(-(total as int) as i64) + encs::<W::R>(lv));
    } else {
        let k0 = s0->Lit_count - s0->Lit_local;
        if lv != ws {
            assert(lv.take(k0) =~= ws.take(k0));
            assert(lv.skip(k0) =~= ws.skip(k0).push(c));
            lemma_encs_push::<W::R>(ws.skip(k0), c);
            assert(b2.skip(base) =~= b0.skip(base) + mid);
        } else {
            assert(b2.skip(base) =~= b0.skip(base));
        }
        lemma_close_pre::<W>(pre, b2.skip(base), gs, lv, k0, h0, total, s0->Lit_header_pos);
    }
}

/// A literal in progress is closed by a run of its last value.
pub(crate) proof fn lemma_step_lit_close<'a, W: WriteOps<'a>>(s0: State<'a, W>, b0: Seq<u8>, s1: State<'a, W>, b1: Seq<u8>, rw: Option<RewriteHeader>, n: nat)
    requires
        s0 is Lit, s1 == State::<'a, W>::Run((n + 1) as usize, s1->Run_1),
        W::val(s1->Run_1) == W::val(s0->Lit_current),
        n >= 1, s0->Lit_count + 1 <= i64::MAX, n + 1 <= i64::MAX, n + 1 < usize::MAX,
        s0->Lit_local == s0->Lit_count ==> rw is None && b1 == b0.take(s0->Lit_header_pos as int)
            + <W::R as RleOps<'a>>::enc_signed(-(s0->Lit_count as int) as i64)
            + b0.skip(s0->Lit_header_pos + ph::<W>().len()),
        s0->Lit_local < s0->Lit_count ==> b1 == b0
            && rw == Some(RewriteHeader { count: s0->Lit_count, pos: s0->Lit_header_pos }),
    ensures step(s0, b0, s1, b1, rw, copies(n, W::val(s0->Lit_current)), s0->Lit_local as int),
{
    let c = W::val(s0->Lit_current);
    assert(b0 + Seq::<u8>::empty() =~= b0);
    assert forall|pre: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64| #[trigger] rel(s0, pre, b0, base, gs, ws, h0)
        implies exists|gs2: Seq<Group<VOf<'a, W>>>, ws2: Seq<VOf<'a, W>>| #[trigger] rel(s1, apply::<W>(rw, pre, h0), b1, base, gs2, ws2, h0)
            && values_of(s1, gs2, ws2) == values_of(s0, gs, ws) + copies(n, c)
            && acc(s1, gs2, ws2) == acc(s0, gs, ws) + s0->Lit_local as int by {
        let gl = Group::Lit { values: ws };
        let gs2 = gs.push(gl);
        let g1 = Group::Run { count: n + 1, value: c };
        lemma_push::<W::R>(gs, gl);
        lemma_close_bytes::<W>(s0, pre, base, gs, ws, h0, b0, Seq::empty(), ws, b1, rw, s0->Lit_count);
        lemma_lit_prefix::<W::R>(gs, ws, c);
        assert(gl == gs2.last());
        lemma_canonical_join::<W::R>(gs2, g1);
        let pre2 = apply::<W>(rw, pre, h0);
        assert(pre2 + b1.skip(base) =~= groups_bytes::<W::R>(gs2) + Seq::<u8>::empty());
        assert(all_groups(s1, gs2, Seq::empty()) == gs2.push(g1));
        assert(rel(s1, pre2, b1, base, gs2, Seq::empty(), h0));
        lemma_vals2::<W::R>(gs, gl, g1);
        lemma_push::<W::R>(gs, Group::Lit { values: ws.push(c) });
        lemma_copies_push(ws, c, n);
        assert(values_of(s1, gs2, Seq::empty()) =~= values_of(s0, gs, ws) + copies(n, c));
    }
}

/// A literal in progress takes its last value and is closed; a run of
/// another value begins.
pub(crate) proof fn lemma_step_lit_close_new<'a, W: WriteOps<'a>>(s0: State<'a, W>, b0: Seq<u8>, s1: State<'a, W>, b1: Seq<u8>, b2: Seq<u8>, rw: Option<RewriteHeader>, n: nat, v: VOf<'a, W>)
    requires
        s0 is Lit, s1 == State::<'a, W>::Run(n as usize, s1->Run_1), W::val(s1->Run_1) == v,
        v != W::val(s0->Lit_current), !<W::R as RleOps<'a>>::is_null(v),
        n >= 2, n <= i64::MAX, n < usize::MAX,
        b1 == b0 + <W::R as RleOps<'a>>::enc(W::val(s0->Lit_current)),
        s0->Lit_local == s0->Lit_count ==> rw is None && b2 == b1.take(s0->Lit_header_pos as int)
            + <W::R as RleOps<'a>>::enc_signed(-((s0->Lit_count + 1) as int) as i64)
            + b1.skip(s0->Lit_header_pos + ph::<W>().len()),
        s0->Lit_local < s0->Lit_count ==> b2 == b1
            && rw == Some(RewriteHeader { count: (s0->Lit_count + 1) as usize, pos: s0->Lit_header_pos }),
    ensures step(s0, b0, s1, b2, rw, copies(n, v), s0->Lit_local + 1),
{
    let c = W::val(s0->Lit_current);
    assert forall|pre: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64| #[trigger] rel(s0, pre, b0, base, gs, ws, h0)
        implies exists|gs2: Seq<Group<VOf<'a, W>>>, ws2: Seq<VOf<'a, W>>| #[trigger] rel(s1, apply::<W>(rw, pre, h0), b2, base, gs2, ws2, h0)
            && values_of(s1, gs2, ws2) == values_of(s0, gs, ws) + copies(n, v)
            && acc(s1, gs2, ws2) == acc(s0, gs, ws) + s0->Lit_local + 1 by {
        let w2 = ws.push(c);
        let gl = Group::Lit { values: w2 };
        let gs2 = gs.push(gl);
        let g1 = Group::Run { count: n, value: v };
        assert(w2 != ws) by {
            assert(w2.len() != ws.len());
        }
        lemma_close_bytes::<W>(s0, pre, base, gs, ws, h0, b0, <W::R as RleOps<'a>>::enc(c), w2, b2, rw, (s0->Lit_count + 1) as usize);
        lemma_canonical_split::<W::R>(gs, gl);
        assert(gl->values.last() == c);
        assert(gl == gs2.last());
        lemma_canonical_join::<W::R>(gs2, g1);
        let pre2 = apply::<W>(rw, pre, h0);
        assert(pre2 + b2.skip(base) =~= groups_bytes::<W::R>(gs2) + Seq::<u8>::empty());
        assert(all_groups(s1, gs2, Seq::empty()) == gs2.push(g1));
        assert(rel(s1, pre2, b2, base, gs2, Seq::empty(), h0));
        lemma_vals2::<W::R>(gs, gl, g1);
        lemma_push::<W::R>(gs, gl);
        assert(values_of(s1, gs2, Seq::empty()) =~= values_of(s0, gs, ws) + copies(n, v));
    }
}

/// After a flush, a null run begins.
pub(crate) proof fn lemma_step_flush_null<'a, W: WriteOps<'a>>(s0: State<'a, W>, b0: Seq<u8>, b1: Seq<u8>, rw: Option<RewriteHeader>, count: usize, seg: int)
    requires
        flushed(s0, b0, b1, rw, seg), !(s0 is Empty), !(s0 is Null),
        1 <= count, <W::R as RleOps<'a>>::nullable(),
    ensures step(s0, b0, State::<'a, W>::Null(count), b1, rw, copies(count as nat, <W::R as RleOps<'a>>::null()), seg),
{
    let s1 = State::<'a, W>::Null(count);
    let g1 = Group::<VOf<'a, W>>::Null { count: count as nat };
    assert forall|pre: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64| #[trigger] rel(s0, pre, b0, base, gs, ws, h0)
        implies exists|gs2: Seq<Group<VOf<'a, W>>>, ws2: Seq<VOf<'a, W>>| #[trigger] rel(s1, apply::<W>(rw, pre, h0), b1, base, gs2, ws2, h0)
            && values_of(s1, gs2, ws2) == values_of(s0, gs, ws) + copies(count as nat, <W::R as RleOps<'a>>::null())
            && acc(s1, gs2, ws2) == acc(s0, gs, ws) + seg by {
        let gs2 = all_groups(s0, gs, ws);
        assert(gs2.last() == pend_group(s0, ws)->Some_0);
        lemma_canonical_join::<W::R>(gs2, g1);
        lemma_push::<W::R>(gs2, g1);
        assert(apply::<W>(rw, pre, h0) + b1.skip(base) =~= groups_bytes::<W::R>(gs2) + Seq::<u8>::empty());
        assert(rel(s1, apply::<W>(rw, pre, h0), b1, base, gs2, Seq::empty(), h0));
    }
}

/// A state's segments, accounted and pending, are its groups', and no
/// more than its values.
pub(crate) proof fn lemma_acc<'a, W: WriteOps<'a>>(s: State<'a, W>, pre: Seq<u8>, buf: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64)
    requires rel(s, pre, buf, base, gs, ws, h0),
    ensures
        acc(s, gs, ws) + s.pseg() == groups_segments(all_groups(s, gs, ws)),
        groups_segments(all_groups(s, gs, ws)) <= values_of(s, gs, ws).len(),
        0 <= acc(s, gs, ws),
{
    let a = all_groups(s, gs, ws);
    if !(s is Empty) {
        lemma_push::<W::R>(gs, pend_group(s, ws)->Some_0);
    }
    super::read::lemma_segments_le::<W::R>(a);
    super::dec::lemma_groups_values_concat(a, Seq::empty(), <W::R as RleOps<'a>>::null());
    assert(a + Seq::<Group<VOf<'a, W>>>::empty() =~= a);
}

impl<'a, W: WriteOps<'a>> State<'a, W> {
    /// The pending group's length: what a flush would emit.
    pub(crate) open spec fn plen(self) -> nat {
        match self {
            State::Empty => 0,
            State::Lone(_) => 1,
            State::Run(n, _) => n as nat,
            State::Null(n) => n as nat,
            State::Lit { count, .. } => (count + 1) as nat,
        }
    }

    /// The segments a flush would add: [`pending_segments`](Self::pending_segments).
    pub(crate) open spec fn pseg(self) -> nat {
        match self {
            State::Empty => 0,
            State::Lit { local, .. } => (local + 1) as nat,
            _ => 1,
        }
    }

    /// The initial (empty) state.
    pub(crate) fn empty() -> (r: Self)
        ensures r is Empty,
    {
        State::Empty
    }

    /// Segments that will be produced when this state is flushed.
    pub(crate) fn pending_segments(&self) -> (r: usize)
        requires self matches State::Lit { local, .. } ==> local < usize::MAX,
        ensures r == self.pseg(),
    {
        match self {
            State::Empty => 0,
            State::Lone(_) => 1,
            State::Run(_, _) => 1,
            State::Lit { local, .. } => *local + 1,
            State::Null(_) => 1,
        }
    }

    /// The state holds nothing, or one run of `value` pending.
    pub(crate) fn single_run_of(&self, value: &W::Cow) -> (r: bool)
        ensures
            r == match *self {
                State::Empty => true,
                State::Lone(v) => W::val(v) == W::val(*value),
                State::Run(_, v) => W::val(v) == W::val(*value),
                State::Null(_) => <W::R as RleOps<'a>>::is_null(W::val(*value)),
                State::Lit { .. } => false,
            },
    {
        match self {
            State::Empty => true,
            State::Lone(v) => W::cow_eq(v, value),
            State::Run(_, v) => W::cow_eq(v, value),
            State::Null(_) => W::cow_is_null(value),
            _ => false,
        }
    }

    fn emit_lit(buf: &mut Vec<u8>, value: &W::Cow) -> (r: (usize, usize))
        requires !<W::R as RleOps<'a>>::is_null(W::val(*value)),
        ensures
            final(buf)@ == old(buf)@ + group_bytes::<W::R>(Group::Lit { values: seq![W::val(*value)] }),
            r.0 == old(buf).len(),
            r.1 == <W::R as RleOps<'a>>::enc(W::val(*value)).len(),
    {
        let pos = buf.len();
        W::put_signed(-1, buf);
        let bytes = W::cow_pack(value, buf);
        proof {
            lemma_lone_bytes::<W::R>(W::val(*value));
        }
        (pos, bytes)
    }

    fn emit_run(buf: &mut Vec<u8>, count: usize, value: &W::Cow) -> (r: WPos)
        requires !<W::R as RleOps<'a>>::is_null(W::val(*value)), 1 <= count <= i64::MAX,
        ensures
            final(buf)@ == old(buf)@ + group_bytes::<W::R>(run_group(count as nat, W::val(*value))),
            count == 1 ==> r is Lit && r->Lit_pos == old(buf).len() && r->Lit_local
                && r->Lit_bytes == <W::R as RleOps<'a>>::enc(W::val(*value)).len(),
            count > 1 ==> r == (WPos::Run { pos: old(buf).len(), local: true }),
    {
        let pos = buf.len();
        if count == 1 {
            W::put_signed(-1, buf);
            let bytes = W::cow_pack(value, buf);
            proof {
                lemma_lone_bytes::<W::R>(W::val(*value));
            }
            WPos::lit(pos, bytes, true)
        } else {
            W::put_signed(count as i64, buf);
            W::cow_pack(value, buf);
            WPos::run(pos, true)
        }
    }

    fn emit_null(buf: &mut Vec<u8>, count: usize) -> (r: WPos)
        ensures
            final(buf)@ == old(buf)@ + group_bytes::<W::R>(Group::Null { count: count as nat }),
            r == (WPos::Run { pos: old(buf).len(), local: true }),
    {
        let pos = buf.len();
        W::put_signed(0, buf);
        W::put_unsigned(count as u64, buf);
        WPos::run(pos, true)
    }

    /// A literal run imported from a slab; nothing written locally yet.
    pub(crate) fn lit(count: usize, current: W::Cow, header_pos: usize) -> (r: Self)
        ensures
            count == 0 ==> r == State::<'a, W>::Lone(current),
            count > 0 ==> r == (State::<'a, W>::Lit { count, local: 0, current, header_pos, bytes: 0 }),
    {
        if count == 0 {
            State::Lone(current)
        } else {
            State::Lit { count, local: 0, current, header_pos, bytes: 0 }
        }
    }

    pub(crate) fn make_run(n: usize, value: W::Cow) -> (r: Self)
        requires n <= i64::MAX,
        ensures
            n == 0 ==> r is Empty,
            n == 1 ==> r == State::<'a, W>::Lone(value),
            n >= 2 ==> r == State::<'a, W>::Run(n, value) && n <= i64::MAX,
    {
        match n {
            0 => State::Empty,
            1 => State::Lone(value),
            n => State::Run(n, value),
        }
    }

    /// Emit the pending group.
    #[verifier::spinoff_prover]
    #[verifier::rlimit(100)]
    pub(crate) fn do_flush(state: Self, buf: &mut Vec<u8>) -> (f: FlushState)
        requires wf(state, old(buf)@),
        ensures
            keeps(old(buf)@, final(buf)@, wlo(state, old(buf)@)),
            flushed(state, old(buf)@, final(buf)@, f.rewrite, f.segments as int), rw_from(state, f.rewrite),
            f.segments == state.pseg(), flushed_tail(state, old(buf)@, final(buf)@, f.rewrite, f.wpos),
            state is Empty ==> final(buf)@ == old(buf)@ && f.wpos == (WPos::Run { pos: 0, local: true }),
    {
        let ghost b0 = buf@;
        proof {
            assert forall|pre: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64| #[trigger] rel(state, pre, b0, base, gs, ws, h0)
                implies groups_segments(all_groups(state, gs, ws)) == acc(state, gs, ws) + state.pseg() by {
                lemma_acc::<W>(state, pre, b0, base, gs, ws, h0);
            }
        }
        match state {
            State::Empty => {
                proof {
                    assert forall|pre: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64| #[trigger] rel(state, pre, b0, base, gs, ws, h0)
                        implies apply::<W>(None, pre, h0) + buf@.skip(base) == groups_bytes::<W::R>(all_groups(state, gs, ws)) by {
                        assert(pre + buf@.skip(base) =~= Seq::<u8>::empty());
                        assert(groups_bytes::<W::R>(gs) =~= Seq::<u8>::empty());
                    }
                }
                FlushState::none()
            },
            State::Lone(value) => {
                let wpos = Self::emit_run(buf, 1, &value);
                proof {
                    assert forall|pre: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64| #[trigger] rel(state, pre, b0, base, gs, ws, h0)
                        implies 0 <= base <= buf@.len() && apply::<W>(None, pre, h0) + buf@.skip(base) == groups_bytes::<W::R>(all_groups(state, gs, ws)) by {
                        lemma_emit::<W::R>(pre, b0, base, gs, run_group(1, W::val(value)), buf@);
                    }
                }
                proof {
                    assert forall|pre: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64| #[trigger] rel(state, pre, b0, base, gs, ws, h0) && !(state is Empty)
                        implies wpos_names::<W>(wpos, all_groups(state, gs, ws).last(), groups_bytes::<W::R>(gs).len() as int,
                            apply::<W>(None, pre, h0).len() as int, base) by {
                        assert((pre + b0.skip(base)).len() == pre.len() + b0.len() - base);
                        let g = Group::Lit { values: seq![W::val(value)] };
                        assert(pend_group(state, ws) == Some(g));
                        assert(gs.push(g).last() == g);
                        assert(seq![W::val(value)].last() == W::val(value));
                    }
                }
                FlushState::new(1, wpos)
            },
            State::Run(count, value) => {
                let wpos = Self::emit_run(buf, count, &value);
                proof {
                    assert forall|pre: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64| #[trigger] rel(state, pre, b0, base, gs, ws, h0)
                        implies 0 <= base <= buf@.len() && apply::<W>(None, pre, h0) + buf@.skip(base) == groups_bytes::<W::R>(all_groups(state, gs, ws)) by {
                        lemma_canonical_split::<W::R>(gs, pend_group(state, ws)->Some_0);
                        lemma_emit::<W::R>(pre, b0, base, gs, run_group(count as nat, W::val(value)), buf@);
                    }
                }
                proof {
                    assert forall|pre: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64| #[trigger] rel(state, pre, b0, base, gs, ws, h0) && !(state is Empty)
                        implies wpos_names::<W>(wpos, all_groups(state, gs, ws).last(), groups_bytes::<W::R>(gs).len() as int,
                            apply::<W>(None, pre, h0).len() as int, base) by {
                        assert((pre + b0.skip(base)).len() == pre.len() + b0.len() - base);

                    }
                }
                FlushState::new(1, wpos)
            },
            State::Lit { count, local, header_pos, current, .. } => {
                assert(count + 1 <= i64::MAX);
                let bytes = W::cow_pack(&current, buf);
                let ghost b1 = buf@;
                let ghost c = W::val(current);
                let wpos = WPos::lit(header_pos, bytes, count == local);
                let total = count + 1;
                if local == count {
                    proof {
                        assert(b1.skip(header_pos as int) =~= b0.skip(header_pos as int) + <W::R as RleOps<'a>>::enc(c));
                        assert(ph::<W>().is_prefix_of(b1.skip(header_pos as int)));
                    }
                    W::rewrite_lit_header(buf, header_pos, total);
                    proof {
                        assert forall|pre: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64| #[trigger] rel(state, pre, b0, base, gs, ws, h0)
                        implies 0 <= base <= buf@.len() && apply::<W>(None, pre, h0) + buf@.skip(base) == groups_bytes::<W::R>(all_groups(state, gs, ws)) by {
                            let gb = groups_bytes::<W::R>(gs);
                            assert(gb + ph::<W>() + encs::<W::R>(ws) =~= gb + (ph::<W>() + encs::<W::R>(ws)));
                            lemma_local(pre, b0.skip(base), gb, ph::<W>() + encs::<W::R>(ws));
                            let a = gb.skip(pre.len() as int);
                            assert(b0.skip(base) =~= a + ph::<W>() + encs::<W::R>(ws));
                            lemma_flush_lit::<W>(b0, base, a, ws, c, b1, buf@);
                            lemma_push::<W::R>(gs, Group::Lit { values: ws.push(c) });
                            assert(pre + buf@.skip(base) =~= gb + group_bytes::<W::R>(Group::Lit { values: ws.push(c) }));
                        }
                    }
                proof {
                    assert forall|pre: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64| #[trigger] rel(state, pre, b0, base, gs, ws, h0) && !(state is Empty)
                        implies wpos_names::<W>(wpos, all_groups(state, gs, ws).last(), groups_bytes::<W::R>(gs).len() as int,
                            apply::<W>(None, pre, h0).len() as int, base) by {
                        assert((pre + b0.skip(base)).len() == pre.len() + b0.len() - base);
                        assert(ws.push(c).last() == c);
                    }
                }
                    FlushState::new(total, wpos)
                } else {
                    let rewrite = RewriteHeader::new(total, header_pos);
                    proof {
                        assert forall|pre: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64| #[trigger] rel(state, pre, b0, base, gs, ws, h0)
                        implies 0 <= base <= buf@.len() && apply::<W>(Some(rewrite), pre, h0) + buf@.skip(base) == groups_bytes::<W::R>(all_groups(state, gs, ws)) by {
                            let k0 = count - local;
                            let w2 = ws.push(c);
                            assert(w2.take(k0) =~= ws.take(k0));
                            assert(w2.skip(k0) =~= ws.skip(k0).push(c));
                            lemma_encs_push::<W::R>(ws.skip(k0), c);
                            assert(buf@.skip(base) =~= b0.skip(base) + <W::R as RleOps<'a>>::enc(c));
                            lemma_close_pre::<W>(pre, buf@.skip(base), gs, w2, k0, h0, total, header_pos);
                        }
                    }
                proof {
                    assert forall|pre: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64| #[trigger] rel(state, pre, b0, base, gs, ws, h0) && !(state is Empty)
                        implies wpos_names::<W>(wpos, all_groups(state, gs, ws).last(), groups_bytes::<W::R>(gs).len() as int,
                            apply::<W>(Some(rewrite), pre, h0).len() as int, base) by {
                        assert((pre + b0.skip(base)).len() == pre.len() + b0.len() - base);
                        assert(ws.push(c).last() == c);
                    }
                }
                    FlushState::rewrite(local + 1, rewrite, wpos)
                }
            },
            State::Null(count) => {
                let wpos = Self::emit_null(buf, count);
                proof {
                    assert forall|pre: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64| #[trigger] rel(state, pre, b0, base, gs, ws, h0)
                        implies 0 <= base <= buf@.len() && apply::<W>(None, pre, h0) + buf@.skip(base) == groups_bytes::<W::R>(all_groups(state, gs, ws)) by {
                        lemma_emit::<W::R>(pre, b0, base, gs, Group::Null { count: count as nat }, buf@);
                    }
                }
                proof {
                    assert forall|pre: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64| #[trigger] rel(state, pre, b0, base, gs, ws, h0) && !(state is Empty)
                        implies wpos_names::<W>(wpos, all_groups(state, gs, ws).last(), groups_bytes::<W::R>(gs).len() as int,
                            apply::<W>(None, pre, h0).len() as int, base) by {
                        assert((pre + b0.skip(base)).len() == pre.len() + b0.len() - base);

                    }
                }
                FlushState::new(1, wpos)
            },
        }
    }

    /// Append `n` copies of `value`.
    #[inline(always)]
    pub(crate) fn append_cow(&mut self, buf: &mut Vec<u8>, value: W::Cow, n: usize) -> (f: FlushState)
        requires
            wf(*old(self), old(buf)@), old(self).plen() + n + 1 < usize::MAX,
            <W::R as RleOps<'a>>::is_null(W::val(value)) ==> <W::R as RleOps<'a>>::nullable(),
        ensures
            keeps(old(buf)@, final(buf)@, wlo(*old(self), old(buf)@)),
            wf(*final(self), final(buf)@),
            final(self).plen() <= old(self).plen() + n,
            f.segments + final(self).pseg() <= old(self).pseg() + n,
            step(*old(self), old(buf)@, *final(self), final(buf)@, f.rewrite, copies(n as nat, W::val(value)), f.segments as int),
            rw_ok(*old(self), *final(self), f.rewrite),
            n >= 1 ==> last_is(*final(self), W::val(value)),
            n >= 2 && !<W::R as RleOps<'a>>::is_null(W::val(value)) ==> (*final(self)) is Run,
    {
        let ghost s0 = *self;
        let ghost b0 = buf@;
        let ghost v = W::val(value);
        let ghost added = copies(n as nat, v);
        if n == 0 {
            proof {
                assert forall|pre: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64| #[trigger] rel(s0, pre, b0, base, gs, ws, h0)
                    implies exists|gs2: Seq<Group<VOf<'a, W>>>, ws2: Seq<VOf<'a, W>>| #[trigger] rel(s0, apply::<W>(None, pre, h0), b0, base, gs2, ws2, h0)
                        && values_of(s0, gs2, ws2) == values_of(s0, gs, ws) + added
                        && acc(s0, gs2, ws2) == acc(s0, gs, ws) + 0 by {
                    assert(values_of(s0, gs, ws) + added =~= values_of(s0, gs, ws));
                    assert(rel(s0, apply::<W>(None, pre, h0), b0, base, gs, ws, h0));
                }
            }
            return FlushState::none();
        }
        if W::cow_is_null(&value) {
            proof {
                <W::R as RleOps<'a>>::axiom_null(v);
            }
            return self.append_null_n(buf, n);
        }
        // Checked before anything is written, so a panic leaves the state
        // as it was. A repeat run's header is an `i64`, and a canonical
        // encoding can't split the run in two, so a longer run has no
        // encoding (like a `Column::fill` that long).
        check_run(n);
        let mut flushed = FlushState::none();
        let ghost mut s1 = State::<'a, W>::Empty;
        let next_state = match self {
            State::Run(count, prev) => {
                let ghost pc = *prev;
                if W::cow_eq(&value, prev) {
                    let ghost c0 = *count;
                    if n as u64 > i64::MAX as u64 - *count as u64 {
                        run_too_long(count.saturating_add(n));
                    }
                    *count = *count + n;
                    proof {
                        s1 = State::Run(*count, pc);
                        lemma_copies(c0 as nat, n as nat, v);
                        lemma_step_grow::<W>(s0, b0, s1, added);
                    }
                    None
                } else {
                    flushed.wpos = Self::emit_run(buf, *count, prev);
                    flushed.segments = 1;
                    let r = Self::make_run(n, value);
                    proof {
                        s1 = r;
                        lemma_run_group::<W::R>(n as nat, v);
                        lemma_lone_bytes::<W::R>(v);
                        lemma_step_emit::<W>(s0, b0, run_group(s0->Run_0 as nat, W::val(pc)), s1, buf@, added);
                    }
                    Some(r)
                }
            },
            State::Lit { count, local, header_pos, current, bytes } => {
                let ghost cur = *current;
                if W::cow_eq(&value, current) {
                    if n as u64 >= i64::MAX as u64 {
                        run_too_long(n.saturating_add(1));
                    }
                    if *local == *count {
                        let ghost hp = *header_pos as int;
                        W::rewrite_lit_header(buf, *header_pos, *count);
                        proof {
                            assert(<W::R as RleOps<'a>>::enc_signed(-1i64).is_prefix_of(b0.skip(hp)));
                        }
                    } else {
                        flushed.rewrite = Some(RewriteHeader::new(*count, *header_pos));
                    }
                    flushed.wpos = WPos::trunc(*count, *header_pos, *bytes, *local == *count);
                    flushed.segments = *local;
                    let r = State::Run(n + 1, value);
                    proof {
                        s1 = r;
                        lemma_step_lit_close::<W>(s0, b0, s1, buf@, flushed.rewrite, n as nat);
                    }
                    Some(r)
                } else if n == 1 {
                    if *count as u64 >= i64::MAX as u64 - 1 {
                        lit_too_long();
                    }
                    *bytes = W::cow_pack(current, buf);
                    *count = *count + 1;
                    *local = *local + 1;
                    *current = value;
                    proof {
                        s1 = State::Lit { count: *count, local: *local, header_pos: *header_pos, bytes: *bytes, current: value };
                        lemma_step_lit_push::<W>(s0, b0, s1, buf@, v);
                    }
                    None
                } else {
                    let bytes = W::cow_pack(current, buf);
                    let ghost b1 = buf@;
                    flushed.wpos = WPos::lit(*header_pos, bytes, *count == *local);
                    if *local == *count {
                        let ghost hp = *header_pos as int;
                        proof {
                            assert(b1.skip(hp) =~= b0.skip(hp) + <W::R as RleOps<'a>>::enc(W::val(cur)));
                            assert(<W::R as RleOps<'a>>::enc_signed(-1i64).is_prefix_of(b1.skip(hp)));
                        }
                        W::rewrite_lit_header(buf, *header_pos, *count + 1);
                    } else {
                        flushed.rewrite = Some(RewriteHeader::new(*count + 1, *header_pos));
                    }
                    flushed.segments = *local + 1;
                    let r = State::Run(n, value);
                    proof {
                        s1 = r;
                        lemma_step_lit_close_new::<W>(s0, b0, s1, b1, buf@, flushed.rewrite, n as nat, v);
                    }
                    Some(r)
                }
            },
            State::Empty => {
                let r = if n == 1 {
                    State::Lone(value)
                } else {
                    State::Run(n, value)
                };
                proof {
                    s1 = r;
                    lemma_run_group::<W::R>(n as nat, v);
                    lemma_lone_bytes::<W::R>(v);
                    lemma_step_empty::<W>(s0, b0, s1, added);
                }
                Some(r)
            },
            State::Lone(prev) => {
                let ghost p = W::val(*prev);
                if W::cow_eq(&value, prev) {
                    if n as u64 >= i64::MAX as u64 {
                        run_too_long(n.saturating_add(1));
                    }
                    let r = State::Run(n + 1, value);
                    proof {
                        s1 = r;
                        lemma_lone_bytes::<W::R>(p);
                        lemma_copies(1, n as nat, v);
                        lemma_step_grow::<W>(s0, b0, s1, added);
                    }
                    Some(r)
                } else if n == 1 {
                    let (header_pos, bytes) = Self::emit_lit(buf, prev);
                    let r = State::Lit { count: 1, local: 1, header_pos, bytes, current: value };
                    proof {
                        s1 = r;
                        lemma_step_lone_lit::<W>(s0, b0, s1, buf@, v);
                    }
                    Some(r)
                } else {
                    let (pos, bytes) = Self::emit_lit(buf, prev);
                    flushed.wpos = WPos::lit(pos, bytes, true);
                    flushed.segments = 1;
                    let r = State::Run(n, value);
                    proof {
                        s1 = r;
                        lemma_step_emit::<W>(s0, b0, Group::Lit { values: seq![p] }, s1, buf@, added);
                    }
                    Some(r)
                }
            },
            State::Null(count) => {
                flushed.wpos = Self::emit_null(buf, *count);
                flushed.segments = 1;
                let r = Self::make_run(n, value);
                proof {
                    s1 = r;
                    lemma_run_group::<W::R>(n as nat, v);
                    lemma_lone_bytes::<W::R>(v);
                    lemma_step_emit::<W>(s0, b0, Group::Null { count: s0->Null_0 as nat }, s1, buf@, added);
                }
                Some(r)
            },
        };
        if let Some(s) = next_state {
            *self = s;
        }
        proof {
            assert(*self == s1);
        }
        flushed
    }

    /// Append `count` nulls.
    #[inline]
    pub(crate) fn append_null_n(&mut self, buf: &mut Vec<u8>, count: usize) -> (f: FlushState)
        requires
            wf(*old(self), old(buf)@), <W::R as RleOps<'a>>::nullable(),
            1 <= count, old(self).plen() + count + 1 < usize::MAX,
        ensures
            keeps(old(buf)@, final(buf)@, wlo(*old(self), old(buf)@)),
            wf(*final(self), final(buf)@),
            final(self).plen() <= old(self).plen() + count,
            f.segments + final(self).pseg() <= old(self).pseg() + count,
            step(*old(self), old(buf)@, *final(self), final(buf)@, f.rewrite, copies(count as nat, <W::R as RleOps<'a>>::null()), f.segments as int),
            rw_ok(*old(self), *final(self), f.rewrite), (*final(self)) is Null,
    {
        let ghost s0 = *self;
        let ghost b0 = buf@;
        let ghost added = copies(count as nat, <W::R as RleOps<'a>>::null());
        let mut old = State::Empty;
        core::mem::swap(self, &mut old);
        let mut flushed = FlushState::none();
        *self = match old {
            State::Empty => {
                proof {
                    lemma_step_empty::<W>(s0, b0, State::Null(count), added);
                }
                State::Null(count)
            },
            State::Null(n) => {
                proof {
                    lemma_copies(n as nat, count as nat, <W::R as RleOps<'a>>::null());
                    lemma_step_grow::<W>(s0, b0, State::Null((n + count) as usize), added);
                }
                State::Null(n + count)
            },
            other => {
                flushed = Self::do_flush(other, buf);
                proof {
                    lemma_step_flush_null::<W>(s0, b0, buf@, flushed.rewrite, count, flushed.segments as int);
                }
                State::Null(count)
            },
        };
        flushed
    }

    /// Flush onto `lit` literal values that follow in the slab: the
    /// pending values join them in one literal.
    #[verifier::spinoff_prover]
    #[verifier::rlimit(40)]
    pub(crate) fn flush_with_lit(&mut self, buf: &mut Vec<u8>, lit: usize) -> (f: FlushState)
        requires
            wf(*old(self), old(buf)@), old(self).plen() + lit + 1 < usize::MAX,
        ensures
            keeps(old(buf)@, final(buf)@, wlo(*old(self), old(buf)@)),
            (*final(self)) is Empty, rw_from(*old(self), f.rewrite),
            lit > 0 ==> lit_fits(*old(self), lit as nat), f.segments == old(self).pseg(),
            lit == 0 ==> flushed_tail(*old(self), old(buf)@, final(buf)@, f.rewrite, f.wpos),
            lit > 0 ==> flushed_lit_tail(*old(self), old(buf)@, final(buf)@, f.rewrite, f.wpos),
            lit == 0 ==> flushed(*old(self), old(buf)@, final(buf)@, f.rewrite, f.segments as int),
            lit > 0 ==> flushed_lit(*old(self), old(buf)@, final(buf)@, f.rewrite, lit as nat, f.segments as int),
    {
        // Checked before anything is written, so a panic leaves the state
        // as it was: the literal's header must hold its values and these.
        let held: usize = match self {
            State::Lit { count, .. } => *count + 1,
            State::Lone(_) => 1,
            _ => 0,
        };
        if lit as u64 > i64::MAX as u64 - held as u64 {
            lit_too_long();
        }
        let ghost s0 = *self;
        let ghost b0 = buf@;
        let mut old = State::Empty;
        core::mem::swap(self, &mut old);
        if lit == 0 {
            return Self::do_flush(old, buf);
        }
        match old {
            State::Lit { count, local, header_pos, current, .. } => {
                let ghost c = W::val(current);
                let bytes = W::cow_pack(&current, buf);
                let ghost b1 = buf@;
                let wpos = WPos::lit(header_pos, bytes, count == local);
                let total = count + 1 + lit;
                if local == count {
                    proof {
                        assert(b1.skip(header_pos as int) =~= b0.skip(header_pos as int) + <W::R as RleOps<'a>>::enc(c));
                        assert(ph::<W>().is_prefix_of(b1.skip(header_pos as int)));
                    }
                    W::rewrite_lit_header(buf, header_pos, total);
                    let f = FlushState::new(local + 1, wpos);
                    proof {
                        assert forall|pre: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64| #[trigger] rel(s0, pre, b0, base, gs, ws, h0)
                            implies 0 <= base <= buf@.len() && apply::<W>(f.rewrite, pre, h0) + buf@.skip(base) == lit_flush_bytes(s0, gs, ws, lit as nat) by {
                            assert(ws.push(c) != ws) by {
                                assert(ws.push(c).len() != ws.len());
                            }
                            lemma_close_bytes::<W>(s0, pre, base, gs, ws, h0, b0, <W::R as RleOps<'a>>::enc(c), ws.push(c), buf@, f.rewrite, total);
                        }
                    }
                    f
                } else {
                    let f = FlushState::rewrite(local + 1, RewriteHeader::new(total, header_pos), wpos);
                    proof {
                        assert forall|pre: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64| #[trigger] rel(s0, pre, b0, base, gs, ws, h0)
                            implies 0 <= base <= buf@.len() && apply::<W>(f.rewrite, pre, h0) + buf@.skip(base) == lit_flush_bytes(s0, gs, ws, lit as nat) by {
                            assert(ws.push(c) != ws) by {
                                assert(ws.push(c).len() != ws.len());
                            }
                            lemma_close_bytes::<W>(s0, pre, base, gs, ws, h0, b0, <W::R as RleOps<'a>>::enc(c), ws.push(c), buf@, f.rewrite, total);
                        }
                    }
                    f
                }
            },
            State::Lone(value) => {
                let total = 1 + lit;
                let pos = buf.len();
                W::put_signed(-(total as i64), buf);
                let bytes = W::cow_pack(&value, buf);
                proof {
                    assert forall|pre: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64| #[trigger] rel(s0, pre, b0, base, gs, ws, h0)
                        implies 0 <= base <= buf@.len() && apply::<W>(None, pre, h0) + buf@.skip(base) == lit_flush_bytes(s0, gs, ws, lit as nat) by {
                        assert(buf@.skip(base) =~= b0.skip(base) + <W::R as RleOps<'a>>::enc_signed(-(total as int) as i64)
                            + <W::R as RleOps<'a>>::enc(W::val(value)));
                    }
                }
                FlushState::new(1, WPos::lit(pos, bytes, true))
            },
            other => {
                let mut flushed = Self::do_flush(other, buf);
                let ghost b1 = buf@;
                let pos = buf.len();
                W::put_signed(-(lit as i64), buf);
                flushed.wpos = WPos::lit(pos, 0, true);
                proof {
                    assert forall|pre: Seq<u8>, base: int, gs: Seq<Group<VOf<'a, W>>>, ws: Seq<VOf<'a, W>>, h0: i64| #[trigger] rel(s0, pre, b0, base, gs, ws, h0)
                        implies 0 <= base <= buf@.len() && apply::<W>(flushed.rewrite, pre, h0) + buf@.skip(base) == lit_flush_bytes(s0, gs, ws, lit as nat) by {
                        assert(buf@.skip(base) =~= b1.skip(base) + <W::R as RleOps<'a>>::enc_signed(-(lit as int) as i64));
                    }
                }
                flushed
            },
        }
    }

    pub(crate) fn flush(&mut self, buf: &mut Vec<u8>) -> (f: FlushState)
        requires wf(*old(self), old(buf)@),
        ensures
            keeps(old(buf)@, final(buf)@, wlo(*old(self), old(buf)@)),
            flushed(*old(self), old(buf)@, final(buf)@, f.rewrite, f.segments as int), (*final(self)) is Empty,
            rw_from(*old(self), f.rewrite), f.segments == old(self).pseg(),
            flushed_tail(*old(self), old(buf)@, final(buf)@, f.rewrite, f.wpos),
            (*old(self)) is Empty ==> final(buf)@ == old(buf)@ && f.wpos == (WPos::Run { pos: 0, local: true }),
    {
        let mut old = State::Empty;
        core::mem::swap(self, &mut old);
        Self::do_flush(old, buf)
    }
}

} // verus!

/// The byte length of a slab's last run, which its [`RleTail`](crate::rle::RleTail) records
/// in a `u32`.
///
/// # Panics
///
/// If the run is 4 GiB or more: it has no tail record, so — like a run
/// of more than `i64::MAX` values — it is not encodable.
pub(crate) fn tail_bytes(n: usize) -> u32 {
    u32::try_from(n).unwrap_or_else(|_| {
        panic!("a run of {n} bytes is not encodable: a run must stay under 4 GiB")
    })
}

impl Default for WPos {
    fn default() -> Self {
        WPos::Run {
            pos: 0,
            local: true,
        }
    }
}

impl std::ops::AddAssign for FlushState {
    fn add_assign(&mut self, rhs: Self) {
        self.segments += rhs.segments;
        self.wpos = rhs.wpos;
        if rhs.rewrite.is_some() {
            self.rewrite = rhs.rewrite;
        }
    }
}

impl std::ops::AddAssign<Option<FlushState>> for FlushState {
    fn add_assign(&mut self, rhs: Option<FlushState>) {
        if let Some(rhs) = rhs {
            *self += rhs;
        }
    }
}

impl<'a, W: WriteOps<'a>> std::fmt::Debug for State<'a, W>
where
    W::Cow: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            State::Empty => f.write_str("Empty"),
            State::Lone(v) => f.debug_tuple("Lone").field(v).finish(),
            State::Run(n, v) => f.debug_tuple("Run").field(n).field(v).finish(),
            State::Lit {
                count,
                local,
                header_pos,
                bytes,
                current,
            } => f
                .debug_struct("Lit")
                .field("count", count)
                .field("local", local)
                .field("header_pos", header_pos)
                .field("bytes", bytes)
                .field("current", current)
                .finish(),
            State::Null(n) => f.debug_tuple("Null").field(n).finish(),
        }
    }
}

#[cfg(test)]
impl<'a, T: RleValue, V: AsColumnRef<T>, C: Codec> RleState<'a, T, V, C> {
    pub fn append(&mut self, buf: &mut Vec<u8>, value: impl Into<RleCow<'a, T, V>>) -> FlushState {
        self.append_cow(buf, value.into(), 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rle::rle_validate_encoding;

    fn encode_vals(vals: &[u64]) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut state = RleState::<u64, u64>::empty();
        for &v in vals {
            state.append(&mut buf, RleCow::Ref(v));
        }
        state.flush(&mut buf);
        buf
    }

    fn decode(buf: &[u8]) -> Vec<u64> {
        let mut result = Vec::new();
        let mut pos = 0;
        while pos < buf.len() {
            let (cb, raw) = Leb128::read_signed(&buf[pos..]).unwrap();
            match raw {
                n if n > 0 => {
                    let (vl, val) = u64::try_unpack::<Leb128>(&buf[pos + cb..]).unwrap();
                    for _ in 0..n as usize {
                        result.push(val);
                    }
                    pos += cb + vl;
                }
                n if n < 0 => {
                    let mut scan = pos + cb;
                    for _ in 0..(-n) as usize {
                        let (vl, val) = u64::try_unpack::<Leb128>(&buf[scan..]).unwrap();
                        result.push(val);
                        scan += vl;
                    }
                    pos = scan;
                }
                _ => {
                    let (ncb, nc) = Leb128::read_unsigned(&buf[pos + cb..]).unwrap();
                    result.resize(result.len() + nc as usize, 0);
                    pos += cb + ncb;
                }
            }
        }
        result
    }

    fn check(vals: &[u64]) {
        let buf = encode_vals(vals);
        if let Err(e) = rle_validate_encoding::<u64, Leb128>(&buf) {
            panic!("invalid encoding for {vals:?}: {e}\n  bytes: {buf:?}");
        }
        assert_eq!(decode(&buf), vals, "roundtrip failed for {vals:?}");
    }

    #[test]
    fn empty() {
        check(&[]);
    }
    #[test]
    fn single() {
        check(&[42]);
    }
    #[test]
    fn repeat() {
        check(&[7, 7, 7, 7, 7]);
    }
    #[test]
    fn literal() {
        check(&[1, 2, 3, 4]);
    }
    #[test]
    fn lit_then_repeat() {
        check(&[1, 2, 3, 3, 3]);
    }
    #[test]
    fn repeat_then_lit() {
        check(&[5, 5, 5, 1, 2, 3]);
    }
    #[test]
    fn lit_repeat_lit() {
        check(&[1, 2, 3, 3, 3, 4, 5]);
    }
    #[test]
    fn adjacent_repeats() {
        check(&[1, 1, 1, 2, 2, 2]);
    }
    #[test]
    fn lone_between_repeats() {
        check(&[1, 1, 2, 3, 3]);
    }
    #[test]
    fn two_same() {
        check(&[9, 9]);
    }
    #[test]
    fn two_different() {
        check(&[1, 2]);
    }
    #[test]
    fn long_literal() {
        check(&(0..100).collect::<Vec<_>>());
    }
    #[test]
    fn alternating() {
        check(
            &(0..20)
                .map(|i| if i % 2 == 0 { 1 } else { 2 })
                .collect::<Vec<_>>(),
        );
    }
    #[test]
    fn roundtrip_complex() {
        check(&[1, 2, 3, 3, 3, 4, 5, 5, 6, 7, 7, 7, 7, 8]);
    }
    #[test]
    fn single_then_repeat() {
        check(&[1, 2, 2]);
    }
    #[test]
    fn repeat_then_single() {
        check(&[2, 2, 1]);
    }

    #[test]
    fn nullable_with_nulls() {
        let vals: Vec<Option<u64>> = vec![Some(1), None, None, Some(2), Some(2)];
        let mut buf = Vec::new();
        let mut state = RleState::<Option<u64>, Option<u64>>::empty();
        for &v in &vals {
            state.append(&mut buf, RleCow::Ref(v));
        }
        state.flush(&mut buf);
        rle_validate_encoding::<Option<u64>, Leb128>(&buf).unwrap();
    }
    #[test]
    fn nullable_null_value_null() {
        let vals: Vec<Option<u64>> = vec![None, Some(5), None];
        let mut buf = Vec::new();
        let mut state = RleState::<Option<u64>, Option<u64>>::empty();
        for &v in &vals {
            state.append(&mut buf, RleCow::Ref(v));
        }
        state.flush(&mut buf);
        rle_validate_encoding::<Option<u64>, Leb128>(&buf).unwrap();
    }
    #[test]
    fn nullable_value_then_null() {
        let vals: Vec<Option<u64>> = vec![Some(5), None];
        let mut buf = Vec::new();
        let mut state = RleState::<Option<u64>, Option<u64>>::empty();
        for &v in &vals {
            state.append(&mut buf, RleCow::Ref(v));
        }
        state.flush(&mut buf);
        rle_validate_encoding::<Option<u64>, Leb128>(&buf).unwrap();
    }
}
