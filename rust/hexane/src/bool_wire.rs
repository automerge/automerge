//! The boolean wire format, verified with Verus.
//!
//! A bool slab is a sequence of unsigned counts (`counts_bytes`), the
//! lengths of alternating runs starting with `false` (`counts_values`).
//! It is canonical (`counts_ok`) when only the first count is zero (so
//! that a slab can start with `true`) and the last is not.
//!
//! [`validate`] accepts exactly the canonical encodings of at most
//! `usize::MAX` values, and [`BoolLoad`] cuts them into slabs of an even
//! number of runs, each again canonical, which together hold the input's
//! values.

// Verus proofs read more plainly with `x = x + n`.
#![allow(
    clippy::len_zero,
    clippy::assign_op_pattern,
    clippy::too_many_arguments,
    clippy::manual_unwrap_or_default,
    clippy::manual_unwrap_or,
    clippy::manual_is_multiple_of
)]

use crate::{PackError, Run};
#[allow(unused_imports)]
use vstd::prelude::*;

verus! {

/// What the bool format needs of its varint codec (trusted, like the
/// RLE format's codec: the codec's own encoder and decoder are verified,
/// but are reached through the generic `Codec` trait).
pub trait CountOps {
    /// The codec's encoding of an unsigned integer.
    spec fn enc(n: u64) -> Seq<u8>;

    /// Encodings are nonempty, at most 10 bytes, and no two start the
    /// same string.
    proof fn axiom_enc(a: u64, b: u64, s: Seq<u8>, t: Seq<u8>)
        ensures
            0 < Self::enc(a).len() <= 10,
            Self::enc(a) + s == Self::enc(b) + t ==> a == b;

    /// Append the encoding of `n`.
    fn put_count(n: usize, buf: &mut Vec<u8>)
        ensures final(buf)@ == old(buf)@ + Self::enc(n as u64);

    /// Decode the count at the start of `data`, if one that fits a
    /// `usize` is there.
    fn read_count(data: &[u8]) -> (r: Option<(usize, usize)>)
        ensures
            match r {
                Some((n, x)) => n <= data.len() && data@.take(n as int) == Self::enc(x as u64),
                None => forall|x: u64| x <= usize::MAX ==> !(#[trigger] Self::enc(x)).is_prefix_of(data@),
            };
}

// ── The model ───────────────────────────────────────────────────────────────

/// The bytes of counts `cs`.
pub(crate) open spec fn counts_bytes<K: CountOps>(cs: Seq<u64>) -> Seq<u8>
    decreases cs.len(),
{
    if cs.len() == 0 { Seq::empty() } else { counts_bytes::<K>(cs.drop_last()) + K::enc(cs.last()) }
}

/// How many values counts `cs` stand for.
pub(crate) open spec fn counts_len(cs: Seq<u64>) -> nat
    decreases cs.len(),
{
    if cs.len() == 0 { 0 } else { counts_len(cs.drop_last()) + cs.last() as nat }
}

/// Run `i`'s value: runs alternate, starting with `false`.
pub(crate) open spec fn run_value(i: int) -> bool {
    i % 2 == 1
}

/// The values counts `cs` stand for.
pub(crate) open spec fn counts_values(cs: Seq<u64>) -> Seq<bool>
    decreases cs.len(),
{
    if cs.len() == 0 {
        Seq::empty()
    } else {
        counts_values(cs.drop_last()) + Seq::new(cs.last() as nat, |_j: int| run_value(cs.len() - 1))
    }
}

/// Canonical counts: only the first may be zero, and not if it is the
/// last.
pub(crate) open spec fn counts_ok(cs: Seq<u64>) -> bool {
    &&& forall|i: int| 1 <= i < cs.len() ==> #[trigger] cs[i] > 0
    &&& cs.len() > 0 ==> cs.last() > 0
}

/// `cs` is the canonical parse of `data`, of at most `usize::MAX` values.
pub(crate) open spec fn bool_parses<K: CountOps>(cs: Seq<u64>, data: Seq<u8>) -> bool {
    &&& counts_ok(cs)
    &&& counts_len(cs) <= usize::MAX
    &&& counts_bytes::<K>(cs) == data
}

/// `data` is a valid bool slab.
pub(crate) open spec fn bool_valid<K: CountOps>(data: Seq<u8>) -> bool {
    exists|cs: Seq<u64>| bool_parses::<K>(cs, data)
}

/// A valid bool slab's counts.
pub(crate) open spec fn bool_counts<K: CountOps>(data: Seq<u8>) -> Seq<u64> {
    choose|cs: Seq<u64>| bool_parses::<K>(cs, data)
}

/// A valid bool slab's values.
pub(crate) open spec fn bool_values<K: CountOps>(data: Seq<u8>) -> Seq<bool> {
    counts_values(bool_counts::<K>(data))
}

/// The byte length of the last count: a bool slab's tail record.
pub(crate) open spec fn counts_tail<K: CountOps>(cs: Seq<u64>) -> nat {
    if cs.len() == 0 { 0 } else { K::enc(cs.last()).len() }
}

// ── Lemmas ──────────────────────────────────────────────────────────────────

pub(crate) proof fn lemma_bytes_push<K: CountOps>(cs: Seq<u64>, x: u64)
    ensures counts_bytes::<K>(cs.push(x)) == counts_bytes::<K>(cs) + K::enc(x),
        counts_len(cs.push(x)) == counts_len(cs) + x,
{
    assert(cs.push(x).drop_last() =~= cs);
}

/// Counts' bytes and lengths, split.
pub(crate) proof fn lemma_concat<K: CountOps>(a: Seq<u64>, b: Seq<u64>)
    ensures
        counts_bytes::<K>(a + b) == counts_bytes::<K>(a) + counts_bytes::<K>(b),
        counts_len(a + b) == counts_len(a) + counts_len(b),
    decreases b.len(),
{
    if b.len() == 0 {
        assert(a + b =~= a);
        assert(counts_bytes::<K>(a) + Seq::<u8>::empty() =~= counts_bytes::<K>(a));
    } else {
        assert((a + b).drop_last() =~= a + b.drop_last());
        lemma_concat::<K>(a, b.drop_last());
        assert(counts_bytes::<K>(a + b) =~= counts_bytes::<K>(a) + counts_bytes::<K>(b));
    }
}

/// Counts' values, split after an even number of runs.
pub(crate) proof fn lemma_values_concat(a: Seq<u64>, b: Seq<u64>)
    requires a.len() % 2 == 0,
    ensures counts_values(a + b) == counts_values(a) + counts_values(b),
    decreases b.len(),
{
    if b.len() == 0 {
        assert(a + b =~= a);
        assert(counts_values(a) + Seq::<bool>::empty() =~= counts_values(a));
    } else {
        assert((a + b).drop_last() =~= a + b.drop_last());
        lemma_values_concat(a, b.drop_last());
        assert(run_value((a + b).len() - 1) == run_value(b.len() - 1));
        assert(counts_values(a + b) =~= counts_values(a) + counts_values(b));
    }
}

pub(crate) proof fn lemma_values_len(cs: Seq<u64>)
    ensures counts_values(cs).len() == counts_len(cs),
    decreases cs.len(),
{
    if cs.len() > 0 {
        lemma_values_len(cs.drop_last());
    }
}

/// Counts' bytes are at least a byte each.
pub(crate) proof fn lemma_bytes_len<K: CountOps>(cs: Seq<u64>)
    ensures counts_bytes::<K>(cs).len() >= cs.len(),
    decreases cs.len(),
{
    if cs.len() > 0 {
        K::axiom_enc(cs.last(), cs.last(), Seq::empty(), Seq::empty());
        lemma_bytes_len::<K>(cs.drop_last());
    }
}

/// Two parses whose bytes agree are the same.
pub(crate) proof fn lemma_unique<K: CountOps>(a: Seq<u64>, b: Seq<u64>)
    requires counts_bytes::<K>(a) == counts_bytes::<K>(b),
    ensures a == b,
    decreases a.len() + b.len(),
{
    if a.len() == 0 || b.len() == 0 {
        if a.len() > 0 {
            K::axiom_enc(a.last(), a.last(), Seq::empty(), Seq::empty());
        }
        if b.len() > 0 {
            K::axiom_enc(b.last(), b.last(), Seq::empty(), Seq::empty());
        }
        assert(a =~= b);
    } else {
        lemma_first::<K>(a);
        lemma_first::<K>(b);
        K::axiom_enc(a[0], b[0], counts_bytes::<K>(a.skip(1)), counts_bytes::<K>(b.skip(1)));
        assert(K::enc(a[0]) == K::enc(b[0]));
        assert(counts_bytes::<K>(a).skip(K::enc(a[0]).len() as int) =~= counts_bytes::<K>(a.skip(1)));
        assert(counts_bytes::<K>(b).skip(K::enc(b[0]).len() as int) =~= counts_bytes::<K>(b.skip(1)));
        lemma_unique::<K>(a.skip(1), b.skip(1));
        assert(a =~= seq![a[0]] + a.skip(1));
        assert(b =~= seq![b[0]] + b.skip(1));
    }
}

/// Counts' bytes, split after the first.
pub(crate) proof fn lemma_first<K: CountOps>(cs: Seq<u64>)
    requires cs.len() > 0,
    ensures counts_bytes::<K>(cs) == K::enc(cs[0]) + counts_bytes::<K>(cs.skip(1)),
{
    assert(cs =~= seq![cs[0]] + cs.skip(1));
    lemma_concat::<K>(seq![cs[0]], cs.skip(1));
    lemma_bytes_push::<K>(Seq::empty(), cs[0]);
    assert(Seq::<u64>::empty().push(cs[0]) =~= seq![cs[0]]);
    assert(counts_bytes::<K>(Seq::<u64>::empty()) =~= Seq::<u8>::empty());
}

/// A valid slab's parse is its counts.
pub(crate) proof fn lemma_counts<K: CountOps>(cs: Seq<u64>, data: Seq<u8>)
    requires bool_parses::<K>(cs, data),
    ensures bool_valid::<K>(data), bool_counts::<K>(data) == cs,
{
    let c2 = bool_counts::<K>(data);
    lemma_unique::<K>(c2, cs);
}

/// Each count is among the values.
pub(crate) proof fn lemma_count_le(cs: Seq<u64>, i: int)
    requires 0 <= i < cs.len(),
    ensures cs[i] <= counts_len(cs),
    decreases cs.len(),
{
    if i < cs.len() - 1 {
        lemma_count_le(cs.drop_last(), i);
    }
}

// ── Validating ──────────────────────────────────────────────────────────────

/// Every parse of `data` begins with the counts `cs`.
pub(crate) open spec fn complete<K: CountOps>(data: Seq<u8>, cs: Seq<u64>) -> bool {
    forall|c2: Seq<u64>| #[trigger] bool_parses::<K>(c2, data) ==> c2.len() >= cs.len() && c2.take(cs.len() as int) == cs
}

/// A parse that begins with `cs` is `cs` then the counts of the bytes
/// after `cs`'s.
proof fn lemma_split<K: CountOps>(c2: Seq<u64>, cs: Seq<u64>)
    requires c2.len() >= cs.len(), c2.take(cs.len() as int) == cs,
    ensures
        counts_bytes::<K>(c2) == counts_bytes::<K>(cs) + counts_bytes::<K>(c2.skip(cs.len() as int)),
        counts_len(c2) == counts_len(cs) + counts_len(c2.skip(cs.len() as int)),
{
    assert(c2 =~= cs + c2.skip(cs.len() as int));
    lemma_concat::<K>(cs, c2.skip(cs.len() as int));
}

/// Reading count `x` from where `cs` ends: every parse continues with it.
proof fn lemma_next<K: CountOps>(data: Seq<u8>, cs: Seq<u64>, pos: int, n: int, x: u64)
    requires
        complete::<K>(data, cs), data.take(pos) == counts_bytes::<K>(cs), 0 <= pos < data.len(),
        0 <= n <= data.len() - pos, data.skip(pos).take(n) == K::enc(x),
    ensures
        complete::<K>(data, cs.push(x)), data.take(pos + n) == counts_bytes::<K>(cs.push(x)),
        n == K::enc(x).len(),
{
    let k = cs.len() as int;
    K::axiom_enc(x, x, Seq::empty(), Seq::empty());
    lemma_bytes_push::<K>(cs, x);
    assert(data.take(pos + n) =~= data.take(pos) + data.skip(pos).take(n));
    assert forall|c2: Seq<u64>| #[trigger] bool_parses::<K>(c2, data) implies
        c2.len() >= k + 1 && c2.take(k + 1) == cs.push(x) by {
        lemma_split::<K>(c2, cs);
        let r = c2.skip(k);
        assert(data.skip(pos) =~= counts_bytes::<K>(r)) by {
            assert(data =~= counts_bytes::<K>(cs) + counts_bytes::<K>(r));
        }
        if r.len() == 0 {
            assert(counts_bytes::<K>(r) =~= Seq::<u8>::empty());
        }
        lemma_first::<K>(r);
        let rest = data.skip(pos + n);
        assert(data.skip(pos) =~= K::enc(x) + rest);
        K::axiom_enc(x, r[0], rest, counts_bytes::<K>(r.skip(1)));
        assert(c2.take(k + 1) =~= cs.push(x));
    }
}

/// Nothing decodes where `cs` ends: no parse.
proof fn lemma_none<K: CountOps>(data: Seq<u8>, cs: Seq<u64>, pos: int)
    requires
        complete::<K>(data, cs), data.take(pos) == counts_bytes::<K>(cs), 0 <= pos < data.len(),
        forall|x: u64| x <= usize::MAX ==> !(#[trigger] K::enc(x)).is_prefix_of(data.skip(pos)),
    ensures !bool_valid::<K>(data),
{
    let k = cs.len() as int;
    if bool_valid::<K>(data) {
        let c2 = choose|c2: Seq<u64>| bool_parses::<K>(c2, data);
        lemma_split::<K>(c2, cs);
        let r = c2.skip(k);
        assert(data =~= counts_bytes::<K>(cs) + counts_bytes::<K>(r));
        assert(data.skip(pos) =~= counts_bytes::<K>(r));
        if r.len() == 0 {
            assert(counts_bytes::<K>(r) =~= Seq::<u8>::empty());
        }
        lemma_first::<K>(r);
        lemma_count_le(c2, k);
        assert(K::enc(r[0]).is_prefix_of(data.skip(pos)));
    }
}

/// Every parse continues with `cs`, and has a property `cs` already
/// rules out: no parse.
proof fn lemma_no_parse_count<K: CountOps>(data: Seq<u8>, cs: Seq<u64>)
    requires
        complete::<K>(data, cs), cs.len() >= 2, cs.last() == 0,
    ensures !bool_valid::<K>(data),
{
    if bool_valid::<K>(data) {
        let c2 = choose|c2: Seq<u64>| bool_parses::<K>(c2, data);
        assert(c2[cs.len() - 1] == c2.take(cs.len() as int)[cs.len() - 1]);
    }
}

proof fn lemma_no_parse_trailing<K: CountOps>(data: Seq<u8>, cs: Seq<u64>)
    requires
        complete::<K>(data, cs), cs.len() >= 1, cs.last() == 0, data == counts_bytes::<K>(cs),
    ensures !bool_valid::<K>(data),
{
    if bool_valid::<K>(data) {
        let c2 = choose|c2: Seq<u64>| bool_parses::<K>(c2, data);
        lemma_split::<K>(c2, cs);
        let r = c2.skip(cs.len() as int);
        lemma_bytes_len::<K>(r);
        assert(counts_bytes::<K>(r).len() == 0);
        assert(c2 =~= cs);
    }
}

proof fn lemma_no_parse_len<K: CountOps>(data: Seq<u8>, cs: Seq<u64>)
    requires complete::<K>(data, cs), counts_len(cs) > usize::MAX,
    ensures !bool_valid::<K>(data),
{
    if bool_valid::<K>(data) {
        let c2 = choose|c2: Seq<u64>| bool_parses::<K>(c2, data);
        lemma_split::<K>(c2, cs);
    }
}

/// A slab's summary: its runs, values, and last count's byte length.
pub struct BoolInfo {
    pub segments: usize,
    pub len: usize,
    pub tail: u8,
}

#[verifier::external_body]
fn bad_format() -> PackError {
    PackError::BadFormat
}

#[verifier::external_body]
fn zero_count(run_index: usize, value: bool) -> PackError {
    PackError::InvalidValue(format!("run {run_index} (value={value}): zero count in non-first run"))
}

#[verifier::external_body]
fn trailing_zero(run_index: usize, value: bool) -> PackError {
    PackError::InvalidValue(format!("run {run_index} (value={value}): trailing zero-count run"))
}

#[verifier::external_body]
fn too_long() -> PackError {
    PackError::InvalidValue("length overflows usize".into())
}

/// Check that `data` is a canonical bool slab of at most `usize::MAX`
/// values, and summarize it. An error means it is not one.
pub(crate) fn validate<K: CountOps>(data: &[u8]) -> (r: Result<BoolInfo, PackError>)
    ensures
        match r {
            Ok(i) => {
                &&& bool_valid::<K>(data@)
                &&& i.segments == bool_counts::<K>(data@).len()
                &&& i.len == counts_len(bool_counts::<K>(data@))
                &&& i.tail == counts_tail::<K>(bool_counts::<K>(data@))
            },
            Err(_) => !bool_valid::<K>(data@),
        },
{
    let ghost mut cs = Seq::<u64>::empty();
    proof {
        assert(data@.take(0) =~= counts_bytes::<K>(cs));
        assert forall|c2: Seq<u64>| #[trigger] bool_parses::<K>(c2, data@) implies
            c2.len() >= 0 && c2.take(0) == cs by {
            assert(c2.take(0) =~= cs);
        }
    }
    if data.len() == 0 {
        proof {
            assert(data@ =~= counts_bytes::<K>(cs));
            assert(bool_parses::<K>(cs, data@));
            lemma_counts::<K>(cs, data@);
        }
        return Ok(BoolInfo { segments: 0, len: 0, tail: 0 });
    }
    let mut byte_pos: usize = 0;
    let mut run_index: usize = 0;
    let mut value = false;
    let mut len: usize = 0;
    let mut last_cb: u8 = 0;
    while byte_pos < data.len()
        invariant
            byte_pos <= data@.len(), data@.len() > 0,
            data@.take(byte_pos as int) == counts_bytes::<K>(cs), complete::<K>(data@, cs),
            run_index == cs.len(), value == run_value(cs.len() as int), len == counts_len(cs),
            forall|i: int| 1 <= i < cs.len() ==> #[trigger] cs[i] > 0,
            cs.len() > 0 && cs.last() == 0 ==> cs.len() == 1 && byte_pos < data@.len(),
            cs.len() > 0 ==> last_cb == K::enc(cs.last()).len(),
            byte_pos == data@.len() ==> cs.len() > 0,
        decreases data@.len() - byte_pos,
    {
        let rest = &data[byte_pos..];
        assert(rest@ == data@.skip(byte_pos as int));
        let (cb, count) = match K::read_count(rest) {
            Some(v) => v,
            None => {
                proof {
                    lemma_none::<K>(data@, cs, byte_pos as int);
                }
                return Err(bad_format());
            },
        };
        proof {
            lemma_next::<K>(data@, cs, byte_pos as int, cb as int, count as u64);
            K::axiom_enc(count as u64, count as u64, Seq::empty(), Seq::empty());
            lemma_bytes_push::<K>(cs, count as u64);
        }
        let ghost c1 = cs.push(count as u64);
        if count == 0 && run_index > 0 {
            proof {
                lemma_no_parse_count::<K>(data@, c1);
            }
            return Err(zero_count(run_index, value));
        }
        let next_pos = byte_pos + cb;
        if next_pos >= data.len() && count == 0 {
            proof {
                assert(data@ =~= data@.take(next_pos as int));
                lemma_no_parse_trailing::<K>(data@, c1);
            }
            return Err(trailing_zero(run_index, value));
        }
        len = match len.checked_add(count) {
            Some(l) => l,
            None => {
                proof {
                    lemma_no_parse_len::<K>(data@, c1);
                }
                return Err(too_long());
            },
        };
        proof {
            lemma_bytes_len::<K>(c1);
        }
        last_cb = cb as u8;
        byte_pos = next_pos;
        value = !value;
        run_index = run_index + 1;
        proof {
            cs = c1;
            assert forall|i: int| 1 <= i < cs.len() implies #[trigger] cs[i] > 0 by {
                if i < cs.len() - 1 {
                    assert(cs[i] == cs.drop_last()[i]);
                }
            }
        }
    }
    proof {
        assert(data@.take(byte_pos as int) =~= data@);
        assert(bool_parses::<K>(cs, data@));
        lemma_counts::<K>(cs, data@);
    }
    Ok(BoolInfo { segments: run_index, len, tail: last_cb })
}

// ── Loading ─────────────────────────────────────────────────────────────────

/// A bool slab.
pub type BSlab = crate::column::Slab<u8>;

/// A bool slab's bytes and recorded metadata.  Opaque, so proofs that
/// only pass slabs along see them as they did when `Slab` was external.
#[verifier::opaque]
pub(crate) open spec fn bslab_bytes(s: BSlab) -> Seq<u8> {
    s.data@
}

#[verifier::opaque]
pub(crate) open spec fn bslab_len(s: BSlab) -> usize {
    s.len
}

#[verifier::opaque]
pub(crate) open spec fn bslab_segments(s: BSlab) -> usize {
    s.segments
}

#[verifier::opaque]
pub(crate) open spec fn bslab_tail(s: BSlab) -> u8 {
    s.tail
}

/// A slab of a copy of `data`.
#[inline(always)]
fn new_bslab(data: &[u8], len: usize, segments: usize, tail: u8) -> (s: BSlab)
    ensures bslab_bytes(s) == data@, bslab_len(s) == len, bslab_segments(s) == segments, bslab_tail(s) == tail,
{
    proof {
        reveal(bslab_bytes);
        reveal(bslab_len);
        reveal(bslab_segments);
        reveal(bslab_tail);
    }
    crate::column::Slab { data: vstd::slice::slice_to_vec(data), len, segments, tail }
}

#[inline(always)]
fn bslab_len_of(s: &BSlab) -> (n: usize)
    ensures n == bslab_len(*s),
{
    proof {
        reveal(bslab_len);
    }
    s.len
}

/// A valid bool slab with its metadata.
pub(crate) open spec fn bslab_ok<K: CountOps>(s: BSlab) -> bool {
    let cs = bool_counts::<K>(bslab_bytes(s));
    &&& bool_valid::<K>(bslab_bytes(s))
    &&& bslab_len(s) == counts_len(cs)
    &&& bslab_segments(s) == cs.len()
    &&& bslab_tail(s) == counts_tail::<K>(cs)
}

/// The values of bool slabs, in order.
pub(crate) open spec fn bslabs_values<K: CountOps>(s: Seq<BSlab>) -> Seq<bool>
    decreases s.len(),
{
    if s.len() == 0 { Seq::empty() } else { bslabs_values::<K>(s.drop_last()) + bool_values::<K>(bslab_bytes(s.last())) }
}

/// Where a load of bool bytes stands: what has been read, and the slab
/// being cut from it. Kept apart from the slabs so a scan can hold it in
/// registers.
#[derive(Clone, Copy)]
pub(crate) struct Scan {
    pos: usize,
    run_index: usize,
    slab_start: usize,
    slab_items: usize,
    slab_segs: usize,
    tail: u8,
    /// items read so far, which bounds every slab's `len`
    len: usize,
    /// the counts read, and the first of the slab being cut
    #[allow(dead_code)]
    cs: Ghost<Seq<u64>>,
    #[allow(dead_code)]
    a: Ghost<int>,
}

/// A scan of `data`, cutting slabs of `target` runs (even, so each starts
/// on `false`) into `slabs`.
closed spec fn scan_ok<K: CountOps>(sc: Scan, data: Seq<u8>, slabs: Seq<BSlab>, target: usize) -> bool {
    let cs = sc.cs@;
    let a = sc.a@;
    &&& sc.pos <= data.len()
    &&& data.take(sc.pos as int) == counts_bytes::<K>(cs) && complete::<K>(data, cs)
    &&& sc.run_index == cs.len() && sc.len == counts_len(cs)
    &&& forall|i: int| 1 <= i < cs.len() ==> #[trigger] cs[i] > 0
    &&& cs.len() > 0 && cs.last() == 0 ==> cs.len() == 1 && sc.pos < data.len()
    &&& 0 <= a <= cs.len() && a % 2 == 0
    &&& sc.slab_start == counts_bytes::<K>(cs.take(a)).len()
    &&& sc.slab_items == counts_len(cs.skip(a)) && sc.slab_segs == cs.len() - a
    &&& sc.slab_segs > 0 ==> sc.tail == K::enc(cs.last()).len()
    &&& 2 <= target && target % 2 == 0 && sc.slab_segs < target
    &&& forall|i: int| 0 <= i < slabs.len() ==> bslab_ok::<K>(#[trigger] slabs[i])
    &&& bslabs_values::<K>(slabs) == counts_values(cs.take(a))
}

/// Cut the slab `sc` is reading, at its position: `scan_ok` again (if
/// the counts read are even), and the slabs hold every value read.
#[inline(always)]
fn cut_slab<K: CountOps>(sc: &mut Scan, data: &[u8], slabs: &mut Vec<BSlab>)
    requires
        0 <= old(sc).a@ < old(sc).cs@.len(), old(sc).a@ % 2 == 0,
        old(sc).pos <= data@.len(),
        data@.take(old(sc).pos as int) == counts_bytes::<K>(old(sc).cs@),
        forall|i: int| 1 <= i < old(sc).cs@.len() ==> #[trigger] old(sc).cs@[i] > 0,
        old(sc).cs@.len() - old(sc).a@ >= 2 || old(sc).cs@.last() > 0,
        counts_len(old(sc).cs@) <= usize::MAX,
        old(sc).slab_start == counts_bytes::<K>(old(sc).cs@.take(old(sc).a@)).len(),
        old(sc).slab_items == counts_len(old(sc).cs@.skip(old(sc).a@)),
        old(sc).slab_segs == old(sc).cs@.len() - old(sc).a@,
        old(sc).tail == K::enc(old(sc).cs@.last()).len(),
        forall|i: int| 0 <= i < old(slabs)@.len() ==> bslab_ok::<K>(#[trigger] old(slabs)@[i]),
        bslabs_values::<K>(old(slabs)@) == counts_values(old(sc).cs@.take(old(sc).a@)),
    ensures
        final(sc).cs@ == old(sc).cs@, final(sc).a@ == old(sc).cs@.len(),
        final(sc).pos == old(sc).pos, final(sc).run_index == old(sc).run_index, final(sc).len == old(sc).len,
        final(sc).tail == old(sc).tail,
        final(sc).slab_start == old(sc).pos, final(sc).slab_items == 0, final(sc).slab_segs == 0,
        final(sc).slab_start == counts_bytes::<K>(final(sc).cs@.take(final(sc).a@)).len(),
        final(sc).slab_items == counts_len(final(sc).cs@.skip(final(sc).a@)),
        forall|i: int| 0 <= i < final(slabs)@.len() ==> bslab_ok::<K>(#[trigger] final(slabs)@[i]),
        bslabs_values::<K>(final(slabs)@) == counts_values(old(sc).cs@),
        final(slabs)@.len() == old(slabs)@.len() + 1,
{
    let ghost cs = sc.cs@;
    let ghost a = sc.a@;
    let ghost s = cs.subrange(a, cs.len() as int);
    let ghost s0 = slabs@;
    proof {
        lemma_cut::<K>(data@, cs, a, sc.pos as int);
    }
    let slab = new_bslab(&data[sc.slab_start..sc.pos], sc.slab_items, sc.slab_segs, sc.tail);
    slabs.push(slab);
    sc.slab_start = sc.pos;
    sc.slab_items = 0;
    sc.slab_segs = 0;
    proof {
        sc.a = Ghost(cs.len() as int);
        assert(slabs@.drop_last() =~= s0);
        assert(bslab_bytes(slab) == counts_bytes::<K>(s));
        lemma_counts::<K>(s, bslab_bytes(slab));
        assert forall|i: int| 0 <= i < slabs@.len() implies bslab_ok::<K>(#[trigger] slabs@[i]) by {
            if i < s0.len() {
                assert(slabs@[i] == s0[i]);
            }
        }
        lemma_values_concat(cs.take(a), s);
        assert(cs.take(a) + s =~= cs.take(cs.len() as int));
        assert(cs.skip(cs.len() as int) =~= Seq::<u64>::empty());
        assert(cs.take(cs.len() as int) =~= cs);
    }
}

/// Read one run (and cut the slab if it reaches its target): its count
/// and value, or `None` at the end of the input.
#[inline(always)]
#[verifier::spinoff_prover]
fn read_seg<K: CountOps>(sc: &mut Scan, data: &[u8], slabs: &mut Vec<BSlab>, target: usize) -> (r: Result<Option<(usize, bool)>, PackError>)
    requires scan_ok::<K>(*old(sc), data@, old(slabs)@, target),
    ensures
        r is Ok ==> scan_ok::<K>(*final(sc), data@, final(slabs)@, target),
        r is Ok ==> final(sc).pos >= old(sc).pos,
        r matches Ok(Some(_)) ==> final(sc).pos > old(sc).pos,
        r matches Ok(None) ==> final(sc).pos == data@.len() && *final(sc) == *old(sc) && final(slabs)@ == old(slabs)@,
        r matches Ok(Some((c, v))) ==> final(sc).cs@ == old(sc).cs@.push(c as u64)
            && v == run_value(old(sc).cs@.len() as int),
        r is Err ==> !bool_valid::<K>(data@),
{
    if sc.pos >= data.len() {
        return Ok(None);
    }
    let ghost cs = sc.cs@;
    let rest = &data[sc.pos..];
    assert(rest@ == data@.skip(sc.pos as int));
    let (cb, count) = match K::read_count(rest) {
        Some(v) => v,
        None => {
            proof {
                lemma_none::<K>(data@, cs, sc.pos as int);
            }
            return Err(bad_format());
        },
    };
    proof {
        lemma_next::<K>(data@, cs, sc.pos as int, cb as int, count as u64);
        K::axiom_enc(count as u64, count as u64, Seq::empty(), Seq::empty());
        lemma_bytes_push::<K>(cs, count as u64);
    }
    let ghost c1 = cs.push(count as u64);
    // only the very first run may have count 0 (structural padding)
    if count == 0 && sc.run_index > 0 {
        proof {
            lemma_no_parse_count::<K>(data@, c1);
        }
        return Err(bad_format());
    }
    let next_pos = sc.pos + cb;
    // a trailing zero-count run is invalid
    if next_pos >= data.len() && count == 0 {
        proof {
            assert(data@ =~= data@.take(next_pos as int));
            lemma_no_parse_trailing::<K>(data@, c1);
        }
        return Err(bad_format());
    }
    // untrusted count: the column's length must fit a usize
    let len = match sc.len.checked_add(count) {
        Some(l) => l,
        None => {
            proof {
                lemma_no_parse_len::<K>(data@, c1);
            }
            return Err(bad_format());
        },
    };
    let value = sc.run_index % 2 == 1;
    proof {
        lemma_bytes_len::<K>(c1);
        let a = sc.a@;
        assert(c1.skip(a) =~= cs.skip(a).push(count as u64));
        lemma_bytes_push::<K>(cs.skip(a), count as u64);
        assert(c1.take(a) =~= cs.take(a));
        assert(c1 =~= c1.take(a) + c1.skip(a));
        lemma_concat::<K>(c1.take(a), c1.skip(a));
    }
    sc.tail = cb as u8;
    sc.pos = next_pos;
    sc.run_index = sc.run_index + 1;
    sc.len = len;
    sc.slab_items = sc.slab_items + count;
    sc.slab_segs = sc.slab_segs + 1;
    proof {
        sc.cs = Ghost(c1);
        assert forall|i: int| 1 <= i < c1.len() implies #[trigger] c1[i] > 0 by {
            if i < c1.len() - 1 {
                assert(c1[i] == cs[i]);
            }
        }
    }
    // cut after target runs — always even, so the next slab starts on a
    // false run and is a copy of the input's bytes
    if sc.slab_segs >= target {
        cut_slab::<K>(sc, data, slabs);
        proof {
            assert(c1.take(c1.len() as int) =~= c1);
            assert(sc.a@ % 2 == 0);
        }
    }
    Ok(Some((count, value)))
}

/// A load of bool bytes in progress.
pub(crate) struct BoolLoad<'a> {
    data: &'a [u8],
    slabs: Vec<BSlab>,
    target: usize,
    sc: Scan,
}

impl<'a> BoolLoad<'a> {
    pub(crate) closed spec fn inv<K: CountOps>(&self) -> bool {
        scan_ok::<K>(self.sc, self.data@, self.slabs@, self.target)
    }

    pub(crate) closed spec fn data(&self) -> Seq<u8> {
        self.data@
    }

    pub(crate) closed spec fn done_slabs(&self) -> Seq<BSlab> {
        self.slabs@
    }

    /// The values read so far.
    pub(crate) closed spec fn values_read(&self) -> Seq<bool> {
        counts_values(self.sc.cs@)
    }

    /// Start loading `data`, into slabs of about `max_segments / 2` runs.
    pub(crate) fn new<K: CountOps>(data: &'a [u8], max_segments: usize) -> (l: Self)
        ensures l.inv::<K>(), l.data() == data@,
    {
        let half = max_segments / 2;
        let even = half - half % 2;
        let target = if even < 2 { 2 } else { even };
        let l = BoolLoad {
            data,
            slabs: Vec::new(),
            target,
            sc: Scan {
                pos: 0,
                run_index: 0,
                slab_start: 0,
                slab_items: 0,
                slab_segs: 0,
                tail: 0,
                len: 0,
                cs: Ghost(Seq::empty()),
                a: Ghost(0),
            },
        };
        proof {
            let e = Seq::<u64>::empty();
            assert(data@.take(0) =~= counts_bytes::<K>(e));
            assert forall|c2: Seq<u64>| #[trigger] bool_parses::<K>(c2, data@) implies
                c2.len() >= 0 && c2.take(0) == e by {
                assert(c2.take(0) =~= e);
            }
            assert(e.take(0) =~= e);
            assert(e.skip(0) =~= e);
            assert(bslabs_values::<K>(l.slabs@) =~= counts_values(e));
        }
        l
    }

    pub(crate) fn slabs_completed(&self) -> (n: usize)
        ensures n == self.done_slabs().len(),
    {
        self.slabs.len()
    }

    pub(crate) fn completed_slab_len(&self, i: usize) -> (n: usize)
        requires i < self.done_slabs().len(),
        ensures n == bslab_len(self.done_slabs()[i as int]),
    {
        bslab_len_of(&self.slabs[i])
    }

    /// The next run, or `None` at the end of the input.
    ///
    /// # Errors
    ///
    /// If the input is not a canonical bool encoding.
    #[inline]
    pub(crate) fn try_next_run<K: CountOps>(&mut self) -> (r: Result<Option<Run<bool>>, PackError>)
        requires old(self).inv::<K>(),
        ensures
            final(self).data() == old(self).data(),
            r is Ok ==> final(self).inv::<K>(),
            r matches Ok(Some(run)) ==> run.count > 0
                && final(self).values_read() == old(self).values_read() + Seq::new(run.count as nat, |_j: int| run.value),
            r matches Ok(None) ==> final(self).values_read() == old(self).values_read(),
            r is Err ==> !bool_valid::<K>(old(self).data()),
    {
        loop
            invariant
                self.inv::<K>(), self.data() == old(self).data(),
                self.values_read() == old(self).values_read(),
            decreases self.data@.len() - self.sc.pos,
        {
            let ghost c0 = self.sc.cs@;
            match read_seg::<K>(&mut self.sc, self.data, &mut self.slabs, self.target) {
                Ok(None) => return Ok(None),
                Ok(Some((count, value))) => {
                    proof {
                        assert(self.sc.cs@.drop_last() =~= c0);
                    }
                    if count > 0 {
                        return Ok(Some(Run { count, value }));
                    }
                    proof {
                        assert(counts_values(self.sc.cs@) =~= counts_values(c0));
                    }
                },
                Err(e) => return Err(e),
            }
        }
    }

    /// Read and check the rest of the input, and cut the last slab.
    ///
    /// # Errors
    ///
    /// If the input is not a canonical bool encoding.
    pub(crate) fn finalize<K: CountOps>(&mut self) -> (r: Result<Vec<BSlab>, PackError>)
        requires old(self).inv::<K>(),
        ensures
            r matches Ok(slabs) ==> {
                &&& bool_valid::<K>(old(self).data())
                &&& forall|i: int| 0 <= i < slabs@.len() ==> bslab_ok::<K>(#[trigger] slabs@[i])
                &&& bslabs_values::<K>(slabs@) == bool_values::<K>(old(self).data())
            },
            r is Err ==> !bool_valid::<K>(old(self).data()),
    {
        // the scan in locals, so the drain loop keeps it in registers
        let data = self.data;
        let target = self.target;
        let mut sc = self.sc;
        let mut slabs = Vec::new();
        core::mem::swap(&mut self.slabs, &mut slabs);
        loop
            invariant scan_ok::<K>(sc, data@, slabs@, target), data@ == old(self).data@,
            ensures scan_ok::<K>(sc, data@, slabs@, target), data@ == old(self).data@, sc.pos == data@.len(),
            decreases data@.len() - sc.pos,
        {
            match read_seg::<K>(&mut sc, data, &mut slabs, target) {
                Ok(None) => break,
                Ok(Some(_)) => {},
                Err(e) => return Err(e),
            }
        }
        let ghost cs = sc.cs@;
        proof {
            assert(data@.take(sc.pos as int) =~= data@);
            assert(bool_parses::<K>(cs, data@));
            lemma_counts::<K>(cs, data@);
        }
        if sc.slab_segs > 0 {
            cut_slab::<K>(&mut sc, data, &mut slabs);
        } else {
            proof {
                assert(cs.take(sc.a@) =~= cs);
            }
        }
        Ok(slabs)
    }
}

/// The counts from `a` on, as a slab: canonical, and its bytes the input's
/// from where they start.
proof fn lemma_cut<K: CountOps>(data: Seq<u8>, cs: Seq<u64>, a: int, pos: int)
    requires
        0 <= a < cs.len(), 0 <= pos <= data.len(),
        data.take(pos) == counts_bytes::<K>(cs),
        forall|i: int| 1 <= i < cs.len() ==> #[trigger] cs[i] > 0,
        cs.len() - a >= 2 || cs.last() > 0,
        counts_len(cs) <= usize::MAX,
    ensures ({
        let sc = cs.subrange(a, cs.len() as int);
        &&& bool_parses::<K>(sc, data.subrange(counts_bytes::<K>(cs.take(a)).len() as int, pos))
        &&& counts_len(sc) == counts_len(cs.skip(a))
        &&& counts_tail::<K>(sc) == K::enc(cs.last()).len()
        &&& counts_bytes::<K>(cs.take(a)).len() <= pos
    }),
{
    let sc = cs.subrange(a, cs.len() as int);
    assert(sc =~= cs.skip(a));
    assert(cs =~= cs.take(a) + cs.skip(a));
    lemma_concat::<K>(cs.take(a), cs.skip(a));
    let b1 = counts_bytes::<K>(cs.take(a));
    let b2 = counts_bytes::<K>(cs.skip(a));
    assert(data.take(pos) == b1 + b2);
    assert(data.subrange(b1.len() as int, pos) =~= data.take(pos).skip(b1.len() as int));
    assert((b1 + b2).skip(b1.len() as int) =~= b2);
    assert forall|i: int| 1 <= i < sc.len() implies #[trigger] sc[i] > 0 by {
        assert(sc[i] == cs[a + i]);
    }
    assert(sc.last() == cs.last());
    if sc.len() >= 2 {
        assert(cs[cs.len() - 1] > 0);
    }
}

// ── Decoding ────────────────────────────────────────────────────────────────

/// Counts' values start with the values of any prefix of them.
pub(crate) proof fn lemma_values_prefix(a: Seq<u64>, b: Seq<u64>)
    ensures
        counts_values(a + b).len() >= counts_values(a).len(),
        counts_values(a + b).take(counts_values(a).len() as int) == counts_values(a),
    decreases b.len(),
{
    if b.len() == 0 {
        assert(a + b =~= a);
        assert(counts_values(a).take(counts_values(a).len() as int) =~= counts_values(a));
    } else {
        assert((a + b).drop_last() =~= a + b.drop_last());
        lemma_values_prefix(a, b.drop_last());
        let v = counts_values(a + b.drop_last());
        let w = counts_values(a);
        assert(counts_values(a + b).take(w.len() as int) =~= v.take(w.len() as int));
    }
}

/// Run `k`'s values in a slab's: all its value.
pub(crate) proof fn lemma_run_values(cs: Seq<u64>, k: int, j: int)
    requires 0 <= k < cs.len(), 0 <= j < cs[k],
    ensures
        counts_len(cs.take(k)) + j < counts_values(cs).len(),
        counts_values(cs)[counts_len(cs.take(k)) + j] == run_value(k),
{
    let t = cs.take(k + 1);
    assert(t.drop_last() =~= cs.take(k));
    assert(cs =~= t + cs.skip(k + 1));
    lemma_values_prefix(t, cs.skip(k + 1));
    lemma_values_len(cs.take(k));
    lemma_values_len(t);
    assert(counts_values(t)[counts_len(cs.take(k)) + j] == run_value(k));
}

/// A decoder's position: `byte_pos` past the counts it has started
/// (`k` of them), `remaining` of the last one's values still to give,
/// and that run's value (`true` before the first).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BoolDecoderState {
    pub(crate) byte_pos: usize,
    pub(crate) remaining: usize,
    /// starts `true` so the first run's flip yields `false`, as the wire
    /// format requires
    pub(crate) value: bool,
}

impl BoolDecoderState {
    /// The state of a decoder standing at the start of a slab.
    pub(crate) fn start() -> (r: Self)
        ensures r.byte_pos == 0 && r.remaining == 0 && r.value,
    {
        BoolDecoderState { byte_pos: 0, remaining: 0, value: true }
    }
}

/// The state stands `k` runs into counts `cs`.
pub(crate) open spec fn dstate_at<K: CountOps>(cs: Seq<u64>, st: BoolDecoderState, k: int) -> bool {
    &&& 0 <= k <= cs.len()
    &&& st.byte_pos == counts_bytes::<K>(cs.take(k)).len()
    &&& if k == 0 { st.value && st.remaining == 0 } else { st.value == run_value(k - 1) && st.remaining <= cs[k - 1] }
}

/// The state is a position in the valid slab `data`.
pub(crate) open spec fn bstate_ok<K: CountOps>(data: Seq<u8>, st: BoolDecoderState) -> bool {
    &&& bool_valid::<K>(data) && data.len() <= usize::MAX
    &&& exists|k: int| dstate_at::<K>(bool_counts::<K>(data), st, k)
}

/// The runs a state has started (one only, as counts' bytes grow).
pub(crate) open spec fn bstate_k<K: CountOps>(data: Seq<u8>, st: BoolDecoderState) -> int {
    choose|k: int| dstate_at::<K>(bool_counts::<K>(data), st, k)
}

/// The value index a state stands at.
pub(crate) open spec fn bstate_pos<K: CountOps>(data: Seq<u8>, st: BoolDecoderState) -> int {
    counts_len(bool_counts::<K>(data).take(bstate_k::<K>(data, st))) - st.remaining
}

/// Counts' prefixes have more bytes the longer they are.
pub(crate) proof fn lemma_take_bytes_mono<K: CountOps>(cs: Seq<u64>, i: int, j: int)
    requires 0 <= i < j <= cs.len(),
    ensures counts_bytes::<K>(cs.take(i)).len() < counts_bytes::<K>(cs.take(j)).len(),
{
    assert(cs.take(j) =~= cs.take(i) + cs.take(j).skip(i));
    lemma_concat::<K>(cs.take(i), cs.take(j).skip(i));
    lemma_bytes_len::<K>(cs.take(j).skip(i));
}

/// Where run `k` starts in a slab's bytes, its count's encoding.
pub(crate) proof fn lemma_next_count<K: CountOps>(data: Seq<u8>, cs: Seq<u64>, k: int, bp: int)
    requires
        bool_parses::<K>(cs, data), 0 <= k <= cs.len(), bp == counts_bytes::<K>(cs.take(k)).len(), bp < data.len(),
    ensures
        k < cs.len(), cs[k] <= usize::MAX,
        data.skip(bp) == K::enc(cs[k]) + counts_bytes::<K>(cs.skip(k + 1)),
        counts_bytes::<K>(cs.take(k + 1)) == counts_bytes::<K>(cs.take(k)) + K::enc(cs[k]),
        cs.take(k + 1).drop_last() == cs.take(k),
        K::enc(cs[k]).is_prefix_of(data.skip(bp)),
{
    if k == cs.len() {
        assert(cs.take(k) =~= cs);
    }
    assert(cs =~= cs.take(k) + cs.skip(k));
    lemma_concat::<K>(cs.take(k), cs.skip(k));
    lemma_first::<K>(cs.skip(k));
    assert(cs.skip(k).skip(1) =~= cs.skip(k + 1));
    assert(cs.skip(k)[0] == cs[k]);
    assert(data.skip(bp) =~= counts_bytes::<K>(cs.skip(k)));
    lemma_count_le(cs, k);
    lemma_bytes_push::<K>(cs.take(k), cs[k]);
    assert(cs.take(k + 1) =~= cs.take(k).push(cs[k]));
    assert(cs.take(k + 1).drop_last() =~= cs.take(k));
    assert((K::enc(cs[k]) + counts_bytes::<K>(cs.skip(k + 1))).take(K::enc(cs[k]).len() as int) =~= K::enc(cs[k]));
}

/// Counts' prefixes have no more bytes than they.
pub(crate) proof fn lemma_take_bytes_le<K: CountOps>(cs: Seq<u64>, i: int)
    requires 0 <= i <= cs.len(),
    ensures counts_bytes::<K>(cs.take(i)).len() <= counts_bytes::<K>(cs).len(),
{
    if i < cs.len() {
        lemma_take_bytes_mono::<K>(cs, i, cs.len() as int);
    }
    assert(cs.take(cs.len() as int) =~= cs);
}

/// Two runs-started counts for one state are the same.
pub(crate) proof fn lemma_k_unique<K: CountOps>(cs: Seq<u64>, st: BoolDecoderState, k1: int, k2: int)
    requires dstate_at::<K>(cs, st, k1), dstate_at::<K>(cs, st, k2),
    ensures k1 == k2,
{
    if k1 < k2 {
        lemma_take_bytes_mono::<K>(cs, k1, k2);
    } else if k2 < k1 {
        lemma_take_bytes_mono::<K>(cs, k2, k1);
    }
}

/// A decoder over a valid bool slab's values.
#[derive(Clone, Copy)]
pub(crate) struct BDec<'a> {
    data: &'a [u8],
    st: BoolDecoderState,
}

impl<'a> BDec<'a> {
    pub(crate) closed spec fn data(&self) -> Seq<u8> {
        self.data@
    }

    pub(crate) closed spec fn state(&self) -> BoolDecoderState {
        self.st
    }

    pub(crate) open spec fn inv<K: CountOps>(&self) -> bool {
        bstate_ok::<K>(self.data(), self.state())
    }

    /// The index of the next value.
    pub(crate) open spec fn pos<K: CountOps>(&self) -> int {
        bstate_pos::<K>(self.data(), self.state())
    }

    /// The runs started.
    pub(crate) open spec fn k<K: CountOps>(&self) -> int {
        bstate_k::<K>(self.data(), self.state())
    }

    /// A decoder at the start of the valid slab `data`.
    pub(crate) fn new<K: CountOps>(data: &'a [u8]) -> (d: Self)
        requires bool_valid::<K>(data@),
        ensures d.inv::<K>(), d.data() == data@, d.pos::<K>() == 0,
    {
        let d = BDec { data, st: BoolDecoderState::start() };
        let _ = data.len();
        proof {
            let cs = bool_counts::<K>(data@);
            assert(cs.take(0) =~= Seq::<u64>::empty());
            assert(dstate_at::<K>(cs, d.st, 0));
            lemma_k_unique::<K>(cs, d.st, bstate_k::<K>(data@, d.st), 0);
        }
        d
    }

    /// Resume a suspended position in `data`.
    #[inline(always)]
    pub(crate) fn resume<K: CountOps>(data: &'a [u8], st: &BoolDecoderState) -> (d: Self)
        requires bstate_ok::<K>(data@, *st),
        ensures d.inv::<K>(), d.data() == data@, d.state() == *st,
    {
        BDec { data, st: *st }
    }

    /// The position, without the slab.
    #[inline(always)]
    pub(crate) fn suspend(&self) -> (st: BoolDecoderState)
        ensures st == self.state(),
    {
        self.st
    }

    /// The decoder's state, as fields.
    #[inline(always)]
    pub(crate) fn remaining(&self) -> (n: usize)
        ensures n == self.state().remaining,
    {
        self.st.remaining
    }

    /// Start the next run (`remaining` is spent).
    #[inline(always)]
    #[verifier::spinoff_prover]
    fn advance_run<K: CountOps>(&mut self)
        requires old(self).inv::<K>(), old(self).state().remaining == 0,
            old(self).state().byte_pos < old(self).data().len(),
        ensures
            final(self).inv::<K>(), final(self).data() == old(self).data(),
            final(self).pos::<K>() == old(self).pos::<K>(),
            final(self).state().byte_pos > old(self).state().byte_pos,
            final(self).state().byte_pos <= final(self).data().len(),
            final(self).k::<K>() == old(self).k::<K>() + 1,
            old(self).k::<K>() < bool_counts::<K>(old(self).data()).len(),
            final(self).state().remaining == bool_counts::<K>(old(self).data())[old(self).k::<K>()],
    {
        let ghost data = self.data@;
        let ghost cs = bool_counts::<K>(data);
        let ghost k = bstate_k::<K>(data, self.st);
        proof {
            lemma_next_count::<K>(data, cs, k, self.st.byte_pos as int);
        }
        let rest = &self.data[self.st.byte_pos..];
        assert(rest@ == data.skip(self.st.byte_pos as int));
        if let Some((cb, count)) = K::read_count(rest) {
            proof {
                let after = counts_bytes::<K>(cs.skip(k + 1));
                assert(rest@ =~= rest@.take(cb as int) + rest@.skip(cb as int));
                K::axiom_enc(count as u64, cs[k], rest@.skip(cb as int), after);
            }
            self.st.byte_pos = self.st.byte_pos + cb;
            self.st.value = !self.st.value;
            self.st.remaining = count;
            proof {
                assert(dstate_at::<K>(cs, self.st, k + 1));
                lemma_k_unique::<K>(cs, self.st, bstate_k::<K>(data, self.st), k + 1);
                lemma_take_bytes_le::<K>(cs, k + 1);
            }
        } else {
            // unreachable on a valid slab, but stating the state on this
            // path keeps the decode loop as tight as the original's (a
            // third slower without it)
            self.st.remaining = 0;
        }
    }

    /// The value at the end, a run at a time: at the end exactly when
    /// the bytes are spent.
    proof fn lemma_end<K: CountOps>(&self)
        requires self.inv::<K>(), self.state().remaining == 0,
        ensures (self.state().byte_pos >= self.data().len()) == (self.pos::<K>() == bool_values::<K>(self.data()).len()),
            self.pos::<K>() <= bool_values::<K>(self.data()).len(),
            self.state().byte_pos <= self.data().len(),
    {
        let data = self.data@;
        let cs = bool_counts::<K>(data);
        let k = bstate_k::<K>(data, self.st);
        lemma_counts::<K>(cs, data);
        lemma_values_len(cs);
        assert(cs =~= cs.take(k) + cs.skip(k));
        lemma_concat::<K>(cs.take(k), cs.skip(k));
        if k == cs.len() {
            assert(cs.take(k) =~= cs);
            assert(cs.skip(k) =~= Seq::<u64>::empty());
        } else {
            lemma_bytes_len::<K>(cs.skip(k));
            // a later run has values: only the first may be empty, and it
            // is not the last
            if k == 0 && cs[0] == 0 {
                assert(cs.len() >= 2);
                assert(cs[1] > 0);
                lemma_count_le(cs.skip(k), 1);
            } else {
                lemma_count_le(cs.skip(k), 0);
            }
        }
    }

    /// The next value, or `None` at the end.
    #[inline]
    pub(crate) fn next<K: CountOps>(&mut self) -> (r: Option<bool>)
        requires old(self).inv::<K>(),
        ensures
            final(self).inv::<K>(), final(self).data() == old(self).data(),
            match r {
                Some(v) => old(self).pos::<K>() < bool_values::<K>(old(self).data()).len()
                    && v == bool_values::<K>(old(self).data())[old(self).pos::<K>()]
                    && final(self).pos::<K>() == old(self).pos::<K>() + 1,
                None => old(self).pos::<K>() == bool_values::<K>(old(self).data()).len()
                    && final(self).pos::<K>() == old(self).pos::<K>(),
            },
    {
        proof {
            self.lemma_bytes_le::<K>();
        }
        loop
            invariant
                self.inv::<K>(), self.data() == old(self).data(), self.pos::<K>() == old(self).pos::<K>(),
                self.st.byte_pos <= self.data@.len(),
            decreases self.data@.len() - self.st.byte_pos,
        {
            if self.st.remaining > 0 {
                proof {
                    self.lemma_value::<K>();
                }
                let ghost st0 = self.st;
                self.st.remaining = self.st.remaining - 1;
                proof {
                    self.lemma_same_k::<K>(st0);
                }
                return Some(self.st.value);
            }
            proof {
                self.lemma_end::<K>();
            }
            if self.st.byte_pos >= self.data.len() {
                return None;
            }
            self.advance_run::<K>();
        }
    }

    /// Skip `n` values, then the next value, or `None` past the end.
    pub(crate) fn nth<K: CountOps>(&mut self, n0: usize) -> (r: Option<bool>)
        requires old(self).inv::<K>(),
        ensures
            final(self).inv::<K>(), final(self).data() == old(self).data(),
            match r {
                Some(v) => old(self).pos::<K>() + n0 < bool_values::<K>(old(self).data()).len()
                    && v == bool_values::<K>(old(self).data())[old(self).pos::<K>() + n0]
                    && final(self).pos::<K>() == old(self).pos::<K>() + n0 + 1,
                None => old(self).pos::<K>() + n0 >= bool_values::<K>(old(self).data()).len()
                    && final(self).pos::<K>() == bool_values::<K>(old(self).data()).len(),
            },
    {
        let mut n = n0;
        proof {
            self.lemma_bytes_le::<K>();
        }
        loop
            invariant
                self.inv::<K>(), self.data() == old(self).data(),
                self.pos::<K>() + n == old(self).pos::<K>() + n0,
                self.st.byte_pos <= self.data@.len(),
            decreases self.data@.len() - self.st.byte_pos, self.st.remaining,
        {
            if self.st.remaining > 0 {
                let ghost st0 = self.st;
                if n < self.st.remaining {
                    self.st.remaining = self.st.remaining - n;
                    proof {
                        self.lemma_same_k::<K>(st0);
                    }
                    return self.next::<K>();
                }
                n = n - self.st.remaining;
                self.st.remaining = 0;
                proof {
                    self.lemma_same_k::<K>(st0);
                }
            }
            proof {
                self.lemma_end::<K>();
            }
            if self.st.byte_pos >= self.data.len() {
                return None;
            }
            self.advance_run::<K>();
        }
    }

    /// The next run, of at most `max` values, or `None` at the end.
    #[inline]
    pub(crate) fn next_run_max<K: CountOps>(&mut self, max: usize) -> (r: Option<crate::Run<bool>>)
        requires old(self).inv::<K>(), max >= 1,
        ensures
            final(self).inv::<K>(), final(self).data() == old(self).data(),
            match r {
                Some(run) => 1 <= run.count <= max
                    && old(self).pos::<K>() + run.count <= bool_values::<K>(old(self).data()).len()
                    && bool_values::<K>(old(self).data()).subrange(old(self).pos::<K>(), old(self).pos::<K>() + run.count)
                        == Seq::new(run.count as nat, |_j: int| run.value)
                    && final(self).pos::<K>() == old(self).pos::<K>() + run.count
                    && (old(self).state().remaining > 0 ==> final(self).k::<K>() == old(self).k::<K>()
                        && final(self).state().remaining == old(self).state().remaining - run.count)
                    && (old(self).state().remaining == 0 && old(self).k::<K>() >= 1 ==>
                        final(self).k::<K>() == old(self).k::<K>() + 1
                        && final(self).state().remaining == bool_counts::<K>(old(self).data())[old(self).k::<K>()] - run.count),
                None => old(self).pos::<K>() == bool_values::<K>(old(self).data()).len()
                    && final(self).pos::<K>() == old(self).pos::<K>(),
            },
    {
        proof {
            self.lemma_bytes_le::<K>();
        }
        loop
            invariant
                self.inv::<K>(), self.data() == old(self).data(), self.pos::<K>() == old(self).pos::<K>(),
                self.st.byte_pos <= self.data@.len(), max >= 1,
                self.k::<K>() == old(self).k::<K>() && self.st.remaining == old(self).st.remaining
                    || old(self).st.remaining == 0 && self.k::<K>() == old(self).k::<K>() + 1 && self.k::<K>() >= 2
                        && self.st.remaining == bool_counts::<K>(old(self).data())[old(self).k::<K>()]
                    || old(self).st.remaining == 0 && old(self).k::<K>() == 0 && self.k::<K>() >= 1,
            decreases self.data@.len() - self.st.byte_pos,
        {
            if self.st.remaining > 0 {
                proof {
                    self.lemma_value::<K>();
                }
                let count = if self.st.remaining < max { self.st.remaining } else { max };
                let value = self.st.value;
                let ghost st0 = self.st;
                let ghost p0 = self.pos::<K>();
                let ghost vs = bool_values::<K>(self.data());
                self.st.remaining = self.st.remaining - count;
                proof {
                    self.lemma_same_k::<K>(st0);
                    assert forall|j: int| 0 <= j < count implies
                        #[trigger] vs.subrange(p0, p0 + count)[j] == value by {
                        assert(vs.subrange(p0, p0 + count)[j] == vs[p0 + j]);
                    }
                    assert(vs.subrange(p0, p0 + count) =~= Seq::new(count as nat, |_j: int| value));
                }
                return Some(crate::Run { count, value });
            }
            proof {
                self.lemma_end::<K>();
            }
            if self.st.byte_pos >= self.data.len() {
                return None;
            }
            self.advance_run::<K>();
        }
    }

    /// The decoder's bytes are among the slab's.
    proof fn lemma_bytes_le<K: CountOps>(&self)
        requires self.inv::<K>(),
        ensures self.state().byte_pos <= self.data().len(),
    {
        let data = self.data@;
        let cs = bool_counts::<K>(data);
        lemma_counts::<K>(cs, data);
        lemma_take_bytes_le::<K>(cs, bstate_k::<K>(data, self.st));
    }

    /// The value the decoder stands at, while a run is in hand.
    proof fn lemma_value<K: CountOps>(&self)
        requires self.inv::<K>(), self.state().remaining > 0,
        ensures
            self.pos::<K>() < bool_values::<K>(self.data()).len(),
            self.pos::<K>() + self.state().remaining <= bool_values::<K>(self.data()).len(),
            bool_values::<K>(self.data())[self.pos::<K>()] == self.state().value,
            forall|j: int| 0 <= j < self.state().remaining ==>
                #[trigger] bool_values::<K>(self.data())[self.pos::<K>() + j] == self.state().value,
    {
        let data = self.data@;
        let cs = bool_counts::<K>(data);
        let k = bstate_k::<K>(data, self.st);
        assert(cs.take(k).drop_last() =~= cs.take(k - 1));
        assert forall|j: int| 0 <= j < self.state().remaining implies
            #[trigger] bool_values::<K>(self.data())[self.pos::<K>() + j] == self.state().value by {
            lemma_run_values(cs, k - 1, cs[k - 1] - self.st.remaining + j);
        }
        lemma_run_values(cs, k - 1, cs[k - 1] - self.st.remaining);
        lemma_run_values(cs, k - 1, cs[k - 1] - 1);
    }

    /// Spending `n` of the run in hand keeps the runs started.
    proof fn lemma_same_k<K: CountOps>(&self, old_st: BoolDecoderState)
        requires
            bool_valid::<K>(self.data()), self.data().len() <= usize::MAX,
            bstate_ok::<K>(self.data(), old_st),
            self.state().byte_pos == old_st.byte_pos, self.state().value == old_st.value,
            self.state().remaining <= old_st.remaining,
        ensures
            self.inv::<K>(),
            bstate_k::<K>(self.data(), self.state()) == bstate_k::<K>(self.data(), old_st),
            self.pos::<K>() == bstate_pos::<K>(self.data(), old_st) + (old_st.remaining - self.state().remaining),
    {
        let data = self.data@;
        let cs = bool_counts::<K>(data);
        let k = bstate_k::<K>(data, old_st);
        assert(dstate_at::<K>(cs, self.st, k));
        lemma_k_unique::<K>(cs, self.st, bstate_k::<K>(data, self.st), k);
    }
}

// ── Merging ─────────────────────────────────────────────────────────────────

/// The values of counts `cs` whose first run is run `p` (so its value
/// is `run_value(p)`).
pub(crate) open spec fn vals_at(cs: Seq<u64>, p: int) -> Seq<bool>
    decreases cs.len(),
{
    if cs.len() == 0 {
        Seq::empty()
    } else {
        vals_at(cs.drop_last(), p) + Seq::new(cs.last() as nat, |_j: int| run_value(p + cs.len() - 1))
    }
}

pub(crate) proof fn lemma_vals_at_zero(cs: Seq<u64>)
    ensures counts_values(cs) == vals_at(cs, 0),
    decreases cs.len(),
{
    if cs.len() > 0 {
        lemma_vals_at_zero(cs.drop_last());
        assert(counts_values(cs) =~= vals_at(cs, 0));
    }
}

pub(crate) proof fn lemma_vals_at_concat(a: Seq<u64>, b: Seq<u64>, p: int)
    ensures vals_at(a + b, p) == vals_at(a, p) + vals_at(b, p + a.len()),
    decreases b.len(),
{
    if b.len() == 0 {
        assert(a + b =~= a);
        assert(vals_at(a, p) + Seq::<bool>::empty() =~= vals_at(a, p));
    } else {
        assert((a + b).drop_last() =~= a + b.drop_last());
        lemma_vals_at_concat(a, b.drop_last(), p);
        assert(vals_at(a + b, p) =~= vals_at(a, p) + vals_at(b, p + a.len()));
    }
}

pub(crate) proof fn lemma_vals_at_parity(cs: Seq<u64>, p: int, q: int)
    requires 0 <= p, 0 <= q, p % 2 == q % 2,
    ensures vals_at(cs, p) == vals_at(cs, q),
    decreases cs.len(),
{
    if cs.len() > 0 {
        lemma_vals_at_parity(cs.drop_last(), p, q);
        assert(run_value(p + cs.len() - 1) == run_value(q + cs.len() - 1));
        assert(vals_at(cs, p) =~= vals_at(cs, q));
    }
}

pub(crate) proof fn lemma_vals_single(c: u64, p: int)
    ensures vals_at(seq![c], p) == Seq::new(c as nat, |_j: int| run_value(p)),
{
    assert(seq![c].drop_last() =~= Seq::<u64>::empty());
    reveal_with_fuel(vals_at, 2);
    assert(vals_at(seq![c], p) =~= Seq::new(c as nat, |_j: int| run_value(p)));
}

/// A run of `x + y`, as runs of `x` then `y`.
proof fn lemma_split_run(x: u64, y: u64, p: int)
    requires x + y <= u64::MAX,
    ensures vals_at(seq![(x + y) as u64], p) == vals_at(seq![x], p) + vals_at(seq![y], p),
{
    lemma_vals_single((x + y) as u64, p);
    lemma_vals_single(x, p);
    lemma_vals_single(y, p);
    assert(vals_at(seq![(x + y) as u64], p) =~= vals_at(seq![x], p) + vals_at(seq![y], p));
}

/// Merging two runs of one value at the meeting point of `d + [x]` and
/// `[y] + r` (so runs `|d|` and `0` of each share a value).
proof fn lemma_merge_values(d: Seq<u64>, x: u64, y: u64, r: Seq<u64>, q: int)
    requires x + y <= u64::MAX, 0 <= q, d.len() % 2 == q % 2,
    ensures
        vals_at(d + seq![(x + y) as u64] + r, 0)
            == vals_at(d + seq![x], 0) + vals_at(seq![y] + r, q),
{
    let m = seq![(x + y) as u64];
    lemma_vals_at_concat(d + m, r, 0);
    lemma_vals_at_concat(d, m, 0);
    lemma_vals_at_concat(d, seq![x], 0);
    lemma_vals_at_concat(seq![y], r, q);
    lemma_split_run(x, y, d.len() as int);
    lemma_vals_single(y, d.len() as int);
    lemma_vals_single(y, q);
    assert(run_value(d.len() as int) == run_value(q));
    lemma_vals_at_parity(r, d.len() + 1 as int, q + 1);
    assert(vals_at(d + m + r, 0) =~= vals_at(d + seq![x], 0) + vals_at(seq![y] + r, q));
}

/// A merged slab's counts: canonical if both were, past the first.
proof fn lemma_merged_ok(c: Seq<u64>, a: Seq<u64>, b: Seq<u64>)
    requires
        counts_ok(a), counts_ok(b), a.len() > 0,
        c.len() >= a.len(), c.take(a.len() - 1) == a.take(a.len() - 1),
        c[a.len() - 1] >= a[a.len() - 1],
        forall|i: int| a.len() <= i < c.len() ==> #[trigger] c[i] > 0,
        c.last() > 0,
    ensures counts_ok(c),
{
    assert forall|i: int| 1 <= i < c.len() implies #[trigger] c[i] > 0 by {
        if i < a.len() - 1 {
            assert(c[i] == c.take(a.len() - 1)[i]);
            assert(a[i] == a.take(a.len() - 1)[i]);
        }
    }
}

#[inline(always)]
fn truncate(v: &mut Vec<u8>, n: usize)
    requires n <= old(v)@.len(),
    ensures final(v)@ == old(v)@.take(n as int),
{
    v.truncate(n);
    assert(v@ =~= old(v)@.take(n as int));
}

#[inline(always)]
fn extend(v: &mut Vec<u8>, s: &[u8])
    ensures final(v)@ == old(v)@ + s@,
{
    let ghost v0 = v@;
    v.extend_from_slice(s);
    proof {
        assert forall|i: int| 0 <= i < v@.len() implies #[trigger] v@[i] == (v0 + s@)[i] by {
            if i >= v0.len() {
                assert(cloned::<u8>(s@[i - v0.len()], v@[i]));
            }
        }
        assert(v@ =~= v0 + s@);
    }
}

/// `m` is `aa` merged with `bb`: a valid slab of the values of one then
/// the other, with run count and tail `r`.
pub(crate) open spec fn merged_ok<K: CountOps>(m: Seq<u8>, aa: Seq<u8>, bb: Seq<u8>, r: (usize, u8)) -> bool {
    &&& bool_valid::<K>(m)
    &&& bool_values::<K>(m) == bool_values::<K>(aa) + bool_values::<K>(bb)
    &&& r.0 == bool_counts::<K>(m).len()
    &&& r.1 == counts_tail::<K>(bool_counts::<K>(m))
}

/// Counts `cm` of `m`, whose values are `ca`'s then `cb`'s.
proof fn lemma_merged<K: CountOps>(cm: Seq<u64>, m: Seq<u8>, ca: Seq<u64>, aa: Seq<u8>, cb: Seq<u64>, bb: Seq<u8>, r: (usize, u8))
    requires
        counts_ok(cm), counts_bytes::<K>(cm) == m,
        bool_parses::<K>(ca, aa), bool_parses::<K>(cb, bb),
        counts_len(ca) + counts_len(cb) <= usize::MAX,
        vals_at(cm, 0) == vals_at(ca, 0) + vals_at(cb, 0),
        r.0 == cm.len(), r.1 == counts_tail::<K>(cm),
    ensures merged_ok::<K>(m, aa, bb, r),
{
    lemma_vals_at_zero(cm);
    lemma_vals_at_zero(ca);
    lemma_vals_at_zero(cb);
    lemma_values_len(cm);
    lemma_values_len(ca);
    lemma_values_len(cb);
    lemma_counts::<K>(ca, aa);
    lemma_counts::<K>(cb, bb);
    lemma_counts::<K>(cm, m);
}

/// The merge where `a`'s last run, `x` long from byte `a_last_start`,
/// joins `b`'s run `y` (b's counts are `pre`, then `y`, then `rc`, whose
/// bytes are `rest`).
#[verifier::spinoff_prover]
#[verifier::rlimit(40)]
fn merge_join<K: CountOps>(
    a: &mut Vec<u8>, Ghost(start): Ghost<usize>, a_last_start: usize, x: usize, y: usize, rest: &[u8], a_segs: usize, b_segs: usize, b_tail: u8, pre_len: usize,
    Ghost(ca): Ghost<Seq<u64>>, Ghost(cb): Ghost<Seq<u64>>, Ghost(pre): Ghost<Seq<u64>>, Ghost(rc): Ghost<Seq<u64>>, Ghost(bb): Ghost<Seq<u8>>,
) -> (r: (usize, u8))
    requires
        start <= a_last_start <= old(a)@.len(),
        bool_parses::<K>(ca, old(a)@.skip(start as int)), ca.len() == a_segs, a_segs >= 1, ca.last() == x,
        old(a)@.take(a_last_start as int) == old(a)@.take(start as int) + counts_bytes::<K>(ca.drop_last()),
        bool_parses::<K>(cb, bb), cb == pre + seq![y as u64] + rc, rest@ == counts_bytes::<K>(rc),
        pre.len() <= 1, pre.len() == 1 ==> pre[0] == 0, pre.len() == 0 ==> y > 0, pre_len == pre.len(),
        (ca.len() - 1) % 2 == pre.len() % 2,
        b_segs == cb.len(), b_tail == counts_tail::<K>(cb),
        counts_len(ca) + counts_len(cb) <= usize::MAX,
    ensures
        final(a)@.take(start as int) == old(a)@.take(start as int),
        merged_ok::<K>(final(a)@.skip(start as int), old(a)@.skip(start as int), bb, r),
{
    let ghost head = old(a)@.take(start as int);
    let ghost d = ca.drop_last();
    proof {
        assert(ca =~= d + seq![x as u64]);
        lemma_count_le(ca, ca.len() - 1);
        lemma_count_le(cb, pre.len() as int);
        assert(cb[pre.len() as int] == y);
        lemma_values_len(ca);
        lemma_values_len(cb);
    }
    truncate(a, a_last_start);
    let n1 = a.len();
    K::put_count(x + y, a);
    let merge_bytes = a.len() - n1;
    extend(a, rest);
    let _total = a.len();
    let ghost m = (x + y) as u64;
    let ghost cm = d + seq![m] + rc;
    proof {
        K::axiom_enc(m, m, Seq::empty(), Seq::empty());
        lemma_merge_values(d, x as u64, y as u64, rc, pre.len() as int);
        if pre.len() == 1 {
            assert(cb =~= seq![0u64] + (seq![y as u64] + rc));
            lemma_vals_at_concat(seq![0u64], seq![y as u64] + rc, 0);
            lemma_vals_single(0, 0);
            assert(vals_at(seq![0u64], 0) =~= Seq::<bool>::empty());
            assert(vals_at(cb, 0) =~= vals_at(seq![y as u64] + rc, 1));
        } else {
            assert(cb =~= seq![y as u64] + rc);
        }
        lemma_concat::<K>(d + seq![m], rc);
        lemma_bytes_push::<K>(d, m);
        assert(d.push(m) =~= d + seq![m]);
        assert(a@.skip(start as int) =~= counts_bytes::<K>(cm));
        assert(a@.take(start as int) =~= head);
        assert(cm.take(ca.len() - 1) =~= ca.take(ca.len() - 1));
        assert forall|i: int| ca.len() <= i < cm.len() implies #[trigger] cm[i] > 0 by {
            assert(cm[i] == rc[i - ca.len()]);
            assert(rc[i - ca.len()] == cb[i - ca.len() + pre.len() + 1]);
        }
        if rc.len() > 0 {
            assert(cm.last() == cb.last());
        }
        lemma_merged_ok(cm, ca, cb);
        lemma_bytes_len::<K>(cm);
        lemma_bytes_len::<K>(rc);
    }
    let tail = if rest.len() == 0 { merge_bytes as u8 } else { b_tail };
    let r = (a_segs - 1 + (b_segs - pre_len), tail);
    proof {
        if rest.len() > 0 {
            assert(rc.len() > 0) by {
                if rc.len() == 0 {
                    assert(counts_bytes::<K>(rc) =~= Seq::<u8>::empty());
                }
            }
        }
        lemma_merged::<K>(cm, a@.skip(start as int), ca, old(a)@.skip(start as int), cb, bb, r);
    }
    r
}

/// Merge the bool slab `b` (its counts, segments and tail) onto the end
/// of the one in `a` from `start` (the bytes before are others').
/// Returns the merged slab's segments and tail.
#[verifier::spinoff_prover]
#[verifier::rlimit(60)]
#[allow(unused_variables)] // `start` is for the spec: the tail record finds a's last run
pub(crate) fn merge<K: CountOps>(a: &mut Vec<u8>, start: usize, a_tail: u8, a_segs: usize, b: &[u8], b_segs: usize, b_tail: u8) -> (r: (usize, u8))
    requires
        start <= old(a)@.len(),
        bool_valid::<K>(old(a)@.skip(start as int)),
        a_segs == bool_counts::<K>(old(a)@.skip(start as int)).len(),
        a_tail == counts_tail::<K>(bool_counts::<K>(old(a)@.skip(start as int))),
        bool_valid::<K>(b@),
        b_segs == bool_counts::<K>(b@).len(),
        b_tail == counts_tail::<K>(bool_counts::<K>(b@)),
        counts_len(bool_counts::<K>(old(a)@.skip(start as int))) + counts_len(bool_counts::<K>(b@)) <= usize::MAX,
    ensures
        final(a)@.take(start as int) == old(a)@.take(start as int),
        merged_ok::<K>(final(a)@.skip(start as int), old(a)@.skip(start as int), b@, r),
{
    let ghost a0 = a@;
    let ghost aa = a0.skip(start as int);
    let ghost ca = bool_counts::<K>(aa);
    let ghost cb = bool_counts::<K>(b@);
    let ghost head = a0.take(start as int);
    proof {
        lemma_counts::<K>(ca, aa);
        lemma_counts::<K>(cb, b@);
        assert(a0 =~= head + aa);
    }
    if b.len() == 0 {
        proof {
            assert(cb.len() == 0) by {
                if cb.len() > 0 {
                    lemma_bytes_len::<K>(cb);
                }
            }
            lemma_merged::<K>(ca, aa, ca, aa, cb, b@, (a_segs, a_tail));
            assert(vals_at(cb, 0) =~= Seq::<bool>::empty());
            assert(vals_at(ca, 0) + vals_at(cb, 0) =~= vals_at(ca, 0));
            lemma_merged::<K>(ca, aa, ca, aa, cb, b@, (a_segs, a_tail));
        }
        return (a_segs, a_tail);
    }
    proof {
        if cb.len() == 0 {
            assert(counts_bytes::<K>(cb) =~= Seq::<u8>::empty());
        }
        lemma_first::<K>(cb);
        lemma_count_le(cb, 0);
        lemma_values_len(ca);
        lemma_values_len(cb);
    }
    if a_segs == 0 {
        // nothing to merge with: the slab is `b`
        extend(a, b);
        proof {
            assert(ca =~= Seq::<u64>::empty());
            assert(counts_bytes::<K>(ca) =~= Seq::<u8>::empty());
            assert(a@.skip(start as int) =~= b@);
            assert(a@.take(start as int) =~= head);
            assert(vals_at(ca, 0) =~= Seq::<bool>::empty());
            assert(vals_at(ca, 0) + vals_at(cb, 0) =~= vals_at(cb, 0));
            lemma_merged::<K>(cb, b@, ca, aa, cb, b@, (b_segs, b_tail));
        }
        return (b_segs, b_tail);
    }
    // a's last run, found by its tail record
    let ghost d = ca.drop_last();
    let ghost x = ca.last();
    proof {
        assert(ca =~= d.push(x));
        lemma_bytes_push::<K>(d, x);
        K::axiom_enc(x, x, Seq::empty(), Seq::empty());
        lemma_count_le(ca, ca.len() - 1);
        assert(a0.take(a0.len() - a_tail) =~= head + counts_bytes::<K>(d));
    }
    let a_last_start = a.len() - a_tail as usize;
    let tail_slice = slice_from(a, a_last_start);
    proof {
        assert(tail_slice@ =~= K::enc(x));
        assert(K::enc(x).is_prefix_of(tail_slice@));
    }
    let (_n, a_last_count) = match K::read_count(tail_slice) {
        Some(v) => v,
        None => (0, 0),
    };
    proof {
        K::axiom_enc(a_last_count as u64, x, tail_slice@.skip(_n as int), Seq::empty());
        assert(tail_slice@ =~= tail_slice@.take(_n as int) + tail_slice@.skip(_n as int));
    }
    // runs alternate false/true: an even run count ends on true
    let a_last_value = a_segs % 2 == 0;
    // b's first run (always false)
    proof {
        assert(K::enc(cb[0]).is_prefix_of(b@));
    }
    let (b_first_cb, b_first_count) = match K::read_count(b) {
        Some(v) => v,
        None => (0, 0),
    };
    proof {
        K::axiom_enc(b_first_count as u64, cb[0], b@.skip(b_first_cb as int), counts_bytes::<K>(cb.skip(1)));
        assert(b@ =~= b@.take(b_first_cb as int) + b@.skip(b_first_cb as int));
    }
    let b_rest = slice_from(b, b_first_cb);
    proof {
        assert(b_rest@ =~= counts_bytes::<K>(cb.skip(1)));
        assert(seq![cb[0]] + cb.skip(1) =~= cb);
    }
    if !a_last_value {
        if b_first_count > 0 {
            // a ends false, b starts false: one run of the two
            proof {
                assert(Seq::<u64>::empty() + seq![cb[0]] + cb.skip(1) =~= cb);
            }
            let ghost pre0 = Seq::<u64>::empty();
            let ghost rc1 = cb.skip(1);
            let ghost bb = b@;
            merge_join::<K>(a, Ghost(start), a_last_start, a_last_count, b_first_count, b_rest, a_segs, b_segs, b_tail, 0,
                Ghost(ca), Ghost(cb), Ghost(pre0), Ghost(rc1), Ghost(bb))
        } else {
            // b's zero-count pad goes: its runs carry on a's alternation
            let ghost rc1 = cb.skip(1);
            let ghost bb = b@;
            merge_append::<K>(a, Ghost(start), b_rest, a_segs, b_segs - 1, Ghost(b_segs), b_tail,
                Ghost(ca), Ghost(cb), Ghost(rc1), Ghost(bb))
        }
    } else if b_first_count > 0 {
        // a ends true, b starts false: b follows as is
        proof {
            assert(cb.skip(0) =~= cb);
        }
        let ghost bb = b@;
        merge_append::<K>(a, Ghost(start), b, a_segs, b_segs, Ghost(b_segs), b_tail, Ghost(ca), Ghost(cb), Ghost(cb), Ghost(bb))
    } else {
        // b starts with its zero-count pad then true: that run joins a's
        proof {
            assert(cb.len() >= 2);
            lemma_first::<K>(cb.skip(1));
            assert(cb.skip(1)[0] == cb[1]);
            assert(cb.skip(1).skip(1) =~= cb.skip(2));
            lemma_count_le(cb, 1);
            assert(K::enc(cb[1]).is_prefix_of(b_rest@));
        }
        let (cb2, count2) = match K::read_count(b_rest) {
            Some(v) => v,
            None => (0, 0),
        };
        proof {
            K::axiom_enc(count2 as u64, cb[1], b_rest@.skip(cb2 as int), counts_bytes::<K>(cb.skip(2)));
            assert(b_rest@ =~= b_rest@.take(cb2 as int) + b_rest@.skip(cb2 as int));
            assert(seq![cb[0]] + seq![cb[1]] + cb.skip(2) =~= cb);
        }
        let b_rest2 = slice_from(b_rest, cb2);
        proof {
            assert(b_rest2@ =~= counts_bytes::<K>(cb.skip(2)));
        }
        let ghost pre1 = seq![cb[0]];
        let ghost rc2 = cb.skip(2);
        let ghost bb = b@;
        merge_join::<K>(a, Ghost(start), a_last_start, a_last_count, count2, b_rest2, a_segs, b_segs, b_tail, 1,
            Ghost(ca), Ghost(cb), Ghost(pre1), Ghost(rc2), Ghost(bb))
    }
}

/// The merge where `b`'s counts from `rc` on (their bytes `rest`) follow
/// `a`'s: `rc` is all of b's, or all but its zero-count pad.
#[verifier::spinoff_prover]
fn merge_append<K: CountOps>(
    a: &mut Vec<u8>, Ghost(start): Ghost<usize>, rest: &[u8], a_segs: usize, rest_segs: usize, Ghost(b_segs): Ghost<usize>, b_tail: u8,
    Ghost(ca): Ghost<Seq<u64>>, Ghost(cb): Ghost<Seq<u64>>, Ghost(rc): Ghost<Seq<u64>>, Ghost(bb): Ghost<Seq<u8>>,
) -> (r: (usize, u8))
    requires
        start <= old(a)@.len(),
        bool_parses::<K>(ca, old(a)@.skip(start as int)), ca.len() == a_segs, a_segs >= 1,
        bool_parses::<K>(cb, bb), rest@ == counts_bytes::<K>(rc), rc.len() == rest_segs, rc.len() >= 1, cb.len() >= 1,
        rc == cb && ca.len() % 2 == 0 && cb[0] > 0 || rc == cb.skip(1) && ca.len() % 2 == 1 && cb[0] == 0,
        b_segs == cb.len(), b_tail == counts_tail::<K>(cb),
        counts_len(ca) + counts_len(cb) <= usize::MAX,
    ensures
        final(a)@.take(start as int) == old(a)@.take(start as int),
        merged_ok::<K>(final(a)@.skip(start as int), old(a)@.skip(start as int), bb, r),
{
    let ghost head = old(a)@.take(start as int);
    let ghost aa = old(a)@.skip(start as int);
    proof {
        assert(old(a)@ =~= head + aa);
    }
    extend(a, rest);
    let _total = a.len();
    let ghost cm = ca + rc;
    proof {
        lemma_concat::<K>(ca, rc);
        lemma_vals_at_concat(ca, rc, 0);
        if rc == cb.skip(1) {
            assert(seq![cb[0]] + cb.skip(1) =~= cb);
            lemma_vals_at_concat(seq![cb[0]], cb.skip(1), 0);
            lemma_vals_single(cb[0], 0);
            assert(vals_at(seq![cb[0]], 0) =~= Seq::<bool>::empty());
            lemma_vals_at_parity(rc, ca.len() as int, 1);
            assert(vals_at(cb, 0) =~= vals_at(rc, 1));
        } else {
            lemma_vals_at_parity(rc, ca.len() as int, 0);
        }
        assert(a@.skip(start as int) =~= counts_bytes::<K>(cm));
        assert(a@.take(start as int) =~= head);
        assert(cm.take(ca.len() - 1) =~= ca.take(ca.len() - 1));
        assert forall|i: int| ca.len() <= i < cm.len() implies #[trigger] cm[i] > 0 by {
            assert(cm[i] == rc[i - ca.len()]);
            if rc == cb.skip(1) {
                assert(rc[i - ca.len()] == cb[i - ca.len() + 1]);
            }
        }
        assert(cm.last() == rc.last());
        assert(rc.last() == cb.last());
        lemma_merged_ok(cm, ca, cb);
        lemma_bytes_len::<K>(cm);
    }
    let r = (a_segs + rest_segs, b_tail);
    proof {
        lemma_merged::<K>(cm, a@.skip(start as int), ca, aa, cb, bb, r);
    }
    r
}

/// The counts a codec writes, for code that names columns by their codec
/// (`BoolEncoding<C>`) but is verified over [`CountOps`].
pub trait CountsOf {
    type K: CountOps;
}

#[inline(always)]
fn slice_from(v: &[u8], n: usize) -> (s: &[u8])
    requires n <= v@.len(),
    ensures s@ == v@.skip(n as int),
{
    let s = &v[n..];
    assert(s@ == v@.skip(n as int));
    s
}

} // verus!

/// [`CountOps`] for codec `C`: the trusted bridge from the verified bool
/// code to the codec's own.
///
/// Public only because `BoolEncoding<C>`'s slab model names it; this
/// module is not.
pub struct Counts<C>(std::marker::PhantomData<C>);

impl<C: crate::Codec> CountsOf for C {
    type K = Counts<C>;
}

// Outside `verus!`, so Verus takes it on trust; its spec items exist only
// when Verus compiles the crate.
impl<C: crate::Codec> CountOps for Counts<C> {
    #[cfg(verus_keep_ghost)]
    fn enc(_n: u64) -> Seq<u8> {
        unimplemented!()
    }

    #[cfg(verus_keep_ghost)]
    fn axiom_enc(_a: u64, _b: u64, _s: Seq<u8>, _t: Seq<u8>) {}

    #[inline(always)]
    fn put_count(n: usize, buf: &mut Vec<u8>) {
        buf.extend(C::encode_count(n));
    }

    #[inline(always)]
    fn read_count(data: &[u8]) -> Option<(usize, usize)> {
        C::read_count(data)
    }
}
