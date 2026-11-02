//! The boolean wire format, verified with Verus.
//!
//! A bool slab is a sequence of unsigned counts ([`counts_bytes`]), the
//! lengths of alternating runs starting with `false` ([`counts_values`]).
//! It is canonical ([`counts_ok`]) when only the first count is zero (so
//! that a slab can start with `true`) and the last is not.
//!
//! [`validate`] accepts exactly the canonical encodings of at most
//! `usize::MAX` values, and [`BoolLoad`] cuts them into slabs of an even
//! number of runs, each again canonical, which together hold the input's
//! values.

// Verus proofs read more plainly with `x = x + n`.
#![allow(clippy::len_zero, clippy::assign_op_pattern)]

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
pub open spec fn counts_bytes<K: CountOps>(cs: Seq<u64>) -> Seq<u8>
    decreases cs.len(),
{
    if cs.len() == 0 { Seq::empty() } else { counts_bytes::<K>(cs.drop_last()) + K::enc(cs.last()) }
}

/// How many values counts `cs` stand for.
pub open spec fn counts_len(cs: Seq<u64>) -> nat
    decreases cs.len(),
{
    if cs.len() == 0 { 0 } else { counts_len(cs.drop_last()) + cs.last() as nat }
}

/// Run `i`'s value: runs alternate, starting with `false`.
pub open spec fn run_value(i: int) -> bool {
    i % 2 == 1
}

/// The values counts `cs` stand for.
pub open spec fn counts_values(cs: Seq<u64>) -> Seq<bool>
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
pub open spec fn counts_ok(cs: Seq<u64>) -> bool {
    &&& forall|i: int| 1 <= i < cs.len() ==> #[trigger] cs[i] > 0
    &&& cs.len() > 0 ==> cs.last() > 0
}

/// `cs` is the canonical parse of `data`, of at most `usize::MAX` values.
pub open spec fn bool_parses<K: CountOps>(cs: Seq<u64>, data: Seq<u8>) -> bool {
    &&& counts_ok(cs)
    &&& counts_len(cs) <= usize::MAX
    &&& counts_bytes::<K>(cs) == data
}

/// `data` is a valid bool slab.
pub open spec fn bool_valid<K: CountOps>(data: Seq<u8>) -> bool {
    exists|cs: Seq<u64>| bool_parses::<K>(cs, data)
}

/// A valid bool slab's counts.
pub open spec fn bool_counts<K: CountOps>(data: Seq<u8>) -> Seq<u64> {
    choose|cs: Seq<u64>| bool_parses::<K>(cs, data)
}

/// A valid bool slab's values.
pub open spec fn bool_values<K: CountOps>(data: Seq<u8>) -> Seq<bool> {
    counts_values(bool_counts::<K>(data))
}

/// The byte length of the last count: a bool slab's tail record.
pub open spec fn counts_tail<K: CountOps>(cs: Seq<u64>) -> nat {
    if cs.len() == 0 { 0 } else { K::enc(cs.last()).len() }
}

// ── Lemmas ──────────────────────────────────────────────────────────────────

pub proof fn lemma_bytes_push<K: CountOps>(cs: Seq<u64>, x: u64)
    ensures counts_bytes::<K>(cs.push(x)) == counts_bytes::<K>(cs) + K::enc(x),
        counts_len(cs.push(x)) == counts_len(cs) + x,
{
    assert(cs.push(x).drop_last() =~= cs);
}

/// Counts' bytes and lengths, split.
pub proof fn lemma_concat<K: CountOps>(a: Seq<u64>, b: Seq<u64>)
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
pub proof fn lemma_values_concat(a: Seq<u64>, b: Seq<u64>)
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

pub proof fn lemma_values_len(cs: Seq<u64>)
    ensures counts_values(cs).len() == counts_len(cs),
    decreases cs.len(),
{
    if cs.len() > 0 {
        lemma_values_len(cs.drop_last());
    }
}

/// Counts' bytes are at least a byte each.
pub proof fn lemma_bytes_len<K: CountOps>(cs: Seq<u64>)
    ensures counts_bytes::<K>(cs).len() >= cs.len(),
    decreases cs.len(),
{
    if cs.len() > 0 {
        K::axiom_enc(cs.last(), cs.last(), Seq::empty(), Seq::empty());
        lemma_bytes_len::<K>(cs.drop_last());
    }
}

/// Two parses whose bytes agree are the same.
pub proof fn lemma_unique<K: CountOps>(a: Seq<u64>, b: Seq<u64>)
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
pub proof fn lemma_first<K: CountOps>(cs: Seq<u64>)
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
pub proof fn lemma_counts<K: CountOps>(cs: Seq<u64>, data: Seq<u8>)
    requires bool_parses::<K>(cs, data),
    ensures bool_valid::<K>(data), bool_counts::<K>(data) == cs,
{
    let c2 = bool_counts::<K>(data);
    lemma_unique::<K>(c2, cs);
}

/// Each count is among the values.
pub proof fn lemma_count_le(cs: Seq<u64>, i: int)
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
pub open spec fn complete<K: CountOps>(data: Seq<u8>, cs: Seq<u64>) -> bool {
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
pub fn validate<K: CountOps>(data: &[u8]) -> (r: Result<BoolInfo, PackError>)
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

/// A bool slab's bytes and recorded metadata.
pub uninterp spec fn bslab_bytes(s: BSlab) -> Seq<u8>;

pub uninterp spec fn bslab_len(s: BSlab) -> usize;

pub uninterp spec fn bslab_segments(s: BSlab) -> usize;

pub uninterp spec fn bslab_tail(s: BSlab) -> u8;

/// A slab of a copy of `data`.
#[verifier::external_body]
#[inline(always)]
fn new_bslab(data: &[u8], len: usize, segments: usize, tail: u8) -> (s: BSlab)
    ensures bslab_bytes(s) == data@, bslab_len(s) == len, bslab_segments(s) == segments, bslab_tail(s) == tail,
{
    crate::column::Slab { data: data.to_vec(), len, segments, tail }
}

#[verifier::external_body]
#[inline(always)]
fn bslab_len_of(s: &BSlab) -> (n: usize)
    ensures n == bslab_len(*s),
{
    s.len
}

/// A valid bool slab with its metadata.
pub open spec fn bslab_ok<K: CountOps>(s: BSlab) -> bool {
    let cs = bool_counts::<K>(bslab_bytes(s));
    &&& bool_valid::<K>(bslab_bytes(s))
    &&& bslab_len(s) == counts_len(cs)
    &&& bslab_segments(s) == cs.len()
    &&& bslab_tail(s) == counts_tail::<K>(cs)
}

/// The values of bool slabs, in order.
pub open spec fn bslabs_values<K: CountOps>(s: Seq<BSlab>) -> Seq<bool>
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

} // verus!

/// [`CountOps`] for codec `C`: the trusted bridge from the verified bool
/// code to the codec's own.
pub(crate) struct Counts<C>(std::marker::PhantomData<C>);

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
    fn read_count(data: &[u8]) -> Option<(usize, usize)> {
        C::read_count(data)
    }
}
