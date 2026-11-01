//! Verified bijou64 / bijou64s codec.
//!
//! The executable code is the encoder/decoder from the `bijoux` crate
//! (v0.3.1, <https://github.com/inkandswitch/bijou>, © Brooklyn Zelenka,
//! MIT OR Apache-2.0), reduced to what hexane calls and adapted to the
//! subset of Rust that [Verus](https://github.com/verus-lang/verus) checks:
//!
//! * `u64::{to,from}_be_bytes` are spelled as shifts (the decoder still
//!   reads a zero-padded `[u8; 8]`, as bijoux does — that shape is what
//!   LLVM turns into one load + bswap), and slice patterns as length
//!   checks plus indexing;
//! * the tier tables are literals rather than `const fn` computations
//!   (the proof checks every entry against the format definition).
//!
//! Nothing about the codec is assumed. What the proofs do trust is Verus
//! and Z3, and `vstd`'s specifications of the std operations used here:
//! indexing, `leading_zeros`, `checked_add`, `saturating_sub`,
//! `split_first` and `split_at`.
//!
//! Above the code sits the format itself, written as mathematics — big-
//! endian digits and per-tier offsets over unbounded naturals — and proofs
//! that the code implements it and that it is a bijection:
//!
//! * [`spec_encode`] / [`spec_decode`] define bijou64;
//! * [`lemma_roundtrip`]: decoding an encoding gives back the value;
//! * [`lemma_canonical`]: whatever decodes to a `u64` *is* that value's
//!   encoding, so every integer has exactly one encoding and no other
//!   byte string is accepted;
//! * [`decode`], [`encoded_bytes`], [`encoded_len`] and [`encoded_len_at`]
//!   are proved against those definitions, and the signed variants
//!   against zigzag composed with them.
//!
//! Outside verification, `verus!` erases every spec and proof, leaving
//! plain Rust. To re-check the proofs, run Verus on this file alone:
//!
//! ```text
//! verus --crate-type=lib src/bijou64.rs
//! ```

#[allow(unused_imports)]
use vstd::prelude::*;

verus! {

// ── The format, as mathematics ──────────────────────────────────────────────

pub open spec fn pow256(e: nat) -> nat
    decreases e,
{
    if e == 0 { 1 } else { 256 * pow256((e - 1) as nat) }
}

/// The first value of tier `t`, whose encodings carry `t` payload bytes
/// after a tag byte `247 + t`. Tier 0 is the bare byte `0..=247`; each
/// later tier starts where the previous one's code space ends, which is
/// what makes the tiers disjoint and the format canonical.
pub open spec fn offset(t: nat) -> nat
    decreases t,
{
    if t <= 1 { 248 * t } else { offset((t - 1) as nat) + pow256((t - 1) as nat) }
}

/// The big-endian number in the `n` bytes of `s` starting at `start`.
pub open spec fn be_at(s: Seq<u8>, start: int, n: nat) -> nat
    decreases n,
{
    if n == 0 { 0 } else { be_at(s, start, (n - 1) as nat) * 256 + s[start + n - 1] as nat }
}

/// `x` as exactly `n` big-endian bytes.
pub open spec fn be_bytes(x: nat, n: nat) -> Seq<u8>
    decreases n,
{
    if n == 0 { Seq::empty() } else { be_bytes(x / 256, (n - 1) as nat).push((x % 256) as u8) }
}

/// The tier a value belongs to. Every `u64` lies below `offset(9)`.
pub open spec fn tier(v: nat) -> nat {
    if v < offset(1) { 0 }
    else if v < offset(2) { 1 }
    else if v < offset(3) { 2 }
    else if v < offset(4) { 3 }
    else if v < offset(5) { 4 }
    else if v < offset(6) { 5 }
    else if v < offset(7) { 6 }
    else if v < offset(8) { 7 }
    else { 8 }
}

/// The bijou64 encoding of `v`.
pub open spec fn spec_encode(v: nat) -> Seq<u8> {
    let t = tier(v);
    if t == 0 {
        seq![v as u8]
    } else {
        seq![(247 + t) as u8] + be_bytes((v - offset(t)) as nat, t)
    }
}

/// What a prefix of `s` decodes to, as `(value, bytes consumed)`, over
/// unbounded naturals; `None` if `s` is too short for its tag. A `u64`
/// decoder must additionally reject values above `u64::MAX` (only
/// reachable with tag `0xFF`).
pub open spec fn spec_decode(s: Seq<u8>) -> Option<(nat, nat)> {
    if s.len() == 0 {
        None
    } else if s[0] < 248 {
        Some((s[0] as nat, 1))
    } else {
        let t = (s[0] - 247) as nat;
        if s.len() < t + 1 {
            None
        } else {
            Some((offset(t) + be_at(s, 1, t), t + 1))
        }
    }
}

// ── Facts about the definitions ─────────────────────────────────────────────

pub proof fn lemma_constants()
    ensures
        pow256(1) == 0x100,
        pow256(2) == 0x1_0000,
        pow256(3) == 0x100_0000,
        pow256(4) == 0x1_0000_0000,
        pow256(5) == 0x100_0000_0000,
        pow256(6) == 0x1_0000_0000_0000,
        pow256(7) == 0x100_0000_0000_0000,
        pow256(8) == 0x1_0000_0000_0000_0000,
        offset(1) == 248,
        offset(2) == 504,
        offset(3) == 66040,
        offset(4) == 16843256,
        offset(5) == 4311810552,
        offset(6) == 1103823438328,
        offset(7) == 282578800148984,
        offset(8) == 72340172838076920,
        offset(9) > u64::MAX,
{
    reveal_with_fuel(pow256, 9);
    reveal_with_fuel(offset, 10);
}

pub proof fn lemma_be_at_bound(s: Seq<u8>, start: int, n: nat)
    ensures be_at(s, start, n) < pow256(n),
    decreases n,
{
    if n > 0 {
        lemma_be_at_bound(s, start, (n - 1) as nat);
        let (x, d) = (be_at(s, start, (n - 1) as nat), s[start + n - 1] as nat);
        assert(x * 256 + d < pow256(n)) by (nonlinear_arith)
            requires x < pow256((n - 1) as nat), d < 256, pow256(n) == 256 * pow256((n - 1) as nat);
    }
}

/// `be_at` reads only the `n` bytes it is pointed at.
pub proof fn lemma_be_at_frame(s1: Seq<u8>, start1: int, s2: Seq<u8>, start2: int, n: nat)
    requires forall|i: int| 0 <= i < n ==> #[trigger] s1[start1 + i] == s2[start2 + i],
    ensures be_at(s1, start1, n) == be_at(s2, start2, n),
    decreases n,
{
    if n > 0 {
        lemma_be_at_frame(s1, start1, s2, start2, (n - 1) as nat);
        assert(s1[start1 + n - 1] == s2[start2 + n - 1]);
    }
}

pub proof fn lemma_be_bytes_len(x: nat, n: nat)
    ensures be_bytes(x, n).len() == n,
    decreases n,
{
    if n > 0 { lemma_be_bytes_len(x / 256, (n - 1) as nat); }
}

/// Reading `n` big-endian bytes back gives the number written.
pub proof fn lemma_be_of_bytes(x: nat, n: nat)
    requires x < pow256(n),
    ensures be_at(be_bytes(x, n), 0, n) == x,
    decreases n,
{
    if n > 0 {
        let q = x / 256;
        let b = be_bytes(x, n);
        let p = be_bytes(q, (n - 1) as nat);
        assert(q < pow256((n - 1) as nat)) by (nonlinear_arith)
            requires x < pow256(n), pow256(n) == 256 * pow256((n - 1) as nat), q == x / 256;
        lemma_be_of_bytes(q, (n - 1) as nat);
        lemma_be_bytes_len(q, (n - 1) as nat);
        assert forall|i: int| 0 <= i < n - 1 implies #[trigger] b[0 + i] == p[0 + i] by {}
        lemma_be_at_frame(b, 0, p, 0, (n - 1) as nat);
        assert(x == q * 256 + x % 256);
    }
}

/// Writing out the number read from `n` bytes gives back those bytes.
pub proof fn lemma_bytes_of_be(s: Seq<u8>, start: int, n: nat)
    requires 0 <= start, start + n <= s.len(),
    ensures be_bytes(be_at(s, start, n), n) == s.subrange(start, start + n),
    decreases n,
{
    if n == 0 {
        assert(s.subrange(start, start) =~= Seq::<u8>::empty());
    } else {
        let m = (n - 1) as nat;
        let x = be_at(s, start, n);
        let y = be_at(s, start, m);
        let d = s[start + m] as nat;
        lemma_bytes_of_be(s, start, m);
        assert(x / 256 == y && x % 256 == d) by (nonlinear_arith)
            requires x == y * 256 + d, d < 256;
        assert(s.subrange(start, start + n) =~= s.subrange(start, start + m).push(s[start + m]));
    }
}

/// `offset(t) <= v < offset(t + 1)` pins down the tier.
pub proof fn lemma_tier_of(v: nat, t: nat)
    requires 1 <= t <= 8, offset(t) <= v < offset(t) + pow256(t),
    ensures tier(v) == t,
{
    lemma_constants();
}

/// A value's tier bounds it from both sides.
pub proof fn lemma_tier_bounds(v: nat)
    requires v <= u64::MAX,
    ensures
        tier(v) <= 8,
        tier(v) == 0 ==> v < 248,
        tier(v) >= 1 ==> offset(tier(v)) <= v < offset(tier(v)) + pow256(tier(v)),
{
    lemma_constants();
}

/// Decoding an encoding gives back the value.
pub proof fn lemma_roundtrip(v: nat, rest: Seq<u8>)
    requires v <= u64::MAX,
    ensures spec_decode(spec_encode(v) + rest) == Some((v, spec_encode(v).len())),
{
    lemma_tier_bounds(v);
    let t = tier(v);
    let s = spec_encode(v) + rest;
    if t > 0 {
        let p = (v - offset(t)) as nat;
        let e = spec_encode(v);
        lemma_be_bytes_len(p, t);
        lemma_be_of_bytes(p, t);
        assert forall|i: int| 0 <= i < t implies #[trigger] s[1 + i] == be_bytes(p, t)[0 + i] by {}
        lemma_be_at_frame(s, 1, be_bytes(p, t), 0, t);
        assert(s[0] == (247 + t) as u8);
    }
}

/// Canonicality: any byte string that decodes to a `u64` begins with
/// exactly that value's encoding. With [`lemma_roundtrip`], every `u64`
/// has one encoding and every accepted encoding one value.
pub proof fn lemma_canonical(s: Seq<u8>)
    requires spec_decode(s) is Some, spec_decode(s).unwrap().0 <= u64::MAX,
    ensures
        s.subrange(0, spec_decode(s).unwrap().1 as int) == spec_encode(spec_decode(s).unwrap().0),
{
    let (v, n) = spec_decode(s).unwrap();
    if s[0] < 248 {
        lemma_constants();
        assert(s.subrange(0, 1) =~= seq![s[0]]);
    } else {
        let t = (s[0] - 247) as nat;
        let p = be_at(s, 1, t);
        lemma_be_at_bound(s, 1, t);
        lemma_tier_of(v, t);
        lemma_bytes_of_be(s, 1, t);
        assert(s.subrange(0, n as int) =~= seq![s[0]] + s.subrange(1, 1 + t as int));
    }
}

// ── Decoding ────────────────────────────────────────────────────────────────

/// Errors that can occur when decoding a bijou64.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeError {
    /// The input buffer is shorter than the encoding requires.
    BufferTooShort,
    /// The decoded value exceeds `u64::MAX` (tier 8 only).
    Overflow,
}

/// Maximum number of bytes a bijou64 encoding can occupy.
pub const MAX_BYTES: usize = 9;

/// Tag byte threshold: values below this are encoded as a single byte.
const TAG_THRESHOLD: u8 = 248;

/// Number of multi-byte tiers (tags 248–255).
const NUM_TIERS: usize = 8;

/// `OFFSETS[t]` is the first value that requires tier `t`.
const OFFSETS: [u64; NUM_TIERS + 1] = [
    0,
    248,
    504,
    66_040,
    16_843_256,
    4_311_810_552,
    1_103_823_438_328,
    282_578_800_148_984,
    72_340_172_838_076_920,
];

/// Per-tier exclusive upper bounds: `BOUNDS[t] == OFFSETS[t + 1]`, with
/// tier 8 extending to `u64::MAX`.
const BOUNDS: [u64; NUM_TIERS + 1] = [
    248,
    504,
    66_040,
    16_843_256,
    4_311_810_552,
    1_103_823_438_328,
    282_578_800_148_984,
    72_340_172_838_076_920,
    u64::MAX,
];

/// Leading zero bytes add nothing to a big-endian number.
pub proof fn lemma_be_at_zeros(a: Seq<u8>, k: nat, m: nat)
    requires k + m <= a.len(), forall|i: int| 0 <= i < k ==> #[trigger] a[i] == 0,
    ensures be_at(a, 0, k + m) == be_at(a, k as int, m),
    decreases k + m,
{
    if m > 0 {
        lemma_be_at_zeros(a, k, (m - 1) as nat);
    } else if k > 0 {
        lemma_be_at_zeros(a, (k - 1) as nat, 0);
    }
}

/// An 8-byte array holding `t` payload bytes of `s` (from index 1) behind
/// `8 - t` zeros reads as those `t` bytes.
pub proof fn lemma_be_padded(s: Seq<u8>, a: Seq<u8>, t: nat)
    requires
        a.len() == 8, 1 <= t <= 8, t + 1 <= s.len(),
        forall|i: int| 0 <= i < 8 - t ==> #[trigger] a[i] == 0,
        forall|i: int| 0 <= i < t ==> #[trigger] a[8 - t + i] == s[1 + i],
    ensures be_at(a, 0, 8) == be_at(s, 1, t),
{
    lemma_be_at_zeros(a, (8 - t) as nat, t);
    assert forall|i: int| 0 <= i < t implies #[trigger] a[(8 - t) + i] == s[1 + i] by {}
    lemma_be_at_frame(a, (8 - t) as int, s, 1, t);
}

/// Reads 8 big-endian bytes — `u64::from_be_bytes`, spelled as shifts so
/// that no unverified std behaviour is assumed. Taking a fixed-size array
/// (as bijoux does) matters: LLVM sees one 8-byte value and emits a load +
/// bswap, where the same shifts over separately indexed bytes decode ~1.8×
/// slower.
#[inline(always)]
fn be8(b: [u8; 8]) -> (r: u64)
    ensures r as nat == be_at(b@, 0, 8),
{
    proof {
        reveal_with_fuel(be_at, 9);
        let (x0, x1, x2, x3, x4, x5, x6, x7) = (
            b[0] as u64, b[1] as u64, b[2] as u64, b[3] as u64,
            b[4] as u64, b[5] as u64, b[6] as u64, b[7] as u64,
        );
        assert(x0 << 56 | x1 << 48 | x2 << 40 | x3 << 32 | x4 << 24 | x5 << 16 | x6 << 8 | x7
            == ((((((x0 * 256 + x1) * 256 + x2) * 256 + x3) * 256 + x4) * 256 + x5) * 256 + x6)
                * 256 + x7)
            by (bit_vector)
            requires x0 < 256, x1 < 256, x2 < 256, x3 < 256, x4 < 256, x5 < 256, x6 < 256, x7 < 256;
    }
    (b[0] as u64) << 56 | (b[1] as u64) << 48 | (b[2] as u64) << 40 | (b[3] as u64) << 32
        | (b[4] as u64) << 24 | (b[5] as u64) << 16 | (b[6] as u64) << 8 | b[7] as u64
}

/// The decoder's contract, over the format definition.
pub open spec fn decode_ok(buf: Seq<u8>, r: Result<(u64, usize), DecodeError>) -> bool {
    match r {
        Ok((v, n)) => spec_decode(buf) == Some((v as nat, n as nat))
            // canonicality, restated for callers: these bytes are *the*
            // encoding of v
            && buf.subrange(0, n as int) == spec_encode(v as nat),
        Err(DecodeError::BufferTooShort) => spec_decode(buf) is None,
        Err(DecodeError::Overflow) => spec_decode(buf) is Some
            && spec_decode(buf).unwrap().0 > u64::MAX,
    }
}

/// Decodes a bijou64 from the front of `buf`, returning
/// `(value, bytes_consumed)`.
#[inline]
pub fn decode(buf: &[u8]) -> (r: Result<(u64, usize), DecodeError>)
    ensures decode_ok(buf@, r),
{
    proof { lemma_constants(); }
    let n = buf.len();
    if n == 0 {
        return Err(DecodeError::BufferTooShort);
    }
    let tag = buf[0];
    if tag < TAG_THRESHOLD {
        proof { lemma_canonical(buf@); }
        return Ok((tag as u64, 1));
    }
    // One arm per tier, each a fixed-size big-endian read of a zero-padded
    // 8-byte array: LLVM turns every arm into one load + bswap (bijoux's
    // OPTIMISATION.md explains why a shared loop is several times slower).
    let (off, payload, consumed): (u64, u64, usize) = match tag {
        0xF8 => {
            if n < 2 { return Err(DecodeError::BufferTooShort); }
            proof { reveal_with_fuel(be_at, 2); }
            (OFFSETS[1], buf[1] as u64, 2)
        },
        0xF9 => {
            if n < 3 { return Err(DecodeError::BufferTooShort); }
            let a = [0, 0, 0, 0, 0, 0, buf[1], buf[2]];
            proof { lemma_be_padded(buf@, a@, 2); }
            (OFFSETS[2], be8(a), 3)
        },
        0xFA => {
            if n < 4 { return Err(DecodeError::BufferTooShort); }
            let a = [0, 0, 0, 0, 0, buf[1], buf[2], buf[3]];
            proof { lemma_be_padded(buf@, a@, 3); }
            (OFFSETS[3], be8(a), 4)
        },
        0xFB => {
            if n < 5 { return Err(DecodeError::BufferTooShort); }
            let a = [0, 0, 0, 0, buf[1], buf[2], buf[3], buf[4]];
            proof { lemma_be_padded(buf@, a@, 4); }
            (OFFSETS[4], be8(a), 5)
        },
        0xFC => {
            if n < 6 { return Err(DecodeError::BufferTooShort); }
            let a = [0, 0, 0, buf[1], buf[2], buf[3], buf[4], buf[5]];
            proof { lemma_be_padded(buf@, a@, 5); }
            (OFFSETS[5], be8(a), 6)
        },
        0xFD => {
            if n < 7 { return Err(DecodeError::BufferTooShort); }
            let a = [0, 0, buf[1], buf[2], buf[3], buf[4], buf[5], buf[6]];
            proof { lemma_be_padded(buf@, a@, 6); }
            (OFFSETS[6], be8(a), 7)
        },
        0xFE => {
            if n < 8 { return Err(DecodeError::BufferTooShort); }
            let a = [0, buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7]];
            proof { lemma_be_padded(buf@, a@, 7); }
            (OFFSETS[7], be8(a), 8)
        },
        // 0xFF — the only tag left
        _ => {
            if n < 9 { return Err(DecodeError::BufferTooShort); }
            let a = [buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7], buf[8]];
            proof { lemma_be_padded(buf@, a@, 8); }
            (OFFSETS[8], be8(a), 9)
        },
    };
    proof {
        let t = (tag - 247) as nat;
        assert(consumed == t + 1);
        assert(off == offset(t));
        assert(payload == be_at(buf@, 1, t));
    }
    match off.checked_add(payload) {
        Some(value) => {
            proof { lemma_canonical(buf@); }
            Ok((value, consumed))
        },
        None => Err(DecodeError::Overflow),
    }
}

/// The byte the encoder extracts at position `k` of a tier-`t` payload.
pub open spec fn payload_byte(p: u64, t: u64, k: u64) -> u8 {
    (((p << (8 * (8 - t))) >> (56 - 8 * k)) & 0xff) as u8
}

/// `x`'s big-endian digits, one at a time.
pub proof fn lemma_be_bytes_index(x: nat, n: nat, k: int)
    requires 0 <= k < n,
    ensures
        be_bytes(x, n).len() == n,
        be_bytes(x, n)[k] == ((x / pow256((n - 1 - k) as nat)) % 256) as u8,
    decreases n,
{
    lemma_be_bytes_len(x, n);
    let m = (n - 1) as nat;
    if k < m {
        lemma_be_bytes_index(x / 256, m, k);
        let e = (m - 1 - k) as nat;
        assert(pow256((n - 1 - k) as nat) == 256 * pow256(e));
        lemma_pow256_positive(e);
        vstd::arithmetic::div_mod::lemma_div_denominator(x as int, 256, pow256(e) as int);
        assert(be_bytes(x, n)[k] == be_bytes(x / 256, m)[k]);
    } else {
        assert(pow256(0) == 1);
        assert(x / 1 == x);
    }
}

pub proof fn lemma_pow256_positive(e: nat)
    ensures pow256(e) > 0,
    decreases e,
{
    if e > 0 { lemma_pow256_positive((e - 1) as nat); }
}

/// Shifting a tier-`t` payload to the top of a `u64` and reading bytes
/// off the top yields its big-endian digits — the proof obligation behind
/// bijoux's `payload << (8 * (8 - tier))` + `to_be_bytes` trick.
pub proof fn lemma_payload_bytes(p: u64, t: u64)
    requires 1 <= t <= 8, (p as nat) < pow256(t as nat),
    ensures
        be_bytes(p as nat, t as nat).len() == t,
        forall|k: u64| k < t ==> #[trigger] payload_byte(p, t, k) == be_bytes(p as nat, t as nat)[k as int],
{
    lemma_constants();
    lemma_be_bytes_len(p as nat, t as nat);
    assert forall|k: u64| k < t implies #[trigger] payload_byte(p, t, k)
        == be_bytes(p as nat, t as nat)[k as int] by {
        lemma_be_bytes_index(p as nat, t as nat, k as int);
        if t == 1 && k == 0 {
            assert(((p << 56u64) >> 56u64) & 0xff == (p / 0x1) % 256) by (bit_vector) requires p < 0x100;
        } else if t == 2 && k == 0 {
            assert(((p << 48u64) >> 56u64) & 0xff == (p / 0x100) % 256) by (bit_vector) requires p < 0x10000;
        } else if t == 2 && k == 1 {
            assert(((p << 48u64) >> 48u64) & 0xff == (p / 0x1) % 256) by (bit_vector) requires p < 0x10000;
        } else if t == 3 && k == 0 {
            assert(((p << 40u64) >> 56u64) & 0xff == (p / 0x10000) % 256) by (bit_vector) requires p < 0x1000000;
        } else if t == 3 && k == 1 {
            assert(((p << 40u64) >> 48u64) & 0xff == (p / 0x100) % 256) by (bit_vector) requires p < 0x1000000;
        } else if t == 3 && k == 2 {
            assert(((p << 40u64) >> 40u64) & 0xff == (p / 0x1) % 256) by (bit_vector) requires p < 0x1000000;
        } else if t == 4 && k == 0 {
            assert(((p << 32u64) >> 56u64) & 0xff == (p / 0x1000000) % 256) by (bit_vector) requires p < 0x100000000;
        } else if t == 4 && k == 1 {
            assert(((p << 32u64) >> 48u64) & 0xff == (p / 0x10000) % 256) by (bit_vector) requires p < 0x100000000;
        } else if t == 4 && k == 2 {
            assert(((p << 32u64) >> 40u64) & 0xff == (p / 0x100) % 256) by (bit_vector) requires p < 0x100000000;
        } else if t == 4 && k == 3 {
            assert(((p << 32u64) >> 32u64) & 0xff == (p / 0x1) % 256) by (bit_vector) requires p < 0x100000000;
        } else if t == 5 && k == 0 {
            assert(((p << 24u64) >> 56u64) & 0xff == (p / 0x100000000) % 256) by (bit_vector) requires p < 0x10000000000;
        } else if t == 5 && k == 1 {
            assert(((p << 24u64) >> 48u64) & 0xff == (p / 0x1000000) % 256) by (bit_vector) requires p < 0x10000000000;
        } else if t == 5 && k == 2 {
            assert(((p << 24u64) >> 40u64) & 0xff == (p / 0x10000) % 256) by (bit_vector) requires p < 0x10000000000;
        } else if t == 5 && k == 3 {
            assert(((p << 24u64) >> 32u64) & 0xff == (p / 0x100) % 256) by (bit_vector) requires p < 0x10000000000;
        } else if t == 5 && k == 4 {
            assert(((p << 24u64) >> 24u64) & 0xff == (p / 0x1) % 256) by (bit_vector) requires p < 0x10000000000;
        } else if t == 6 && k == 0 {
            assert(((p << 16u64) >> 56u64) & 0xff == (p / 0x10000000000) % 256) by (bit_vector) requires p < 0x1000000000000;
        } else if t == 6 && k == 1 {
            assert(((p << 16u64) >> 48u64) & 0xff == (p / 0x100000000) % 256) by (bit_vector) requires p < 0x1000000000000;
        } else if t == 6 && k == 2 {
            assert(((p << 16u64) >> 40u64) & 0xff == (p / 0x1000000) % 256) by (bit_vector) requires p < 0x1000000000000;
        } else if t == 6 && k == 3 {
            assert(((p << 16u64) >> 32u64) & 0xff == (p / 0x10000) % 256) by (bit_vector) requires p < 0x1000000000000;
        } else if t == 6 && k == 4 {
            assert(((p << 16u64) >> 24u64) & 0xff == (p / 0x100) % 256) by (bit_vector) requires p < 0x1000000000000;
        } else if t == 6 && k == 5 {
            assert(((p << 16u64) >> 16u64) & 0xff == (p / 0x1) % 256) by (bit_vector) requires p < 0x1000000000000;
        } else if t == 7 && k == 0 {
            assert(((p << 8u64) >> 56u64) & 0xff == (p / 0x1000000000000) % 256) by (bit_vector) requires p < 0x100000000000000;
        } else if t == 7 && k == 1 {
            assert(((p << 8u64) >> 48u64) & 0xff == (p / 0x10000000000) % 256) by (bit_vector) requires p < 0x100000000000000;
        } else if t == 7 && k == 2 {
            assert(((p << 8u64) >> 40u64) & 0xff == (p / 0x100000000) % 256) by (bit_vector) requires p < 0x100000000000000;
        } else if t == 7 && k == 3 {
            assert(((p << 8u64) >> 32u64) & 0xff == (p / 0x1000000) % 256) by (bit_vector) requires p < 0x100000000000000;
        } else if t == 7 && k == 4 {
            assert(((p << 8u64) >> 24u64) & 0xff == (p / 0x10000) % 256) by (bit_vector) requires p < 0x100000000000000;
        } else if t == 7 && k == 5 {
            assert(((p << 8u64) >> 16u64) & 0xff == (p / 0x100) % 256) by (bit_vector) requires p < 0x100000000000000;
        } else if t == 7 && k == 6 {
            assert(((p << 8u64) >> 8u64) & 0xff == (p / 0x1) % 256) by (bit_vector) requires p < 0x100000000000000;
        } else if t == 8 && k == 0 {
            assert(((p << 0u64) >> 56u64) & 0xff == (p / 0x100000000000000) % 256) by (bit_vector) requires true;
        } else if t == 8 && k == 1 {
            assert(((p << 0u64) >> 48u64) & 0xff == (p / 0x1000000000000) % 256) by (bit_vector) requires true;
        } else if t == 8 && k == 2 {
            assert(((p << 0u64) >> 40u64) & 0xff == (p / 0x10000000000) % 256) by (bit_vector) requires true;
        } else if t == 8 && k == 3 {
            assert(((p << 0u64) >> 32u64) & 0xff == (p / 0x100000000) % 256) by (bit_vector) requires true;
        } else if t == 8 && k == 4 {
            assert(((p << 0u64) >> 24u64) & 0xff == (p / 0x1000000) % 256) by (bit_vector) requires true;
        } else if t == 8 && k == 5 {
            assert(((p << 0u64) >> 16u64) & 0xff == (p / 0x10000) % 256) by (bit_vector) requires true;
        } else if t == 8 && k == 6 {
            assert(((p << 0u64) >> 8u64) & 0xff == (p / 0x100) % 256) by (bit_vector) requires true;
        } else if t == 8 && k == 7 {
            assert(((p << 0u64) >> 0u64) & 0xff == (p / 0x1) % 256) by (bit_vector) requires true;
        }
    }
}

/// What `64 - leading_zeros` tells the encoder: the value's bit width.
pub proof fn lemma_bit_width(v: u64)
    requires v != 0,
    ensures
        0 <= vstd::std_specs::bits::u64_leading_zeros(v) < 64,
        v >= (1u64 << (63 - vstd::std_specs::bits::u64_leading_zeros(v)) as u64),
        vstd::std_specs::bits::u64_leading_zeros(v) > 0 ==> v < (1u64 << (64 - vstd::std_specs::bits::u64_leading_zeros(v)) as u64),
{
    vstd::std_specs::bits::axiom_u64_leading_zeros(v);
    let lz = vstd::std_specs::bits::u64_leading_zeros(v);
    let s = (63 - lz) as u64;
    assert((v >> s) & 1u64 != 0u64);
    assert((v >> s) & 1u64 != 0u64 && s < 64 ==> v >= (1u64 << s)) by (bit_vector);
    if lz > 0 {
        let w = (64 - lz) as u64;
        assert(v >> w == 0);
        assert(v >> w == 0 && w < 64 ==> v < (1u64 << w)) by (bit_vector);
    }
}

pub proof fn lemma_shl_mono(a: u64, b: u64)
    requires a <= b < 64,
    ensures (1u64 << a) <= (1u64 << b),
{
    assert(a <= b < 64 ==> (1u64 << a) <= (1u64 << b)) by (bit_vector);
}

pub proof fn lemma_shl_constants()
    ensures
        1u64 << 7u64 == 0x80,
        1u64 << 8u64 == 0x100,
        1u64 << 16u64 == 0x1_0000,
        1u64 << 24u64 == 0x100_0000,
        1u64 << 32u64 == 0x1_0000_0000,
        1u64 << 40u64 == 0x100_0000_0000,
        1u64 << 48u64 == 0x1_0000_0000_0000,
        1u64 << 56u64 == 0x100_0000_0000_0000,
{
    assert(1u64 << 7u64 == 0x80) by (bit_vector);
    assert(1u64 << 8u64 == 0x100) by (bit_vector);
    assert(1u64 << 16u64 == 0x1_0000) by (bit_vector);
    assert(1u64 << 24u64 == 0x100_0000) by (bit_vector);
    assert(1u64 << 32u64 == 0x1_0000_0000) by (bit_vector);
    assert(1u64 << 40u64 == 0x100_0000_0000) by (bit_vector);
    assert(1u64 << 48u64 == 0x1_0000_0000_0000) by (bit_vector);
    assert(1u64 << 56u64 == 0x100_0000_0000_0000) by (bit_vector);
}

pub proof fn lemma_encode_len(v: nat)
    ensures spec_encode(v).len() == tier(v) + 1,
{
    if tier(v) > 0 { lemma_be_bytes_len((v - offset(tier(v))) as nat, tier(v)); }
}

/// The tier the encoder derives from the bit width — one comparison
/// corrects the candidate `(bw - 1) / 8 + 1`.
proof fn lemma_candidate_tier(v: u64)
    requires v >= 248,
    ensures ({
        let c = ((64 - vstd::std_specs::bits::u64_leading_zeros(v)) - 1) / 8 + 1;
        &&& 1 <= c <= 8
        &&& v < BOUNDS[c - 1] ==> tier(v as nat) == c - 1
        &&& v >= BOUNDS[c - 1] ==> tier(v as nat) == c
    }),
{
    lemma_constants();
    lemma_shl_constants();
    lemma_bit_width(v);
    let bw = 64 - vstd::std_specs::bits::u64_leading_zeros(v);
    let c = (bw - 1) / 8 + 1;
    // 2^(8(c-1)) <= v < 2^(8c), and each tier's offset sits just above a
    // power of 256
    lemma_shl_mono((8 * (c - 1)) as u64, (bw - 1) as u64);
    if c < 8 {
        lemma_shl_mono(bw as u64, (8 * c) as u64);
    }
    assert(1 <= c <= 8);
    if c == 1 { assert(v < 0x100); }
    else if c == 2 { assert(0x100 <= v < 0x1_0000); }
    else if c == 3 { assert(0x1_0000 <= v < 0x100_0000); }
    else if c == 4 { assert(0x100_0000 <= v < 0x1_0000_0000); }
    else if c == 5 { assert(0x1_0000_0000 <= v < 0x100_0000_0000); }
    else if c == 6 { assert(0x100_0000_0000 <= v < 0x1_0000_0000_0000); }
    else if c == 7 { assert(0x1_0000_0000_0000 <= v < 0x100_0000_0000_0000); }
    else { assert(0x100_0000_0000_0000 <= v); }
}

// ── Encoding ────────────────────────────────────────────────────────────────

/// Returns the encoded length of `value` in bytes (1–9).
#[inline]
pub fn encoded_len(value: u64) -> (r: usize)
    ensures r == spec_encode(value as nat).len(),
{
    proof { lemma_constants(); lemma_encode_len(value as nat); }
    // Fast path: tier 0 values (0–247) need a single comparison.
    if value < BOUNDS[0] {
        return 1;
    }
    proof {
        lemma_candidate_tier(value);
        vstd::std_specs::bits::axiom_u64_leading_zeros(value);
    }
    // Derive the tier from the bit width, then correct with one comparison.
    let bw = 64 - value.leading_zeros(); // 8..=64 here
    let candidate = ((bw - 1) / 8 + 2) as usize;
    if value < BOUNDS[candidate - 2] {
        candidate - 1
    } else {
        candidate
    }
}

/// A stack-allocated bijou64 encoding, carrying its own length.
#[derive(Debug, Clone, Copy)]
pub struct EncodedU64 {
    buf: [u8; MAX_BYTES],
    len: u8,
}

impl View for EncodedU64 {
    type V = Seq<u8>;

    closed spec fn view(&self) -> Seq<u8> {
        self.buf@.subrange(0, self.len as int)
    }
}

impl EncodedU64 {
    #[verifier::type_invariant]
    spec fn wf(self) -> bool {
        1 <= self.len <= MAX_BYTES
    }

    /// The encoded bytes.
    #[inline]
    pub fn as_slice(&self) -> (r: &[u8])
        ensures r@ == self@,
    {
        proof { use_type_invariant(self); }
        self.buf.as_slice().split_at(self.len as usize).0
    }
}

/// Encodes `value` as a stack-allocated [`EncodedU64`].
#[inline]
pub fn encoded_bytes(value: u64) -> (r: EncodedU64)
    ensures r@ == spec_encode(value as nat),
{
    proof { lemma_constants(); lemma_encode_len(value as nat); }
    if value < BOUNDS[0] {
        let r = EncodedU64 { buf: [(value & 0xFF) as u8, 0, 0, 0, 0, 0, 0, 0, 0], len: 1 };
        assert(value & 0xFF == value) by (bit_vector) requires value < 248;
        assert(r@ =~= spec_encode(value as nat));
        return r;
    }
    proof {
        lemma_candidate_tier(value);
        vstd::std_specs::bits::axiom_u64_leading_zeros(value);
        lemma_tier_bounds(value as nat);
    }
    let bw = 64 - value.leading_zeros();
    let mut tier = ((bw - 1) / 8 + 1) as usize;
    if value < BOUNDS[tier - 1] {
        tier -= 1;
    }

    // Shift the payload so its `tier` bytes sit at the top of a u64, then
    // read bytes off the top: LLVM compiles this to one bswap + store.
    let tag = (TAG_THRESHOLD as usize + tier - 1) as u8;
    let p = value - OFFSETS[tier];
    let payload = p << (8 * (NUM_TIERS - tier)) as u64;
    let r = EncodedU64 {
        buf: [
            tag,
            ((payload >> 56) & 0xff) as u8,
            ((payload >> 48) & 0xff) as u8,
            ((payload >> 40) & 0xff) as u8,
            ((payload >> 32) & 0xff) as u8,
            ((payload >> 24) & 0xff) as u8,
            ((payload >> 16) & 0xff) as u8,
            ((payload >> 8) & 0xff) as u8,
            (payload & 0xff) as u8,
        ],
        len: (tier + 1) as u8,
    };
    proof {
        let t = tier as u64;
        lemma_payload_bytes(p, t);
        assert(payload == p << (8 * (8 - t)));
        assert forall|k: u64| k < 8 implies #[trigger] r.buf@[k as int + 1] == payload_byte(p, t, k) by {
            assert((payload >> 0u64) == payload) by (bit_vector);
        }
        assert forall|i: int| 0 <= i < r@.len() implies r@[i] == spec_encode(value as nat)[i] by {
            if i > 0 {
                assert(payload_byte(p, t, (i - 1) as u64) == be_bytes(p as nat, t as nat)[i - 1]);
            }
        }
        assert(r@ =~= spec_encode(value as nat));
    }
    r
}

/// The length of the encoding at the front of `data`, from its tag byte
/// alone, or `None` if `data` is too short for it. Agrees with
/// [`decode`] on every input whose value fits a `u64`; for skipping over
/// already-validated bytes.
///
/// `1 + (b − 0xF7)` saturating: tags below `0xF8` are single-byte values,
/// tag `0xF7 + k` carries `k` payload bytes. Pure ALU on the loaded byte —
/// no table, no branch — since it sits in the loop-carried chain of
/// every skip.
#[inline(always)]
pub fn encoded_len_at(data: &[u8]) -> (r: Option<usize>)
    ensures
        r == (match spec_decode(data@) {
            Some((_, n)) => Some(n as usize),
            None => None::<usize>,
        }),
{
    let b = match data.split_first() {
        Some((&b, _)) => b,
        None => return None,
    };
    let len = 1 + b.saturating_sub(0xF7) as usize;
    if data.len() >= len { Some(len) } else { None }
}

// ── bijou64s: zigzag over bijou64 ───────────────────────────────────────────

/// Zigzag: 0, -1, 1, -2, 2, … ↦ 0, 1, 2, 3, 4, …
pub open spec fn spec_zigzag(n: int) -> nat {
    if n >= 0 { (2 * n) as nat } else { (-2 * n - 1) as nat }
}

/// The bijou64s encoding of `n`.
pub open spec fn spec_encode_signed(n: int) -> Seq<u8> {
    spec_encode(spec_zigzag(n))
}

/// Zigzag is injective, so the signed format inherits canonicality: every
/// `i64` has exactly one encoding.
pub proof fn lemma_zigzag_injective(a: int, b: int)
    requires spec_zigzag(a) == spec_zigzag(b),
    ensures a == b,
{
}

#[inline]
pub fn zigzag(n: i64) -> (r: u64)
    ensures r as nat == spec_zigzag(n as int),
{
    // n ≥ 0 ↦ 2n; n < 0 ↦ 2·!n + 1, where !n = −(n + 1) ≥ 0
    assert(n >= 0 ==> #[verifier::truncate] (((n << 1) ^ (n >> 63)) as u64) == (n as u64) * 2)
        by (bit_vector);
    assert(n < 0 ==> #[verifier::truncate] (((n << 1) ^ (n >> 63)) as u64)
        == ((!n) as u64) * 2 + 1) by (bit_vector);
    assert(n < 0 ==> !n >= 0 && (!n) + n == -1) by (bit_vector);
    // (no `#[verifier::truncate]` here: stable rustc rejects attributes on
    // expressions that survive erasure; Verus only notes the wrapping cast)
    ((n << 1) ^ (n >> 63)) as u64
}

#[inline]
pub fn unzigzag(u: u64) -> (r: i64)
    ensures spec_zigzag(r as int) == u as nat,
{
    assert(u >> 1 <= 0x7FFF_FFFF_FFFF_FFFFu64 && u & 1 <= 1) by (bit_vector);
    assert(u >> 1 == u / 2 && u & 1 == u % 2) by (bit_vector);
    let h = (u >> 1) as i64;
    let m = -((u & 1) as i64);
    // m is 0 or −1, so h ^ m is h or !h = −h − 1
    assert(h ^ 0i64 == h) by (bit_vector);
    assert(h >= 0 ==> (h ^ -1i64) >= -0x8000_0000_0000_0000i64 && (h ^ -1i64) + h == -1)
        by (bit_vector);
    h ^ m
}

/// Returns the encoded length of `value` in bytes (1–9).
#[inline]
pub fn encoded_len_signed(value: i64) -> (r: usize)
    ensures r == spec_encode_signed(value as int).len(),
{
    encoded_len(zigzag(value))
}

/// Encodes `value` as a stack-allocated [`EncodedU64`].
#[inline]
pub fn encoded_bytes_signed(value: i64) -> (r: EncodedU64)
    ensures r@ == spec_encode_signed(value as int),
{
    encoded_bytes(zigzag(value))
}

/// Decodes a bijou64s from the front of `buf`, returning
/// `(value, bytes_consumed)`.
#[inline]
pub fn decode_signed(buf: &[u8]) -> (r: Result<(i64, usize), DecodeError>)
    ensures
        match r {
            Ok((v, n)) => spec_decode(buf@) == Some((spec_zigzag(v as int), n as nat))
                && buf@.subrange(0, n as int) == spec_encode_signed(v as int),
            Err(DecodeError::BufferTooShort) => spec_decode(buf@) is None,
            Err(DecodeError::Overflow) => spec_decode(buf@) is Some
                && spec_decode(buf@).unwrap().0 > u64::MAX,
        },
{
    match decode(buf) {
        Ok((value, consumed)) => Ok((unzigzag(value), consumed)),
        Err(err) => Err(err),
    }
}

} // verus!

impl core::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(match self {
            DecodeError::BufferTooShort => "buffer too short for bijou64 encoding",
            DecodeError::Overflow => "bijou64 tier 8 payload overflows u64",
        })
    }
}
