//! Verified canonical LEB128 codec.
//!
//! Unsigned and signed LEB128 as hexane writes them, with a decoder that
//! accepts only *canonical* encodings: the shortest one for each value.
//! LEB128 itself admits redundant forms — `0x80 0x00` also spells 0, and
//! `0x7f` as an unsigned value can be padded to `0xff 0x00` — and a decoder
//! that accepted them would let two byte strings stand for one column, so
//! re-encoding loaded data would not reproduce its bytes (or its hash).
//!
//! Checked with [Verus](https://github.com/verus-lang/verus). Nothing
//! about the codec is assumed; what the proofs trust is Verus and Z3, and
//! `vstd`'s specifications of the std operations used here: indexing,
//! `leading_zeros` and `split_at`.
//!
//! The format is written as mathematics over unbounded integers:
//!
//! * [`spec_encode`] / [`spec_encode_signed`] — seven bits per byte, least
//!   significant first, high bit set on every byte but the last, stopping
//!   as soon as the rest of the value fits in the final byte;
//! * [`lemma_unique`] / [`lemma_unique_signed`]: two encodings that are
//!   both prefixes of one byte string are the same value's, so a byte
//!   string starts with at most one encoding.
//!
//! [`decode`] and [`decode_signed`] are proved to succeed exactly when the
//! input starts with the encoding of some `u64` / `i64`, returning that
//! value and length; every other input is an error. The encoders and
//! length functions are proved to produce [`spec_encode`] /
//! [`spec_encode_signed`] and their lengths.
//!
//! Outside verification, `verus!` erases every spec and proof, leaving
//! plain Rust. To re-check the proofs, run Verus on this file alone:
//!
//! ```text
//! verus --crate-type=lib src/leb128_codec.rs
//! ```

#[allow(unused_imports)]
use vstd::prelude::*;

verus! {

// ── The format, as mathematics ──────────────────────────────────────────────

/// The canonical unsigned LEB128 encoding of `v`.
pub open spec fn spec_encode(v: nat) -> Seq<u8>
    decreases v,
{
    if v < 128 {
        seq![v as u8]
    } else {
        seq![(v % 128 + 128) as u8] + spec_encode(v / 128)
    }
}

/// The canonical signed LEB128 encoding of `v`: the same 7-bit groups of
/// `v` in two's complement (`%` and `/` round towards −∞), ending as soon
/// as what is left is in `-64..64`, where the final byte's bit 6 can carry
/// the sign.
pub open spec fn spec_encode_signed(v: int) -> Seq<u8>
    decreases (if v < 0 { -v } else { v }),
{
    if -64 <= v < 64 {
        seq![(v % 128) as u8]
    } else {
        seq![(v % 128 + 128) as u8] + spec_encode_signed(v / 128)
    }
}

// ── Proof vocabulary ────────────────────────────────────────────────────────

pub open spec fn pow128(e: nat) -> nat
    decreases e,
{
    if e == 0 { 1 } else { 128 * pow128((e - 1) as nat) }
}

/// The number the 7-bit groups of `s` stand for, least significant first.
pub open spec fn groups(s: Seq<u8>) -> nat
    decreases s.len(),
{
    if s.len() == 0 { 0 } else { (s[0] % 128) as nat + 128 * groups(s.drop_first()) }
}

/// A final byte's group as a 7-bit two's complement number.
pub open spec fn signed_group(b: u8) -> int {
    if b % 128 >= 64 { b % 128 - 128 } else { (b % 128) as int }
}

/// The groups of `s` with the last one sign-extended.
pub open spec fn groups_signed(s: Seq<u8>) -> int
    decreases s.len(),
{
    if s.len() == 0 {
        0
    } else if s.len() == 1 {
        signed_group(s[0])
    } else {
        (s[0] % 128) + 128 * groups_signed(s.drop_first())
    }
}

/// The continuation bit is set on every byte but the last.
pub open spec fn framed(s: Seq<u8>) -> bool {
    &&& s.len() >= 1
    &&& forall|j: int| 0 <= j < s.len() - 1 ==> #[trigger] s[j] >= 128
    &&& s.last() < 128
}

/// No redundant final byte: a zero group adds nothing to an unsigned value.
pub open spec fn minimal(s: Seq<u8>) -> bool {
    s.len() == 1 || s.last() != 0
}

/// No redundant final byte: `0x00` after a group whose bit 6 is clear, or
/// `0x7f` after one whose bit 6 is set, only repeats the sign.
pub open spec fn minimal_signed(s: Seq<u8>) -> bool {
    s.len() == 1 || {
        let p = s[s.len() - 2] % 128;
        !(s.last() == 0 && p < 64) && !(s.last() == 0x7f && p >= 64)
    }
}

// ── Facts about the definitions ─────────────────────────────────────────────

pub proof fn lemma_pow128_constants()
    ensures
        pow128(0) == 1,
        pow128(1) == 0x80,
        pow128(2) == 0x4000,
        pow128(3) == 0x20_0000,
        pow128(4) == 0x1000_0000,
        pow128(5) == 0x8_0000_0000,
        pow128(6) == 0x400_0000_0000,
        pow128(7) == 0x2_0000_0000_0000,
        pow128(8) == 0x100_0000_0000_0000,
        pow128(9) == 0x8000_0000_0000_0000,
        pow128(10) == 0x40_0000_0000_0000_0000,
{
    reveal_with_fuel(pow128, 11);
}

pub proof fn lemma_pow128_positive(e: nat)
    ensures pow128(e) >= 1,
    decreases e,
{
    if e > 0 { lemma_pow128_positive((e - 1) as nat); }
}

/// Appending a byte adds its group at the next power of 128.
pub proof fn lemma_groups_push(s: Seq<u8>, b: u8)
    ensures groups(s.push(b)) == groups(s) + (b % 128) as nat * pow128(s.len()),
    decreases s.len(),
{
    let t = s.push(b);
    if s.len() == 0 {
        reveal_with_fuel(groups, 2);
        assert(t.drop_first() =~= Seq::<u8>::empty());
        assert(t[0] == b);
        assert(groups(t) == (b % 128) as nat);
        assert(pow128(0) == 1);
        assert(groups(s) == 0);
    } else {
        assert(t[0] == s[0]);
        assert(t.drop_first() =~= s.drop_first().push(b));
        lemma_groups_push(s.drop_first(), b);
        let x = (b % 128) as nat;
        let p = pow128((s.len() - 1) as nat);
        assert(128 * (groups(s.drop_first()) + x * p) == 128 * groups(s.drop_first()) + x * (128 * p))
            by (nonlinear_arith);
        assert(groups(t) == (t[0] % 128) as nat + 128 * groups(t.drop_first()));
        assert(groups(t.drop_first()) == groups(s.drop_first()) + x * p);
        assert(groups(s) == (s[0] % 128) as nat + 128 * groups(s.drop_first()));
        assert(pow128(s.len()) == 128 * p);
    }
}

/// Appending a final byte adds its sign-extended group.
pub proof fn lemma_groups_signed_push(s: Seq<u8>, b: u8)
    ensures groups_signed(s.push(b)) == groups(s) + signed_group(b) * pow128(s.len()),
    decreases s.len(),
{
    let t = s.push(b);
    if s.len() == 0 {
        assert(t[0] == b);
        assert(groups(s) == 0);
        assert(groups_signed(t) == signed_group(b));
        assert(pow128(0) == 1);
    } else {
        assert(t[0] == s[0]);
        assert(t.drop_first() =~= s.drop_first().push(b));
        lemma_groups_signed_push(s.drop_first(), b);
        let x = signed_group(b);
        let p = pow128((s.len() - 1) as nat);
        assert(128 * (groups(s.drop_first()) + x * p) == 128 * groups(s.drop_first()) + x * (128 * p))
            by (nonlinear_arith);
        assert(groups_signed(t) == (t[0] % 128) as int + 128 * groups_signed(t.drop_first()));
        assert(groups(s) == (s[0] % 128) as nat + 128 * groups(s.drop_first()));
        assert(pow128(s.len()) == 128 * p);
    }
}

pub proof fn lemma_groups_bound(s: Seq<u8>)
    ensures groups(s) < pow128(s.len()),
    decreases s.len(),
{
    if s.len() > 0 {
        lemma_groups_bound(s.drop_first());
    }
}

/// Every encoding is framed, minimal, and spells its value.
pub proof fn lemma_encode_shape(v: nat)
    ensures
        framed(spec_encode(v)),
        minimal(spec_encode(v)),
        groups(spec_encode(v)) == v,
    decreases v,
{
    let e = spec_encode(v);
    if v < 128 {
        reveal_with_fuel(groups, 2);
        assert(e.drop_first() =~= Seq::<u8>::empty());
    } else {
        let t = spec_encode(v / 128);
        lemma_encode_shape(v / 128);
        vstd::arithmetic::div_mod::lemma_fundamental_div_mod(v as int, 128);
        assert(e[0] % 128 == v % 128);
        assert(e.drop_first() =~= t);
        assert forall|j: int| 0 <= j < e.len() - 1 implies #[trigger] e[j] >= 128 by {
            if j > 0 { assert(e[j] == t[j - 1]); }
        }
        if t.len() == 1 {
            assert(t[0] == (v / 128) as u8);
        }
    }
}

/// The converse: a framed, minimal byte string is the encoding of the
/// number it spells.
pub proof fn lemma_shape_encode(s: Seq<u8>)
    requires framed(s), minimal(s),
    ensures s == spec_encode(groups(s)),
    decreases s.len(),
{
    if s.len() == 1 {
        reveal_with_fuel(groups, 2);
        assert(s.drop_first() =~= Seq::<u8>::empty());
        assert(s =~= seq![s[0]]);
    } else {
        let t = s.drop_first();
        assert forall|j: int| 0 <= j < t.len() - 1 implies #[trigger] t[j] >= 128 by {
            assert(t[j] == s[j + 1]);
        }
        assert(t.last() == s.last());
        lemma_shape_encode(t);
        let w = groups(t);
        if w == 0 {
            // t would be [0], so s ends in a redundant zero byte
            assert(t.len() == 1);
            assert(false);
        }
        let v = groups(s);
        vstd::arithmetic::div_mod::lemma_fundamental_div_mod_converse(
            v as int, 128, w as int, (s[0] % 128) as int);
        assert(s =~= seq![s[0]] + t);
    }
}

/// Every signed encoding is framed, minimal, and spells its value.
pub proof fn lemma_encode_signed_shape(v: int)
    ensures
        framed(spec_encode_signed(v)),
        minimal_signed(spec_encode_signed(v)),
        groups_signed(spec_encode_signed(v)) == v,
    decreases (if v < 0 { -v } else { v }),
{
    let e = spec_encode_signed(v);
    if !(-64 <= v < 64) {
        let t = spec_encode_signed(v / 128);
        lemma_encode_signed_shape(v / 128);
        vstd::arithmetic::div_mod::lemma_fundamental_div_mod(v, 128);
        assert(e[0] % 128 == v % 128);
        assert(e.drop_first() =~= t);
        assert forall|j: int| 0 <= j < e.len() - 1 implies #[trigger] e[j] >= 128 by {
            if j > 0 { assert(e[j] == t[j - 1]); }
        }
        if t.len() == 1 {
            assert(e[e.len() - 2] == e[0]);
        } else {
            assert(e[e.len() - 2] == t[t.len() - 2]);
        }
    }
}

/// The converse for signed encodings.
pub proof fn lemma_shape_encode_signed(s: Seq<u8>)
    requires framed(s), minimal_signed(s),
    ensures s == spec_encode_signed(groups_signed(s)),
    decreases s.len(),
{
    if s.len() == 1 {
        assert(s =~= seq![s[0]]);
    } else {
        let t = s.drop_first();
        assert forall|j: int| 0 <= j < t.len() - 1 implies #[trigger] t[j] >= 128 by {
            assert(t[j] == s[j + 1]);
        }
        if t.len() > 1 {
            assert(t[t.len() - 2] == s[s.len() - 2]);
        }
        assert(t.last() == s.last());
        lemma_shape_encode_signed(t);
        let w = groups_signed(t);
        let v = groups_signed(s);
        vstd::arithmetic::div_mod::lemma_fundamental_div_mod_converse(
            v, 128, w, (s[0] % 128) as int);
        if -64 <= v < 64 {
            // then w is 0 or -1, whose encodings are one byte that only
            // repeats the sign of s[0]
            assert(w == 0 || w == -1);
            assert(t.len() == 1);
            assert(false);
        }
        assert(s =~= seq![s[0]] + t);
    }
}

/// How many bytes an unsigned value needs.
pub proof fn lemma_len(v: nat, k: nat)
    requires k >= 1,
    ensures (spec_encode(v).len() <= k) == (v < pow128(k)),
    decreases k,
{
    reveal_with_fuel(pow128, 2);
    vstd::arithmetic::div_mod::lemma_fundamental_div_mod(v as int, 128);
    if k > 1 {
        lemma_pow128_positive((k - 1) as nat);
        if v >= 128 { lemma_len(v / 128, (k - 1) as nat); }
    }
}

/// How many bytes a signed value needs.
pub proof fn lemma_len_signed(v: int, k: nat)
    requires k >= 1,
    ensures
        (spec_encode_signed(v).len() <= k)
            == (-64 * pow128((k - 1) as nat) <= v < 64 * pow128((k - 1) as nat)),
    decreases k,
{
    reveal_with_fuel(pow128, 2);
    vstd::arithmetic::div_mod::lemma_fundamental_div_mod(v, 128);
    if k > 1 {
        lemma_pow128_positive((k - 2) as nat);
        if !(-64 <= v < 64) { lemma_len_signed(v / 128, (k - 1) as nat); }
    }
}

/// Two framed prefixes of one byte string are the same prefix: each ends
/// at the string's first byte without a continuation bit.
pub proof fn lemma_framed_prefix(p: Seq<u8>, q: Seq<u8>, s: Seq<u8>)
    requires framed(p), framed(q), p.is_prefix_of(s), q.is_prefix_of(s),
    ensures p == q,
{
    if p.len() < q.len() {
        assert(q[p.len() - 1] == s[p.len() - 1]);
    } else if q.len() < p.len() {
        assert(p[q.len() - 1] == s[q.len() - 1]);
    }
    assert(p =~= q);
}

/// Unique decoding: a byte string starts with at most one encoding.
pub proof fn lemma_unique(v: nat, w: nat, s: Seq<u8>)
    requires spec_encode(v).is_prefix_of(s), spec_encode(w).is_prefix_of(s),
    ensures v == w,
{
    lemma_encode_shape(v);
    lemma_encode_shape(w);
    lemma_framed_prefix(spec_encode(v), spec_encode(w), s);
}

/// Unique decoding of signed encodings.
pub proof fn lemma_unique_signed(v: int, w: int, s: Seq<u8>)
    requires spec_encode_signed(v).is_prefix_of(s), spec_encode_signed(w).is_prefix_of(s),
    ensures v == w,
{
    lemma_encode_signed_shape(v);
    lemma_encode_signed_shape(w);
    lemma_framed_prefix(spec_encode_signed(v), spec_encode_signed(w), s);
}

/// Where a framed prefix of `s` ends, given `s`'s first `i` bytes carry
/// continuation bits: past them, and at byte `i` if that one doesn't.
proof fn lemma_framed_end(p: Seq<u8>, s: Seq<u8>, i: int)
    requires
        framed(p), p.is_prefix_of(s), 0 <= i <= s.len(),
        forall|j: int| 0 <= j < i ==> #[trigger] s[j] >= 128,
    ensures
        p.len() > i,
        i < s.len() && s[i] < 128 ==> p == s.subrange(0, i + 1),
{
    assert(p[p.len() - 1] == s[p.len() - 1]);
    if i < s.len() && s[i] < 128 && p.len() > i + 1 {
        assert(p[i] == s[i]);
    }
}

/// A `u64` needs at most ten bytes, the tenth being `0x01`.
proof fn lemma_u64_len(v: nat)
    requires v <= u64::MAX,
    ensures
        spec_encode(v).len() <= 10,
        spec_encode(v).len() == 10 ==> spec_encode(v)[9] == 1,
{
    let e = spec_encode(v);
    lemma_pow128_constants();
    lemma_len(v, 10);
    if e.len() == 10 {
        lemma_encode_shape(v);
        assert(e =~= e.subrange(0, 9).push(e[9]));
        lemma_groups_push(e.subrange(0, 9), e[9]);
    }
}

/// An `i64` needs at most ten bytes, the tenth being `0x00` or `0x7f`.
proof fn lemma_i64_len(v: int)
    requires i64::MIN <= v <= i64::MAX,
    ensures
        spec_encode_signed(v).len() <= 10,
        spec_encode_signed(v).len() == 10 ==> spec_encode_signed(v)[9] == 0
            || spec_encode_signed(v)[9] == 0x7f,
{
    let e = spec_encode_signed(v);
    lemma_pow128_constants();
    lemma_len_signed(v, 10);
    if e.len() == 10 {
        lemma_encode_signed_shape(v);
        assert(e =~= e.subrange(0, 9).push(e[9]));
        lemma_groups_signed_push(e.subrange(0, 9), e[9]);
        lemma_groups_bound(e.subrange(0, 9));
    }
}

// ── Decoding ────────────────────────────────────────────────────────────────

/// Why a byte string does not start with a canonical LEB128 value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeError {
    /// The input ends inside an encoding.
    Truncated,
    /// The encoding has a redundant final byte.
    Overlong,
    /// The value does not fit in 64 bits.
    Overflow,
}

/// Maximum number of bytes a 64-bit LEB128 encoding can occupy.
pub const MAX_BYTES: usize = 10;

/// The unsigned decoder's contract.
pub open spec fn decode_ok(buf: Seq<u8>, r: Result<(u64, usize), DecodeError>) -> bool {
    match r {
        Ok((v, n)) => n <= buf.len() && buf.subrange(0, n as int) == spec_encode(v as nat),
        Err(e) => {
            &&& forall|v: nat| v <= u64::MAX ==> !(#[trigger] spec_encode(v)).is_prefix_of(buf)
            &&& e == DecodeError::Truncated ==> buf.len() < MAX_BYTES
                && forall|j: int| 0 <= j < buf.len() ==> #[trigger] buf[j] >= 128
        },
    }
}

/// The signed decoder's contract.
pub open spec fn decode_signed_ok(buf: Seq<u8>, r: Result<(i64, usize), DecodeError>) -> bool {
    match r {
        Ok((v, n)) => n <= buf.len() && buf.subrange(0, n as int) == spec_encode_signed(v as int),
        Err(e) => {
            &&& forall|v: int| i64::MIN <= v <= i64::MAX
                ==> !(#[trigger] spec_encode_signed(v)).is_prefix_of(buf)
            &&& e == DecodeError::Truncated ==> buf.len() < MAX_BYTES
                && forall|j: int| 0 <= j < buf.len() ==> #[trigger] buf[j] >= 128
        },
    }
}

/// `1 << 7i` is `128^i`.
proof fn lemma_shift(i: u64)
    requires i <= 9,
    ensures (1u64 << (7 * i)) as nat == pow128(i as nat),
{
    lemma_pow128_constants();
    assert(1u64 << 0u64 == 1) by (bit_vector);
    assert(1u64 << 7u64 == 0x80) by (bit_vector);
    assert(1u64 << 14u64 == 0x4000) by (bit_vector);
    assert(1u64 << 21u64 == 0x20_0000) by (bit_vector);
    assert(1u64 << 28u64 == 0x1000_0000) by (bit_vector);
    assert(1u64 << 35u64 == 0x8_0000_0000) by (bit_vector);
    assert(1u64 << 42u64 == 0x400_0000_0000) by (bit_vector);
    assert(1u64 << 49u64 == 0x2_0000_0000_0000) by (bit_vector);
    assert(1u64 << 56u64 == 0x100_0000_0000_0000) by (bit_vector);
    assert(1u64 << 63u64 == 0x8000_0000_0000_0000) by (bit_vector);
}

/// Adding the group at `shift` to an accumulator below `1 << shift`.
proof fn lemma_accumulate(acc: u64, b: u8, shift: u64)
    requires shift <= 56, acc < (1u64 << shift),
    ensures
        (acc | ((b & 0x7f) as u64) << shift) == acc + (b % 128) as nat * (1u64 << shift),
        (acc | ((b & 0x7f) as u64) << shift) < (1u64 << (shift + 7)),
{
    let x = (b & 0x7f) as u64;
    assert(b & 0x7f == b % 128) by (bit_vector);
    assert(x < 128 && shift <= 56 && acc < (1u64 << shift) ==> {
        &&& (acc | (x << shift)) == acc + (x << shift)
        &&& (x << shift) == x * (1u64 << shift)
        &&& acc + (x << shift) < (1u64 << (shift + 7))
    }) by (bit_vector);
}

/// `-1 << 7i` is `-(128^i)`.
proof fn lemma_neg_shift(i: u64)
    requires 1 <= i <= 9,
    ensures (-1i64 << (7 * i)) as int == -pow128(i as nat),
{
    lemma_pow128_constants();
    assert(-1i64 << 7u64 == -0x80) by (bit_vector);
    assert(-1i64 << 14u64 == -0x4000) by (bit_vector);
    assert(-1i64 << 21u64 == -0x20_0000) by (bit_vector);
    assert(-1i64 << 28u64 == -0x1000_0000) by (bit_vector);
    assert(-1i64 << 35u64 == -0x8_0000_0000) by (bit_vector);
    assert(-1i64 << 42u64 == -0x400_0000_0000) by (bit_vector);
    assert(-1i64 << 49u64 == -0x2_0000_0000_0000) by (bit_vector);
    assert(-1i64 << 56u64 == -0x100_0000_0000_0000) by (bit_vector);
    assert(-1i64 << 63u64 == -0x8000_0000_0000_0000) by (bit_vector);
}

/// An accumulator below `1 << k` fits an `i64`, and or-ing in `-1 << k`
/// sign-extends it from bit `k - 1`.
proof fn lemma_sign_extend(r: u64, k: u64)
    requires 7 <= k <= 63, r < (1u64 << k),
    ensures
        r as i64 == r,
        ((r as i64) | (-1i64 << k)) == r + (-1i64 << k),
{
    assert(7 <= k <= 63 && r < (1u64 << k) ==> r < 0x8000_0000_0000_0000u64) by (bit_vector);
    let x = r as i64;
    assert(7 <= k <= 63 && r < (1u64 << k) && r < 0x8000_0000_0000_0000u64
        ==> (#[verifier::truncate] (r as i64)) >> k == 0) by (bit_vector);
    assert(7 <= k <= 63 && x >= 0 && x >> k == 0 ==> (x | (-1i64 << k)) == x + (-1i64 << k))
        by (bit_vector);
}

/// Decodes a canonical unsigned LEB128 value from the front of `buf`,
/// returning `(value, bytes_consumed)`.
#[inline]
pub fn decode(buf: &[u8]) -> (r: Result<(u64, usize), DecodeError>)
    ensures decode_ok(buf@, r),
{
    let mut result: u64 = 0;
    let mut i: usize = 0;
    proof { lemma_shift(0); }
    loop
        invariant
            i <= 9, i <= buf.len(),
            forall|j: int| 0 <= j < i ==> #[trigger] buf@[j] >= 128,
            result as nat == groups(buf@.subrange(0, i as int)),
            result < (1u64 << (7 * i) as u64),
        decreases 10 - i,
    {
        proof { lemma_shift(i as u64); }
        if i == buf.len() {
            proof {
                assert forall|v: nat| v <= u64::MAX implies !(#[trigger] spec_encode(v)).is_prefix_of(buf@) by {
                    if spec_encode(v).is_prefix_of(buf@) {
                        lemma_encode_shape(v);
                        lemma_framed_end(spec_encode(v), buf@, i as int);
                    }
                }
            }
            return Err(DecodeError::Truncated);
        }
        let b = buf[i];
        let ghost prefix = buf@.subrange(0, i as int);
        proof {
            assert(buf@.subrange(0, i + 1) =~= prefix.push(b));
            lemma_groups_push(prefix, b);
        }
        if i == 9 {
            // Only bit 63 is left, so the last byte must be exactly 1.
            if b != 1 {
                proof {
                    assert forall|v: nat| v <= u64::MAX implies !(#[trigger] spec_encode(v)).is_prefix_of(buf@) by {
                        if spec_encode(v).is_prefix_of(buf@) {
                            lemma_encode_shape(v);
                            lemma_u64_len(v);
                            lemma_framed_end(spec_encode(v), buf@, 9);
                            assert(spec_encode(v)[9] == buf@[9]);
                        }
                    }
                }
                return Err(if b == 0 { DecodeError::Overlong } else { DecodeError::Overflow });
            }
            proof {
                lemma_pow128_constants();
                assert(result | 1u64 << 63u64 == result + 0x8000_0000_0000_0000)
                    by (bit_vector) requires result < 0x8000_0000_0000_0000u64;
                lemma_shape_encode(buf@.subrange(0, 10));
            }
            return Ok((result | 1u64 << 63u64, 10));
        }
        let shift = (7 * i) as u64;
        proof { lemma_accumulate(result, b, shift); lemma_shift((i + 1) as u64); }
        result |= ((b & 0x7f) as u64) << shift;
        if b < 0x80 {
            if b == 0 && i > 0 {
                proof {
                    assert forall|v: nat| v <= u64::MAX implies !(#[trigger] spec_encode(v)).is_prefix_of(buf@) by {
                        if spec_encode(v).is_prefix_of(buf@) {
                            lemma_encode_shape(v);
                            lemma_framed_end(spec_encode(v), buf@, i as int);
                        }
                    }
                }
                return Err(DecodeError::Overlong);
            }
            proof { lemma_shape_encode(buf@.subrange(0, i + 1)); }
            return Ok((result, i + 1));
        }
        i += 1;
    }
}

/// Decodes a canonical signed LEB128 value from the front of `buf`,
/// returning `(value, bytes_consumed)`.
#[inline]
pub fn decode_signed(buf: &[u8]) -> (r: Result<(i64, usize), DecodeError>)
    ensures decode_signed_ok(buf@, r),
{
    let mut result: u64 = 0;
    let mut i: usize = 0;
    // the previous byte, whose bit 6 a redundant final byte would repeat
    let mut prev: u8 = 0;
    proof { lemma_shift(0); }
    loop
        invariant
            i <= 9, i <= buf.len(),
            i > 0 ==> prev == buf@[i - 1],
            forall|j: int| 0 <= j < i ==> #[trigger] buf@[j] >= 128,
            result as nat == groups(buf@.subrange(0, i as int)),
            result < (1u64 << (7 * i) as u64),
        decreases 10 - i,
    {
        proof { lemma_shift(i as u64); }
        if i == buf.len() {
            proof {
                assert forall|v: int| i64::MIN <= v <= i64::MAX
                    implies !(#[trigger] spec_encode_signed(v)).is_prefix_of(buf@) by {
                    if spec_encode_signed(v).is_prefix_of(buf@) {
                        lemma_encode_signed_shape(v);
                        lemma_framed_end(spec_encode_signed(v), buf@, i as int);
                    }
                }
            }
            return Err(DecodeError::Truncated);
        }
        let b = buf[i];
        let ghost prefix = buf@.subrange(0, i as int);
        proof {
            assert(buf@.subrange(0, i + 1) =~= prefix.push(b));
            lemma_groups_push(prefix, b);
            lemma_groups_signed_push(prefix, b);
        }
        proof {
            assert(b & 0x7f == b % 128) by (bit_vector);
            assert((prev & 0x40 == 0) == (prev % 128 < 64)) by (bit_vector);
        }
        if i == 9 {
            // Bits 63..70 are left, so the last byte must be all sign:
            // 0x00 or 0x7f.
            // A final byte only repeating the previous group's sign bit.
            let redundant = b == (if prev & 0x40 != 0 { 0x7f } else { 0 });
            if (b != 0 && b != 0x7f) || redundant {
                proof {
                    assert forall|v: int| i64::MIN <= v <= i64::MAX
                        implies !(#[trigger] spec_encode_signed(v)).is_prefix_of(buf@) by {
                        if spec_encode_signed(v).is_prefix_of(buf@) {
                            lemma_encode_signed_shape(v);
                            lemma_i64_len(v);
                            lemma_framed_end(spec_encode_signed(v), buf@, 9);
                            assert(spec_encode_signed(v)[9] == buf@[9]);
                            assert(spec_encode_signed(v)[8] == buf@[8]);
                        }
                    }
                }
                return Err(if redundant { DecodeError::Overlong } else { DecodeError::Overflow });
            }
            proof { lemma_pow128_constants(); }
            let x = result as i64;
            proof {
                assert(buf@.subrange(0, 10)[8] == prev);
                lemma_shape_encode_signed(buf@.subrange(0, 10));
                assert(x >= 0 ==> (x | i64::MIN) == x + i64::MIN) by (bit_vector);
            }
            // The tenth group is bit 63 alone, as 0 or -1 (0x7f).
            let v = if b == 0 { x } else { x | i64::MIN };
            assert(v == groups_signed(buf@.subrange(0, 10))) by {
                if b != 0 {
                    assert(signed_group(b) * pow128(9) == -0x8000_0000_0000_0000int)
                        by (nonlinear_arith)
                        requires signed_group(b) == -1, pow128(9) == 0x8000_0000_0000_0000int;
                } else {
                    assert(signed_group(b) * pow128(9) == 0) by (nonlinear_arith)
                        requires signed_group(b) == 0;
                }
            }
            return Ok((v, 10));
        }
        let shift = (7 * i) as u64;
        proof { lemma_accumulate(result, b, shift); lemma_shift((i + 1) as u64); }
        result |= ((b & 0x7f) as u64) << shift;
        if b < 0x80 {
            if i > 0 && b == (if prev & 0x40 != 0 { 0x7f } else { 0 }) {
                proof {
                    assert forall|v: int| i64::MIN <= v <= i64::MAX
                        implies !(#[trigger] spec_encode_signed(v)).is_prefix_of(buf@) by {
                        if spec_encode_signed(v).is_prefix_of(buf@) {
                            lemma_encode_signed_shape(v);
                            lemma_framed_end(spec_encode_signed(v), buf@, i as int);
                            assert(spec_encode_signed(v)[i - 1] == buf@[i - 1]);
                        }
                    }
                }
                return Err(DecodeError::Overlong);
            }
            let k = shift + 7;
            proof {
                lemma_shape_encode_signed(buf@.subrange(0, i + 1));
                lemma_sign_extend(result, k);
                lemma_neg_shift((i + 1) as u64);
                assert((b & 0x40 == 0) == (b % 128 < 64)) by (bit_vector);
                let p = pow128(i as nat);
                assert(pow128((i + 1) as nat) == 128 * p);
                assert((b % 128 - 128) * p == (b % 128) as int * p - 128 * p) by (nonlinear_arith);
            }
            let x = result as i64;
            if b & 0x40 == 0 {
                return Ok((x, i + 1));
            }
            // Sign-extend from bit k - 1.
            return Ok((x | (-1i64 << k), i + 1));
        }
        prev = b;
        i += 1;
    }
}

/// The length of the LEB128 value at the front of `data` — the position
/// of the first byte without a continuation bit — or `None` if there is
/// none in the first ten bytes. Agrees with [`decode`] and
/// [`decode_signed`] wherever they succeed; for skipping over
/// already-validated bytes.
#[inline]
pub fn encoded_len_at(data: &[u8]) -> (r: Option<usize>)
    ensures
        forall|v: nat| v <= u64::MAX && #[trigger] spec_encode(v).is_prefix_of(data@)
            ==> r == Some(spec_encode(v).len() as usize),
        forall|v: int| i64::MIN <= v <= i64::MAX && #[trigger] spec_encode_signed(v).is_prefix_of(data@)
            ==> r == Some(spec_encode_signed(v).len() as usize),
{
    let n = if data.len() < MAX_BYTES { data.len() } else { MAX_BYTES };
    let mut i: usize = 0;
    while i < n
        invariant
            n <= data.len(), n <= MAX_BYTES, i <= n,
            forall|j: int| 0 <= j < i ==> #[trigger] data@[j] >= 128,
        decreases n - i,
    {
        if data[i] < 0x80 {
            proof {
                assert forall|v: nat| v <= u64::MAX && #[trigger] spec_encode(v).is_prefix_of(data@)
                    implies spec_encode(v).len() == i + 1 by {
                    lemma_encode_shape(v);
                    lemma_framed_end(spec_encode(v), data@, i as int);
                }
                assert forall|v: int| i64::MIN <= v <= i64::MAX
                    && #[trigger] spec_encode_signed(v).is_prefix_of(data@)
                    implies spec_encode_signed(v).len() == i + 1 by {
                    lemma_encode_signed_shape(v);
                    lemma_framed_end(spec_encode_signed(v), data@, i as int);
                }
            }
            return Some(i + 1);
        }
        i += 1;
    }
    proof {
        assert forall|v: nat| v <= u64::MAX implies !(#[trigger] spec_encode(v)).is_prefix_of(data@) by {
            if spec_encode(v).is_prefix_of(data@) {
                lemma_encode_shape(v);
                lemma_u64_len(v);
                lemma_framed_end(spec_encode(v), data@, n as int);
            }
        }
        assert forall|v: int| i64::MIN <= v <= i64::MAX
            implies !(#[trigger] spec_encode_signed(v)).is_prefix_of(data@) by {
            if spec_encode_signed(v).is_prefix_of(data@) {
                lemma_encode_signed_shape(v);
                lemma_i64_len(v);
                lemma_framed_end(spec_encode_signed(v), data@, n as int);
            }
        }
    }
    None
}

// ── Encoding ────────────────────────────────────────────────────────────────

/// Size of [`Encoded`]'s buffer: hexane's `VarBuf` capacity, so that an
/// encoding moves into a `VarBuf` whole rather than being copied out.
pub const BUF_BYTES: usize = 16;

/// A stack-allocated LEB128 encoding, carrying its own length.
#[derive(Debug, Clone, Copy)]
pub struct Encoded {
    buf: [u8; BUF_BYTES],
    len: u8,
}

impl View for Encoded {
    type V = Seq<u8>;

    closed spec fn view(&self) -> Seq<u8> {
        self.buf@.subrange(0, self.len as int)
    }
}

impl Encoded {
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

    /// The buffer and the length of the encoding at its front.
    #[inline]
    pub fn into_parts(self) -> (r: ([u8; BUF_BYTES], u8))
        ensures 1 <= r.1 <= MAX_BYTES, r.0@.subrange(0, r.1 as int) == self@,
    {
        proof { use_type_invariant(self); }
        (self.buf, self.len)
    }
}

/// Encodes `value` as unsigned LEB128.
#[inline]
pub fn encode(value: u64) -> (r: Encoded)
    ensures r@ == spec_encode(value as nat),
{
    proof { lemma_u64_len(value as nat); }
    let mut buf = [0u8; BUF_BYTES];
    let mut len: usize = 0;
    let mut v = value;
    while v >= 0x80
        invariant
            len + spec_encode(v as nat).len() == spec_encode(value as nat).len() <= MAX_BYTES,
            buf@.subrange(0, len as int) + spec_encode(v as nat) == spec_encode(value as nat),
        decreases v,
    {
        assert((v & 0x7f) | 0x80 == v % 128 + 128 && v >> 7 == v / 128) by (bit_vector);
        let ghost before = buf@.subrange(0, len as int);
        buf[len] = ((v & 0x7f) | 0x80) as u8;
        assert(buf@.subrange(0, len + 1) =~= before.push(buf@[len as int]));
        assert(spec_encode(v as nat) =~= seq![buf@[len as int]] + spec_encode((v >> 7) as nat));
        assert(before.push(buf@[len as int]) + spec_encode((v >> 7) as nat)
            =~= before + spec_encode(v as nat));
        v >>= 7;
        len += 1;
    }
    let ghost before = buf@.subrange(0, len as int);
    buf[len] = v as u8;
    assert(buf@.subrange(0, len + 1) =~= before + spec_encode(v as nat));
    Encoded { buf, len: (len + 1) as u8 }
}

/// Encodes `value` as signed LEB128.
#[inline]
#[allow(clippy::manual_range_contains)]
pub fn encode_signed(value: i64) -> (r: Encoded)
    ensures r@ == spec_encode_signed(value as int),
{
    proof { lemma_i64_len(value as int); }
    let mut buf = [0u8; BUF_BYTES];
    let mut len: usize = 0;
    let mut v = value;
    while v < -64 || v >= 64
        invariant
            len + spec_encode_signed(v as int).len() == spec_encode_signed(value as int).len()
                <= MAX_BYTES,
            buf@.subrange(0, len as int) + spec_encode_signed(v as int)
                == spec_encode_signed(value as int),
        decreases (if v < 0 { -(v as int) } else { v as int }),
    {
        assert((v >> 7u64) * 128 + (v & 0x7f) == v && 0 <= (v & 0x7f) < 128) by (bit_vector);
        let ghost before = buf@.subrange(0, len as int);
        buf[len] = ((v & 0x7f) | 0x80) as u8;
        assert(((v & 0x7f) | 0x80) == (v & 0x7f) + 128) by (bit_vector);
        assert(buf@.subrange(0, len + 1) =~= before.push(buf@[len as int]));
        assert(spec_encode_signed(v as int)
            =~= seq![buf@[len as int]] + spec_encode_signed((v >> 7u64) as int));
        assert(before.push(buf@[len as int]) + spec_encode_signed((v >> 7u64) as int)
            =~= before + spec_encode_signed(v as int));
        v >>= 7u64;
        len += 1;
    }
    assert((v & 0x7f) == v % 128) by (bit_vector);
    let ghost before = buf@.subrange(0, len as int);
    buf[len] = (v & 0x7f) as u8;
    assert(buf@.subrange(0, len + 1) =~= before + spec_encode_signed(v as int));
    Encoded { buf, len: (len + 1) as u8 }
}

/// What `64 - leading_zeros` tells the encoder: the value's bit width.
proof fn lemma_bit_width(v: u64)
    ensures ({
        let w = 64 - vstd::std_specs::bits::u64_leading_zeros(v);
        &&& 0 <= w <= 64
        &&& v == 0 <==> w == 0
        &&& w > 0 ==> v >= (1u64 << (w - 1) as u64)
        &&& w < 64 ==> v < (1u64 << w as u64)
    }),
{
    vstd::std_specs::bits::axiom_u64_leading_zeros(v);
    let lz = vstd::std_specs::bits::u64_leading_zeros(v);
    if lz < 64 {
        let s = (63 - lz) as u64;
        assert((v >> s) & 1u64 != 0u64);
        assert((v >> s) & 1u64 != 0u64 && s < 64 ==> v >= (1u64 << s)) by (bit_vector);
    }
    if lz > 0 {
        let w = (64 - lz) as u64;
        assert(v >> w == 0);
        assert(v >> w == 0 && w < 64 ==> v < (1u64 << w)) by (bit_vector);
    }
}

proof fn lemma_shl_mono(a: u64, b: u64)
    requires a <= b < 64,
    ensures (1u64 << a) <= (1u64 << b),
{
    assert(a <= b < 64 ==> (1u64 << a) <= (1u64 << b)) by (bit_vector);
}

proof fn lemma_shl_double(a: u64)
    requires 1 <= a <= 63,
    ensures (1u64 << (a - 1) as u64) * 2 == (1u64 << a),
{
    assert(1 <= a <= 63 ==> (1u64 << (a - 1) as u64) * 2 == (1u64 << a)) by (bit_vector);
}

/// Returns the unsigned LEB128 length of `value` in bytes (1–10).
#[inline]
#[allow(clippy::manual_div_ceil)] // vstd has no spec for div_ceil
pub fn encoded_len(value: u64) -> (r: usize)
    ensures r == spec_encode(value as nat).len(),
{
    proof {
        let w = 64 - vstd::std_specs::bits::u64_leading_zeros(value);
        let l = if w == 0 { 1 } else { (w + 6) / 7 };
        lemma_bit_width(value);
        lemma_pow128_constants();
        lemma_u64_len(value as nat);
        // 128^(l-1) <= value < 128^l, so the encoding has exactly l bytes
        lemma_len(value as nat, l as nat);
        if l > 1 {
            lemma_len(value as nat, (l - 1) as nat);
            lemma_shift((l - 1) as u64);
            lemma_shl_mono((7 * (l - 1)) as u64, (w - 1) as u64);
        }
        if l < 10 {
            lemma_shift(l as u64);
            lemma_shl_mono(w as u64, (7 * l) as u64);
        }
    }
    let w = 64 - value.leading_zeros() as usize;
    if w == 0 { 1 } else { (w + 6) / 7 }
}

/// Returns the signed LEB128 length of `value` in bytes (1–10).
#[inline]
pub fn encoded_len_signed(value: i64) -> (r: usize)
    ensures r == spec_encode_signed(value as int).len(),
{
    // The magnitude bits of value, or of !value = -value - 1 when negative;
    // the encoding needs one more bit for the sign.
    let m = (if value < 0 { !value } else { value }) as u64;
    proof {
        assert(value < 0 ==> !value == -value - 1 && !value >= 0) by (bit_vector);
        assert(m < 0x8000_0000_0000_0000);
        lemma_bit_width(m);
        lemma_shift(9);
        lemma_pow128_constants();
        lemma_i64_len(value as int);
        let w = 64 - vstd::std_specs::bits::u64_leading_zeros(m);
        let l = (w + 7) / 7;
        // -2^(7l-1) <= value < 2^(7l-1), but not with l - 1
        lemma_len_signed(value as int, l as nat);
        if l < 10 {
            let a = (7 * l) as u64;
            lemma_shift(l as u64);
            lemma_shl_double(a);
            lemma_shl_mono(w as u64, (a - 1) as u64);
            assert(pow128(l as nat) == 128 * pow128((l - 1) as nat));
        }
        if l > 1 {
            let a = (7 * (l - 1)) as u64;
            lemma_len_signed(value as int, (l - 1) as nat);
            lemma_shift((l - 1) as u64);
            lemma_shl_double(a);
            lemma_shl_mono((a - 1) as u64, (w - 1) as u64);
            assert(pow128((l - 1) as nat) == 128 * pow128((l - 2) as nat));
        }
    }
    (64 - m.leading_zeros() as usize + 7) / 7
}

} // verus!

impl core::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(match self {
            DecodeError::Truncated => "LEB128 encoding is truncated",
            DecodeError::Overlong => "LEB128 encoding is not canonical (overlong)",
            DecodeError::Overflow => "LEB128 value overflows 64 bits",
        })
    }
}

impl std::error::Error for DecodeError {}
