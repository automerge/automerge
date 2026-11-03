//! Fenwick tree (binary indexed tree) over per-slab weights, verified with
//! Verus.
//!
//! `bit[1..=n]` is the tree over the slab weights `w[0..n]`: node `i` holds
//! the sum of `w[i - low(i) .. i]`, where `low(i)` is the lowest set bit of
//! `i` ([`bit_ok`]).  `bit[0]` is unused.  Only the weights' item counts
//! ([`SlabWeight::weight_len`]) are specified; the prefix-sum half of a
//! `PrefixSlabWeight` rides along unverified.
//!
//! [`build`] makes the tree, [`update`] changes one weight, [`prefix`] sums
//! the first `i` weights and [`find`] locates the slab holding an item, each
//! proved against the weight sequence.  Callers keep the total item count
//! within `usize`, so no sum overflows.

use crate::column::SlabWeight;
#[allow(unused_imports)]
use vstd::prelude::*;

verus! {

// ── Model ───────────────────────────────────────────────────────────────────

/// The lowest set bit of `i` (for `i >= 1`).
pub open spec fn low(i: usize) -> usize {
    i & !((i - 1) as usize)
}

/// `k` is a power of two.
pub open spec fn pow2(k: usize) -> bool {
    k >= 1 && k & ((k - 1) as usize) == 0
}

/// `s[lo..hi]`, summed.
pub open spec fn sum(s: Seq<nat>, lo: int, hi: int) -> nat
    decreases hi - lo,
{
    if hi <= lo {
        0
    } else {
        sum(s, lo, hi - 1) + s[hi - 1]
    }
}

/// All of `s`, summed.
pub open spec fn total(s: Seq<nat>) -> nat {
    sum(s, 0, s.len() as int)
}

/// The item counts of a sequence of weights.
pub open spec fn lens<W: SlabWeight>(ws: Seq<W>) -> Seq<nat> {
    ws.map_values(|x: W| x.weight_len())
}

/// `bit` is the Fenwick tree of `w`.
pub open spec fn bit_ok(bit: Seq<nat>, w: Seq<nat>) -> bool {
    &&& bit.len() == w.len() + 1
    &&& forall|i: int| 1 <= i <= w.len() ==> #[trigger] bit[i] == sum(w, i - low(i as usize), i)
}

// ── Sums ────────────────────────────────────────────────────────────────────

pub proof fn lemma_sum_split(s: Seq<nat>, lo: int, mid: int, hi: int)
    requires
        lo <= mid <= hi,
    ensures
        sum(s, lo, hi) == sum(s, lo, mid) + sum(s, mid, hi),
    decreases hi - mid,
{
    if hi > mid {
        lemma_sum_split(s, lo, mid, hi - 1);
    }
}

pub proof fn lemma_sum_le(s: Seq<nat>, lo: int, hi: int, lo2: int, hi2: int)
    requires
        lo2 <= lo <= hi <= hi2,
    ensures
        sum(s, lo, hi) <= sum(s, lo2, hi2),
{
    lemma_sum_split(s, lo2, lo, hi2);
    lemma_sum_split(s, lo, hi, hi2);
}

pub proof fn lemma_sum_one(s: Seq<nat>, k: int)
    ensures
        sum(s, k, k + 1) == s[k],
{
    assert(sum(s, k, k) == 0);
}

/// Changing `s[k]` changes exactly the sums over ranges that hold `k`.
pub proof fn lemma_sum_update(s: Seq<nat>, lo: int, hi: int, k: int, v: nat)
    requires
        0 <= lo,
        hi <= s.len(),
        0 <= k < s.len(),
    ensures
        lo <= k < hi ==> sum(s.update(k, v), lo, hi) + s[k] == sum(s, lo, hi) + v,
        !(lo <= k < hi) ==> sum(s.update(k, v), lo, hi) == sum(s, lo, hi),
    decreases hi - lo,
{
    if hi > lo {
        lemma_sum_update(s, lo, hi - 1, k, v);
    }
}

// ── Bits ────────────────────────────────────────────────────────────────────

pub proof fn lemma_low(i: usize)
    requires
        i >= 1,
    ensures
        1 <= low(i) <= i,
        pow2(low(i)),
        i & ((i - 1) as usize) == i - low(i),
{
    assert(1 <= (i & !((i - 1) as usize)) <= i) by (bit_vector)
        requires
            i >= 1,
    ;
    assert((i & !((i - 1) as usize)) & (((i & !((i - 1) as usize)) - 1) as usize) == 0)
        by (bit_vector)
        requires
            i >= 1,
    ;
    assert(i & ((i - 1) as usize) == (i - (i & !((i - 1) as usize))) as usize) by (bit_vector)
        requires
            i >= 1,
    ;
}

/// The children of node `j` are `j - k` for the powers of two `k < low(j)`;
/// child `j - k` covers `[j - 2k, j - k)`.
proof fn lemma_child(j: usize, k: usize)
    requires
        j >= 1,
        pow2(k),
        k < low(j),
    ensures
        j - k >= 1,
        low((j - k) as usize) == k,
        k <= low(j) - k,
        k * 2 <= usize::MAX,
        pow2((k * 2) as usize),
{
    lemma_low(j);
    assert((((j - k) as usize) & !((((j - k) as usize) - 1) as usize)) == k) by (bit_vector)
        requires
            j >= 1,
            k >= 1,
            k & ((k - 1) as usize) == 0,
            k < (j & !((j - 1) as usize)),
    ;
    assert(k <= ((j & !((j - 1) as usize)) - k) as usize) by (bit_vector)
        requires
            j >= 1,
            k >= 1,
            k & ((k - 1) as usize) == 0,
            k < (j & !((j - 1) as usize)),
    ;
    assert(((k * 2) as usize) & ((((k * 2) as usize) - 1) as usize) == 0 && (k * 2) as usize > k)
        by (bit_vector)
        requires
            j >= 1,
            k >= 1,
            k & ((k - 1) as usize) == 0,
            k < (j & !((j - 1) as usize)),
    ;
}

/// The nodes strictly between `i` and its parent `i + low(i)` cover nothing
/// below `i`.
proof fn lemma_skip(i: usize, d: usize)
    requires
        i >= 1,
        0 < d < low(i),
        i + d <= usize::MAX,
    ensures
        (i + d) - low((i + d) as usize) >= i,
{
    lemma_low(d);
    assert(((i + d) as usize & !((((i + d) as usize) - 1) as usize)) == (d & !((d - 1) as usize)))
        by (bit_vector)
        requires
            0 < d,
            d < (i & !((i - 1) as usize)),
            i >= 1,
            (i + d) as usize >= i,
    ;
}

/// Node `i`'s parent `i + low(i)` covers everything `i` does.
proof fn lemma_parent(i: usize)
    requires
        i >= 1,
        i + low(i) <= usize::MAX,
    ensures
        (i + low(i)) - low((i + low(i)) as usize) <= i - low(i),
{
    lemma_low(i);
    let j = (i + low(i)) as usize;
    lemma_low(j);
    assert(((((i + (i & !((i - 1) as usize))) as usize) - (((i + (i & !((i - 1) as usize))) as usize) & !(((((i + (i & !((i - 1) as usize))) as usize)) - 1) as usize))) as usize) <= ((i - (i & !((i - 1) as usize))) as usize))
        by (bit_vector)
        requires
            i >= 1,
            ((i + (i & !((i - 1) as usize))) as usize) >= i,
    ;
}

/// One step of [`find`]'s binary lifting: `idx` is a multiple of `2k`.
proof fn lemma_lift(idx: usize, k: usize)
    requires
        pow2(k),
        idx & (k | ((k - 1) as usize)) == 0,
    ensures
        low((idx + k) as usize) == k,
        (idx + k) as usize >= 1,
        k / 2 == 0 ==> k == 1,
        k / 2 >= 1 ==> {
            &&& pow2(k / 2)
            &&& (k / 2) * 2 == k
            &&& idx & ((k / 2) | ((k / 2 - 1) as usize)) == 0
            &&& ((idx + k) as usize) & ((k / 2) | ((k / 2 - 1) as usize)) == 0
        },
{
    assert((((idx + k) as usize) & !((((idx + k) as usize) - 1) as usize)) == k && (idx + k) as usize >= 1)
        by (bit_vector)
        requires
            k >= 1,
            k & ((k - 1) as usize) == 0,
            idx & (k | ((k - 1) as usize)) == 0,
    ;
    assert(k / 2 >= 1 ==> {
        &&& (k / 2) & ((k / 2 - 1) as usize) == 0
        &&& ((k / 2) * 2) as usize == k
        &&& idx & ((k / 2) | ((k / 2 - 1) as usize)) == 0
        &&& ((idx + k) as usize) & ((k / 2) | ((k / 2 - 1) as usize)) == 0
    }) by (bit_vector)
        requires
            k >= 1,
            k & ((k - 1) as usize) == 0,
            idx & (k | ((k - 1) as usize)) == 0,
    ;
}

proof fn lemma_double(k: usize)
    requires
        pow2(k),
        k * 2 <= usize::MAX,
    ensures
        pow2((k * 2) as usize),
{
    assert(((k * 2) as usize) & ((((k * 2) as usize) - 1) as usize) == 0) by (bit_vector)
        requires
            k >= 1,
            k & ((k - 1) as usize) == 0,
            (k * 2) as usize > k,
    ;
}

// ── Operations ──────────────────────────────────────────────────────────────

/// The Fenwick tree of the weights `weight(s)` of `slabs`.  O(n): each node
/// adds up its children, which come before it.
pub fn build<S, W: SlabWeight, F: Fn(&S) -> W>(
    slabs: &[S],
    weight: F,
    Ghost(wl): Ghost<spec_fn(S) -> nat>,
) -> (bit: Vec<W>)
    requires
        forall|s: &S| #[trigger] weight.requires((s,)),
        forall|s: &S, x: W| #[trigger] weight.ensures((s,), x) ==> x.weight_len() == wl(*s),
        total(slabs@.map_values(wl)) <= usize::MAX,
    ensures
        bit_ok(lens(bit@), slabs@.map_values(wl)),
{
    let ghost w = slabs@.map_values(wl);
    let n = slabs.len();
    let mut bit: Vec<W> = Vec::with_capacity(n.saturating_add(1));
    bit.push(W::zero());
    let mut t: usize = 0;
    while t < n
        invariant
            w == slabs@.map_values(wl),
            n == slabs@.len() == w.len(),
            total(w) <= usize::MAX,
            forall|s: &S| #[trigger] weight.requires((s,)),
            forall|s: &S, x: W| #[trigger] weight.ensures((s,), x) ==> x.weight_len() == wl(*s),
            t <= n,
            bit@.len() == t + 1,
            forall|i: int| 1 <= i <= t ==> #[trigger] lens(bit@)[i] == sum(w, i - low(i as usize), i),
        decreases n - t,
    {
        let j = t + 1;
        let low_j = j & !(j - 1);
        proof {
            lemma_low(j);
        }
        let mut acc = weight(&slabs[t]);
        proof {
            lemma_sum_one(w, t as int);
        }
        let mut k: usize = 1;
        proof {
            assert(1usize & 0usize == 0) by (bit_vector);
        }
        while k < low_j
            invariant
                1 <= j <= n,
                bit@.len() == j,
                low_j == low(j),
                low_j <= j,
                pow2(k),
                k <= low_j,
                acc.weight_len() == sum(w, j - k, j as int),
                total(w) <= usize::MAX,
                w.len() == n,
                forall|i: int| 1 <= i < j ==> #[trigger] lens(bit@)[i] == sum(w, i - low(i as usize), i),
            decreases low_j - k,
        {
            proof {
                lemma_child(j, k);
                let c = (j - k) as int;
                assert(lens(bit@)[c] == sum(w, c - low(c as usize), c));
                lemma_sum_split(w, j - 2 * k, j - k, j as int);
                lemma_sum_le(w, j - 2 * k, j as int, 0, n as int);
            }
            acc.add_weight(&bit[j - k]);
            k *= 2;
        }
        let ghost bit1 = bit@;
        bit.push(acc);
        proof {
            assert forall|i: int| 1 <= i <= j implies #[trigger] lens(bit@)[i] == sum(
                w,
                i - low(i as usize),
                i,
            ) by {
                if i < j {
                    assert(lens(bit1)[i] == sum(w, i - low(i as usize), i));
                }
            }
        }
        t = j;
    }
    bit
}

/// Sum of the first `i` weights.  O(log n).
pub fn prefix<W: SlabWeight>(bit: &[W], i: usize, Ghost(w): Ghost<Seq<nat>>) -> (acc: usize)
    requires
        bit_ok(lens(bit@), w),
        i <= w.len(),
        total(w) <= usize::MAX,
    ensures
        acc == sum(w, 0, i as int),
{
    let mut acc = 0usize;
    let mut j = i;
    while j > 0
        invariant
            bit_ok(lens(bit@), w),
            j <= i <= w.len(),
            acc + sum(w, 0, j as int) == sum(w, 0, i as int),
            total(w) <= usize::MAX,
        decreases j,
    {
        proof {
            lemma_low(j);
            assert(lens(bit@)[j as int] == sum(w, j - low(j), j as int));
            lemma_sum_split(w, 0, j - low(j), j as int);
            lemma_sum_le(w, 0, i as int, 0, w.len() as int);
        }
        acc += bit[j].len();
        j &= j - 1;
    }
    acc
}

/// Change weight `si` from `from` to `to`.  O(log n): only `si + 1` and
/// its ancestors cover `si`.
pub fn update<W: SlabWeight>(
    bit: &mut Vec<W>,
    si: usize,
    from: &W,
    to: &W,
    Ghost(w): Ghost<Seq<nat>>,
)
    requires
        bit_ok(lens(old(bit)@), w),
        si < w.len(),
        from.weight_len() == w[si as int],
        total(w.update(si as int, to.weight_len())) <= usize::MAX,
    ensures
        bit_ok(lens(final(bit)@), w.update(si as int, to.weight_len())),
{
    let ghost w2 = w.update(si as int, to.weight_len());
    let ghost bit0 = bit@;
    let len = bit.len();
    let mut i = si + 1;
    proof {
        lemma_low(i);
        assert forall|j: int| 1 <= j < i implies #[trigger] lens(bit@)[j] == sum(
            w2,
            j - low(j as usize),
            j,
        ) by {
            lemma_low(j as usize);
            assert(lens(bit0)[j] == sum(w, j - low(j as usize), j));
            lemma_sum_update(w, j - low(j as usize), j, si as int, to.weight_len());
        }
    }
    while i < len
        invariant_except_break
            1 <= i,
            i - low(i) <= si < i,
            forall|j: int| 1 <= j < i && j < len ==> #[trigger] lens(bit@)[j] == sum(
                w2,
                j - low(j as usize),
                j,
            ),
            forall|j: int| i <= j < len ==> #[trigger] bit@[j] == bit0[j],
        invariant
            len == bit@.len() == bit0.len() == w.len() + 1,
            bit_ok(lens(bit0), w),
            w2 == w.update(si as int, to.weight_len()),
            si < w.len(),
            from.weight_len() == w[si as int],
            total(w2) <= usize::MAX,
        ensures
            forall|j: int| 1 <= j < len ==> #[trigger] lens(bit@)[j] == sum(w2, j - low(j as usize), j),
        decreases usize::MAX - i,
    {
        let ghost lo = i - low(i);
        proof {
            lemma_low(i);
            assert(lens(bit0)[i as int] == sum(w, lo, i as int));
            lemma_sum_update(w, lo, i as int, si as int, to.weight_len());
            lemma_sum_split(w, lo, si as int, i as int);
            lemma_sum_split(w, si as int, si + 1, i as int);
            lemma_sum_one(w, si as int);
            lemma_sum_le(w2, lo, i as int, 0, w2.len() as int);
        }
        let ghost bit1 = bit@;
        let mut x = bit[i].copy();
        x.sub_weight(from);
        x.add_weight(to);
        bit.set(i, x);
        let low_i = i & !(i - 1);
        proof {
            assert forall|j: int| 1 <= j < len && j < i + low_i implies #[trigger] lens(bit@)[j]
                == sum(w2, j - low(j as usize), j) by {
                if j < i {
                    assert(lens(bit1)[j] == sum(w2, j - low(j as usize), j));
                } else if j > i {
                    lemma_skip(i, (j - i) as usize);
                    lemma_low(j as usize);
                    assert(lens(bit0)[j] == sum(w, j - low(j as usize), j));
                    lemma_sum_update(w, j - low(j as usize), j, si as int, to.weight_len());
                }
            }
        }
        // `while i < len` (and not `low_i < len - i`) lets the compiler drop
        // the bounds check on `bit[i]`; the overflow check then folds away.
        match i.checked_add(low_i) {
            Some(next) => {
                proof {
                    lemma_parent(i);
                }
                i = next;
            },
            None => {
                break;
            },
        }
    }
}

/// The slab holding item `index`, and the item's offset in it: the last
/// slab whose items start at or before `index` (so past the end, `n`).
/// O(log n), by binary lifting.
pub fn find<W: SlabWeight>(bit: &[W], index: usize, n: usize, Ghost(w): Ghost<Seq<nat>>) -> (r: (
    usize,
    usize,
))
    requires
        bit_ok(lens(bit@), w),
        n == w.len(),
    ensures
        n == 0 ==> r == (0usize, 0usize),
        n > 0 ==> {
            &&& r.0 <= n
            &&& sum(w, 0, r.0 as int) + r.1 == index
            &&& r.0 < n ==> r.1 < w[r.0 as int]
        },
{
    if n == 0 {
        return (0, 0);
    }
    let mut pos = 0usize;
    let mut idx = 0usize;
    // The largest power of two `<= n`.
    let mut bit_k = 1usize;
    proof {
        assert(1usize & 0usize == 0) by (bit_vector);
    }
    while bit_k <= n / 2
        invariant
            pow2(bit_k),
            bit_k <= n,
        decreases n - bit_k,
    {
        proof {
            lemma_double(bit_k);
        }
        bit_k *= 2;
    }
    // `idx` is the last slab start `<= index` among multiples of `up`.
    let ghost mut up: int = 2 * bit_k;
    proof {
        assert(idx & (bit_k | ((bit_k - 1) as usize)) == 0) by (bit_vector)
            requires
                idx == 0,
        ;
    }
    while bit_k > 0
        invariant
            bit_ok(lens(bit@), w),
            n == w.len(),
            n >= 1,
            idx <= n,
            pos == sum(w, 0, idx as int),
            pos <= index,
            bit_k > 0 ==> pow2(bit_k) && up == 2 * bit_k && idx & (bit_k | ((bit_k - 1) as usize))
                == 0,
            bit_k == 0 ==> up == 1,
            idx + up > n || sum(w, 0, idx + up) > index,
        decreases bit_k,
    {
        proof {
            lemma_lift(idx, bit_k);
        }
        let ghost idx0 = idx;
        if bit_k <= n - idx {
            let next = idx + bit_k;
            proof {
                assert(lens(bit@)[next as int] == sum(w, next - low(next), next as int));
                lemma_sum_split(w, 0, idx as int, next as int);
            }
            let l = bit[next].len();
            if l <= index - pos {
                pos += l;
                idx = next;
            }
        }
        proof {
            up = bit_k as int;
        }
        bit_k /= 2;
    }
    proof {
        if idx < n {
            lemma_sum_split(w, 0, idx as int, idx + 1);
            lemma_sum_one(w, idx as int);
        }
    }
    (idx, index - pos)
}

} // verus!
