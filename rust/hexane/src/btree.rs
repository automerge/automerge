//! Generic B+tree slab index.  Generic over an aggregate type `A`
//! implementing [`SlabAggregate`], so the same tree can back multiple
//! column types:
//!
//! * `DeltaColumn` uses `SlabBTree<SlabAgg>` with min/max
//!   pruning for value queries.
//! * `PrefixColumn` uses `SlabBTree<PrefixSlabWeight<P>>` as a direct
//!   replacement for the Fenwick BIT — stores len + prefix sum per slab.
//!
//! Each leaf holds up to `B` slab aggregates in slab-index order.  Each
//! internal node holds up to `B` child entries, where each entry carries
//! the child's merged aggregate and its slab count.
//!
//! Mutation API ([`splice`](SlabBTree::splice)) matches what
//! `Column::splice_inner` produces: a slab-range to replace + a stream
//! of new aggregates.
//!
//! Deletion underflow: emptied nodes are removed (cascading upward), a
//! single-child root chain is collapsed, and underfull *leaves* merge
//! into siblings.  Internal nodes are deliberately not rebalanced — they
//! thin ~B× slower than leaves (losing a child only when a whole leaf
//! dies), so under realistic churn their fill follows the leaf count;
//! worst case is extra memory, never incorrect queries.
//!
//! Verification (Verus, `cargo verus verify -p hexane`): the tree stands
//! for a sequence of slab aggregates (its `View`), and `wf` is the
//! invariant tying the arena to it: every cached child slab count and
//! aggregate is right, subtrees don't share nodes, the free list names
//! distinct empty slots, and every grouping of slabs can be merged
//! (`valid`).  `splice`'s core (`splice_vec`), `from_vec`, `update_slab`,
//! `find_by_prefix`, `find_slab_at_item`, `find_slab_at_prefix`,
//! `items_before_slab`, `root_agg` and the value-range search
//! (`ValueSearch`, behind `find_by_value` and `find_by_value_range`) are
//! proved against that sequence: a splice yields exactly
//! `old[..start] + new + old[end..]` provided the caller keeps that result
//! `valid`, and the value search yields, in order, exactly the slabs whose
//! min/max running sums meet the range.  Only the `IntoIterator` wrappers
//! around `splice_vec` and `from_vec` and the `Iterator` wrappers around
//! `ValueSearch` are unverified.  `merge`'s laws are proved for `SlabAgg` and `usize`; for
//! `PrefixSlabWeight<P>` they, and the prefix arithmetic, are assumed of
//! the user's `P` (`axiom_prefix_add`).

// Verus has no specs for `Vec::is_empty` or `usize::is_multiple_of`, and
// ghost arguments make some signatures long.
#![allow(
    clippy::len_zero,
    clippy::manual_is_multiple_of,
    clippy::too_many_arguments
)]

use crate::prefix::PrefixSlabWeight;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::{Add, AddAssign, Range, SubAssign};
#[allow(unused_imports)]
use vstd::prelude::*;

// ── SlabAggregate trait ─────────────────────────────────────────────────────

verus! {

/// Branching factor.  Smaller = deeper tree + cheaper per-node rebalance;
/// larger = fewer nodes + more per-node cost.  64 is empirically a good
/// compromise across the `marks_at` (query-heavy) and `insert_null`
/// (mutation-heavy) workloads.
const B: usize = 64;

/// A per-slab value that aggregates associatively (in left-to-right
/// order) into per-subtree values.  Used both as the leaf's per-slab
/// payload and the internal node's per-subtree summary.
///
/// The `merge` operation does **not** need to be commutative — only
/// associative and order-preserving.  Specifically, `merge(a, b) != merge(b, a)`
/// is fine (e.g. delta-running-min semantics) as long as the tree
/// always walks children left-to-right.
///
/// For verification, `merge` is also given as mathematics
/// (`spec_merge`), defined wherever
/// `merge_ok` holds (for fixed-width fields: no
/// overflow), with `spec_empty` as its identity.  The
/// `lemma_*` proof functions are the laws the tree relies on; each impl
/// proves them.
#[allow(clippy::len_without_is_empty)]
pub trait SlabAggregate: Clone + Default + std::fmt::Debug + Sized {
    /// [`len`](Self::len), for specifications.
    spec fn spec_len(&self) -> nat;

    /// [`merge`](Self::merge), for specifications.
    spec fn spec_merge(l: Self, r: Self) -> Self;

    /// Whether `merge(l, r)` is representable (e.g. its sums don't
    /// overflow).  The column keeps every aggregate the tree forms within
    /// this; see [`valid`].
    spec fn merge_ok(l: Self, r: Self) -> bool;

    /// The aggregate of no slabs.
    spec fn spec_empty() -> Self;

    /// Combine two adjacent subtree aggregates into one: exactly, where
    /// that is representable; in any case counting both's items. (A
    /// column edit can merge unrepresentable sums for a moment: see
    /// `SlabAgg::merge`.)
    fn merge(l: &Self, r: &Self) -> (m: Self)
        requires l.spec_len() + r.spec_len() <= usize::MAX,
        ensures
            m.spec_len() == l.spec_len() + r.spec_len(),
            Self::merge_ok(*l, *r) ==> m == Self::spec_merge(*l, *r);

    /// Number of items covered by this aggregate.  Used by generic
    /// tree-walking helpers (e.g. `find_by_prefix`); concrete impls
    /// typically access a `.len` field directly.
    fn len(&self) -> (n: usize)
        ensures n == self.spec_len();

    /// The aggregate of no slabs ([`Default`] for every impl here).
    fn empty() -> (e: Self)
        ensures e == Self::spec_empty();

    /// The empty aggregate is a left identity, and covers no items.  (Not
    /// a right identity: `SlabAgg`'s `merge` keeps the right operand when
    /// the left covers no items.)
    proof fn lemma_empty(a: Self)
        ensures
            Self::merge_ok(Self::spec_empty(), a),
            Self::spec_merge(Self::spec_empty(), a) == a,
            Self::spec_empty().spec_len() == 0;

    /// Every aggregate's item count fits a `usize`.
    proof fn lemma_len_bound(a: Self)
        ensures a.spec_len() <= usize::MAX;

    /// Item counts add up.
    proof fn lemma_merge_len(a: Self, b: Self)
        requires Self::merge_ok(a, b),
        ensures Self::spec_merge(a, b).spec_len() == a.spec_len() + b.spec_len();

    /// Associativity, where all four merges are representable.
    proof fn lemma_assoc(a: Self, b: Self, c: Self)
        requires
            Self::merge_ok(a, b),
            Self::merge_ok(b, c),
            Self::merge_ok(Self::spec_merge(a, b), c),
            Self::merge_ok(a, Self::spec_merge(b, c)),
        ensures
            Self::spec_merge(Self::spec_merge(a, b), c) == Self::spec_merge(a, Self::spec_merge(b, c));

    /// Whether every merge whose item counts fit a `usize` is
    /// representable: true for item counts (and prefix sums, whose
    /// arithmetic is the user's), false for delta sums.
    spec fn plain() -> bool;

    proof fn lemma_plain(a: Self, b: Self)
        requires
            Self::plain(),
            a.spec_len() + b.spec_len() <= usize::MAX,
        ensures
            Self::merge_ok(a, b);
}

// ── SlabAgg (used by DeltaColumn) ───────────────────────────────────────────

/// Running min/max of a delta column's prefix-sum range.  The specific
/// aggregate used by `DeltaColumn`'s default `IndexedDeltaWeightFn` for
/// value-range pruning.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct SlabAgg {
    pub len: usize,
    pub total: i64,
    pub min_offset: i64,
    pub max_offset: i64,
}

/// `x` fits an `i64`.
pub open spec fn in_i64(x: int) -> bool {
    i64::MIN <= x <= i64::MAX
}

pub open spec fn min_i(a: int, b: int) -> int {
    if a <= b { a } else { b }
}

pub open spec fn max_i(a: int, b: int) -> int {
    if a >= b { a } else { b }
}

impl SlabAggregate for SlabAgg {
    open spec fn spec_len(&self) -> nat {
        self.len as nat
    }

    open spec fn spec_merge(l: Self, r: Self) -> Self {
        if l.len == 0 {
            r
        } else if r.len == 0 {
            l
        } else {
            SlabAgg {
                len: (l.len + r.len) as usize,
                total: (l.total + r.total) as i64,
                min_offset: min_i(l.min_offset as int, l.total + r.min_offset) as i64,
                max_offset: max_i(l.max_offset as int, l.total + r.max_offset) as i64,
            }
        }
    }

    /// The delta domain contract keeps every running sum within `i64`.
    open spec fn merge_ok(l: Self, r: Self) -> bool {
        l.len == 0 || r.len == 0 || {
            &&& l.len + r.len <= usize::MAX
            &&& in_i64(l.total + r.total)
            &&& in_i64(l.total + r.min_offset)
            &&& in_i64(l.total + r.max_offset)
        }
    }

    open spec fn spec_empty() -> Self {
        SlabAgg { len: 0, total: 0, min_offset: 0, max_offset: 0 }
    }

    fn merge(l: &Self, r: &Self) -> Self {
        if l.len == 0 {
            return *r;
        }
        if r.len == 0 {
            return *l;
        }
        // Wrapping, as a release build's `+` is: exact whenever the merge
        // is representable (`merge_ok`), the only place the tree's proofs
        // need the sums (the item counts they need always). A column edit
        // can merge unrepresentable sums for a moment
        // — a delete that drops a whole slab makes its neighbours
        // adjacent before the delta that joins them is written — and
        // those nodes are recomputed when it is; a debug build's checked
        // `+` would panic on them instead.
        SlabAgg {
            len: l.len + r.len,
            total: l.total.wrapping_add(r.total),
            min_offset: l.min_offset.min(l.total.wrapping_add(r.min_offset)),
            max_offset: l.max_offset.max(l.total.wrapping_add(r.max_offset)),
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    fn empty() -> Self {
        SlabAgg { len: 0, total: 0, min_offset: 0, max_offset: 0 }
    }

    proof fn lemma_empty(a: Self) {
    }

    proof fn lemma_len_bound(a: Self) {
    }

    proof fn lemma_merge_len(a: Self, b: Self) {
    }

    proof fn lemma_assoc(a: Self, b: Self, c: Self) {
    }

    open spec fn plain() -> bool {
        false
    }

    proof fn lemma_plain(a: Self, b: Self) {
    }
}

// ── SlabAggregate for usize (LenWeight) ─────────────────────────────────────

impl SlabAggregate for usize {
    open spec fn spec_len(&self) -> nat {
        *self as nat
    }

    open spec fn spec_merge(l: Self, r: Self) -> Self {
        (l + r) as usize
    }

    open spec fn merge_ok(l: Self, r: Self) -> bool {
        l + r <= usize::MAX
    }

    open spec fn spec_empty() -> Self {
        0
    }

    fn merge(l: &Self, r: &Self) -> Self {
        *l + *r
    }

    fn len(&self) -> usize {
        *self
    }

    fn empty() -> Self {
        0
    }

    proof fn lemma_empty(a: Self) {
    }

    proof fn lemma_len_bound(a: Self) {
    }

    proof fn lemma_merge_len(a: Self, b: Self) {
    }

    proof fn lemma_assoc(a: Self, b: Self, c: Self) {
    }

    open spec fn plain() -> bool {
        true
    }

    proof fn lemma_plain(a: Self, b: Self) {
    }
}

// ── SlabAggregate for PrefixSlabWeight ──────────────────────────────────────

/// `+=` on a prefix type `P`.  `P` comes from the column's user (e.g.
/// automerge's mark sets), so this is the one place the tree trusts code
/// it doesn't verify: `P`'s `+=` must be associative, never overflow, and
/// have `P::default()` as its identity — see the axioms below.
pub uninterp spec fn prefix_add<P>(a: P, b: P) -> P;

/// `P::default()`.
pub uninterp spec fn prefix_zero<P>() -> P;

#[verifier::external_body]
pub proof fn axiom_prefix_add<P>(a: P, b: P, c: P)
    ensures
        prefix_add(prefix_add(a, b), c) == prefix_add(a, prefix_add(b, c)),
        prefix_add(prefix_zero(), a) == a,
        prefix_add(a, prefix_zero()) == a,
{
}

/// A prefix weight's length, as an aggregate: its item count.
pub(crate) proof fn lemma_prefix_len<P: Clone + Default + std::fmt::Debug + AddAssign + SubAssign>(w: PrefixSlabWeight<P>)
    ensures
        w.spec_len() == w.len as nat,
{
}

/// A prefix weight's sum, as an aggregate: its `prefix`.
pub(crate) proof fn lemma_prefix_sum<P: Clone + Default + std::fmt::Debug + AddAssign + SubAssign + Add<Output = P> + for<'a> AddAssign<&'a P>>(w: PrefixSlabWeight<P>)
    ensures
        w.spec_prefix() == w.prefix,
{
}

impl<P> SlabAggregate for PrefixSlabWeight<P>
where
    P: Clone + Default + std::fmt::Debug + AddAssign + SubAssign,
{
    closed spec fn spec_len(&self) -> nat {
        self.len as nat
    }

    closed spec fn spec_merge(l: Self, r: Self) -> Self {
        PrefixSlabWeight { len: (l.len + r.len) as usize, prefix: prefix_add(l.prefix, r.prefix) }
    }

    closed spec fn merge_ok(l: Self, r: Self) -> bool {
        l.len + r.len <= usize::MAX
    }

    closed spec fn spec_empty() -> Self {
        PrefixSlabWeight { len: 0, prefix: prefix_zero() }
    }

    // Trusted: `clone` + `+=` compute `prefix_add` (see above).
    #[verifier::external_body]
    fn merge(l: &Self, r: &Self) -> Self {
        let mut out = l.clone();
        out += r.clone();
        out
    }

    fn len(&self) -> usize {
        self.len
    }

    // Trusted: `P::default()` is `prefix_zero`.
    #[verifier::external_body]
    fn empty() -> Self {
        Self::default()
    }

    proof fn lemma_empty(a: Self) {
        axiom_prefix_add(a.prefix, a.prefix, a.prefix);
    }

    proof fn lemma_len_bound(a: Self) {
    }

    proof fn lemma_merge_len(a: Self, b: Self) {
    }

    proof fn lemma_assoc(a: Self, b: Self, c: Self) {
        axiom_prefix_add(a.prefix, b.prefix, c.prefix);
    }

    open spec fn plain() -> bool {
        true
    }

    proof fn lemma_plain(a: Self, b: Self) {
    }
}

/// Aggregate that additionally carries a prefix-sum value per slab,
/// enabling [`find_slab_at_item`](SlabBTree::find_slab_at_item) and
/// [`find_slab_at_prefix`](SlabBTree::find_slab_at_prefix) to work
/// aggregate-agnostically.
///
/// Implemented by [`PrefixSlabWeight<P>`](crate::prefix::PrefixSlabWeight)
/// (prefix lives in `.prefix`) and [`SlabAgg`] (prefix lives in `.total`).
///
/// For verification, prefix addition is given as mathematics
/// (`prefix_plus`) with laws tying it to `merge`.
pub trait PrefixAggregate: SlabAggregate {
    /// `Ord` is not required here — only forward accumulation needs `Add`.
    /// The "find slab whose prefix reaches target" search is provided in a
    /// separate impl block bounded on `Self::Prefix: Ord`.
    ///
    /// `Clone` (not `Copy`) so non-scalar prefixes (e.g. a `HashMap`-backed
    /// mark set) can serve as the per-slab aggregate.
    ///
    /// `for<'a> AddAssign<&'a Self::Prefix>` lets the B-tree descent
    /// accumulate prefixes via `+=` against the stored aggregate by
    /// reference — avoiding a per-level clone of large non-scalar
    /// prefixes (the dominant cost for `MarkAcc`-style HashMap prefixes).
    /// All scalar primitives (`u128`, `i128`, `u64`, etc.) impl this in
    /// `core` already.
    type Prefix: Clone
        + Default
        + Add<Output = Self::Prefix>
        + Debug
        + for<'a> AddAssign<&'a Self::Prefix>;

    /// [`prefix`](Self::prefix), for specifications.
    spec fn spec_prefix(&self) -> Self::Prefix;

    /// Whether the prefix laws cover this aggregate (for `SlabAgg`: one
    /// covering no items has a zero total, as every computed one does).
    spec fn prefix_ok(&self) -> bool;

    spec fn prefix_zero() -> Self::Prefix;

    /// `+=` on prefixes.
    spec fn prefix_plus(a: Self::Prefix, b: Self::Prefix) -> Self::Prefix;

    /// Whether `a + b` is representable.
    spec fn prefix_plus_ok(a: Self::Prefix, b: Self::Prefix) -> bool;

    /// Borrow-typed accessor — the B-tree descent reads prefixes many
    /// times per query and aggregates by reference, so cloning here would
    /// be wasted work for non-scalar prefixes.
    fn prefix(&self) -> (r: &Self::Prefix)
        ensures *r == self.spec_prefix();

    /// `Prefix::default()`.
    fn zero_prefix() -> (r: Self::Prefix)
        ensures r == Self::prefix_zero();

    /// `*acc += x`.
    fn add_prefix(acc: &mut Self::Prefix, x: &Self::Prefix)
        requires Self::prefix_plus_ok(*old(acc), *x),
        ensures *final(acc) == Self::prefix_plus(*old(acc), *x);

    proof fn lemma_prefix_empty(x: Self::Prefix)
        ensures
            Self::spec_empty().prefix_ok(),
            Self::spec_empty().spec_prefix() == Self::prefix_zero();

    /// A merge's prefix is the sum of its parts'.
    proof fn lemma_prefix_merge(a: Self, b: Self)
        requires Self::merge_ok(a, b), a.prefix_ok(), b.prefix_ok(),
        ensures
            Self::spec_merge(a, b).prefix_ok(),
            Self::prefix_plus_ok(a.spec_prefix(), b.spec_prefix()),
            Self::spec_merge(a, b).spec_prefix() == Self::prefix_plus(a.spec_prefix(), b.spec_prefix());
}

impl PrefixAggregate for SlabAgg {
    type Prefix = i64;

    open spec fn spec_prefix(&self) -> i64 {
        self.total
    }

    open spec fn prefix_ok(&self) -> bool {
        self.len == 0 ==> self.total == 0
    }

    open spec fn prefix_zero() -> i64 {
        0
    }

    open spec fn prefix_plus(a: i64, b: i64) -> i64 {
        (a + b) as i64
    }

    open spec fn prefix_plus_ok(a: i64, b: i64) -> bool {
        in_i64(a + b)
    }

    fn prefix(&self) -> &i64 {
        &self.total
    }

    fn zero_prefix() -> i64 {
        0
    }

    fn add_prefix(acc: &mut i64, x: &i64) {
        *acc += *x;
    }

    proof fn lemma_prefix_empty(x: i64) {
    }

    proof fn lemma_prefix_merge(a: Self, b: Self) {
    }
}

impl<P> PrefixAggregate for PrefixSlabWeight<P>
where
    P: Clone
        + Default
        + std::fmt::Debug
        + AddAssign
        + SubAssign
        + Add<Output = P>
        + for<'a> AddAssign<&'a P>,
{
    type Prefix = P;

    closed spec fn spec_prefix(&self) -> P {
        self.prefix
    }

    open spec fn prefix_ok(&self) -> bool {
        true
    }

    open spec fn prefix_zero() -> P {
        prefix_zero::<P>()
    }

    open spec fn prefix_plus(a: P, b: P) -> P {
        prefix_add(a, b)
    }

    open spec fn prefix_plus_ok(a: P, b: P) -> bool {
        true
    }

    fn prefix(&self) -> &P {
        &self.prefix
    }

    // Trusted: `P::default()` is `prefix_zero` (see `prefix_add`).
    #[verifier::external_body]
    fn zero_prefix() -> P {
        P::default()
    }

    // Trusted: `P`'s `+=` is `prefix_add`.
    #[verifier::external_body]
    fn add_prefix(acc: &mut P, x: &P) {
        *acc += x;
    }

    proof fn lemma_prefix_empty(x: P) {
    }

    proof fn lemma_prefix_merge(a: Self, b: Self) {
    }
}

/// A prefix aggregate whose prefixes can be compared, for
/// [`find_slab_at_prefix`](SlabBTree::find_slab_at_prefix).  Nothing is
/// assumed of the order: the search's contract is stated in terms of
/// whatever `<=` answers.
pub trait OrdPrefix: PrefixAggregate {
    /// `a <= b`.
    spec fn prefix_le(a: Self::Prefix, b: Self::Prefix) -> bool;

    fn le_prefix(a: &Self::Prefix, b: &Self::Prefix) -> (r: bool)
        ensures r == Self::prefix_le(*a, *b);

    /// `a.clone() + b`.
    fn plus_prefix(a: &Self::Prefix, b: &Self::Prefix) -> (r: Self::Prefix)
        requires Self::prefix_plus_ok(*a, *b),
        ensures r == Self::prefix_plus(*a, *b);
}

impl OrdPrefix for SlabAgg {
    open spec fn prefix_le(a: i64, b: i64) -> bool {
        a <= b
    }

    fn le_prefix(a: &i64, b: &i64) -> bool {
        *a <= *b
    }

    fn plus_prefix(a: &i64, b: &i64) -> i64 {
        *a + *b
    }
}

/// `a <= b` on a prefix type `P` (its `Ord`).
pub uninterp spec fn prefix_leq<P>(a: P, b: P) -> bool;

impl<P> OrdPrefix for PrefixSlabWeight<P>
where
    P: Clone
        + Default
        + std::fmt::Debug
        + AddAssign
        + SubAssign
        + Add<Output = P>
        + for<'a> AddAssign<&'a P>
        + Ord,
{
    open spec fn prefix_le(a: P, b: P) -> bool {
        prefix_leq(a, b)
    }

    // Trusted only to name `P`'s `<=`.
    #[verifier::external_body]
    fn le_prefix(a: &P, b: &P) -> bool {
        a <= b
    }

    // Trusted: `clone` + `+=` compute `prefix_add`.
    #[verifier::external_body]
    fn plus_prefix(a: &P, b: &P) -> P {
        let mut r = a.clone();
        r += b;
        r
    }
}

#[derive(Clone, PartialEq, Debug)]
#[verifier::allow(autoderive_clone_without_spec)]
pub(crate) struct FoundSlab<A: PrefixAggregate> {
    pub(crate) index: usize,
    pub(crate) prefix: A::Prefix,
    pub(crate) pos: usize,
}

// ── Nodes ───────────────────────────────────────────────────────────────────

/// Panics: the arena has used every `NodeId`.  Trusted only in that a
/// panic does not return.
#[verifier::external_body]
fn out_of_node_ids()
    ensures false,
{
    panic!("SlabBTree: out of node ids")
}

type NodeId = u32;

#[derive(Debug, Clone)]
#[verifier::allow(autoderive_clone_without_spec)]
enum Node<A: SlabAggregate> {
    Leaf(Leaf<A>),
    Internal(Internal<A>),
}

#[derive(Debug, Clone)]
#[verifier::allow(autoderive_clone_without_spec)]
struct Leaf<A: SlabAggregate> {
    aggs: Vec<A>,
}

#[derive(Debug, Clone)]
#[verifier::allow(autoderive_clone_without_spec)]
struct Internal<A: SlabAggregate> {
    children: Vec<ChildSlot<A>>,
}

/// One entry in an internal node: pointer to a child subtree plus that
/// subtree's aggregate and slab count (redundant with what the child
/// root could recompute, but cached here so top-down walks are
/// constant-time per node).
#[derive(Debug, Clone)]
#[verifier::allow(autoderive_clone_without_spec)]
struct ChildSlot<A: SlabAggregate> {
    id: NodeId,
    agg: A,
    slab_count: usize,
}

// ── Tree ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
#[verifier::allow(autoderive_clone_without_spec)]
pub struct SlabBTree<A: SlabAggregate> {
    nodes: Vec<Option<Node<A>>>,
    free: Vec<NodeId>,
    root: NodeId,
    total_slabs: usize,
    _phantom: PhantomData<A>,
}

// ── The tree as a sequence of slab aggregates ───────────────────────────────

/// The number of items in a sequence of slabs.
pub open spec fn sum_len<A: SlabAggregate>(s: Seq<A>) -> nat
    decreases s.len(),
{
    if s.len() == 0 { 0 } else { sum_len(s.drop_last()) + s.last().spec_len() }
}

pub proof fn lemma_sum_len_concat<A: SlabAggregate>(a: Seq<A>, b: Seq<A>)
    ensures sum_len(a + b) == sum_len(a) + sum_len(b),
    decreases b.len(),
{
    if b.len() == 0 {
        assert(a + b =~= a);
    } else {
        assert((a + b).drop_last() =~= a + b.drop_last());
        lemma_sum_len_concat(a, b.drop_last());
    }
}

/// A run of `s` counts no more items than `s`.
pub proof fn lemma_sum_len_sub<A: SlabAggregate>(s: Seq<A>, a: int, b: int)
    requires 0 <= a <= b <= s.len(),
    ensures sum_len(s.subrange(a, b)) <= sum_len(s),
{
    assert(s =~= s.subrange(0, a) + s.subrange(a, b) + s.subrange(b, s.len() as int));
    lemma_sum_len_concat(s.subrange(0, a), s.subrange(a, b));
    lemma_sum_len_concat(s.subrange(0, a) + s.subrange(a, b), s.subrange(b, s.len() as int));
}

/// The middle of a valid sequence is valid.
pub proof fn lemma_valid_mid<A: SlabAggregate>(l: Seq<A>, m: Seq<A>, r: Seq<A>)
    requires valid(l + m + r),
    ensures valid(m),
{
    let s = l + m + r;
    assert(s.subrange(l.len() as int, (l.len() + m.len()) as int) =~= m);
    lemma_valid_subrange(s, l.len() as int, (l.len() + m.len()) as int);
}

pub proof fn lemma_valid_empty<A: SlabAggregate>()
    ensures valid(Seq::<A>::empty()),
{
    let e = Seq::<A>::empty();
    assert forall|i: int, j: int, k: int| 0 <= i <= j <= k <= e.len()
        implies #[trigger] A::merge_ok(fold(e.subrange(i, j)), fold(e.subrange(j, k))) by {
        assert(e.subrange(i, j) =~= e);
        assert(e.subrange(j, k) =~= e);
        A::lemma_empty(A::spec_empty());
    }
}

/// The aggregate of a sequence of slabs: `merge` from the left, starting
/// from the empty aggregate.
pub open spec fn fold<A: SlabAggregate>(s: Seq<A>) -> A
    decreases s.len(),
{
    if s.len() == 0 { A::spec_empty() } else { A::spec_merge(fold(s.drop_last()), s.last()) }
}

/// Every merge of two adjacent runs of `s` is representable, so the tree
/// may group slabs any way it likes.  For delta columns this is the
/// domain contract (all running sums fit `i64`).
pub open spec fn valid<A: SlabAggregate>(s: Seq<A>) -> bool {
    forall|i: int, j: int, k: int| 0 <= i <= j <= k <= s.len()
        ==> #[trigger] A::merge_ok(fold(s.subrange(i, j)), fold(s.subrange(j, k)))
}

pub proof fn lemma_fold_one<A: SlabAggregate>(s: Seq<A>, i: int)
    requires 0 <= i < s.len(),
    ensures fold(s.subrange(i, i + 1)) == s[i],
{
    let t = s.subrange(i, i + 1);
    assert(t.drop_last() =~= Seq::<A>::empty());
    assert(t.last() == s[i]);
    reveal_with_fuel(fold, 2);
    A::lemma_empty(s[i]);
}

/// Folding a run is merging the folds of its two halves (the second
/// nonempty).
pub proof fn lemma_fold_split<A: SlabAggregate>(s: Seq<A>, i: int, j: int, k: int)
    requires valid(s), 0 <= i <= j < k <= s.len(),
    ensures fold(s.subrange(i, k)) == A::spec_merge(fold(s.subrange(i, j)), fold(s.subrange(j, k))),
    decreases k - j,
{
    assert(s.subrange(i, k).drop_last() =~= s.subrange(i, k - 1));
    lemma_fold_one(s, k - 1);
    if k == j + 1 {
        assert(s.subrange(j, k) =~= s.subrange(k - 1, k));
    } else {
        lemma_fold_split(s, i, j, k - 1);
        assert(s.subrange(j, k).drop_last() =~= s.subrange(j, k - 1));
        let (a, b, c) = (fold(s.subrange(i, j)), fold(s.subrange(j, k - 1)), s[k - 1]);
        assert(A::merge_ok(a, b));
        assert(A::merge_ok(fold(s.subrange(j, k - 1)), fold(s.subrange(k - 1, k))));
        assert(A::merge_ok(fold(s.subrange(i, k - 1)), fold(s.subrange(k - 1, k))));
        assert(A::merge_ok(fold(s.subrange(i, j)), fold(s.subrange(j, k))));
        A::lemma_assoc(a, b, c);
    }
}

/// A run's aggregate covers the run's items.
pub proof fn lemma_fold_len<A: SlabAggregate>(s: Seq<A>, i: int, j: int)
    requires valid(s), 0 <= i <= j <= s.len(),
    ensures fold(s.subrange(i, j)).spec_len() == sum_len(s.subrange(i, j)),
    decreases j - i,
{
    if j == i {
        A::lemma_empty(A::spec_empty());
    } else {
        lemma_fold_len(s, i, j - 1);
        assert(s.subrange(i, j).drop_last() =~= s.subrange(i, j - 1));
        lemma_fold_one(s, j - 1);
        assert(A::merge_ok(fold(s.subrange(i, j - 1)), fold(s.subrange(j - 1, j))));
        A::lemma_merge_len(fold(s.subrange(i, j - 1)), s[j - 1]);
    }
}

/// A run of slabs the prefix laws cover has an aggregate they cover.
pub proof fn lemma_fold_prefix_ok<A: PrefixAggregate>(s: Seq<A>, a: int, b: int)
    requires
        valid(s), 0 <= a <= b <= s.len(),
        forall|i: int| 0 <= i < s.len() ==> #[trigger] s[i].prefix_ok(),
    ensures fold(s.subrange(a, b)).prefix_ok(),
    decreases b - a,
{
    if a == b {
        A::lemma_prefix_empty(A::prefix_zero());
    } else {
        lemma_fold_prefix_ok(s, a, b - 1);
        assert(s.subrange(a, b).drop_last() =~= s.subrange(a, b - 1));
        lemma_fold_one(s, b - 1);
        assert(A::merge_ok(fold(s.subrange(a, b - 1)), fold(s.subrange(b - 1, b))));
        A::lemma_prefix_merge(fold(s.subrange(a, b - 1)), s[b - 1]);
    }
}

/// The prefix of the first `k` slabs, split at `j`.
pub proof fn lemma_fold_prefix<A: PrefixAggregate>(s: Seq<A>, j: int, k: int)
    requires
        valid(s), 0 <= j < k <= s.len(),
        forall|i: int| 0 <= i < s.len() ==> #[trigger] s[i].prefix_ok(),
    ensures
        A::prefix_plus_ok(fold(s.subrange(0, j)).spec_prefix(), fold(s.subrange(j, k)).spec_prefix()),
        fold(s.subrange(0, k)).spec_prefix()
            == A::prefix_plus(fold(s.subrange(0, j)).spec_prefix(), fold(s.subrange(j, k)).spec_prefix()),
{
    lemma_fold_prefix_ok(s, 0, j);
    lemma_fold_prefix_ok(s, j, k);
    lemma_fold_split(s, 0, j, k);
    assert(A::merge_ok(fold(s.subrange(0, j)), fold(s.subrange(j, k))));
    A::lemma_prefix_merge(fold(s.subrange(0, j)), fold(s.subrange(j, k)));
}

/// The prefix of the first `j` slabs.
pub open spec fn pre_at<A: PrefixAggregate>(s: Seq<A>, j: int) -> A::Prefix {
    fold(s.subrange(0, j)).spec_prefix()
}

/// The prefix at slab end `j` (the first `j` slabs) reaches `target`.
pub open spec fn reached<A: OrdPrefix>(s: Seq<A>, target: A::Prefix, j: int) -> bool {
    A::prefix_le(target, pre_at(s, j))
}

/// Every slab end up to `n` that reaches `target` is followed, by `n`,
/// by one that doesn't.
#[verifier::opaque]
pub open spec fn passed<A: OrdPrefix>(s: Seq<A>, target: A::Prefix, n: int) -> bool {
    forall|k: int| 0 < k <= n && #[trigger] reached(s, target, k)
        ==> exists|j: int| k < j <= n && !(#[trigger] reached(s, target, j))
}

/// Once the running prefix reaches `target` it stays there, as for
/// unsigned prefixes.
#[verifier::opaque]
pub open spec fn stays_reached<A: OrdPrefix>(s: Seq<A>, target: A::Prefix) -> bool {
    forall|i: int, j: int| 0 <= i <= j <= s.len() && #[trigger] reached(s, target, i)
        ==> #[trigger] reached(s, target, j)
}

pub proof fn lemma_passed_step<A: OrdPrefix>(s: Seq<A>, target: A::Prefix, n: int, e: int)
    requires passed(s, target, n), n < e, !reached(s, target, e),
    ensures passed(s, target, e),
{
    reveal(passed);
    assert forall|k: int| 0 < k <= e && #[trigger] reached(s, target, k)
        implies exists|j: int| k < j <= e && !(#[trigger] reached(s, target, j)) by {
        if k <= n {
            let j = choose|j: int| k < j <= n && !(#[trigger] reached(s, target, j));
            assert(k < j <= e && !reached(s, target, j));
        } else {
            assert(k < e && !reached(s, target, e));
        }
    }
}

/// When the prefix stays reached, passing slab ends means none reached.
pub proof fn lemma_passed_first<A: OrdPrefix>(s: Seq<A>, target: A::Prefix, n: int)
    requires passed(s, target, n), stays_reached(s, target), n <= s.len(),
    ensures forall|k: int| 0 < k <= n ==> !#[trigger] reached(s, target, k),
{
    reveal(passed);
    reveal(stays_reached);
    assert forall|k: int| 0 < k <= n implies !#[trigger] reached(s, target, k) by {
        if reached(s, target, k) {
            let j = choose|j: int| k < j <= n && !(#[trigger] reached(s, target, j));
            assert(reached(s, target, j));
        }
    }
}

/// The middle part `b` of `a + b + c`, placed between `l` and `r`.
pub proof fn lemma_seq_part<T>(l: Seq<T>, a: Seq<T>, b: Seq<T>, c: Seq<T>, r: Seq<T>)
    ensures
        (l + (a + b + c) + r).subrange((l.len() + a.len()) as int, (l.len() + a.len() + b.len()) as int) == b,
        l + (a + b + c) + r == (l + a) + b + (c + r),
{
    assert((l + (a + b + c) + r).subrange((l.len() + a.len()) as int, (l.len() + a.len() + b.len()) as int) =~= b);
    assert(l + (a + b + c) + r =~= (l + a) + b + (c + r));
}

/// Runs of a valid sequence are valid.
pub proof fn lemma_valid_subrange<A: SlabAggregate>(s: Seq<A>, a: int, b: int)
    requires valid(s), 0 <= a <= b <= s.len(),
    ensures valid(s.subrange(a, b)),
{
    let t = s.subrange(a, b);
    assert forall|i: int, j: int, k: int| 0 <= i <= j <= k <= t.len()
        implies #[trigger] A::merge_ok(fold(t.subrange(i, j)), fold(t.subrange(j, k))) by {
        assert(t.subrange(i, j) =~= s.subrange(a + i, a + j));
        assert(t.subrange(j, k) =~= s.subrange(a + j, a + k));
        assert(A::merge_ok(fold(s.subrange(a + i, a + j)), fold(s.subrange(a + j, a + k))));
    }
}


/// `s` with `a..b` replaced by `n`.
pub open spec fn spliced<T>(s: Seq<T>, a: int, b: int, n: Seq<T>) -> Seq<T> {
    s.subrange(0, a) + n + s.subrange(b, s.len() as int)
}

/// Splicing within one part of a concatenation.
pub proof fn lemma_splice_in_part<T>(p: Seq<T>, c: Seq<T>, q: Seq<T>, start: int, end: int, n: Seq<T>)
    requires p.len() <= start <= end <= p.len() + c.len(),
    ensures
        spliced(p + c + q, start, end, n)
            == p + spliced(c, start - p.len(), end - p.len(), n) + q,
{
    let s = p + c + q;
    assert(spliced(s, start, end, n) =~= p + spliced(c, start - p.len(), end - p.len(), n) + q);
}

/// Where the piece of a splice that falls to one of several parts lands
/// in the result: the part at `pk..pk1` of `s` takes its overlap with
/// `start..end` (`lo..hi`, local) and the stretch of `n` that covers it
/// at the same length (`cut_k..cut_k1`), the last part whatever is left.
pub proof fn lemma_splice_piece<T>(
    s: Seq<T>, start: int, end: int, n: Seq<T>, pk: int, pk1: int, first: bool, last: bool,
)
    requires
        0 <= pk <= pk1 <= s.len(), 0 <= start < end <= s.len(),
        first ==> pk <= start < pk1 && !last,
        !first ==> start < pk,
        last ==> pk < end <= pk1,
        !last ==> pk1 < end,
    ensures ({
        let m = n.len() as int;
        let lo = if start > pk { start - pk } else { 0 };
        let hi = if end < pk1 { end - pk } else { pk1 - pk };
        let cut_k = if first { 0 } else if pk - start < m { pk - start } else { m };
        let cut_k1 = if last { m } else if pk1 - start < m { pk1 - start } else { m };
        let q_k = if first { pk } else { start + cut_k };
        let q_k1 = if last { start + m + (pk1 - end) } else { start + cut_k1 };
        &&& 0 <= cut_k <= cut_k1 <= m
        &&& 0 <= lo <= hi <= pk1 - pk
        &&& 0 <= q_k <= q_k1 <= spliced(s, start, end, n).len()
        &&& spliced(s.subrange(pk, pk1), lo, hi, n.subrange(cut_k, cut_k1))
            == spliced(s, start, end, n).subrange(q_k, q_k1)
    }),
{
    let m = n.len() as int;
    let f = spliced(s, start, end, n);
    let lo = if start > pk { start - pk } else { 0 };
    let hi = if end < pk1 { end - pk } else { pk1 - pk };
    let cut_k = if first { 0 } else if pk - start < m { pk - start } else { m };
    let cut_k1 = if last { m } else if pk1 - start < m { pk1 - start } else { m };
    let q_k = if first { pk } else { start + cut_k };
    let q_k1 = if last { start + m + (pk1 - end) } else { start + cut_k1 };
    let c = s.subrange(pk, pk1);
    let piece = spliced(c, lo, hi, n.subrange(cut_k, cut_k1));
    lemma_spliced_index(s, start, end, n);
    assert(piece.len() == q_k1 - q_k);
    assert forall|i: int| 0 <= i < piece.len() implies #[trigger] piece[i] == f[q_k + i] by {
        if i < lo {
            assert(piece[i] == c[i]);
            assert(c[i] == s[pk + i]);
        } else if i < lo + (cut_k1 - cut_k) {
            assert(piece[i] == n[cut_k + i - lo]);
        } else {
            assert(piece[i] == c[hi + i - lo - (cut_k1 - cut_k)]);
            assert(c[hi + i - lo - (cut_k1 - cut_k)] == s[pk + hi + i - lo - (cut_k1 - cut_k)]);
        }
    }
    assert(piece =~= f.subrange(q_k, q_k1));
}

/// Where each slab of a splice's result comes from.
pub proof fn lemma_spliced_index<T>(s: Seq<T>, start: int, end: int, n: Seq<T>)
    requires 0 <= start <= end <= s.len(),
    ensures
        spliced(s, start, end, n).len() == s.len() - (end - start) + n.len(),
        forall|i: int| 0 <= i < start ==> #[trigger] spliced(s, start, end, n)[i] == s[i],
        forall|j: int| 0 <= j < n.len() ==> #[trigger] spliced(s, start, end, n)[start + j] == n[j],
        forall|j: int| 0 <= j < s.len() - end ==> #[trigger] spliced(s, start, end, n)[start + n.len() + j] == s[end + j],
{
}

/// How much of `n` (length `m`) children `ks..j` take in a splice of
/// `start..end` spanning children `ks..=ke` (child `j` starting at `pj`).
pub open spec fn cut_at(j: int, ks: int, ke: int, m: int, start: int, pj: int) -> int {
    if j == ke + 1 { m } else if j == ks { 0 } else if pj - start < m { pj - start } else { m }
}

/// Where child `j`'s share starts in the splice's result.
pub open spec fn share_at(j: int, ks: int, ke: int, m: int, start: int, end: int, pj: int) -> int {
    if j == ks { pj } else if j == ke + 1 { start + m + (pj - end) } else { start + cut_at(j, ks, ke, m, start, pj) }
}

/// The slab counts of the first `k` slots.
spec fn counts<A: SlabAggregate>(cs: Seq<ChildSlot<A>>, k: int) -> nat
    decreases k,
{
    if k <= 0 { 0 } else { counts(cs, k - 1) + cs[k - 1].slab_count as nat }
}

/// Where chunk `c` starts when `n` items are cut into chunks of `base`
/// items, the first `rem` of them one bigger.
pub open spec fn chunk_at(c: int, base: int, rem: int) -> int {
    c * base + if c < rem { c } else { rem }
}

/// Chunk `c` of `num` starts before chunk `c + 1`, within the items.
pub proof fn lemma_chunk(num: int, base: int, rem: int, c: int)
    requires num >= 1, base >= 1, 0 <= rem < num, 0 <= c < num,
    ensures
        chunk_at(0, base, rem) == 0,
        chunk_at(num, base, rem) == num * base + rem,
        0 <= chunk_at(c, base, rem) < chunk_at(c + 1, base, rem) <= num * base + rem,
{
    assert((c + 1) * base == c * base + base) by (nonlinear_arith);
    assert((c + 1) * base <= num * base) by (nonlinear_arith) requires c + 1 <= num, base >= 1;
    assert(c * base >= 0) by (nonlinear_arith) requires c >= 0, base >= 1;
}

impl<A: SlabAggregate> SlabBTree<A> {
    spec fn node_spec(&self, id: NodeId) -> Option<Node<A>> {
        if (id as int) < self.nodes@.len() { self.nodes@[id as int] } else { None }
    }

    /// The slabs under node `id`, read as a subtree of height `h` (a
    /// leaf has height 0), in order.
    spec fn slabs(&self, id: NodeId, h: nat) -> Seq<A>
        decreases h, 0nat,
    {
        match self.node_spec(id) {
            Some(Node::Leaf(l)) => l.aggs@,
            Some(Node::Internal(n)) => if h == 0 {
                Seq::empty()
            } else {
                self.children_slabs(n.children@, (h - 1) as nat)
            },
            None => Seq::empty(),
        }
    }

    /// The slabs under a run of sibling subtrees of height `h`.
    spec fn children_slabs(&self, cs: Seq<ChildSlot<A>>, h: nat) -> Seq<A>
        decreases h, cs.len(),
    {
        if cs.len() == 0 {
            Seq::empty()
        } else {
            self.children_slabs(cs.drop_last(), h) + self.slabs(cs.last().id, h)
        }
    }

    /// The node ids in the subtree at `id`, read at height `h`.
    spec fn owned(&self, id: NodeId, h: nat) -> Set<NodeId>
        decreases h, 0nat,
    {
        match self.node_spec(id) {
            Some(Node::Internal(n)) => if h == 0 {
                set![id]
            } else {
                set![id] + self.children_owned(n.children@, (h - 1) as nat)
            },
            _ => set![id],
        }
    }

    spec fn children_owned(&self, cs: Seq<ChildSlot<A>>, h: nat) -> Set<NodeId>
        decreases h, cs.len(),
    {
        if cs.len() == 0 {
            Set::empty()
        } else {
            self.children_owned(cs.drop_last(), h) + self.owned(cs.last().id, h)
        }
    }

    /// Node `id` is a tree of height `h` whose cached child slab counts
    /// and item counts are right.
    spec fn wf_node(&self, id: NodeId, h: nat) -> bool
        decreases h, 1nat,
    {
        &&& self.slabs(id, h).len() <= usize::MAX
        &&& sum_len(self.slabs(id, h)) <= usize::MAX
        &&& match self.node_spec(id) {
            Some(Node::Leaf(_)) => h == 0,
            Some(Node::Internal(n)) => h > 0 && n.children@.len() > 0 && self.wf_children(id, n, h),
            None => false,
        }
    }

    /// An internal node's children: well formed one level down, sharing
    /// no nodes with each other or with it, with right cached slots.
    #[verifier::opaque]
    spec fn wf_children(&self, id: NodeId, n: Internal<A>, h: nat) -> bool
        decreases h, 0nat,
    {
        h > 0 && (forall|i: int, j: int|
            0 <= i < j < n.children@.len() ==> #[trigger] self.owned(n.children@[i].id, (h - 1) as nat)
                .disjoint(#[trigger] self.owned(n.children@[j].id, (h - 1) as nat)))
        && forall|i: int| 0 <= i < n.children@.len() ==> {
            let c = #[trigger] n.children@[i];
            &&& self.wf_node(c.id, (h - 1) as nat)
            &&& !self.owned(c.id, (h - 1) as nat).contains(id)
            &&& c.slab_count == self.slabs(c.id, (h - 1) as nat).len()
            &&& c.slab_count > 0
            &&& valid(self.slabs(c.id, (h - 1) as nat)) ==> c.agg == fold(self.slabs(c.id, (h - 1) as nat))
            &&& c.agg.spec_len() == sum_len(self.slabs(c.id, (h - 1) as nat))
        }
    }

    /// `cs` are slots for well-formed sibling subtrees of height `h`, with
    /// right cached counts and aggregates, sharing no nodes.
    #[verifier::opaque]
    spec fn slots_ok(&self, cs: Seq<ChildSlot<A>>, h: nat) -> bool {
        &&& forall|i: int, j: int| 0 <= i < j < cs.len()
            ==> #[trigger] self.owned(cs[i].id, h).disjoint(#[trigger] self.owned(cs[j].id, h))
        &&& forall|i: int| 0 <= i < cs.len() ==> {
            let c = #[trigger] cs[i];
            &&& self.wf_node(c.id, h)
            &&& c.slab_count == self.slabs(c.id, h).len()
            &&& c.slab_count > 0
            &&& valid(self.slabs(c.id, h)) ==> c.agg == fold(self.slabs(c.id, h))
            &&& c.agg.spec_len() == sum_len(self.slabs(c.id, h))
        }
    }

    /// A well-formed tree's slabs hold fewer than `usize::MAX` items.
    pub proof fn lemma_wf_len(&self)
        requires self.wf(),
        ensures sum_len(self@) <= usize::MAX,
    {
    }

    pub closed spec fn height(&self) -> nat {
        choose|h: nat| self.wf_node(self.root, h)
    }

    /// The free list names distinct empty slots, and every id fits a
    /// `NodeId`.
    spec fn arena_ok(&self) -> bool {
        &&& self.free@.no_duplicates()
        &&& forall|i: int| 0 <= i < self.free@.len()
            ==> (#[trigger] self.free@[i] as int) < self.nodes@.len() && self.nodes@[self.free@[i] as int] is None
        &&& self.nodes@.len() <= u32::MAX + 1
    }

    /// The tree invariant.
    pub closed spec fn wf(&self) -> bool {
        &&& self.arena_ok()
        &&& exists|h: nat| self.wf_node(self.root, h)
        &&& self.total_slabs == self@.len()
        &&& sum_len(self@) <= usize::MAX
    }

    proof fn lemma_children_slabs_push(&self, cs: Seq<ChildSlot<A>>, k: int, h: nat)
        requires 0 <= k < cs.len(),
        ensures
            self.children_slabs(cs.take(k + 1), h)
                == self.children_slabs(cs.take(k), h) + self.slabs(cs[k].id, h),
    {
        assert(cs.take(k + 1).drop_last() =~= cs.take(k));
    }

    proof fn lemma_children_slabs_concat(&self, a: Seq<ChildSlot<A>>, b: Seq<ChildSlot<A>>, h: nat)
        ensures self.children_slabs(a + b, h) == self.children_slabs(a, h) + self.children_slabs(b, h),
        decreases b.len(),
    {
        if b.len() == 0 {
            assert(a + b =~= a);
            assert(self.children_slabs(a, h) + Seq::<A>::empty() =~= self.children_slabs(a, h));
        } else {
            assert((a + b).drop_last() =~= a + b.drop_last());
            self.lemma_children_slabs_concat(a, b.drop_last(), h);
            assert(self.children_slabs(a + b, h) =~= self.children_slabs(a, h)
                + (self.children_slabs(b.drop_last(), h) + self.slabs(b.last().id, h)));
        }
    }

    /// Splitting a node's children at `k`: what lies before child `k`,
    /// child `k`'s own slabs, and what lies after.
    proof fn lemma_children_slabs_split(&self, cs: Seq<ChildSlot<A>>, k: int, h: nat)
        requires 0 <= k < cs.len(),
        ensures
            self.children_slabs(cs, h) == self.children_slabs(cs.take(k), h)
                + self.slabs(cs[k].id, h) + self.children_slabs(cs.skip(k + 1), h),
    {
        assert(cs =~= cs.take(k + 1) + cs.skip(k + 1));
        self.lemma_children_slabs_concat(cs.take(k + 1), cs.skip(k + 1), h);
        self.lemma_children_slabs_push(cs, k, h);
    }

    /// What a well-formed internal node says about its `k`-th child.
    proof fn lemma_child(&self, id: NodeId, h: nat, n: Internal<A>, k: int)
        requires
            self.wf_node(id, h),
            self.node_spec(id) == Some(Node::Internal(n)),
            0 <= k < n.children@.len(),
        ensures
            h > 0, n.children@.len() > 0,
            self.slabs(id, h) == self.children_slabs(n.children@, (h - 1) as nat),
            self.owned(id, h) == set![id] + self.children_owned(n.children@, (h - 1) as nat),
            !self.owned(n.children@[k].id, (h - 1) as nat).contains(id),
            forall|j: int| 0 <= j < n.children@.len() && j != k
                ==> self.owned(n.children@[k].id, (h - 1) as nat).disjoint(self.owned(n.children@[j].id, (h - 1) as nat)),
            self.wf_node(n.children@[k].id, (h - 1) as nat),
            n.children@[k].slab_count == self.slabs(n.children@[k].id, (h - 1) as nat).len(),
            n.children@[k].slab_count > 0,
            valid(self.slabs(n.children@[k].id, (h - 1) as nat))
                ==> n.children@[k].agg == fold(self.slabs(n.children@[k].id, (h - 1) as nat)),
            n.children@[k].agg.spec_len() == sum_len(self.slabs(n.children@[k].id, (h - 1) as nat)),
    {
        reveal(SlabBTree::wf_children);
        let c = n.children@[k];
        assert(c == n.children@[k]);
        assert forall|j: int| 0 <= j < n.children@.len() && j != k
            implies self.owned(n.children@[k].id, (h - 1) as nat).disjoint(self.owned(n.children@[j].id, (h - 1) as nat)) by {
            if j < k {
                assert(self.owned(n.children@[j].id, (h - 1) as nat).disjoint(self.owned(n.children@[k].id, (h - 1) as nat)));
            } else {
                assert(self.owned(n.children@[k].id, (h - 1) as nat).disjoint(self.owned(n.children@[j].id, (h - 1) as nat)));
            }
        }
    }

    /// A well-formed subtree has one height.
    proof fn lemma_height_unique(&self, id: NodeId, h1: nat, h2: nat)
        requires self.wf_node(id, h1), self.wf_node(id, h2),
        ensures h1 == h2,
        decreases h1,
    {
        reveal(SlabBTree::wf_children);
        if let Some(Node::Internal(n)) = self.node_spec(id) {
            self.lemma_child(id, h1, n, 0);
            self.lemma_child(id, h2, n, 0);
            self.lemma_height_unique(n.children@[0].id, (h1 - 1) as nat, (h2 - 1) as nat);
        }
    }

    /// Frame: a subtree means the same in any tree that agrees on its
    /// nodes.
    proof fn lemma_frame(t1: &Self, t2: &Self, id: NodeId, h: nat)
        requires forall|x: NodeId| t1.owned(id, h).contains(x) ==> t1.node_spec(x) == t2.node_spec(x),
        ensures
            t1.slabs(id, h) == t2.slabs(id, h),
            t1.owned(id, h) == t2.owned(id, h),
            t1.wf_node(id, h) == t2.wf_node(id, h),
        decreases h, 0nat,
    {
        reveal(SlabBTree::wf_children);
        assert(t1.owned(id, h).contains(id));
        if let Some(Node::Internal(n)) = t1.node_spec(id) {
            if h > 0 {
                let hc = (h - 1) as nat;
                Self::lemma_frame_children(t1, t2, n.children@, hc);
                assert(t1.owned(id, h) =~= t2.owned(id, h));
                if t1.wf_node(id, h) != t2.wf_node(id, h) {
                    assert(t1.slabs(id, h) == t2.slabs(id, h));
                    assert forall|i: int, j: int| 0 <= i < j < n.children@.len() implies
                        t1.owned(n.children@[i].id, hc) == t2.owned(n.children@[i].id, hc)
                        && t1.owned(n.children@[j].id, hc) == t2.owned(n.children@[j].id, hc) by {}
                }
            }
        }
    }

    proof fn lemma_frame_children(t1: &Self, t2: &Self, cs: Seq<ChildSlot<A>>, h: nat)
        requires forall|x: NodeId| t1.children_owned(cs, h).contains(x) ==> t1.node_spec(x) == t2.node_spec(x),
        ensures
            t1.children_slabs(cs, h) == t2.children_slabs(cs, h),
            t1.children_owned(cs, h) == t2.children_owned(cs, h),
            forall|i: int| 0 <= i < cs.len() ==> {
                &&& t1.slabs(#[trigger] cs[i].id, h) == t2.slabs(cs[i].id, h)
                &&& t1.owned(cs[i].id, h) == t2.owned(cs[i].id, h)
                &&& t1.wf_node(cs[i].id, h) == t2.wf_node(cs[i].id, h)
            },
        decreases h, cs.len() + 1,
    {
        if cs.len() > 0 {
            Self::lemma_frame_children(t1, t2, cs.drop_last(), h);
            Self::lemma_frame(t1, t2, cs.last().id, h);
            assert forall|i: int| 0 <= i < cs.len() implies {
                &&& t1.slabs(#[trigger] cs[i].id, h) == t2.slabs(cs[i].id, h)
                &&& t1.owned(cs[i].id, h) == t2.owned(cs[i].id, h)
                &&& t1.wf_node(cs[i].id, h) == t2.wf_node(cs[i].id, h)
            } by {
                if i < cs.len() - 1 { assert(cs.drop_last()[i] == cs[i]); }
            }
        }
    }

    /// Congruence: two child lists naming subtrees with the same slabs
    /// and nodes stand for the same slabs and nodes.
    proof fn lemma_children_congr(t1: &Self, cs1: Seq<ChildSlot<A>>, t2: &Self, cs2: Seq<ChildSlot<A>>, h: nat)
        requires
            cs1.len() == cs2.len(),
            forall|i: int| 0 <= i < cs1.len() ==> t1.slabs(#[trigger] cs1[i].id, h) == t2.slabs(cs2[i].id, h)
                && t1.owned(cs1[i].id, h) == t2.owned(cs2[i].id, h),
        ensures
            t1.children_slabs(cs1, h) == t2.children_slabs(cs2, h),
            t1.children_owned(cs1, h) == t2.children_owned(cs2, h),
        decreases cs1.len(),
    {
        if cs1.len() > 0 {
            assert forall|i: int| 0 <= i < cs1.drop_last().len() implies
                t1.slabs(#[trigger] cs1.drop_last()[i].id, h) == t2.slabs(cs2.drop_last()[i].id, h)
                && t1.owned(cs1.drop_last()[i].id, h) == t2.owned(cs2.drop_last()[i].id, h) by {
                assert(cs1.drop_last()[i] == cs1[i]);
            }
            Self::lemma_children_congr(t1, cs1.drop_last(), t2, cs2.drop_last(), h);
            assert(t1.slabs(cs1[cs1.len() - 1].id, h) == t2.slabs(cs2[cs1.len() - 1].id, h));
        }
    }

    /// Merge a leaf's slab aggregates: their fold, where that is
    /// representable, and in any case their item count.
    fn fold_leaf(aggs: &[A]) -> (r: A)
        requires sum_len(aggs@) <= usize::MAX,
        ensures
            r.spec_len() == sum_len(aggs@),
            valid(aggs@) ==> r == fold(aggs@),
    {
        let mut acc = A::empty();
        proof {
            A::lemma_empty(acc);
            assert(aggs@.subrange(0, 0) =~= Seq::<A>::empty());
        }
        let mut i: usize = 0;
        while i < aggs.len()
            invariant
                i <= aggs@.len(),
                sum_len(aggs@) <= usize::MAX,
                acc.spec_len() == sum_len(aggs@.subrange(0, i as int)),
                valid(aggs@) ==> acc == fold(aggs@.subrange(0, i as int)),
            decreases aggs@.len() - i,
        {
            proof {
                assert(aggs@.subrange(0, i + 1).drop_last() =~= aggs@.subrange(0, i as int));
                assert(aggs@ =~= aggs@.subrange(0, i + 1) + aggs@.subrange(i + 1, aggs@.len() as int));
                lemma_sum_len_concat(aggs@.subrange(0, i + 1), aggs@.subrange(i + 1, aggs@.len() as int));
                if valid(aggs@) {
                    lemma_fold_one(aggs@, i as int);
                    assert(A::merge_ok(fold(aggs@.subrange(0, i as int)), fold(aggs@.subrange(i as int, i + 1))));
                }
            }
            acc = A::merge(&acc, &aggs[i]);
            i += 1;
        }
        assert(aggs@.subrange(0, i as int) =~= aggs@);
        acc
    }

    /// Merge an internal node's cached child aggregates: the fold of its
    /// slabs', where that is representable, and in any case their item
    /// count.
    fn fold_children(&self, Ghost(id): Ghost<NodeId>, n: &Internal<A>, Ghost(h): Ghost<nat>) -> (r: A)
        requires self.wf_node(id, h), self.node_spec(id) == Some(Node::Internal(*n)),
        ensures
            r.spec_len() == sum_len(self.slabs(id, h)),
            valid(self.slabs(id, h)) ==> r == fold(self.slabs(id, h)),
    {
        let ghost cs = n.children@;
        let ghost hc = (h - 1) as nat;
        let ghost all = self.children_slabs(cs, hc);
        proof {
            if cs.len() > 0 { self.lemma_child(id, h, *n, 0); } else { assert(self.wf_node(id, h)); }
        }
        let mut acc = A::empty();
        proof {
            A::lemma_empty(acc);
            assert(cs.take(0) =~= Seq::<ChildSlot<A>>::empty());
            self.lemma_children_slabs_split(cs, 0, hc);
        }
        let mut k: usize = 0;
        while k < n.children.len()
            invariant
                k <= cs.len(), cs == n.children@, h > 0, hc == h - 1,
                self.wf_node(id, h), self.node_spec(id) == Some(Node::Internal(*n)),
                all == self.slabs(id, h),
                sum_len(all) <= usize::MAX,
                acc.spec_len() == sum_len(self.children_slabs(cs.take(k as int), hc)),
                valid(all) ==> acc == fold(self.children_slabs(cs.take(k as int), hc)),
            decreases cs.len() - k,
        {
            let ghost before = self.children_slabs(cs.take(k as int), hc);
            let ghost mid = self.slabs(cs[k as int].id, hc);
            proof {
                self.lemma_child(id, h, *n, k as int);
                self.lemma_children_slabs_split(cs, k as int, hc);
                self.lemma_children_slabs_push(cs, k as int, hc);
                let (p, q) = (before.len() as int, (before.len() + mid.len()) as int);
                assert(all.subrange(0, p) =~= before);
                assert(all.subrange(p, q) =~= mid);
                assert(all.subrange(0, q) =~= before + mid);
                assert(all =~= all.subrange(0, q) + all.subrange(q, all.len() as int));
                lemma_sum_len_concat(before, mid);
                lemma_sum_len_concat(all.subrange(0, q), all.subrange(q, all.len() as int));
                if valid(all) {
                    lemma_valid_subrange(all, p, q);
                    lemma_fold_split(all, 0, p, q);
                    assert(A::merge_ok(fold(all.subrange(0, p)), fold(all.subrange(p, q))));
                }
            }
            acc = A::merge(&acc, &n.children[k].agg);
            k += 1;
        }
        assert(cs.take(k as int) =~= cs);
        acc
    }

    fn node_mut(&mut self, id: NodeId) -> (r: &mut Node<A>)
        requires old(self).node_spec(id) is Some,
        ensures
            Some(*r) == old(self).node_spec(id),
            final(self).nodes@ == old(self).nodes@.update(id as int, Some(*final(r))),
            final(self).root == old(self).root,
            final(self).total_slabs == old(self).total_slabs,
            final(self).free == old(self).free,
    {
        self.nodes[id as usize].as_mut().expect("freed node id")
    }

    /// Aggregate of all slabs under node `id`: their fold, where that is
    /// representable, and in any case their item count.
    fn subtree_agg(&self, id: NodeId, Ghost(h): Ghost<nat>) -> (r: A)
        requires self.wf_node(id, h),
        ensures
            r.spec_len() == sum_len(self.slabs(id, h)),
            valid(self.slabs(id, h)) ==> r == fold(self.slabs(id, h)),
    {
        match self.node(id) {
            Node::Leaf(l) => Self::fold_leaf(&l.aggs),
            Node::Internal(n) => self.fold_children(Ghost(id), n, Ghost(h)),
        }
    }

    /// Replace slab `slab_idx`'s aggregate.
    pub(crate) fn update_slab(&mut self, slab_idx: usize, new_agg: A)
        requires
            old(self).wf(),
            slab_idx < old(self)@.len(),
            sum_len(old(self)@.update(slab_idx as int, new_agg)) <= usize::MAX,
        ensures
            final(self).wf(),
            final(self)@ == old(self)@.update(slab_idx as int, new_agg),
    {
        assert!(slab_idx < self.total_slabs, "update_slab out of bounds");
        let ghost h = self.height();
        proof { assert(self.wf_node(self.root, h)); }
        self.update_rec(self.root, Ghost(h), slab_idx, new_agg);
        proof {
            assert(self.wf_node(self.root, h));
            assert forall|i: int| 0 <= i < self.free@.len() implies
                (#[trigger] self.free@[i] as int) < self.nodes@.len() && self.nodes@[self.free@[i] as int] is None by {
                let x = self.free@[i];
                assert(old(self).free@[i] == x);
                assert(old(self).node_spec(x) is None);
                if old(self).owned(old(self).root, h).contains(x) {
                    old(self).lemma_owned_live(old(self).root, h, x);
                }
                assert(self.node_spec(x) == old(self).node_spec(x));
            }
            self.lemma_height_unique(self.root, h, self.height());
        }
    }

    /// Replace slab `idx` of the subtree at `id` (height `h`), refreshing
    /// the cached child aggregates on the way back up.  Only the subtree's
    /// own nodes change.
    fn update_rec(&mut self, id: NodeId, Ghost(h): Ghost<nat>, idx: usize, new_agg: A)
        requires
            old(self).wf_node(id, h),
            idx < old(self).slabs(id, h).len(),
            sum_len(old(self).slabs(id, h).update(idx as int, new_agg)) <= usize::MAX,
        ensures
            final(self).wf_node(id, h),
            final(self).slabs(id, h) == old(self).slabs(id, h).update(idx as int, new_agg),
            final(self).owned(id, h) == old(self).owned(id, h),
            final(self).nodes@.len() == old(self).nodes@.len(),
            forall|x: NodeId| !old(self).owned(id, h).contains(x)
                ==> #[trigger] final(self).node_spec(x) == old(self).node_spec(x),
            final(self).root == old(self).root,
            final(self).total_slabs == old(self).total_slabs,
            final(self).free == old(self).free,
        decreases h,
    {
        let ghost t0 = *self;
        let (child_idx, child_id, remaining) = match self.node(id) {
            Node::Leaf(_) => {
                let r = self.node_mut(id);
                if let Node::Leaf(l) = r {
                    l.aggs[idx] = new_agg;
                }
                proof {
                    assert forall|x: NodeId| !t0.owned(id, h).contains(x)
                        implies #[trigger] self.node_spec(x) == t0.node_spec(x) by {
                        assert(t0.owned(id, h).contains(id));
                    }
                    assert(self.owned(id, h) =~= t0.owned(id, h));
                    assert(self.slabs(id, h) =~= t0.slabs(id, h).update(idx as int, new_agg));
                }
                return;
            },
            Node::Internal(n) => {
                let ghost cs = n.children@;
                let ghost hc = (h - 1) as nat;
                proof { self.lemma_child(id, h, *n, 0); }
                let mut remaining = idx;
                let mut k: usize = 0;
                let ghost len = n.children.len();
                loop
                    invariant
                        k < cs.len(), cs == n.children@, h > 0, hc == h - 1, len == cs.len(),
                        self.wf_node(id, h), self.node_spec(id) == Some(Node::Internal(*n)),
                        self.slabs(id, h) == self.children_slabs(cs, hc),
                        self.children_slabs(cs.take(k as int), hc).len() + remaining == idx,
                        idx < self.slabs(id, h).len(),
                    ensures
                        k < cs.len(),
                        self.children_slabs(cs.take(k as int), hc).len() + remaining == idx,
                        remaining < cs[k as int].slab_count,
                    decreases cs.len() - k,
                {
                    let c = &n.children[k];
                    proof {
                        self.lemma_child(id, h, *n, k as int);
                        self.lemma_children_slabs_split(cs, k as int, hc);
                        self.lemma_children_slabs_push(cs, k as int, hc);
                    }
                    if remaining < c.slab_count {
                        break;
                    }
                    remaining -= c.slab_count;
                    proof {
                        if k + 1 == cs.len() {
                            assert(cs.take(k + 1) =~= cs);
                        }
                    }
                    k += 1;
                }
                (k, n.children[k].id, remaining)
            },
        };
        let ghost n0 = match t0.node_spec(id) { Some(Node::Internal(n)) => n, _ => arbitrary() };
        let ghost cs = n0.children@;
        let ghost hc = (h - 1) as nat;
        let ghost before = t0.children_slabs(cs.take(child_idx as int), hc);
        let ghost mid = t0.slabs(child_id, hc);
        let ghost after = t0.children_slabs(cs.skip(child_idx + 1), hc);
        let ghost all = t0.slabs(id, h);
        let ghost new_all = all.update(idx as int, new_agg);
        proof {
            assert(child_idx < cs.len());
            assert(child_id == cs[child_idx as int].id);
            assert(before.len() + remaining == idx);
            t0.lemma_child(id, h, n0, child_idx as int);
            t0.lemma_children_slabs_split(cs, child_idx as int, hc);
            // the child's slabs, updated, are a run of the node's
            let (p, q) = (before.len() as int, (before.len() + mid.len()) as int);
            assert(new_all.subrange(p, q) =~= mid.update(remaining as int, new_agg));
            lemma_sum_len_sub(new_all, p, q);
        }
        self.update_rec(child_id, Ghost(hc), remaining, new_agg);
        let ghost t1 = *self;
        proof {
            // siblings and this node are untouched
            assert forall|j: int| 0 <= j < cs.len() && j != child_idx implies
                t1.slabs(#[trigger] cs[j].id, hc) == t0.slabs(cs[j].id, hc)
                && t1.owned(cs[j].id, hc) == t0.owned(cs[j].id, hc)
                && t1.wf_node(cs[j].id, hc) == t0.wf_node(cs[j].id, hc) by {
                t0.lemma_child(id, h, n0, j);
                assert forall|x: NodeId| t0.owned(cs[j].id, hc).contains(x) implies
                    t0.node_spec(x) == t1.node_spec(x) by {
                    assert(!t0.owned(child_id, hc).contains(x));
                }
                Self::lemma_frame(&t0, &t1, cs[j].id, hc);
            }
            assert(t1.node_spec(id) == t0.node_spec(id));
        }
        let new_child_agg = self.subtree_agg(child_id, Ghost(hc));
        let r = self.node_mut(id);
        if let Node::Internal(p) = r {
            p.children[child_idx].agg = new_child_agg;
        }
        proof {
            let t2 = *self;
            let n2 = match t2.node_spec(id) { Some(Node::Internal(n)) => n, _ => arbitrary() };
            let cs2 = n2.children@;
            assert(cs2 == cs.update(child_idx as int, ChildSlot { agg: new_child_agg, ..cs[child_idx as int] }));
            // every child subtree is untouched by rewriting this node
            assert forall|j: int| 0 <= j < cs.len() implies
                t2.slabs(#[trigger] cs[j].id, hc) == t1.slabs(cs[j].id, hc)
                && t2.owned(cs[j].id, hc) == t1.owned(cs[j].id, hc)
                && t2.wf_node(cs[j].id, hc) == t1.wf_node(cs[j].id, hc) by {
                t0.lemma_child(id, h, n0, j);
                assert forall|x: NodeId| t1.owned(cs[j].id, hc).contains(x) implies
                    t1.node_spec(x) == t2.node_spec(x) by {}
                Self::lemma_frame(&t1, &t2, cs[j].id, hc);
            }
            assert(cs2.len() == cs.len());
            assert forall|j: int| 0 <= j < cs.len() implies #[trigger] cs2[j].id == cs[j].id
                && cs2[j].slab_count == cs[j].slab_count
                && (j != child_idx ==> cs2[j].agg == cs[j].agg) by {}
            // what each child stands for now
            let new_mid = mid.update(remaining as int, new_agg);
            assert forall|j: int| 0 <= j < cs.len() implies
                t2.owned(#[trigger] cs2[j].id, hc) == t0.owned(cs[j].id, hc)
                && t2.slabs(cs2[j].id, hc) == (if j == child_idx { new_mid } else { t0.slabs(cs[j].id, hc) })
                && t2.wf_node(cs2[j].id, hc) by {
                t0.lemma_child(id, h, n0, j);
            }
            // the node's slabs: before + the updated child + after
            t2.lemma_children_slabs_split(cs2, child_idx as int, hc);
            assert forall|i: int| 0 <= i < cs.take(child_idx as int).len() implies
                t2.slabs(#[trigger] cs2.take(child_idx as int)[i].id, hc) == t0.slabs(cs.take(child_idx as int)[i].id, hc)
                && t2.owned(cs2.take(child_idx as int)[i].id, hc) == t0.owned(cs.take(child_idx as int)[i].id, hc) by {
                assert(cs2.take(child_idx as int)[i] == cs2[i]);
                assert(cs.take(child_idx as int)[i] == cs[i]);
            }
            Self::lemma_children_congr(&t2, cs2.take(child_idx as int), &t0, cs.take(child_idx as int), hc);
            assert forall|i: int| 0 <= i < cs.skip(child_idx + 1).len() implies
                t2.slabs(#[trigger] cs2.skip(child_idx + 1)[i].id, hc) == t0.slabs(cs.skip(child_idx + 1)[i].id, hc)
                && t2.owned(cs2.skip(child_idx + 1)[i].id, hc) == t0.owned(cs.skip(child_idx + 1)[i].id, hc) by {
                assert(cs2.skip(child_idx + 1)[i] == cs2[child_idx + 1 + i]);
                assert(cs.skip(child_idx + 1)[i] == cs[child_idx + 1 + i]);
            }
            Self::lemma_children_congr(&t2, cs2.skip(child_idx + 1), &t0, cs.skip(child_idx + 1), hc);
            assert(t2.slabs(id, h) == t2.children_slabs(cs2, hc));
            assert(t2.slabs(id, h) =~= new_all);
            // its nodes
            Self::lemma_children_congr_owned(&t2, cs2, &t0, cs, hc);
            assert(t2.owned(id, h) =~= t0.owned(id, h));
            // nothing outside the subtree changed
            t0.lemma_children_owned_contains(cs, child_idx as int, hc);
            // and it is well formed
            assert forall|j: int| 0 <= j < cs2.len() implies {
                let c = #[trigger] cs2[j];
                &&& t2.wf_node(c.id, hc)
                &&& !t2.owned(c.id, hc).contains(id)
                &&& c.slab_count == t2.slabs(c.id, hc).len()
                &&& c.slab_count > 0
                &&& valid(t2.slabs(c.id, hc)) ==> c.agg == fold(t2.slabs(c.id, hc))
                &&& c.agg.spec_len() == sum_len(t2.slabs(c.id, hc))
            } by {
                t0.lemma_child(id, h, n0, j);
            }
            assert forall|i: int, j: int| 0 <= i < j < cs2.len() implies
                #[trigger] t2.owned(cs2[i].id, hc).disjoint(#[trigger] t2.owned(cs2[j].id, hc)) by {
                t0.lemma_child(id, h, n0, i);
                t0.lemma_child(id, h, n0, j);
            }
            assert(t2.wf_node(id, h));
            assert forall|x: NodeId| !t0.owned(id, h).contains(x)
                implies #[trigger] t2.node_spec(x) == t0.node_spec(x) by {
                assert(t0.owned(id, h).contains(id));
                assert(!t0.owned(child_id, hc).contains(x));
                assert(t1.node_spec(x) == t0.node_spec(x));
            }
        }
    }

    // ── Splice ──────────────────────────────────────────────────────────

    /// Replace slabs `start..end` with `new`, recursively from the root:
    /// descend while the range lies within one child; at a leaf, splice
    /// its slab list in place (overwriting when the lengths match,
    /// splitting it into leaves of at most B if it overflows, merging it
    /// into a sibling if it drops below B/4); at a node whose range spans
    /// several children, give each its overlap and the matching piece of
    /// `new`.  On the way back up each level refreshes its cached slot,
    /// splits past B children and drops out when empty.  O(log n + k).
    ///
    /// The caller guarantees the result is `valid` (for delta columns,
    /// the domain contract); every node goes straight to its final
    /// contents, so nothing else need be.
    pub(crate) fn splice_vec(&mut self, start: usize, end: usize, new: Vec<A>)
        requires
            old(self).wf(),
            start <= end <= old(self)@.len(),
            sum_len(spliced(old(self)@, start as int, end as int, new@)) <= usize::MAX,
            spliced(old(self)@, start as int, end as int, new@).len() <= usize::MAX,
        ensures
            final(self).wf(),
            final(self)@ == spliced(old(self)@, start as int, end as int, new@),
    {
        assert!(end <= self.total_slabs, "splice out of bounds");
        assert!(start <= end, "invalid range");
        if start == end && new.len() == 0 {
            proof { assert(spliced(old(self)@, start as int, end as int, new@) =~= old(self)@); }
            return;
        }
        let ghost h0 = self.height();
        let ghost f = spliced(old(self)@, start as int, end as int, new@);
        proof { assert(self.wf_node(self.root, h0)); }
        let added = new.len();
        let h = self.subtree_height(self.root, Ghost(h0));
        let slots = self.splice_rec(self.root, h, start, end, new);
        self.set_root(slots, Ghost(h0));
        let ghost t2 = *self;
        let ghost hh = choose|hh: nat| t2.wf_node(t2.root, hh) && t2.slabs(t2.root, hh) == f;
        self.total_slabs = self.total_slabs - (end - start) + added;
        proof {
            assert(f.len() == old(self)@.len() - (end - start) + added);
            assert forall|x: NodeId| t2.owned(t2.root, hh).contains(x) implies t2.node_spec(x) == self.node_spec(x) by {}
            Self::lemma_frame(&t2, self, self.root, hh);
            self.lemma_height_unique(self.root, hh, self.height());
        }
        // Large deletions can leave a single-child chain at the top;
        // promote so query depth tracks the live tree.
        self.collapse_root();
    }

    /// Height of the subtree at `id` (leaves are 0).
    fn subtree_height(&self, id: NodeId, Ghost(h): Ghost<nat>) -> (r: usize)
        requires self.wf_node(id, h), self.arena_ok(),
        ensures r == h,
        decreases h,
    {
        proof { self.lemma_height_bound(id, h); }
        match self.node(id) {
            Node::Leaf(_) => 0,
            Node::Internal(n) => {
                proof { self.lemma_child(id, h, *n, 0); }
                1 + self.subtree_height(n.children[0].id, Ghost((h - 1) as nat))
            },
        }
    }

    /// Splice `start..end` ↦ `new` into the subtree at `id` (height `h`).
    /// Returns the child slots that replace the subtree's slot in its
    /// parent: none if it emptied (the caller frees `id`), one, or several
    /// if it split (the first keeps `id`).
    fn splice_rec(&mut self, id: NodeId, h: usize, start: usize, end: usize, new: Vec<A>) -> (r: Vec<ChildSlot<A>>)
        requires
            old(self).arena_ok(),
            old(self).wf_node(id, h as nat),
            start <= end <= old(self).slabs(id, h as nat).len(),
            sum_len(spliced(old(self).slabs(id, h as nat), start as int, end as int, new@)) <= usize::MAX,
            spliced(old(self).slabs(id, h as nat), start as int, end as int, new@).len() <= usize::MAX,
        ensures
            final(self).arena_ok(),
            final(self).slots_ok(r@, h as nat),
            final(self).children_slabs(r@, h as nat)
                == spliced(old(self).slabs(id, h as nat), start as int, end as int, new@),
            forall|x: NodeId| #[trigger] final(self).children_owned(r@, h as nat).contains(x)
                ==> old(self).owned(id, h as nat).contains(x) || old(self).node_spec(x) is None,
            forall|x: NodeId| !old(self).owned(id, h as nat).contains(x) && old(self).node_spec(x) is Some
                ==> #[trigger] final(self).node_spec(x) == old(self).node_spec(x),
            r@.len() == 0 ==> final(self).node_spec(id) is Some,
            final(self).root == old(self).root,
            final(self).total_slabs == old(self).total_slabs,
        decreases h, 2nat,
    {
        if h == 0 {
            proof {
                if let Some(Node::Internal(n)) = old(self).node_spec(id) {
                    old(self).lemma_wf_internal(id, 0, n);
                }
            }
            let r = self.splice_leaf(id, start, end, new);
            proof {
                assert(old(self).owned(id, 0).contains(id));
                assert(old(self).owned(id, 0) == set![id]);
            }
            return r;
        }
        let ghost hn = h as nat;
        let ghost hc = (h - 1) as nat;
        let ghost n0 = match old(self).node_spec(id) { Some(Node::Internal(n)) => n, _ => arbitrary() };
        let ghost cs = n0.children@;
        let ghost sl = old(self).slabs(id, hn);
        let ghost new0 = new@;
        let ghost m = new0.len() as int;
        let ghost f = spliced(sl, start as int, end as int, new0);
        proof {
            if let Some(Node::Leaf(l)) = old(self).node_spec(id) {
                old(self).lemma_wf_leaf(id, hn, l);
            }
            old(self).lemma_wf_internal(id, hn, n0);
            old(self).lemma_not_in_children(cs, hc, id);
            old(self).lemma_counts(cs, hc, cs.len() as int);
            assert(cs.take(cs.len() as int) =~= cs);
            Self::lemma_counts_mono(cs);
        }
        let (ks, ke, off_s, off_e) = self.locate_range(id, start, end, Ghost(cs));
        if ks == ke {
            proof {
                old(self).lemma_slabs_at(cs, hc, ks as int);
                let (pre, post) = (cs.subrange(0, ks as int), cs.subrange(ks + 1, cs.len() as int));
                let c = old(self).slabs(cs[ks as int].id, hc);
                old(self).lemma_children_slabs_split(cs, ks as int, hc);
                assert(cs.take(ks as int) == pre && cs.skip(ks + 1) == post);
                lemma_splice_in_part(old(self).children_slabs(pre, hc), c, old(self).children_slabs(post, hc),
                    start as int, end as int, new0);
                let piece = spliced(c, start - off_s, end - off_s, new0);
                let p = old(self).children_slabs(pre, hc).len() as int;
                assert(f.subrange(p, p + piece.len()) =~= piece);
                lemma_sum_len_sub(f, p, p + piece.len());
            }
            self.splice_child(id, h, ks, start - off_s, end - off_s, new, true);
            proof {
                old(self).lemma_slabs_at(cs, hc, ks as int);
                old(self).lemma_children_owned_contains(cs, ks as int, hc);
            }
        } else {
            self.splice_span(id, h, ks, ke, off_e, start, end, new, Ghost(cs));
        }
        let ghost t9 = *self;
        proof {
            // what the branches left: id's children stand for the result,
            // using its old subtree and empty slots, with nothing else
            // touched
            assert forall|x: NodeId| x != id && old(self).node_spec(x) is Some
                && !old(self).children_owned(cs, hc).contains(x)
                implies #[trigger] t9.node_spec(x) == old(self).node_spec(x) by {
                if ks == ke {
                    old(self).lemma_children_owned_contains(cs, ks as int, hc);
                }
            }
        }
        let r = self.finish_internal(id, Ghost(h as nat));
        proof {
            old(self).lemma_wf_internal(id, hn, n0);
            assert forall|x: NodeId| #[trigger] self.children_owned(r@, hn).contains(x)
                implies old(self).owned(id, hn).contains(x) || old(self).node_spec(x) is None by {
                if x != id && !old(self).children_owned(cs, hc).contains(x) && old(self).node_spec(x) is Some {
                    assert(t9.node_spec(x) == old(self).node_spec(x));
                }
            }
            assert forall|x: NodeId| !old(self).owned(id, hn).contains(x) && old(self).node_spec(x) is Some
                implies #[trigger] self.node_spec(x) == old(self).node_spec(x) by {
                assert(t9.node_spec(x) == old(self).node_spec(x));
            }
        }
        r
    }

    /// Splice `start..end` ↦ `new` across children `ks..=ke` of internal
    /// node `id` (`ks < ke`; child `ke` starts at `off_e`).  Each takes its
    /// overlap and the piece of `new` that would cover it at the same
    /// length (so most are overwritten in place), the last one whatever is
    /// left.  They are spliced last to first, so each one's piece splits
    /// off the end of `new` and earlier indices stay put.
    #[verifier::rlimit(30)]
    fn splice_span(&mut self, id: NodeId, h: usize, ks: usize, ke: usize, off_e: usize,
        start: usize, end: usize, new: Vec<A>, Ghost(cs): Ghost<Seq<ChildSlot<A>>>)
        requires
            h >= 1,
            old(self).arena_ok(),
            old(self).node_spec(id) matches Some(Node::Internal(n)) && n.children@ == cs,
            old(self).slots_ok(cs, (h - 1) as nat),
            !old(self).children_owned(cs, (h - 1) as nat).contains(id),
            ks < ke < cs.len(), off_e == counts(cs, ke as int),
            counts(cs, ks as int) <= start < counts(cs, ks + 1), off_e < end <= counts(cs, ke + 1),
            counts(cs, cs.len() as int) == old(self).children_slabs(cs, (h - 1) as nat).len(),
            sum_len(spliced(old(self).children_slabs(cs, (h - 1) as nat), start as int, end as int, new@)) <= usize::MAX,
            spliced(old(self).children_slabs(cs, (h - 1) as nat), start as int, end as int, new@).len() <= usize::MAX,
            old(self).children_slabs(cs, (h - 1) as nat).len() <= usize::MAX,
        ensures
            final(self).arena_ok(),
            final(self).node_spec(id) matches Some(Node::Internal(n2)) && {
                let l = n2.children@;
                let hc = (h - 1) as nat;
                &&& final(self).slots_ok(l, hc)
                &&& !final(self).children_owned(l, hc).contains(id)
                &&& final(self).children_slabs(l, hc)
                    == spliced(old(self).children_slabs(cs, hc), start as int, end as int, new@)
                &&& forall|x: NodeId| #[trigger] final(self).children_owned(l, hc).contains(x)
                    ==> old(self).children_owned(cs, hc).contains(x) || old(self).node_spec(x) is None
            },
            forall|x: NodeId| x != id && old(self).node_spec(x) is Some
                && !old(self).children_owned(cs, (h - 1) as nat).contains(x)
                ==> #[trigger] final(self).node_spec(x) == old(self).node_spec(x),
            final(self).root == old(self).root,
            final(self).total_slabs == old(self).total_slabs,
        decreases h, 1nat,
    {
        let ghost hn = h as nat;
        let ghost hc = (h - 1) as nat;
        let ghost sl = old(self).children_slabs(cs, hc);
        let ghost new0 = new@;
        let ghost m = new0.len() as int;
        let ghost f = spliced(sl, start as int, end as int, new0);
        let ghost off_s = counts(cs, ks as int);
        proof { Self::lemma_counts_mono(cs); }
        // The range spans children ks..=ke.  Each takes its overlap and
        // the piece of `new` that would cover it at the same length (so
        // most are overwritten in place), the last one whatever is
        // left.  Splice them last to first, so each one's piece splits
        // off the end of `new` and earlier indices stay put.
        let m_ = new.len();
        let mut new = new;
        let mut k = ke;
        let mut off = off_e;
        proof {
            assert(cs.subrange(0, ke + 1) + cs.subrange(ke + 1, cs.len() as int) =~= cs);
            old(self).lemma_slabs_at(cs, hc, ke as int);
            old(self).lemma_slabs_tail(cs, hc, ke + 1);
            lemma_spliced_index(sl, start as int, end as int, new0);
            let q = share_at(ke + 1, ks as int, ke as int, m, start as int, end as int, counts(cs, ke + 1) as int);
            assert(f.subrange(q, f.len() as int) =~= sl.subrange(counts(cs, ke + 1) as int, sl.len() as int));
            assert(new@ =~= new0.subrange(0, cut_at(ke + 1, ks as int, ke as int, m, start as int, counts(cs, ke + 1) as int)));
            old(self).lemma_slots_nil(hc);
        }
        loop
            invariant_except_break
                h >= 1, hn == h, hc == h - 1, m == m_, ks < ke < cs.len(),
                ks <= k <= ke, off == counts(cs, k as int),
                off_s == counts(cs, ks as int), off_e == counts(cs, ke as int),
                off_s <= start < counts(cs, ks + 1), off_e < end <= counts(cs, ke + 1),
                counts(cs, cs.len() as int) == sl.len(), sl.len() <= usize::MAX,
                forall|i: int, j: int| 0 <= i <= j <= cs.len() ==> #[trigger] counts(cs, i) <= #[trigger] counts(cs, j),
                old(self).slots_ok(cs, hc), sl == old(self).children_slabs(cs, hc),
                f == spliced(sl, start as int, end as int, new0), sum_len(f) <= usize::MAX, f.len() <= usize::MAX,
                new0.len() == m,
                new@ == new0.subrange(0, cut_at(k + 1, ks as int, ke as int, m, start as int, counts(cs, k + 1) as int)),
                self.arena_ok(),
                self.root == old(self).root, self.total_slabs == old(self).total_slabs,
                self.node_spec(id) matches Some(Node::Internal(n)) && {
                    let l = n.children@;
                    &&& self.slots_ok(l, hc)
                    &&& !self.children_owned(l, hc).contains(id)
                    &&& k < l.len()
                    &&& l.subrange(0, k + 1) == cs.subrange(0, k + 1)
                    &&& self.children_slabs(l, hc) == old(self).children_slabs(cs.subrange(0, k + 1), hc)
                        + f.subrange(share_at(k + 1, ks as int, ke as int, m, start as int, end as int, counts(cs, k + 1) as int), f.len() as int)
                    &&& forall|x: NodeId| #[trigger] self.children_owned(l, hc).contains(x)
                        ==> old(self).children_owned(cs, hc).contains(x) || old(self).node_spec(x) is None
                },
                forall|x: NodeId| x != id && old(self).node_spec(x) is Some
                    && !old(self).children_owned(cs.subrange(k + 1, cs.len() as int), hc).contains(x)
                    ==> #[trigger] self.node_spec(x) == old(self).node_spec(x),
                !old(self).children_owned(cs, hc).contains(id),
                old(self).node_spec(id) is Some,
            ensures
                self.arena_ok(),
                self.root == old(self).root, self.total_slabs == old(self).total_slabs,
                k == ks,
                self.node_spec(id) matches Some(Node::Internal(n)) && {
                    let l = n.children@;
                    &&& self.slots_ok(l, hc)
                    &&& !self.children_owned(l, hc).contains(id)
                    &&& self.children_slabs(l, hc) == old(self).children_slabs(cs.subrange(0, ks as int), hc)
                        + f.subrange(share_at(ks as int, ks as int, ke as int, m, start as int, end as int, counts(cs, ks as int) as int), f.len() as int)
                    &&& forall|x: NodeId| #[trigger] self.children_owned(l, hc).contains(x)
                        ==> old(self).children_owned(cs, hc).contains(x) || old(self).node_spec(x) is None
                },
                forall|x: NodeId| x != id && old(self).node_spec(x) is Some
                    && !old(self).children_owned(cs.subrange(ks as int, cs.len() as int), hc).contains(x)
                    ==> #[trigger] self.node_spec(x) == old(self).node_spec(x),
            decreases k,
        {
            let ghost t0 = *self;
            let ghost (pk, pk1) = (counts(cs, k as int) as int, counts(cs, k + 1) as int);
            proof {
                old(self).lemma_slabs_at(cs, hc, k as int);
                old(self).lemma_slot(cs, hc, k as int);
                Self::lemma_untouched_child(old(self), &t0, cs, hc, k as int, id);
            }
            let count = match self.node(id) {
                Node::Internal(n) => n.children[k].slab_count,
                Node::Leaf(_) => unreachable!(),
            };
            let lo = start.saturating_sub(off);
            let hi = if end - off < count { end - off } else { count };
            let cut = if k == ks { 0 } else if off - start < m_ { off - start } else { m_ };
            let part = new.split_off(cut);
            proof {
                lemma_splice_piece(sl, start as int, end as int, new0, pk, pk1, k == ks, k == ke);
                assert(sl.subrange(pk, pk1) == old(self).slabs(cs[k as int].id, hc));
                assert(part@ =~= new0.subrange(cut as int, cut_at(k + 1, ks as int, ke as int, m, start as int, pk1)));
                let qk = share_at(k as int, ks as int, ke as int, m, start as int, end as int, pk);
                let qk1 = share_at(k + 1, ks as int, ke as int, m, start as int, end as int, pk1);
                lemma_sum_len_sub(f, qk, qk1);
            }
            let ghost l0 = match t0.node_spec(id) { Some(Node::Internal(n)) => n.children@, _ => arbitrary() };
            let ghost piece = spliced(t0.slabs(cs[k as int].id, hc), lo as int, hi as int, part@);
            self.splice_child(id, h, k, lo, hi, part, false);
            proof {
                let l2 = match self.node_spec(id) { Some(Node::Internal(n)) => n.children@, _ => arbitrary() };
                assert(l0[k as int] == cs[k as int]) by {
                    assert(l0.subrange(0, k + 1)[k as int] == cs.subrange(0, k + 1)[k as int]);
                };
                Self::lemma_multi_step(old(self), &t0, self, cs, l0, l2, hc, k as int, id, f,
                    share_at(k as int, ks as int, ke as int, m, start as int, end as int, pk),
                    share_at(k + 1, ks as int, ke as int, m, start as int, end as int, pk1), piece);
            }
            if k == ks {
                break;
            }
            k -= 1;
            off -= match self.node(id) {
                Node::Internal(n) => n.children[k].slab_count,
                Node::Leaf(_) => unreachable!(),
            };
            proof {
                old(self).lemma_slabs_at(cs, hc, k as int);
                assert(new@ =~= new0.subrange(0, cut_at(k + 1, ks as int, ke as int, m, start as int, counts(cs, k + 1) as int)));
            }
        }
        proof {
            old(self).lemma_slabs_at(cs, hc, ks as int);
            lemma_spliced_index(sl, start as int, end as int, new0);
            assert(f =~= sl.subrange(0, off_s as int) + f.subrange(off_s as int, f.len() as int));
            old(self).lemma_slots_sub(cs, ks as int, cs.len() as int, hc);
        }
    }

    /// Splice a leaf's slab list.
    #[allow(clippy::vec_init_then_push)] // Verus has no `vec!`
    fn splice_leaf(&mut self, id: NodeId, start: usize, end: usize, new: Vec<A>) -> (r: Vec<ChildSlot<A>>)
        requires
            old(self).arena_ok(),
            old(self).wf_node(id, 0),
            start <= end <= old(self).slabs(id, 0).len(),
            sum_len(spliced(old(self).slabs(id, 0), start as int, end as int, new@)) <= usize::MAX,
            spliced(old(self).slabs(id, 0), start as int, end as int, new@).len() <= usize::MAX,
        ensures
            final(self).arena_ok(),
            final(self).slots_ok(r@, 0),
            final(self).children_slabs(r@, 0) == spliced(old(self).slabs(id, 0), start as int, end as int, new@),
            forall|x: NodeId| #[trigger] final(self).children_owned(r@, 0).contains(x)
                ==> x == id || old(self).node_spec(x) is None,
            forall|x: NodeId| x != id && old(self).node_spec(x) is Some
                ==> #[trigger] final(self).node_spec(x) == old(self).node_spec(x),
            r@.len() == 0 ==> final(self).node_spec(id) is Some,
            final(self).root == old(self).root,
            final(self).total_slabs == old(self).total_slabs,
    {
        let ghost old_aggs = old(self).slabs(id, 0);
        let ghost new0 = new@;
        let ghost f = spliced(old_aggs, start as int, end as int, new0);
        proof {
            let l0 = match old(self).node_spec(id) { Some(Node::Leaf(l)) => l, _ => arbitrary() };
            if let Some(Node::Internal(n)) = old(self).node_spec(id) {
                old(self).lemma_wf_internal(id, 0, n);
            } else {
                old(self).lemma_wf_leaf(id, 0, l0);
            }
        }
        let mut new = new;
        let ln = self.node_mut(id);
        let len = if let Node::Leaf(l) = ln {
            if end - start == new.len() {
                let mut i = new.len();
                while i > 0
                    invariant
                        i <= new0.len(), end - start == new0.len(), end <= old_aggs.len(),
                        new@ == new0.subrange(0, i as int),
                        l.aggs@ == old_aggs.subrange(0, start + i) + new0.subrange(i as int, new0.len() as int)
                            + old_aggs.subrange(end as int, old_aggs.len() as int),
                    decreases i,
                {
                    i -= 1;
                    let ghost before = l.aggs@;
                    let v = new.pop().unwrap();
                    proof { assert(v == new0[i as int]); }
                    l.aggs[start + i] = v;
                    proof {
                        assert(l.aggs@ =~= old_aggs.subrange(0, start + i) + new0.subrange(i as int, new0.len() as int)
                            + old_aggs.subrange(end as int, old_aggs.len() as int));
                        assert(new@ =~= new0.subrange(0, i as int));
                    }
                }
                proof { assert(l.aggs@ =~= f); }
            } else {
                let mut tail = l.aggs.split_off(end);
                l.aggs.truncate(start);
                l.aggs.append(&mut new);
                l.aggs.append(&mut tail);
                proof { assert(l.aggs@ =~= f); }
            }
            l.aggs.len()
        } else {
            unreachable!()
        };
        let ghost t1 = *self;
        let ghost leaf = Leaf { aggs: match t1.node_spec(id) { Some(Node::Leaf(l)) => l.aggs, _ => arbitrary() } };
        proof {
            assert(t1.node_spec(id) == Some(Node::Leaf(leaf)));
            assert(leaf.aggs@ == f);
            t1.lemma_wf_leaf(id, 0, leaf);
            assert forall|x: NodeId| x != id && old(self).node_spec(x) is Some
                implies #[trigger] t1.node_spec(x) == old(self).node_spec(x) by {}
            assert forall|i: int| 0 <= i < t1.free@.len() implies
                (#[trigger] t1.free@[i] as int) < t1.nodes@.len() && t1.nodes@[t1.free@[i] as int] is None by {
                assert(old(self).free@[i] != id);
            }
        }
        if len == 0 {
            let out = Vec::new();
            proof {
                self.lemma_slots_nil(0);
                assert(f =~= Seq::<A>::empty());
            }
            return out;
        }
        if len <= B {
            let agg = self.leaf_agg(id, Ghost(0));
            let mut out = Vec::new();
            let slot = ChildSlot { id, agg, slab_count: len };
            proof {
                self.lemma_slot_one(slot, 0);
                assert(seq![slot] =~= Seq::<ChildSlot<A>>::empty().push(slot));
            }
            out.push(slot);
            proof { assert(out@ =~= seq![slot]); }
            return out;
        }
        // Overflow: split into ⌈len/B⌉ leaves, the first keeping `id`.
        let mut all = Vec::new();
        let ln = self.node_mut(id);
        if let Node::Leaf(l) = ln {
            std::mem::swap(&mut all, &mut l.aggs);
        }
        let ghost t2 = *self;
        proof {
            assert(all@ == f);
            assert forall|x: NodeId| x != id && old(self).node_spec(x) is Some
                implies #[trigger] t2.node_spec(x) == old(self).node_spec(x) by {
                assert(t1.node_spec(x) == old(self).node_spec(x));
            }
            assert forall|i: int| 0 <= i < t2.free@.len() implies
                (#[trigger] t2.free@[i] as int) < t2.nodes@.len() && t2.nodes@[t2.free@[i] as int] is None by {
                assert(old(self).free@[i] != id);
            }
        }
        let r = self.split_leaf(id, all);
        proof {
            assert forall|x: NodeId| x != id && old(self).node_spec(x) is Some
                implies #[trigger] self.node_spec(x) == old(self).node_spec(x) by {
                assert(t2.node_spec(x) == old(self).node_spec(x));
            }
            assert forall|x: NodeId| #[trigger] self.children_owned(r@, 0).contains(x)
                implies x == id || old(self).node_spec(x) is None by {
                if x != id && t2.node_spec(x) is None {
                    if old(self).node_spec(x) is Some { assert(t2.node_spec(x) == old(self).node_spec(x)); }
                }
            }
        }
        r
    }

    /// Which children hold `start` (or, appending, the last child) and
    /// the last slab of `start..end`, and where each starts.
    fn locate_range(&self, id: NodeId, start: usize, end: usize, Ghost(cs): Ghost<Seq<ChildSlot<A>>>) -> (r: (usize, usize, usize, usize))
        requires
            self.node_spec(id) matches Some(Node::Internal(n)) && n.children@ == cs,
            cs.len() > 0,
            start <= end <= counts(cs, cs.len() as int),
            counts(cs, cs.len() as int) <= usize::MAX,
        ensures ({
            let (ks, ke, off_s, off_e) = r;
            &&& ks <= ke < cs.len()
            &&& off_s == counts(cs, ks as int) && off_e == counts(cs, ke as int)
            &&& off_s <= start
            &&& (start < counts(cs, ks + 1) || ks == cs.len() - 1)
            &&& end > start ==> off_e < end <= counts(cs, ke + 1)
            &&& end == start ==> ke == ks
        }),
    {
        let n = match self.node(id) {
            Node::Internal(n) => n,
            Node::Leaf(_) => unreachable!(),
        };
        proof { Self::lemma_counts_mono(cs); }
        let last = n.children.len() - 1;
        let mut off: usize = 0;
        let mut k: usize = 0;
        while !(start < off + n.children[k].slab_count || k == last)
            invariant
                cs == n.children@, last == cs.len() - 1, k <= last,
                off == counts(cs, k as int), off <= start,
                counts(cs, k + 1) == off + cs[k as int].slab_count,
                forall|i: int, j: int| 0 <= i <= j <= cs.len() ==> #[trigger] counts(cs, i) <= #[trigger] counts(cs, j),
                counts(cs, cs.len() as int) <= usize::MAX,
                start <= end <= counts(cs, cs.len() as int),
            decreases last - k,
        {
            off += n.children[k].slab_count;
            k += 1;
        }
        let (ks, off_s) = (k, off);
        if end > start {
            while !(end - 1 < off + n.children[k].slab_count || k == last)
                invariant
                    cs == n.children@, last == cs.len() - 1, ks <= k <= last,
                    off == counts(cs, k as int), off < end,
                    counts(cs, k + 1) == off + cs[k as int].slab_count,
                    forall|i: int, j: int| 0 <= i <= j <= cs.len() ==> #[trigger] counts(cs, i) <= #[trigger] counts(cs, j),
                    counts(cs, cs.len() as int) <= usize::MAX,
                    start < end <= counts(cs, cs.len() as int),
                decreases last - k,
            {
                off += n.children[k].slab_count;
                k += 1;
            }
        }
        (ks, k, off_s, off)
    }

    /// Splice `lo..hi` ↦ `part` into child `k` of internal node `id`,
    /// replacing its slot with whatever slots come back (freeing it if
    /// none); with `merge`, then merge it into a sibling if it is a leaf
    /// that shrank below B/4.
    #[verifier::rlimit(30)]
    fn splice_child(&mut self, id: NodeId, h: usize, k: usize, lo: usize, hi: usize, part: Vec<A>, merge: bool)
        requires
            h >= 1,
            old(self).arena_ok(),
            old(self).node_spec(id) matches Some(Node::Internal(n)) && {
                let cs = n.children@;
                let hc = (h - 1) as nat;
                let c = old(self).slabs(cs[k as int].id, hc);
                let piece = spliced(c, lo as int, hi as int, part@);
                &&& k < cs.len()
                &&& old(self).slots_ok(cs, hc)
                &&& !old(self).children_owned(cs, hc).contains(id)
                &&& lo <= hi <= c.len()
                &&& sum_len(piece) <= usize::MAX && piece.len() <= usize::MAX
                &&& merge ==> {
                    let all = old(self).children_slabs(cs.subrange(0, k as int), hc) + piece
                        + old(self).children_slabs(cs.subrange(k + 1, cs.len() as int), hc);
                    sum_len(all) <= usize::MAX && all.len() <= usize::MAX
                }
            },
        ensures
            final(self).arena_ok(),
            old(self).node_spec(id) matches Some(Node::Internal(n))
                && final(self).node_spec(id) matches Some(Node::Internal(n2)) && {
                let (cs, cs2, hc) = (n.children@, n2.children@, (h - 1) as nat);
                let piece = spliced(old(self).slabs(cs[k as int].id, hc), lo as int, hi as int, part@);
                &&& final(self).slots_ok(cs2, hc)
                &&& !final(self).children_owned(cs2, hc).contains(id)
                &&& final(self).children_slabs(cs2, hc) == old(self).children_slabs(cs.subrange(0, k as int), hc)
                    + piece + old(self).children_slabs(cs.subrange(k + 1, cs.len() as int), hc)
                &&& !merge ==> cs2.subrange(0, k as int) == cs.subrange(0, k as int) && cs2.len() >= k
                &&& forall|x: NodeId| #[trigger] final(self).children_owned(cs2, hc).contains(x)
                    ==> old(self).children_owned(cs, hc).contains(x) || old(self).node_spec(x) is None
                &&& forall|x: NodeId| x != id && old(self).node_spec(x) is Some
                    && !old(self).owned(cs[k as int].id, hc).contains(x)
                    && (merge ==> !old(self).children_owned(cs, hc).contains(x))
                    ==> #[trigger] final(self).node_spec(x) == old(self).node_spec(x)
            },
            final(self).root == old(self).root,
            final(self).total_slabs == old(self).total_slabs,
        decreases h, 0nat,
    {
        let ghost n0 = match old(self).node_spec(id) { Some(Node::Internal(n)) => n, _ => arbitrary() };
        let ghost cs = n0.children@;
        let ghost hc = (h - 1) as nat;
        let ghost (pre, post) = (cs.subrange(0, k as int), cs.subrange(k + 1, cs.len() as int));
        let ghost t0 = *self;
        let child_id = match self.node(id) {
            Node::Internal(n) => n.children[k].id,
            Node::Leaf(_) => unreachable!(),
        };
        let ghost piece = spliced(t0.slabs(child_id, hc), lo as int, hi as int, part@);
        proof {
            t0.lemma_slot(cs, hc, k as int);
            t0.lemma_children_owned_contains(cs, k as int, hc);
            t0.lemma_slots_sub(cs, 0, k as int, hc);
            t0.lemma_slots_sub(cs, k + 1, cs.len() as int, hc);
            Self::lemma_child_apart(&t0, cs, k as int, hc);
        }
        let shrink = part.len() < hi - lo;
        let mut slots = self.splice_rec(child_id, h - 1, lo, hi, part);
        let ghost t1 = *self;
        let merge_leaf = merge && h == 1 && shrink && slots.len() == 1;
        if slots.len() == 0 {
            self.free(child_id);
        }
        let ghost t2 = *self;
        let ghost rs = slots@;
        let nn = self.node_mut(id);
        if let Node::Internal(n) = nn {
            let ghost len = n.children.len();
            assert(k < len);
            let mut tail = n.children.split_off(k + 1);
            n.children.truncate(k);
            n.children.append(&mut slots);
            n.children.append(&mut tail);
        }
        let ghost t3 = *self;
        proof {
            let r = t1.children_owned(rs, hc);
            let cs3 = match t3.node_spec(id) { Some(Node::Internal(n)) => n.children@, _ => arbitrary() };
            assert(cs3 =~= pre + rs + post);
            // nothing outside the child's subtree changed but `id`
            assert forall|x: NodeId| x != id && t0.node_spec(x) is Some && !t0.owned(child_id, hc).contains(x)
                implies #[trigger] t3.node_spec(x) == t0.node_spec(x) by {
                assert(t1.node_spec(x) == t0.node_spec(x));
                assert(t2.node_spec(x) == t1.node_spec(x));
            }
            // the returned slots are still good
            assert forall|x: NodeId| x != id && x != child_id implies #[trigger] t3.node_spec(x) == t1.node_spec(x) by {
                assert(t2.node_spec(x) == t1.node_spec(x));
            }
            if rs.len() > 0 {
                assert(t2 == t1);
                assert forall|x: NodeId| x != id implies #[trigger] t3.node_spec(x) == t1.node_spec(x) by {}
            }
            Self::lemma_child_result_keep(&t1, &t3, rs, hc, id, child_id, &t0);
            Self::lemma_slots_keep(&t0, &t3, pre, hc, t0.owned(child_id, hc).insert(id));
            Self::lemma_slots_keep(&t0, &t3, post, hc, t0.owned(child_id, hc).insert(id));
            assert forall|x: NodeId| #[trigger] t0.children_owned(pre, hc).contains(x) implies t0.node_spec(x) is Some by {
                t0.lemma_slots_live(pre, hc, x);
            }
            assert forall|x: NodeId| #[trigger] t0.children_owned(post, hc).contains(x) implies t0.node_spec(x) is Some by {
                t0.lemma_slots_live(post, hc, x);
            }
            Self::lemma_child_join(&t3, pre, rs, post, hc, id, &t0, child_id);
            assert(cs3.subrange(0, k as int) =~= pre);
            assert forall|i: int| 0 <= i < t3.free@.len() implies
                (#[trigger] t3.free@[i] as int) < t3.nodes@.len() && t3.nodes@[t3.free@[i] as int] is None by {
                assert(t2.free@[i] != id);
            }
        }
        if merge_leaf {
            self.maybe_merge_leaf(id, k);
            proof {
                assert forall|x: NodeId| x != id && t0.node_spec(x) is Some
                    && !t0.children_owned(cs, hc).contains(x)
                    implies #[trigger] self.node_spec(x) == t0.node_spec(x) by {
                    t0.lemma_children_owned_contains(cs, k as int, hc);
                    assert(t3.node_spec(x) == t0.node_spec(x));
                }
            }
        }
    }

    /// The slots for internal node `id` (height `h`) after its children
    /// changed: none if it has none left, its own, or several if it has
    /// more than B.
    #[allow(clippy::vec_init_then_push)] // Verus has no `vec!`
    fn finish_internal(&mut self, id: NodeId, Ghost(h): Ghost<nat>) -> (r: Vec<ChildSlot<A>>)
        requires
            h > 0,
            old(self).arena_ok(),
            old(self).node_spec(id) matches Some(Node::Internal(n)) && {
                &&& old(self).slots_ok(n.children@, (h - 1) as nat)
                &&& !old(self).children_owned(n.children@, (h - 1) as nat).contains(id)
                &&& sum_len(old(self).children_slabs(n.children@, (h - 1) as nat)) <= usize::MAX
                &&& old(self).children_slabs(n.children@, (h - 1) as nat).len() <= usize::MAX
            },
        ensures
            final(self).arena_ok(),
            final(self).slots_ok(r@, h),
            old(self).node_spec(id) matches Some(Node::Internal(n)) && {
                &&& final(self).children_slabs(r@, h) == old(self).children_slabs(n.children@, (h - 1) as nat)
                &&& forall|x: NodeId| #[trigger] final(self).children_owned(r@, h).contains(x)
                    ==> x == id || old(self).node_spec(x) is None
                        || old(self).children_owned(n.children@, (h - 1) as nat).contains(x)
            },
            forall|x: NodeId| x != id && old(self).node_spec(x) is Some
                ==> #[trigger] final(self).node_spec(x) == old(self).node_spec(x),
            r@.len() == 0 ==> final(self).node_spec(id) is Some,
            final(self).root == old(self).root,
            final(self).total_slabs == old(self).total_slabs,
    {
        let ghost n0 = match old(self).node_spec(id) { Some(Node::Internal(n)) => n, _ => arbitrary() };
        let ghost cs = n0.children@;
        let ghost hc = (h - 1) as nat;
        let count = match self.node(id) {
            Node::Internal(n) => n.children.len(),
            Node::Leaf(_) => unreachable!(),
        };
        if count == 0 {
            proof {
                self.lemma_slots_nil(h);
                assert(cs =~= Seq::<ChildSlot<A>>::empty());
                self.lemma_slots_nil(hc);
            }
            return Vec::new();
        }
        if count <= B {
            proof {
                self.lemma_wf_internal(id, h, n0);
                assert forall|i: int| 0 <= i < cs.len() implies !self.owned(#[trigger] cs[i].id, hc).contains(id) by {
                    self.lemma_children_owned_contains(cs, i, hc);
                }
            }
            let (agg, slab_count) = self.internal_summary(id, Ghost(h));
            let mut out = Vec::new();
            let slot = ChildSlot { id, agg, slab_count };
            proof {
                self.lemma_wf_internal(id, h, n0);
                self.lemma_slot_one(slot, h);
            }
            out.push(slot);
            proof {
                assert(out@ =~= seq![slot]);
                assert forall|x: NodeId| #[trigger] self.children_owned(out@, h).contains(x)
                    implies x == id || old(self).node_spec(x) is None || self.children_owned(cs, hc).contains(x) by {}
            }
            return out;
        }
        // Overflow: split into ⌈count/B⌉ internal nodes, the first
        // keeping `id`.
        let mut all = Vec::new();
        let nn = self.node_mut(id);
        if let Node::Internal(n) = nn {
            std::mem::swap(&mut all, &mut n.children);
        }
        let ghost t1 = *self;
        proof {
            assert(all@ == cs);
            Self::lemma_slots_keep(old(self), &t1, cs, hc, set![id]);
            assert forall|i: int| 0 <= i < t1.free@.len() implies
                (#[trigger] t1.free@[i] as int) < t1.nodes@.len() && t1.nodes@[t1.free@[i] as int] is None by {
                assert(old(self).free@[i] != id);
            }
        }
        let r = self.split_internal(id, all, Ghost(h));
        proof {
            assert forall|x: NodeId| x != id && old(self).node_spec(x) is Some
                implies #[trigger] self.node_spec(x) == old(self).node_spec(x) by {
                assert(t1.node_spec(x) == old(self).node_spec(x));
            }
            assert forall|x: NodeId| #[trigger] self.children_owned(r@, h).contains(x)
                implies x == id || old(self).node_spec(x) is None || old(self).children_owned(cs, hc).contains(x) by {
                if x != id && t1.node_spec(x) is None && old(self).node_spec(x) is Some {
                    assert(t1.node_spec(x) == old(self).node_spec(x));
                }
            }
        }
        r
    }




    /// After a leaf child shrank, merge it into an adjacent sibling when
    /// it is under a quarter full and the pair fits in one node — mirrors
    /// `Column::try_merge`'s (min = max/4, combined ≤ max) policy.
    ///
    /// Leaf-level only: internal nodes thin ~B× slower than leaves (they
    /// lose a child only when an entire leaf dies), so leaf merging is
    /// where fill degradation actually shows up.
    #[verifier::rlimit(30)]
    fn maybe_merge_leaf(&mut self, parent_id: NodeId, child_idx: usize)
        requires
            old(self).arena_ok(),
            old(self).node_spec(parent_id) matches Some(Node::Internal(n)) && {
                &&& child_idx < n.children@.len()
                &&& old(self).slots_ok(n.children@, 0)
                &&& !old(self).children_owned(n.children@, 0).contains(parent_id)
                &&& sum_len(old(self).children_slabs(n.children@, 0)) <= usize::MAX
                &&& old(self).children_slabs(n.children@, 0).len() <= usize::MAX
            },
        ensures
            final(self).arena_ok(),
            final(self).node_spec(parent_id) matches Some(Node::Internal(n2)) &&
                old(self).node_spec(parent_id) matches Some(Node::Internal(n)) && {
                &&& n2.children@.len() > 0
                &&& final(self).slots_ok(n2.children@, 0)
                &&& final(self).children_slabs(n2.children@, 0) == old(self).children_slabs(n.children@, 0)
                &&& final(self).children_owned(n2.children@, 0).subset_of(old(self).children_owned(n.children@, 0))
            },
            forall|x: NodeId| x != parent_id && old(self).node_spec(x) is Some
                && !(old(self).node_spec(parent_id) matches Some(Node::Internal(n))
                    && old(self).children_owned(n.children@, 0).contains(x))
                ==> #[trigger] final(self).node_spec(x) == old(self).node_spec(x),
            final(self).nodes@.len() == old(self).nodes@.len(),
            final(self).root == old(self).root,
            final(self).total_slabs == old(self).total_slabs,
    {
        let ghost n0 = match old(self).node_spec(parent_id) { Some(Node::Internal(n)) => n, _ => arbitrary() };
        let ghost cs = n0.children@;
        let (sibling_count, my_len) = match self.node(parent_id) {
            Node::Internal(p) => (p.children.len(), p.children[child_idx].slab_count),
            Node::Leaf(_) => unreachable!(),
        };
        if my_len >= B / 4 {
            return;
        }
        // Merge right-into-left so slab order is preserved.
        let (dst_idx, src_idx) = if child_idx > 0 {
            (child_idx - 1, child_idx)
        } else if child_idx + 1 < sibling_count {
            (child_idx, child_idx + 1)
        } else {
            return; // only child — handled by cascade/collapse instead
        };
        let ghost (d, sr) = (dst_idx as int, src_idx as int);
        let ghost (pre, post) = (cs.subrange(0, d), cs.subrange(sr + 1, cs.len() as int));
        let ghost (dc, sc) = (cs[d], cs[sr]);
        proof {
            self.lemma_slot(cs, 0, d);
            self.lemma_slot(cs, 0, sr);
            self.lemma_pair_slabs(cs, d, 0);
        }
        let (dst_id, src_id, combined) = match self.node(parent_id) {
            Node::Internal(p) => (
                p.children[dst_idx].id,
                p.children[src_idx].id,
                p.children[dst_idx].slab_count + p.children[src_idx].slab_count,
            ),
            Node::Leaf(_) => unreachable!(),
        };
        if combined > B {
            return;
        }
        let ghost t0 = *self;
        let ghost dl = match t0.node_spec(dst_id) { Some(Node::Leaf(l)) => l, _ => arbitrary() };
        let ghost sl = match t0.node_spec(src_id) { Some(Node::Leaf(l)) => l, _ => arbitrary() };
        proof {
            // both are leaves (height 0), distinct, and not the parent
            assert(t0.wf_node(dst_id, 0) && t0.wf_node(src_id, 0));
            assert(t0.owned(dst_id, 0) == set![dst_id]) by { if let Some(Node::Internal(_)) = t0.node_spec(dst_id) {} };
            assert(t0.owned(src_id, 0) == set![src_id]) by { if let Some(Node::Internal(_)) = t0.node_spec(src_id) {} };
            reveal(SlabBTree::slots_ok);
            assert(t0.owned(cs[d].id, 0).disjoint(t0.owned(cs[sr].id, 0)));
            t0.lemma_children_owned_contains(cs, d, 0);
            t0.lemma_children_owned_contains(cs, sr, 0);
            assert(dst_id == cs[d].id && src_id == cs[sr].id);
            assert(t0.owned(dst_id, 0).contains(dst_id));
            assert(t0.owned(src_id, 0).contains(src_id));
            assert(dst_id != src_id);
            assert(dst_id != parent_id && src_id != parent_id);
        }
        let mut src_aggs = Vec::new();
        let sn = self.node_mut(src_id);
        if let Node::Leaf(l) = sn {
            std::mem::swap(&mut src_aggs, &mut l.aggs);
        }
        let ghost ta = *self;
        self.free(src_id);
        let ghost tb = *self;
        let dn = self.node_mut(dst_id);
        if let Node::Leaf(l) = dn {
            l.aggs.append(&mut src_aggs);
        }
        let ghost t1 = *self;
        let ghost dl2 = match t1.node_spec(dst_id) { Some(Node::Leaf(l)) => l, _ => arbitrary() };
        proof {
            assert(src_aggs@.len() == 0);
            assert(dl2.aggs@ =~= dl.aggs@ + sl.aggs@);
            t0.lemma_pair_sum_len(cs, d, 0);
            t1.lemma_wf_leaf(dst_id, 0, dl2);
        }
        let new_agg = self.leaf_agg(dst_id, Ghost(0));
        let ghost slot = ChildSlot { id: dst_id, agg: new_agg, slab_count: combined };
        proof {
            assert(ta.node_spec(parent_id) == t0.node_spec(parent_id));
            assert(tb.node_spec(parent_id) == ta.node_spec(parent_id));
            assert(t1.node_spec(parent_id) == tb.node_spec(parent_id));
        }
        let pn = self.node_mut(parent_id);
        if let Node::Internal(p) = pn {
            p.children[dst_idx].agg = new_agg;
            p.children[dst_idx].slab_count = combined;
            p.children.remove(src_idx);
        }
        proof {
            let n2 = match self.node_spec(parent_id) { Some(Node::Internal(n)) => n, _ => arbitrary() };
            assert(n2.children@ =~= pre + seq![slot] + post);
            let changed = set![src_id, dst_id, parent_id];
            // only the pair and the parent changed
            assert forall|x: NodeId| !changed.contains(x) && t0.node_spec(x) is Some
                implies #[trigger] self.node_spec(x) == t0.node_spec(x) by {
                assert(ta.node_spec(x) == t0.node_spec(x));
                assert(tb.node_spec(x) == ta.node_spec(x));
                assert(t1.node_spec(x) == tb.node_spec(x));
            }
            assert forall|x: NodeId| t1.owned(dst_id, 0).contains(x) implies t1.node_spec(x) == self.node_spec(x) by {}
            Self::lemma_frame(&t1, self, dst_id, 0);
            Self::lemma_merged_slots(&t0, self, cs, d, slot, parent_id);
            // the free list still names empty slots
            assert forall|i: int| 0 <= i < self.free@.len() implies
                (#[trigger] self.free@[i] as int) < self.nodes@.len() && self.nodes@[self.free@[i] as int] is None by {
                assert(tb.free@[i] == self.free@[i]);
            }
            assert forall|x: NodeId| x != parent_id && old(self).node_spec(x) is Some
                && !t0.children_owned(cs, 0).contains(x)
                implies #[trigger] self.node_spec(x) == old(self).node_spec(x) by {
                t0.lemma_children_owned_contains(cs, d, 0);
                t0.lemma_children_owned_contains(cs, sr, 0);
            }
        }
    }








    /// Merge a leaf's slab aggregates.
    fn leaf_agg(&self, id: NodeId, Ghost(h): Ghost<nat>) -> (r: A)
        requires self.wf_node(id, h), self.node_spec(id) matches Some(Node::Leaf(_)),
        ensures
            r.spec_len() == sum_len(self.slabs(id, h)),
            valid(self.slabs(id, h)) ==> r == fold(self.slabs(id, h)),
    {
        match self.node(id) {
            Node::Leaf(l) => Self::fold_leaf(&l.aggs),
            Node::Internal(_) => unreachable!(),
        }
    }

    /// An internal node's merged aggregate and slab count.
    fn internal_summary(&self, id: NodeId, Ghost(h): Ghost<nat>) -> (r: (A, usize))
        requires self.wf_node(id, h), self.node_spec(id) matches Some(Node::Internal(_)),
        ensures
            r.0.spec_len() == sum_len(self.slabs(id, h)),
            valid(self.slabs(id, h)) ==> r.0 == fold(self.slabs(id, h)),
            r.1 == self.slabs(id, h).len(),
    {
        match self.node(id) {
            Node::Internal(n) => {
                let agg = self.fold_children(Ghost(id), n, Ghost(h));
                let ghost cs = n.children@;
                let ghost hc = (h - 1) as nat;
                proof { self.lemma_wf_internal(id, h, *n); }
                let mut count: usize = 0;
                let mut k: usize = 0;
                while k < n.children.len()
                    invariant
                        k <= cs.len(), cs == n.children@, h > 0, hc == h - 1,
                        self.slots_ok(cs, hc),
                        self.children_slabs(cs, hc).len() <= usize::MAX,
                        count == self.children_slabs(cs.take(k as int), hc).len(),
                    decreases cs.len() - k,
                {
                    proof {
                        self.lemma_slot(cs, hc, k as int);
                        self.lemma_children_slabs_split(cs, k as int, hc);
                        self.lemma_children_slabs_push(cs, k as int, hc);
                    }
                    count += n.children[k].slab_count;
                    k += 1;
                }
                proof { assert(cs.take(k as int) =~= cs); }
                (agg, count)
            }
            Node::Leaf(_) => unreachable!(),
        }
    }

    /// `n` throwaway slots, to be overwritten.
    fn placeholders(n: usize) -> (r: Vec<ChildSlot<A>>)
        ensures r@.len() == n,
    {
        let mut v = Vec::with_capacity(n);
        let mut i = 0;
        while i < n
            invariant v@.len() == i, i <= n,
            decreases n - i,
        {
            v.push(ChildSlot { id: 0, agg: A::empty(), slab_count: 0 });
            i += 1;
        }
        v
    }

    /// `n` items in ⌈n/B⌉ chunks, as even as possible: chunk `c` has
    /// `base + (c < rem)` items.  Returns `(chunks, base, rem)`.
    fn chunking(n: usize) -> (r: (usize, usize, usize))
        requires n > 0,
        ensures r.0 >= 1, r.1 >= 1, r.2 < r.0, r.0 * r.1 + r.2 == n, r.0 <= n, n > 1 ==> r.0 < n,
    {
        let num = n / B + if n % B != 0 { 1 } else { 0 };
        assert(1 <= num <= n);
        let base = n / num;
        let rem = n % num;
        assert(num * base + rem == n) by (nonlinear_arith) requires base == n / num, rem == n % num, num > 0;
        assert(base >= 1) by (nonlinear_arith) requires base == n / num, num <= n, num > 0;
        (num, base, rem)
    }

    /// Where chunk `c` of a chunking starts.
    fn chunk_start(c: usize, Ghost(num): Ghost<usize>, base: usize, rem: usize) -> (r: usize)
        requires c <= num, rem < num, num * base + rem <= usize::MAX,
        ensures r == chunk_at(c as int, base as int, rem as int),
    {
        assert(c * base <= num * base) by (nonlinear_arith) requires c <= num;
        c * base + if c < rem { c } else { rem }
    }

    /// Split `aggs` into leaves (one per chunk), the first being
    /// `first_id`.
    #[verifier::rlimit(30)]
    fn split_leaf(&mut self, first_id: NodeId, aggs: Vec<A>) -> (r: Vec<ChildSlot<A>>)
        requires
            old(self).arena_ok(),
            old(self).node_spec(first_id) matches Some(Node::Leaf(_)),
            aggs@.len() > 0, sum_len(aggs@) <= usize::MAX,
        ensures
            final(self).arena_ok(),
            final(self).slots_ok(r@, 0),
            final(self).children_slabs(r@, 0) == aggs@,
            r@.len() > 0, r@[0].id == first_id,
            forall|x: NodeId| #[trigger] final(self).children_owned(r@, 0).contains(x)
                ==> x == first_id || old(self).node_spec(x) is None,
            forall|x: NodeId| x != first_id && old(self).node_spec(x) is Some
                ==> #[trigger] final(self).node_spec(x) == old(self).node_spec(x),
            final(self).root == old(self).root,
            final(self).total_slabs == old(self).total_slabs,
    {
        let ghost all = aggs@;
        let ghost n = all.len() as int;
        let mut aggs = aggs;
        let (num, base, rem) = Self::chunking(aggs.len());
        let ghost (b, r) = (base as int, rem as int);
        proof { lemma_chunk(num as int, b, r, 0); }
        // Build the chunks last to first, splitting each off the end.
        let mut out: Vec<ChildSlot<A>> = Self::placeholders(num);
        proof {
            self.lemma_slots_nil(0);
            assert(out@.subrange(num as int, num as int) =~= Seq::<ChildSlot<A>>::empty());
        }
        let mut c = num;
        while c > 1
            invariant
                out@.len() == num,
                1 <= c <= num, rem < num, num * base + rem == n, n <= usize::MAX, b == base, r == rem,
                base >= 1, chunk_at(num as int, b, r) == n, chunk_at(0, b, r) == 0,
                sum_len(all) <= usize::MAX, all.len() == n,
                aggs@ == all.subrange(0, chunk_at(c as int, b, r)),
                self.arena_ok(),
                self.node_spec(first_id) matches Some(Node::Leaf(_)),
                self.slots_ok(out@.subrange(c as int, num as int), 0),
                self.children_slabs(out@.subrange(c as int, num as int), 0) == all.subrange(chunk_at(c as int, b, r), n),
                forall|x: NodeId| #[trigger] self.children_owned(out@.subrange(c as int, num as int), 0).contains(x) ==> old(self).node_spec(x) is None,
                forall|x: NodeId| x != first_id && old(self).node_spec(x) is Some
                    ==> #[trigger] self.node_spec(x) == old(self).node_spec(x),
                self.root == old(self).root,
                self.total_slabs == old(self).total_slabs,
            decreases c,
        {
            let ghost o = out@.subrange(c as int, num as int);
            c -= 1;
            let ghost (lo, hi) = (chunk_at(c as int, b, r), chunk_at(c + 1, b, r));
            proof { lemma_chunk(num as int, b, r, c as int); }
            let at = Self::chunk_start(c, Ghost(num), base, rem);
            let chunk = aggs.split_off(at);
            proof {
                assert(chunk@ =~= all.subrange(lo, hi));
                lemma_sum_len_sub(all, lo, hi);
            }
            let slab_count = chunk.len();
            let agg = Self::fold_leaf(&chunk);
            let ghost t0 = *self;
            let ghost leaf = Leaf { aggs: chunk };
            let id = self.alloc(Node::Leaf(Leaf { aggs: chunk }));
            let slot = ChildSlot { id, agg, slab_count };
            proof {
                if t0.children_owned(o, 0).contains(id) { t0.lemma_slots_live(o, 0, id); }
                Self::lemma_slots_keep(&t0, self, o, 0, set![id]);
                self.lemma_wf_leaf(id, 0, leaf);
                self.lemma_slot_one(slot, 0);
                self.lemma_slots_prepend(slot, o, 0);
                assert(all.subrange(lo, n) =~= all.subrange(lo, hi) + all.subrange(hi, n));
            }
            out.set(c, slot);
            proof { assert(out@.subrange(c as int, num as int) =~= o.insert(0, slot)); }
        }
        let ghost o = out@.subrange(1, num as int);
        let ghost t0 = *self;
        let ghost hi = chunk_at(1, b, r);
        proof { lemma_chunk(num as int, b, r, 0); }
        let slab_count = aggs.len();
        proof { lemma_sum_len_sub(all, 0, hi); }
        let agg = Self::fold_leaf(&aggs);
        let ghost leaf = Leaf { aggs: aggs };
        let node = self.node_mut(first_id);
        if let Node::Leaf(l) = node {
            l.aggs = aggs;
        }
        let slot = ChildSlot { id: first_id, agg, slab_count };
        proof {
            assert(self.node_spec(first_id) == Some(Node::Leaf(leaf)));
            if t0.children_owned(o, 0).contains(first_id) { t0.lemma_slots_live(o, 0, first_id); }
            Self::lemma_slots_keep(&t0, self, o, 0, set![first_id]);
            self.lemma_wf_leaf(first_id, 0, leaf);
            self.lemma_slot_one(slot, 0);
            self.lemma_slots_prepend(slot, o, 0);
            assert(all =~= all.subrange(0, hi) + all.subrange(hi, n));
            assert forall|x: NodeId| x != first_id && old(self).node_spec(x) is Some
                implies #[trigger] self.node_spec(x) == old(self).node_spec(x) by {
                assert(self.node_spec(x) == t0.node_spec(x));
            }
        }
        out.set(0, slot);
        proof { assert(out@ =~= o.insert(0, slot)); }
        out
    }

    /// Split `children` (slots one level down) into internal nodes (one
    /// per chunk), the first being `first_id`.
    #[verifier::rlimit(60)]
    #[verifier::spinoff_prover]
    fn split_internal(&mut self, first_id: NodeId, children: Vec<ChildSlot<A>>, Ghost(h): Ghost<nat>) -> (r: Vec<ChildSlot<A>>)
        requires
            h > 0,
            old(self).arena_ok(),
            old(self).node_spec(first_id) matches Some(Node::Internal(_)),
            children@.len() > 0,
            old(self).slots_ok(children@, (h - 1) as nat),
            !old(self).children_owned(children@, (h - 1) as nat).contains(first_id),
            sum_len(old(self).children_slabs(children@, (h - 1) as nat)) <= usize::MAX,
            old(self).children_slabs(children@, (h - 1) as nat).len() <= usize::MAX,
        ensures
            final(self).arena_ok(),
            final(self).slots_ok(r@, h),
            final(self).children_slabs(r@, h) == old(self).children_slabs(children@, (h - 1) as nat),
            r@.len() > 0, r@[0].id == first_id,
            r@.len() < children@.len() || children@.len() == 1,
            forall|x: NodeId| #[trigger] final(self).children_owned(r@, h).contains(x)
                ==> x == first_id || old(self).node_spec(x) is None
                    || old(self).children_owned(children@, (h - 1) as nat).contains(x),
            forall|x: NodeId| x != first_id && old(self).node_spec(x) is Some
                ==> #[trigger] final(self).node_spec(x) == old(self).node_spec(x),
            final(self).root == old(self).root,
            final(self).total_slabs == old(self).total_slabs,
    {
        let ghost hc = (h - 1) as nat;
        let ghost all = children@;
        let ghost n = all.len() as int;
        let ghost slabs_all = old(self).children_slabs(all, hc);
        let ghost owned_all = old(self).children_owned(all, hc);
        let mut children = children;
        let (num, base, rem) = Self::chunking(children.len());
        let ghost (b, r) = (base as int, rem as int);
        proof { lemma_chunk(num as int, b, r, 0); }
        let mut out: Vec<ChildSlot<A>> = Self::placeholders(num);
        proof {
            self.lemma_slots_nil(h);
            assert(out@.subrange(num as int, num as int) =~= Seq::<ChildSlot<A>>::empty());
            assert forall|x: NodeId| #[trigger] owned_all.contains(x) implies old(self).node_spec(x) is Some by {
                old(self).lemma_slots_live(all, hc, x);
            }
            assert(all.subrange(chunk_at(num as int, b, r), n) =~= Seq::<ChildSlot<A>>::empty());
        }
        let mut c = num;
        while c > 1
            invariant
                out@.len() == num,
                h > 0, hc == h - 1,
                1 <= c <= num, rem < num, num * base + rem == n, n <= usize::MAX, b == base, r == rem,
                base >= 1, chunk_at(num as int, b, r) == n, chunk_at(0, b, r) == 0,
                all.len() == n,
                children@ == all.subrange(0, chunk_at(c as int, b, r)),
                self.arena_ok(),
                self.node_spec(first_id) matches Some(Node::Internal(_)),
                self.slots_ok(all, hc),
                self.children_slabs(all, hc) == slabs_all,
                self.children_owned(all, hc) == owned_all,
                !owned_all.contains(first_id),
                sum_len(slabs_all) <= usize::MAX, slabs_all.len() <= usize::MAX,
                forall|x: NodeId| #[trigger] owned_all.contains(x) ==> old(self).node_spec(x) is Some,
                self.slots_ok(out@.subrange(c as int, num as int), h),
                out@.len() == num,
                self.children_slabs(out@.subrange(c as int, num as int), h) == self.children_slabs(all.subrange(chunk_at(c as int, b, r), n), hc),
                forall|x: NodeId| #[trigger] self.children_owned(out@.subrange(c as int, num as int), h).contains(x)
                    ==> old(self).node_spec(x) is None
                        || self.children_owned(all.subrange(chunk_at(c as int, b, r), n), hc).contains(x),
                forall|x: NodeId| x != first_id && old(self).node_spec(x) is Some
                    ==> #[trigger] self.node_spec(x) == old(self).node_spec(x),
                self.root == old(self).root,
                self.total_slabs == old(self).total_slabs,
            decreases c,
        {
            let ghost o = out@.subrange(c as int, num as int);
            c -= 1;
            let ghost (lo, hi) = (chunk_at(c as int, b, r), chunk_at(c + 1, b, r));
            proof { lemma_chunk(num as int, b, r, c as int); }
            let at = Self::chunk_start(c, Ghost(num), base, rem);
            let chunk = children.split_off(at);
            let ghost t0 = *self;
            let ghost node = Internal { children: chunk };
            let ghost rest = all.subrange(hi, n);
            proof {
                assert(chunk@ =~= all.subrange(lo, hi));
                t0.lemma_slots_sub(all, lo, hi, hc);
                t0.lemma_slots_apart(all, lo, hi, n, hc);
                assert(all.subrange(lo, n) =~= chunk@ + rest);
                t0.lemma_children_slabs_concat(chunk@, rest, hc);
                t0.lemma_children_owned_concat(chunk@, rest, hc);
                // the chunk's slabs are a run of all of them
                assert(all =~= all.subrange(0, lo) + all.subrange(lo, n));
                t0.lemma_children_slabs_concat(all.subrange(0, lo), all.subrange(lo, n), hc);
                let p = t0.children_slabs(all.subrange(0, lo), hc).len() as int;
                let q = p + t0.children_slabs(chunk@, hc).len();
                assert(slabs_all.subrange(p, q) =~= t0.children_slabs(chunk@, hc));
                lemma_sum_len_sub(slabs_all, p, q);
            }
            let id = self.alloc(Node::Internal(Internal { children: chunk }));
            proof {
                assert(!owned_all.contains(id));
                if t0.children_owned(o, h).contains(id) {
                    t0.lemma_slots_live(o, h, id);
                }
                Self::lemma_slots_keep(&t0, self, all, hc, set![id]);
                Self::lemma_slots_keep(&t0, self, o, h, set![id]);
                Self::lemma_slots_keep(&t0, self, chunk@, hc, set![id]);
                assert forall|i: int| 0 <= i < chunk@.len() implies
                    !self.owned(#[trigger] chunk@[i].id, hc).contains(id) by {
                    self.lemma_children_owned_contains(chunk@, i, hc);
                }
                self.lemma_wf_internal(id, h, node);
            }
            let (agg, slab_count) = self.internal_summary(id, Ghost(h));
            let slot = ChildSlot { id, agg, slab_count };
            proof {
                self.lemma_wf_internal(id, h, node);
                // the chunk's first child has a slab
                self.lemma_slot(chunk@, hc, 0);
                self.lemma_children_slabs_split(chunk@, 0, hc);
                self.lemma_slot_one(slot, h);
                assert(self.owned(slot.id, h).disjoint(self.children_owned(o, h))) by {
                    assert forall|x: NodeId| self.owned(id, h).contains(x) implies
                        !self.children_owned(o, h).contains(x) by {
                        if x == id {
                        } else {
                            assert(self.children_owned(chunk@, hc).contains(x));
                            self.lemma_children_owned_contains(all, 0, hc);
                            assert(owned_all.contains(x)) by {
                                t0.lemma_slots_sub(all, lo, hi, hc);
                            }
                        }
                    }
                };
                self.lemma_slots_prepend(slot, o, h);
                // what the list now stands for: this chunk and the rest
                t0.lemma_slots_sub(all, hi, n, hc);
                Self::lemma_slots_keep(&t0, self, rest, hc, set![id]);
                self.lemma_children_slabs_concat(chunk@, rest, hc);
                self.lemma_children_owned_concat(chunk@, rest, hc);
                assert(id != first_id);
                assert(old(self).node_spec(id) is None);
            }
            out.set(c, slot);
            proof { assert(out@.subrange(c as int, num as int) =~= o.insert(0, slot)); }
        }
        let ghost o = out@.subrange(1, num as int);
        let ghost t0 = *self;
        let ghost hi = chunk_at(1, b, r);
        let ghost rest = all.subrange(hi, n);
        let ghost first = children@;
        proof {
            lemma_chunk(num as int, b, r, 0);
            assert(first =~= all.subrange(0, hi));
            t0.lemma_slots_sub(all, 0, hi, hc);
            t0.lemma_slots_sub(all, hi, n, hc);
            t0.lemma_slots_apart(all, 0, hi, n, hc);
            assert(all =~= first + rest);
            t0.lemma_children_slabs_concat(first, rest, hc);
            t0.lemma_children_owned_concat(first, rest, hc);
            let p = t0.children_slabs(first, hc).len() as int;
            assert(slabs_all.subrange(0, p) =~= t0.children_slabs(first, hc));
            lemma_sum_len_sub(slabs_all, 0, p);
        }
        let ghost node = Internal { children: children };
        let nd = self.node_mut(first_id);
        if let Node::Internal(nd) = nd {
            nd.children = children;
        }
        proof {
            assert(self.node_spec(first_id) == Some(Node::Internal(node)));
            if t0.children_owned(o, h).contains(first_id) {
                assert(old(self).node_spec(first_id) is Some);
            }
            Self::lemma_slots_keep(&t0, self, o, h, set![first_id]);
            Self::lemma_slots_keep(&t0, self, first, hc, set![first_id]);
            Self::lemma_slots_keep(&t0, self, rest, hc, set![first_id]);
            assert forall|i: int| 0 <= i < first.len() implies
                !self.owned(#[trigger] first[i].id, hc).contains(first_id) by {
                self.lemma_children_owned_contains(first, i, hc);
            }
            self.lemma_wf_internal(first_id, h, node);
        }
        let (agg, slab_count) = self.internal_summary(first_id, Ghost(h));
        let slot = ChildSlot { id: first_id, agg, slab_count };
        proof {
            self.lemma_wf_internal(first_id, h, node);
            // the first chunk's first child has a slab
            self.lemma_slot(first, hc, 0);
            self.lemma_children_slabs_split(first, 0, hc);
            self.lemma_slot_one(slot, h);
            assert(self.owned(first_id, h).disjoint(self.children_owned(o, h))) by {
                assert forall|x: NodeId| self.owned(first_id, h).contains(x) implies
                    !self.children_owned(o, h).contains(x) by {
                    if x != first_id {
                        assert(self.children_owned(first, hc).contains(x));
                        assert(owned_all.contains(x));
                    }
                }
            };
            self.lemma_slots_prepend(slot, o, h);
            self.lemma_children_slabs_concat(first, rest, hc);
            self.lemma_children_owned_concat(first, rest, hc);
            assert forall|x: NodeId| x != first_id && old(self).node_spec(x) is Some
                implies #[trigger] self.node_spec(x) == old(self).node_spec(x) by {
                assert(self.node_spec(x) == t0.node_spec(x));
            }
        }
        out.set(0, slot);
        proof { assert(out@ =~= o.insert(0, slot)); }
        out
    }

    /// Group sibling slots (height `h - 1`) under new internal nodes, one
    /// per chunk.
    fn group(&mut self, slots: Vec<ChildSlot<A>>, Ghost(h): Ghost<nat>) -> (r: Vec<ChildSlot<A>>)
        requires
            h > 0,
            old(self).arena_ok(),
            slots@.len() > 0,
            old(self).slots_ok(slots@, (h - 1) as nat),
            sum_len(old(self).children_slabs(slots@, (h - 1) as nat)) <= usize::MAX,
            old(self).children_slabs(slots@, (h - 1) as nat).len() <= usize::MAX,
        ensures
            final(self).arena_ok(),
            final(self).slots_ok(r@, h),
            final(self).children_slabs(r@, h) == old(self).children_slabs(slots@, (h - 1) as nat),
            r@.len() > 0,
            r@.len() < slots@.len() || slots@.len() == 1,
            forall|x: NodeId| #[trigger] final(self).children_owned(r@, h).contains(x)
                ==> old(self).node_spec(x) is None
                    || old(self).children_owned(slots@, (h - 1) as nat).contains(x),
            forall|x: NodeId| old(self).node_spec(x) is Some
                ==> #[trigger] final(self).node_spec(x) == old(self).node_spec(x),
            final(self).root == old(self).root,
            final(self).total_slabs == old(self).total_slabs,
    {
        let ghost t0 = *self;
        let first = self.alloc(Node::Internal(Internal { children: Vec::new() }));
        proof {
            if t0.children_owned(slots@, (h - 1) as nat).contains(first) {
                t0.lemma_slots_live(slots@, (h - 1) as nat, first);
            }
            Self::lemma_slots_keep(&t0, self, slots@, (h - 1) as nat, set![first]);
        }
        self.split_internal(first, slots, Ghost(h))
    }

    /// Install the slots a splice returned for the root (height `h`).
    fn set_root(&mut self, slots: Vec<ChildSlot<A>>, Ghost(h): Ghost<nat>)
        requires
            old(self).arena_ok(),
            old(self).slots_ok(slots@, h),
            sum_len(old(self).children_slabs(slots@, h)) <= usize::MAX,
            old(self).children_slabs(slots@, h).len() <= usize::MAX,
            slots@.len() == 0 ==> old(self).node_spec(old(self).root) is Some,
        ensures
            final(self).arena_ok(),
            exists|hh: nat| final(self).wf_node(final(self).root, hh)
                && final(self).slabs(final(self).root, hh) == old(self).children_slabs(slots@, h),
            final(self).total_slabs == old(self).total_slabs,
    {
        let mut slots = slots;
        if slots.len() == 0 {
            // The whole tree was deleted: reset to a fresh empty leaf,
            // reusing the root id.
            let ghost t0 = *self;
            let root = self.root;
            self.nodes[root as usize] = Some(Node::Leaf(Leaf { aggs: Vec::new() }));
            proof {
                let leaf = match self.node_spec(root) { Some(Node::Leaf(l)) => l, _ => arbitrary() };
                assert(self.node_spec(root) == Some(Node::Leaf(leaf)));
                self.lemma_slots_nil(h);
                assert(slots@ =~= Seq::<ChildSlot<A>>::empty());
                lemma_valid_empty::<A>();
                self.lemma_wf_leaf(root, 0, leaf);
                assert(leaf.aggs@ =~= Seq::<A>::empty());
                assert forall|i: int| 0 <= i < self.free@.len() implies
                    (#[trigger] self.free@[i] as int) < self.nodes@.len() && self.nodes@[self.free@[i] as int] is None by {
                    assert(t0.free@[i] != root);
                }
                old(self).lemma_slots_nil(h);
                assert(self.wf_node(self.root, 0) && self.slabs(self.root, 0) == old(self).children_slabs(slots@, h));
            }
            return;
        }
        // Grow new root levels while there are too many for one node.
        let ghost mut hh = h;
        let ghost all = old(self).children_slabs(slots@, h);
        while slots.len() > B
            invariant
                slots@.len() > 0,
                self.arena_ok(),
                self.slots_ok(slots@, hh),
                self.children_slabs(slots@, hh) == all,
                sum_len(all) <= usize::MAX, all.len() <= usize::MAX,
                self.total_slabs == old(self).total_slabs,
            decreases slots@.len(),
        {
            slots = self.group(slots, Ghost(hh + 1));
            proof { hh = hh + 1; }
        }
        if slots.len() == 1 {
            let id = slots[0].id;
            proof {
                self.lemma_slot(slots@, hh, 0);
                self.lemma_children_slabs_one(slots@[0], hh);
                assert(slots@ =~= seq![slots@[0]]);
            }
            let ghost t1 = *self;
            self.root = id;
            proof {
                assert forall|x: NodeId| t1.owned(id, hh).contains(x) implies t1.node_spec(x) == self.node_spec(x) by {}
                Self::lemma_frame(&t1, self, id, hh);
                assert(self.wf_node(self.root, hh) && self.slabs(self.root, hh) == all);
            }
            return;
        }
        let ghost t0 = *self;
        let ghost node = Internal { children: slots };
        let root = self.alloc(Node::Internal(Internal { children: slots }));
        let ghost t1 = *self;
        proof {
            if t0.children_owned(node.children@, hh).contains(root) {
                t0.lemma_slots_live(node.children@, hh, root);
            }
            Self::lemma_slots_keep(&t0, &t1, node.children@, hh, set![root]);
            assert forall|i: int| 0 <= i < node.children@.len()
                implies !t1.owned(#[trigger] node.children@[i].id, hh).contains(root) by {
                t1.lemma_children_owned_contains(node.children@, i, hh);
            }
            t1.lemma_wf_internal(root, hh + 1, node);
        }
        self.root = root;
        proof {
            assert forall|x: NodeId| t1.owned(root, hh + 1).contains(x) implies t1.node_spec(x) == self.node_spec(x) by {}
            Self::lemma_frame(&t1, self, root, hh + 1);
            assert(self.wf_node(self.root, hh + 1) && self.slabs(self.root, hh + 1) == all);
        }
    }

    /// While the root is an internal node with exactly one child, promote
    /// the child.  Keeps query depth proportional to the live tree after
    /// large deletions.  Called once per structural mutation.
    fn collapse_root(&mut self)
        requires old(self).wf(),
        ensures final(self).wf(), final(self)@ == old(self)@,
    {
        loop
            invariant self.wf(), self@ == old(self)@,
            decreases self.height(),
        {
            let ghost t0 = *self;
            let ghost h = self.height();
            proof { assert(self.wf_node(self.root, h)); }
            let child = match self.node(self.root) {
                Node::Internal(n) => {
                    if n.children.len() == 1 {
                        proof {
                            self.lemma_child(self.root, h, *n, 0);
                            self.lemma_children_slabs_one(n.children@[0], (h - 1) as nat);
                            assert(n.children@ =~= seq![n.children@[0]]);
                        }
                        n.children[0].id
                    } else {
                        return;
                    }
                },
                Node::Leaf(_) => return,
            };
            let ghost old_root = self.root;
            let ghost n0 = match t0.node_spec(t0.root) { Some(Node::Internal(n)) => n, _ => arbitrary() };
            proof { t0.lemma_child(t0.root, h, n0, 0); }
            self.free(self.root);
            let ghost t1 = *self;
            self.root = child;
            proof {
                let hc = (h - 1) as nat;
                assert(self.nodes == t1.nodes);
                assert(child == n0.children@[0].id);
                assert(!t0.owned(child, hc).contains(old_root));
                assert forall|x: NodeId| t0.owned(child, hc).contains(x) implies
                    t0.node_spec(x) == self.node_spec(x) by {
                    assert(x != old_root);
                    assert(t1.node_spec(x) == t0.node_spec(x));
                }
                Self::lemma_frame(&t0, self, child, hc);
                self.lemma_height_unique(self.root, hc, self.height());
                assert forall|i: int| 0 <= i < self.free@.len() implies
                    (#[trigger] self.free@[i] as int) < self.nodes@.len() && self.nodes@[self.free@[i] as int] is None by {}
            }
        }
    }

    /// A tree of the slabs `aggs`: leaves of at most B, grouped level by
    /// level.
    pub(crate) fn from_vec(aggs: Vec<A>) -> (t: Self)
        requires sum_len(aggs@) <= usize::MAX,
        ensures t.wf(), t@ == aggs@,
    {
        let mut t = Self::new();
        if aggs.len() == 0 {
            proof { assert(aggs@ =~= t@); }
            return t;
        }
        let ghost all = aggs@;
        let n = aggs.len();
        let root = t.root;
        let slots = t.split_leaf(root, aggs);
        t.set_root(slots, Ghost(0));
        let ghost t2 = t;
        let ghost hh = choose|hh: nat| t2.wf_node(t2.root, hh) && t2.slabs(t2.root, hh) == all;
        t.total_slabs = n;
        proof {
            assert forall|x: NodeId| t2.owned(t2.root, hh).contains(x) implies t2.node_spec(x) == t.node_spec(x) by {}
            Self::lemma_frame(&t2, &t, t.root, hh);
            t.lemma_height_unique(t.root, hh, t.height());
        }
        t
    }

    /// Find the landing slab for `item_idx`: returns
    /// `(slab_idx, prefix_before_slab, items_before_slab)`.  The
    /// landing slab is the one containing item `item_idx`; if
    /// `item_idx >= total_items` returns the final position with the
    /// full prefix/items totals.
    ///
    /// O(log n) — descends the tree by `len`, accumulating `prefix`
    /// and `len` of children visited before the descent.
    #[verifier::rlimit(40)]
    pub(crate) fn find_slab_at_item(&self, item_idx: usize) -> (r: FoundSlab<A>)
        where A: PrefixAggregate
        requires
            self.wf(),
            valid(self@),
            forall|i: int| 0 <= i < self@.len() ==> #[trigger] self@[i].prefix_ok(),
        ensures
            r.index <= self@.len(),
            r.pos == sum_len(self@.subrange(0, r.index as int)),
            r.prefix == fold(self@.subrange(0, r.index as int)).spec_prefix(),
            r.index < self@.len() ==> r.pos <= item_idx < r.pos + self@[r.index as int].spec_len(),
            r.index == self@.len() ==> item_idx >= sum_len(self@),
    {
        let ghost view = self@;
        let ghost mut h = self.height();
        let ghost mut left = Seq::<A>::empty();
        let ghost mut right = Seq::<A>::empty();
        let mut prefix = A::zero_prefix();
        let mut pos = 0usize;
        let mut slab_base = 0usize;
        let mut remaining = item_idx;
        let mut node = self.root;
        proof {
            assert(self.wf_node(self.root, h));
            assert(self@ =~= left + self.slabs(node, h) + right);
            assert(view.subrange(0, 0) =~= Seq::<A>::empty());
            A::lemma_prefix_empty(A::prefix_zero());
        }
        loop
            invariant
                self.wf(), view == self@, valid(view),
                forall|i: int| 0 <= i < view.len() ==> #[trigger] view[i].prefix_ok(),
                self.wf_node(node, h),
                view == left + self.slabs(node, h) + right,
                slab_base == left.len(),
                pos == sum_len(left),
                prefix == fold(view.subrange(0, slab_base as int)).spec_prefix(),
                pos + remaining == item_idx,
                remaining < sum_len(self.slabs(node, h)) || (left.len() == 0 && right.len() == 0),
            decreases h,
        {
            proof {
                lemma_sum_len_concat(left, self.slabs(node, h));
                lemma_sum_len_concat(left + self.slabs(node, h), right);
                assert(view.subrange(0, left.len() as int) =~= left);
                lemma_valid_subrange(view, 0, view.len() as int);
                assert(view.subrange(0, view.len() as int) =~= view);
            }
            match self.node(node) {
                Node::Leaf(l) => {
                    let ghost aggs = l.aggs@;
                    assert(self.slabs(node, h) == aggs);
                    let mut i: usize = 0;
                    while i < l.aggs.len()
                        invariant
                            self.wf(), view == self@, valid(view),
                            forall|j: int| 0 <= j < view.len() ==> #[trigger] view[j].prefix_ok(),
                            i <= aggs.len(), aggs == l.aggs@,
                            view == left + aggs + right,
                            slab_base == left.len(),
                            pos == sum_len(left) + sum_len(aggs.take(i as int)),
                            prefix == fold(view.subrange(0, left.len() + i)).spec_prefix(),
                            pos + remaining == item_idx,
                            remaining + sum_len(aggs.take(i as int)) < sum_len(aggs)
                                || (left.len() == 0 && right.len() == 0),
                        decreases aggs.len() - i,
                    {
                        let a = &l.aggs[i];
                        let ghost j = left.len() + i;
                        proof {
                            assert(aggs.take(i + 1).drop_last() =~= aggs.take(i as int));
                            assert(view.subrange(0, j) =~= left + aggs.take(i as int));
                            assert(view[j] == aggs[i as int]);
                            lemma_sum_len_concat(left, aggs.take(i as int));
                        }
                        let l_len = a.len();
                        if remaining < l_len {
                            return FoundSlab { index: slab_base + i, prefix, pos };
                        }
                        proof {
                            lemma_valid_subrange(view, 0, view.len() as int);
                            assert(view.subrange(0, view.len() as int) =~= view);
                            lemma_fold_prefix(view, j, j + 1);
                            lemma_fold_one(view, j);
                        }
                        A::add_prefix(&mut prefix, a.prefix());
                        pos += l_len;
                        remaining -= l_len;
                        i += 1;
                    }
                    proof {
                        assert(aggs.take(i as int) =~= aggs);
                        assert(view =~= aggs);
                        assert(view.subrange(0, view.len() as int) =~= view);
                    }
                    return FoundSlab { index: slab_base + l.aggs.len(), prefix, pos };
                }
                Node::Internal(n) => {
                    let ghost cs = n.children@;
                    let ghost hc = (h - 1) as nat;
                    let ghost h0 = h;
                    proof {
                        self.lemma_child(node, h, *n, 0);
                    }
                    assert(self.slabs(node, h) == self.children_slabs(cs, hc));
                    let mut k: usize = 0;
                    loop
                        invariant_except_break
                            k <= cs.len(), h == h0,
                            self.wf_node(node, h),
                            self.node_spec(node) == Some(Node::Internal(*n)),
                            view == left + self.children_slabs(cs, hc) + right,
                            slab_base == left.len() + self.children_slabs(cs.take(k as int), hc).len(),
                            pos == sum_len(left) + sum_len(self.children_slabs(cs.take(k as int), hc)),
                            prefix == fold(view.subrange(0, slab_base as int)).spec_prefix(),
                            pos + remaining == item_idx,
                            remaining + sum_len(self.children_slabs(cs.take(k as int), hc))
                                < sum_len(self.children_slabs(cs, hc))
                                || (left.len() == 0 && right.len() == 0),
                        invariant
                            self.wf(), view == self@, valid(view), cs == n.children@, h0 > 0, hc == h0 - 1,
                            forall|j: int| 0 <= j < view.len() ==> #[trigger] view[j].prefix_ok(),
                        ensures
                            h < h0,
                            self.wf_node(node, h),
                            view == left + self.slabs(node, h) + right,
                            slab_base == left.len(),
                            pos == sum_len(left),
                            prefix == fold(view.subrange(0, slab_base as int)).spec_prefix(),
                            pos + remaining == item_idx,
                            remaining < sum_len(self.slabs(node, h)),
                        decreases cs.len() - k,
                    {
                        if k == n.children.len() {
                            proof {
                                assert(cs.take(k as int) =~= cs);
                                assert(view =~= self.children_slabs(cs, hc));
                                assert(view.subrange(0, view.len() as int) =~= view);
                            }
                            return FoundSlab { index: slab_base, prefix, pos };
                        }
                        let c = &n.children[k];
                        let ghost before = self.children_slabs(cs.take(k as int), hc);
                        let ghost mid = self.slabs(c.id, hc);
                        let ghost after = self.children_slabs(cs.skip(k + 1), hc);
                        proof {
                            assert(c == cs[k as int]);
                            self.lemma_child(node, h, *n, k as int);
                            self.lemma_children_slabs_split(cs, k as int, hc);
                            self.lemma_children_slabs_push(cs, k as int, hc);
                            lemma_sum_len_concat(before, mid);
                            lemma_sum_len_concat(before + mid, after);
                        }
                        let c_len = c.agg.len();
                        if remaining < c_len {
                            proof {
                                lemma_sum_len_concat(left, before);
                                left = left + before;
                                right = after + right;
                                h = hc;
                                assert(view =~= left + self.slabs(c.id, h) + right);
                            }
                            node = c.id;
                            break;
                        }
                        proof {
                            let (p, q) = (slab_base as int, slab_base + mid.len());
                            assert(view.subrange(p, q) =~= mid);
                            lemma_valid_subrange(view, p, q);
                            lemma_valid_subrange(view, 0, view.len() as int);
                            assert(view.subrange(0, view.len() as int) =~= view);
                            lemma_fold_prefix(view, p, q);
                        }
                        A::add_prefix(&mut prefix, c.agg.prefix());
                        pos += c_len;
                        slab_base += c.slab_count;
                        remaining -= c_len;
                        k += 1;
                    }
                }
            }
        }
    }

    /// Find the slab whose prefix sum first reaches `target`: returns
    /// `(slab_idx, prefix_before_slab, items_before_slab)`, or
    /// one-past-the-end if none does.
    ///
    /// O(log n) — descends by comparing `target` against running prefix
    /// sums of sibling subtrees.  If the running prefix can fall back
    /// below `target` after reaching it (signed prefixes), the descent may
    /// skip a crossing inside a subtree whose end is back below `target`;
    /// every slab end it passes that reaches `target` is followed, before
    /// the result, by one that doesn't.
    #[verifier::spinoff_prover]
    #[verifier::rlimit(40)]
    pub(crate) fn find_slab_at_prefix(&self, target: &A::Prefix) -> (r: (usize, A::Prefix, usize))
        where A: OrdPrefix
        requires
            self.wf(),
            valid(self@),
            forall|i: int| 0 <= i < self@.len() ==> #[trigger] self@[i].prefix_ok(),
        ensures
            r.0 <= self@.len(),
            r.1 == pre_at(self@, r.0 as int),
            r.2 == sum_len(self@.subrange(0, r.0 as int)),
            r.0 < self@.len() ==> reached(self@, *target, r.0 + 1),
            passed(self@, *target, r.0 as int),
            stays_reached(self@, *target)
                ==> forall|k: int| 0 < k <= r.0 ==> !#[trigger] reached(self@, *target, k),
    {
        let ghost view = self@;
        let ghost mut h = self.height();
        let ghost mut left = Seq::<A>::empty();
        let ghost mut right = Seq::<A>::empty();
        let mut acc_prefix = A::zero_prefix();
        let mut acc_items = 0usize;
        let mut slab_base = 0usize;
        let mut node = self.root;
        proof {
            assert(self.wf_node(self.root, h));
            assert(self@ =~= left + self.slabs(node, h) + right);
            assert(view.subrange(0, 0) =~= Seq::<A>::empty());
            A::lemma_prefix_empty(A::prefix_zero());
            reveal(passed);
            lemma_valid_subrange(view, 0, view.len() as int);
            assert(view.subrange(0, view.len() as int) =~= view);
        }
        loop
            invariant
                self.wf(), view == self@, valid(view),
                forall|i: int| 0 <= i < view.len() ==> #[trigger] view[i].prefix_ok(),
                self.wf_node(node, h),
                view == left + self.slabs(node, h) + right,
                slab_base == left.len(),
                acc_items == sum_len(left),
                acc_prefix == pre_at(view, slab_base as int),
                passed(view, *target, slab_base as int),
                (self.slabs(node, h).len() > 0 && reached(view, *target, (left.len() + self.slabs(node, h).len()) as int))
                    || (left.len() == 0 && right.len() == 0),
            decreases h,
        {
            proof {
                lemma_sum_len_concat(left, self.slabs(node, h));
                lemma_sum_len_concat(left + self.slabs(node, h), right);
                assert(view.subrange(0, left.len() as int) =~= left);
            }
            match self.node(node) {
                Node::Leaf(l) => {
                    let ghost aggs = l.aggs@;
                    assert(self.slabs(node, h) == aggs);
                    let mut i: usize = 0;
                    while i < l.aggs.len()
                        invariant
                            self.wf(), view == self@, valid(view),
                            forall|j: int| 0 <= j < view.len() ==> #[trigger] view[j].prefix_ok(),
                            i <= aggs.len(), aggs == l.aggs@,
                            view == left + aggs + right,
                            slab_base == left.len(),
                            acc_items == sum_len(left) + sum_len(aggs.take(i as int)),
                            acc_prefix == pre_at(view, left.len() + i),
                            passed(view, *target, left.len() + i),
                            (aggs.len() > 0 && reached(view, *target, (left.len() + aggs.len()) as int))
                                || (left.len() == 0 && right.len() == 0),
                            i > 0 ==> !reached(view, *target, left.len() + i),
                        decreases aggs.len() - i,
                    {
                        let a = &l.aggs[i];
                        let ghost j = left.len() + i;
                        proof {
                            assert(aggs.take(i + 1).drop_last() =~= aggs.take(i as int));
                            assert(view.subrange(0, j) =~= left + aggs.take(i as int));
                            assert(view[j] == aggs[i as int]);
                            lemma_sum_len_concat(left, aggs.take(i as int));
                            lemma_fold_prefix(view, j, j + 1);
                            lemma_fold_one(view, j);
                            assert(aggs =~= aggs.take(i + 1) + aggs.skip(i + 1));
                            lemma_sum_len_concat(aggs.take(i + 1), aggs.skip(i + 1));
                            lemma_sum_len_concat(left, aggs);
                            lemma_sum_len_concat(left + aggs, right);
                        }
                        let after = A::plus_prefix(&acc_prefix, a.prefix());
                        if A::le_prefix(target, &after) {
                            proof {
                                if stays_reached(view, *target) {
                                    lemma_passed_first(view, *target, j);
                                }
                            }
                            return (slab_base + i, acc_prefix, acc_items);
                        }
                        proof {
                            lemma_passed_step(view, *target, j, j + 1);
                        }
                        acc_prefix = after;
                        acc_items += a.len();
                        i += 1;
                    }
                    proof {
                        assert(aggs.take(i as int) =~= aggs);
                        assert(view =~= aggs);
                        assert(view.subrange(0, view.len() as int) =~= view);
                        if stays_reached(view, *target) {
                            lemma_passed_first(view, *target, view.len() as int);
                        }
                    }
                    return (slab_base + l.aggs.len(), acc_prefix, acc_items);
                }
                Node::Internal(n) => {
                    let ghost cs = n.children@;
                    let ghost hc = (h - 1) as nat;
                    let ghost h0 = h;
                    proof {
                        self.lemma_child(node, h, *n, 0);
                    }
                    assert(self.slabs(node, h) == self.children_slabs(cs, hc));
                    let mut k: usize = 0;
                    proof {
                        assert(sum_len(left) + sum_len(self.children_slabs(cs, hc)) <= usize::MAX);
                    }
                    loop
                        invariant_except_break
                            k <= cs.len(), h == h0,
                            self.wf_node(node, h),
                            self.node_spec(node) == Some(Node::Internal(*n)),
                            view == left + self.children_slabs(cs, hc) + right,
                            slab_base == left.len() + self.children_slabs(cs.take(k as int), hc).len(),
                            acc_items == sum_len(left) + sum_len(self.children_slabs(cs.take(k as int), hc)),
                            acc_prefix == pre_at(view, slab_base as int),
                            passed(view, *target, slab_base as int),
                            (self.children_slabs(cs, hc).len() > 0
                                && reached(view, *target, (left.len() + self.children_slabs(cs, hc).len()) as int))
                                || (left.len() == 0 && right.len() == 0),
                            k > 0 ==> !reached(view, *target, slab_base as int),
                            sum_len(left) + sum_len(self.children_slabs(cs, hc)) <= usize::MAX,
                        invariant
                            self.wf(), view == self@, valid(view), cs == n.children@, cs.len() > 0, h0 > 0, hc == h0 - 1,
                            forall|j: int| 0 <= j < view.len() ==> #[trigger] view[j].prefix_ok(),
                        ensures
                            h < h0,
                            self.wf_node(node, h),
                            view == left + self.slabs(node, h) + right,
                            slab_base == left.len(),
                            acc_items == sum_len(left),
                            acc_prefix == pre_at(view, slab_base as int),
                            passed(view, *target, slab_base as int),
                            self.slabs(node, h).len() > 0
                                && reached(view, *target, (left.len() + self.slabs(node, h).len()) as int),
                        decreases cs.len() - k,
                    {
                        if k == n.children.len() {
                            proof {
                                assert(cs.take(k as int) =~= cs);
                                assert(view =~= self.children_slabs(cs, hc));
                                assert(view.subrange(0, view.len() as int) =~= view);
                                if stays_reached(view, *target) {
                                    lemma_passed_first(view, *target, view.len() as int);
                                }
                            }
                            return (slab_base, acc_prefix, acc_items);
                        }
                        let c = &n.children[k];
                        let ghost before = self.children_slabs(cs.take(k as int), hc);
                        let ghost mid = self.slabs(c.id, hc);
                        let ghost rest = self.children_slabs(cs.skip(k + 1), hc);
                        let ghost (p, q) = (slab_base as int, slab_base + mid.len());
                        proof {
                            assert(c == cs[k as int]);
                            self.lemma_child(node, h, *n, k as int);
                            self.lemma_children_slabs_split(cs, k as int, hc);
                            self.lemma_children_slabs_push(cs, k as int, hc);
                            lemma_sum_len_concat(before, mid);
                            lemma_sum_len_concat(before + mid, rest);
                            lemma_seq_part(left, before, mid, rest, right);
                            lemma_valid_subrange(view, p, q);
                            lemma_fold_prefix(view, p, q);
                        }
                        let after = A::plus_prefix(&acc_prefix, c.agg.prefix());
                        if A::le_prefix(target, &after) {
                            proof {
                                lemma_sum_len_concat(left, before);
                                left = left + before;
                                right = rest + right;
                                h = hc;
                            }
                            node = c.id;
                            break;
                        }
                        proof {
                            lemma_passed_step(view, *target, p, q);
                        }
                        acc_prefix = after;
                        acc_items += c.agg.len();
                        slab_base += c.slab_count;
                        k += 1;
                    }
                }
            }
        }
    }

    /// Slab `slab_idx`'s aggregate. Unspecified: only overflow checks read
    /// it (`IndexOps::ix_weight`), and they can only panic.
    #[verifier::external_body]
    pub(crate) fn slab_agg(&self, slab_idx: usize) -> A
        requires
            self.wf(),
            slab_idx < self@.len(),
    {
        let mut remaining = slab_idx;
        let mut id = self.root;
        loop {
            match self.nodes[id as usize].as_ref().expect("freed node id") {
                Node::Leaf(l) => return l.aggs[remaining].clone(),
                Node::Internal(n) => {
                    let mut next = None;
                    for c in &n.children {
                        if remaining < c.slab_count {
                            next = Some(c.id);
                            break;
                        }
                        remaining -= c.slab_count;
                    }
                    id = next.expect("slab_idx should have been bounds-checked");
                }
            }
        }
    }

    /// Aggregate of all slabs in the tree.
    pub(crate) fn root_agg(&self) -> (r: A)
        requires self.wf(),
        ensures
            r.spec_len() == sum_len(self@),
            valid(self@) ==> r == fold(self@),
    {
        proof { assert(self.wf_node(self.root, self.height())); }
        match self.node(self.root) {
            Node::Leaf(l) => Self::fold_leaf(&l.aggs),
            Node::Internal(n) => self.fold_children(Ghost(self.root), n, Ghost(self.height())),
        }
    }

    proof fn lemma_children_congr_owned(t1: &Self, cs1: Seq<ChildSlot<A>>, t2: &Self, cs2: Seq<ChildSlot<A>>, h: nat)
        requires
            cs1.len() == cs2.len(),
            forall|i: int| 0 <= i < cs1.len() ==> t1.owned(#[trigger] cs1[i].id, h) == t2.owned(cs2[i].id, h),
        ensures t1.children_owned(cs1, h) == t2.children_owned(cs2, h),
        decreases cs1.len(),
    {
        if cs1.len() > 0 {
            assert forall|i: int| 0 <= i < cs1.drop_last().len() implies
                t1.owned(#[trigger] cs1.drop_last()[i].id, h) == t2.owned(cs2.drop_last()[i].id, h) by {
                assert(cs1.drop_last()[i] == cs1[i]);
            }
            Self::lemma_children_congr_owned(t1, cs1.drop_last(), t2, cs2.drop_last(), h);
            assert(t1.owned(cs1[cs1.len() - 1].id, h) == t2.owned(cs2[cs1.len() - 1].id, h));
        }
    }

    /// Each child's nodes are among its parent's.
    proof fn lemma_children_owned_contains(&self, cs: Seq<ChildSlot<A>>, k: int, h: nat)
        requires 0 <= k < cs.len(),
        ensures self.owned(cs[k].id, h).subset_of(self.children_owned(cs, h)),
        decreases cs.len(),
    {
        if k < cs.len() - 1 {
            self.lemma_children_owned_contains(cs.drop_last(), k, h);
            assert(cs.drop_last()[k] == cs[k]);
        }
    }

    /// Every node of a well-formed subtree is live.
    proof fn lemma_owned_live(&self, id: NodeId, h: nat, x: NodeId)
        requires self.wf_node(id, h), self.owned(id, h).contains(x),
        ensures self.node_spec(x) is Some,
        decreases h,
    {
        if x != id {
            if let Some(Node::Internal(n)) = self.node_spec(id) {
                let k = self.lemma_children_owned_find(n.children@, (h - 1) as nat, x);
                self.lemma_child(id, h, n, k);
                self.lemma_owned_live(n.children@[k].id, (h - 1) as nat, x);
            }
        }
    }

    /// Which child owns a node of a run of children.
    proof fn lemma_children_owned_find(&self, cs: Seq<ChildSlot<A>>, h: nat, x: NodeId) -> (k: int)
        requires self.children_owned(cs, h).contains(x),
        ensures 0 <= k < cs.len(), self.owned(cs[k].id, h).contains(x),
        decreases cs.len(),
    {
        if self.owned(cs.last().id, h).contains(x) {
            cs.len() - 1
        } else {
            let k = self.lemma_children_owned_find(cs.drop_last(), h, x);
            assert(cs.drop_last()[k] == cs[k]);
            k
        }
    }

    /// Store `node` in a free slot.
    fn alloc(&mut self, node: Node<A>) -> (id: NodeId)
        requires old(self).arena_ok(),
        ensures
            final(self).arena_ok(),
            old(self).node_spec(id) is None,
            final(self).node_spec(id) == Some(node),
            forall|x: NodeId| x != id ==> #[trigger] final(self).node_spec(x) == old(self).node_spec(x),
            final(self).nodes@.len() >= old(self).nodes@.len(),
            final(self).root == old(self).root,
            final(self).total_slabs == old(self).total_slabs,
    {
        if let Some(id) = self.free.pop() {
            proof {
                let f = old(self).free@;
                assert(f[f.len() - 1] == id);
                assert forall|i: int| 0 <= i < self.free@.len() implies #[trigger] self.free@[i] != id by {
                    assert(self.free@[i] == f[i]);
                }
            }
            self.nodes[id as usize] = Some(node);
            proof {
                let f = old(self).free@;
                assert forall|i: int| 0 <= i < self.free@.len() implies
                    (#[trigger] self.free@[i] as int) < self.nodes@.len() && self.nodes@[self.free@[i] as int] is None by {
                    assert(self.free@[i] == f[i]);
                }
                assert forall|x: NodeId| x != id implies #[trigger] self.node_spec(x) == old(self).node_spec(x) by {}
            }
            id
        } else {
            if self.nodes.len() > u32::MAX as usize {
                out_of_node_ids();
            }
            let id = self.nodes.len() as NodeId;
            self.nodes.push(Some(node));
            proof {
                assert forall|i: int| 0 <= i < self.free@.len() implies
                    (#[trigger] self.free@[i] as int) < self.nodes@.len() && self.nodes@[self.free@[i] as int] is None by {}
                assert forall|x: NodeId| x != id implies #[trigger] self.node_spec(x) == old(self).node_spec(x) by {}
            }
            id
        }
    }

    /// Return a live node's slot to the free list.
    fn free(&mut self, id: NodeId)
        requires old(self).arena_ok(), old(self).node_spec(id) is Some,
        ensures
            final(self).arena_ok(),
            final(self).node_spec(id) is None,
            forall|x: NodeId| x != id ==> #[trigger] final(self).node_spec(x) == old(self).node_spec(x),
            final(self).nodes@.len() == old(self).nodes@.len(),
            final(self).root == old(self).root,
            final(self).total_slabs == old(self).total_slabs,
    {
        proof {
            assert forall|i: int| 0 <= i < self.free@.len() implies #[trigger] self.free@[i] != id by {}
        }
        self.nodes[id as usize] = None;
        self.free.push(id);
        proof {
            let f = old(self).free@;
            assert forall|i: int| 0 <= i < self.free@.len() implies
                (#[trigger] self.free@[i] as int) < self.nodes@.len() && self.nodes@[self.free@[i] as int] is None by {
                if i < f.len() { assert(self.free@[i] == f[i]); }
            }
            assert forall|x: NodeId| x != id implies #[trigger] self.node_spec(x) == old(self).node_spec(x) by {}
        }
    }

    // ── Slot lists ──────────────────────────────────────────────────────

    /// An internal node is well formed exactly when its children are good
    /// slots one level down that don't contain it.
    proof fn lemma_wf_internal(&self, id: NodeId, h: nat, n: Internal<A>)
        requires self.node_spec(id) == Some(Node::Internal(n)),
        ensures
            self.wf_node(id, h) == {
                &&& h > 0
                &&& n.children@.len() > 0
                &&& self.slots_ok(n.children@, (h - 1) as nat)
                &&& forall|i: int| 0 <= i < n.children@.len()
                    ==> !self.owned(#[trigger] n.children@[i].id, (h - 1) as nat).contains(id)
                &&& self.children_slabs(n.children@, (h - 1) as nat).len() <= usize::MAX
                &&& sum_len(self.children_slabs(n.children@, (h - 1) as nat)) <= usize::MAX
            },
            h > 0 ==> self.slabs(id, h) == self.children_slabs(n.children@, (h - 1) as nat),
            h > 0 ==> self.owned(id, h) == set![id] + self.children_owned(n.children@, (h - 1) as nat),
    {
        reveal(SlabBTree::wf_children);
        reveal(SlabBTree::slots_ok);
        if h > 0 {
            let cs = n.children@;
            let hc = (h - 1) as nat;
            if self.wf_node(id, h) {
                assert forall|i: int| 0 <= i < cs.len() implies {
                    let c = #[trigger] cs[i];
                    &&& self.wf_node(c.id, hc)
                    &&& c.slab_count == self.slabs(c.id, hc).len()
                    &&& c.slab_count > 0
                    &&& valid(self.slabs(c.id, hc)) ==> c.agg == fold(self.slabs(c.id, hc))
                    &&& c.agg.spec_len() == sum_len(self.slabs(c.id, hc))
                } by {
                    self.lemma_child(id, h, n, i);
                }
            }
        }
    }

    proof fn lemma_wf_leaf(&self, id: NodeId, h: nat, l: Leaf<A>)
        requires self.node_spec(id) == Some(Node::Leaf(l)),
        ensures
            self.wf_node(id, h) == (h == 0 && l.aggs@.len() <= usize::MAX && sum_len(l.aggs@) <= usize::MAX),
            self.slabs(id, h) == l.aggs@,
            self.owned(id, h) == set![id],
    {
        reveal(SlabBTree::wf_children);
    }

    /// A slot list means the same in any tree that agrees on its nodes.
    proof fn lemma_slots_frame(t1: &Self, t2: &Self, cs: Seq<ChildSlot<A>>, h: nat)
        requires
            forall|x: NodeId| t1.children_owned(cs, h).contains(x) ==> t1.node_spec(x) == t2.node_spec(x),
        ensures
            t1.slots_ok(cs, h) == t2.slots_ok(cs, h),
            t1.children_slabs(cs, h) == t2.children_slabs(cs, h),
            t1.children_owned(cs, h) == t2.children_owned(cs, h),
            forall|i: int| 0 <= i < cs.len() ==> t1.owned(#[trigger] cs[i].id, h) == t2.owned(cs[i].id, h),
    {
        reveal(SlabBTree::slots_ok);
        Self::lemma_frame_children(t1, t2, cs, h);
        if t1.slots_ok(cs, h) != t2.slots_ok(cs, h) {
            assert forall|i: int, j: int| 0 <= i < j < cs.len() implies
                t1.owned(cs[i].id, h) == t2.owned(cs[i].id, h) && t1.owned(cs[j].id, h) == t2.owned(cs[j].id, h) by {}
        }
    }

    /// A node in none of the children's subtrees is in none of them all.
    proof fn lemma_not_in_children(&self, cs: Seq<ChildSlot<A>>, h: nat, x: NodeId)
        requires forall|i: int| 0 <= i < cs.len() ==> !self.owned(#[trigger] cs[i].id, h).contains(x),
        ensures !self.children_owned(cs, h).contains(x),
    {
        if self.children_owned(cs, h).contains(x) {
            let k = self.lemma_children_owned_find(cs, h, x);
        }
    }

    /// Every node a good slot list owns is live.
    proof fn lemma_slots_live(&self, cs: Seq<ChildSlot<A>>, h: nat, x: NodeId)
        requires self.slots_ok(cs, h), self.children_owned(cs, h).contains(x),
        ensures self.node_spec(x) is Some,
    {
        reveal(SlabBTree::slots_ok);
        let k = self.lemma_children_owned_find(cs, h, x);
        assert(self.wf_node(cs[k].id, h));
        self.lemma_owned_live(cs[k].id, h, x);
    }

    proof fn lemma_children_slabs_one(&self, c: ChildSlot<A>, h: nat)
        ensures
            self.children_slabs(seq![c], h) == self.slabs(c.id, h),
            self.children_owned(seq![c], h) == self.owned(c.id, h),
    {
        reveal_with_fuel(SlabBTree::children_slabs, 2);
        reveal_with_fuel(SlabBTree::children_owned, 2);
        assert(seq![c].drop_last() =~= Seq::<ChildSlot<A>>::empty());
        assert(seq![c].last() == c);
        assert(Seq::<A>::empty() + self.slabs(c.id, h) =~= self.slabs(c.id, h));
        assert(Set::<NodeId>::empty() + self.owned(c.id, h) =~= self.owned(c.id, h));
    }

    proof fn lemma_children_owned_concat(&self, a: Seq<ChildSlot<A>>, b: Seq<ChildSlot<A>>, h: nat)
        ensures self.children_owned(a + b, h) == self.children_owned(a, h) + self.children_owned(b, h),
        decreases b.len(),
    {
        if b.len() == 0 {
            assert(a + b =~= a);
            assert(self.children_owned(a, h) + Set::<NodeId>::empty() =~= self.children_owned(a, h));
        } else {
            assert((a + b).drop_last() =~= a + b.drop_last());
            self.lemma_children_owned_concat(a, b.drop_last(), h);
            assert(self.children_owned(a + b, h) =~= self.children_owned(a, h)
                + (self.children_owned(b.drop_last(), h) + self.owned(b.last().id, h)));
        }
    }

    /// Joining two good slot lists that share no nodes.
    proof fn lemma_slots_concat(&self, a: Seq<ChildSlot<A>>, b: Seq<ChildSlot<A>>, h: nat)
        requires
            self.slots_ok(a, h), self.slots_ok(b, h),
            self.children_owned(a, h).disjoint(self.children_owned(b, h)),
        ensures
            self.slots_ok(a + b, h),
            self.children_slabs(a + b, h) == self.children_slabs(a, h) + self.children_slabs(b, h),
            self.children_owned(a + b, h) == self.children_owned(a, h) + self.children_owned(b, h),
    {
        reveal(SlabBTree::slots_ok);
        let ab = a + b;
        self.lemma_children_slabs_concat(a, b, h);
        self.lemma_children_owned_concat(a, b, h);
        assert forall|i: int| 0 <= i < ab.len() implies {
            let c = #[trigger] ab[i];
            &&& self.wf_node(c.id, h)
            &&& c.slab_count == self.slabs(c.id, h).len()
            &&& c.slab_count > 0
            &&& valid(self.slabs(c.id, h)) ==> c.agg == fold(self.slabs(c.id, h))
            &&& c.agg.spec_len() == sum_len(self.slabs(c.id, h))
        } by {
            if i < a.len() { assert(ab[i] == a[i]); } else { assert(ab[i] == b[i - a.len()]); }
        }
        assert forall|i: int, j: int| 0 <= i < j < ab.len() implies
            #[trigger] self.owned(ab[i].id, h).disjoint(#[trigger] self.owned(ab[j].id, h)) by {
            if j < a.len() {
                assert(ab[i] == a[i] && ab[j] == a[j]);
            } else if i >= a.len() {
                assert(ab[i] == b[i - a.len()] && ab[j] == b[j - a.len()]);
            } else {
                assert(ab[i] == a[i] && ab[j] == b[j - a.len()]);
                self.lemma_children_owned_contains(a, i, h);
                self.lemma_children_owned_contains(b, j - a.len(), h);
            }
        }
    }

    /// A run of a good slot list is good, and owns part of its nodes.
    proof fn lemma_slots_sub(&self, cs: Seq<ChildSlot<A>>, i: int, j: int, h: nat)
        requires self.slots_ok(cs, h), 0 <= i <= j <= cs.len(),
        ensures
            self.slots_ok(cs.subrange(i, j), h),
            self.children_owned(cs.subrange(i, j), h).subset_of(self.children_owned(cs, h)),
    {
        reveal(SlabBTree::slots_ok);
        let t = cs.subrange(i, j);
        assert forall|a: int| 0 <= a < t.len() implies {
            let c = #[trigger] t[a];
            &&& self.wf_node(c.id, h)
            &&& c.slab_count == self.slabs(c.id, h).len()
            &&& c.slab_count > 0
            &&& valid(self.slabs(c.id, h)) ==> c.agg == fold(self.slabs(c.id, h))
            &&& c.agg.spec_len() == sum_len(self.slabs(c.id, h))
        } by {
            assert(t[a] == cs[i + a]);
        }
        assert forall|a: int, b: int| 0 <= a < b < t.len() implies
            #[trigger] self.owned(t[a].id, h).disjoint(#[trigger] self.owned(t[b].id, h)) by {
            assert(t[a] == cs[i + a] && t[b] == cs[i + b]);
        }
        assert(cs =~= cs.subrange(0, i) + t + cs.subrange(j, cs.len() as int));
        self.lemma_children_owned_concat(cs.subrange(0, i) + t, cs.subrange(j, cs.len() as int), h);
        self.lemma_children_owned_concat(cs.subrange(0, i), t, h);
    }

    /// Two runs of a good slot list at different places share no nodes.
    proof fn lemma_slots_apart(&self, cs: Seq<ChildSlot<A>>, i: int, j: int, k: int, h: nat)
        requires self.slots_ok(cs, h), 0 <= i <= j <= k <= cs.len(),
        ensures self.children_owned(cs.subrange(i, j), h).disjoint(self.children_owned(cs.subrange(j, k), h)),
    {
        reveal(SlabBTree::slots_ok);
        let (a, b) = (cs.subrange(i, j), cs.subrange(j, k));
        assert forall|x: NodeId| self.children_owned(a, h).contains(x) implies !self.children_owned(b, h).contains(x) by {
            let p = self.lemma_children_owned_find(a, h, x);
            if self.children_owned(b, h).contains(x) {
                let q = self.lemma_children_owned_find(b, h, x);
                assert(a[p] == cs[i + p] && b[q] == cs[j + q]);
                assert(self.owned(cs[i + p].id, h).disjoint(self.owned(cs[j + q].id, h)));
            }
        }
    }

    proof fn lemma_slots_nil(&self, h: nat)
        ensures
            self.slots_ok(Seq::empty(), h),
            self.children_slabs(Seq::empty(), h) == Seq::<A>::empty(),
            self.children_owned(Seq::empty(), h) == Set::<NodeId>::empty(),
    {
        reveal(SlabBTree::slots_ok);
    }

    /// What a good slot list says about one of its slots.
    proof fn lemma_slot(&self, cs: Seq<ChildSlot<A>>, h: nat, i: int)
        requires self.slots_ok(cs, h), 0 <= i < cs.len(),
        ensures
            self.wf_node(cs[i].id, h),
            cs[i].slab_count == self.slabs(cs[i].id, h).len(),
            cs[i].slab_count > 0,
            valid(self.slabs(cs[i].id, h)) ==> cs[i].agg == fold(self.slabs(cs[i].id, h)),
            cs[i].agg.spec_len() == sum_len(self.slabs(cs[i].id, h)),
    {
        reveal(SlabBTree::slots_ok);
        let c = cs[i];
    }

    /// The slot of one well-formed, nonempty subtree.
    proof fn lemma_slot_one(&self, slot: ChildSlot<A>, h: nat)
        requires
            self.wf_node(slot.id, h),
            slot.slab_count == self.slabs(slot.id, h).len(),
            slot.slab_count > 0,
            valid(self.slabs(slot.id, h)) ==> slot.agg == fold(self.slabs(slot.id, h)),
            slot.agg.spec_len() == sum_len(self.slabs(slot.id, h)),
        ensures
            self.slots_ok(seq![slot], h),
            self.children_slabs(seq![slot], h) == self.slabs(slot.id, h),
            self.children_owned(seq![slot], h) == self.owned(slot.id, h),
    {
        reveal(SlabBTree::slots_ok);
        let s = self.slabs(slot.id, h);
        self.lemma_children_slabs_one(slot, h);
        assert forall|i: int| 0 <= i < 1 implies #[trigger] seq![slot][i] == slot by {}
    }

    /// A good slot list stays good, with the same slabs and nodes, while
    /// every live node outside `changed` is left alone and `changed`
    /// shares nothing with it.
    proof fn lemma_slots_keep(t0: &Self, t1: &Self, cs: Seq<ChildSlot<A>>, h: nat, changed: Set<NodeId>)
        requires
            t0.slots_ok(cs, h),
            t0.children_owned(cs, h).disjoint(changed),
            forall|x: NodeId| !changed.contains(x) && t0.node_spec(x) is Some
                ==> #[trigger] t1.node_spec(x) == t0.node_spec(x),
        ensures
            t1.slots_ok(cs, h),
            t1.children_slabs(cs, h) == t0.children_slabs(cs, h),
            t1.children_owned(cs, h) == t0.children_owned(cs, h),
    {
        assert forall|x: NodeId| t0.children_owned(cs, h).contains(x) implies
            t0.node_spec(x) == t1.node_spec(x) by {
            t0.lemma_slots_live(cs, h, x);
        }
        Self::lemma_slots_frame(t0, t1, cs, h);
    }

    /// Prepending a slot that shares no nodes with a good slot list.
    proof fn lemma_slots_prepend(&self, slot: ChildSlot<A>, cs: Seq<ChildSlot<A>>, h: nat)
        requires
            self.slots_ok(seq![slot], h), self.slots_ok(cs, h),
            self.owned(slot.id, h).disjoint(self.children_owned(cs, h)),
        ensures
            self.slots_ok(cs.insert(0, slot), h),
            self.children_slabs(cs.insert(0, slot), h) == self.slabs(slot.id, h) + self.children_slabs(cs, h),
            self.children_owned(cs.insert(0, slot), h) == self.owned(slot.id, h) + self.children_owned(cs, h),
    {
        self.lemma_children_slabs_one(slot, h);
        self.lemma_slots_concat(seq![slot], cs, h);
        assert(seq![slot] + cs =~= cs.insert(0, slot));
    }

    /// A subtree of height `h` has at least `h + 1` nodes.
    proof fn lemma_owned_len(&self, id: NodeId, h: nat)
        requires self.wf_node(id, h),
        ensures self.owned(id, h).len() >= h + 1,
        decreases h,
    {
        if let Some(Node::Internal(n)) = self.node_spec(id) {
            let hc = (h - 1) as nat;
            let c = n.children@[0].id;
            self.lemma_child(id, h, n, 0);
            self.lemma_owned_len(c, hc);
            self.lemma_children_owned_contains(n.children@, 0, hc);
            let sub = self.owned(c, hc).insert(id);
            assert(sub.subset_of(self.owned(id, h)));
            vstd::set_lib::lemma_len_subset(sub, self.owned(id, h));
        }
    }

    /// The node ids below `n`.
    spec fn ids_below(n: nat) -> Set<NodeId>
        decreases n,
    {
        if n == 0 { Set::empty() } else { Self::ids_below((n - 1) as nat).insert((n - 1) as NodeId) }
    }

    proof fn lemma_ids_below(n: nat)
        requires n <= u32::MAX + 1,
        ensures
            Self::ids_below(n).len() == n,
            forall|x: NodeId| (x as nat) < n ==> #[trigger] Self::ids_below(n).contains(x),
        decreases n,
    {
        if n > 0 {
            Self::lemma_ids_below((n - 1) as nat);
            assert(!Self::ids_below((n - 1) as nat).contains((n - 1) as NodeId)) by {
                Self::lemma_ids_below_only((n - 1) as nat);
            };
        }
    }

    proof fn lemma_ids_below_only(n: nat)
        requires n <= u32::MAX + 1,
        ensures forall|x: NodeId| #[trigger] Self::ids_below(n).contains(x) ==> (x as nat) < n,
        decreases n,
    {
        if n > 0 {
            Self::lemma_ids_below_only((n - 1) as nat);
            assert forall|x: NodeId| #[trigger] Self::ids_below(n).contains(x) implies (x as nat) < n by {
                if x != (n - 1) as NodeId { assert(Self::ids_below((n - 1) as nat).contains(x)); }
            }
        }
    }

    /// A well-formed tree's height fits a `u32`.
    proof fn lemma_height_bound(&self, id: NodeId, h: nat)
        requires self.wf_node(id, h), self.arena_ok(),
        ensures h <= u32::MAX,
    {
        self.lemma_owned_len(id, h);
        let n = self.nodes@.len();
        Self::lemma_ids_below(n);
        assert forall|x: NodeId| self.owned(id, h).contains(x) implies Self::ids_below(n).contains(x) by {
            self.lemma_owned_live(id, h, x);
        }
        vstd::set_lib::lemma_len_subset(self.owned(id, h), Self::ids_below(n));
    }

    /// A run of children split around two adjacent ones, `d` and `d + 1`.
    proof fn lemma_pair_split(cs: Seq<ChildSlot<A>>, d: int)
        requires 0 <= d, d + 1 < cs.len(),
        ensures
            cs == cs.subrange(0, d) + seq![cs[d]] + seq![cs[d + 1]] + cs.subrange(d + 2, cs.len() as int),
    {
        assert(cs =~= cs.subrange(0, d) + seq![cs[d]] + seq![cs[d + 1]] + cs.subrange(d + 2, cs.len() as int));
    }

    /// A node's slabs and nodes around two adjacent children `d` and
    /// `d + 1`.
    proof fn lemma_pair_slabs(&self, cs: Seq<ChildSlot<A>>, d: int, h: nat)
        requires 0 <= d, d + 1 < cs.len(),
        ensures ({
            let (pre, post) = (cs.subrange(0, d), cs.subrange(d + 2, cs.len() as int));
            let pair = self.slabs(cs[d].id, h) + self.slabs(cs[d + 1].id, h);
            &&& self.children_slabs(cs, h) == self.children_slabs(pre, h) + pair + self.children_slabs(post, h)
            &&& self.children_owned(cs, h) == self.children_owned(pre, h)
                + (self.owned(cs[d].id, h) + self.owned(cs[d + 1].id, h)) + self.children_owned(post, h)
        }),
    {
        let (pre, post) = (cs.subrange(0, d), cs.subrange(d + 2, cs.len() as int));
        let (dc, sc) = (cs[d], cs[d + 1]);
        Self::lemma_pair_split(cs, d);
        self.lemma_children_slabs_concat(pre + seq![dc] + seq![sc], post, h);
        self.lemma_children_slabs_concat(pre + seq![dc], seq![sc], h);
        self.lemma_children_slabs_concat(pre, seq![dc], h);
        self.lemma_children_owned_concat(pre + seq![dc] + seq![sc], post, h);
        self.lemma_children_owned_concat(pre + seq![dc], seq![sc], h);
        self.lemma_children_owned_concat(pre, seq![dc], h);
        self.lemma_children_slabs_one(dc, h);
        self.lemma_children_slabs_one(sc, h);
        let (a, b, c, e) = (self.children_slabs(pre, h), self.slabs(dc.id, h), self.slabs(sc.id, h), self.children_slabs(post, h));
        assert(a + b + c + e =~= a + (b + c) + e);
        let (oa, ob, oc, oe) = (self.children_owned(pre, h), self.owned(dc.id, h), self.owned(sc.id, h), self.children_owned(post, h));
        assert(oa + ob + oc + oe =~= oa + (ob + oc) + oe);
    }

    /// The two adjacent children's slabs are a valid run of the node's.
    proof fn lemma_pair_sum_len(&self, cs: Seq<ChildSlot<A>>, d: int, h: nat)
        requires 0 <= d, d + 1 < cs.len(), sum_len(self.children_slabs(cs, h)) <= usize::MAX,
        ensures sum_len(self.slabs(cs[d].id, h) + self.slabs(cs[d + 1].id, h)) <= usize::MAX,
    {
        self.lemma_pair_slabs(cs, d, h);
        let all = self.children_slabs(cs, h);
        let pre = self.children_slabs(cs.subrange(0, d), h);
        let pair = self.slabs(cs[d].id, h) + self.slabs(cs[d + 1].id, h);
        assert(all.subrange(pre.len() as int, (pre.len() + pair.len()) as int) =~= pair);
        lemma_sum_len_sub(all, pre.len() as int, (pre.len() + pair.len()) as int);
    }

    /// Replacing leaf slots `d` and `d + 1` by one leaf slot holding both
    /// (in a tree where only those two and `parent` changed).
    proof fn lemma_merged_slots(t0: &Self, t: &Self, cs: Seq<ChildSlot<A>>, d: int, slot: ChildSlot<A>, parent: NodeId)
        requires
            t0.slots_ok(cs, 0),
            0 <= d, d + 1 < cs.len(),
            slot.id == cs[d].id,
            !t0.children_owned(cs, 0).contains(parent),
            forall|x: NodeId| !set![cs[d + 1].id, slot.id, parent].contains(x) && t0.node_spec(x) is Some
                ==> #[trigger] t.node_spec(x) == t0.node_spec(x),
            t.wf_node(slot.id, 0),
            t.slabs(slot.id, 0) == t0.slabs(cs[d].id, 0) + t0.slabs(cs[d + 1].id, 0),
            slot.slab_count == t.slabs(slot.id, 0).len(),
            valid(t.slabs(slot.id, 0)) ==> slot.agg == fold(t.slabs(slot.id, 0)),
            slot.agg.spec_len() == sum_len(t.slabs(slot.id, 0)),
        ensures ({
            let n2 = cs.subrange(0, d) + seq![slot] + cs.subrange(d + 2, cs.len() as int);
            &&& t.slots_ok(n2, 0)
            &&& t.children_slabs(n2, 0) == t0.children_slabs(cs, 0)
            &&& t.children_owned(n2, 0).subset_of(t0.children_owned(cs, 0))
            &&& n2.len() > 0
        }),
    {
        let (pre, post) = (cs.subrange(0, d), cs.subrange(d + 2, cs.len() as int));
        let (dc, sc) = (cs[d], cs[d + 1]);
        let changed = set![sc.id, slot.id, parent];
        t0.lemma_slot(cs, 0, d);
        t0.lemma_slot(cs, 0, d + 1);
        assert(t0.owned(dc.id, 0) == set![dc.id]) by { if let Some(Node::Internal(_)) = t0.node_spec(dc.id) {} };
        assert(t0.owned(sc.id, 0) == set![sc.id]) by { if let Some(Node::Internal(_)) = t0.node_spec(sc.id) {} };
        t0.lemma_pair_slabs(cs, d, 0);
        t0.lemma_slots_sub(cs, 0, d, 0);
        t0.lemma_slots_sub(cs, d + 2, cs.len() as int, 0);
        Self::lemma_pair_disjoint(t0, cs, d);
        Self::lemma_slots_keep(t0, t, pre, 0, changed);
        Self::lemma_slots_keep(t0, t, post, 0, changed);
        t.lemma_slot_one(slot, 0);
        assert(t.owned(slot.id, 0) == set![slot.id]) by { if let Some(Node::Internal(_)) = t.node_spec(slot.id) {} };
        t.lemma_slots_concat(pre, seq![slot], 0);
        t.lemma_slots_concat(pre + seq![slot], post, 0);
    }

    /// The runs before and after two adjacent leaf children share no
    /// nodes with them (or with anything outside the node's own).
    proof fn lemma_pair_disjoint(t0: &Self, cs: Seq<ChildSlot<A>>, d: int)
        requires
            t0.slots_ok(cs, 0), 0 <= d, d + 1 < cs.len(),
            t0.owned(cs[d].id, 0) == set![cs[d].id],
            t0.owned(cs[d + 1].id, 0) == set![cs[d + 1].id],
        ensures
            !t0.children_owned(cs.subrange(0, d), 0).contains(cs[d].id),
            !t0.children_owned(cs.subrange(0, d), 0).contains(cs[d + 1].id),
            !t0.children_owned(cs.subrange(d + 2, cs.len() as int), 0).contains(cs[d].id),
            !t0.children_owned(cs.subrange(d + 2, cs.len() as int), 0).contains(cs[d + 1].id),
            t0.children_owned(cs.subrange(0, d), 0).disjoint(t0.children_owned(cs.subrange(d + 2, cs.len() as int), 0)),
    {
        t0.lemma_slots_apart(cs, 0, d, d + 2, 0);
        t0.lemma_slots_apart(cs, d, d + 2, cs.len() as int, 0);
        t0.lemma_slots_apart(cs, 0, d, cs.len() as int, 0);
        assert(cs.subrange(d, d + 2) =~= seq![cs[d], cs[d + 1]]);
        assert(seq![cs[d], cs[d + 1]] =~= seq![cs[d]] + seq![cs[d + 1]]);
        t0.lemma_children_owned_concat(seq![cs[d]], seq![cs[d + 1]], 0);
        t0.lemma_children_slabs_one(cs[d], 0);
        t0.lemma_children_slabs_one(cs[d + 1], 0);
        let rest = cs.subrange(d, cs.len() as int);
        assert(rest =~= cs.subrange(d, d + 2) + cs.subrange(d + 2, cs.len() as int));
        t0.lemma_children_owned_concat(cs.subrange(d, d + 2), cs.subrange(d + 2, cs.len() as int), 0);
    }

    proof fn lemma_counts_mono(cs: Seq<ChildSlot<A>>)
        ensures forall|i: int, j: int| 0 <= i <= j <= cs.len() ==> #[trigger] counts(cs, i) <= #[trigger] counts(cs, j),
    {
        assert forall|i: int, j: int| 0 <= i <= j <= cs.len() implies #[trigger] counts(cs, i) <= #[trigger] counts(cs, j) by {
            Self::lemma_counts_le(cs, i, j);
        }
    }

    proof fn lemma_counts_le(cs: Seq<ChildSlot<A>>, i: int, j: int)
        requires 0 <= i <= j,
        ensures counts(cs, i) <= counts(cs, j),
        decreases j - i,
    {
        if i < j { Self::lemma_counts_le(cs, i, j - 1); }
    }

    /// Slot counts add up to the slabs they stand for.
    proof fn lemma_counts(&self, cs: Seq<ChildSlot<A>>, h: nat, k: int)
        requires self.slots_ok(cs, h), 0 <= k <= cs.len(),
        ensures counts(cs, k) == self.children_slabs(cs.take(k), h).len(),
        decreases k,
    {
        if k > 0 {
            self.lemma_counts(cs, h, k - 1);
            self.lemma_slot(cs, h, k - 1);
            self.lemma_children_slabs_push(cs, k - 1, h);
        } else {
            assert(cs.take(0) =~= Seq::<ChildSlot<A>>::empty());
        }
    }

    /// The slots before and after child `k` share no nodes with it, and
    /// none of them with each other.
    proof fn lemma_child_apart(t: &Self, cs: Seq<ChildSlot<A>>, k: int, h: nat)
        requires t.slots_ok(cs, h), 0 <= k < cs.len(),
        ensures
            t.children_owned(cs.subrange(0, k), h).disjoint(t.owned(cs[k].id, h)),
            t.children_owned(cs.subrange(k + 1, cs.len() as int), h).disjoint(t.owned(cs[k].id, h)),
            t.children_owned(cs.subrange(0, k), h).disjoint(t.children_owned(cs.subrange(k + 1, cs.len() as int), h)),
            t.children_owned(cs.subrange(0, k), h).subset_of(t.children_owned(cs, h)),
            t.children_owned(cs.subrange(k + 1, cs.len() as int), h).subset_of(t.children_owned(cs, h)),
    {
        t.lemma_slots_apart(cs, 0, k, k + 1, h);
        t.lemma_slots_apart(cs, k, k + 1, cs.len() as int, h);
        t.lemma_slots_apart(cs, 0, k, cs.len() as int, h);
        t.lemma_slots_sub(cs, 0, k, h);
        t.lemma_slots_sub(cs, k + 1, cs.len() as int, h);
        assert(cs.subrange(k, k + 1) =~= seq![cs[k]]);
        t.lemma_children_slabs_one(cs[k], h);
        let rest = cs.subrange(k, cs.len() as int);
        assert(rest =~= cs.subrange(k, k + 1) + cs.subrange(k + 1, cs.len() as int));
        t.lemma_children_owned_concat(cs.subrange(k, k + 1), cs.subrange(k + 1, cs.len() as int), h);
    }

    /// The slots a child's splice returned stay good once its parent is
    /// rewritten (and, if none came back, the child freed).
    proof fn lemma_child_result_keep(t1: &Self, t3: &Self, rs: Seq<ChildSlot<A>>, h: nat, id: NodeId, child: NodeId, t0: &Self)
        requires
            t1.slots_ok(rs, h),
            t0.node_spec(id) is Some,
            !t0.owned(child, h).contains(id),
            forall|x: NodeId| #[trigger] t1.children_owned(rs, h).contains(x)
                ==> t0.owned(child, h).contains(x) || t0.node_spec(x) is None,
            forall|x: NodeId| x != id && x != child ==> #[trigger] t3.node_spec(x) == t1.node_spec(x),
            rs.len() > 0 ==> forall|x: NodeId| x != id ==> #[trigger] t3.node_spec(x) == t1.node_spec(x),
            t1.node_spec(id) == t0.node_spec(id),
        ensures
            t3.slots_ok(rs, h),
            t3.children_slabs(rs, h) == t1.children_slabs(rs, h),
            t3.children_owned(rs, h) == t1.children_owned(rs, h),
            !t3.children_owned(rs, h).contains(id),
    {
        if rs.len() == 0 {
            t1.lemma_slots_nil(h);
            t3.lemma_slots_nil(h);
            assert(rs =~= Seq::<ChildSlot<A>>::empty());
        } else {
            assert(!t1.children_owned(rs, h).contains(id));
            Self::lemma_slots_keep(t1, t3, rs, h, set![id]);
        }
    }

    /// The parent's new children: the slots before, the child's
    /// replacement, the slots after.
    proof fn lemma_child_join(t3: &Self, pre: Seq<ChildSlot<A>>, rs: Seq<ChildSlot<A>>, post: Seq<ChildSlot<A>>, h: nat, id: NodeId, t0: &Self, child: NodeId)
        requires
            t3.slots_ok(pre, h), t3.slots_ok(rs, h), t3.slots_ok(post, h),
            t3.children_owned(pre, h) == t0.children_owned(pre, h),
            t3.children_owned(post, h) == t0.children_owned(post, h),
            t0.children_owned(pre, h).disjoint(t0.children_owned(post, h)),
            t0.children_owned(pre, h).disjoint(t0.owned(child, h)),
            t0.children_owned(post, h).disjoint(t0.owned(child, h)),
            forall|x: NodeId| #[trigger] t0.children_owned(pre, h).contains(x) ==> t0.node_spec(x) is Some,
            forall|x: NodeId| #[trigger] t0.children_owned(post, h).contains(x) ==> t0.node_spec(x) is Some,
            forall|x: NodeId| #[trigger] t3.children_owned(rs, h).contains(x)
                ==> t0.owned(child, h).contains(x) || t0.node_spec(x) is None,
            !t3.children_owned(pre, h).contains(id),
            !t3.children_owned(post, h).contains(id),
            !t3.children_owned(rs, h).contains(id),
        ensures
            t3.slots_ok(pre + rs + post, h),
            t3.children_slabs(pre + rs + post, h)
                == t3.children_slabs(pre, h) + t3.children_slabs(rs, h) + t3.children_slabs(post, h),
            t3.children_owned(pre + rs + post, h)
                == t3.children_owned(pre, h) + t3.children_owned(rs, h) + t3.children_owned(post, h),
            !t3.children_owned(pre + rs + post, h).contains(id),
    {
        assert(t3.children_owned(pre, h).disjoint(t3.children_owned(rs, h))) by {
            assert forall|x: NodeId| t3.children_owned(pre, h).contains(x) implies !t3.children_owned(rs, h).contains(x) by {}
        };
        t3.lemma_slots_concat(pre, rs, h);
        assert(t3.children_owned(pre + rs, h).disjoint(t3.children_owned(post, h))) by {
            assert forall|x: NodeId| t3.children_owned(pre + rs, h).contains(x) implies !t3.children_owned(post, h).contains(x) by {}
        };
        t3.lemma_slots_concat(pre + rs, post, h);
    }

    /// Slot `k`'s place among a good slot list's slabs.
    proof fn lemma_slabs_at(&self, cs: Seq<ChildSlot<A>>, h: nat, k: int)
        requires self.slots_ok(cs, h), 0 <= k < cs.len(),
        ensures
            counts(cs, k + 1) == counts(cs, k) + cs[k].slab_count,
            counts(cs, k + 1) <= self.children_slabs(cs, h).len(),
            self.children_slabs(cs, h).subrange(0, counts(cs, k) as int) == self.children_slabs(cs.subrange(0, k), h),
            self.children_slabs(cs, h).subrange(counts(cs, k) as int, counts(cs, k + 1) as int) == self.slabs(cs[k].id, h),
            self.children_slabs(cs.subrange(0, k + 1), h) == self.children_slabs(cs.subrange(0, k), h) + self.slabs(cs[k].id, h),
    {
        self.lemma_counts(cs, h, k);
        self.lemma_counts(cs, h, k + 1);
        self.lemma_slot(cs, h, k);
        self.lemma_children_slabs_split(cs, k, h);
        self.lemma_children_slabs_push(cs, k, h);
        let all = self.children_slabs(cs, h);
        let a = self.children_slabs(cs.take(k), h);
        let b = self.slabs(cs[k].id, h);
        assert(all.subrange(0, a.len() as int) =~= a);
        assert(all.subrange(a.len() as int, (a.len() + b.len()) as int) =~= b);
    }

    /// What a good slot list's slabs past slot `k` are.
    proof fn lemma_slabs_tail(&self, cs: Seq<ChildSlot<A>>, h: nat, k: int)
        requires self.slots_ok(cs, h), 0 <= k <= cs.len(),
        ensures
            self.children_slabs(cs, h) == self.children_slabs(cs.subrange(0, k), h) + self.children_slabs(cs.subrange(k, cs.len() as int), h),
            self.children_slabs(cs.subrange(0, k), h).len() == counts(cs, k),
    {
        self.lemma_counts(cs, h, k);
        assert(cs =~= cs.subrange(0, k) + cs.subrange(k, cs.len() as int));
        self.lemma_children_slabs_concat(cs.subrange(0, k), cs.subrange(k, cs.len() as int), h);
    }

    /// In the middle of a multi-child splice, child `k` and the ones
    /// before it are as they were.
    proof fn lemma_untouched_child(t0: &Self, t: &Self, cs: Seq<ChildSlot<A>>, h: nat, k: int, id: NodeId)
        requires
            t0.slots_ok(cs, h), 0 <= k < cs.len(),
            !t0.children_owned(cs, h).contains(id),
            forall|x: NodeId| x != id && t0.node_spec(x) is Some
                && !t0.children_owned(cs.subrange(k + 1, cs.len() as int), h).contains(x)
                ==> #[trigger] t.node_spec(x) == t0.node_spec(x),
        ensures
            t.slabs(cs[k].id, h) == t0.slabs(cs[k].id, h),
            t.owned(cs[k].id, h) == t0.owned(cs[k].id, h),
            t.wf_node(cs[k].id, h),
            t.slots_ok(cs.subrange(0, k + 1), h),
            t.children_slabs(cs.subrange(0, k + 1), h) == t0.children_slabs(cs.subrange(0, k + 1), h),
            t.children_owned(cs.subrange(0, k + 1), h) == t0.children_owned(cs.subrange(0, k + 1), h),
    {
        let pre = cs.subrange(0, k + 1);
        let post = cs.subrange(k + 1, cs.len() as int);
        t0.lemma_slots_sub(cs, 0, k + 1, h);
        t0.lemma_slots_apart(cs, 0, k + 1, cs.len() as int, h);
        Self::lemma_slots_keep(t0, t, pre, h, t0.children_owned(post, h).insert(id));
        t0.lemma_children_owned_contains(pre, k, h);
        assert(pre[k] == cs[k]);
        assert forall|x: NodeId| t0.owned(cs[k].id, h).contains(x) implies t0.node_spec(x) == t.node_spec(x) by {
            t0.lemma_slots_live(pre, h, x);
        }
        Self::lemma_frame(t0, t, cs[k].id, h);
        t0.lemma_slot(cs, h, k);
    }

    /// One step of a multi-child splice: child `k` is done, so the
    /// children from `k` on stand for the result's tail from child `k`'s
    /// share.
    proof fn lemma_multi_step(
        o: &Self, t0: &Self, t: &Self, cs: Seq<ChildSlot<A>>, l0: Seq<ChildSlot<A>>, l2: Seq<ChildSlot<A>>,
        h: nat, k: int, id: NodeId, f: Seq<A>, qk: int, qk1: int, piece: Seq<A>,
    )
        requires
            o.slots_ok(cs, h), 0 <= k < cs.len(), k < l0.len(),
            0 <= qk <= qk1 <= f.len(),
            piece == f.subrange(qk, qk1),
            !o.children_owned(cs, h).contains(id),
            l0.subrange(0, k + 1) == cs.subrange(0, k + 1),
            t0.slots_ok(l0, h),
            t0.children_slabs(l0, h) == o.children_slabs(cs.subrange(0, k + 1), h) + f.subrange(qk1, f.len() as int),
            forall|x: NodeId| #[trigger] t0.children_owned(l0, h).contains(x)
                ==> o.children_owned(cs, h).contains(x) || o.node_spec(x) is None,
            forall|x: NodeId| x != id && o.node_spec(x) is Some
                && !o.children_owned(cs.subrange(k + 1, cs.len() as int), h).contains(x)
                ==> #[trigger] t0.node_spec(x) == o.node_spec(x),
            // what splicing child k did
            t.children_slabs(l2, h) == t0.children_slabs(l0.subrange(0, k), h) + piece
                + t0.children_slabs(l0.subrange(k + 1, l0.len() as int), h),
            l2.subrange(0, k) == l0.subrange(0, k), l2.len() >= k,
            forall|x: NodeId| #[trigger] t.children_owned(l2, h).contains(x)
                ==> t0.children_owned(l0, h).contains(x) || t0.node_spec(x) is None,
            forall|x: NodeId| x != id && t0.node_spec(x) is Some && !t0.owned(l0[k].id, h).contains(x)
                ==> #[trigger] t.node_spec(x) == t0.node_spec(x),
            t0.node_spec(id) is Some,
        ensures
            l2.subrange(0, k) == cs.subrange(0, k),
            t.children_slabs(l2, h) == o.children_slabs(cs.subrange(0, k), h) + f.subrange(qk, f.len() as int),
            forall|x: NodeId| #[trigger] t.children_owned(l2, h).contains(x)
                ==> o.children_owned(cs, h).contains(x) || o.node_spec(x) is None,
            forall|x: NodeId| x != id && o.node_spec(x) is Some
                && !o.children_owned(cs.subrange(k, cs.len() as int), h).contains(x)
                ==> #[trigger] t.node_spec(x) == o.node_spec(x),
    {
        assert(l0[k] == cs[k]) by { assert(l0.subrange(0, k + 1)[k] == cs.subrange(0, k + 1)[k]); }
        assert(l2.subrange(0, k) =~= cs.subrange(0, k)) by {
            assert(l0.subrange(0, k) =~= l0.subrange(0, k + 1).subrange(0, k));
            assert(cs.subrange(0, k) =~= cs.subrange(0, k + 1).subrange(0, k));
        };
        // child k and those before it were as in `o` at t0
        Self::lemma_untouched_child(o, t0, cs, h, k, id);
        o.lemma_slabs_at(cs, h, k);
        o.lemma_slots_sub(cs, 0, k, h);
        assert(l0.subrange(0, k) == cs.subrange(0, k));
        o.lemma_slots_apart(cs, 0, k, cs.len() as int, h);
        assert(cs.subrange(k, cs.len() as int) =~= cs.subrange(k, k + 1) + cs.subrange(k + 1, cs.len() as int));
        o.lemma_children_owned_concat(cs.subrange(k, k + 1), cs.subrange(k + 1, cs.len() as int), h);
        o.lemma_slots_sub(cs, 0, k, h);
        Self::lemma_slots_keep(o, t0, cs.subrange(0, k), h,
            o.children_owned(cs.subrange(k + 1, cs.len() as int), h).insert(id));
        // so what follows child k in l0 stands for the tail from k + 1's share
        t0.lemma_children_slabs_split(l0, k, h);
        assert(l0.take(k) == l0.subrange(0, k) && l0.skip(k + 1) == l0.subrange(k + 1, l0.len() as int));
        let a = o.children_slabs(cs.subrange(0, k), h);
        let b = o.slabs(cs[k].id, h);
        let rest = t0.children_slabs(l0.subrange(k + 1, l0.len() as int), h);
        let tail = f.subrange(qk1, f.len() as int);
        assert(a + b + rest == a + b + tail);
        assert(rest =~= (a + b + rest).subrange((a.len() + b.len()) as int, (a + b + rest).len() as int));
        assert(tail =~= (a + b + tail).subrange((a.len() + b.len()) as int, (a + b + tail).len() as int));
        assert(f.subrange(qk, f.len() as int) =~= piece + tail);
        // nodes: whatever the step introduced was empty in t0, so in o
        assert forall|x: NodeId| #[trigger] t.children_owned(l2, h).contains(x)
            implies o.children_owned(cs, h).contains(x) || o.node_spec(x) is None by {
            if !t0.children_owned(l0, h).contains(x) && o.node_spec(x) is Some && !o.children_owned(cs, h).contains(x) {
                o.lemma_slots_sub(cs, k + 1, cs.len() as int, h);
                assert(t0.node_spec(x) == o.node_spec(x));
            }
        }
        assert forall|x: NodeId| x != id && o.node_spec(x) is Some
            && !o.children_owned(cs.subrange(k, cs.len() as int), h).contains(x)
            implies #[trigger] t.node_spec(x) == o.node_spec(x) by {
            assert(cs.subrange(k, cs.len() as int) =~= cs.subrange(k, k + 1) + cs.subrange(k + 1, cs.len() as int));
            o.lemma_children_owned_concat(cs.subrange(k, k + 1), cs.subrange(k + 1, cs.len() as int), h);
            o.lemma_children_slabs_one(cs[k], h);
            assert(cs.subrange(k, k + 1) =~= seq![cs[k]]);
            assert(t0.node_spec(x) == o.node_spec(x));
        }
    }

    fn node(&self, id: NodeId) -> (r: &Node<A>)
        requires self.node_spec(id) is Some,
        ensures Some(*r) == self.node_spec(id),
    {
        self.nodes[id as usize].as_ref().expect("freed node id")
    }

    pub(crate) fn new() -> (t: Self)
        ensures t.wf(), t@ == Seq::<A>::empty(),
    {
        let mut nodes = Vec::with_capacity(8);
        nodes.push(Some(Node::Leaf(Leaf { aggs: Vec::new() })));
        let t = Self {
            nodes,
            free: Vec::new(),
            root: 0,
            total_slabs: 0,
            _phantom: PhantomData,
        };
        proof {
            let e = Seq::<A>::empty();
            assert(t.slabs(t.root, 0) =~= e);
            assert forall|i: int, j: int, k: int| 0 <= i <= j <= k <= e.len()
                implies #[trigger] A::merge_ok(fold(e.subrange(i, j)), fold(e.subrange(j, k))) by {
                assert(e.subrange(i, j) =~= e);
                assert(e.subrange(j, k) =~= e);
                A::lemma_empty(A::spec_empty());
            }
        }
        assert(t.wf_node(t.root, 0));
        assert(t@ =~= Seq::<A>::empty());
        assert(t.arena_ok());
        t
    }

    pub(crate) fn len(&self) -> (n: usize)
        requires self.wf(),
        ensures n == self@.len(),
    {
        self.total_slabs
    }

    /// Find the slab index containing the `pos`-th item and the
    /// item-count of items strictly before that slab.  Returns `None`
    /// if `pos >= total_items`.
    ///
    /// Out-of-range `pos` is detected during the descent (no child
    /// covers the remainder), so no upfront root-aggregate merge is
    /// needed — this runs on every positional lookup.
    pub(crate) fn find_by_prefix(&self, pos: usize) -> (r: Option<(usize, usize)>)
        requires self.wf(),
        ensures
            match r {
                Some((i, before)) => i < self@.len()
                    && before == sum_len(self@.take(i as int))
                    && before <= pos < before + self@[i as int].spec_len(),
                None => pos >= sum_len(self@),
            },
    {
        // the items left to skip within `node`'s subtree
        let ghost mut h = self.height();
        // the slabs left and right of `node`'s subtree
        let ghost mut left = Seq::<A>::empty();
        let ghost mut right = Seq::<A>::empty();
        let mut node = self.root;
        let mut rest = pos;
        let mut slab_idx_base: usize = 0;
        let mut items_before: usize = 0;
        proof {
            assert(self.wf_node(self.root, h));
            assert(self@ =~= left + self.slabs(node, h) + right);
        }
        loop
            invariant
                self.wf(),
                self.wf_node(node, h),
                self@ == left + self.slabs(node, h) + right,
                slab_idx_base == left.len(),
                items_before == sum_len(left),
                items_before + rest == pos,
                rest < sum_len(self.slabs(node, h)) || (left.len() == 0 && right.len() == 0),
            decreases h,
        {
            proof {
                lemma_sum_len_concat(left, self.slabs(node, h));
                lemma_sum_len_concat(left + self.slabs(node, h), right);
            }
            match self.node(node) {
                Node::Leaf(l) => {
                    let ghost aggs = l.aggs@;
                    assert(self.slabs(node, h) == aggs);
                    let mut i: usize = 0;
                    while i < l.aggs.len()
                        invariant
                            self.wf(),
                            i <= aggs.len(), aggs == l.aggs@,
                            self@ == left + aggs + right,
                            slab_idx_base == left.len(),
                            items_before == sum_len(left) + sum_len(aggs.take(i as int)),
                            items_before + rest == pos,
                            rest + sum_len(aggs.take(i as int)) < sum_len(aggs)
                                || (left.len() == 0 && right.len() == 0),
                        decreases aggs.len() - i,
                    {
                        let a = &l.aggs[i];
                        proof {
                            assert(aggs.take(i + 1).drop_last() =~= aggs.take(i as int));
                            assert(self@.take(left.len() + i) =~= left + aggs.take(i as int));
                            assert(self@[left.len() + i] == aggs[i as int]);
                            lemma_sum_len_concat(left, aggs.take(i as int));
                        }
                        if rest < a.len() {
                            return Some((slab_idx_base + i, items_before));
                        }
                        rest -= a.len();
                        items_before += a.len();
                        i += 1;
                    }
                    proof {
                        assert(aggs.take(i as int) =~= aggs);
                        assert(self@ =~= aggs);
                    }
                    return None;
                }
                Node::Internal(n) => {
                    let ghost cs = n.children@;
                    let ghost hc = (h - 1) as nat;
                    let ghost h0 = h;
                    proof {
                        if cs.len() > 0 {
                            self.lemma_child(node, h, *n, 0);
                        } else {
                            assert(self.wf_node(node, h));
                        }
                    }
                    assert(self.slabs(node, h) == self.children_slabs(cs, hc));
                    let mut k: usize = 0;
                    loop
                        invariant_except_break
                            k <= cs.len(), h == h0,
                            self.wf_node(node, h),
                            self.node_spec(node) == Some(Node::Internal(*n)),
                            self@ == left + self.children_slabs(cs, hc) + right,
                            slab_idx_base == left.len() + self.children_slabs(cs.take(k as int), hc).len(),
                            items_before == sum_len(left) + sum_len(self.children_slabs(cs.take(k as int), hc)),
                            items_before + rest == pos,
                            rest + sum_len(self.children_slabs(cs.take(k as int), hc))
                                < sum_len(self.children_slabs(cs, hc))
                                || (left.len() == 0 && right.len() == 0),
                        invariant
                            self.wf(), cs == n.children@, h0 > 0, hc == h0 - 1,
                        ensures
                            h < h0,
                            self.wf_node(node, h),
                            self@ == left + self.slabs(node, h) + right,
                            slab_idx_base == left.len(),
                            items_before == sum_len(left),
                            items_before + rest == pos,
                            rest < sum_len(self.slabs(node, h)),
                        decreases cs.len() - k,
                    {
                        if k == n.children.len() {
                            proof {
                                assert(cs.take(k as int) =~= cs);
                                assert(self@ =~= self.children_slabs(cs, hc));
                            }
                            return None;
                        }
                        let c = &n.children[k];
                        let ghost before = self.children_slabs(cs.take(k as int), hc);
                        let ghost mid = self.slabs(c.id, hc);
                        let ghost after = self.children_slabs(cs.skip(k + 1), hc);
                        proof {
                            assert(c == cs[k as int]);
                            self.lemma_child(node, h, *n, k as int);
                            self.lemma_children_slabs_split(cs, k as int, hc);
                            self.lemma_children_slabs_push(cs, k as int, hc);
                            lemma_sum_len_concat(before, mid);
                            lemma_sum_len_concat(before + mid, after);
                        }
                        if rest < c.agg.len() {
                            proof {
                                lemma_sum_len_concat(left, before);
                                left = left + before;
                                right = after + right;
                                h = hc;
                                assert(self@ =~= left + self.slabs(c.id, h) + right);
                            }
                            node = c.id;
                            break;
                        }
                        rest -= c.agg.len();
                        items_before += c.agg.len();
                        slab_idx_base += c.slab_count;
                        k += 1;
                    }
                }
            }
        }
    }

    /// Number of items in slabs `0..slab_idx`.  O(log n) — descends by
    /// slab count, accumulating the item lengths of skipped siblings.
    /// `slab_idx` may equal the total slab count (returns total items).
    pub(crate) fn items_before_slab(&self, slab_idx: usize) -> (r: usize)
        requires self.wf(), slab_idx <= self@.len(),
        ensures r == sum_len(self@.take(slab_idx as int)),
    {
        let ghost mut h = self.height();
        // the slabs left and right of `node`'s subtree
        let ghost mut left = Seq::<A>::empty();
        let ghost mut right = Seq::<A>::empty();
        let mut remaining = slab_idx;
        let mut items = 0;
        let mut node = self.root;
        proof {
            assert(self.wf_node(self.root, h));
            assert(self@ =~= left + self.slabs(node, h) + right);
        }
        loop
            invariant
                self.wf(),
                self.wf_node(node, h),
                self@ == left + self.slabs(node, h) + right,
                left.len() + remaining == slab_idx, slab_idx <= self@.len(),
                items == sum_len(left),
                remaining < self.slabs(node, h).len() || right.len() == 0,
            decreases h,
        {
            proof {
                lemma_sum_len_concat(left, self.slabs(node, h));
                lemma_sum_len_concat(left + self.slabs(node, h), right);
            }
            match self.node(node) {
                Node::Leaf(l) => {
                    let ghost aggs = l.aggs@;
                    assert(self.slabs(node, h) == aggs);
                    let mut i: usize = 0;
                    while i < l.aggs.len()
                        invariant
                            self.wf(),
                            i <= aggs.len(), aggs == l.aggs@,
                            self@ == left + aggs + right,
                            left.len() + i + remaining == slab_idx, slab_idx <= self@.len(),
                            items == sum_len(left) + sum_len(aggs.take(i as int)),
                            i + remaining < aggs.len() || right.len() == 0,
                        decreases aggs.len() - i,
                    {
                        proof {
                            assert(aggs.take(i + 1).drop_last() =~= aggs.take(i as int));
                            lemma_sum_len_concat(left, aggs.take(i as int));
                        }
                        if remaining == 0 {
                            proof { assert(self@.take(slab_idx as int) =~= left + aggs.take(i as int)); }
                            return items;
                        }
                        proof {
                            assert(self@.take(left.len() + i + 1) =~= left + aggs.take(i + 1));
                            lemma_sum_len_concat(left, aggs.take(i + 1));
                            assert(self@ =~= self@.take(left.len() + i + 1) + self@.skip(left.len() + i + 1));
                            lemma_sum_len_concat(self@.take(left.len() + i + 1), self@.skip(left.len() + i + 1));
                        }
                        remaining -= 1;
                        items += l.aggs[i].len();
                        i += 1;
                    }
                    proof {
                        assert(aggs.take(i as int) =~= aggs);
                        assert(self@.take(slab_idx as int) =~= left + aggs);
                        lemma_sum_len_concat(left, aggs);
                    }
                    return items;
                }
                Node::Internal(n) => {
                    let ghost cs = n.children@;
                    let ghost hc = (h - 1) as nat;
                    let ghost h0 = h;
                    proof {
                        if cs.len() > 0 {
                            self.lemma_child(node, h, *n, 0);
                        } else {
                            assert(self.wf_node(node, h));
                        }
                    }
                    assert(self.slabs(node, h) == self.children_slabs(cs, hc));
                    let mut k: usize = 0;
                    loop
                        invariant_except_break
                            k <= cs.len(), h == h0,
                            self.wf_node(node, h),
                            self.node_spec(node) == Some(Node::Internal(*n)),
                            self@ == left + self.children_slabs(cs, hc) + right,
                            left.len() + self.children_slabs(cs.take(k as int), hc).len() + remaining
                                == slab_idx,
                            slab_idx <= self@.len(),
                            items == sum_len(left) + sum_len(self.children_slabs(cs.take(k as int), hc)),
                            self.children_slabs(cs.take(k as int), hc).len() + remaining
                                < self.children_slabs(cs, hc).len() || right.len() == 0,
                        invariant
                            self.wf(), cs == n.children@, h0 > 0, hc == h0 - 1,
                        ensures
                            h < h0,
                            self.wf_node(node, h),
                            self@ == left + self.slabs(node, h) + right,
                            left.len() + remaining == slab_idx,
                            items == sum_len(left),
                            remaining < self.slabs(node, h).len(),
                        decreases cs.len() - k,
                    {
                        if k == n.children.len() {
                            proof {
                                assert(cs.take(k as int) =~= cs);
                                assert(self@.take(slab_idx as int) =~= left + self.children_slabs(cs, hc));
                                lemma_sum_len_concat(left, self.children_slabs(cs, hc));
                            }
                            return items;
                        }
                        let c = &n.children[k];
                        let ghost before = self.children_slabs(cs.take(k as int), hc);
                        let ghost mid = self.slabs(c.id, hc);
                        let ghost after = self.children_slabs(cs.skip(k + 1), hc);
                        proof {
                            assert(c == cs[k as int]);
                            self.lemma_child(node, h, *n, k as int);
                            self.lemma_children_slabs_split(cs, k as int, hc);
                            self.lemma_children_slabs_push(cs, k as int, hc);
                            lemma_sum_len_concat(before, mid);
                            lemma_sum_len_concat(left, before);
                            lemma_sum_len_concat(left, before + mid);
                            assert(self@ =~= (left + (before + mid)) + (after + right));
                            lemma_sum_len_concat(left + (before + mid), after + right);
                        }
                        if remaining < c.slab_count {
                            proof {
                                left = left + before;
                                right = after + right;
                                h = hc;
                                assert(self@ =~= left + self.slabs(c.id, h) + right);
                            }
                            node = c.id;
                            break;
                        }
                        remaining -= c.slab_count;
                        items += c.agg.len();
                        k += 1;
                    }
                }
            }
        }
    }
}

// ── Value-range search (SlabAgg-specific) ───────────────────────────────────

impl SlabAgg {
    /// Some running sum in this slab, starting from `prefix_before`, lies
    /// in `lo..=hi`.
    fn overlaps(&self, lo: i64, hi: i64, prefix_before: i64) -> (r: bool)
        requires
            self.len > 0 ==> in_i64(prefix_before + self.min_offset) && in_i64(prefix_before + self.max_offset),
        ensures
            r == (self.len > 0 && prefix_before + self.max_offset >= lo && prefix_before + self.min_offset <= hi),
    {
        self.len > 0
            && prefix_before + self.max_offset >= lo
            && prefix_before + self.min_offset <= hi
    }
}

/// Slab `i` of `s` may hold a value in `lo..=hi`: the range of running
/// sums its min/max offsets give meets it.
pub open spec fn may_hold(s: Seq<SlabAgg>, lo: int, hi: int, i: int) -> bool {
    s[i].len > 0 && pre_at(s, i) + s[i].max_offset >= lo && pre_at(s, i) + s[i].min_offset <= hi
}

/// A slab's range of running sums lies within its run's, shifted by the
/// total of the slabs before it in the run.
proof fn lemma_agg_covers(u: Seq<SlabAgg>, k: int)
    requires
        valid(u), 0 <= k < u.len(), u[k].len > 0,
        forall|i: int| 0 <= i < u.len() ==> #[trigger] u[i].prefix_ok(),
    ensures
        fold(u).len > 0,
        fold(u).min_offset <= fold(u.take(k)).total + u[k].min_offset,
        fold(u.take(k)).total + u[k].max_offset <= fold(u).max_offset,
    decreases u.len(),
{
    let n = u.len() as int;
    let v = u.drop_last();
    assert(u.subrange(0, n - 1) =~= v);
    lemma_fold_one(u, n - 1);
    assert(SlabAgg::merge_ok(fold(u.subrange(0, n - 1)), fold(u.subrange(n - 1, n))));
    if k < n - 1 {
        lemma_valid_subrange(u, 0, n - 1);
        assert(v.take(k) =~= u.take(k));
        lemma_agg_covers(v, k);
    } else {
        assert(u.take(k) =~= v);
        lemma_fold_prefix_ok(u, 0, n - 1);
    }
}

/// A run of slabs whose running sums miss `lo..=hi` holds no slab that
/// may hold a value there.
proof fn lemma_prune(s: Seq<SlabAgg>, lo: int, hi: int, a: int, b: int, k: int)
    requires
        valid(s), 0 <= a <= k < b <= s.len(), may_hold(s, lo, hi, k),
        forall|i: int| 0 <= i < s.len() ==> #[trigger] s[i].prefix_ok(),
    ensures
        fold(s.subrange(a, b)).len > 0,
        pre_at(s, a) + fold(s.subrange(a, b)).max_offset >= lo,
        pre_at(s, a) + fold(s.subrange(a, b)).min_offset <= hi,
{
    let u = s.subrange(a, b);
    lemma_valid_subrange(s, a, b);
    assert forall|i: int| 0 <= i < u.len() implies #[trigger] u[i].prefix_ok() by {
        assert(u[i] == s[a + i]);
    }
    lemma_agg_covers(u, k - a);
    assert(u.take(k - a) =~= s.subrange(a, k));
    if a < k {
        lemma_fold_prefix(s, a, k);
    } else {
        assert(s.subrange(a, k) =~= Seq::<SlabAgg>::empty());
    }
}

/// The running sums of a run fit an `i64`.
proof fn lemma_agg_in_range(s: Seq<SlabAgg>, a: int, b: int)
    requires
        valid(s), 0 <= a <= b <= s.len(),
        forall|i: int| 0 <= i < s.len() ==> #[trigger] s[i].prefix_ok(),
    ensures
        fold(s.subrange(a, b)).len > 0 ==> in_i64(pre_at(s, a) + fold(s.subrange(a, b)).min_offset)
            && in_i64(pre_at(s, a) + fold(s.subrange(a, b)).max_offset),
{
    assert(SlabAgg::merge_ok(fold(s.subrange(0, a)), fold(s.subrange(a, b))));
    lemma_fold_prefix_ok(s, 0, a);
}

/// The prefix and item count up to the end of a nonempty run, from
/// those at its start.
proof fn lemma_run_totals(s: Seq<SlabAgg>, a: int, b: int)
    requires
        valid(s), 0 <= a < b <= s.len(), sum_len(s) <= usize::MAX,
        forall|i: int| 0 <= i < s.len() ==> #[trigger] s[i].prefix_ok(),
    ensures
        in_i64(pre_at(s, a) + fold(s.subrange(a, b)).total),
        pre_at(s, b) == pre_at(s, a) + fold(s.subrange(a, b)).total,
        sum_len(s.subrange(0, b)) == sum_len(s.subrange(0, a)) + fold(s.subrange(a, b)).len,
        sum_len(s.subrange(0, b)) <= usize::MAX,
{
    lemma_fold_prefix(s, a, b);
    lemma_fold_len(s, a, b);
    assert(s.subrange(0, b) =~= s.subrange(0, a) + s.subrange(a, b));
    lemma_sum_len_concat(s.subrange(0, a), s.subrange(a, b));
    assert(s =~= s.subrange(0, b) + s.subrange(b, s.len() as int));
    lemma_sum_len_concat(s.subrange(0, b), s.subrange(b, s.len() as int));
}

/// `s[start..end] == x + y + z` places `y` right after `x`.
proof fn lemma_sub_part<T>(s: Seq<T>, start: int, end: int, x: Seq<T>, y: Seq<T>, z: Seq<T>)
    requires 0 <= start <= end <= s.len(), s.subrange(start, end) == x + y + z,
    ensures
        start + x.len() + y.len() <= end,
        s.subrange(start + x.len(), start + x.len() + y.len()) == y,
{
    assert((x + y + z).len() == end - start);
    assert(s.subrange(start + x.len(), start + x.len() + y.len()) =~= y) by {
        assert forall|i: int| 0 <= i < y.len() implies
            s.subrange(start + x.len(), start + x.len() + y.len())[i] == y[i] by {
            assert(s.subrange(start, end)[x.len() + i] == (x + y + z)[x.len() + i]);
            assert((x + y + z)[x.len() + i] == y[i]);
            assert(s.subrange(start, end)[x.len() + i] == s[start + x.len() + i]);
        }
    }
}

/// One level of a value search's descent: a node, the next child (or
/// slab, in a leaf) to look at, and the running sum, slab index and item
/// count where that child starts — for a leaf, `slab_base` stays at the
/// leaf's first slab.
#[derive(Copy, Clone)]
#[verifier::allow(autoderive_clone_without_spec)]
struct Frame {
    node: NodeId,
    next_child: usize,
    prefix_before: i64,
    slab_base: usize,
    items_before: usize,
}

/// The slab a frame looks at next.
spec fn frame_at(t: SlabBTree<SlabAgg>, f: Frame) -> int {
    match t.node_spec(f.node) {
        Some(Node::Leaf(_)) => f.slab_base + f.next_child,
        _ => f.slab_base as int,
    }
}

/// Frame `f` has looked at all its children.
spec fn frame_done(t: SlabBTree<SlabAgg>, f: Frame) -> bool {
    match t.node_spec(f.node) {
        Some(Node::Leaf(l)) => f.next_child >= l.aggs.len(),
        Some(Node::Internal(n)) => f.next_child >= n.children.len(),
        None => true,
    }
}

/// Frame `f` is for a node of height `h` whose slabs end at slab `end`,
/// with right running totals.
#[verifier::opaque]
spec fn frame_ok(t: SlabBTree<SlabAgg>, f: Frame, end: int, h: int) -> bool {
    let s = t.slabs(f.node, h as nat);
    let start = end - s.len();
    &&& 0 <= h && 0 <= start && end <= t@.len()
    &&& t.wf_node(f.node, h as nat)
    &&& t@.subrange(start, end) == s
    &&& match t.node_spec(f.node) {
        Some(Node::Leaf(l)) => f.slab_base == start && f.next_child <= l.aggs.len(),
        Some(Node::Internal(n)) => f.next_child <= n.children.len() && f.slab_base == start
            + t.children_slabs(n.children@.take(f.next_child as int), (h - 1) as nat).len(),
        None => false,
    }
    &&& f.prefix_before == pre_at(t@, frame_at(t, f))
    &&& f.items_before == sum_len(t@.subrange(0, frame_at(t, f)))
}

/// Where frame `j` of a stack ends: where its parent goes on, or the
/// end of the tree.
spec fn frame_end(t: SlabBTree<SlabAgg>, st: Seq<Frame>, j: int) -> int {
    if j == 0 { t@.len() as int } else { st[j - 1].slab_base as int }
}

/// Every frame of the stack is right, frame `j` for a node `j` levels
/// below the root.
#[verifier::opaque]
spec fn frames_ok(t: SlabBTree<SlabAgg>, st: Seq<Frame>) -> bool {
    forall|j: int| 0 <= j < st.len() ==> frame_ok(t, #[trigger] st[j], frame_end(t, st, j), t.height() - j)
}

/// The slab a search with stack `st` looks at next.
spec fn search_pos(t: SlabBTree<SlabAgg>, st: Seq<Frame>) -> int {
    if st.len() == 0 { t@.len() as int } else { frame_at(t, st.last()) }
}

/// The number of finished frames on top of the stack.
spec fn done_top(t: SlabBTree<SlabAgg>, st: Seq<Frame>) -> nat
    decreases st.len(),
{
    if st.len() > 0 && frame_done(t, st.last()) { 1 + done_top(t, st.drop_last()) } else { 0 }
}

proof fn lemma_frame_basics(t: SlabBTree<SlabAgg>, f: Frame, end: int, h: int)
    requires t.wf(), frame_ok(t, f, end, h),
    ensures
        0 <= h, 0 <= frame_at(t, f) <= end <= t@.len(),
        t.wf_node(f.node, h as nat),
        h > 0 ==> frame_at(t, f) == f.slab_base,
        frame_done(t, f) ==> frame_at(t, f) == end,
        f.prefix_before == pre_at(t@, frame_at(t, f)),
        f.items_before == sum_len(t@.subrange(0, frame_at(t, f))),
{
    reveal(frame_ok);
    match t.node_spec(f.node) {
        Some(Node::Leaf(l)) => {}
        Some(Node::Internal(n)) => {
            let cs = n.children@;
            let k = f.next_child as int;
            t.lemma_child(f.node, h as nat, n, 0);
            assert(cs =~= cs.take(k) + cs.skip(k));
            t.lemma_children_slabs_concat(cs.take(k), cs.skip(k), (h - 1) as nat);
        }
        None => {}
    }
}

proof fn lemma_frames_top(t: SlabBTree<SlabAgg>, st: Seq<Frame>)
    requires frames_ok(t, st), 0 < st.len(),
    ensures frame_ok(t, st.last(), frame_end(t, st, st.len() - 1), t.height() - (st.len() - 1)),
{
    reveal(frames_ok);
    assert(st.last() == st[st.len() - 1]);
}

proof fn lemma_frames_pop(t: SlabBTree<SlabAgg>, st: Seq<Frame>)
    requires t.wf(), frames_ok(t, st), 0 < st.len(), frame_done(t, st.last()),
    ensures
        frames_ok(t, st.drop_last()),
        search_pos(t, st.drop_last()) == frame_at(t, st.last()),
{
    reveal(frames_ok);
    let n = st.len() as int;
    let u = st.drop_last();
    let hh = t.height() as int;
    assert forall|j: int| 0 <= j < u.len() implies
        frame_ok(t, #[trigger] u[j], frame_end(t, u, j), t.height() - j) by {
        assert(u[j] == st[j]);
        assert(frame_end(t, u, j) == frame_end(t, st, j));
    }
    assert(st.last() == st[n - 1]);
    lemma_frame_basics(t, st[n - 1], frame_end(t, st, n - 1), hh - (n - 1));
    if n > 1 {
        lemma_frame_basics(t, st[n - 2], frame_end(t, st, n - 2), hh - (n - 2));
        assert(u.last() == st[n - 2]);
    }
}

proof fn lemma_frames_set(t: SlabBTree<SlabAgg>, st: Seq<Frame>, f: Frame)
    requires
        frames_ok(t, st), 0 < st.len(),
        frame_ok(t, f, frame_end(t, st, st.len() - 1), t.height() - (st.len() - 1)),
    ensures frames_ok(t, st.update(st.len() - 1, f)),
{
    reveal(frames_ok);
    let n = st.len() as int;
    let u = st.update(n - 1, f);
    assert forall|j: int| 0 <= j < u.len() implies
        frame_ok(t, #[trigger] u[j], frame_end(t, u, j), t.height() - j) by {
        assert(frame_end(t, u, j) == frame_end(t, st, j));
        if j < n - 1 {
            assert(u[j] == st[j]);
        }
    }
}

proof fn lemma_frames_push(t: SlabBTree<SlabAgg>, st: Seq<Frame>, f: Frame)
    requires
        frames_ok(t, st), 0 < st.len(),
        frame_ok(t, f, st.last().slab_base as int, t.height() - st.len()),
    ensures frames_ok(t, st.push(f)),
{
    reveal(frames_ok);
    let n = st.len() as int;
    let u = st.push(f);
    assert forall|j: int| 0 <= j < u.len() implies
        frame_ok(t, #[trigger] u[j], frame_end(t, u, j), t.height() - j) by {
        if j < n {
            assert(u[j] == st[j]);
            assert(frame_end(t, u, j) == frame_end(t, st, j));
        } else {
            assert(st.last() == st[n - 1]);
        }
    }
}

/// A search starts at the root, on the first slab.
proof fn lemma_frames_root(t: SlabBTree<SlabAgg>, f: Frame)
    requires
        t.wf(), f.node == t.root,
        f.next_child == 0, f.prefix_before == 0, f.slab_base == 0, f.items_before == 0,
        forall|i: int| 0 <= i < t@.len() ==> #[trigger] t@[i].prefix_ok(),
    ensures frames_ok(t, seq![f]), search_pos(t, seq![f]) == 0,
{
    reveal(frames_ok);
    reveal(frame_ok);
    let h = t.height();
    assert(t.wf_node(t.root, h));
    assert(t@.subrange(0, t@.len() as int) =~= t@);
    assert(t@.subrange(0, 0) =~= Seq::<SlabAgg>::empty());
    SlabAgg::lemma_prefix_empty(0);
    if let Some(Node::Internal(n)) = t.node_spec(t.root) {
        assert(n.children@.take(0) =~= Seq::<ChildSlot<SlabAgg>>::empty());
        t.lemma_child(t.root, h, n, 0);
    }
    assert(seq![f][0] == f);
    assert(frame_ok(t, f, frame_end(t, seq![f], 0), h - 0));
}

/// Looking at the next slab of a leaf frame.
proof fn lemma_leaf_step(t: SlabBTree<SlabAgg>, f: Frame, end: int, h: int, l: Leaf<SlabAgg>, lo: int, hi: int)
    requires
        t.wf(), valid(t@),
        forall|i: int| 0 <= i < t@.len() ==> #[trigger] t@[i].prefix_ok(),
        frame_ok(t, f, end, h), t.node_spec(f.node) == Some(Node::Leaf(l)), f.next_child < l.aggs.len(),
    ensures ({
        let g = f.slab_base + f.next_child;
        let x = l.aggs@[f.next_child as int];
        let f2 = Frame {
            node: f.node,
            next_child: (f.next_child + 1) as usize,
            prefix_before: (f.prefix_before + x.total) as i64,
            slab_base: f.slab_base,
            items_before: (f.items_before + x.len) as usize,
        };
        &&& frame_at(t, f) == g && g < end && end <= t@.len() && end <= usize::MAX
        &&& in_i64(f.prefix_before + x.total) && f.items_before + x.len <= usize::MAX
        &&& x.len > 0 ==> in_i64(f.prefix_before + x.min_offset) && in_i64(f.prefix_before + x.max_offset)
        &&& may_hold(t@, lo, hi, g)
            == (x.len > 0 && f.prefix_before + x.max_offset >= lo && f.prefix_before + x.min_offset <= hi)
        &&& f.prefix_before == pre_at(t@, g) && f.items_before == sum_len(t@.subrange(0, g))
        &&& frame_ok(t, f2, end, h) && frame_at(t, f2) == g + 1
    }),
{
    reveal(frame_ok);
    let s = t@;
    let aggs = l.aggs@;
    let i = f.next_child as int;
    let g = f.slab_base + i;
    let start = end - aggs.len();
    assert(t.slabs(f.node, h as nat) == aggs);
    assert(s.subrange(start, end)[i] == s[g]);
    lemma_fold_one(s, g);
    lemma_agg_in_range(s, g, g + 1);
    lemma_run_totals(s, g, g + 1);
}

/// Looking at the next child of an internal frame: stepping past it, or
/// descending into it.
proof fn lemma_internal_step(t: SlabBTree<SlabAgg>, f: Frame, end: int, h: int, n: Internal<SlabAgg>, lo: int, hi: int)
    requires
        t.wf(), valid(t@),
        forall|i: int| 0 <= i < t@.len() ==> #[trigger] t@[i].prefix_ok(),
        frame_ok(t, f, end, h), t.node_spec(f.node) == Some(Node::Internal(n)),
        f.next_child < n.children.len(),
    ensures ({
        let c = n.children@[f.next_child as int];
        let a = f.slab_base as int;
        let b = a + c.slab_count;
        let f2 = Frame {
            node: f.node,
            next_child: (f.next_child + 1) as usize,
            prefix_before: (f.prefix_before + c.agg.total) as i64,
            slab_base: b as usize,
            items_before: (f.items_before + c.agg.len) as usize,
        };
        let fc = Frame {
            node: c.id,
            next_child: 0,
            prefix_before: f.prefix_before,
            slab_base: f.slab_base,
            items_before: f.items_before,
        };
        &&& 0 < h && frame_at(t, f) == a && a < b <= end && end <= usize::MAX
        &&& in_i64(f.prefix_before + c.agg.total) && f.items_before + c.agg.len <= usize::MAX
        &&& c.agg.len > 0 ==> in_i64(f.prefix_before + c.agg.min_offset)
            && in_i64(f.prefix_before + c.agg.max_offset)
        &&& frame_ok(t, f2, end, h) && frame_at(t, f2) == b
        &&& frame_ok(t, fc, b, h - 1) && frame_at(t, fc) == a && !frame_done(t, fc)
        &&& !(c.agg.len > 0 && f.prefix_before + c.agg.max_offset >= lo && f.prefix_before + c.agg.min_offset <= hi)
            ==> forall|k: int| a <= k < b ==> !may_hold(t@, lo, hi, k)
    }),
{
    reveal(frame_ok);
    let s = t@;
    let cs = n.children@;
    let k = f.next_child as int;
    let hc = (h - 1) as nat;
    let c = cs[k];
    let before = t.children_slabs(cs.take(k), hc);
    let mid = t.slabs(c.id, hc);
    let rest = t.children_slabs(cs.skip(k + 1), hc);
    let start = end - t.slabs(f.node, h as nat).len();
    let a = f.slab_base as int;
    let b = a + mid.len();
    t.lemma_child(f.node, h as nat, n, k);
    t.lemma_children_slabs_split(cs, k, hc);
    t.lemma_children_slabs_push(cs, k, hc);
    lemma_sub_part(s, start, end, before, mid, rest);
    lemma_valid_subrange(s, a, b);
    lemma_agg_in_range(s, a, b);
    lemma_run_totals(s, a, b);
    let fc = Frame { node: c.id, next_child: 0, prefix_before: f.prefix_before, slab_base: f.slab_base, items_before: f.items_before };
    if let Some(Node::Internal(m)) = t.node_spec(c.id) {
        assert(m.children@.take(0) =~= Seq::<ChildSlot<SlabAgg>>::empty());
        t.lemma_child(c.id, hc, m, 0);
    }
    assert(frame_ok(t, fc, b, h - 1));
    assert(s.subrange(a, b) == mid);
    if !(c.agg.len > 0 && f.prefix_before + c.agg.max_offset >= lo && f.prefix_before + c.agg.min_offset <= hi) {
        assert forall|kk: int| a <= kk < b implies !may_hold(s, lo, hi, kk) by {
            if may_hold(s, lo, hi, kk) {
                lemma_prune(s, lo, hi, a, b, kk);
            }
        }
    }
}

/// A depth-first walk over the slabs whose running sums may meet
/// `lo..=hi`, pruning subtrees whose sums can't.  Yields
/// `(slab_idx, items_before_slab, prefix_before_slab)` in slab order.
pub(crate) struct ValueSearch<'a> {
    tree: &'a SlabBTree<SlabAgg>,
    lo: i64,
    hi: i64,
    stack: Vec<Frame>,
}

impl<'a> ValueSearch<'a> {
    pub(crate) closed spec fn view_tree(&self) -> Seq<SlabAgg> {
        self.tree@
    }

    pub(crate) closed spec fn spec_lo(&self) -> int {
        self.lo as int
    }

    pub(crate) closed spec fn spec_hi(&self) -> int {
        self.hi as int
    }

    /// The next slab the search looks at.
    pub(crate) closed spec fn pos(&self) -> int {
        search_pos(*self.tree, self.stack@)
    }

    pub(crate) closed spec fn inv(&self) -> bool {
        &&& self.tree.wf()
        &&& valid(self.tree@)
        &&& forall|i: int| 0 <= i < self.tree@.len() ==> #[trigger] self.tree@[i].prefix_ok()
        &&& frames_ok(*self.tree, self.stack@)
    }

    pub(crate) fn new(tree: &'a SlabBTree<SlabAgg>, lo: i64, hi: i64) -> (s: Self)
        requires
            tree.wf(),
            valid(tree@),
            forall|i: int| 0 <= i < tree@.len() ==> #[trigger] tree@[i].prefix_ok(),
        ensures
            s.inv(), s.pos() == 0, s.view_tree() == tree@, s.spec_lo() == lo, s.spec_hi() == hi,
    {
        let mut stack = Vec::with_capacity(16);
        let root = Frame { node: tree.root, next_child: 0, prefix_before: 0, slab_base: 0, items_before: 0 };
        stack.push(root);
        proof {
            assert(stack@ =~= seq![root]);
            lemma_frames_root(*tree, root);
            assert(tree.wf_node(tree.root, tree.height()));
            lemma_valid_subrange(tree@, 0, tree@.len() as int);
            assert(tree@.subrange(0, tree@.len() as int) =~= tree@);
        }
        Self { tree, lo, hi, stack }
    }

    /// The next slab, at or after `pos`, that may hold a value in
    /// `lo..=hi`.
    pub(crate) fn next_hit(&mut self) -> (r: Option<(usize, usize, i64)>)
        requires old(self).inv(),
        ensures
            final(self).inv(),
            final(self).view_tree() == old(self).view_tree(),
            final(self).spec_lo() == old(self).spec_lo(),
            final(self).spec_hi() == old(self).spec_hi(),
            match r {
                Some((i, items, prefix)) => {
                    &&& old(self).pos() <= i < old(self).view_tree().len()
                    &&& may_hold(old(self).view_tree(), old(self).spec_lo(), old(self).spec_hi(), i as int)
                    &&& forall|k: int| old(self).pos() <= k < i
                        ==> !may_hold(old(self).view_tree(), old(self).spec_lo(), old(self).spec_hi(), k)
                    &&& items == sum_len(old(self).view_tree().subrange(0, i as int))
                    &&& prefix == pre_at(old(self).view_tree(), i as int)
                    &&& final(self).pos() == i + 1
                },
                None => {
                    &&& forall|k: int| old(self).pos() <= k < old(self).view_tree().len()
                        ==> !may_hold(old(self).view_tree(), old(self).spec_lo(), old(self).spec_hi(), k)
                    &&& final(self).pos() == old(self).view_tree().len()
                },
            },
    {
        let ghost t = *self.tree;
        let ghost hh = t.height() as int;
        let ghost p0 = self.pos();
        let lo = self.lo;
        let hi = self.hi;
        proof {
            if self.stack@.len() > 0 {
                lemma_frames_top(t, self.stack@);
                lemma_frame_basics(t, self.stack@.last(), frame_end(t, self.stack@, self.stack@.len() - 1),
                    hh - (self.stack@.len() - 1));
            }
        }
        loop
            invariant
                self.inv(), *self.tree == t, hh == t.height(), self.lo == lo, self.hi == hi,
                old(self).pos() == p0, old(self).view_tree() == t@,
                old(self).spec_lo() == lo, old(self).spec_hi() == hi,
                p0 <= self.pos() <= t@.len(),
                forall|k: int| p0 <= k < self.pos() ==> !may_hold(t@, lo as int, hi as int, k),
            decreases t@.len() - self.pos(), done_top(t, self.stack@), hh + 1 - self.stack@.len(),
        {
            let d = self.stack.len();
            if d == 0 {
                return None;
            }
            let ghost st = self.stack@;
            let ghost h = hh - (d - 1);
            let ghost end = frame_end(t, st, d - 1);
            let mut frame = self.stack[d - 1];
            proof {
                lemma_frames_top(t, st);
                assert(frame == st.last());
                lemma_frame_basics(t, frame, end, h);
            }
            match self.tree.node(frame.node) {
                Node::Leaf(l) => {
                    while frame.next_child < l.aggs.len()
                        invariant
                            self.inv(), *self.tree == t, hh == t.height(), self.lo == lo, self.hi == hi,
                            old(self).pos() == p0, old(self).view_tree() == t@,
                            old(self).spec_lo() == lo, old(self).spec_hi() == hi,
                            self.stack@ == st, d == st.len(), d > 0, h == hh - (d - 1),
                            end == frame_end(t, st, d - 1),
                            t.node_spec(frame.node) == Some(Node::<SlabAgg>::Leaf(*l)),
                            frame_ok(t, frame, end, h),
                            frame == st.last() || frame_at(t, frame) > frame_at(t, st.last()),
                            p0 <= frame_at(t, st.last()) <= frame_at(t, frame),
                            forall|k: int| p0 <= k < frame_at(t, frame) ==> !may_hold(t@, lo as int, hi as int, k),
                        decreases l.aggs.len() - frame.next_child,
                    {
                        let i = frame.next_child;
                        let agg = &l.aggs[i];
                        proof {
                            lemma_leaf_step(t, frame, end, h, *l, lo as int, hi as int);
                        }
                        let prefix = frame.prefix_before;
                        let slab_idx = frame.slab_base + i;
                        let items_before = frame.items_before;
                        frame.next_child += 1;
                        frame.prefix_before += agg.total;
                        frame.items_before += agg.len;
                        if agg.overlaps(lo, hi, prefix) {
                            self.stack.set(d - 1, frame);
                            proof {
                                lemma_frames_set(t, st, frame);
                                assert(self.stack@ == st.update(d - 1, frame));
                                assert(self.stack@.last() == frame);
                            }
                            return Some((slab_idx, items_before, prefix));
                        }
                    }
                    proof {
                        lemma_frame_basics(t, frame, end, h);
                        lemma_frames_set(t, st, frame);
                        lemma_frames_pop(t, st.update(d - 1, frame));
                        assert(st.update(d - 1, frame).drop_last() =~= st.drop_last());
                    }
                    self.stack.pop();
                    proof {
                        assert(self.stack@ =~= st.drop_last());
                    }
                }
                Node::Internal(n) => {
                    if frame.next_child >= n.children.len() {
                        self.stack.pop();
                        proof {
                            lemma_frames_pop(t, st);
                            assert(self.stack@ =~= st.drop_last());
                        }
                        continue;
                    }
                    let k = frame.next_child;
                    let c = &n.children[k];
                    proof {
                        lemma_internal_step(t, frame, end, h, *n, lo as int, hi as int);
                    }
                    let prefix = frame.prefix_before;
                    let slab_base = frame.slab_base;
                    let items_before = frame.items_before;
                    frame.next_child += 1;
                    frame.prefix_before += c.agg.total;
                    frame.slab_base += c.slab_count;
                    frame.items_before += c.agg.len;
                    self.stack.set(d - 1, frame);
                    let ghost st1 = self.stack@;
                    proof {
                        lemma_frames_set(t, st, frame);
                        assert(st1 == st.update(d - 1, frame));
                    }
                    if c.agg.overlaps(lo, hi, prefix) {
                        let child = Frame { node: c.id, next_child: 0, prefix_before: prefix, slab_base, items_before };
                        self.stack.push(child);
                        proof {
                            assert(st1.last() == frame);
                            lemma_frames_push(t, st1, child);
                        }
                    }
                }
            }
        }
    }
}

impl<A: SlabAggregate> View for SlabBTree<A> {
    type V = Seq<A>;

    /// The slab aggregates, in order.
    closed spec fn view(&self) -> Seq<A> {
        self.slabs(self.root, self.height())
    }
}

} // verus!

impl<A: SlabAggregate> Default for Leaf<A> {
    fn default() -> Self {
        Self { aggs: Vec::new() }
    }
}

impl<A: SlabAggregate + Copy> Copy for ChildSlot<A> {}

impl<A: SlabAggregate> Default for SlabBTree<A> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
fn merge_all<'a, A: SlabAggregate + 'a>(aggs: impl IntoIterator<Item = &'a A>) -> A {
    let mut acc = A::default();
    for a in aggs {
        acc = A::merge(&acc, a);
    }
    acc
}

// ── SlabAgg (used by DeltaColumn) ───────────────────────────────────────────

impl SlabAgg {
    #[cfg(test)]
    fn contains(&self, target: i64, prefix_before: i64) -> bool {
        self.len > 0
            && target >= prefix_before + self.min_offset
            && target <= prefix_before + self.max_offset
    }
}

impl<A: SlabAggregate> SlabBTree<A> {
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.total_slabs == 0
    }

    // ── Arena helpers ───────────────────────────────────────────────────

    /// Replace slabs `range` with `new_aggs`.
    pub(crate) fn splice<I: IntoIterator<Item = A>>(&mut self, range: Range<usize>, new_aggs: I) {
        let new: Vec<A> = new_aggs.into_iter().collect();
        self.splice_vec(range.start, range.end, new);
    }

    // ── Bulk construction ───────────────────────────────────────────────

    pub(crate) fn from_iter<I: IntoIterator<Item = A>>(iter: I) -> Self {
        Self::from_vec(iter.into_iter().collect())
    }
}

// ── Structural invariant checker (test builds only) ─────────────────────────

#[cfg(test)]
impl<A: SlabAggregate> SlabBTree<A> {
    /// The root's height (leaves are 0), following first children.
    pub(crate) fn root_height(&self) -> usize {
        let mut h = 0;
        let mut id = self.root;
        while let Node::Internal(n) = self.node(id) {
            id = n.children[0].id;
            h += 1;
        }
        h
    }

    /// Verify every structural invariant; panics with a description on the
    /// first violation.  O(nodes) — test builds only.
    ///
    /// * arena consistency: reachable ⟺ live, dead ⟺ on the free list,
    ///   no node reachable twice, no duplicate free-list entries
    /// * all leaves at uniform depth
    /// * no empty nodes (except the root leaf of an empty tree);
    ///   an internal root has ≥ 2 children (else it must be collapsed)
    /// * node sizes ≤ B
    /// * every cached `slab_count` and `agg.len()` matches recomputation;
    ///   `total_slabs` matches the real slab count
    pub(crate) fn check_invariants(&self) {
        let mut reachable = vec![false; self.nodes.len()];
        let mut leaf_depth: Option<usize> = None;
        if let Node::Internal(n) = self.node(self.root) {
            assert!(
                n.children.len() >= 2,
                "internal root with {} children (should be collapsed)",
                n.children.len()
            );
        }
        let (slabs, _items) = self.check_node(self.root, 0, &mut leaf_depth, &mut reachable);
        assert_eq!(slabs, self.total_slabs, "total_slabs mismatch");

        let free_set: std::collections::HashSet<NodeId> = self.free.iter().copied().collect();
        assert_eq!(
            free_set.len(),
            self.free.len(),
            "duplicate free-list entries"
        );
        for (i, slot) in self.nodes.iter().enumerate() {
            let live = slot.is_some();
            let reach = reachable[i];
            let freed = free_set.contains(&(i as NodeId));
            assert!(!reach || live, "node {i}: reachable but freed");
            assert!(!live || reach, "node {i}: live but unreachable (leak)");
            assert!(!(live && freed), "node {i}: live but on free list");
            assert!(live || freed, "node {i}: dead but not on free list");
        }
    }

    /// Returns `(slab_count, item_count)` for the subtree at `id`.
    fn check_node(
        &self,
        id: NodeId,
        depth: usize,
        leaf_depth: &mut Option<usize>,
        reachable: &mut [bool],
    ) -> (usize, usize) {
        assert!(!reachable[id as usize], "node {id}: reachable twice");
        reachable[id as usize] = true;
        match self.node(id) {
            Node::Leaf(l) => {
                assert!(l.aggs.len() <= B, "leaf {id}: over B ({})", l.aggs.len());
                if id != self.root {
                    assert!(!l.aggs.is_empty(), "leaf {id}: empty non-root leaf");
                }
                match *leaf_depth {
                    Some(d) => assert_eq!(d, depth, "leaf {id}: non-uniform depth"),
                    None => *leaf_depth = Some(depth),
                }
                (l.aggs.len(), merge_all(&l.aggs).len())
            }
            Node::Internal(n) => {
                assert!(!n.children.is_empty(), "internal {id}: empty");
                assert!(n.children.len() <= B, "internal {id}: over B");
                let mut slabs = 0;
                let mut items = 0;
                for c in &n.children {
                    let (s, it) = self.check_node(c.id, depth + 1, leaf_depth, reachable);
                    assert_eq!(
                        s, c.slab_count,
                        "internal {id}: cached slab_count for child {} is {} (actual {s})",
                        c.id, c.slab_count
                    );
                    assert_eq!(
                        it,
                        c.agg.len(),
                        "internal {id}: cached agg.len() for child {} is {} (actual {it})",
                        c.id,
                        c.agg.len()
                    );
                    slabs += s;
                    items += it;
                }
                (slabs, items)
            }
        }
    }
}

// ── Value-range queries (SlabAgg-specific) ──────────────────────────────────

impl SlabBTree<SlabAgg> {
    pub(crate) fn find_by_value(&self, target: i64) -> FindByValue<'_> {
        FindByValue(ValueSearch::new(self, target, target))
    }

    pub(crate) fn find_by_value_range(&self, lo: i64, hi: i64) -> FindByValueRange<'_> {
        FindByValueRange(ValueSearch::new(self, lo, hi))
    }
}

/// The slabs that may hold `target`: `(slab_idx, items_before_slab,
/// prefix_before_slab)`, in order.
pub struct FindByValue<'a>(ValueSearch<'a>);

impl Iterator for FindByValue<'_> {
    type Item = (usize, usize, i64);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next_hit()
    }
}

/// The slabs that may hold a value in `lo..=hi`: `(slab_idx,
/// items_before_slab, prefix_before_slab)`, in order.
pub struct FindByValueRange<'a>(ValueSearch<'a>);

impl Iterator for FindByValueRange<'_> {
    type Item = (usize, usize, i64);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next_hit()
    }
}

// ── Test helpers ────────────────────────────────────────────────────────────

impl<A: PrefixAggregate> FoundSlab<A> {
    #[cfg(test)]
    pub(crate) fn decompose(&self) -> (usize, A::Prefix, usize) {
        (self.index, self.prefix.clone(), self.pos)
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::SmallRng;
    use rand::{RngExt, SeedableRng};

    fn reference_find_by_value(aggs: &[SlabAgg], target: i64) -> Vec<(usize, usize, i64)> {
        let mut out = Vec::new();
        let mut prefix = 0i64;
        let mut items_before = 0usize;
        for (i, a) in aggs.iter().enumerate() {
            if a.contains(target, prefix) {
                out.push((i, items_before, prefix));
            }
            prefix += a.total;
            items_before += a.len;
        }
        out
    }

    fn reference_find_by_range(aggs: &[SlabAgg], lo: i64, hi: i64) -> Vec<(usize, usize, i64)> {
        let mut out = Vec::new();
        let mut prefix = 0i64;
        let mut items_before = 0usize;
        for (i, a) in aggs.iter().enumerate() {
            if a.overlaps(lo, hi, prefix) {
                out.push((i, items_before, prefix));
            }
            prefix += a.total;
            items_before += a.len;
        }
        out
    }

    fn simple_agg(total: i64, len: usize) -> SlabAgg {
        SlabAgg {
            len,
            total,
            min_offset: total.min(0),
            max_offset: total.max(0),
        }
    }

    #[test]
    fn empty_tree() {
        let t: SlabBTree<SlabAgg> = SlabBTree::new();
        assert!(t.is_empty());
        assert_eq!(t.len(), 0);
        assert_eq!(t.root_agg(), SlabAgg::default());
        assert_eq!(t.find_by_value(0).collect::<Vec<_>>(), vec![]);
    }

    #[test]
    fn from_iter_single() {
        let aggs = vec![simple_agg(10, 5)];
        let t = SlabBTree::<SlabAgg>::from_iter(aggs.clone());
        assert_eq!(t.len(), 1);
        assert_eq!(t.root_agg().len, 5);
        assert_eq!(t.root_agg().total, 10);
    }

    #[test]
    fn from_iter_many_levels() {
        let aggs: Vec<_> = (0..100).map(|i| simple_agg((i + 1) as i64, 3)).collect();
        let t = SlabBTree::<SlabAgg>::from_iter(aggs.clone());
        assert_eq!(t.len(), 100);
        assert_eq!(t.root_agg().len, 300);
        for target in [1i64, 100, 5050, 9999] {
            let got: Vec<_> = t.find_by_value(target).collect();
            let want = reference_find_by_value(&aggs, target);
            assert_eq!(got, want, "target={target}");
        }
    }

    #[test]
    fn find_by_prefix_basic() {
        let aggs: Vec<_> = (0..10).map(|_| simple_agg(10, 5)).collect();
        let t = SlabBTree::<SlabAgg>::from_iter(aggs);
        assert_eq!(t.find_by_prefix(0), Some((0, 0)));
        assert_eq!(t.find_by_prefix(4), Some((0, 0)));
        assert_eq!(t.find_by_prefix(5), Some((1, 5)));
        assert_eq!(t.find_by_prefix(49), Some((9, 45)));
        assert_eq!(t.find_by_prefix(50), None);
    }

    /// One splice into a lone leaf that splits it into more than B leaves:
    /// the root has to grow two levels at once.
    #[test]
    fn splice_grows_root_by_levels() {
        let mut t: SlabBTree<usize> = SlabBTree::new();
        t.splice(0..0, vec![1usize; B * B + 1]);
        t.check_invariants();
        assert_eq!(t.len(), B * B + 1);
        assert_eq!(t.root_agg(), B * B + 1);
        assert_eq!(t.root_height(), 2);
        t.splice(10..20, vec![2usize; 3]);
        t.check_invariants();
        assert_eq!(t.root_agg(), B * B + 1 - 10 + 6);
    }

    #[test]
    fn splice_no_op() {
        let aggs: Vec<_> = (0..20).map(|i| simple_agg(i as i64, 1)).collect();
        let mut t = SlabBTree::<SlabAgg>::from_iter(aggs.clone());
        t.splice(5..5, std::iter::empty());
        assert_eq!(t.len(), 20);
        for target in [3i64, 10, 100] {
            let got: Vec<_> = t.find_by_value(target).collect();
            let want = reference_find_by_value(&aggs, target);
            assert_eq!(got, want);
        }
    }

    #[test]
    fn splice_replace_single() {
        let mut aggs: Vec<_> = (0..20).map(|i| simple_agg(i as i64, 2)).collect();
        let mut t = SlabBTree::<SlabAgg>::from_iter(aggs.clone());
        let new_agg = simple_agg(999, 3);
        t.splice(10..11, std::iter::once(new_agg));
        aggs[10] = new_agg;
        let prefix_before_10: i64 = aggs[..10].iter().map(|a| a.total).sum();
        let target = prefix_before_10 + 999;
        let got: Vec<_> = t.find_by_value(target).collect();
        let want = reference_find_by_value(&aggs, target);
        assert_eq!(got, want);
    }

    #[test]
    fn splice_grow_and_shrink() {
        let mut aggs: Vec<_> = (0..10).map(|i| simple_agg(i as i64, 2)).collect();
        let mut t = SlabBTree::<SlabAgg>::from_iter(aggs.clone());

        let new: Vec<_> = (100..105).map(|v| simple_agg(v, 1)).collect();
        t.splice(3..3, new.clone());
        aggs.splice(3..3, new);
        assert_eq!(t.len(), 15);

        t.splice(7..11, std::iter::empty());
        aggs.drain(7..11);
        assert_eq!(t.len(), 11);

        for target in -10i64..200 {
            let got: Vec<_> = t.find_by_value(target).collect();
            let want = reference_find_by_value(&aggs, target);
            assert_eq!(got, want, "target={target}");
        }
    }

    #[test]
    fn find_by_value_unique_hit() {
        let mut aggs: Vec<_> = (0..1000).map(|_| simple_agg(1, 1)).collect();
        aggs[500] = simple_agg(10_000, 1);
        let t = SlabBTree::<SlabAgg>::from_iter(aggs);
        let target = 500 + 10_000;
        let mut iter = t.find_by_value(target);
        let next = iter.next();
        assert_eq!(next.map(|(si, ib, _)| (si, ib)), Some((500, 500)));
        drop(iter);
    }

    #[test]
    fn find_by_range_returns_iterator() {
        let aggs: Vec<_> = (0..50).map(|i| simple_agg((i + 1) * 10, 1)).collect();
        let t = SlabBTree::<SlabAgg>::from_iter(aggs.clone());
        let got: Vec<_> = t.find_by_value_range(100, 500).collect();
        let want = reference_find_by_range(&aggs, 100, 500);
        assert_eq!(got, want);
    }

    #[test]
    fn fuzz_splice_vs_reference() {
        let mut rng = SmallRng::seed_from_u64(0xC0FFEE);
        let mut reference: Vec<SlabAgg> = (0..30)
            .map(|_| simple_agg(rand_total(&mut rng), 1))
            .collect();
        let mut tree = SlabBTree::<SlabAgg>::from_iter(reference.clone());

        for _ in 0..2_000 {
            let op = rng.random_range(0..3);
            let len = reference.len();
            match op {
                0 => {
                    let at = rng.random_range(0..=len);
                    let count = rng.random_range(1..=5);
                    let new: Vec<_> = (0..count)
                        .map(|_| simple_agg(rand_total(&mut rng), 1))
                        .collect();
                    tree.splice(at..at, new.clone());
                    reference.splice(at..at, new);
                }
                1 if len > 0 => {
                    let at = rng.random_range(0..len);
                    let count = rng.random_range(1..=(len - at).min(5));
                    tree.splice(at..at + count, std::iter::empty());
                    reference.drain(at..at + count);
                }
                _ if len > 0 => {
                    let at = rng.random_range(0..len);
                    let del = rng.random_range(1..=(len - at).min(3));
                    let count = rng.random_range(1..=4);
                    let new: Vec<_> = (0..count)
                        .map(|_| simple_agg(rand_total(&mut rng), 1))
                        .collect();
                    tree.splice(at..at + del, new.clone());
                    reference.splice(at..at + del, new);
                }
                _ => {}
            }
            assert_eq!(tree.len(), reference.len());
            tree.check_invariants();
            for _ in 0..4 {
                let t: i64 = rng.random_range(-5_000..5_000);
                let got: Vec<_> = tree.find_by_value(t).collect();
                let want = reference_find_by_value(&reference, t);
                assert_eq!(got, want, "target={t}");
            }
        }
    }

    fn rand_total(rng: &mut SmallRng) -> i64 {
        rng.random_range(-100..100)
    }

    // ── Prefix-sum B-tree smoke test ────────────────────────────────────

    use crate::prefix::PrefixSlabWeight;

    fn psw(len: usize, prefix: i64) -> PrefixSlabWeight<i64> {
        PrefixSlabWeight { len, prefix }
    }

    #[test]
    fn update_slab_single_slab_no_descend() {
        let mut tree = SlabBTree::<SlabAgg>::from_iter(vec![simple_agg(10, 3)]);
        tree.update_slab(0, simple_agg(20, 5));
        let got: Vec<_> = tree.find_by_value(20).collect();
        assert_eq!(got, vec![(0, 0, 0)]);
    }

    #[test]
    fn update_slab_deep_tree() {
        // 200 slabs forces multi-level tree at B=16.
        let aggs: Vec<_> = (0..200).map(|i| simple_agg((i + 1) as i64, 1)).collect();
        let mut tree = SlabBTree::<SlabAgg>::from_iter(aggs.clone());
        // Update slab 100 to carry a unique large value.
        let prefix_before_100: i64 = aggs[..100].iter().map(|a| a.total).sum();
        tree.update_slab(100, simple_agg(999_999, 1));
        let target = prefix_before_100 + 999_999;
        let mut iter = tree.find_by_value(target);
        let first = iter.next().unwrap();
        assert_eq!(first.0, 100);
    }

    #[test]
    fn update_slab_root_agg_refreshed() {
        let aggs: Vec<_> = (0..50).map(|_| simple_agg(1, 1)).collect();
        let mut tree = SlabBTree::<SlabAgg>::from_iter(aggs);
        assert_eq!(tree.root_agg().total, 50);
        tree.update_slab(25, simple_agg(100, 1));
        // Total went from 50 to 50 - 1 + 100 = 149.
        assert_eq!(tree.root_agg().total, 149);
    }

    #[test]
    fn prefix_btree_find_slab_at_item() {
        // 10 slabs, each len=5, prefix values chosen to be distinct.
        let weights: Vec<_> = (0..10).map(|i| psw(5, (i + 1) as i64 * 10)).collect();
        let tree = SlabBTree::from_iter(weights.clone());
        assert_eq!(tree.len(), 10);
        // Item 0 is in slab 0; nothing before → (0, 0, 0).
        assert_eq!(tree.find_slab_at_item(0).decompose(), (0, 0, 0));
        // Item 5 is the start of slab 1 → prefix = weights[0].prefix = 10,
        // items_before = 5.
        assert_eq!(tree.find_slab_at_item(5).decompose(), (1, 10, 5));
        // Item 24 is inside slab 4 → prefix = sum of slabs 0..=3 = 100,
        // items_before = 20.
        assert_eq!(tree.find_slab_at_item(24).decompose(), (4, 100, 20));
    }

    #[test]
    fn prefix_btree_find_slab_at_prefix() {
        let weights: Vec<_> = (0..10).map(|i| psw(5, (i + 1) as i64 * 10)).collect();
        let tree = SlabBTree::from_iter(weights);
        // Prefix sum of slab 0 is 10; target=10 lands in slab 0 (≤ 10).
        assert_eq!(tree.find_slab_at_prefix(&10), (0, 0, 0));
        // Target 15: crosses into slab 1 (0+10 < 15 <= 0+10+20=30).
        assert_eq!(tree.find_slab_at_prefix(&15), (1, 10, 5));
        // Target 100: prefix_before slab 3 = 60; slab 3 ends at 100; target=100 ≤ 100 → slab 3.
        assert_eq!(tree.find_slab_at_prefix(&100), (3, 60, 15));
        // Target past total (sum = 550): one-past-end.
        assert_eq!(tree.find_slab_at_prefix(&9999), (10, 550, 50));
    }

    // ── Fuzz: find_slab, find_slab_at_item, find_slab_at_prefix, root_agg ──

    fn reference_find_by_prefix(aggs: &[SlabAgg], pos: usize) -> Option<(usize, usize)> {
        let mut items_before: usize = 0;
        for (i, a) in aggs.iter().enumerate() {
            if pos < items_before + a.len {
                return Some((i, items_before));
            }
            items_before += a.len;
        }
        None
    }

    fn reference_find_slab_at_item(aggs: &[SlabAgg], item_idx: usize) -> (usize, i64, usize) {
        let mut prefix: i64 = 0;
        let mut items_before: usize = 0;
        for (i, a) in aggs.iter().enumerate() {
            if item_idx < items_before + a.len {
                return (i, prefix, items_before);
            }
            prefix += a.total;
            items_before += a.len;
        }
        (aggs.len(), prefix, items_before)
    }

    fn reference_find_slab_at_prefix(aggs: &[SlabAgg], target: i64) -> (usize, i64, usize) {
        let mut prefix: i64 = 0;
        let mut items_before: usize = 0;
        for (i, a) in aggs.iter().enumerate() {
            if prefix + a.total >= target {
                return (i, prefix, items_before);
            }
            prefix += a.total;
            items_before += a.len;
        }
        (aggs.len(), prefix, items_before)
    }

    fn reference_root_agg(aggs: &[SlabAgg]) -> SlabAgg {
        aggs.iter()
            .fold(SlabAgg::default(), |acc, a| SlabAgg::merge(&acc, a))
    }

    #[test]
    fn fuzz_find_slab_and_prefix() {
        let mut rng = SmallRng::seed_from_u64(0xDEADBEEF);

        for trial in 0..500 {
            let mut reference: Vec<SlabAgg> = (0..10)
                .map(|_| simple_agg(rand_total(&mut rng), rng.random_range(1..=10)))
                .collect();
            let mut tree = SlabBTree::<SlabAgg>::from_iter(reference.clone());

            for step in 0..200 {
                let len = reference.len();
                let op = rng.random_range(0..3);
                match op {
                    0 => {
                        let at = rng.random_range(0..=len);
                        let count = rng.random_range(1..=3);
                        let new: Vec<_> = (0..count)
                            .map(|_| simple_agg(rand_total(&mut rng), rng.random_range(1..=10)))
                            .collect();
                        tree.splice(at..at, new.clone());
                        reference.splice(at..at, new);
                    }
                    1 if len > 1 => {
                        let at = rng.random_range(0..len);
                        let count = rng.random_range(1..=(len - at).min(3));
                        tree.splice(at..at + count, std::iter::empty());
                        reference.drain(at..at + count);
                    }
                    _ if len > 0 => {
                        let at = rng.random_range(0..len);
                        let new_agg = simple_agg(rand_total(&mut rng), rng.random_range(1..=10));
                        tree.update_slab(at, new_agg);
                        reference[at] = new_agg;
                    }
                    _ => {}
                }

                assert_eq!(
                    tree.len(),
                    reference.len(),
                    "trial={trial} step={step}: slab count"
                );
                tree.check_invariants();

                // Verify root_agg
                let expected_root = reference_root_agg(&reference);
                assert_eq!(
                    tree.root_agg(),
                    expected_root,
                    "trial={trial} step={step}: root_agg"
                );

                let total_items: usize = reference.iter().map(|a| a.len).sum();

                // find_by_prefix (positional slab lookup)
                if total_items > 0 {
                    for _ in 0..3 {
                        let pos = rng.random_range(0..total_items);
                        let got = tree.find_by_prefix(pos);
                        let want = reference_find_by_prefix(&reference, pos);
                        assert_eq!(
                            got, want,
                            "trial={trial} step={step}: find_by_prefix({pos})"
                        );
                    }
                    // Edge: last item
                    let got = tree.find_by_prefix(total_items - 1);
                    let want = reference_find_by_prefix(&reference, total_items - 1);
                    assert_eq!(got, want, "trial={trial} step={step}: find_by_prefix(last)");
                    // Edge: past end
                    assert_eq!(tree.find_by_prefix(total_items), None);
                }

                // find_slab_at_item (prefix-aware positional lookup)
                if total_items > 0 {
                    for _ in 0..3 {
                        let idx = rng.random_range(0..total_items);
                        let got = tree.find_slab_at_item(idx).decompose();
                        let want = reference_find_slab_at_item(&reference, idx);
                        assert_eq!(
                            got, want,
                            "trial={trial} step={step}: find_slab_at_item({idx})"
                        );
                    }
                }

                // find_slab_at_prefix (prefix-sum based lookup)
                let total_prefix: i64 = reference.iter().map(|a| a.total).sum();
                if total_prefix > 0 && reference.iter().all(|a| a.total >= 0) {
                    for _ in 0..3 {
                        let target = rng.random_range(1..=total_prefix);
                        let got = tree.find_slab_at_prefix(&target);
                        let want = reference_find_slab_at_prefix(&reference, target);
                        assert_eq!(
                            got, want,
                            "trial={trial} step={step}: find_slab_at_prefix({target})"
                        );
                    }
                }

                // find_by_value (existing fuzz coverage, but now also after
                // update_slab which wasn't fuzzed before)
                for _ in 0..2 {
                    let t: i64 = rng.random_range(-250..250);
                    let got: Vec<_> = tree.find_by_value(t).collect();
                    let want = reference_find_by_value(&reference, t);
                    assert_eq!(got, want, "trial={trial} step={step}: find_by_value({t})");
                }

                // find_by_value_range
                {
                    let lo: i64 = rng.random_range(-200..200);
                    let hi = lo + rng.random_range(0..200);
                    let got: Vec<_> = tree.find_by_value_range(lo, hi).collect();
                    let want = reference_find_by_range(&reference, lo, hi);
                    assert_eq!(
                        got, want,
                        "trial={trial} step={step}: find_by_value_range({lo}..{hi})"
                    );
                }
            }
        }
    }

    #[test]
    fn fuzz_btree_with_prefix_weights() {
        use crate::prefix::PrefixSlabWeight;
        type W = PrefixSlabWeight<u64>;
        type Tree = SlabBTree<W>;

        let mut rng = SmallRng::seed_from_u64(0xCAFEBABE);

        fn rand_weight(rng: &mut SmallRng) -> W {
            let len = rng.random_range(1..=8);
            let prefix = rng.random_range(0..50);
            W { len, prefix }
        }

        for trial in 0..500 {
            let mut reference: Vec<W> = (0..10).map(|_| rand_weight(&mut rng)).collect();
            let mut tree = Tree::from_iter(reference.clone());

            for step in 0..200 {
                let len = reference.len();
                let op = rng.random_range(0..4);
                match op {
                    0 => {
                        let at = rng.random_range(0..=len);
                        let count = rng.random_range(1..=3);
                        let new: Vec<_> = (0..count).map(|_| rand_weight(&mut rng)).collect();
                        tree.splice(at..at, new.clone());
                        reference.splice(at..at, new);
                    }
                    1 if len > 1 => {
                        let at = rng.random_range(0..len);
                        let count = rng.random_range(1..=(len - at).min(3));
                        tree.splice(at..at + count, std::iter::empty());
                        reference.drain(at..at + count);
                    }
                    2 if len > 0 => {
                        let at = rng.random_range(0..len);
                        let new = rand_weight(&mut rng);
                        tree.update_slab(at, new);
                        reference[at] = new;
                    }
                    _ if len > 0 => {
                        let at = rng.random_range(0..len);
                        let del = rng.random_range(1..=(len - at).min(2));
                        let ins = rng.random_range(1..=3);
                        let new: Vec<_> = (0..ins).map(|_| rand_weight(&mut rng)).collect();
                        tree.splice(at..at + del, new.clone());
                        reference.splice(at..at + del, new);
                    }
                    _ => {}
                }

                assert_eq!(
                    tree.len(),
                    reference.len(),
                    "trial={trial} step={step}: len"
                );
                tree.check_invariants();

                // Verify root aggregate matches
                let expected_root = reference.iter().copied().fold(W::default(), |mut acc, w| {
                    acc += w;
                    acc
                });
                let root = tree.root_agg();
                assert_eq!(
                    root.len, expected_root.len,
                    "trial={trial} step={step}: root_agg.len"
                );
                assert_eq!(
                    root.prefix, expected_root.prefix,
                    "trial={trial} step={step}: root_agg.prefix"
                );

                let total_items: usize = reference.iter().map(|w| w.len).sum();
                let total_prefix: u64 = reference.iter().map(|w| w.prefix).sum();

                // find_by_prefix (positional)
                if total_items > 0 {
                    let pos = rng.random_range(0..total_items);
                    let got = tree.find_by_prefix(pos);
                    let mut items = 0;
                    let mut expected = None;
                    for (i, w) in reference.iter().enumerate() {
                        if pos < items + w.len {
                            expected = Some((i, items));
                            break;
                        }
                        items += w.len;
                    }
                    assert_eq!(
                        got, expected,
                        "trial={trial} step={step}: find_by_prefix({pos})"
                    );
                }

                // find_slab_at_item
                if total_items > 0 {
                    let idx = rng.random_range(0..total_items);
                    let found = tree.find_slab_at_item(idx);
                    let mut exp_prefix = 0u64;
                    let mut exp_items = 0;
                    let mut exp_si = reference.len();
                    for (i, w) in reference.iter().enumerate() {
                        if idx < exp_items + w.len {
                            exp_si = i;
                            break;
                        }
                        exp_prefix += w.prefix;
                        exp_items += w.len;
                    }
                    assert_eq!(
                        (found.index, found.prefix, found.pos),
                        (exp_si, exp_prefix, exp_items),
                        "trial={trial} step={step}: find_slab_at_item({idx})"
                    );
                }

                // find_slab_at_prefix
                if total_prefix > 0 {
                    let target = rng.random_range(1..=total_prefix);
                    let (si, prefix_before, items_before) = tree.find_slab_at_prefix(&target);
                    let mut exp_prefix = 0u64;
                    let mut exp_items = 0;
                    let mut exp_si = reference.len();
                    for (i, w) in reference.iter().enumerate() {
                        if exp_prefix + w.prefix >= target {
                            exp_si = i;
                            break;
                        }
                        exp_prefix += w.prefix;
                        exp_items += w.len;
                    }
                    assert_eq!(
                        (si, prefix_before, items_before),
                        (exp_si, exp_prefix, exp_items),
                        "trial={trial} step={step}: find_slab_at_prefix({target})"
                    );
                }
            }
        }
    }
    // ── Underflow regression tests ──────────────────────────────────────
    //
    // These pin the three bugs found in the deletion-underflow audit:
    // an LCA left empty by an exact-span delete (silent corruption), a
    // whole-leaf delete falling into an O(N) full rebuild, and the root
    // never collapsing after mass deletion.

    #[test]
    fn full_wipe_then_reinsert() {
        let aggs: Vec<_> = (0..65).map(|i| simple_agg(i as i64 + 1, 1)).collect();
        let mut t = SlabBTree::<SlabAgg>::from_iter(aggs);
        t.splice(0..65, std::iter::empty());
        assert_eq!(t.len(), 0);
        t.check_invariants();
        t.splice(0..0, std::iter::once(simple_agg(5, 1)));
        assert_eq!(t.len(), 1);
        assert_eq!(t.find_by_prefix(0), Some((0, 0)));
        t.check_invariants();
    }

    #[test]
    fn exact_internal_span_delete() {
        // Pure delete exactly covering the middle level-1 internal's span.
        let n = 64 * 64 * 3;
        let aggs: Vec<_> = (0..n).map(|i| simple_agg(i as i64, 1)).collect();
        let mut t = SlabBTree::<SlabAgg>::from_iter(aggs);
        assert_eq!(t.root_height(), 2);
        t.splice(4096..8192, std::iter::empty());
        assert_eq!(t.len(), 8192);
        assert_eq!(t.find_by_prefix(5000), Some((5000, 5000)));
        assert_eq!(t.root_agg().len, 8192);
        t.check_invariants();
    }

    #[test]
    fn whole_leaf_delete_removes_leaf() {
        // Deleting all 64 slabs of leaf 0 removes the leaf (no rebuild)
        // and the root collapses onto the surviving leaf.
        let aggs: Vec<_> = (0..65).map(|i| simple_agg(i as i64 + 1, 1)).collect();
        let mut t = SlabBTree::<SlabAgg>::from_iter(aggs);
        t.splice(0..64, std::iter::empty());
        assert_eq!(t.len(), 1);
        assert_eq!(t.find_by_prefix(0), Some((0, 0)));
        assert_eq!(t.root_height(), 0, "root should collapse to the leaf");
        t.check_invariants();
    }

    #[test]
    fn sequential_single_deletes_stay_valid() {
        // The pattern that used to trigger an O(N) rebuild every ~64th
        // delete.  Now each delete is O(log n): leaves shrink in place,
        // empty and merge away, and the root collapses at the end.
        let aggs: Vec<_> = (0..2_000).map(|i| simple_agg(i as i64, 1)).collect();
        let mut t = SlabBTree::<SlabAgg>::from_iter(aggs);
        for i in 0..1_999 {
            t.splice(0..1, std::iter::empty());
            if i % 64 == 0 {
                t.check_invariants();
            }
        }
        assert_eq!(t.len(), 1);
        assert_eq!(t.root_height(), 0);
        t.check_invariants();
    }

    #[test]
    fn root_collapses_after_mass_delete() {
        let n = 64 * 64 + 5;
        let aggs: Vec<_> = (0..n).map(|i| simple_agg(i as i64, 1)).collect();
        let mut t = SlabBTree::<SlabAgg>::from_iter(aggs);
        assert_eq!(t.root_height(), 2);
        t.splice(5..n, std::iter::empty());
        assert_eq!(t.len(), 5);
        assert!(
            t.root_height() <= 1,
            "height {} for 5 slabs",
            t.root_height()
        );
        for i in 0..5 {
            assert_eq!(t.find_by_prefix(i), Some((i, i)));
        }
        t.check_invariants();
    }

    // ── Deep structural fuzz ────────────────────────────────────────────
    //
    // The older fuzzers run at ~10-70 slabs, which at B=64 almost never
    // leaves a single root leaf.  This one starts at 200-1500 slabs so
    // splits, LCA splices, underflow removal, sibling merges, and root
    // collapse all actually fire — with full invariant checking each step.

    #[test]
    fn fuzz_deep_structural() {
        let mut rng = SmallRng::seed_from_u64(0xB7EE5);

        for trial in 0..25 {
            let n0 = rng.random_range(200..1500);
            let mut reference: Vec<SlabAgg> = (0..n0)
                .map(|_| simple_agg(rand_total(&mut rng), rng.random_range(1..=4)))
                .collect();
            let mut tree = SlabBTree::<SlabAgg>::from_iter(reference.clone());

            for step in 0..300 {
                let len = reference.len();
                match rng.random_range(0..12) {
                    // small insert
                    0..=2 => {
                        let at = rng.random_range(0..=len);
                        let count = rng.random_range(1..=3);
                        let new: Vec<_> = (0..count)
                            .map(|_| simple_agg(rand_total(&mut rng), rng.random_range(1..=4)))
                            .collect();
                        tree.splice(at..at, new.clone());
                        reference.splice(at..at, new);
                    }
                    // bulk insert (forces splits + root growth)
                    3 => {
                        let at = rng.random_range(0..=len);
                        let count = rng.random_range(50..=200);
                        let new: Vec<_> = (0..count)
                            .map(|_| simple_agg(rand_total(&mut rng), rng.random_range(1..=4)))
                            .collect();
                        tree.splice(at..at, new.clone());
                        reference.splice(at..at, new);
                    }
                    // small delete
                    4..=6 if len > 0 => {
                        let at = rng.random_range(0..len);
                        let count = rng.random_range(1..=(len - at).min(3));
                        tree.splice(at..at + count, std::iter::empty());
                        reference.drain(at..at + count);
                    }
                    // large delete (crosses leaves / internal boundaries)
                    7 if len > 8 => {
                        let at = rng.random_range(0..len);
                        let count = rng.random_range(1..=(len - at).min(len / 4).max(1));
                        tree.splice(at..at + count, std::iter::empty());
                        reference.drain(at..at + count);
                    }
                    // 64-aligned delete (exact-span shapes)
                    8 if len > 64 => {
                        let at = (rng.random_range(0..len) / 64) * 64;
                        let count = 64.min(len - at);
                        tree.splice(at..at + count, std::iter::empty());
                        reference.drain(at..at + count);
                    }
                    // replace k with m
                    9 if len > 0 => {
                        let at = rng.random_range(0..len);
                        let del = rng.random_range(1..=(len - at).min(4));
                        let ins = rng.random_range(0..=4);
                        let new: Vec<_> = (0..ins)
                            .map(|_| simple_agg(rand_total(&mut rng), rng.random_range(1..=4)))
                            .collect();
                        tree.splice(at..at + del, new.clone());
                        reference.splice(at..at + del, new);
                    }
                    // update_slab
                    10 if len > 0 => {
                        let at = rng.random_range(0..len);
                        let new = simple_agg(rand_total(&mut rng), rng.random_range(1..=4));
                        tree.update_slab(at, new);
                        reference[at] = new;
                    }
                    // rare full wipe
                    11 if len > 0 && rng.random_range(0..20) == 0 => {
                        tree.splice(0..len, std::iter::empty());
                        reference.clear();
                    }
                    _ => {}
                }

                assert_eq!(tree.len(), reference.len(), "trial={trial} step={step}");
                tree.check_invariants();

                let total_items: usize = reference.iter().map(|a| a.len).sum();
                if total_items > 0 {
                    for _ in 0..2 {
                        let pos = rng.random_range(0..total_items);
                        assert_eq!(
                            tree.find_by_prefix(pos),
                            reference_find_by_prefix(&reference, pos),
                            "trial={trial} step={step}: find_by_prefix({pos})"
                        );
                    }
                }
                assert_eq!(
                    tree.root_agg(),
                    reference_root_agg(&reference),
                    "trial={trial} step={step}: root_agg"
                );
            }
        }
    }
}
