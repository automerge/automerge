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
//! aggregate is right, subtrees don't share nodes, and every grouping of
//! slabs can be merged (`valid`).  `find_by_prefix`, `items_before_slab`,
//! `root_agg` and `update_slab` are proved against that sequence;
//! `splice`, `from_iter` and the value/prefix queries are not yet.
//! `merge`'s laws are proved for `SlabAgg` and `usize`; for
//! `PrefixSlabWeight<P>` they are assumed of the user's `P`
//! (`axiom_prefix_add`).

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
/// ([`spec_merge`](Self::spec_merge)), defined wherever
/// [`merge_ok`](Self::merge_ok) holds (for fixed-width fields: no
/// overflow), with [`spec_empty`](Self::spec_empty) as its identity.  The
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

    /// Combine two adjacent subtree aggregates into one.
    fn merge(l: &Self, r: &Self) -> (m: Self)
        requires Self::merge_ok(*l, *r),
        ensures m == Self::spec_merge(*l, *r);

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
        SlabAgg {
            len: l.len + r.len,
            total: l.total + r.total,
            min_offset: l.min_offset.min(l.total + r.min_offset),
            max_offset: l.max_offset.max(l.total + r.max_offset),
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
        decreases h,
    {
        &&& valid(self.slabs(id, h))
        &&& self.slabs(id, h).len() <= usize::MAX
        &&& match self.node_spec(id) {
            Some(Node::Leaf(_)) => h == 0,
            Some(Node::Internal(n)) => h > 0 && n.children@.len() > 0 && (forall|i: int, j: int|
                0 <= i < j < n.children@.len() ==> #[trigger] self.owned(n.children@[i].id, (h - 1) as nat)
                    .disjoint(#[trigger] self.owned(n.children@[j].id, (h - 1) as nat)))
            && forall|i: int| 0 <= i < n.children@.len() ==> {
                let c = #[trigger] n.children@[i];
                &&& self.wf_node(c.id, (h - 1) as nat)
                &&& !self.owned(c.id, (h - 1) as nat).contains(id)
                &&& c.slab_count == self.slabs(c.id, (h - 1) as nat).len()
                &&& c.slab_count > 0
                &&& c.agg == fold(self.slabs(c.id, (h - 1) as nat))
                &&& c.agg.spec_len() == sum_len(self.slabs(c.id, (h - 1) as nat))
            },
            None => false,
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
            &&& c.agg == fold(self.slabs(c.id, h))
            &&& c.agg.spec_len() == sum_len(self.slabs(c.id, h))
        }
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
            n.children@[k].agg == fold(self.slabs(n.children@[k].id, (h - 1) as nat)),
            n.children@[k].agg.spec_len() == sum_len(self.slabs(n.children@[k].id, (h - 1) as nat)),
    {
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

    /// Merge a leaf's slab aggregates.
    fn fold_leaf(aggs: &[A]) -> (r: A)
        requires valid(aggs@),
        ensures r == fold(aggs@),
    {
        let mut acc = A::empty();
        let mut i: usize = 0;
        while i < aggs.len()
            invariant i <= aggs@.len(), valid(aggs@), acc == fold(aggs@.subrange(0, i as int)),
            decreases aggs@.len() - i,
        {
            proof {
                lemma_fold_one(aggs@, i as int);
                assert(A::merge_ok(fold(aggs@.subrange(0, i as int)), fold(aggs@.subrange(i as int, i + 1))));
                assert(aggs@.subrange(0, i + 1).drop_last() =~= aggs@.subrange(0, i as int));
            }
            acc = A::merge(&acc, &aggs[i]);
            i += 1;
        }
        assert(aggs@.subrange(0, i as int) =~= aggs@);
        acc
    }

    /// Merge an internal node's cached child aggregates.
    fn fold_children(&self, Ghost(id): Ghost<NodeId>, n: &Internal<A>, Ghost(h): Ghost<nat>) -> (r: A)
        requires self.wf_node(id, h), self.node_spec(id) == Some(Node::Internal(*n)),
        ensures r == fold(self.slabs(id, h)),
    {
        let ghost cs = n.children@;
        let ghost hc = (h - 1) as nat;
        let ghost all = self.children_slabs(cs, hc);
        proof {
            if cs.len() > 0 { self.lemma_child(id, h, *n, 0); } else { assert(self.wf_node(id, h)); }
        }
        let mut acc = A::empty();
        let mut k: usize = 0;
        while k < n.children.len()
            invariant
                k <= cs.len(), cs == n.children@, h > 0, hc == h - 1,
                self.wf_node(id, h), self.node_spec(id) == Some(Node::Internal(*n)),
                all == self.slabs(id, h), valid(all),
                acc == fold(self.children_slabs(cs.take(k as int), hc)),
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
                lemma_fold_split(all, 0, p, q);
                assert(A::merge_ok(fold(all.subrange(0, p)), fold(all.subrange(p, q))));
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

    /// Aggregate of all slabs under node `id`.
    fn subtree_agg(&self, id: NodeId, Ghost(h): Ghost<nat>) -> (r: A)
        requires self.wf_node(id, h),
        ensures r == fold(self.slabs(id, h)),
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
            valid(old(self)@.update(slab_idx as int, new_agg)),
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
            let v = self@;
            lemma_fold_len(v, 0, v.len() as int);
            assert(v.subrange(0, v.len() as int) =~= v);
            A::lemma_len_bound(fold(v));
        }
    }

    /// Replace slab `idx` of the subtree at `id` (height `h`), refreshing
    /// the cached child aggregates on the way back up.  Only the subtree's
    /// own nodes change.
    fn update_rec(&mut self, id: NodeId, Ghost(h): Ghost<nat>, idx: usize, new_agg: A)
        requires
            old(self).wf_node(id, h),
            idx < old(self).slabs(id, h).len(),
            valid(old(self).slabs(id, h).update(idx as int, new_agg)),
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
            lemma_valid_subrange(new_all, p, q);
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
                &&& c.agg == fold(t2.slabs(c.id, hc))
                &&& c.agg.spec_len() == sum_len(t2.slabs(c.id, hc))
            } by {
                t0.lemma_child(id, h, n0, j);
                if j == child_idx {
                    let cl = t2.slabs(cs2[j].id, hc);
                    assert(valid(cl));
                    lemma_fold_len(cl, 0, cl.len() as int);
                    assert(cl.subrange(0, cl.len() as int) =~= cl);
                }
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
    /// into a sibling if it drops below B/4); at the first node whose
    /// range spans several children, overwrite them in place if the
    /// lengths match and otherwise rebuild them at the same height.  On
    /// the way back up each level refreshes its cached slot, splits past
    /// B children and drops out when empty.  O(log n + k).
    #[verifier::external_body]
    fn splice_vec(&mut self, start: usize, end: usize, new: Vec<A>) {
        assert!(end <= self.total_slabs, "splice out of bounds");
        assert!(start <= end, "invalid range");
        if start == end && new.len() == 0 {
            return;
        }
        let added = new.len();
        let h = self.subtree_height(self.root);
        let slots = self.splice_rec(self.root, h, start, end, new);
        self.total_slabs = self.total_slabs + added - (end - start);
        self.set_root(slots);
        // Large deletions can leave a single-child chain at the top;
        // promote so query depth tracks the live tree.
        self.collapse_root();
    }

    /// Height of the subtree at `id` (leaves are 0).
    #[verifier::external_body]
    fn subtree_height(&self, id: NodeId) -> usize {
        match self.node(id) {
            Node::Leaf(_) => 0,
            Node::Internal(n) => 1 + self.subtree_height(n.children[0].id),
        }
    }

    /// Splice `start..end` ↦ `new` into the subtree at `id` (height `h`).
    /// Returns the child slots that replace the subtree's slot in its
    /// parent: none if it emptied (the caller frees `id`), one (always
    /// `id`'s own when the lengths match), or several if it split (the
    /// first keeps `id`).
    #[verifier::external_body]
    #[allow(clippy::vec_init_then_push)] // Verus has no `vec!`
    fn splice_rec(&mut self, id: NodeId, h: usize, start: usize, end: usize, new: Vec<A>) -> Vec<ChildSlot<A>> {
        let mut new = new;
        if h == 0 {
            let len = if let Node::Leaf(l) = self.node_mut(id) {
                if end - start == new.len() {
                    let mut i = new.len();
                    while i > 0 {
                        i -= 1;
                        l.aggs[start + i] = new.pop().unwrap();
                    }
                } else {
                    let mut tail = l.aggs.split_off(end);
                    l.aggs.truncate(start);
                    l.aggs.append(&mut new);
                    l.aggs.append(&mut tail);
                }
                l.aggs.len()
            } else {
                unreachable!()
            };
            if len == 0 {
                return Vec::new();
            }
            if len <= B {
                let agg = self.leaf_agg(id, Ghost(0));
                let mut out = Vec::new();
                out.push(ChildSlot { id, agg, slab_count: len });
                return out;
            }
            // Overflow: split into ⌈len/B⌉ leaves, the first keeping `id`.
            let mut all = Vec::new();
            if let Node::Leaf(l) = self.node_mut(id) {
                std::mem::swap(&mut all, &mut l.aggs);
            }
            return self.split_leaf(id, all);
        }

        // Which children hold `start` (or, appending, the last child) and
        // the last slab of the range, and where the first one starts.
        let (ks, ke, off_s) = {
            let n = match self.node(id) {
                Node::Internal(n) => n,
                Node::Leaf(_) => unreachable!(),
            };
            let last = n.children.len() - 1;
            let mut off = 0;
            let mut k = 0;
            while !(start < off + n.children[k].slab_count || k == last) {
                off += n.children[k].slab_count;
                k += 1;
            }
            let (ks, off_s) = (k, off);
            if end > start {
                while !(end - 1 < off + n.children[k].slab_count || k == last) {
                    off += n.children[k].slab_count;
                    k += 1;
                }
            }
            (ks, k, off_s)
        };

        if ks == ke {
            let child_id = match self.node(id) {
                Node::Internal(n) => n.children[ks].id,
                Node::Leaf(_) => unreachable!(),
            };
            let shrink = new.len() < end - start;
            let mut slots = self.splice_rec(child_id, h - 1, start - off_s, end - off_s, new);
            let merge_leaf = h == 1 && shrink && slots.len() == 1;
            if slots.len() == 0 {
                self.free(child_id);
            }
            if let Node::Internal(n) = self.node_mut(id) {
                let mut tail = n.children.split_off(ks + 1);
                n.children.truncate(ks);
                n.children.append(&mut slots);
                n.children.append(&mut tail);
            }
            if merge_leaf {
                self.maybe_merge_leaf(id, ks);
            }
        } else {
            // The range spans children ks..=ke.  Hand each its overlap and
            // the next piece of `new` of the same length (so most are
            // overwritten in place), the last one taking whatever is
            // left; then splice them last to first, so each one's piece
            // splits off the end of `new` and earlier indices stay put.
            let mut los: Vec<usize> = Vec::new();
            let mut his: Vec<usize> = Vec::new();
            let mut cuts: Vec<usize> = Vec::new();
            {
                let n = match self.node(id) {
                    Node::Internal(n) => n,
                    Node::Leaf(_) => unreachable!(),
                };
                let mut off = off_s;
                let mut used = 0;
                let mut j = ks;
                while j <= ke {
                    let count = n.children[j].slab_count;
                    let lo = if start > off { start - off } else { 0 };
                    let hi = if end - off < count { end - off } else { count };
                    los.push(lo);
                    his.push(hi);
                    cuts.push(used);
                    let want = if j == ke { new.len() - used } else { hi - lo };
                    used += if want < new.len() - used { want } else { new.len() - used };
                    off += count;
                    j += 1;
                }
            }
            let mut k = ke + 1;
            while k > ks {
                k -= 1;
                let child_id = match self.node(id) {
                    Node::Internal(n) => n.children[k].id,
                    Node::Leaf(_) => unreachable!(),
                };
                let part = new.split_off(cuts[k - ks]);
                let mut slots = self.splice_rec(child_id, h - 1, los[k - ks], his[k - ks], part);
                if slots.len() == 0 {
                    self.free(child_id);
                }
                if let Node::Internal(n) = self.node_mut(id) {
                    let mut tail = n.children.split_off(k + 1);
                    n.children.truncate(k);
                    n.children.append(&mut slots);
                    n.children.append(&mut tail);
                }
            }
        }

        let count = match self.node(id) {
            Node::Internal(n) => n.children.len(),
            Node::Leaf(_) => unreachable!(),
        };
        if count == 0 {
            return Vec::new();
        }
        if count <= B {
            let (agg, slab_count) = self.internal_summary(id, Ghost(h as nat));
            let mut out = Vec::new();
            out.push(ChildSlot { id, agg, slab_count });
            return out;
        }
        // Overflow: split into ⌈count/B⌉ internal nodes, the first
        // keeping `id`.
        let mut all = Vec::new();
        if let Node::Internal(n) = self.node_mut(id) {
            std::mem::swap(&mut all, &mut n.children);
        }
        self.split_internal(id, all, Ghost(h as nat))
    }




    /// After a leaf child shrank, merge it into an adjacent sibling when
    /// it is under a quarter full and the pair fits in one node — mirrors
    /// `Column::try_merge`'s (min = max/4, combined ≤ max) policy.
    ///
    /// Leaf-level only: internal nodes thin ~B× slower than leaves (they
    /// lose a child only when an entire leaf dies), so leaf merging is
    /// where fill degradation actually shows up.
    #[verifier::external_body]
    fn maybe_merge_leaf(&mut self, parent_id: NodeId, child_idx: usize) {
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
        let mut src_aggs = match self.nodes[src_id as usize].take() {
            Some(Node::Leaf(l)) => l.aggs,
            _ => unreachable!(),
        };
        self.free.push(src_id);
        if let Node::Leaf(l) = self.node_mut(dst_id) {
            l.aggs.append(&mut src_aggs);
        }
        let new_agg = self.leaf_agg(dst_id, Ghost(0));
        if let Node::Internal(p) = self.node_mut(parent_id) {
            p.children[dst_idx].agg = new_agg;
            p.children[dst_idx].slab_count = combined;
            p.children.remove(src_idx);
        }
    }








    /// Merge a leaf's slab aggregates.
    fn leaf_agg(&self, id: NodeId, Ghost(h): Ghost<nat>) -> (r: A)
        requires self.wf_node(id, h), self.node_spec(id) matches Some(Node::Leaf(_)),
        ensures r == fold(self.slabs(id, h)),
    {
        match self.node(id) {
            Node::Leaf(l) => Self::fold_leaf(&l.aggs),
            Node::Internal(_) => unreachable!(),
        }
    }

    /// An internal node's merged aggregate and slab count.
    fn internal_summary(&self, id: NodeId, Ghost(h): Ghost<nat>) -> (r: (A, usize))
        requires self.wf_node(id, h), self.node_spec(id) matches Some(Node::Internal(_)),
        ensures r.0 == fold(self.slabs(id, h)), r.1 == self.slabs(id, h).len(),
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
    fn chunk_start(c: usize, num: usize, base: usize, rem: usize) -> (r: usize)
        requires c <= num, rem < num, num * base + rem <= usize::MAX,
        ensures r == chunk_at(c as int, base as int, rem as int),
    {
        assert(c * base <= num * base) by (nonlinear_arith) requires c <= num;
        c * base + if c < rem { c } else { rem }
    }

    /// Split `aggs` into leaves (one per chunk), the first being
    /// `first_id`.
    fn split_leaf(&mut self, first_id: NodeId, aggs: Vec<A>) -> (r: Vec<ChildSlot<A>>)
        requires
            old(self).arena_ok(),
            old(self).node_spec(first_id) matches Some(Node::Leaf(_)),
            aggs@.len() > 0, valid(aggs@),
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
        let mut out: Vec<ChildSlot<A>> = Vec::new();
        proof { self.lemma_slots_nil(0); }
        let mut c = num;
        while c > 1
            invariant
                1 <= c <= num, rem < num, num * base + rem == n, n <= usize::MAX, b == base, r == rem,
                base >= 1, chunk_at(num as int, b, r) == n, chunk_at(0, b, r) == 0,
                valid(all), all.len() == n,
                aggs@ == all.subrange(0, chunk_at(c as int, b, r)),
                self.arena_ok(),
                self.node_spec(first_id) matches Some(Node::Leaf(_)),
                self.slots_ok(out@, 0),
                self.children_slabs(out@, 0) == all.subrange(chunk_at(c as int, b, r), n),
                forall|x: NodeId| #[trigger] self.children_owned(out@, 0).contains(x) ==> old(self).node_spec(x) is None,
                forall|x: NodeId| x != first_id && old(self).node_spec(x) is Some
                    ==> #[trigger] self.node_spec(x) == old(self).node_spec(x),
                self.root == old(self).root,
                self.total_slabs == old(self).total_slabs,
            decreases c,
        {
            c -= 1;
            let ghost (lo, hi) = (chunk_at(c as int, b, r), chunk_at(c + 1, b, r));
            proof { lemma_chunk(num as int, b, r, c as int); }
            let at = Self::chunk_start(c, num, base, rem);
            let chunk = aggs.split_off(at);
            proof {
                assert(chunk@ =~= all.subrange(lo, hi));
                lemma_valid_subrange(all, lo, hi);
            }
            let slab_count = chunk.len();
            let agg = Self::fold_leaf(&chunk);
            let ghost t0 = *self;
            let ghost leaf = Leaf { aggs: chunk };
            let id = self.alloc(Node::Leaf(Leaf { aggs: chunk }));
            let slot = ChildSlot { id, agg, slab_count };
            proof {
                if t0.children_owned(out@, 0).contains(id) { t0.lemma_slots_live(out@, 0, id); }
                Self::lemma_slots_keep(&t0, self, out@, 0, set![id]);
                self.lemma_wf_leaf(id, 0, leaf);
                self.lemma_slot_one(slot, 0);
                self.lemma_slots_prepend(slot, out@, 0);
                assert(all.subrange(lo, n) =~= all.subrange(lo, hi) + all.subrange(hi, n));
            }
            out.insert(0, slot);
        }
        let ghost t0 = *self;
        let ghost hi = chunk_at(1, b, r);
        proof { lemma_chunk(num as int, b, r, 0); }
        let slab_count = aggs.len();
        proof { lemma_valid_subrange(all, 0, hi); }
        let agg = Self::fold_leaf(&aggs);
        let ghost leaf = Leaf { aggs: aggs };
        let node = self.node_mut(first_id);
        if let Node::Leaf(l) = node {
            l.aggs = aggs;
        }
        let slot = ChildSlot { id: first_id, agg, slab_count };
        proof {
            assert(self.node_spec(first_id) == Some(Node::Leaf(leaf)));
            if t0.children_owned(out@, 0).contains(first_id) { t0.lemma_slots_live(out@, 0, first_id); }
            Self::lemma_slots_keep(&t0, self, out@, 0, set![first_id]);
            self.lemma_wf_leaf(first_id, 0, leaf);
            self.lemma_slot_one(slot, 0);
            self.lemma_slots_prepend(slot, out@, 0);
            assert(all =~= all.subrange(0, hi) + all.subrange(hi, n));
            assert forall|x: NodeId| x != first_id && old(self).node_spec(x) is Some
                implies #[trigger] self.node_spec(x) == old(self).node_spec(x) by {
                assert(self.node_spec(x) == t0.node_spec(x));
            }
        }
        out.insert(0, slot);
        out
    }

    /// Split `children` (slots one level down) into internal nodes (one
    /// per chunk), the first being `first_id`.
    fn split_internal(&mut self, first_id: NodeId, children: Vec<ChildSlot<A>>, Ghost(h): Ghost<nat>) -> (r: Vec<ChildSlot<A>>)
        requires
            h > 0,
            old(self).arena_ok(),
            old(self).node_spec(first_id) matches Some(Node::Internal(_)),
            children@.len() > 0,
            old(self).slots_ok(children@, (h - 1) as nat),
            !old(self).children_owned(children@, (h - 1) as nat).contains(first_id),
            valid(old(self).children_slabs(children@, (h - 1) as nat)),
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
        let mut out: Vec<ChildSlot<A>> = Vec::new();
        proof {
            self.lemma_slots_nil(h);
            assert forall|x: NodeId| #[trigger] owned_all.contains(x) implies old(self).node_spec(x) is Some by {
                old(self).lemma_slots_live(all, hc, x);
            }
            assert(all.subrange(chunk_at(num as int, b, r), n) =~= Seq::<ChildSlot<A>>::empty());
        }
        let mut c = num;
        while c > 1
            invariant
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
                valid(slabs_all), slabs_all.len() <= usize::MAX,
                forall|x: NodeId| #[trigger] owned_all.contains(x) ==> old(self).node_spec(x) is Some,
                self.slots_ok(out@, h),
                out@.len() == num - c,
                self.children_slabs(out@, h) == self.children_slabs(all.subrange(chunk_at(c as int, b, r), n), hc),
                forall|x: NodeId| #[trigger] self.children_owned(out@, h).contains(x)
                    ==> old(self).node_spec(x) is None
                        || self.children_owned(all.subrange(chunk_at(c as int, b, r), n), hc).contains(x),
                forall|x: NodeId| x != first_id && old(self).node_spec(x) is Some
                    ==> #[trigger] self.node_spec(x) == old(self).node_spec(x),
                self.root == old(self).root,
                self.total_slabs == old(self).total_slabs,
            decreases c,
        {
            c -= 1;
            let ghost (lo, hi) = (chunk_at(c as int, b, r), chunk_at(c + 1, b, r));
            proof { lemma_chunk(num as int, b, r, c as int); }
            let at = Self::chunk_start(c, num, base, rem);
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
                lemma_valid_subrange(slabs_all, p, q);
            }
            let id = self.alloc(Node::Internal(Internal { children: chunk }));
            proof {
                assert(!owned_all.contains(id));
                if t0.children_owned(out@, h).contains(id) {
                    t0.lemma_slots_live(out@, h, id);
                }
                Self::lemma_slots_keep(&t0, self, all, hc, set![id]);
                Self::lemma_slots_keep(&t0, self, out@, h, set![id]);
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
                self.lemma_slot_one(slot, h);
                assert(self.owned(slot.id, h).disjoint(self.children_owned(out@, h))) by {
                    assert forall|x: NodeId| self.owned(id, h).contains(x) implies
                        !self.children_owned(out@, h).contains(x) by {
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
                self.lemma_slots_prepend(slot, out@, h);
                // what the list now stands for: this chunk and the rest
                t0.lemma_slots_sub(all, hi, n, hc);
                Self::lemma_slots_keep(&t0, self, rest, hc, set![id]);
                self.lemma_children_slabs_concat(chunk@, rest, hc);
                self.lemma_children_owned_concat(chunk@, rest, hc);
                assert(id != first_id);
                assert(old(self).node_spec(id) is None);
            }
            out.insert(0, slot);
        }
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
            lemma_valid_subrange(slabs_all, 0, p);
        }
        let ghost node = Internal { children: children };
        let nd = self.node_mut(first_id);
        if let Node::Internal(nd) = nd {
            nd.children = children;
        }
        proof {
            assert(self.node_spec(first_id) == Some(Node::Internal(node)));
            if t0.children_owned(out@, h).contains(first_id) {
                assert(old(self).node_spec(first_id) is Some);
            }
            Self::lemma_slots_keep(&t0, self, out@, h, set![first_id]);
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
            self.lemma_slot_one(slot, h);
            assert(self.owned(first_id, h).disjoint(self.children_owned(out@, h))) by {
                assert forall|x: NodeId| self.owned(first_id, h).contains(x) implies
                    !self.children_owned(out@, h).contains(x) by {
                    if x != first_id {
                        assert(self.children_owned(first, hc).contains(x));
                        assert(owned_all.contains(x));
                    }
                }
            };
            self.lemma_slots_prepend(slot, out@, h);
            self.lemma_children_slabs_concat(first, rest, hc);
            self.lemma_children_owned_concat(first, rest, hc);
            assert forall|x: NodeId| x != first_id && old(self).node_spec(x) is Some
                implies #[trigger] self.node_spec(x) == old(self).node_spec(x) by {
                assert(self.node_spec(x) == t0.node_spec(x));
            }
        }
        out.insert(0, slot);
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
            valid(old(self).children_slabs(slots@, (h - 1) as nat)),
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

    /// Install the slots a splice returned for the root.
    #[verifier::external_body]
    fn set_root(&mut self, slots: Vec<ChildSlot<A>>) {
        let mut slots = slots;
        if slots.len() == 0 {
            // The whole tree was deleted: reset to a fresh empty leaf,
            // reusing the root id.
            self.nodes[self.root as usize] = Some(Node::Leaf(Leaf { aggs: Vec::new() }));
            return;
        }
        // Grow new root levels while there are too many for one node.
        while slots.len() > B {
            slots = self.group(slots, Ghost(0));
        }
        if slots.len() == 1 {
            let id = slots[0].id;
            if id != self.root {
                self.free(self.root);
                self.root = id;
            }
            return;
        }
        self.root = self.alloc(Node::Internal(Internal { children: slots }));
    }

    /// While the root is an internal node with exactly one child, promote
    /// the child.  Keeps query depth proportional to the live tree after
    /// large deletions.  Called once per structural mutation.
    #[verifier::external_body]
    fn collapse_root(&mut self) {
        loop {
            let child = match self.node(self.root) {
                Node::Internal(n) => {
                    if n.children.len() == 1 {
                        n.children[0].id
                    } else {
                        return;
                    }
                }
                Node::Leaf(_) => return,
            };
            self.free(self.root);
            self.root = child;
        }
    }

    /// Aggregate of all slabs in the tree.
    #[allow(dead_code)]
    pub(crate) fn root_agg(&self) -> (r: A)
        requires self.wf(),
        ensures r == fold(self@),
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
                &&& valid(self.children_slabs(n.children@, (h - 1) as nat))
                &&& self.children_slabs(n.children@, (h - 1) as nat).len() <= usize::MAX
            },
            h > 0 ==> self.slabs(id, h) == self.children_slabs(n.children@, (h - 1) as nat),
            h > 0 ==> self.owned(id, h) == set![id] + self.children_owned(n.children@, (h - 1) as nat),
    {
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
                    &&& c.agg == fold(self.slabs(c.id, hc))
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
            self.wf_node(id, h) == (h == 0 && valid(l.aggs@) && l.aggs@.len() <= usize::MAX),
            self.slabs(id, h) == l.aggs@,
            self.owned(id, h) == set![id],
    {
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
            &&& c.agg == fold(self.slabs(c.id, h))
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
            &&& c.agg == fold(self.slabs(c.id, h))
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
            cs[i].agg == fold(self.slabs(cs[i].id, h)),
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
            slot.agg == fold(self.slabs(slot.id, h)),
        ensures
            self.slots_ok(seq![slot], h),
            self.children_slabs(seq![slot], h) == self.slabs(slot.id, h),
            self.children_owned(seq![slot], h) == self.owned(slot.id, h),
    {
        reveal(SlabBTree::slots_ok);
        let s = self.slabs(slot.id, h);
        lemma_fold_len(s, 0, s.len() as int);
        assert(s.subrange(0, s.len() as int) =~= s);
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

/// Aggregate that additionally carries a prefix-sum value per slab,
/// enabling [`find_slab_at_item`](SlabBTree::find_slab_at_item) and
/// [`find_slab_at_prefix`](SlabBTree::find_slab_at_prefix) to work
/// aggregate-agnostically.
///
/// Implemented by [`PrefixSlabWeight<P>`](crate::prefix::PrefixSlabWeight)
/// (prefix lives in `.prefix`) and [`SlabAgg`] (prefix lives in `.total`).
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
    /// Borrow-typed accessor — the B-tree descent reads prefixes many
    /// times per query and aggregates by reference, so cloning here would
    /// be wasted work for non-scalar prefixes.
    fn prefix(&self) -> &Self::Prefix;
}

fn merge_all<'a, A: SlabAggregate + 'a>(aggs: impl IntoIterator<Item = &'a A>) -> A {
    let mut acc = A::default();
    for a in aggs {
        acc = A::merge(&acc, a);
    }
    acc
}

// ── SlabAgg (used by DeltaColumn) ───────────────────────────────────────────

impl PrefixAggregate for SlabAgg {
    type Prefix = i64;
    fn prefix(&self) -> &i64 {
        &self.total
    }
}

impl SlabAgg {
    fn contains(&self, target: i64, prefix_before: i64) -> bool {
        self.len > 0
            && target >= prefix_before + self.min_offset
            && target <= prefix_before + self.max_offset
    }

    fn overlaps(&self, lo: i64, hi: i64, prefix_before: i64) -> bool {
        self.len > 0
            && prefix_before + self.max_offset >= lo
            && prefix_before + self.min_offset <= hi
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
    fn prefix(&self) -> &P {
        &self.prefix
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
        let aggs: Vec<A> = iter.into_iter().collect();
        let mut tree = Self::new();
        if aggs.is_empty() {
            return tree;
        }
        tree.total_slabs = aggs.len();

        let mut level: Vec<(NodeId, A, usize)> = aggs
            .chunks(B)
            .map(|chunk| {
                let agg = merge_all(chunk);
                let slab_count = chunk.len();
                let leaf = Leaf {
                    aggs: chunk.to_vec(),
                };
                let id = tree.alloc(Node::Leaf(leaf));
                (id, agg, slab_count)
            })
            .collect();

        while level.len() > 1 {
            level = level
                .chunks(B)
                .map(|chunk| {
                    let children: Vec<ChildSlot<A>> = chunk
                        .iter()
                        .map(|(id, agg, slab_count)| ChildSlot {
                            id: *id,
                            agg: agg.clone(),
                            slab_count: *slab_count,
                        })
                        .collect();
                    let agg = merge_all(children.iter().map(|c| &c.agg));
                    let slab_count = children.iter().map(|c| c.slab_count).sum();
                    let id = tree.alloc(Node::Internal(Internal { children }));
                    (id, agg, slab_count)
                })
                .collect();
        }

        let (root_id, _, _) = level[0];
        tree.free(tree.root);
        tree.root = root_id;
        tree
    }
}

// ── Structural invariant checker (test builds only) ─────────────────────────

#[cfg(test)]
impl<A: SlabAggregate> SlabBTree<A> {
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
        FindByValue::new(self, target)
    }

    pub(crate) fn find_by_value_range(&self, lo: i64, hi: i64) -> FindByValueRange<'_> {
        FindByValueRange::new(self, lo, hi)
    }
}

pub struct FindByValue<'a> {
    tree: &'a SlabBTree<SlabAgg>,
    target: i64,
    stack: Vec<Frame>,
}

#[derive(Copy, Clone)]
struct Frame {
    node: NodeId,
    next_child: usize,
    prefix_before: i64,
    slab_base: usize,
    items_before: usize,
}

impl<'a> FindByValue<'a> {
    fn new(tree: &'a SlabBTree<SlabAgg>, target: i64) -> Self {
        let mut s = Self {
            tree,
            target,
            stack: Vec::with_capacity(16),
        };
        s.stack.push(Frame {
            node: tree.root,
            next_child: 0,
            prefix_before: 0,
            slab_base: 0,
            items_before: 0,
        });
        s
    }
}

impl Iterator for FindByValue<'_> {
    /// `(slab_idx, items_before_slab, prefix_before_slab)`.
    type Item = (usize, usize, i64);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(frame) = self.stack.last_mut() {
            match self.tree.node(frame.node) {
                Node::Leaf(l) => {
                    while frame.next_child < l.aggs.len() {
                        let i = frame.next_child;
                        let agg = &l.aggs[i];
                        let prefix = frame.prefix_before;
                        let slab_idx = frame.slab_base + i;
                        let items_before = frame.items_before;
                        frame.next_child += 1;
                        frame.prefix_before += agg.total;
                        frame.items_before += agg.len;
                        if agg.contains(self.target, prefix) {
                            return Some((slab_idx, items_before, prefix));
                        }
                    }
                    self.stack.pop();
                }
                Node::Internal(n) => {
                    if frame.next_child >= n.children.len() {
                        self.stack.pop();
                        continue;
                    }
                    let child = n.children[frame.next_child];
                    let prefix = frame.prefix_before;
                    let slab_base = frame.slab_base;
                    let items_before = frame.items_before;
                    frame.next_child += 1;
                    frame.prefix_before += child.agg.total;
                    frame.slab_base += child.slab_count;
                    frame.items_before += child.agg.len;
                    if child.agg.contains(self.target, prefix) {
                        self.stack.push(Frame {
                            node: child.id,
                            next_child: 0,
                            prefix_before: prefix,
                            slab_base,
                            items_before,
                        });
                    }
                }
            }
        }
        None
    }
}

pub struct FindByValueRange<'a> {
    tree: &'a SlabBTree<SlabAgg>,
    lo: i64,
    hi: i64,
    stack: Vec<Frame>,
}

impl<'a> FindByValueRange<'a> {
    fn new(tree: &'a SlabBTree<SlabAgg>, lo: i64, hi: i64) -> Self {
        let mut s = Self {
            tree,
            lo,
            hi,
            stack: Vec::with_capacity(16),
        };
        s.stack.push(Frame {
            node: tree.root,
            next_child: 0,
            prefix_before: 0,
            slab_base: 0,
            items_before: 0,
        });
        s
    }
}

impl Iterator for FindByValueRange<'_> {
    /// `(slab_idx, items_before_slab, prefix_before_slab)`.
    type Item = (usize, usize, i64);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(frame) = self.stack.last_mut() {
            match self.tree.node(frame.node) {
                Node::Leaf(l) => {
                    while frame.next_child < l.aggs.len() {
                        let i = frame.next_child;
                        let agg = &l.aggs[i];
                        let prefix = frame.prefix_before;
                        let slab_idx = frame.slab_base + i;
                        let items_before = frame.items_before;
                        frame.next_child += 1;
                        frame.prefix_before += agg.total;
                        frame.items_before += agg.len;
                        if agg.overlaps(self.lo, self.hi, prefix) {
                            return Some((slab_idx, items_before, prefix));
                        }
                    }
                    self.stack.pop();
                }
                Node::Internal(n) => {
                    if frame.next_child >= n.children.len() {
                        self.stack.pop();
                        continue;
                    }
                    let child = n.children[frame.next_child];
                    let prefix = frame.prefix_before;
                    let slab_base = frame.slab_base;
                    let items_before = frame.items_before;
                    frame.next_child += 1;
                    frame.prefix_before += child.agg.total;
                    frame.slab_base += child.slab_count;
                    frame.items_before += child.agg.len;
                    if child.agg.overlaps(self.lo, self.hi, prefix) {
                        self.stack.push(Frame {
                            node: child.id,
                            next_child: 0,
                            prefix_before: prefix,
                            slab_base,
                            items_before,
                        });
                    }
                }
            }
        }
        None
    }
}

// ── Prefix-sum queries (generic over PrefixAggregate) ───────────────────────

#[derive(Clone, PartialEq, Debug)]
pub(crate) struct FoundSlab<A: PrefixAggregate> {
    pub(crate) index: usize,
    pub(crate) prefix: A::Prefix,
    pub(crate) pos: usize,
}

impl<A: PrefixAggregate> FoundSlab<A> {
    #[cfg(test)]
    pub(crate) fn decompose(&self) -> (usize, A::Prefix, usize) {
        (self.index, self.prefix.clone(), self.pos)
    }
}

impl<A: PrefixAggregate> SlabBTree<A> {
    /// Find the landing slab for `item_idx`: returns
    /// `(slab_idx, prefix_before_slab, items_before_slab)`.  The
    /// landing slab is the one containing item `item_idx`; if
    /// `item_idx >= total_items` returns the final position with the
    /// full prefix/items totals.
    ///
    /// O(log n) — descends the tree by `len`, accumulating `prefix`
    /// and `len` of children visited before the descent.
    pub(crate) fn find_slab_at_item(&self, item_idx: usize) -> FoundSlab<A> {
        let mut prefix = A::Prefix::default();
        let mut pos = 0usize;
        let mut slab_base = 0usize;
        let mut remaining = item_idx;
        let mut node = self.root;
        loop {
            match self.node(node) {
                Node::Leaf(l) => {
                    for (i, a) in l.aggs.iter().enumerate() {
                        let l_len = a.len();
                        if remaining < l_len {
                            return FoundSlab {
                                index: slab_base + i,
                                prefix,
                                pos,
                            };
                        }
                        prefix += a.prefix();
                        pos += l_len;
                        remaining -= l_len;
                    }
                    return FoundSlab {
                        index: slab_base + l.aggs.len(),
                        prefix,
                        pos,
                    };
                }
                Node::Internal(n) => {
                    let mut descended = false;
                    for c in &n.children {
                        let c_len = c.agg.len();
                        if remaining < c_len {
                            node = c.id;
                            descended = true;
                            break;
                        }
                        prefix += c.agg.prefix();
                        pos += c_len;
                        slab_base += c.slab_count;
                        remaining -= c_len;
                    }
                    if !descended {
                        return FoundSlab {
                            index: slab_base,
                            prefix,
                            pos,
                        };
                    }
                }
            }
        }
    }
}

impl<A: PrefixAggregate> SlabBTree<A>
where
    A::Prefix: Ord,
{
    /// Find the slab whose prefix sum first reaches or exceeds `target`.
    /// Returns `(slab_idx, prefix_before_slab, items_before_slab)`.  If
    /// `target` exceeds the total prefix, returns one-past-the-end.
    ///
    /// O(log n) — descends by comparing `target` against running prefix
    /// sums of sibling subtrees.
    pub(crate) fn find_slab_at_prefix(&self, target: A::Prefix) -> (usize, A::Prefix, usize) {
        let mut acc_prefix = A::Prefix::default();
        let mut acc_items = 0usize;
        let mut slab_base = 0usize;
        let mut node = self.root;
        loop {
            match self.node(node) {
                Node::Leaf(l) => {
                    for (i, a) in l.aggs.iter().enumerate() {
                        let mut after = acc_prefix.clone();
                        after += a.prefix();
                        if target <= after {
                            return (slab_base + i, acc_prefix, acc_items);
                        }
                        acc_prefix = after;
                        acc_items += a.len();
                    }
                    return (slab_base + l.aggs.len(), acc_prefix, acc_items);
                }
                Node::Internal(n) => {
                    let mut descended = false;
                    for c in &n.children {
                        let mut after = acc_prefix.clone();
                        after += c.agg.prefix();
                        if target <= after {
                            node = c.id;
                            descended = true;
                            break;
                        }
                        acc_prefix = after;
                        acc_items += c.agg.len();
                        slab_base += c.slab_count;
                    }
                    if !descended {
                        return (slab_base, acc_prefix, acc_items);
                    }
                }
            }
        }
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
        assert_eq!(tree.find_slab_at_prefix(10), (0, 0, 0));
        // Target 15: crosses into slab 1 (0+10 < 15 <= 0+10+20=30).
        assert_eq!(tree.find_slab_at_prefix(15), (1, 10, 5));
        // Target 100: prefix_before slab 3 = 60; slab 3 ends at 100; target=100 ≤ 100 → slab 3.
        assert_eq!(tree.find_slab_at_prefix(100), (3, 60, 15));
        // Target past total (sum = 550): one-past-end.
        assert_eq!(tree.find_slab_at_prefix(9999), (10, 550, 50));
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
                        let got = tree.find_slab_at_prefix(target);
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
                    let (si, prefix_before, items_before) = tree.find_slab_at_prefix(target);
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
        assert_eq!(t.subtree_height(t.root), 2);
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
        assert_eq!(
            t.subtree_height(t.root),
            0,
            "root should collapse to the leaf"
        );
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
        assert_eq!(t.subtree_height(t.root), 0);
        t.check_invariants();
    }

    #[test]
    fn root_collapses_after_mass_delete() {
        let n = 64 * 64 + 5;
        let aggs: Vec<_> = (0..n).map(|i| simple_agg(i as i64, 1)).collect();
        let mut t = SlabBTree::<SlabAgg>::from_iter(aggs);
        assert_eq!(t.subtree_height(t.root), 2);
        t.splice(5..n, std::iter::empty());
        assert_eq!(t.len(), 5);
        assert!(
            t.subtree_height(t.root) <= 1,
            "height {} for 5 slabs",
            t.subtree_height(t.root)
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
