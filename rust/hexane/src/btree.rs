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

/// Branching factor.  Smaller = deeper tree + cheaper per-node rebalance;
/// larger = fewer nodes + more per-node cost.  64 is empirically a good
/// compromise across the `marks_at` (query-heavy) and `insert_null`
/// (mutation-heavy) workloads.
const B: usize = 64;

// ── SlabAggregate trait ─────────────────────────────────────────────────────

verus! {

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

    pub closed spec fn height(&self) -> nat {
        choose|h: nat| self.wf_node(self.root, h)
    }

    /// The tree invariant.
    pub closed spec fn wf(&self) -> bool {
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

    fn alloc(&mut self, node: Node<A>) -> NodeId {
        if let Some(id) = self.free.pop() {
            self.nodes[id as usize] = Some(node);
            id
        } else {
            let id = self.nodes.len() as NodeId;
            self.nodes.push(Some(node));
            id
        }
    }

    fn free(&mut self, id: NodeId) {
        self.nodes[id as usize] = None;
        self.free.push(id);
    }

    /// Aggregate of all slabs under node `id`.
    fn agg_of(&self, id: NodeId) -> A {
        match self.node(id) {
            Node::Leaf(l) => merge_all(&l.aggs),
            Node::Internal(n) => merge_all(n.children.iter().map(|c| &c.agg)),
        }
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

    // ── Splice ──────────────────────────────────────────────────────────

    pub(crate) fn splice<I: IntoIterator<Item = A>>(&mut self, range: Range<usize>, new_aggs: I) {
        assert!(range.end <= self.total_slabs, "splice out of bounds");
        assert!(range.start <= range.end, "invalid range");
        let new: Vec<A> = new_aggs.into_iter().collect();

        if range.is_empty() && new.is_empty() {
            return;
        }

        // Decompose into overwrite + structural delta.  When the number
        // of removed and inserted aggs match (the common case for
        // replace-style mutations), the structural delta is empty and
        // the overwrite is a pure in-place rewrite with zero allocation.
        let common = range.len().min(new.len());
        if common > 0 {
            self.overwrite_range(range.start, &new[..common]);
        }

        let delta_start = range.start + common;
        let delta_range = delta_start..range.end;
        let delta_new = &new[common..];
        if delta_range.is_empty() && delta_new.is_empty() {
            return;
        }

        self.splice_structural(&delta_range, delta_new);
    }

    /// Overwrite existing agg values in-place without changing tree
    /// structure.  Walks across leaf boundaries as needed.  O(k + log n).
    fn overwrite_range(&mut self, start: usize, new: &[A]) {
        let mut cursor = start;
        let mut i = 0;
        while i < new.len() {
            let Some((path, leaf_id, leaf_start)) = self.locate_leaf_for_start(cursor) else {
                return;
            };
            let leaf_len = match self.node(leaf_id) {
                Node::Leaf(l) => l.aggs.len(),
                _ => unreachable!(),
            };
            let local_start = cursor - leaf_start;
            let count = (leaf_len - local_start).min(new.len() - i);
            if count == 0 {
                // Only possible if cached slab counts are inconsistent
                // (cursor landed on a leaf boundary mid-overwrite) —
                // bail rather than loop forever.
                debug_assert!(false, "overwrite_range: zero-progress step");
                return;
            }
            if let Node::Leaf(l) = self.node_mut(leaf_id) {
                l.aggs[local_start..local_start + count].clone_from_slice(&new[i..i + count]);
            }
            self.update_ancestor_aggregates(&path, leaf_id);
            cursor += count;
            i += count;
        }
    }

    /// Handle the structural part of a splice (the delta after the common
    /// prefix has been overwritten).  Usually tiny (0-2 items).
    fn splice_structural(&mut self, range: &Range<usize>, new: &[A]) {
        if self.total_slabs == 0 {
            self.splice_into_empty(new);
        } else if !self.try_single_leaf_splice(range, new) {
            self.splice_via_lca(range, new);
        }
        // Large deletions can leave a single-child chain at the top;
        // promote so query depth tracks the live tree.
        self.collapse_root();
    }

    /// General multi-leaf splice via the lowest common ancestor (LCA).
    ///
    /// Locates the LCA of the start and end positions, collects
    /// aggregates from the affected subtrees, applies the splice, builds
    /// replacement subtrees at the correct height, and replaces the
    /// LCA's child span.  O(log n + k) — no full-tree rebuild.
    fn splice_via_lca(&mut self, range: &Range<usize>, new: &[A]) {
        let Some((start_path, _start_leaf, _)) = self.locate_leaf_for_start(range.start) else {
            return;
        };
        let end_query = if range.is_empty() {
            range.start
        } else {
            range.end - 1
        };
        let Some((end_path, _end_leaf, _)) = self.locate_leaf_for_start(end_query) else {
            return;
        };

        // Find the divergence point — where start and end paths go to
        // different children of the same node.  That node is the LCA.
        let diverge = start_path
            .iter()
            .zip(end_path.iter())
            .position(|(a, b)| a.1 != b.1);
        let Some(d) = diverge else {
            // Paths identical — the range lies within a single leaf, which
            // `try_single_leaf_splice` handles for every case (including
            // whole-leaf deletion, via `remove_and_cascade`).  Defensive
            // fallback: rebuild rather than corrupt, but flag in debug —
            // reaching this is a routing bug.
            debug_assert!(
                false,
                "splice_via_lca: single-leaf range should be handled by try_single_leaf_splice"
            );
            let mut all: Vec<A> = Vec::with_capacity(self.total_slabs + new.len() - range.len());
            self.collect_into(self.root, &mut all);
            all.splice(range.clone(), new.iter().cloned());
            *self = Self::from_iter(all);
            return;
        };

        debug_assert_eq!(start_path[d].0, end_path[d].0);
        let lca_id = start_path[d].0;
        let left_child_idx = start_path[d].1;
        let right_child_idx = end_path[d].1;
        let lca_path = &start_path[..d];

        // Compute the absolute slab offset of LCA.children[left_child_idx].
        let mut lca_abs_offset: usize = 0;
        for &(ancestor_id, child_idx) in lca_path {
            let n = match self.node(ancestor_id) {
                Node::Internal(n) => n,
                _ => unreachable!(),
            };
            lca_abs_offset += n.children[..child_idx]
                .iter()
                .map(|c| c.slab_count)
                .sum::<usize>();
        }
        let left_child_offset: usize = {
            let n = match self.node(lca_id) {
                Node::Internal(n) => n,
                _ => unreachable!(),
            };
            n.children[..left_child_idx]
                .iter()
                .map(|c| c.slab_count)
                .sum()
        };
        let combined_abs_start = lca_abs_offset + left_child_offset;

        // Determine height of the affected children.
        let child_height = {
            let n = match self.node(lca_id) {
                Node::Internal(n) => n,
                _ => unreachable!(),
            };
            self.subtree_height(n.children[left_child_idx].id)
        };

        // Collect aggregates from the affected subtrees + free them.
        let child_ids: Vec<NodeId> = {
            let n = match self.node(lca_id) {
                Node::Internal(n) => n,
                _ => unreachable!(),
            };
            n.children[left_child_idx..=right_child_idx]
                .iter()
                .map(|c| c.id)
                .collect()
        };
        let mut combined: Vec<A> = Vec::new();
        for &cid in &child_ids {
            self.collect_into(cid, &mut combined);
        }
        for &cid in &child_ids {
            self.free_subtree(cid);
        }

        // Apply the splice in local coordinates.
        let local_start = range.start - combined_abs_start;
        let local_end = range.end - combined_abs_start;
        debug_assert!(local_end <= combined.len());
        combined.splice(local_start..local_end, new.iter().cloned());

        self.total_slabs = self.total_slabs + new.len() - range.len();

        // Build fresh subtrees at the same height as the old ones.
        let new_children = self.build_children_at_height(&combined, child_height);

        // Replace child span in LCA.
        let new_lca_len = {
            let n = match self.node_mut(lca_id) {
                Node::Internal(n) => n,
                _ => unreachable!(),
            };
            n.children
                .splice(left_child_idx..=right_child_idx, new_children);
            n.children.len()
        };

        // Propagate up.
        let lca_path_vec = lca_path.to_vec();
        if new_lca_len == 0 {
            // Every child of the LCA was deleted (`new` must be empty for
            // the combined span to vanish).  An empty internal node is not
            // allowed: remove the LCA from its parent and cascade.  A root
            // LCA means the whole tree was deleted — reset to an empty
            // leaf root.
            debug_assert!(new.is_empty());
            if lca_path_vec.is_empty() {
                debug_assert_eq!(self.total_slabs, 0);
                debug_assert_eq!(lca_id, self.root);
                self.nodes[lca_id as usize] = Some(Node::Leaf(Leaf::default()));
            } else {
                self.remove_and_cascade(lca_path_vec);
            }
        } else if new_lca_len <= B {
            let (agg, count) = self.internal_summary(lca_id);
            let replacement = vec![ChildSlot {
                id: lca_id,
                agg,
                slab_count: count,
            }];
            self.propagate_splits(lca_path_vec, replacement);
        } else {
            self.split_and_propagate(lca_id, lca_path_vec);
        }
    }

    fn free_subtree(&mut self, id: NodeId) {
        let child_ids: Vec<NodeId> = match self.node(id) {
            Node::Internal(n) => n.children.iter().map(|c| c.id).collect(),
            Node::Leaf(_) => Vec::new(),
        };
        for cid in child_ids {
            self.free_subtree(cid);
        }
        self.free(id);
    }

    // ── Underflow handling ──────────────────────────────────────────────

    /// Remove the child slot at `path.last()` from its parent, freeing the
    /// child's subtree.  If the parent becomes empty, cascade the removal
    /// upward (empty non-root nodes are not allowed); surviving ancestors'
    /// cached aggregates and slab counts are refreshed.  An emptied root is
    /// reset to a fresh leaf so the tree returns to its empty state.
    ///
    /// `path` is an ancestor path as produced by `locate_leaf_for_start`:
    /// `path[i] = (ancestor_id, child_idx_taken)`, ending with the parent
    /// of the node being removed.
    fn remove_and_cascade(&mut self, mut path: Vec<(NodeId, usize)>) {
        while let Some((parent_id, child_idx)) = path.pop() {
            let child_id = match self.node(parent_id) {
                Node::Internal(n) => n.children[child_idx].id,
                Node::Leaf(_) => unreachable!("leaf can't be a parent"),
            };
            self.free_subtree(child_id);
            let remaining = match self.node_mut(parent_id) {
                Node::Internal(p) => {
                    p.children.remove(child_idx);
                    p.children.len()
                }
                Node::Leaf(_) => unreachable!(),
            };
            if remaining == 0 {
                if path.is_empty() {
                    // The root itself emptied — the whole tree was deleted.
                    // Reset to a fresh empty leaf, reusing the root id.
                    debug_assert_eq!(self.total_slabs, 0);
                    self.nodes[parent_id as usize] = Some(Node::Leaf(Leaf::default()));
                    return;
                }
                continue; // cascade: remove the now-empty parent too
            }
            // Parent survives — refresh the ancestors above it and stop.
            self.update_ancestor_aggregates(&path, parent_id);
            return;
        }
    }

    /// While the root is an internal node with exactly one child, promote
    /// the child.  Keeps query depth proportional to the live tree after
    /// large deletions.  Called once per structural mutation.
    fn collapse_root(&mut self) {
        loop {
            let child = match self.node(self.root) {
                Node::Internal(n) if n.children.len() == 1 => n.children[0].id,
                _ => return,
            };
            self.free(self.root);
            self.root = child;
        }
    }

    /// After a leaf shrinks, merge it into an adjacent sibling when it is
    /// under a quarter full and the pair fits in one node — mirrors
    /// `Column::try_merge`'s (min = max/4, combined ≤ max) policy.
    ///
    /// Leaf-level only: internal nodes thin ~B× slower than leaves (they
    /// lose a child only when an entire leaf dies), so leaf merging is
    /// where fill degradation actually shows up.
    ///
    /// `path` ends with `(parent_id, child_idx)` for the shrunken leaf.
    fn maybe_merge_leaf(&mut self, path: &[(NodeId, usize)]) {
        let Some(&(parent_id, child_idx)) = path.last() else {
            return; // leaf is the root — nothing to merge with
        };
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
            Node::Internal(p) => {
                let d = &p.children[dst_idx];
                let s = &p.children[src_idx];
                (d.id, s.id, d.slab_count + s.slab_count)
            }
            Node::Leaf(_) => unreachable!(),
        };
        if combined > B {
            return;
        }
        let src_aggs = match self.node_mut(src_id) {
            Node::Leaf(l) => std::mem::take(&mut l.aggs),
            Node::Internal(_) => unreachable!("uniform depth: leaf siblings are leaves"),
        };
        match self.node_mut(dst_id) {
            Node::Leaf(l) => l.aggs.extend(src_aggs),
            Node::Internal(_) => unreachable!(),
        }
        self.free(src_id);
        let new_agg = self.agg_of(dst_id);
        match self.node_mut(parent_id) {
            Node::Internal(p) => {
                p.children[dst_idx].agg = new_agg;
                p.children[dst_idx].slab_count = combined;
                p.children.remove(src_idx);
            }
            Node::Leaf(_) => unreachable!(),
        }
        self.update_ancestor_aggregates(&path[..path.len() - 1], parent_id);
    }

    fn subtree_height(&self, id: NodeId) -> usize {
        match self.node(id) {
            Node::Leaf(_) => 0,
            Node::Internal(n) => {
                if n.children.is_empty() {
                    0
                } else {
                    1 + self.subtree_height(n.children[0].id)
                }
            }
        }
    }

    fn build_children_at_height(&mut self, aggs: &[A], height: usize) -> Vec<ChildSlot<A>> {
        if aggs.is_empty() {
            return Vec::new();
        }
        if height == 0 {
            return self.make_fresh_leaves(aggs);
        }
        let below = self.build_children_at_height(aggs, height - 1);
        if below.is_empty() {
            return Vec::new();
        }
        let num = below.len().div_ceil(B);
        let base = below.len() / num;
        let rem = below.len() % num;
        let mut result = Vec::with_capacity(num);
        let mut i = 0;
        for c in 0..num {
            let size = base + if c < rem { 1 } else { 0 };
            let children: Vec<ChildSlot<A>> = below[i..i + size].to_vec();
            let agg = merge_all(children.iter().map(|c| &c.agg));
            let count: usize = children.iter().map(|c| c.slab_count).sum();
            let id = self.alloc(Node::Internal(Internal { children }));
            result.push(ChildSlot {
                id,
                agg,
                slab_count: count,
            });
            i += size;
        }
        result
    }

    fn make_fresh_leaves(&mut self, aggs: &[A]) -> Vec<ChildSlot<A>> {
        if aggs.is_empty() {
            return Vec::new();
        }
        let total = aggs.len();
        let num = total.div_ceil(B);
        let base = total / num;
        let rem = total % num;
        let mut result = Vec::with_capacity(num);
        let mut i = 0;
        for c in 0..num {
            let size = base + if c < rem { 1 } else { 0 };
            let chunk = &aggs[i..i + size];
            let agg = merge_all(chunk);
            let leaf = Leaf {
                aggs: chunk.to_vec(),
            };
            let id = self.alloc(Node::Leaf(leaf));
            result.push(ChildSlot {
                id,
                agg,
                slab_count: size,
            });
            i += size;
        }
        result
    }

    fn split_and_propagate(&mut self, node_id: NodeId, path: Vec<(NodeId, usize)>) {
        let total = match self.node(node_id) {
            Node::Internal(n) => n.children.len(),
            _ => unreachable!(),
        };
        let num_chunks = total.div_ceil(B);
        let base = total / num_chunks;
        let rem = total % num_chunks;
        let first_size = base + if rem > 0 { 1 } else { 0 };

        let drained: Vec<ChildSlot<A>> = {
            let n = match self.node_mut(node_id) {
                Node::Internal(n) => n,
                _ => unreachable!(),
            };
            n.children.drain(first_size..).collect()
        };

        let mut replacement: Vec<ChildSlot<A>> = Vec::new();
        let (first_agg, first_count) = self.internal_summary(node_id);
        replacement.push(ChildSlot {
            id: node_id,
            agg: first_agg,
            slab_count: first_count,
        });

        let mut iter = drained.into_iter();
        for c in 1..num_chunks {
            let size = base + if c < rem { 1 } else { 0 };
            let children: Vec<ChildSlot<A>> = iter.by_ref().take(size).collect();
            let agg = merge_all(children.iter().map(|c| &c.agg));
            let count: usize = children.iter().map(|c| c.slab_count).sum();
            let id = self.alloc(Node::Internal(Internal { children }));
            replacement.push(ChildSlot {
                id,
                agg,
                slab_count: count,
            });
        }

        self.propagate_splits(path, replacement);
    }

    /// Splice into an empty tree.  If ≤ B new aggregates, fill the root
    /// leaf; otherwise bulk-build.
    fn splice_into_empty(&mut self, new: &[A]) {
        if new.is_empty() {
            return;
        }
        if new.len() <= B {
            match self.node_mut(self.root) {
                Node::Leaf(l) => l.aggs.extend_from_slice(new),
                // An empty tree always has a leaf root: `remove_and_cascade`
                // and the LCA wipe path both reset it.  Reaching this means
                // the tree was corrupted — fail loudly rather than silently
                // dropping the inserted aggregates.
                Node::Internal(_) => unreachable!("empty tree must have a leaf root"),
            }
            self.total_slabs = new.len();
            return;
        }
        *self = Self::from_iter(new.iter().cloned());
    }

    /// Try a single-leaf splice.  Returns `false` if `range` spans
    /// multiple leaves (caller falls back to rebuild).
    ///
    /// Handles two sub-cases:
    ///   * result ≤ B — in-place edit, O(log n) ancestor aggregate update.
    ///   * result > B — split the leaf into ⌈len/B⌉ pieces, cascade
    ///     splits up the parent chain, grow the root if it overflows.
    fn try_single_leaf_splice(&mut self, range: &Range<usize>, new: &[A]) -> bool {
        let Some((path, leaf_id, leaf_start)) = self.locate_leaf_for_start(range.start) else {
            return false;
        };

        let leaf_aggs_len = match self.node(leaf_id) {
            Node::Leaf(l) => l.aggs.len(),
            _ => unreachable!(),
        };
        let leaf_end = leaf_start + leaf_aggs_len;

        // Must fit entirely in this one leaf.
        if range.start < leaf_start || range.end > leaf_end {
            return false;
        }

        let local_start = range.start - leaf_start;
        let local_end = range.end - leaf_start;
        let new_leaf_len = leaf_aggs_len + new.len() - range.len();

        self.total_slabs = self.total_slabs + new.len() - range.len();

        // Fast path: result fits in one leaf.  Just splice in place and
        // walk the path updating aggregates.
        if new_leaf_len <= B {
            if new_leaf_len == 0 && leaf_id != self.root {
                // The splice deletes every agg in this leaf.  Empty
                // non-root nodes are not allowed — remove the leaf from
                // its parent, cascading if the parent empties too.
                self.remove_and_cascade(path);
                return true;
            }
            if let Node::Leaf(l) = self.node_mut(leaf_id) {
                l.aggs.splice(local_start..local_end, new.iter().cloned());
            }
            self.update_ancestor_aggregates(&path, leaf_id);
            if new.len() < range.len() {
                // The leaf shrank — merge with a sibling if it dropped
                // below a quarter full.
                self.maybe_merge_leaf(&path);
            }
            return true;
        }

        // Overflow: build the combined vector, split it into ≤ B-sized
        // chunks, reuse `leaf_id` for the first chunk and allocate new
        // leaves for the rest.  Then propagate up.
        let combined: Vec<A> = {
            let l = match self.node(leaf_id) {
                Node::Leaf(l) => l,
                _ => unreachable!(),
            };
            let mut v = Vec::with_capacity(new_leaf_len);
            v.extend_from_slice(&l.aggs[..local_start]);
            v.extend_from_slice(new);
            v.extend_from_slice(&l.aggs[local_end..]);
            v
        };

        let new_children = self.make_leaves_from_aggs(leaf_id, &combined);
        self.propagate_splits(path, new_children);
        true
    }

    /// Walk from the root toward the leaf containing slab index `start`.
    /// Returns the ancestor path (`(parent_id, child_idx)` stack), the
    /// leaf id, and `leaf_start` (the slab index of the leaf's first
    /// entry).  Handles the append case (`start == total_slabs`) by
    /// landing in the last leaf.
    #[allow(clippy::type_complexity)]
    fn locate_leaf_for_start(&self, start: usize) -> Option<(Vec<(NodeId, usize)>, NodeId, usize)> {
        let mut path: Vec<(NodeId, usize)> = Vec::with_capacity(8);
        let mut remaining = start;
        let mut leaf_start = 0usize;
        let mut node_id = self.root;
        loop {
            match self.node(node_id) {
                Node::Leaf(_) => return Some((path, node_id, leaf_start)),
                Node::Internal(n) => {
                    if n.children.is_empty() {
                        return None;
                    }
                    let last_idx = n.children.len() - 1;
                    let mut descended = false;
                    for (i, c) in n.children.iter().enumerate() {
                        // Descend into this child if `remaining` falls
                        // strictly inside its range, or if we're at the
                        // last child and hit its right boundary (append).
                        if remaining < c.slab_count || (i == last_idx && remaining == c.slab_count)
                        {
                            path.push((node_id, i));
                            node_id = c.id;
                            descended = true;
                            break;
                        }
                        remaining -= c.slab_count;
                        leaf_start += c.slab_count;
                    }
                    if !descended {
                        return None;
                    }
                }
            }
        }
    }

    /// After an in-place leaf edit, walk up `path` recomputing each
    /// ancestor's `agg` + `slab_count` from scratch.  O(log n) nodes,
    /// each summary is O(B) ops.
    fn update_ancestor_aggregates(&mut self, path: &[(NodeId, usize)], leaf_id: NodeId) {
        let mut current = leaf_id;
        for &(parent_id, child_idx) in path.iter().rev() {
            let new_agg = self.agg_of(current);
            let new_count = self.slab_count_of(current);
            if let Node::Internal(p) = self.node_mut(parent_id) {
                p.children[child_idx].agg = new_agg;
                p.children[child_idx].slab_count = new_count;
            }
            current = parent_id;
        }
    }

    /// Split `aggs` into ⌈len/B⌉ chunks as evenly as possible, reusing
    /// `first_leaf_id` for the first chunk and allocating new leaves for
    /// the rest.  Returns the resulting child slots in slab order.
    fn make_leaves_from_aggs(&mut self, first_leaf_id: NodeId, aggs: &[A]) -> Vec<ChildSlot<A>> {
        debug_assert!(!aggs.is_empty());
        let total = aggs.len();
        let num_leaves = total.div_ceil(B);
        let base = total / num_leaves;
        let rem = total % num_leaves;

        let mut result = Vec::with_capacity(num_leaves);
        let mut i = 0;
        for c in 0..num_leaves {
            let size = base + if c < rem { 1 } else { 0 };
            let chunk = &aggs[i..i + size];
            let agg = merge_all(chunk);
            let slab_count = size;
            let id = if c == 0 {
                if let Node::Leaf(l) = self.node_mut(first_leaf_id) {
                    l.aggs.clear();
                    l.aggs.extend_from_slice(chunk);
                }
                first_leaf_id
            } else {
                let leaf = Leaf {
                    aggs: chunk.to_vec(),
                };
                self.alloc(Node::Leaf(leaf))
            };
            result.push(ChildSlot {
                id,
                agg,
                slab_count,
            });
            i += size;
        }
        result
    }

    /// Cascade structural changes up the ancestor path.  At each level,
    /// `replacement` is the set of child slots that should replace the
    /// single child slot at `path[level]`.  If that overflows the
    /// parent, split the parent and produce a new replacement set for
    /// the next level up.  If we run out of path (reached the root) and
    /// still have multiple replacements, grow a new root.
    fn propagate_splits(&mut self, path: Vec<(NodeId, usize)>, mut replacement: Vec<ChildSlot<A>>) {
        for (parent_id, child_idx) in path.into_iter().rev() {
            let new_total = {
                let p = match self.node_mut(parent_id) {
                    Node::Internal(n) => n,
                    _ => unreachable!(),
                };
                p.children
                    .splice(child_idx..child_idx + 1, std::mem::take(&mut replacement));
                p.children.len()
            };

            if new_total <= B {
                let (agg, count) = self.internal_summary(parent_id);
                replacement.push(ChildSlot {
                    id: parent_id,
                    agg,
                    slab_count: count,
                });
                continue;
            }

            // Parent overflowed — split into ⌈new_total/B⌉ internals.
            let num_chunks = new_total.div_ceil(B);
            let base = new_total / num_chunks;
            let rem = new_total % num_chunks;
            let first_size = base + if rem > 0 { 1 } else { 0 };

            // Drain the overflow tail out of parent.
            let drained: Vec<ChildSlot<A>> = {
                let p = match self.node_mut(parent_id) {
                    Node::Internal(n) => n,
                    _ => unreachable!(),
                };
                p.children.drain(first_size..).collect()
            };

            let (first_agg, first_count) = self.internal_summary(parent_id);
            replacement.push(ChildSlot {
                id: parent_id,
                agg: first_agg,
                slab_count: first_count,
            });

            let mut iter = drained.into_iter();
            for c in 1..num_chunks {
                let size = base + if c < rem { 1 } else { 0 };
                let children: Vec<ChildSlot<A>> = iter.by_ref().take(size).collect();
                debug_assert_eq!(children.len(), size);
                let agg = merge_all(children.iter().map(|c| &c.agg));
                let count: usize = children.iter().map(|c| c.slab_count).sum();
                let id = self.alloc(Node::Internal(Internal { children }));
                replacement.push(ChildSlot {
                    id,
                    agg,
                    slab_count: count,
                });
            }
            debug_assert!(iter.next().is_none());
        }

        // Path exhausted — decide what to do at the root.
        match replacement.len() {
            0 => {}
            1 => {
                let slot = replacement[0].clone();
                if slot.id != self.root {
                    self.free(self.root);
                    self.root = slot.id;
                }
            }
            _ => {
                // Root overflowed — grow a new root one level up.
                let new_root = Internal {
                    children: replacement,
                };
                let new_root_id = self.alloc(Node::Internal(new_root));
                self.root = new_root_id;
            }
        }
    }

    fn internal_summary(&self, id: NodeId) -> (A, usize) {
        match self.node(id) {
            Node::Internal(n) => {
                let agg = merge_all(n.children.iter().map(|c| &c.agg));
                let count: usize = n.children.iter().map(|c| c.slab_count).sum();
                (agg, count)
            }
            _ => unreachable!(),
        }
    }

    fn slab_count_of(&self, id: NodeId) -> usize {
        match self.node(id) {
            Node::Leaf(l) => l.aggs.len(),
            Node::Internal(n) => n.children.iter().map(|c| c.slab_count).sum(),
        }
    }

    fn collect_into(&self, id: NodeId, out: &mut Vec<A>) {
        match self.node(id) {
            Node::Leaf(l) => out.extend_from_slice(&l.aggs),
            Node::Internal(n) => {
                for c in &n.children {
                    self.collect_into(c.id, out);
                }
            }
        }
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
