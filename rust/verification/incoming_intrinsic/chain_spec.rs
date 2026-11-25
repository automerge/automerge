use vstd::laws_cmp::obeys_cmp;
use vstd::laws_eq::obeys_concrete_eq;
use vstd::prelude::*;
use vstd::std_specs::{
    cmp::OrdSpec,
    slice::{cmp_le, sorted_by_le},
};

verus! {

pub(crate) open spec fn order_laws<A:Ord>() -> bool {
    obeys_concrete_eq::<A>() && obeys_cmp::<(A,u64,usize)>() && <(A,u64,usize) as OrdSpec>::obeys_cmp_spec()
}

pub(crate) open spec fn ordered<A:Ord>(rows:Seq<(A,u64,usize)>) -> bool {
    sorted_by_le(rows, |a:(A,u64,usize), b:(A,u64,usize)| cmp_le(a,b))
}

pub(crate) open spec fn bounded<A>(rows:Seq<(A,u64,usize)>) -> bool {
    forall|row:(A,u64,usize)| #[trigger] rows.contains(row) ==> 0 < row.1 <= u32::MAX
}

pub(crate) open spec fn edge_value<F:Fn(usize,usize)->bool>(edge:F, pred:usize, child:usize) -> bool {
    choose|r:bool| edge.ensures((pred,child),r)
}

pub(crate) open spec fn can_query<F:Fn(usize,usize)->bool>(edge:F, pred:usize, child:usize) -> bool {
    &&& edge.requires((pred,child))
    &&& edge.ensures((pred,child),edge_value(edge,pred,child))
    &&& forall|r:bool| #[trigger] edge.ensures((pred,child),r) ==> r == edge_value(edge,pred,child)
}

pub(crate) open spec fn stable_edges<A,F:Fn(usize,usize)->bool>(rows:Seq<(A,u64,usize)>, edge:F) -> bool {
    forall|p:(A,u64,usize), c:(A,u64,usize)| #[trigger] rows.contains(p) && #[trigger] rows.contains(c)
        && p.0 == c.0 && p.1+1 == c.1 ==> can_query(edge,p.2,c.2)
}

pub(crate) open spec fn adjacent<A,F:Fn(usize,usize)->bool>(left:(A,u64,usize), right:(A,u64,usize), edge:F) -> bool {
    left.0 != right.0 || (right.1 == left.1+1 && edge_value(edge,left.2,right.2))
}

pub(crate) open spec fn valid_prefix<A,F:Fn(usize,usize)->bool>(rows:Seq<(A,u64,usize)>, edge:F, end:int) -> bool {
    forall|i:int| 0 < i < end ==> adjacent(rows[i-1], #[trigger] rows[i], edge)
}

// Lexicographic (actor, seq, original position) order puts each actor in
// its own run. Each adjacent pair of that run must be an exact sequence
// successor and have the corresponding direct dependency edge.
pub(crate) open spec fn chain_result<A,F:Fn(usize,usize)->bool>(rows:Seq<(A,u64,usize)>, edge:F, result:Result<(),usize>) -> bool {
    match result {
        Ok(_) => valid_prefix(rows,edge,rows.len() as int),
        Err(child) => exists|i:int| 0 < i < rows.len() && #[trigger] rows[i].2 == child
            && !adjacent(rows[i-1],rows[i],edge) && valid_prefix(rows,edge,i),
    }
}

pub(crate) proof fn broken_at<A,F:Fn(usize,usize)->bool>(rows:Seq<(A,u64,usize)>, edge:F, i:int)
    requires 0 < i < rows.len(), valid_prefix(rows,edge,i), !adjacent(rows[i-1],rows[i],edge),
    ensures chain_result(rows,edge,Err(rows[i].2)),
{}

pub(crate) proof fn permutation_facts<A,F:Fn(usize,usize)->bool>(before:Seq<(A,u64,usize)>, after:Seq<(A,u64,usize)>, edge:F)
    requires bounded(before), stable_edges(before,edge), before.to_multiset() == after.to_multiset(),
    ensures bounded(after), stable_edges(after,edge),
{
    assert forall|r:(A,u64,usize)| #[trigger] after.contains(r) implies before.contains(r) by {
        vstd::seq_lib::to_multiset_contains(before,r);
        vstd::seq_lib::to_multiset_contains(after,r);
    }
}

} // verus!
