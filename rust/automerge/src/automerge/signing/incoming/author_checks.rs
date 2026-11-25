//! Author-sensitive checks shared with the Verus harness.

#![allow(unexpected_cfgs)]

#[cfg(automerge_verus)]
use vstd::laws_eq::obeys_concrete_eq;
#[cfg(automerge_verus)]
use vstd::prelude::*;
#[cfg(automerge_verus)]
use vstd::std_specs::cmp::PartialEqSpec;

#[cfg_attr(automerge_verus, verus_spec(equal =>
    requires obeys_concrete_eq::<A>(),
    ensures equal == (*left == *right),
))]
pub(super) fn same<A: Eq>(left: &A, right: &A) -> bool {
    let equal = <A as PartialEq>::eq(left, right);
    #[cfg(automerge_verus)]
    proof! {
        reveal(obeys_concrete_eq);
        assert(<A as PartialEqSpec>::eq_spec(left, right) == (*left == *right));
    }
    equal
}

/// Once a hash is applied, use its historical slot, even if it has no
/// author. The candidate table is only a source for unapplied hashes.
#[cfg_attr(automerge_verus, verus_spec(out =>
    requires match slot { Some(i)=>history.requires((i,)), None=>candidate.requires(()) },
    ensures match slot {
        Some(i)=>history.ensures((i,),out),
        None=>({
            exists|answer:Option<Option<A>>| #[trigger] candidate.ensures((),answer)
                && out==crate::source_spec::author_value(answer)
        }),
    },
))]
pub(super) fn established<A>(
    slot: Option<usize>,
    history: impl FnOnce(usize) -> Option<A>,
    candidate: impl FnOnce() -> Option<Option<A>>,
) -> Option<A> {
    match slot {
        Some(i) => history(i),
        None => candidate().unwrap_or_default(),
    }
}

/// Before ancestry resolution, obey the existing precedence: a registered
/// actor is governed by the applied-history lookup (including "not yet
/// applied"); only an unregistered actor can use a delivery or batch hint.
#[cfg_attr(automerge_verus, verus_spec(out =>
    requires match slot {
        Some(i)=>history.requires((i,)),
        None=>delivery.requires(()) && (delivery.ensures((),None) ==> batch.requires(())),
    },
    ensures match slot {
        Some(i)=>history.ensures((i,),out),
        None=>match out {
            Some(_) => delivery.ensures((),out) || (delivery.ensures((),None) && batch.ensures((),out)),
            None => delivery.ensures((),None) && batch.ensures((),None),
        },
    },
))]
pub(super) fn tentative<A>(
    slot: Option<usize>,
    history: impl FnOnce(usize) -> Option<Option<A>>,
    delivery: impl FnOnce() -> Option<Option<A>>,
    batch: impl FnOnce() -> Option<Option<A>>,
) -> Option<Option<A>> {
    if let Some(i) = slot {
        return history(i);
    }
    match delivery() {
        Some(author) => Some(author),
        None => batch(),
    }
}
