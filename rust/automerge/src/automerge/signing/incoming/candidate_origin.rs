//! Preserve a delivery's provenance when retrying the pending queue.

#![allow(unexpected_cfgs)]

use super::pending::Origin;

#[cfg(automerge_verus)]
use crate::origin_spec::*;
#[cfg(automerge_verus)]
use vstd::prelude::*;

#[cfg_attr(automerge_verus, verus_verify)]
pub(crate) struct Candidate<G> {
    pub origin: Origin,
    pub group: G,
}

/// Old pending deliveries precede fresh deliveries. Neither list is filtered
/// or reordered: equal content may carry different attached evidence.
#[cfg_attr(automerge_verus, verus_spec(out =>
    requires pending@.len()+incoming@.len()<=usize::MAX,
    ensures out@ =~= tagged(pending@,Origin::ExistingPending)+tagged(incoming@,Origin::IncomingBatch),
))]
pub(super) fn candidates<G>(pending: Vec<G>, incoming: Vec<G>) -> Vec<Candidate<G>> {
    let mut out = Vec::with_capacity(pending.len() + incoming.len());
    append(&mut out, pending, Origin::ExistingPending);
    append(&mut out, incoming, Origin::IncomingBatch);
    out
}

#[cfg_attr(automerge_verus, verus_spec(
    ensures final(out)@ =~= old(out)@ + tagged(groups@, tag),
))]
fn append<G>(out: &mut Vec<Candidate<G>>, groups: Vec<G>, tag: Origin) {
    #[cfg(automerge_verus)]
    proof_decl! {
        let ghost input=groups@;
        let ghost base=out@;
    }
    #[cfg_attr(automerge_verus, verus_spec(iter =>
        invariant iter.seq()==input, base==old(out)@,
            out@ =~= base+tagged(input.take(iter.index()),tag),
        ensures out@ =~= base+tagged(input,tag), base==old(out)@,
    ))]
    for group in groups {
        #[cfg(automerge_verus)]
        proof! { append_tagged(input,tag,iter.index()); }
        out.push(Candidate { origin: tag, group });
    }
}
