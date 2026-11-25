use vstd::prelude::*;

verus! {

// One immutable group attachment and one author resolver for the proof
// being built. Signature verification is an external operation; its witness
// must identify the exact signature tested with the exact hash and author.
#[verifier::reject_recursive_types(H)]
#[verifier::reject_recursive_types(A)]
#[verifier::reject_recursive_types(Sig)]
#[verifier::reject_recursive_types(S)]
pub(crate) ghost struct Group<H, A, Sig, S> {
    pub attached: spec_fn(H, Sig) -> bool,
    pub has_supported_entry: spec_fn(H) -> bool,
    pub established_author: spec_fn(H) -> Option<A>,
    pub valid: spec_fn(H, A, Sig) -> bool,
    pub required_now: spec_fn(H) -> bool,
    pub retained: spec_fn(S) -> Map<H, (A, Sig)>,
}

pub(crate) open spec fn local_step<H, A, Sig, S>(group: Group<H, A, Sig, S>, hash: H, before: S, after: S) -> bool {
    let prior = (group.retained)(before);
    let next = (group.retained)(after);
    &&& (group.has_supported_entry)(hash)
    &&& if (group.required_now)(hash) {
        &&& next.contains_key(hash)
        &&& next == prior.insert(hash, next[hash])
        &&& admissible(group, hash, next[hash])
    } else {
        next == prior
    }
}

pub(crate) open spec fn admissible<H, A, Sig, S>(group: Group<H, A, Sig, S>, hash: H, witness: (A, Sig)) -> bool {
    let (author, sig) = witness;
    &&& (group.attached)(hash, sig)
    &&& (group.established_author)(hash) == Some(author)
    &&& (group.valid)(hash, author, sig)
}

pub(crate) open spec fn evidence_exact<H, A, Sig, S>(group: Group<H, A, Sig, S>, hashes: Seq<H>, end: int, out: S) -> bool {
    let entries = (group.retained)(out);
    &&& forall|h: H| #[trigger] hashes.take(end).contains(h) ==> (group.has_supported_entry)(h)
    &&& forall|h: H| {
        &&& #[trigger] entries.contains_key(h) <==> hashes.take(end).contains(h) && (group.required_now)(h)
        &&& entries.contains_key(h) ==> admissible(group, h, entries[h])
    }
}

// The reuse gate supplies the settled-author equality for every required
// hash. Reusing the same store of the same delivery preserves sufficiency
// and provenance, even if author resolution was tentative in Stage 1.
pub(crate) proof fn cache_reuse<H, A, Sig, S>(
    early: Group<H, A, Sig, S>, settled: Group<H, A, Sig, S>, hashes: Seq<H>, cached: S,
)
    requires evidence_exact(early, hashes, hashes.len() as int, cached),
        early.retained == settled.retained, early.attached == settled.attached,
        early.valid == settled.valid, early.has_supported_entry == settled.has_supported_entry,
        forall|h: H| #[trigger] hashes.contains(h) ==> (settled.required_now)(h),
        forall|h: H| #[trigger] hashes.contains(h) ==> {
            let entries = (early.retained)(cached);
            entries.contains_key(h) && (settled.established_author)(h) == Some(entries[h].0)
        },
    ensures evidence_exact(settled, hashes, hashes.len() as int, cached),
{
    assert(hashes.take(hashes.len() as int) =~= hashes);
    let entries = (early.retained)(cached);
    assert forall|h: H| {
        &&& #[trigger] entries.contains_key(h) <==> hashes.contains(h) && (settled.required_now)(h)
        &&& entries.contains_key(h) ==> admissible(settled, h, entries[h])
    } by {
        if entries.contains_key(h) {
            assert(admissible(early, h, entries[h]));
        }
    }
}

} // verus!
