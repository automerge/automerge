use crate::lookup_spec::model;
use vstd::prelude::*;

verus! {

pub(crate) open spec fn no_witness<S, W, F: Fn(S) -> Option<W>>(entries: Seq<S>, check: F, end: int) -> bool {
    forall|i: int| 0 <= i < end ==> (#[trigger] model(check, entries[i])).is_none()
}

pub(crate) open spec fn search_result<S, W, F: Fn(S) -> Option<W>>(entries: Seq<S>, check: F, result: Option<W>) -> bool {
    match result {
        None => no_witness(entries, check, entries.len() as int),
        Some(w) => exists|i: int| 0 <= i < entries.len() && #[trigger] model(check, entries[i]) == Some(w)
            && no_witness(entries, check, i),
    }
}

pub(crate) proof fn report_witness<S, W, F: Fn(S) -> Option<W>>(entries: Seq<S>, check: F, i: int, witness: W)
    requires 0 <= i < entries.len(), model(check, entries[i]) == Some(witness), no_witness(entries, check, i),
    ensures search_result(entries, check, Some(witness)),
{}

// One success per frontier in the processed prefix, using the state left
// by the preceding success. A failure never exposes that partial state.
pub(crate) open spec fn trace<H, S, E, F: Fn(H, S) -> Result<S, E>>(
    hashes: Seq<H>, step: F, initial: S, states: Seq<S>, end: int,
) -> bool {
    &&& 0 <= end <= hashes.len()
    &&& states.len() == end + 1
    &&& states[0] == initial
    &&& forall|j: int| 0 <= j < end ==> #[trigger] step.ensures((hashes[j], states[j]), Ok(states[j + 1]))
}

pub(crate) open spec fn outcome<H, S, E, F: Fn(H, S) -> Result<S, E>>(
    hashes: Seq<H>, step: F, initial: S, result: Result<S, E>,
) -> bool {
    match result {
        Ok(final_state) => exists|states: Seq<S>| #[trigger] trace(hashes, step, initial, states, hashes.len() as int)
            && states[hashes.len() as int] == final_state,
        Err(error) => exists|states: Seq<S>, i: int| 0 <= i < hashes.len()
            && #[trigger] trace(hashes, step, initial, states, i)
            && step.ensures((hashes[i], states[i]), Err(error)),
    }
}

pub(crate) proof fn extend<H, S, E, F: Fn(H, S) -> Result<S, E>>(
    hashes: Seq<H>, step: F, initial: S, states: Seq<S>, i: int, next: S,
)
    requires 0 <= i < hashes.len(), trace(hashes, step, initial, states, i),
        step.ensures((hashes[i], states[i]), Ok(next)),
    ensures trace(hashes, step, initial, states.push(next), i + 1),
{
    let updated = states.push(next);
    assert forall|j: int| 0 <= j < i + 1
        implies #[trigger] step.ensures((hashes[j], updated[j]), Ok(updated[j + 1])) by {
        if j < i {
            assert(step.ensures((hashes[j], states[j]), Ok(states[j + 1])));
        }
    }
}

pub(crate) proof fn record_failure<H, S, E, F: Fn(H, S) -> Result<S, E>>(
    hashes: Seq<H>, step: F, initial: S, states: Seq<S>, i: int, error: E,
)
    requires 0 <= i < hashes.len(), trace(hashes, step, initial, states, i),
        step.ensures((hashes[i], states[i]), Err(error)),
    ensures outcome(hashes, step, initial, Err(error)),
{}

pub(crate) proof fn record_success<H, S, E, F: Fn(H, S) -> Result<S, E>>(
    hashes: Seq<H>, step: F, initial: S, states: Seq<S>, final_state: S,
)
    requires trace(hashes, step, initial, states, hashes.len() as int), states[hashes.len() as int] == final_state,
    ensures outcome(hashes, step, initial, Ok(final_state)),
{}

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

// The previous state never contributes evidence for a different group.
// Starting empty, applying one narrow attachment/author contract per step
// yields exactly the required evidence for this group and no other entries.
pub(crate) proof fn assembly_is_group_local<H, A, Sig, S, E, F: Fn(H, S) -> Result<S, E>>(
    group: Group<H, A, Sig, S>, hashes: Seq<H>, step: F, initial: S, states: Seq<S>, end: int,
)
    requires (group.retained)(initial).is_empty(), trace(hashes, step, initial, states, end),
        forall|h: H, prev: S, next: S| #[trigger] step.ensures((h, prev), Ok(next)) ==> local_step(group, h, prev, next),
    ensures evidence_exact(group, hashes, end, states[end]),
    decreases end,
{
    broadcast use vstd::seq_lib::group_seq_properties;
    if end > 0 {
        let prefix = states.drop_last();
        assert(trace(hashes, step, initial, prefix, end - 1)) by {
            assert forall|j: int| 0 <= j < end - 1
                implies #[trigger] step.ensures((hashes[j], prefix[j]), Ok(prefix[j + 1])) by {
                assert(step.ensures((hashes[j], states[j]), Ok(states[j + 1])));
            }
        }
        assembly_is_group_local(group, hashes, step, initial, prefix, end - 1);
        let hash = hashes[end - 1];
        assert(step.ensures((hash, states[end - 1]), Ok(states[end])));
        assert(local_step(group, hash, states[end - 1], states[end]));
        hashes.lemma_take_succ_push(end - 1);
        let entries = (group.retained)(states[end]);
        assert forall|h: H| #[trigger] hashes.take(end).contains(h) implies (group.has_supported_entry)(h) by {};
        assert forall|h: H| {
            &&& #[trigger] entries.contains_key(h) <==> hashes.take(end).contains(h) && (group.required_now)(h)
            &&& entries.contains_key(h) ==> admissible(group, h, entries[h])
        } by {};
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
