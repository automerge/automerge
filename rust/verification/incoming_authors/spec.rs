use vstd::prelude::*;

verus! {

// Read-only lookups supply a single model value for each queried input.
pub open spec fn model<T, R, F: Fn(T) -> R>(f: F, input: T) -> R {
    choose|r: R| f.ensures((input,), r)
}

pub open spec fn modeled<T, R, F: Fn(T) -> R>(f: F, input: T) -> bool {
    &&& f.requires((input,))
    &&& f.ensures((input,), model(f, input))
    &&& forall|r: R| #[trigger] f.ensures((input,), r) ==> r == model(f, input)
}

pub open spec fn matches_author<'a, K, A: 'a, F: Fn(K) -> Option<A>, C: Fn(K) -> Option<&'a A>>(
    expected: F, cached: C, key: K,
) -> bool {
    match (model(expected, key), model(cached, key)) {
        (Some(author), Some(saved)) => author == *saved,
        _ => false,
    }
}

} // verus!
