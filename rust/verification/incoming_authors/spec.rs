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

pub open spec fn link_author<K, A, T, F: Fn(K) -> Option<(A, u64, T)>>(
    describe: F, dep: K, actor: A, seq: u64,
) -> Option<T> {
    match model(describe, dep) {
        Some((dep_actor, dep_seq, source)) if actor == dep_actor && seq == dep_seq => Some(source),
        _ => None,
    }
}

pub open spec fn predecessor_result<K, A, T, F: Fn(K) -> Option<(A, u64, T)>>(
    deps: Seq<K>, describe: F, actor: A, seq: u64, result: Option<T>,
) -> bool {
    match result {
        None => forall|j: int| 0 <= j < deps.len() ==> link_author(describe, #[trigger] deps[j], actor, seq).is_none(),
        Some(author) => exists|i: int| {
            &&& 0 <= i < deps.len()
            &&& link_author(describe, #[trigger] deps[i], actor, seq) == Some(author)
            &&& forall|j: int| 0 <= j < i ==> link_author(describe, #[trigger] deps[j], actor, seq).is_none()
        },
    }
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
