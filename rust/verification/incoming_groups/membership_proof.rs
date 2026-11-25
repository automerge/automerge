use vstd::prelude::*;

verus! {

pub(crate) proof fn equal_cardinality<K>(canonical: Seq<K>, delivered: Set<K>)
    requires canonical.to_set().len() == delivered.len(),
        forall|j: int| 0 <= j < canonical.len() ==> delivered.contains(#[trigger] canonical[j]),
    ensures canonical.to_set() =~= delivered,
{
    assert(canonical.to_set().subset_of(delivered)) by {
        assert forall|k: K| #[trigger] canonical.to_set().contains(k) implies delivered.contains(k) by {
            let j = choose|j: int| 0 <= j < canonical.len() && canonical[j] == k;
            assert(delivered.contains(canonical[j]));
        }
    }
    if !delivered.subset_of(canonical.to_set()) {
        let extra = choose|k: K| #[trigger] delivered.contains(k) && !canonical.to_set().contains(k);
        assert(canonical.to_set().subset_of(delivered.remove(extra)));
        vstd::set_lib::lemma_len_subset(canonical.to_set(), delivered.remove(extra));
        assert(delivered.remove(extra).len() + 1 == delivered.len());
        assert(false);
    }
}

} // verus!
